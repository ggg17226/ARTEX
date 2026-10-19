package db

// =====================================================================
// 任务 ↔ 全局资产关联（P08-02 Task 2.3）
//
// 两条硬约束：
//  1. **写者协议**（计划 §3.2.2）：所有"向任务追加/剥离子实体"的写者都经
//     `withTaskChildWriter` → `DB.runTaskChildWriter`：业务门禁 → 行锁父任务并核验
//     存活 → 才写子实体。与任务级联删除共用同一把全局门禁，因此"给正在消失的任务
//     挂孤儿资产"这一交错在结构上不可能发生（I1）。
//  2. **双写原子性**（M26 / M30）：PG 靠 `trg_assets_task_links` 触发器把
//     `assets.task_ids` 同步到 `task_asset_links`。TiDB 没有这套触发器，双写改由
//     `attachTaskAssetTx` / `detachTaskAssetTx` 显式完成：**先锁资产行**，再
//     读旧 JSON → 合并/剥离 → 写回，最后按 `row_id` 精确增删 link 行；同一事务内
//     完成，任何一步失败整体回滚，绝不留下"JSON 改了、link 没跟上"的半态。
//
// 方言：占位符 `?`；`$n=ANY(task_ids)` → `JSON_CONTAINS(task_ids, CAST(? AS JSON))`；
// `array_append/array_remove` → 应用层 JSON 合并（parseTaskIDArray 严格 int64，R3）；
// 无 `RETURNING` → `LastInsertId()` + 同事务回读。
// =====================================================================

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"unicode/utf8"
)

const (
	MaxTaskAssetMutationCount = 100
	MaxTaskAssetSummaryRunes  = 500
	defaultTaskAssetSource    = "system"
	manualTaskScopeSummary    = "用户在测试资产页手工新增"
)

var (
	ErrTaskAssetInvalid       = errors.New("invalid task asset association")
	ErrTaskAssetTaskNotFound  = errors.New("task not found")
	ErrTaskAssetAssetNotFound = errors.New("asset not found")
)

// TaskAssetMutation summarizes one attach request. Attached counts newly added
// associations; Existing counts requested assets that were already on the task.
type TaskAssetMutation struct {
	Requested int `json:"requested"`
	Attached  int `json:"attached"`
	Existing  int `json:"existing"`
}

// TaskAssetScopeMutation summarizes one free-form scope registration. Domain
// and IP entries create or reuse global assets; every entry also becomes an
// idempotent task_scope row.
type TaskAssetScopeMutation struct {
	Requested      int `json:"requested"`
	AssetsLinked   int `json:"assets_linked"`
	AssetsExisting int `json:"assets_existing"`
	ScopesAdded    int `json:"scopes_added"`
	ScopesExisting int `json:"scopes_existing"`
}

// IntentAsset describes an asset explicitly anchored to a worker intent.
//
// P03（R09）：intent_id / asset_id / source_node_id / source_task_id 是 BIGINT，
// 对外带引号 string（nil 指针仍是 null / omitempty）。仅 SQL 扫描填充，不是请求体
// 解码目标，也没有整包 Marshal 后回读的路径。
type IntentAsset struct {
	IntentID      int64  `json:"intent_id,string"`
	AssetID       int64  `json:"asset_id,string"`
	Type          string `json:"type"`
	Label         string `json:"label"`
	Source        string `json:"source"`
	SourceSummary string `json:"source_summary"`
	SourceNodeID  *int64 `json:"source_node_id,string,omitempty"`
	SourceTaskID  int64  `json:"source_task_id,string"`
	Inherited     bool   `json:"inherited"`
}

func normalizeTaskAssetIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: asset_ids is required", ErrTaskAssetInvalid)
	}
	seen := make(map[int64]struct{}, len(ids))
	normalized := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("%w: asset id must be positive", ErrTaskAssetInvalid)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
		if len(normalized) > MaxTaskAssetMutationCount {
			return nil, fmt.Errorf("%w: at most %d assets per request", ErrTaskAssetInvalid, MaxTaskAssetMutationCount)
		}
	}
	return normalized, nil
}

func normalizeTaskAssetSource(source, summary string) (string, string, error) {
	source = strings.TrimSpace(strings.ToLower(source))
	if source == "" {
		source = defaultTaskAssetSource
	}
	summary = strings.TrimSpace(summary)
	if utf8.RuneCountInString(summary) > MaxTaskAssetSummaryRunes {
		return "", "", fmt.Errorf("%w: source summary exceeds %d characters", ErrTaskAssetInvalid, MaxTaskAssetSummaryRunes)
	}
	return source, summary, nil
}

// ---------------------------------------------------------------------------
// 写者协议入口（AssetStore 侧）
// ---------------------------------------------------------------------------

// withTaskChildWriter 把 AssetStore 的"任务子实体写者"接入统一写者协议：
//   - 已在外部事务里（s.tx != nil）：直接执行 —— 外层写者事务已经持有业务门禁与
//     父任务行锁，重复加锁只会自锁；
//   - 否则：走 DB.runTaskChildWriter（门禁 + 父任务行锁 + 存活核验 + 整体退避重放）。
func (s *AssetStore) withTaskChildWriter(taskID int64, fn func(ctx context.Context, tx *sql.Tx) error) error {
	if s.tx != nil {
		return fn(context.Background(), s.tx)
	}
	return s.db.runTaskChildWriter(taskID, fn)
}

// mapTaskWriterGone 把写者协议的"父任务已消失"翻译成各调用点原有的错误语义。
func mapTaskWriterGone(err error) error {
	if errors.Is(err, ErrTaskNotAlive) {
		return ErrTaskAssetTaskNotFound
	}
	return err
}

// ---------------------------------------------------------------------------
// 双写原语（assets.task_ids ↔ task_asset_links）
// ---------------------------------------------------------------------------

// lockAssetForTaskLink 以行锁读取资产的当前 task_ids 载荷。
// 锁资产行是共享资产并发 attach/detach 的**唯一**串行点：两个任务同时剥离各自的
// task_id 时，"读旧值 → 改写" 必须互斥，否则后写者会覆盖前写者的位。
func lockAssetForTaskLink(ctx context.Context, tx *sql.Tx, assetID int64) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(task_ids, '[]') FROM assets WHERE id = ? FOR UPDATE`, assetID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: asset %d", ErrTaskAssetAssetNotFound, assetID)
	}
	if err != nil {
		return "", err
	}
	return raw, nil
}

// upsertTaskAssetLinkTx 按 (task_id, asset_id) 唯一键 upsert 一条 link 行。
// 唯一键冲突在 TiDB 上由 ON DUPLICATE KEY UPDATE 表达（PG 的 ON CONFLICT 等价物）；
// source_node_id 传 nil 表示"保留既有来源节点"，与 COALESCE 语义一致。
func upsertTaskAssetLinkTx(
	ctx context.Context, tx *sql.Tx,
	taskID, assetID int64, source, summary string, sourceNodeID any,
) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO task_asset_links(task_id, asset_id, source, source_summary, source_node_id, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, NOW(3), NOW(3))
ON DUPLICATE KEY UPDATE
    source=VALUES(source),
    source_summary=VALUES(source_summary),
    source_node_id=COALESCE(VALUES(source_node_id), source_node_id),
    updated_at=NOW(3)`, taskID, assetID, source, summary, sourceNodeID)
	return err
}

// attachTaskAssetTx 把一个已存在的全局资产挂到任务上（双写第一步：锁资产行 →
// 合并 task_ids → 写回；第二步：upsert link 行）。返回 attached 表示本次是否新增
// 了归属（已存在时 link 仍会被刷新来源信息）。
func attachTaskAssetTx(
	ctx context.Context, tx *sql.Tx,
	taskID, assetID int64, source, summary string, sourceNodeID any,
) (bool, error) {
	raw, err := lockAssetForTaskLink(ctx, tx, assetID)
	if err != nil {
		return false, err
	}
	merged, err := assetUnionTaskIDs(raw, []int64{taskID})
	if err != nil {
		return false, fmt.Errorf("asset %d task_ids: %w", assetID, err)
	}
	attached := merged != raw
	if attached {
		if _, err := tx.ExecContext(ctx,
			`UPDATE assets SET task_ids = ?, updated_at = NOW(3) WHERE id = ?`, merged, assetID); err != nil {
			return false, err
		}
	}
	if err := upsertTaskAssetLinkTx(ctx, tx, taskID, assetID, source, summary, sourceNodeID); err != nil {
		return false, err
	}
	return attached, nil
}

// detachTaskAssetTx 只剥离本任务那一位（共享资产绝不误删），并按**已锁定的物理
// row_id** 精确删除本任务的 link 行。
func detachTaskAssetTx(ctx context.Context, tx *sql.Tx, taskID, assetID int64) (bool, error) {
	raw, err := lockAssetForTaskLink(ctx, tx, assetID)
	if err != nil {
		if errors.Is(err, ErrTaskAssetAssetNotFound) {
			return false, nil
		}
		return false, err
	}
	persisted, err := parseTaskIDArray(raw)
	if err != nil {
		return false, fmt.Errorf("asset %d task_ids: %w", assetID, err)
	}
	kept := make([]int64, 0, len(persisted))
	var found bool
	for _, id := range persisted {
		if id == taskID {
			found = true
			continue
		}
		kept = append(kept, id)
	}
	// link 行先按 row_id 锁定再按 row_id 删除：与级联执行器同一口径（物理 PK 定位），
	// 也避开"MySQL 不能在 DML 的子查询里 FOR UPDATE"。
	var linkRowIDs []uint64
	if err := queryUint64s(ctx, tx, &linkRowIDs, `SELECT row_id FROM task_asset_links
WHERE task_id = ? AND asset_id = ? ORDER BY row_id FOR UPDATE`, taskID, assetID); err != nil {
		return false, err
	}
	for _, rowID := range linkRowIDs {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM task_asset_links WHERE row_id = ?`, rowID); err != nil {
			return false, err
		}
	}
	if !found {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE assets SET task_ids = ?, updated_at = NOW(3) WHERE id = ?`, marshalTaskIDs(kept), assetID); err != nil {
		return false, err
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// 对外写者
// ---------------------------------------------------------------------------

// SetTaskAssetSource improves the generic trigger-created provenance for one
// existing task association. It never creates or deletes an asset.
func (s *AssetStore) SetTaskAssetSource(taskID, assetID int64, source, summary string, sourceNodeID *int64) error {
	if taskID <= 0 || assetID <= 0 {
		return fmt.Errorf("%w: task and asset ids must be positive", ErrTaskAssetInvalid)
	}
	source, summary, err := normalizeTaskAssetSource(source, summary)
	if err != nil {
		return err
	}
	var nodeArg any
	if sourceNodeID != nil {
		nodeArg = *sourceNodeID
	}
	err = s.withTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		// 只允许改善**已存在**的关联：资产行的 task_ids 必须已经包含本任务。
		raw, err := lockAssetForTaskLink(ctx, tx, assetID)
		if err != nil {
			return err
		}
		persisted, err := parseTaskIDArray(raw)
		if err != nil {
			return fmt.Errorf("asset %d task_ids: %w", assetID, err)
		}
		var linked bool
		for _, id := range persisted {
			if id == taskID {
				linked = true
				break
			}
		}
		if !linked {
			return fmt.Errorf("%w: task or asset association does not exist", ErrTaskAssetInvalid)
		}
		return upsertTaskAssetLinkTx(ctx, tx, taskID, assetID, source, summary, nodeArg)
	})
	return mapTaskWriterGone(err)
}

// RegisterTaskAssetScopes accepts the same structured scope rules as enterprise
// assets. The entire request is atomic: invalid input or any storage failure
// leaves both global assets and task scope unchanged.
func (s *AssetStore) RegisterTaskAssetScopes(taskID int64, inputs []ScopeInput) (TaskAssetScopeMutation, error) {
	mutation := TaskAssetScopeMutation{Requested: len(inputs)}
	if taskID <= 0 {
		return mutation, fmt.Errorf("%w: task id must be positive", ErrTaskAssetInvalid)
	}
	if len(inputs) == 0 {
		return mutation, fmt.Errorf("%w: scope is required", ErrTaskAssetInvalid)
	}
	if err := ValidateCompanyScopeInputBounds(inputs); err != nil {
		return mutation, fmt.Errorf("%w: %v", ErrTaskAssetInvalid, err)
	}
	parsed := make([]ParsedScope, 0, len(inputs))
	for index, input := range inputs {
		rule, err := ParseScopeInput(input)
		if err != nil {
			return mutation, fmt.Errorf("%w: 第 %d 条范围无效: %v", ErrTaskAssetInvalid, index+1, err)
		}
		parsed = append(parsed, rule)
	}
	if err := validateParsedScopeBounds(parsed); err != nil {
		return mutation, fmt.Errorf("%w: %v", ErrTaskAssetInvalid, err)
	}

	err := s.withTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		// Company scope edits rebuild assets.company_id. Serialize the task-scope
		// snapshot with those edits（企业/范围写者屏障，Task 2.2 I1）。
		if err := lockCompanyScopeMutation(tx); err != nil {
			return err
		}
		scoped := &AssetStore{db: s.db, company: s.company, tx: tx}
		for _, rule := range parsed {
			taskScope := TaskScope{TaskID: taskID, Source: "manual", Reason: manualTaskScopeSummary}
			var assetID int64
			switch rule.Kind {
			case "domain":
				taskScope.Kind = "root_domain"
				taskScope.Domain = rule.Domain
				var alreadyLinked bool
				err := tx.QueryRowContext(ctx, `SELECT id, `+
					assetJSONContains("COALESCE(task_ids, '[]')", "?")+
					` FROM assets WHERE type='root_domain' AND domain=?`, taskID, rule.Domain).
					Scan(&assetID, &alreadyLinked)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				assetID, err = scoped.UpsertRootDomain(UpsertRootDomainReq{Domain: rule.Domain, TaskID: taskID})
				if err != nil {
					return err
				}
				if alreadyLinked {
					mutation.AssetsExisting++
				} else {
					mutation.AssetsLinked++
				}
			case "ip":
				taskScope.Kind = "ip"
				taskScope.Net = rule.Net
				ip, _, parseErr := net.ParseCIDR(rule.Net)
				if parseErr != nil {
					return fmt.Errorf("%w: 无效 IP: %s", ErrTaskAssetInvalid, rule.Raw)
				}
				ipValue := ip.String()
				var alreadyLinked bool
				err := tx.QueryRowContext(ctx, `SELECT id, `+
					assetJSONContains("COALESCE(task_ids, '[]')", "?")+
					` FROM assets WHERE type='ip' AND ip=?`, taskID, ipValue).
					Scan(&assetID, &alreadyLinked)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				assetID, err = scoped.UpsertIP(UpsertIPReq{IP: ipValue, TaskID: taskID})
				if err != nil {
					return err
				}
				if alreadyLinked {
					mutation.AssetsExisting++
				} else {
					mutation.AssetsLinked++
				}
			case "cidr":
				taskScope.Kind = "cidr"
				taskScope.Net = rule.Net
			case "icp", "keyword":
				taskScope.Kind = rule.Kind
				taskScope.Value = rule.Value
			default:
				return fmt.Errorf("%w: unsupported scope kind %q", ErrTaskAssetInvalid, rule.Kind)
			}

			if assetID > 0 {
				if _, err := attachTaskAssetTx(ctx, tx, taskID, assetID, "manual", manualTaskScopeSummary, nil); err != nil {
					return err
				}
			}
			inserted, err := scoped.upsertTaskScopeResult(taskScope)
			if err != nil {
				return err
			}
			if inserted {
				mutation.ScopesAdded++
			} else {
				mutation.ScopesExisting++
			}
		}
		return nil
	})
	return mutation, mapTaskWriterGone(err)
}

// AttachAssetsToTask associates existing global assets with one live task and
// records an operator-authored source summary. Global asset rows are retained.
func (s *AssetStore) AttachAssetsToTask(taskID int64, assetIDs []int64, sourceSummary string) (TaskAssetMutation, error) {
	var mutation TaskAssetMutation
	assetIDs, err := normalizeTaskAssetIDs(assetIDs)
	if err != nil {
		return mutation, err
	}
	_, sourceSummary, err = normalizeTaskAssetSource("manual", sourceSummary)
	if err != nil {
		return mutation, err
	}
	if sourceSummary == "" {
		return mutation, fmt.Errorf("%w: source_summary is required", ErrTaskAssetInvalid)
	}
	mutation.Requested = len(assetIDs)

	err = s.withTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		// 先按 id 升序锁全部候选资产行：与并发 attach / 级联删除同序，杜绝交叉等待死锁。
		ph := inPlaceholders(len(assetIDs))
		rows, err := tx.QueryContext(ctx,
			`SELECT id, COALESCE(task_ids, '[]') FROM assets WHERE id IN (`+ph+`) ORDER BY id FOR UPDATE`,
			int64Args(assetIDs)...)
		if err != nil {
			return err
		}
		type pending struct {
			id       int64
			merge    string
			attached bool
		}
		targets := make([]pending, 0, len(assetIDs))
		for rows.Next() {
			var id int64
			var raw string
			if err := rows.Scan(&id, &raw); err != nil {
				rows.Close()
				return err
			}
			merged, err := assetUnionTaskIDs(raw, []int64{taskID})
			if err != nil {
				rows.Close()
				return fmt.Errorf("asset %d task_ids: %w", id, err)
			}
			targets = append(targets, pending{id: id, merge: merged, attached: merged != raw})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(targets) != len(assetIDs) {
			return ErrTaskAssetAssetNotFound
		}
		for _, target := range targets {
			if target.attached {
				if _, err := tx.ExecContext(ctx,
					`UPDATE assets SET task_ids = ?, updated_at = NOW(3) WHERE id = ?`, target.merge, target.id); err != nil {
					return err
				}
			}
			if err := upsertTaskAssetLinkTx(ctx, tx, taskID, target.id, "manual", sourceSummary, nil); err != nil {
				return err
			}
			if target.attached {
				mutation.Attached++
			} else {
				mutation.Existing++
			}
		}
		return nil
	})
	return mutation, mapTaskWriterGone(err)
}

// DetachAssetFromTask removes only the task association. The global asset and
// exploration anchors remain available for historical blackboard auditing.
func (s *AssetStore) DetachAssetFromTask(taskID, assetID int64) (bool, error) {
	if taskID <= 0 || assetID <= 0 {
		return false, fmt.Errorf("%w: task and asset ids must be positive", ErrTaskAssetInvalid)
	}
	var detached bool
	err := s.withTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		detached, err = detachTaskAssetTx(ctx, tx, taskID, assetID)
		return err
	})
	if err != nil && !errors.Is(err, ErrTaskAssetAssetNotFound) {
		return false, mapTaskWriterGone(err)
	}
	return detached, nil
}

func (s *AssetStore) hydrateTaskAssetSources(taskID int64, assets []*Asset) error {
	if len(assets) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(assets))
	byID := make(map[int64]*Asset, len(assets))
	for _, asset := range assets {
		ids = append(ids, asset.ID)
		byID[asset.ID] = asset
	}
	rows, err := s.db.Query(`
SELECT asset_id, source, source_summary, source_node_id
FROM task_asset_links
WHERE task_id=? AND asset_id IN (`+inPlaceholders(len(ids))+`)`,
		append([]any{taskID}, int64Args(ids)...)...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var assetID int64
		var source, summary string
		var sourceNodeID sql.NullInt64
		if err := rows.Scan(&assetID, &source, &summary, &sourceNodeID); err != nil {
			return err
		}
		if asset := byID[assetID]; asset != nil {
			asset.TaskSource = source
			asset.TaskSourceSummary = summary
			if sourceNodeID.Valid {
				id := sourceNodeID.Int64
				asset.TaskSourceNodeID = &id
			}
		}
	}
	return rows.Err()
}

// IntentAssets returns all local worker targets plus immutable targets from the
// task's direct sources. Inherited non-terminal intents remain hidden, matching
// the existing source-aware session contract.
func (s *AssetStore) IntentAssets(taskID int64) ([]IntentAsset, error) {
	rows, err := s.db.Query(`
WITH context AS (
    SELECT task.id AS task_id, task.exploration_id, false AS inherited
    FROM tasks task
    WHERE task.id=? AND task.deleted_at IS NULL
    UNION ALL
    SELECT source.id, source.exploration_id, true
    FROM task_relations relation
    JOIN tasks source ON source.id=relation.source_task_id AND source.deleted_at IS NULL
    WHERE relation.task_id=?
)
SELECT intent.id, asset.id, asset.type,
       CASE asset.type
         WHEN 'root_domain' THEN COALESCE(asset.domain,'')
         WHEN 'subdomain' THEN COALESCE(asset.domain,'')
         WHEN 'ip' THEN COALESCE(asset.ip,'')
         WHEN 'app' THEN COALESCE(asset.app_name,'')
         WHEN 'service' THEN COALESCE(NULLIF(asset.url,''),
              NULLIF(CONCAT_WS(':', COALESCE(NULLIF(asset.domain,''), NULLIF(asset.ip,'')), CAST(asset.port AS CHAR)),''),
              NULLIF(asset.service_name,''),
              CONCAT('#', CAST(asset.id AS CHAR)))
         WHEN 'endpoint' THEN COALESCE(NULLIF(asset.url,''), CONCAT('#', CAST(asset.id AS CHAR)))
         ELSE CONCAT('#', CAST(asset.id AS CHAR))
       END,
       COALESCE(link.source,'anchor'),
       COALESCE(NULLIF(link.source_summary,''), '意图在黑板中锚定该资产'),
       link.source_node_id, context.task_id, context.inherited
FROM context
JOIN exploration_nodes intent ON intent.exploration_id=context.exploration_id AND intent.kind='intent'
JOIN exploration_anchors anchor ON anchor.node_id=intent.id
JOIN assets asset ON asset.id=anchor.asset_id
LEFT JOIN task_asset_links link ON link.task_id=context.task_id AND link.asset_id=asset.id
WHERE NOT context.inherited OR intent.state IN ('done','blocked','exhausted','stopped')
ORDER BY context.inherited, intent.id DESC, asset.id`, taskID, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IntentAsset{}
	for rows.Next() {
		var asset IntentAsset
		var sourceNodeID sql.NullInt64
		if err := rows.Scan(&asset.IntentID, &asset.AssetID, &asset.Type, &asset.Label,
			&asset.Source, &asset.SourceSummary, &sourceNodeID, &asset.SourceTaskID, &asset.Inherited); err != nil {
			return nil, err
		}
		if sourceNodeID.Valid {
			id := sourceNodeID.Int64
			asset.SourceNodeID = &id
		}
		out = append(out, asset)
	}
	return out, rows.Err()
}
