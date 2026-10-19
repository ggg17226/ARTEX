package db

// =====================================================================
// 任务级联闭包（P08-02 Task 2.3，spec §3.2.2 闭环 F03）
//
// 三条硬性口径：
//  1. **全物理副作用**：闭包不止"本任务拥有的子行"，还必须枚举 executor 即将触碰
//     的**跨根弱关联物理行**（其他 exploration 的 activity.id、其他任务的
//     task_asset_links.row_id / exploration_anchors.row_id、引用共享资产的其他存活
//     task.id）。它们全部进锁计划，executor 才有"已锁物理 PK 集合"可以收敛。
//  2. **物理 PK 定位**：所有集合都是各表 Catalog 的真实物理 PK（id / row_id / ordinal）。
//     `side_question_sessions` 这类业务唯一键必须先翻译成物理 `row_id`，
//     executor 绝不按 `session_key` 这类业务列做 DELETE。
//  3. **同一套定位 SQL 两种读法**：快照阶段普通读（Fresh Locator 的非加锁快照），
//     持锁后的成员复核显式 `FOR UPDATE` 当前读 —— TiDB SI 下普通 SELECT 读的是本
//     事务旧快照，发现不了别的事务刚提交的新成员。
// =====================================================================

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/Autumn-27/artex/db/lockorder"
)

// TaskClosureOptions 是一次闭包计算的删除选项。零值 = 只解绑，不物理删除。
type TaskClosureOptions struct {
	// DeleteAssets 物理删除"仅被本任务引用"的资产；false 时一律只 detach。
	DeleteAssets bool
	// DeleteFindings 物理删除本任务闭包内的 findings；false 时只置空弱引用列。
	DeleteFindings bool
	// DeleteLLMRecords 物理删除本任务的 llm_records（按枚举出的物理 id 分片删除）。
	DeleteLLMRecords bool
}

// AssetRefRow 是一条"指向候选资产的其他实体物理行"（row_id + 它引用的 assets.id）。
// 保留 asset 维度是 delete/detach 裁剪的唯一依据：资产物理删除时这些行才跟着删除，
// 资产只是 detach 时它们必须**原样存活**。
type AssetRefRow struct {
	RowID   uint64
	AssetID uint64
}

// TaskCascadeClosure 记录待删除 Task 及其全部关联子实体 / 跨根物理副作用的闭包清单。
type TaskCascadeClosure struct {
	TaskID        uint64
	ExplorationID uint64
	Options       TaskClosureOptions

	// ── 本任务拥有的子实体（物理删除，全部为物理 PK）────────────────────────
	ArchiveIDs          []uint64 // task_archives.id
	RelationIDs         []uint64 // task_relations.row_id
	AssetLinkIDs        []uint64 // task_asset_links.row_id (task_id = ?)
	LLMProfileLinkIDs   []uint64 // task_llm_profiles.row_id
	ScopeIDs            []uint64 // task_scope.id
	InterceptRuleIDs    []uint64 // task_intercept_rules.id
	ConstraintIDs       []uint64 // task_constraints.id (exploration_id = ?)
	NodeIDs             []uint64 // exploration_nodes.id
	EdgeIDs             []uint64 // exploration_edges.row_id
	AnchorIDs           []uint64 // exploration_anchors.row_id (node_id IN nodes)
	ActivityIDs         []uint64 // activity.id (exploration_id = ?)
	MainSessionIDs      []uint64 // main_sessions.row_id
	SideSessionRowIDs   []uint64 // side_question_sessions.row_id（物理 PK）
	SideSessionKeys     []string // side_question_sessions.session_key（派生 request 用）
	SideRequestOrdinals []uint64 // side_question_requests.ordinal
	FindingIDs          []uint64 // findings.id（task_id 或 node_id 命中本闭包）
	LLMRecordIDs        []uint64 // llm_records.id（仅 DeleteLLMRecords 时枚举）

	// ── 置空集合（SET NULL，按物理 PK 限定）────────────────────────────────
	NullifyFindingTaskIDs []uint64 // findings.id WHERE task_id = ?
	NullifyFindingNodeIDs []uint64 // findings.id WHERE node_id IN (nodes)

	// ── 跨根物理副作用（闭环 F03：必须进锁计划）────────────────────────────
	CrossRootActivityIDs   []uint64      // 其他 exploration 的 activity.id（置空 node_id）
	CrossRootLinkIDs       []uint64      // 其他任务的 task_asset_links.row_id（置空 source_node_id）
	ForeignAssetLinkRows   []AssetRefRow // 其他任务指向候选资产的 link 行（纳锁；资产删除时才删）
	ForeignAssetAnchorRows []AssetRefRow // 其他节点指向候选资产的 anchor 行（纳锁；同上）
	ForeignAssetLinkIDs    []uint64      // 仲裁后：需要物理删除的跨根 link row_id
	ForeignAssetAnchorIDs  []uint64      // 仲裁后：需要物理删除的跨根 anchor row_id
	RefTaskIDs             []uint64      // 存活且引用候选资产的其他 task.id（纳锁 + 裁决）

	// ── 资产候选与持锁仲裁结果 ─────────────────────────────────────────────
	CandidateAssetIDs []uint64          // 本任务引用到的全部 assets.id
	AssetIDsToDelete  []uint64          // 仅本任务引用 → 物理删除
	AssetIDsToDetach  []uint64          // 多任务共享 / 未开删除选项 → 只剥离 task_id
	AssetDetachJSON   map[uint64]string // detach 后写回的 assets.task_ids JSON 载荷

	// 持锁仲裁输入（外部只读语义不使用；collectTaskClosure / arbitrate 专用）
	assetOtherTasks map[uint64][]uint64 // assets.id → 存活的其他引用任务 id
	assetTaskIDsRaw map[uint64]string   // assets.id → 本次读到的 task_ids 原始 JSON
	jsonRefTaskIDs  []uint64            // assets.task_ids JSON 里登记的其他任务 id
}

// totalSessionCount 优先取物理 row_id 口径；仅填 session_key 的历史构造按 key 计数。
func (c *TaskCascadeClosure) totalSessionCount() int {
	if len(c.SideSessionRowIDs) > 0 {
		return len(c.SideSessionRowIDs)
	}
	return len(c.SideSessionKeys)
}

// TotalCascadeDeleteCount 计算本次闭包将物理删除的实体总条数。
func (c *TaskCascadeClosure) TotalCascadeDeleteCount() int {
	total := 2 // Task 与 Exploration
	total += len(c.ArchiveIDs)
	total += len(c.RelationIDs)
	total += len(c.AssetLinkIDs)
	total += len(c.LLMProfileLinkIDs)
	total += len(c.ScopeIDs)
	total += len(c.InterceptRuleIDs)
	total += len(c.ConstraintIDs)
	total += c.totalSessionCount()
	total += len(c.SideRequestOrdinals)
	total += len(c.EdgeIDs)
	total += len(c.AnchorIDs)
	total += len(c.ActivityIDs)
	total += len(c.MainSessionIDs)
	total += len(c.NodeIDs)
	total += len(c.FindingIDs)
	total += len(c.LLMRecordIDs)
	total += len(c.AssetIDsToDelete)
	total += len(c.ForeignAssetLinkIDs)
	total += len(c.ForeignAssetAnchorIDs)
	return total
}

// Validate 校验闭包自洽性。
func (c *TaskCascadeClosure) Validate() error {
	if c.TaskID == 0 {
		return errors.New("task closure: TaskID is 0")
	}
	if c.ExplorationID == 0 {
		return errors.New("task closure: ExplorationID is 0")
	}
	if !isAscendingUniqueUint64(c.AssetIDsToDelete) || !isAscendingUniqueUint64(c.AssetIDsToDetach) {
		return errors.New("task closure: asset arbitration sets must be ascending and deduplicated")
	}
	deleted := make(map[uint64]struct{}, len(c.AssetIDsToDelete))
	for _, id := range c.AssetIDsToDelete {
		deleted[id] = struct{}{}
	}
	for _, id := range c.AssetIDsToDetach {
		if _, dup := deleted[id]; dup {
			return fmt.Errorf("task closure: asset %d is both deleted and detached", id)
		}
	}
	for _, id := range c.AssetIDsToDelete {
		if len(c.assetOtherTasks[id]) > 0 {
			return fmt.Errorf("task closure: asset %d is shared by %d other tasks but marked deleted",
				id, len(c.assetOtherTasks[id]))
		}
	}
	return nil
}

// lockPlanSpecs 返回"表名 → 本闭包内被物理触碰或守卫的全部 PK"。
// 表名一律经 lockorder Catalog 解析出物理 PK 列；目录外表直接失败（fail closed）。
func (c *TaskCascadeClosure) lockPlanSpecs() []struct {
	table string
	ids   []uint64
} {
	tasks := append([]uint64{c.TaskID}, c.RefTaskIDs...)
	anchors := make([]uint64, 0, len(c.AnchorIDs)+len(c.ForeignAssetAnchorRows))
	anchors = append(anchors, c.AnchorIDs...)
	for _, row := range c.ForeignAssetAnchorRows {
		anchors = append(anchors, row.RowID)
	}
	links := make([]uint64, 0, len(c.AssetLinkIDs)+len(c.CrossRootLinkIDs)+len(c.ForeignAssetLinkRows))
	links = append(links, c.AssetLinkIDs...)
	links = append(links, c.CrossRootLinkIDs...)
	for _, row := range c.ForeignAssetLinkRows {
		links = append(links, row.RowID)
	}
	return []struct {
		table string
		ids   []uint64
	}{
		{"tasks", tasks},
		{"explorations", []uint64{c.ExplorationID}},
		{"exploration_nodes", c.NodeIDs},
		{"assets", c.CandidateAssetIDs},
		{"findings", c.FindingIDs},
		{"activity", append(append([]uint64{}, c.ActivityIDs...), c.CrossRootActivityIDs...)},
		{"exploration_anchors", anchors},
		{"exploration_edges", c.EdgeIDs},
		{"main_sessions", c.MainSessionIDs},
		{"side_question_sessions", c.SideSessionRowIDs},
		{"side_question_requests", c.SideRequestOrdinals},
		{"task_archives", c.ArchiveIDs},
		{"task_asset_links", links},
		{"task_constraints", c.ConstraintIDs},
		{"task_intercept_rules", c.InterceptRuleIDs},
		{"task_llm_profiles", c.LLMProfileLinkIDs},
		{"task_relations", c.RelationIDs},
		{"task_scope", c.ScopeIDs},
	}
}

// LockPlan 把闭包翻译成 `(TableOrder, PK 升序)` 全局全序的悲观锁计划（已去重）。
func (c *TaskCascadeClosure) LockPlan() ([]lockorder.EntityKey, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	specs := c.lockPlanSpecs()
	keys := make([]lockorder.EntityKey, 0, c.TotalCascadeDeleteCount()+len(c.AssetIDsToDetach))
	for _, spec := range specs {
		meta, err := lockorder.LookupTableName(spec.table)
		if err != nil {
			return nil, fmt.Errorf("task closure lock plan: %w", err)
		}
		for _, id := range dedupeSortedUint64(spec.ids) {
			if id == 0 {
				return nil, fmt.Errorf("task closure lock plan: table %s has zero PK", spec.table)
			}
			keys = append(keys, lockorder.EntityKey{Table: meta.Order, PK: id})
		}
	}
	return lockorder.SortEntityKeys(keys), nil
}

// MembershipKeys 是成员漂移复核用的成员集合；与 LockPlan **同源同口径**：
// 锁的就是要复核的，复核的就是要锁的。
func (c *TaskCascadeClosure) MembershipKeys() ([]lockorder.EntityKey, error) {
	return c.LockPlan()
}

// TotalLockEntityCount 锁计划实体总数（含只纳锁不修改的跨根守卫行）。
func (c *TaskCascadeClosure) TotalLockEntityCount() int {
	plan, err := c.LockPlan()
	if err != nil {
		return 0
	}
	return len(plan)
}

// ComputeTaskClosure 以**非加锁快照读**枚举 Task 的完整关联闭包（保持历史签名）。
func ComputeTaskClosure(ctx context.Context, tx DBTX, taskID uint64) (*TaskCascadeClosure, error) {
	return ComputeTaskClosureWithOptions(ctx, tx, taskID, TaskClosureOptions{})
}

// ComputeTaskClosureWithOptions 按删除选项计算闭包（快照读；调用方随后加锁 + 复核）。
func ComputeTaskClosureWithOptions(
	ctx context.Context, tx DBTX, taskID uint64, opts TaskClosureOptions,
) (*TaskCascadeClosure, error) {
	return collectTaskClosure(ctx, tx, taskID, opts, false)
}

// ComputeLockedTaskClosure 持锁后用**当前读**重算闭包并完成仲裁，产出可直接交给
// executor 的最终闭包。资产 delete/detach 的判定必须在持锁之后做，否则"共享"结论
// 可能来自旧快照，会误删仍被其他任务引用的资产。
func ComputeLockedTaskClosure(
	ctx context.Context, tx DBTX, taskID uint64, opts TaskClosureOptions,
) (*TaskCascadeClosure, error) {
	closure, err := collectTaskClosure(ctx, tx, taskID, opts, true)
	if err != nil {
		return nil, err
	}
	if err := closure.arbitrate(); err != nil {
		return nil, err
	}
	return closure, nil
}

// RelocateTaskClosureMembers 是持锁后的成员重定位（lockorder.ReLocator 契约：
// **只发现，绝不补锁**）。定位查询全部显式 `FOR UPDATE` 走当前读；发现差异只由
// CompareMembership 报错，外层 TxRunner 整体回滚 + 指数退避重放。
func RelocateTaskClosureMembers(
	ctx context.Context, tx DBTX, taskID uint64, opts TaskClosureOptions,
) ([]lockorder.EntityKey, error) {
	closure, err := collectTaskClosure(ctx, tx, taskID, opts, true)
	if err != nil {
		return nil, err
	}
	return closure.MembershipKeys()
}

// arbitrate 在**持锁之后**完成资产 delete/detach 划分与跨根删除子集裁剪。
func (c *TaskCascadeClosure) arbitrate() error {
	toDelete := make([]uint64, 0, len(c.CandidateAssetIDs))
	toDetach := make([]uint64, 0, len(c.CandidateAssetIDs))
	detachJSON := make(map[uint64]string, len(c.CandidateAssetIDs))

	for _, assetID := range c.CandidateAssetIDs {
		shared := len(c.assetOtherTasks[assetID]) > 0
		if shared || !c.Options.DeleteAssets {
			toDetach = append(toDetach, assetID)
			kept, err := c.taskIDsExceptTask(assetID)
			if err != nil {
				return err
			}
			detachJSON[assetID] = marshalTaskIDs(kept)
			continue
		}
		toDelete = append(toDelete, assetID)
	}
	c.AssetIDsToDelete = dedupeSortedUint64(toDelete)
	c.AssetIDsToDetach = dedupeSortedUint64(toDetach)
	c.AssetDetachJSON = detachJSON

	// 跨根 link/anchor 只在资产被**物理删除**时才跟着物理删除；共享资产的跨根行
	// 必须原样存活（开放范围 DML 越界清理即判红）。
	c.ForeignAssetLinkIDs = purgeRowsForDeletedAssets(c.ForeignAssetLinkRows, c.AssetIDsToDelete)
	c.ForeignAssetAnchorIDs = purgeRowsForDeletedAssets(c.ForeignAssetAnchorRows, c.AssetIDsToDelete)

	if c.Options.DeleteFindings {
		// findings 整行删除，不再置空。
		c.NullifyFindingTaskIDs = nil
		c.NullifyFindingNodeIDs = nil
	}
	return c.Validate()
}

// taskIDsExceptTask 读回资产当前的 task_ids 并剥离本任务 id（双写维护的另一半）。
func (c *TaskCascadeClosure) taskIDsExceptTask(assetID uint64) ([]int64, error) {
	raw, ok := c.assetTaskIDsRaw[assetID]
	if !ok {
		// 资产行已被并发删除：detach 退化成无操作，写回空数组反而会造出假 JSON。
		return nil, nil
	}
	ids, err := parseTaskIDArray(raw)
	if err != nil {
		return nil, fmt.Errorf("task closure: asset %d task_ids: %w", assetID, err)
	}
	kept := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id == int64(c.TaskID) {
			continue
		}
		kept = append(kept, id)
	}
	return kept, nil
}

func purgeRowsForDeletedAssets(rows []AssetRefRow, deletedAssets []uint64) []uint64 {
	if len(rows) == 0 || len(deletedAssets) == 0 {
		return nil
	}
	victims := make(map[uint64]struct{}, len(deletedAssets))
	for _, id := range deletedAssets {
		victims[id] = struct{}{}
	}
	out := make([]uint64, 0, len(rows))
	for _, row := range rows {
		if _, hit := victims[row.AssetID]; hit {
			out = append(out, row.RowID)
		}
	}
	return dedupeSortedUint64(out)
}

// ---------------------------------------------------------------------------
// 定位查询实现
// ---------------------------------------------------------------------------

// closureReader 把"同一套定位 SQL 既可做快照读，也可做持锁当前读"收敛到一个出口。
type closureReader struct {
	ctx     context.Context
	tx      DBTX
	locking bool
}

// sql 在锁读模式下追加 FOR UPDATE —— TiDB 当前读的唯一可靠开关。
func (r *closureReader) sql(query string) string {
	if r.locking {
		return query + " FOR UPDATE"
	}
	return query
}

func (r *closureReader) ids(dest *[]uint64, query string, args ...any) error {
	rows, err := r.tx.QueryContext(r.ctx, r.sql(query), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		*dest = append(*dest, id)
	}
	return rows.Err()
}

// rows2 读取两列（均为 uint64）；join 查询一律**不加** FOR UPDATE（避免顺着 join
// 把目录外的其他任务整片行也锁住），其"存活"语义由后续单表当前读复核。
func (r *closureReader) rows2(query string, args ...any) ([2][]uint64, error) {
	rows, err := r.tx.QueryContext(r.ctx, query, args...)
	if err != nil {
		return [2][]uint64{}, err
	}
	defer rows.Close()
	var out [2][]uint64
	for rows.Next() {
		var a, b uint64
		if err := rows.Scan(&a, &b); err != nil {
			return [2][]uint64{}, err
		}
		out[0] = append(out[0], a)
		out[1] = append(out[1], b)
	}
	return out, rows.Err()
}

func collectTaskClosure(
	ctx context.Context, tx DBTX, taskID uint64, opts TaskClosureOptions, locking bool,
) (*TaskCascadeClosure, error) {
	closure := &TaskCascadeClosure{
		TaskID:          taskID,
		Options:         opts,
		assetOtherTasks: map[uint64][]uint64{},
		assetTaskIDsRaw: map[uint64]string{},
	}
	r := &closureReader{ctx: ctx, tx: tx, locking: locking}

	// 1. 任务绑定的 exploration_id（tasks 行本身由锁计划负责，这里不做锁定读）
	err := tx.QueryRowContext(ctx, `SELECT exploration_id FROM tasks WHERE id = ? LIMIT 1`, taskID).
		Scan(&closure.ExplorationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: task id=%d not found", ErrForeignKeyViolation, taskID)
		}
		return nil, err
	}

	// 2. 本任务直接拥有的子实体（全部按物理 PK 枚举）
	if err := r.ids(&closure.ArchiveIDs, `SELECT id FROM task_archives WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.RelationIDs,
		`SELECT row_id FROM task_relations WHERE task_id = ? OR source_task_id = ?`, taskID, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.AssetLinkIDs, `SELECT row_id FROM task_asset_links WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.LLMProfileLinkIDs, `SELECT row_id FROM task_llm_profiles WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.ScopeIDs, `SELECT id FROM task_scope WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.InterceptRuleIDs, `SELECT id FROM task_intercept_rules WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.ConstraintIDs,
		`SELECT id FROM task_constraints WHERE exploration_id = ?`, closure.ExplorationID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.NodeIDs,
		`SELECT id FROM exploration_nodes WHERE exploration_id = ?`, closure.ExplorationID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.EdgeIDs,
		`SELECT row_id FROM exploration_edges WHERE exploration_id = ?`, closure.ExplorationID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.MainSessionIDs,
		`SELECT row_id FROM main_sessions WHERE exploration_id = ?`, closure.ExplorationID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.ActivityIDs,
		`SELECT id FROM activity WHERE exploration_id = ?`, closure.ExplorationID); err != nil {
		return nil, err
	}
	if err := r.ids(&closure.NullifyFindingTaskIDs,
		`SELECT id FROM findings WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}

	nodeSet := dedupeSortedUint64(closure.NodeIDs)
	closure.NodeIDs = nodeSet
	if len(nodeSet) > 0 {
		nodePh := closurePlaceholders(len(nodeSet))
		nodeArgs := uint64Args(nodeSet)

		if err := r.ids(&closure.AnchorIDs,
			`SELECT row_id FROM exploration_anchors WHERE node_id IN (`+nodePh+`)`, nodeArgs...); err != nil {
			return nil, err
		}
		// 跨根弱关联（F03）：其他 exploration 的 activity 行 node_id 指向本闭包节点
		if err := r.ids(&closure.CrossRootActivityIDs,
			`SELECT id FROM activity WHERE node_id IN (`+nodePh+`)`+
				` AND (exploration_id IS NULL OR exploration_id <> ?)`,
			append(append([]any{}, nodeArgs...), closure.ExplorationID)...); err != nil {
			return nil, err
		}
		// 跨根弱关联（F03）：其他任务的 link 行 source_node_id 指向本闭包节点
		if err := r.ids(&closure.CrossRootLinkIDs,
			`SELECT row_id FROM task_asset_links WHERE source_node_id IN (`+nodePh+`)`+
				` AND (task_id IS NULL OR task_id <> ?)`,
			append(append([]any{}, nodeArgs...), taskID)...); err != nil {
			return nil, err
		}
		if err := r.ids(&closure.NullifyFindingNodeIDs,
			`SELECT id FROM findings WHERE node_id IN (`+nodePh+`)`, nodeArgs...); err != nil {
			return nil, err
		}
	}
	closure.FindingIDs = joinUint64(closure.NullifyFindingTaskIDs, closure.NullifyFindingNodeIDs)

	// 3. side_question_sessions：业务唯一键 session_key → 物理 row_id
	{
		rows, err := tx.QueryContext(ctx, r.sql(
			`SELECT row_id, session_key FROM side_question_sessions WHERE task_id = ? OR exploration_id = ?`),
			taskID, closure.ExplorationID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var rowID uint64
			var key string
			if err := rows.Scan(&rowID, &key); err != nil {
				rows.Close()
				return nil, err
			}
			closure.SideSessionRowIDs = append(closure.SideSessionRowIDs, rowID)
			closure.SideSessionKeys = append(closure.SideSessionKeys, key)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	if keys := dedupeStrings(closure.SideSessionKeys); len(keys) > 0 {
		if err := r.ids(&closure.SideRequestOrdinals,
			`SELECT ordinal FROM side_question_requests WHERE session_key IN (`+closurePlaceholders(len(keys))+`)`,
			stringArgs(keys)...); err != nil {
			return nil, err
		}
		closure.SideSessionKeys = keys
	}

	// 4. 资产候选（link 表 + 历史 anchor 边表 + assets.task_ids JSON 三路并集）
	if err := collectCandidateAssets(r, closure, taskID); err != nil {
		return nil, err
	}

	// 5. 跨根物理副作用：指向候选资产的其他任务 link / 其他节点 anchor
	if err := collectForeignAssetReferences(r, closure, taskID); err != nil {
		return nil, err
	}

	// 6. llm_records（可选）：按枚举出的物理 id 分片删除，不写开放范围 DML。
	//    刻意**不进**成员漂移复核集合：llm_records 是持续追加的记账表，把它纳入成员
	//    集合会让运行中任务的重试永远漂移；它只受"枚举 → PK 分片"的写集收敛约束。
	if opts.DeleteLLMRecords {
		if err := r.ids(&closure.LLMRecordIDs,
			`SELECT id FROM llm_records WHERE COALESCE(task_id,'') = ?`, fmt.Sprintf("%d", taskID)); err != nil {
			return nil, err
		}
	}

	closure.normalize()
	if err := closure.Validate(); err != nil {
		return nil, err
	}
	return closure, nil
}

// collectCandidateAssets 收集本任务引用到的全部资产主键，并读回其当前 task_ids 载荷
// （锁读模式下即当前读，供持锁仲裁使用）。
func collectCandidateAssets(r *closureReader, closure *TaskCascadeClosure, taskID uint64) error {
	var candidates []uint64
	if err := r.ids(&candidates,
		`SELECT asset_id FROM task_asset_links WHERE task_id = ? AND asset_id IS NOT NULL`, taskID); err != nil {
		return err
	}
	if len(closure.NodeIDs) > 0 {
		nodeArgs := uint64Args(closure.NodeIDs)
		if err := r.ids(&candidates,
			`SELECT asset_id FROM exploration_anchors WHERE asset_id IS NOT NULL AND node_id IN (`+
				closurePlaceholders(len(closure.NodeIDs))+`)`, nodeArgs...); err != nil {
			return err
		}
	}
	if err := r.ids(&candidates,
		`SELECT id FROM assets WHERE `+assetJSONContains("COALESCE(task_ids, '[]')", "?"), taskID); err != nil {
		return err
	}
	candidates = dedupeSortedUint64(candidates)
	if len(candidates) == 0 {
		return nil
	}
	rows, err := r.tx.QueryContext(r.ctx, r.sql(
		`SELECT id, COALESCE(task_ids, '[]') FROM assets WHERE id IN (`+closurePlaceholders(len(candidates))+`)`),
		uint64Args(candidates)...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id uint64
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		closure.assetTaskIDsRaw[id] = raw
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	// JSON 口径的其他引用任务 id（资产行已在本轮当前读里到手，无需再查）
	var jsonRefs []uint64
	for assetID, raw := range closure.assetTaskIDsRaw {
		ids, err := parseTaskIDArray(raw)
		if err != nil {
			return fmt.Errorf("task closure: asset %d task_ids: %w", assetID, err)
		}
		for _, tid := range ids {
			if tid > 0 && uint64(tid) != taskID {
				jsonRefs = append(jsonRefs, uint64(tid))
			}
		}
	}
	closure.jsonRefTaskIDs = dedupeSortedUint64(jsonRefs)
	closure.CandidateAssetIDs = candidates
	return nil
}

// collectForeignAssetReferences 收集"指向候选资产的其他任务物理行"，并算出引用这些
// 资产的**其他存活任务**（delete/detach 的裁决依据，同时是锁计划成员）。
func collectForeignAssetReferences(r *closureReader, closure *TaskCascadeClosure, taskID uint64) error {
	if len(closure.CandidateAssetIDs) == 0 {
		return nil
	}
	assetArgs := uint64Args(closure.CandidateAssetIDs)
	ph := closurePlaceholders(len(closure.CandidateAssetIDs))

	// 共享资产的跨根 link 行同样必须纳锁：并发 detach 同一资产的两个任务不能交错改写
	// assets.task_ids，否则"剥离自己那一位"会互相覆盖。
	linkPairs, err := r.rows2(
		`SELECT row_id, asset_id FROM task_asset_links WHERE asset_id IN (`+ph+`)`+
			` AND (task_id IS NULL OR task_id <> ?)`,
		append(append([]any{}, assetArgs...), taskID)...)
	if err != nil {
		return err
	}
	for i, rowID := range linkPairs[0] {
		closure.ForeignAssetLinkRows = append(closure.ForeignAssetLinkRows,
			AssetRefRow{RowID: rowID, AssetID: linkPairs[1][i]})
	}

	anchorQuery := `SELECT row_id, asset_id FROM exploration_anchors WHERE asset_id IN (` + ph + `) AND asset_id IS NOT NULL`
	anchorArgs := append([]any{}, assetArgs...)
	if len(closure.AnchorIDs) > 0 {
		anchorQuery += ` AND row_id NOT IN (` + closurePlaceholders(len(closure.AnchorIDs)) + `)`
		anchorArgs = append(anchorArgs, uint64Args(closure.AnchorIDs)...)
	}
	anchorPairs, err := r.rows2(anchorQuery, anchorArgs...)
	if err != nil {
		return err
	}
	for i, rowID := range anchorPairs[0] {
		closure.ForeignAssetAnchorRows = append(closure.ForeignAssetAnchorRows,
			AssetRefRow{RowID: rowID, AssetID: anchorPairs[1][i]})
	}

	// anchor → node → exploration → task 反查（join 不加锁，存活由下面的单表当前读裁定）
	taskPairs, err := r.rows2(
		`SELECT ea.asset_id, t.id
FROM exploration_anchors ea
JOIN exploration_nodes n ON n.id = ea.node_id
JOIN tasks t ON t.exploration_id = n.exploration_id
WHERE ea.asset_id IN (`+ph+`) AND t.id <> ? AND t.deleted_at IS NULL`,
		append(append([]any{}, assetArgs...), taskID)...)
	if err != nil {
		return err
	}
	anchorRefTasks := dedupeSortedUint64(taskPairs[1])

	// 存活裁定：JSON 口径登记的任务可能已被并发删除；这里在**已持锁**的模式下就是
	// 当前读（tasks 行已由锁计划持有），快照模式下只是粗估。
	candidateRefs := dedupeSortedUint64(append(append([]uint64{}, closure.jsonRefTaskIDs...), anchorRefTasks...))
	alive, err := filterAliveTasks(r, candidateRefs)
	if err != nil {
		return err
	}
	closure.RefTaskIDs = alive

	// 重建 asset → 存活其他引用任务 映射（JSON 口径 + anchor 口径）
	aliveSet := make(map[uint64]struct{}, len(alive))
	for _, id := range alive {
		aliveSet[id] = struct{}{}
	}
	closure.assetOtherTasks = map[uint64][]uint64{}
	for assetID, raw := range closure.assetTaskIDsRaw {
		ids, err := parseTaskIDArray(raw)
		if err != nil {
			return fmt.Errorf("task closure: asset %d task_ids: %w", assetID, err)
		}
		for _, tid := range ids {
			if tid <= 0 || uint64(tid) == taskID {
				continue
			}
			if _, ok := aliveSet[uint64(tid)]; !ok {
				continue
			}
			closure.assetOtherTasks[assetID] = append(closure.assetOtherTasks[assetID], uint64(tid))
		}
	}
	for i, assetID := range taskPairs[0] {
		taskRef := taskPairs[1][i]
		if taskRef == taskID {
			continue
		}
		if _, ok := aliveSet[taskRef]; !ok {
			continue
		}
		closure.assetOtherTasks[assetID] = append(closure.assetOtherTasks[assetID], taskRef)
	}
	return nil
}

// filterAliveTasks 判定一批 task.id 是否仍存活；锁模式下即当前读（行已由锁计划持有）。
func filterAliveTasks(r *closureReader, ids []uint64) ([]uint64, error) {
	ids = dedupeSortedUint64(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	var alive []uint64
	if err := r.ids(&alive,
		`SELECT id FROM tasks WHERE deleted_at IS NULL AND id IN (`+closurePlaceholders(len(ids))+`)`,
		uint64Args(ids)...); err != nil {
		return nil, err
	}
	return dedupeSortedUint64(alive), nil
}

// normalize 把所有 ID 集合规范化为升序去重，保证锁计划与漂移比对可重现。
func (c *TaskCascadeClosure) normalize() {
	c.ArchiveIDs = dedupeSortedUint64(c.ArchiveIDs)
	c.RelationIDs = dedupeSortedUint64(c.RelationIDs)
	c.AssetLinkIDs = dedupeSortedUint64(c.AssetLinkIDs)
	c.LLMProfileLinkIDs = dedupeSortedUint64(c.LLMProfileLinkIDs)
	c.ScopeIDs = dedupeSortedUint64(c.ScopeIDs)
	c.InterceptRuleIDs = dedupeSortedUint64(c.InterceptRuleIDs)
	c.ConstraintIDs = dedupeSortedUint64(c.ConstraintIDs)
	c.NodeIDs = dedupeSortedUint64(c.NodeIDs)
	c.EdgeIDs = dedupeSortedUint64(c.EdgeIDs)
	c.AnchorIDs = dedupeSortedUint64(c.AnchorIDs)
	c.ActivityIDs = dedupeSortedUint64(c.ActivityIDs)
	c.MainSessionIDs = dedupeSortedUint64(c.MainSessionIDs)
	c.SideSessionRowIDs = dedupeSortedUint64(c.SideSessionRowIDs)
	c.SideRequestOrdinals = dedupeSortedUint64(c.SideRequestOrdinals)
	c.FindingIDs = dedupeSortedUint64(c.FindingIDs)
	c.NullifyFindingTaskIDs = dedupeSortedUint64(c.NullifyFindingTaskIDs)
	c.NullifyFindingNodeIDs = dedupeSortedUint64(c.NullifyFindingNodeIDs)
	c.CrossRootActivityIDs = dedupeSortedUint64(c.CrossRootActivityIDs)
	c.CrossRootLinkIDs = dedupeSortedUint64(c.CrossRootLinkIDs)
	c.RefTaskIDs = dedupeSortedUint64(c.RefTaskIDs)
	c.CandidateAssetIDs = dedupeSortedUint64(c.CandidateAssetIDs)
	c.LLMRecordIDs = dedupeSortedUint64(c.LLMRecordIDs)
	c.SideSessionKeys = dedupeStrings(c.SideSessionKeys)
	sort.Slice(c.ForeignAssetLinkRows, func(i, j int) bool {
		return c.ForeignAssetLinkRows[i].RowID < c.ForeignAssetLinkRows[j].RowID
	})
	sort.Slice(c.ForeignAssetAnchorRows, func(i, j int) bool {
		return c.ForeignAssetAnchorRows[i].RowID < c.ForeignAssetAnchorRows[j].RowID
	})
}

// ---------------------------------------------------------------------------
// 通用小工具
// ---------------------------------------------------------------------------

func joinUint64(lists ...[]uint64) []uint64 {
	var out []uint64
	for _, l := range lists {
		out = append(out, l...)
	}
	return out
}

func dedupeSortedUint64(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	cloned := make([]uint64, len(ids))
	copy(cloned, ids)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	out := make([]uint64, 0, len(cloned))
	for _, id := range cloned {
		if len(out) > 0 && out[len(out)-1] == id {
			continue
		}
		out = append(out, id)
	}
	return out
}

func isAscendingUniqueUint64(ids []uint64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			return false
		}
	}
	return true
}

func dedupeStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, dup := seen[item]; dup {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func queryUint64s(ctx context.Context, tx DBTX, dest *[]uint64, query string, args ...any) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		*dest = append(*dest, id)
	}
	return rows.Err()
}

func queryStrings(ctx context.Context, tx DBTX, dest *[]string, query string, args ...any) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		*dest = append(*dest, s)
	}
	return rows.Err()
}

func closurePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n*2-1)
	for i := 0; i < n; i++ {
		b[i*2] = '?'
		if i < n-1 {
			b[i*2+1] = ','
		}
	}
	return string(b)
}

func uint64Args(ids []uint64) []any {
	args := make([]any, len(ids))
	for i, v := range ids {
		args[i] = v
	}
	return args
}

func stringArgs(strs []string) []any {
	args := make([]any, len(strs))
	for i, v := range strs {
		args[i] = v
	}
	return args
}
