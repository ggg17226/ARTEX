package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DBFinding is a row in the standalone findings table. It persists across task
// deletion unless the caller explicitly requests related finding cleanup.
type DBFinding struct {
	TrafficCount          int
	EvidenceVersion       int64
	ReportEvidenceVersion int64
	TrafficBindings       []FindingTrafficBinding // populated only for export

	ID              int64
	TaskID          *int64
	NodeID          *int64
	VulnClass       string
	Name            string // 漏洞名称(可读标题);为空时前端回退展示 VulnClass
	Severity        string
	Summary         string
	Evidence        string
	Worker          string
	AssetIDs        []int64
	Status          string
	Report          string // 详细报告(Markdown);仅 GetFinding 填充,列表查询不带
	CreatedAt       time.Time
	TaskDescription string // populated via LEFT JOIN on tasks
}

// Finding triage states (findings.status).
const (
	FindingPending       = "pending"        // 待处理
	FindingInProgress    = "in_progress"    // 处理中
	FindingConfirmed     = "confirmed"      // 已确认(真实漏洞,未修复)
	FindingResolved      = "resolved"       // 已处理
	FindingFixed         = "fixed"          // 已修复
	FindingFalsePositive = "false_positive" // 误报
	FindingIgnored       = "ignored"        // 忽略
	FindingDuplicate     = "duplicate"      // 重复
	FindingRiskAccepted  = "risk_accepted"  // 风险接受
)

// ValidFindingStatus reports whether s is a known triage state.
func ValidFindingStatus(s string) bool {
	switch s {
	case FindingPending, FindingInProgress, FindingConfirmed, FindingResolved, FindingFixed,
		FindingFalsePositive, FindingIgnored, FindingDuplicate, FindingRiskAccepted:
		return true
	}
	return false
}

// Finding severity levels (findings.severity).
const (
	SeverityCritical = "critical" // 严重
	SeverityHigh     = "high"     // 高
	SeverityMedium   = "medium"   // 中
	SeverityLow      = "low"      // 低
)

// ValidSeverity reports whether s is a known severity level.
func ValidSeverity(s string) bool {
	switch s {
	case SeverityCritical, SeverityHigh, SeverityMedium, SeverityLow:
		return true
	}
	return false
}

// questionPlaceholders 生成 n 个 `?` 占位符并以 ", " 连接（TiDB 没有 PG 的 $n
// 下标占位符）：任何长度随输入变化的 IN (…) / JSON 谓词列表都必须从这里拼，
// 值本身始终走绑定参数，绝不做字符串内插。
func questionPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// jsonLiteral 把值编成 JSON 文档字面量，供 `CAST(? AS JSON)` 写入 JSON 列。
func jsonLiteral(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(raw)
}

// AddFinding inserts a finding into the standalone findings table. taskID and
// nodeID may be 0 (stored as NULL). name may be "" (frontend falls back to
// vulnclass). Returns the new finding id.
func (d *DB) AddFinding(taskID, nodeID int64, vulnclass, name, severity, summary, evidence, worker string, assetIDs []int64) (int64, error) {
	aidsJSON, _ := json.Marshal(assetIDs)
	if assetIDs == nil {
		aidsJSON = []byte("[]")
	}
	var tid, nid *int64
	if taskID > 0 {
		tid = &taskID
	}
	if nodeID > 0 {
		nid = &nodeID
	}
	// TiDB 没有 INSERT … RETURNING：自增主键一律经 LastInsertId() 取回。
	res, err := d.Exec(`INSERT INTO findings (task_id, node_id, vulnclass, name, severity, summary, evidence, worker, asset_ids)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tid, nid, vulnclass, name, severity, summary, evidence, worker, string(aidsJSON))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("AddFinding: LastInsertId after INSERT: %w", err)
	}
	if id < 1 {
		return 0, fmt.Errorf("AddFinding: auto-increment id %d out of range", id)
	}
	return id, nil
}

// findingSelectCols is the column list (with task_description join) every finding
// list query selects, so scanFinding stays in sync across callers.
const findingSelectCols = `f.id, f.task_id, f.node_id, f.vulnclass, COALESCE(f.name, ''), f.severity, f.summary,
	       f.evidence, f.worker, f.asset_ids, COALESCE(f.status, 'pending'), f.created_at,
	       COALESCE(t.description, '') AS task_description, f.evidence_version, f.report_evidence_version,
 (SELECT count(*) FROM finding_traffic_bindings b WHERE b.finding_id=f.id)`

// scanFindings materializes rows selected via findingSelectCols.
func scanFindings(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]*DBFinding, error) {
	var out []*DBFinding
	for rows.Next() {
		f := &DBFinding{}
		var aidsJSON string
		if err := rows.Scan(&f.ID, &f.TaskID, &f.NodeID, &f.VulnClass, &f.Name, &f.Severity,
			&f.Summary, &f.Evidence, &f.Worker, &aidsJSON, &f.Status, &f.CreatedAt, &f.TaskDescription, &f.EvidenceVersion, &f.ReportEvidenceVersion, &f.TrafficCount); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(aidsJSON), &f.AssetIDs)
		out = append(out, f)
	}
	return out, rows.Err()
}

// ListFindings returns all findings (newest first), joined with task description.
// Kept for the dashboard's summary; the paginated 发现 page uses ListFindingsPage.
func (d *DB) ListFindings(limit int) ([]*DBFinding, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := d.Query(`
		SELECT `+findingSelectCols+`
		FROM findings f
		LEFT JOIN tasks t ON f.task_id = t.id
		ORDER BY f.created_at DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFindings(rows)
}

// FindingFilter narrows a paginated findings query. Empty-string fields mean "no
// filter on that column". Sort is "severity" (severity desc, then newest) or
// anything else (newest first).
type FindingFilter struct {
	Severity  string // high | medium | low
	Status    string // pending | false_positive | ignored | resolved
	VulnClass string
	TaskID    string // 任务 id(字符串形式;空/非法 = 不按任务筛选)
	Query     string // 名称/类型/摘要/证据/报告正文的模糊检索关键词
	Sort      string // "severity" | "time"
	// AssetScope 是资产树的节点 key(a:<id> / c:<id> / r:<domain> / __none__),
	// 选中一个节点等于选中它的整棵子树。空 = 不按资产筛选。
	AssetScope string

	// 下面三个由 applyAssetScope 从 AssetScope 解析而来,调用方不用设置。
	assetIDs  []int64 // 子树里所有资产 id
	assetNone bool    // 只要「未关联资产」的发现
	assetMiss bool    // 选中的节点在当前筛选下不存在 → 结果恒空
}

// FindingUnassignedTask is the task filter sentinel for findings whose task is
// absent. That includes rows created without a task and rows retained after their
// originating task was deleted (the findings FK is ON DELETE SET NULL).
const FindingUnassignedTask = "__unassigned__"

// where builds the WHERE clause (shared by the page and count queries) plus its
// positional args. All values are parameterized; Query also escapes ILIKE
// wildcards so user input is always matched literally.
func (f FindingFilter) where() (string, []any) {
	var conds []string
	var args []any
	add := func(col, val string) {
		if val == "" {
			return
		}
		args = append(args, val)
		conds = append(conds, "f."+col+" = ?")
	}
	add("severity", f.Severity)
	add("status", f.Status)
	add("vulnclass", f.VulnClass)
	// task_id 是 bigint 列,按整数比较(不能走上面的文本 add);空/非法值忽略。
	if f.TaskID == FindingUnassignedTask {
		conds = append(conds, "(f.task_id IS NULL OR t.id IS NULL)")
	} else if tid, err := strconv.ParseInt(f.TaskID, 10, 64); err == nil && tid > 0 {
		args = append(args, tid)
		conds = append(conds, "f.task_id = ?")
	}
	// 资产筛选:asset_ids 是 JSON 数组列。TiDB 没有 jsonb 的 @> 算子,改用
	// JSON_CONTAINS(target, CAST(id AS JSON));它吃不到 GIN 索引,但资产树的 id
	// 集合来自同一次构树(有界),逐个 OR 的开销可控。
	switch {
	case f.assetMiss:
		conds = append(conds, "FALSE")
	case f.assetNone:
		// 「未关联资产」= asset_ids 为空,或者里面的 id 一个都不在 assets 表里
		// (资产已被删除)。两类都进资产树的未关联桶,这里必须同样收下,否则桶上
		// 的计数会大于点开后能查到的条数。
		conds = append(conds, `(
			COALESCE(JSON_LENGTH(f.asset_ids), 0) = 0
			OR NOT EXISTS (
				SELECT 1 FROM assets a
				WHERE JSON_CONTAINS(f.asset_ids, CAST(a.id AS JSON))
			)
		)`)
	case len(f.assetIDs) > 0:
		ors := make([]string, 0, len(f.assetIDs))
		for _, id := range f.assetIDs {
			ors = append(ors, `JSON_CONTAINS(COALESCE(f.asset_ids, '[]'), CAST(? AS JSON))`)
			args = append(args, id)
		}
		conds = append(conds, "("+strings.Join(ors, " OR ")+")")
	}
	if query := strings.TrimSpace(f.Query); query != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(query)
		// TiDB 的列默认排序规则是 *_ci(大小写不敏感),LIKE 即等价于 PG 的 ILIKE;
		// ESCAPE 里的反斜杠在 MySQL 方言下必须写两枚。
		like := `(
			COALESCE(f.name, '') LIKE ? ESCAPE '\\' OR
			f.vulnclass LIKE ? ESCAPE '\\' OR
			f.summary LIKE ? ESCAPE '\\' OR
			f.evidence LIKE ? ESCAPE '\\' OR
			COALESCE(f.report, '') LIKE ? ESCAPE '\\'
		)`
		pattern := "%" + escaped + "%"
		for range 5 {
			args = append(args, pattern)
		}
		conds = append(conds, like)
	}
	if len(conds) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// ListFindingsPage returns one page of findings matching the filter, plus the
// total count of matching rows (for the frontend pager). page is 1-based.
func (d *DB) ListFindingsPage(f FindingFilter, page, pageSize int) ([]*DBFinding, int, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	f, err := d.applyAssetScope(f)
	if err != nil {
		return nil, 0, err
	}
	where, args := f.where()

	var total int
	if err := d.QueryRow(`SELECT COUNT(*) FROM findings f LEFT JOIN tasks t ON f.task_id=t.id`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	// Avoid overflowing (page-1)*pageSize for an arbitrarily large page number.
	// Once the requested page is beyond the exact count, no data query is needed.
	if total == 0 || page > (total-1)/pageSize+1 {
		return []*DBFinding{}, total, nil
	}

	order := "f.created_at DESC, f.id DESC"
	if f.Sort == "severity" {
		// critical > high > medium > low > 其它, then newest first.
		order = `CASE f.severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC, f.created_at DESC, f.id DESC`
	}
	pageArgs := append(append([]any{}, args...), pageSize, (page-1)*pageSize)
	q := fmt.Sprintf(`
		SELECT %s
		FROM findings f
		LEFT JOIN tasks t ON f.task_id = t.id%s
		ORDER BY %s
		LIMIT ? OFFSET ?`, findingSelectCols, where, order)
	rows, err := d.Query(q, pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out, err := scanFindings(rows)
	return out, total, err
}

// FindingGroup is one task-level bucket in the global findings view. TaskID is
// nil for both findings that never had a task and findings retained after task
// deletion; those records intentionally share one "unassigned/deleted" bucket.
//
// P03（R13）:task_id 是 BIGINT,对外带引号 string;nil(未分配/任务已删)仍是 null,
// 无 omitempty 语义不变。count/critical… 是计数,保持 number。
type FindingGroup struct {
	TaskID          *int64    `json:"task_id,string"`
	TaskName        string    `json:"task_name"` // 可选任务名称;空=未命名
	TaskDescription string    `json:"task_description"`
	TaskStatus      string    `json:"task_status"`
	Count           int       `json:"count"`
	Critical        int       `json:"critical"`
	High            int       `json:"high"`
	Medium          int       `json:"medium"`
	Low             int       `json:"low"`
	LastFoundAt     time.Time `json:"last_found_at"`
}

// ListFindingGroups returns a page of task groups matching the same filters as
// ListFindingsPage. The group count and finding count are independent totals so
// clients can page groups without losing the exact export/selection count.
func (d *DB) ListFindingGroups(f FindingFilter, page, pageSize int) ([]FindingGroup, int, int, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	f, err := d.applyAssetScope(f)
	if err != nil {
		return nil, 0, 0, err
	}
	where, args := f.where()
	grouped := ` FROM findings f LEFT JOIN tasks t ON f.task_id=t.id` + where +
		` GROUP BY t.id, t.name, t.description, t.status, t.paused, t.queued`

	var groupTotal, findingTotal int
	countQuery := `SELECT COUNT(*), COALESCE(SUM(finding_count),0) FROM (` +
		`SELECT COUNT(*) AS finding_count` + grouped + `) grouped_findings`
	if err := d.QueryRow(countQuery, args...).Scan(&groupTotal, &findingTotal); err != nil {
		return nil, 0, 0, err
	}
	if groupTotal == 0 || page > (groupTotal-1)/pageSize+1 {
		return []FindingGroup{}, groupTotal, findingTotal, nil
	}

	// MySQL/TiDB 把 NULL 排在最小端:DESC 时天然落在最后,正是 PG NULLS LAST 的语义。
	order := "MAX(f.created_at) DESC, t.id DESC"
	if f.Sort == "severity" {
		order = `MAX(CASE f.severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END) DESC, MAX(f.created_at) DESC, t.id DESC`
	}
	pageArgs := append(append([]any{}, args...), pageSize, (page-1)*pageSize)
	query := fmt.Sprintf(`SELECT t.id, COALESCE(t.name,''), COALESCE(t.description,''), COALESCE(
		CASE
			WHEN t.status IN ('done','failed','timeout') THEN t.status
			WHEN t.queued THEN 'queued'
			WHEN t.paused THEN 'paused'
			ELSE t.status
		END, ''),
		COUNT(*),
		COUNT(CASE WHEN f.severity='critical' THEN 1 END),
		COUNT(CASE WHEN f.severity='high' THEN 1 END),
		COUNT(CASE WHEN f.severity='medium' THEN 1 END),
		COUNT(CASE WHEN f.severity='low' THEN 1 END),
		MAX(f.created_at)%s
		ORDER BY %s LIMIT ? OFFSET ?`, grouped, order)
	rows, err := d.Query(query, pageArgs...)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()
	groups := []FindingGroup{}
	for rows.Next() {
		var group FindingGroup
		var taskID sql.NullInt64
		if err := rows.Scan(&taskID, &group.TaskName, &group.TaskDescription, &group.TaskStatus, &group.Count,
			&group.Critical, &group.High, &group.Medium, &group.Low, &group.LastFoundAt); err != nil {
			return nil, 0, 0, err
		}
		if taskID.Valid {
			id := taskID.Int64
			group.TaskID = &id
		}
		groups = append(groups, group)
	}
	return groups, groupTotal, findingTotal, rows.Err()
}

// ErrFindingOriginUnavailable means a retained finding no longer has a live
// owning task and finding node from which a follow-up intent can be derived.
var ErrFindingOriginUnavailable = errors.New("finding origin is no longer available")

// AddFindingFollowUpIntent atomically creates a priority-10 human intent from a
// live finding node, copies that finding's asset anchors, records the
// finding --derived_from--> intent lineage edge, and persists its audit activity.
// The returned activity is the committed row and can be broadcast as-is without
// calling AppendActivity again.
func (s *ExplorationStore) AddFindingFollowUpIntent(findingID, findingNodeID int64, description string, audit Activity) (int64, Activity, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, Activity{}, err
	}
	defer tx.Rollback()

	// TiDB 的 SELECT … FOR UPDATE 只吃单表点/范围读（PG 的 JOIN + FOR SHARE OF
	// 不被支持），因此按 findings → tasks → exploration_nodes 的固定表序逐行加锁，
	// 校验条件与原 JOIN 完全一致：同一探索、节点确属该漏洞、kind='finding'。
	var linkedNodeID, linkedTaskID sql.NullInt64
	if err := tx.QueryRow(`SELECT node_id, task_id FROM findings WHERE id = ? FOR UPDATE`, findingID).Scan(&linkedNodeID, &linkedTaskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, Activity{}, ErrFindingOriginUnavailable
		}
		return 0, Activity{}, err
	}
	if !linkedNodeID.Valid || !linkedTaskID.Valid || linkedNodeID.Int64 != findingNodeID {
		return 0, Activity{}, ErrFindingOriginUnavailable
	}
	var taskExplorationID int64
	if err := tx.QueryRow(`SELECT exploration_id FROM tasks WHERE id = ? FOR UPDATE`, linkedTaskID.Int64).Scan(&taskExplorationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, Activity{}, ErrFindingOriginUnavailable
		}
		return 0, Activity{}, err
	}
	if taskExplorationID != s.expID {
		return 0, Activity{}, ErrFindingOriginUnavailable
	}
	var nodeKind string
	if err := tx.QueryRow(`SELECT kind FROM exploration_nodes WHERE id = ? FOR UPDATE`, findingNodeID).Scan(&nodeKind); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, Activity{}, ErrFindingOriginUnavailable
		}
		return 0, Activity{}, err
	}
	if nodeKind != KindFinding {
		return 0, Activity{}, ErrFindingOriginUnavailable
	}
	liveNodeID := findingNodeID

	anchors := []int64{}
	anchorRows, err := tx.Query(`SELECT asset_id FROM exploration_anchors WHERE node_id = ? ORDER BY asset_id`, liveNodeID)
	if err != nil {
		return 0, Activity{}, err
	}
	for anchorRows.Next() {
		var assetID int64
		if err := anchorRows.Scan(&assetID); err != nil {
			anchorRows.Close()
			return 0, Activity{}, err
		}
		anchors = append(anchors, assetID)
	}
	if err := anchorRows.Err(); err != nil {
		anchorRows.Close()
		return 0, Activity{}, err
	}
	if err := anchorRows.Close(); err != nil {
		return 0, Activity{}, err
	}

	payload := map[string]any{
		"summary":                description,
		"source_finding_id":      findingID,
		"source_finding_node_id": liveNodeID,
	}
	if len(anchors) > 0 {
		payload["asset_ids"] = anchors
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, Activity{}, err
	}
	// 自增主键经 LastInsertId 取回（TiDB 没有 RETURNING）。
	nodeRes, err := tx.Exec(`INSERT INTO exploration_nodes(exploration_id,kind,payload,priority,state,origin)
		VALUES (?, 'intent', ?, 10, 'open', 'human')`, s.expID, string(raw))
	if err != nil {
		return 0, Activity{}, err
	}
	intentID, err := nodeRes.LastInsertId()
	if err != nil {
		return 0, Activity{}, fmt.Errorf("exploration_nodes: LastInsertId after INSERT: %w", err)
	}
	if intentID < 1 {
		return 0, Activity{}, fmt.Errorf("exploration_nodes: auto-increment id %d out of range", intentID)
	}
	// 锚点唯一键 uq_exploration_anchors(node_id,asset_id)：从来源节点复制锚点时重复
	// 是常态，PG 的 ON CONFLICT DO NOTHING 在这里对应 INSERT IGNORE（被静默的只有
	// 这一条唯一键，其余列都是 NOT NULL，真正的写入错误照样抛出）。
	if _, err := tx.Exec(`INSERT IGNORE INTO exploration_anchors(node_id,asset_id)
		SELECT ?, asset_id FROM exploration_anchors WHERE node_id = ?`, intentID, liveNodeID); err != nil {
		return 0, Activity{}, err
	}
	if _, err := tx.Exec(`INSERT INTO exploration_edges(exploration_id,src_id,rel,dst_id)
		VALUES (?, ?, ?, ?)`, s.expID, liveNodeID, RelDerivedFrom, intentID); err != nil {
		return 0, Activity{}, err
	}

	audit.NodeID = &intentID
	if summary := strings.TrimSpace(audit.Summary); summary != "" {
		audit.Summary = fmt.Sprintf("%s #%d", summary, intentID)
	} else {
		audit.Summary = ""
	}
	metadata := audit.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	// TiDB 没有 RETURNING：自增 id 走 LastInsertId，created_at 由列的
	// CURRENT_TIMESTAMP(3) 默认值盖章后再按主键回读，Go 端绝不自造时间戳。
	activityRes, err := tx.Exec(`
INSERT INTO activity(exploration_id, node_id, worker, kind, tool, tool_use_id, is_error, summary, detail, metadata, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)
VALUES (?,?,NULLIF(?, ''),NULLIF(?, ''),NULLIF(?, ''),NULLIF(?, ''),?,NULLIF(?, ''),NULLIF(?, ''),?,?,?,?,?)`,
		s.expID, audit.NodeID, utf8Clean(audit.Worker), utf8Clean(audit.Kind), utf8Clean(audit.Tool), utf8Clean(audit.ToolUseID), audit.IsError,
		utf8Clean(audit.Summary), utf8Clean(audit.Detail), metadata, audit.InputTokens, audit.OutputTokens, audit.CacheReadTokens, audit.CacheWriteTokens)
	if err != nil {
		return 0, Activity{}, err
	}
	audit.ID, err = activityRes.LastInsertId()
	if err != nil {
		return 0, Activity{}, fmt.Errorf("activity: LastInsertId after INSERT: %w", err)
	}
	if audit.ID < 1 {
		return 0, Activity{}, fmt.Errorf("activity: auto-increment id %d out of range", audit.ID)
	}
	if err := tx.QueryRow(`SELECT created_at FROM activity WHERE id = ?`, audit.ID).Scan(&audit.CreatedAt); err != nil {
		return 0, Activity{}, err
	}
	audit.Metadata = metadata
	if err := tx.Commit(); err != nil {
		return 0, Activity{}, err
	}
	return intentID, audit, nil
}

// ListFindingsForExport returns findings for the 发现 page 导出功能，携带完整
// report 字段、不分页。ids 非空时按这批 finding id 精确导出(勾选导出),忽略
// filter;ids 为空时按 filter 导出(导出当前筛选/全部)。结果按严重等级降序、
// 再按时间倒序,与「导出汇总报告」的分组顺序一致。
func (d *DB) ListFindingsForExport(f FindingFilter, ids []int64) ([]*DBFinding, error) {
	const order = `ORDER BY CASE f.severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC, f.created_at DESC`
	cols := findingSelectCols + `, COALESCE(f.report, '')`

	var q string
	var args []any
	if len(ids) > 0 {
		for _, id := range ids {
			args = append(args, id)
		}
		q = `SELECT ` + cols + `
			FROM findings f
			LEFT JOIN tasks t ON f.task_id = t.id
			WHERE f.id IN (` + questionPlaceholders(len(ids)) + `)
			` + order
	} else {
		scoped, err := d.applyAssetScope(f)
		if err != nil {
			return nil, err
		}
		where, wargs := scoped.where()
		q = `SELECT ` + cols + `
			FROM findings f
			LEFT JOIN tasks t ON f.task_id = t.id` + where + `
			` + order
		args = wargs
	}

	rows, err := d.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*DBFinding
	for rows.Next() {
		f := &DBFinding{}
		var aidsJSON string
		if err := rows.Scan(&f.ID, &f.TaskID, &f.NodeID, &f.VulnClass, &f.Name, &f.Severity,
			&f.Summary, &f.Evidence, &f.Worker, &aidsJSON, &f.Status, &f.CreatedAt,
			&f.TaskDescription, &f.EvidenceVersion, &f.ReportEvidenceVersion, &f.TrafficCount, &f.Report); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(aidsJSON), &f.AssetIDs)
		out = append(out, f)
	}
	return out, rows.Err()
}

// FindingStats is the whole-table aggregate powering the 发现 page's stat cards
// and vuln-class filter — computed server-side so it stays exact regardless of
// pagination.
type FindingStats struct {
	Total       int                 `json:"total"`
	Pending     int                 `json:"pending"`
	Critical    int                 `json:"critical"`
	High        int                 `json:"high"`
	Medium      int                 `json:"medium"`
	Low         int                 `json:"low"`
	VulnClasses []string            `json:"vulnclasses"`
	Tasks       []FindingTaskOption `json:"tasks"` // 有漏洞的任务(供「按任务」下拉)
}

// FindingTaskOption is one entry in the 发现 page's 任务 filter: a task that has at
// least one finding, with its description and finding count. Description is empty when
// the task has since been deleted (finding rows persist), so the frontend falls back to
// the id.
//
// P03（R14）:id 是 BIGINT,对外带引号 string;count 是计数,保持 number。
// 持久化回读核查:FindingStats 会作为归档 aggregate_stats 的 finding_stats 段被
// json.Unmarshal 回读(task_archive_aggregate_stats.go),但该段由 SQL
// jsonb_build_object 生成,只有 total/pending/critical/high/medium/low/vulnclasses
// 七个键(db/task_archives.go 的 "finding_stats" 段,历史版本同样),从不含 tasks,
// 因此给 tasks[].id 加 ,string 不会破坏归档统计的反序列化。
type FindingTaskOption struct {
	ID          int64  `json:"id,string"`
	Name        string `json:"name"` // 可选任务名称;空=未命名
	Description string `json:"description"`
	Count       int    `json:"count"`
}

// FindingStats returns whole-table counts (by severity + pending) and the sorted
// set of distinct vuln classes.
func (d *DB) FindingStats() (*FindingStats, error) {
	st := &FindingStats{VulnClasses: []string{}, Tasks: []FindingTaskOption{}}
	// 分桶必须是 COUNT(CASE WHEN … THEN 1 END):TiDB 没有 PG 的聚合 FILTER 子句,
	// 而 SUM/COUNT 的 CASE 形状在无匹配行(含整张表为空)时恒为数值 0,不会像
	// 裸聚合那样把 NULL 灌进 Scan。
	err := d.QueryRow(`SELECT
		COUNT(*),
		COUNT(CASE WHEN status = 'pending' THEN 1 END),
		COUNT(CASE WHEN severity = 'critical' THEN 1 END),
		COUNT(CASE WHEN severity = 'high' THEN 1 END),
		COUNT(CASE WHEN severity = 'medium' THEN 1 END),
		COUNT(CASE WHEN severity = 'low' THEN 1 END)
		FROM findings`).Scan(&st.Total, &st.Pending, &st.Critical, &st.High, &st.Medium, &st.Low)
	if err != nil {
		return nil, err
	}
	rows, err := d.Query(`SELECT DISTINCT vulnclass FROM findings WHERE vulnclass <> '' ORDER BY vulnclass`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var vc string
		if err := rows.Scan(&vc); err != nil {
			return nil, err
		}
		st.VulnClasses = append(st.VulnClasses, vc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 任务下拉:有漏洞的任务,带描述(任务删除后为空,前端回退 id)和条数,最新有漏洞的排前。
	trows, err := d.Query(`SELECT f.task_id, COALESCE(t.name, ''), COALESCE(t.description, ''), COUNT(*)
		FROM findings f
		LEFT JOIN tasks t ON f.task_id = t.id
		WHERE f.task_id IS NOT NULL
		GROUP BY f.task_id, t.name, t.description
		ORDER BY MAX(f.created_at) DESC`)
	if err != nil {
		return nil, err
	}
	defer trows.Close()
	for trows.Next() {
		var opt FindingTaskOption
		if err := trows.Scan(&opt.ID, &opt.Name, &opt.Description, &opt.Count); err != nil {
			return nil, err
		}
		st.Tasks = append(st.Tasks, opt)
	}
	if err := trows.Err(); err != nil {
		return nil, err
	}
	archived, err := d.archivedTaskAggregates()
	if err != nil {
		return nil, err
	}
	vulnclasses := make(map[string]bool, len(st.VulnClasses))
	for _, vulnclass := range st.VulnClasses {
		vulnclasses[vulnclass] = true
	}
	for _, aggregate := range archived {
		cold := aggregate.FindingStats
		st.Total += cold.Total
		st.Pending += cold.Pending
		st.Critical += cold.Critical
		st.High += cold.High
		st.Medium += cold.Medium
		st.Low += cold.Low
		for _, vulnclass := range cold.VulnClasses {
			if vulnclass != "" {
				vulnclasses[vulnclass] = true
			}
		}
	}
	st.VulnClasses = st.VulnClasses[:0]
	for vulnclass := range vulnclasses {
		st.VulnClasses = append(st.VulnClasses, vulnclass)
	}
	sort.Strings(st.VulnClasses)
	return st, nil
}

// GetFinding returns a single finding row (with task_description joined and the
// full Markdown report), or nil when no row has that id. Unlike the list queries
// it also selects `report` — that column is only needed on the detail page.
func (d *DB) GetFinding(id int64) (*DBFinding, error) {
	f := &DBFinding{}
	var aidsJSON string
	err := d.QueryRow(`SELECT `+findingSelectCols+`, COALESCE(f.report, '')
		FROM findings f
		LEFT JOIN tasks t ON f.task_id = t.id
		WHERE f.id = ?`, id).Scan(
		&f.ID, &f.TaskID, &f.NodeID, &f.VulnClass, &f.Name, &f.Severity,
		&f.Summary, &f.Evidence, &f.Worker, &aidsJSON, &f.Status, &f.CreatedAt,
		&f.TaskDescription, &f.EvidenceVersion, &f.ReportEvidenceVersion, &f.TrafficCount, &f.Report)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(aidsJSON), &f.AssetIDs)
	return f, nil
}

// DeleteFinding removes a finding entirely: the standalone findings row and its
// originating exploration node (kind='finding'), so it disappears from the findings
// list, the per-task 发现 Tab, and the exploration graph alike.
//
// 新库没有外键，级联全部要靠这里显式做（同一个证据事务内、按锁序 findings →
// finding_traffic_bindings → finding_retests → exploration_*）：
//   - finding_traffic_bindings 整行删除（finding_id 是 NOT NULL 列，没有置空这条路），
//     快照文件由证据存储的 GC 负责；
//   - 尚未收尾的复测封成终态：active_finding_id 是 IF(status IN ('pending','running'),
//     finding_id, NULL) 的生成列，漏封会让前端永久显示「复测中」，并让后续复测被去重吞掉；
//   - 节点的边与锚点显式删除、activity 的 node_id 置空（保留活动流水）。
//
// Returns rows affected (0 = no finding with that id)。
func (d *DB) DeleteFinding(id int64) (n int64, err error) {
	err = d.WithEvidenceTx(context.Background(), func(tx *sql.Tx) error {
		if err := LockFindingEvidenceTx(tx, id, nil); err != nil {
			if errors.Is(err, ErrFindingNotFound) {
				return nil
			}
			return err
		}
		var nodeID sql.NullInt64
		if err := tx.QueryRow(`SELECT node_id FROM findings WHERE id = ?`, id).Scan(&nodeID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(`DELETE FROM finding_traffic_bindings WHERE finding_id = ?`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE finding_retests
			SET status='stopped', error='漏洞已删除，复测随之终止', finished_at=CURRENT_TIMESTAMP(3)
			WHERE finding_id = ? AND status IN ('pending','running')`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM findings WHERE id = ?`, id); err != nil {
			return err
		}
		if nodeID.Valid {
			if err := detachExplorationNodeTx(tx, nodeID.Int64); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM exploration_nodes WHERE id = ? AND kind='finding'`, nodeID.Int64); err != nil {
				return err
			}
		}
		n = 1
		return nil
	})
	return
}

// detachExplorationNodeTx 摘掉一个节点的全部弱关联：边（任一端）、资产锚点，并把
// 引用它的 activity.node_id 置空。没有外键级联，删除节点前必须先走这一步，否则
// 库里会留下指向已消失节点的悬挂边与活动。
func detachExplorationNodeTx(tx *sql.Tx, nodeID int64) error {
	if _, err := tx.Exec(`DELETE FROM exploration_edges WHERE src_id = ? OR dst_id = ?`, nodeID, nodeID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM exploration_anchors WHERE node_id = ?`, nodeID); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE activity SET node_id = NULL WHERE node_id = ?`, nodeID)
	return err
}

// DeleteFindingsByTask removes all findings rows of a task. The originating
// exploration finding nodes are cascade-deleted separately when the task's
// exploration subgraph is dropped. Returns rows deleted.
//
// 与 DeleteFinding 同一口径：没有外键级联，证据绑定与未收尾的复测必须在删
// findings 行之前先解除，否则会留下指向已删漏洞的绑定与永久「复测中」的记录。
func (d *DB) DeleteFindingsByTask(taskID int64) (int64, error) {
	if _, err := d.Exec(`DELETE FROM finding_traffic_bindings
		WHERE finding_id IN (SELECT id FROM findings WHERE task_id = ?)`, taskID); err != nil {
		return 0, err
	}
	if _, err := d.Exec(`UPDATE finding_retests
		SET status='stopped', error='漏洞已删除，复测随之终止', finished_at=CURRENT_TIMESTAMP(3)
		WHERE status IN ('pending','running')
		  AND finding_id IN (SELECT id FROM findings WHERE task_id = ?)`, taskID); err != nil {
		return 0, err
	}
	res, err := d.Exec(`DELETE FROM findings WHERE task_id = ?`, taskID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SetFindingStatus updates one finding's triage state. Returns 1 when the finding
// exists and 0 when it does not.
//
// 底层 setter：只改状态、不登记推送事件。生产代码改状态请走
// SetFindingStatusWithNotify —— 直接调本函数会让「状态变更推送」静默失效。
// 保留它是为了让不关心通知的用例（参数校验、复测流程）能单独驱动状态。
//
// 存在性判定不能拿 RowsAffected：MySQL/TiDB 只把「值真的变了」的行计入 affected，
// 把一个已经是 fixed 的漏洞再设成 fixed 会返回 0，上层会误读成「漏洞不存在」。
// 因此写入后按主键回查一次（同库同表，无跨表锁序问题）。
func (d *DB) SetFindingStatus(id int64, status string) (int64, error) {
	if _, err := d.Exec(`UPDATE findings SET status = ? WHERE id = ?`, status, id); err != nil {
		return 0, err
	}
	var found int
	if err := d.QueryRow(`SELECT COUNT(*) FROM findings WHERE id = ?`, id).Scan(&found); err != nil {
		return 0, err
	}
	if found == 0 {
		return 0, nil
	}
	return 1, nil
}

// SetFindingReportByNodeID sets the Markdown report on the standalone finding row
// whose node_id matches — report_finding returns that node id, so an agent tool
// can address the finding it just created. Returns rows affected (0 when no row).
func (d *DB) SetFindingReportByNodeID(nodeID int64, report string) (int64, error) {
	return d.SetFindingReportVersionByNodeID(context.Background(), nodeID, report, nil)
}

// setFindingCol updates one text column on the standalone finding row AND mirrors
// the new value into the originating exploration node's payload under jsonKey, so the
// per-task 发现 Tab (which reads the node payload, not this table) stays in sync.
// Returns 1 when a finding with that id exists (0 when it does not); the node sync
// is best-effort. col and jsonKey MUST be trusted constants (they are interpolated
// into SQL) — never pass user input.
//
// PG 用 `UPDATE … RETURNING node_id` 一次拿回归属节点；TiDB 没有 RETURNING，改成
// 「先写、后按主键回读」，顺带把「值未变更 → RowsAffected=0」的陷阱彻底绕开。
// 节点侧的 jsonb_set 对应 JSON_SET，写入值必须是 JSON 文档字面量（字符串要带引号）。
func (d *DB) setFindingCol(id int64, col, jsonKey, val string) (int64, error) {
	if _, err := d.Exec(`UPDATE findings SET `+col+` = ? WHERE id = ?`, val, id); err != nil {
		return 0, err
	}
	var nodeID sql.NullInt64
	err := d.QueryRow(`SELECT node_id FROM findings WHERE id = ?`, id).Scan(&nodeID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if nodeID.Valid {
		_, _ = d.Exec(`UPDATE exploration_nodes
			SET payload = JSON_SET(COALESCE(payload, '{}'), '$.`+jsonKey+`', CAST(? AS JSON))
			WHERE id = ?`, jsonLiteral(val), nodeID.Int64)
	}
	return 1, nil
}

// SetFindingSeverity updates one finding's severity (+ node payload sync). Returns
// rows affected (0 when no finding has that id).
func (d *DB) SetFindingSeverity(id int64, severity string) (int64, error) {
	return d.setFindingCol(id, "severity", "severity", severity)
}

// SetFindingName updates one finding's 漏洞名称 (+ node payload sync). Empty name is
// allowed — the frontend falls back to the vuln class for display.
func (d *DB) SetFindingName(id int64, name string) (int64, error) {
	return d.setFindingCol(id, "name", "name", name)
}

// SetFindingVulnClass updates one finding's 漏洞类别 (+ node payload sync).
func (d *DB) SetFindingVulnClass(id int64, vulnclass string) (int64, error) {
	return d.setFindingCol(id, "vulnclass", "vulnclass", vulnclass)
}

// FindingMeta is the standalone-row data (id, triage state, anchored assets) the
// per-task view grafts onto its exploration-node findings.
type FindingMeta struct {
	TrafficCount int

	ID       int64
	Status   string
	AssetIDs []int64
}

// FindingMetaByNodeID maps a task's finding node ids to their standalone-row
// metadata (status + anchored asset ids) via the asset store, so callers holding
// only an AssetStore (e.g. the agent ToolSet) can reach it without a raw *DB.
func (a *AssetStore) FindingMetaByNodeID(taskID int64) (map[int64]FindingMeta, error) {
	return a.db.FindingMetaByNodeID(taskID)
}

// FindingMetaByNodeID maps a task's finding node ids to their standalone-row
// metadata, so the per-task view (which reads exploration nodes) can show and
// edit the same status — and the same anchored assets — as the global 发现 page.
func (d *DB) FindingMetaByNodeID(taskID int64) (map[int64]FindingMeta, error) {
	out := map[int64]FindingMeta{}
	if taskID <= 0 {
		return out, nil
	}
	rows, err := d.Query(`SELECT node_id, id, COALESCE(status,'pending'), asset_ids, (SELECT count(*) FROM finding_traffic_bindings b WHERE b.finding_id=findings.id) FROM findings
		WHERE task_id=? AND node_id IS NOT NULL`, taskID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var nid int64
		var m FindingMeta
		var aidsJSON string
		if err := rows.Scan(&nid, &m.ID, &m.Status, &aidsJSON, &m.TrafficCount); err != nil {
			return out, err
		}
		_ = json.Unmarshal([]byte(aidsJSON), &m.AssetIDs)
		out[nid] = m
	}
	return out, rows.Err()
}
