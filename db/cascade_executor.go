package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
)

var ErrDanglingReferenceRemaining = errors.New("cascade_executor: dangling reference remaining after cascade execution")

// CascadeExecutionAudit 记录级联操作的行数审计事实
type CascadeExecutionAudit struct {
	RootKind        string
	RootID          uint64
	EntitiesDeleted map[string]int64
	EntitiesUpdated map[string]int64
	ExecutedAt      time.Time
}

func (a *CascadeExecutionAudit) TotalDeleted() int64 {
	var total int64
	for _, n := range a.EntitiesDeleted {
		total += n
	}
	return total
}

func (a *CascadeExecutionAudit) TotalUpdated() int64 {
	var total int64
	for _, n := range a.EntitiesUpdated {
		total += n
	}
	return total
}

func newAudit(kind string, id uint64) *CascadeExecutionAudit {
	return &CascadeExecutionAudit{
		RootKind:        kind,
		RootID:          id,
		EntitiesDeleted: make(map[string]int64),
		EntitiesUpdated: make(map[string]int64),
		ExecutedAt:      time.Now().UTC(),
	}
}

// ---------------------------------------------------------------------------
// Task 级联执行器（P08-02 Task 2.3，闭环 F03：写集收敛）
//
// 红线：**本执行器不写任何开放范围 DML**。每一条 UPDATE / DELETE 都必须由
// `narrowWriter` 生成，形如
//
//	DELETE FROM <table> WHERE <catalog 物理 PK 列> IN (?, ?, ...)
//	UPDATE <table> SET <col> = NULL  WHERE <catalog 物理 PK 列> IN (?, ?, ...)
//
// 且绑定的 PK 必须落在**本次已经悲观锁定的物理 PK 集合**内（由闭包 LockPlan 提供）。
// 越界（例如 `UPDATE activity SET node_id = NULL WHERE node_id IN (...)` 或
// `DELETE FROM task_asset_links WHERE task_id = ?`）会在运行期直接返回
// ErrCascadeWriteSetEscaped —— 跨根行要么原样存活，要么先被闭包枚举 + 纳锁。
// ---------------------------------------------------------------------------

// ErrCascadeWriteSetEscaped 表示 executor 试图修改未纳入本次锁计划的物理行。
var ErrCascadeWriteSetEscaped = errors.New("cascade_executor: DML escaped the locked physical PK set")

// ErrCascadeUnknownTable 表示级联写集引用了 lockorder 目录之外的表。
var ErrCascadeUnknownTable = errors.New("cascade_executor: table is not in the lock-order catalog")

// CascadeStatementRecord 是 executor 发出的**每一条** DML 的完整事实。
// 测试据此断言"写集收敛于已锁 PK 集合"与"跨根行未被越界改写"。
type CascadeStatementRecord struct {
	RootKind      string
	RootID        uint64
	Table         string
	PKColumn      string
	Verb          string // "delete" | "update"
	Columns       []string
	SQL           string
	PKs           []uint64
	FromLockedSet bool // 绑定 PK 是否全部来自已锁集合（越界即报错，正常路径恒为 true）

	extraArgs []any // SET 子句绑定的值（排在 PK 列表之前）
}

// CascadeStatementHook 是交付给测试的观测钩子（默认 nil，生产零开销）。
// 非 nil 时 executor 每发出一条 DML 前同步回调一次；回调内不得执行 SQL。
var CascadeStatementHook func(CascadeStatementRecord)

// cascadeDMLChunk 是单条 IN 列表绑定的最大物理 PK 数：既保证"绑定完整物理 PK 列表"，
// 又避免一次语句把上万个占位符压进同一条 SQL。
const cascadeDMLChunk = 512

type cascadeExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// narrowWriter 是级联执行器的**唯一** DML 出口。
type narrowWriter struct {
	ctx      context.Context
	tx       cascadeExecer
	audit    *CascadeExecutionAudit
	rootKind string
	rootID   uint64
	locked   map[string]map[uint64]struct{} // table → 已锁物理 PK 集合
}

func newNarrowWriter(ctx context.Context, tx cascadeExecer, audit *CascadeExecutionAudit, plan []lockorder.EntityKey) (*narrowWriter, error) {
	w := &narrowWriter{
		ctx:      ctx,
		tx:       tx,
		audit:    audit,
		rootKind: audit.RootKind,
		rootID:   audit.RootID,
		locked:   make(map[string]map[uint64]struct{}, len(plan)),
	}
	for _, key := range plan {
		meta, err := lockorder.LookupTableOrder(key.Table)
		if err != nil {
			return nil, err
		}
		if w.locked[meta.Name] == nil {
			w.locked[meta.Name] = make(map[uint64]struct{})
		}
		w.locked[meta.Name][key.PK] = struct{}{}
	}
	return w, nil
}

// check 校验目标表在 Catalog 内，且绑定 PK 全部落在收敛集合内。
func (w *narrowWriter) check(table string, pks []uint64) (string, error) {
	meta, err := lockorder.LookupTableName(table)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrCascadeUnknownTable, table)
	}
	allowed := w.locked[table]
	for _, pk := range pks {
		if _, ok := allowed[pk]; !ok {
			return "", fmt.Errorf("%w: table %s pk %d is not in the locked set of %s %d",
				ErrCascadeWriteSetEscaped, table, pk, w.rootKind, w.rootID)
		}
	}
	return meta.PKColumn, nil
}

func (w *narrowWriter) emit(rec CascadeStatementRecord) error {
	if CascadeStatementHook != nil {
		CascadeStatementHook(rec)
	}
	res, err := w.tx.ExecContext(w.ctx, rec.SQL, rec.args()...)
	if err != nil {
		return fmt.Errorf("%s %s: %w", rec.Verb, rec.Table, err)
	}
	rows, _ := res.RowsAffected()
	if rec.Verb == "delete" {
		w.audit.EntitiesDeleted[rec.Table] += rows
		return nil
	}
	w.audit.EntitiesUpdated[rec.Table+"."+strings.Join(rec.Columns, "+")] += rows
	return nil
}

// deleteByPK 按物理 PK 集合分片删除。
func (w *narrowWriter) deleteByPK(table string, pks []uint64) error {
	normalized := dedupeSortedUint64(pks)
	if len(normalized) == 0 {
		return nil
	}
	pkCol, err := w.check(table, normalized)
	if err != nil {
		return err
	}
	for _, chunk := range chunkUint64(normalized, cascadeDMLChunk) {
		if err := w.emit(CascadeStatementRecord{
			RootKind: w.rootKind, RootID: w.rootID,
			Table: table, PKColumn: pkCol, Verb: "delete",
			SQL:           "DELETE FROM " + table + " WHERE " + pkCol + " IN (" + cascadePlaceholders(len(chunk)) + ")",
			PKs:           chunk,
			FromLockedSet: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

// nullifyByPK 把指定列置 NULL，且只作用于给定物理 PK 集合。
func (w *narrowWriter) nullifyByPK(table string, columns []string, pks []uint64) error {
	if len(columns) == 0 {
		return errors.New("cascade_executor: nullify requires at least one column")
	}
	normalized := dedupeSortedUint64(pks)
	if len(normalized) == 0 {
		return nil
	}
	pkCol, err := w.check(table, normalized)
	if err != nil {
		return err
	}
	sets := make([]string, 0, len(columns))
	for _, col := range columns {
		if !isSafeColumnName(col) {
			return fmt.Errorf("cascade_executor: illegal column name %q", col)
		}
		sets = append(sets, col+" = NULL")
	}
	for _, chunk := range chunkUint64(normalized, cascadeDMLChunk) {
		if err := w.emit(CascadeStatementRecord{
			RootKind: w.rootKind, RootID: w.rootID,
			Table: table, PKColumn: pkCol, Verb: "update", Columns: columns,
			SQL: "UPDATE " + table + " SET " + strings.Join(sets, ", ") +
				" WHERE " + pkCol + " IN (" + cascadePlaceholders(len(chunk)) + ")",
			PKs:           chunk,
			FromLockedSet: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

// updateRowByPK 写回单行（setExpr 自带占位符，其绑定值排在 PK 之前）。
func (w *narrowWriter) updateRowByPK(table, column, setExpr string, pk uint64, args ...any) error {
	if !isSafeColumnName(column) {
		return fmt.Errorf("cascade_executor: illegal column name %q", column)
	}
	pkCol, err := w.check(table, []uint64{pk})
	if err != nil {
		return err
	}
	return w.emit(CascadeStatementRecord{
		RootKind: w.rootKind, RootID: w.rootID,
		Table: table, PKColumn: pkCol, Verb: "update", Columns: []string{column},
		SQL:           "UPDATE " + table + " SET " + setExpr + " WHERE " + pkCol + " = ?",
		PKs:           []uint64{pk},
		FromLockedSet: true,
		extraArgs:     args,
	})
}

// ExecuteTaskCascade 在事务内按逆拓扑序执行 Task 级联清理；
// 全部 DML 收敛于闭包锁计划给出的物理 PK 集合（见文件头红线说明）。
func ExecuteTaskCascade(ctx context.Context, tx *sql.Tx, closure *TaskCascadeClosure) (*CascadeExecutionAudit, error) {
	return executeTaskCascade(ctx, tx, closure, func(ctx context.Context) error {
		return AssertNoOrphansForTask(ctx, tx, closure.TaskID, closure.ExplorationID)
	})
}

// executeTaskCascade 是执行器的可测内核：DML 经 cascadeExecer 接口下发、防悬空断言
// 经回调注入，用例因此可以在**没有真实数据库**的情况下捕获 executor 发出的每一条
// 语句，断言"全部 UPDATE / DELETE 都收敛在已锁物理 PK 集合内"。
func executeTaskCascade(
	ctx context.Context,
	tx cascadeExecer,
	closure *TaskCascadeClosure,
	assertOrphans func(ctx context.Context) error,
) (*CascadeExecutionAudit, error) {
	if err := closure.Validate(); err != nil {
		return nil, err
	}
	audit := newAudit("task", closure.TaskID)
	plan, err := closure.LockPlan()
	if err != nil {
		return nil, err
	}
	w, err := newNarrowWriter(ctx, tx, audit, plan)
	if err != nil {
		return nil, err
	}

	// Step 1: 记账表（可选）
	if closure.Options.DeleteLLMRecords {
		if err := w.deleteByPK("llm_records", closure.LLMRecordIDs); err != nil {
			return nil, err
		}
	}

	// Step 2: findings —— 整行删除，或按列置空（同行两列都要置空时只发一条语句）
	if closure.Options.DeleteFindings {
		// findings 的三张无 FK 子表必须由闭包**枚举物理 PK → 纳锁 → 逆拓扑按 PK 删除**。
		// 这里曾经写过 `DELETE FROM finding_assets WHERE finding_id IN (...)`：那是
		// 开放范围 DML —— 拿计划外的锁、删计划外的行、绕过 narrowWriter 的收敛校验
		// 与行数审计（R1 判红样本），现已彻底移除。
		for _, child := range []struct {
			table string
			ids   []uint64
		}{
			{"finding_traffic_bindings", closure.FindingTrafficBindingIDs},
			{"finding_retests", closure.FindingRetestIDs},
			{"finding_assets", closure.FindingAssetIDs},
		} {
			if err := w.deleteByPK(child.table, child.ids); err != nil {
				return nil, err
			}
		}
		if err := w.deleteByPK("findings", closure.FindingIDs); err != nil {
			return nil, err
		}
	} else {
		both, onlyTask, onlyNode := splitFindingNullify(closure)
		if err := w.nullifyByPK("findings", []string{"task_id"}, onlyTask); err != nil {
			return nil, err
		}
		if err := w.nullifyByPK("findings", []string{"node_id"}, onlyNode); err != nil {
			return nil, err
		}
		if err := w.nullifyByPK("findings", []string{"task_id", "node_id"}, both); err != nil {
			return nil, err
		}
	}

	// Step 3: 跨根弱关联置空（按已锁 activity.id / task_asset_links.row_id 限定）
	if err := w.nullifyByPK("activity", []string{"node_id"}, closure.CrossRootActivityIDs); err != nil {
		return nil, err
	}
	if nullifyLinks := subtractUint64(closure.CrossRootLinkIDs, closure.ForeignAssetLinkIDs); len(nullifyLinks) > 0 {
		if err := w.nullifyByPK("task_asset_links", []string{"source_node_id"}, nullifyLinks); err != nil {
			return nil, err
		}
	}

	// Step 4: 资产双写维护 —— 共享资产只剥离本 task_id（JSON 由闭包持锁后算好）
	for _, assetID := range closure.AssetIDsToDetach {
		payload, ok := closure.AssetDetachJSON[assetID]
		if !ok {
			continue // 资产行已被并发删除：不写回，避免造出幽灵 JSON
		}
		if err := w.updateRowByPK("assets", "task_ids", "task_ids = ?", assetID, payload); err != nil {
			return nil, err
		}
	}

	// Step 5: 跨根 link / anchor 物理清理（只含资产被物理删除的子集）
	if err := w.deleteByPK("task_asset_links", closure.ForeignAssetLinkIDs); err != nil {
		return nil, err
	}
	if err := w.deleteByPK("exploration_anchors", closure.ForeignAssetAnchorIDs); err != nil {
		return nil, err
	}

	// Step 6: 本任务子实体逆拓扑清理
	for _, step := range []struct {
		table string
		ids   []uint64
	}{
		{"side_question_requests", closure.SideRequestOrdinals},
		{"side_question_sessions", closure.SideSessionRowIDs},
		{"exploration_anchors", closure.AnchorIDs},
		{"task_asset_links", closure.AssetLinkIDs},
		{"activity", closure.ActivityIDs},
		{"exploration_edges", closure.EdgeIDs},
		{"task_constraints", closure.ConstraintIDs},
		{"main_sessions", closure.MainSessionIDs},
		{"task_relations", closure.RelationIDs},
		{"task_llm_profiles", closure.LLMProfileLinkIDs},
		{"task_scope", closure.ScopeIDs},
		{"task_intercept_rules", closure.InterceptRuleIDs},
		{"task_archives", closure.ArchiveIDs},
		// Step 7: 仅本任务引用的资产物理删除（共享资产已在 Step 4 detach 并存活）
		{"assets", closure.AssetIDsToDelete},
		// Step 8: 中间节点与两个根
		{"exploration_nodes", closure.NodeIDs},
		{"tasks", []uint64{closure.TaskID}},
		{"explorations", []uint64{closure.ExplorationID}},
	} {
		if err := w.deleteByPK(step.table, step.ids); err != nil {
			return nil, err
		}
	}

	// Step 9: 防悬空断言
	if assertOrphans == nil {
		assertOrphans = func(context.Context) error { return nil }
	}
	if err := assertOrphans(ctx); err != nil {
		return nil, err
	}
	return audit, nil
}

// splitFindingNullify 把待置空的 findings 按需要置空的列分组，
// 保证同一行只被一条 UPDATE 完整置空（两条重叠 UPDATE 的先后值不可预期）。
func splitFindingNullify(closure *TaskCascadeClosure) (both, onlyTask, onlyNode []uint64) {
	taskSet := make(map[uint64]struct{}, len(closure.NullifyFindingTaskIDs))
	for _, id := range closure.NullifyFindingTaskIDs {
		taskSet[id] = struct{}{}
	}
	nodeSet := make(map[uint64]struct{}, len(closure.NullifyFindingNodeIDs))
	for _, id := range closure.NullifyFindingNodeIDs {
		nodeSet[id] = struct{}{}
	}
	for id := range taskSet {
		if _, ok := nodeSet[id]; ok {
			both = append(both, id)
			continue
		}
		onlyTask = append(onlyTask, id)
	}
	for id := range nodeSet {
		if _, ok := taskSet[id]; ok {
			continue
		}
		onlyNode = append(onlyNode, id)
	}
	return dedupeSortedUint64(both), dedupeSortedUint64(onlyTask), dedupeSortedUint64(onlyNode)
}

// subtractUint64 返回 left \ right（升序去重）。
func subtractUint64(left, right []uint64) []uint64 {
	if len(left) == 0 {
		return nil
	}
	exclude := make(map[uint64]struct{}, len(right))
	for _, id := range right {
		exclude[id] = struct{}{}
	}
	out := make([]uint64, 0, len(left))
	for _, id := range left {
		if _, hit := exclude[id]; hit {
			continue
		}
		out = append(out, id)
	}
	return dedupeSortedUint64(out)
}

func chunkUint64(ids []uint64, size int) [][]uint64 {
	if len(ids) == 0 || size <= 0 {
		return nil
	}
	out := make([][]uint64, 0, (len(ids)+size-1)/size)
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		cloned := make([]uint64, end-start)
		copy(cloned, ids[start:end])
		out = append(out, cloned)
	}
	return out
}

func cascadePlaceholders(n int) string {
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

// args 拼出真正绑定的参数：先 SET 值，后物理 PK 列表。
func (r CascadeStatementRecord) args() []any {
	out := make([]any, 0, len(r.extraArgs)+len(r.PKs))
	out = append(out, r.extraArgs...)
	for _, pk := range r.PKs {
		out = append(out, pk)
	}
	return out
}

func isSafeColumnName(col string) bool {
	if col == "" {
		return false
	}
	for i, r := range col {
		if r == '_' && i > 0 {
			continue
		}
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// ExecuteCompanyCascade 在事务内执行 Company 级联清理
func ExecuteCompanyCascade(ctx context.Context, tx *sql.Tx, closure *CompanyCascadeClosure) (*CascadeExecutionAudit, error) {
	if err := closure.Validate(); err != nil {
		return nil, err
	}
	audit := newAudit("company", closure.CompanyID)

	// Step 1: SET NULL 弱引用
	{
		res, err := tx.ExecContext(ctx, `UPDATE assets SET company_id = NULL WHERE company_id = ?`, closure.CompanyID)
		if err != nil {
			return nil, fmt.Errorf("nullify assets.company_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["assets.company_id"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET company_id = NULL WHERE company_id = ?`, closure.CompanyID)
		if err != nil {
			return nil, fmt.Errorf("nullify tasks.company_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["tasks.company_id"] = rows
	}

	// Step 2: 级联删除 scope
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM company_scope WHERE company_id = ?`, closure.CompanyID)
		if err != nil {
			return nil, fmt.Errorf("delete company_scope: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["company_scope"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_scope WHERE company_id = ?`, closure.CompanyID)
		if err != nil {
			return nil, fmt.Errorf("delete task_scope: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_scope"] = rows
	}

	// Step 3: 删除根公司
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM companies WHERE id = ?`, closure.CompanyID)
		if err != nil {
			return nil, fmt.Errorf("delete companies: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["companies"] = rows
	}

	// Step 4: 断言无孤儿
	if err := AssertNoOrphansForCompany(ctx, tx, closure.CompanyID); err != nil {
		return nil, err
	}

	return audit, nil
}

// ExecuteConversationCascade 在事务内执行 Conversation 级联清理
func ExecuteConversationCascade(ctx context.Context, tx *sql.Tx, closure *ConversationCascadeClosure) (*CascadeExecutionAudit, error) {
	if err := closure.Validate(); err != nil {
		return nil, err
	}
	audit := newAudit("conversation", closure.ConversationID)

	// Step 1: 停止并置空 retest 引用
	if len(closure.ActiveRetestIDsToStop) > 0 {
		_, err := tx.ExecContext(ctx, `UPDATE finding_retests SET status = 'stopped' WHERE id IN (`+closurePlaceholders(len(closure.ActiveRetestIDsToStop))+`)`, uint64Args(closure.ActiveRetestIDsToStop)...)
		if err != nil {
			return nil, fmt.Errorf("stop active retests: %w", err)
		}
	}
	{
		res, err := tx.ExecContext(ctx, `UPDATE finding_retests SET conversation_id = NULL WHERE conversation_id = ?`, closure.ConversationID)
		if err != nil {
			return nil, fmt.Errorf("nullify finding_retests.conversation_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["finding_retests.conversation_id"] = rows
	}

	// Step 2: 级联删除子表
	if len(closure.SideSessionKeys) > 0 {
		res, err := tx.ExecContext(ctx, `DELETE FROM side_question_requests WHERE session_key IN (`+closurePlaceholders(len(closure.SideSessionKeys))+`)`, stringArgs(closure.SideSessionKeys)...)
		if err != nil {
			return nil, fmt.Errorf("delete side_question_requests: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["side_question_requests"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM side_question_sessions WHERE conversation_id = ?`, closure.ConversationID)
		if err != nil {
			return nil, fmt.Errorf("delete side_question_sessions: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["side_question_sessions"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM conversation_activities WHERE conversation_id = ?`, closure.ConversationID)
		if err != nil {
			return nil, fmt.Errorf("delete conversation_activities: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["conversation_activities"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM intercept_pending WHERE conversation_id = ?`, closure.ConversationID)
		if err != nil {
			return nil, fmt.Errorf("delete intercept_pending: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["intercept_pending"] = rows
	}

	// Step 3: 删除根会话
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM conversations WHERE id = ?`, closure.ConversationID)
		if err != nil {
			return nil, fmt.Errorf("delete conversations: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["conversations"] = rows
	}

	// Step 4: 断言无孤儿
	if err := AssertNoOrphansForConversation(ctx, tx, closure.ConversationID); err != nil {
		return nil, err
	}

	return audit, nil
}

// AssertNoOrphansForTask 断言数据库中无任何孤儿残留引用该 Task 或其 Exploration
func AssertNoOrphansForTask(ctx context.Context, q DBTX, taskID, expID uint64) error {
	checks := []struct {
		table string
		query string
		args  []any
	}{
		{"task_archives", `SELECT COUNT(1) FROM task_archives WHERE task_id = ?`, []any{taskID}},
		{"task_relations", `SELECT COUNT(1) FROM task_relations WHERE task_id = ? OR source_task_id = ?`, []any{taskID, taskID}},
		{"task_asset_links", `SELECT COUNT(1) FROM task_asset_links WHERE task_id = ?`, []any{taskID}},
		{"task_llm_profiles", `SELECT COUNT(1) FROM task_llm_profiles WHERE task_id = ?`, []any{taskID}},
		{"task_scope", `SELECT COUNT(1) FROM task_scope WHERE task_id = ?`, []any{taskID}},
		{"task_intercept_rules", `SELECT COUNT(1) FROM task_intercept_rules WHERE task_id = ?`, []any{taskID}},
		{"exploration_nodes", `SELECT COUNT(1) FROM exploration_nodes WHERE exploration_id = ?`, []any{expID}},
		{"exploration_edges", `SELECT COUNT(1) FROM exploration_edges WHERE exploration_id = ?`, []any{expID}},
		{"task_constraints", `SELECT COUNT(1) FROM task_constraints WHERE exploration_id = ?`, []any{expID}},
		{"activity", `SELECT COUNT(1) FROM activity WHERE exploration_id = ?`, []any{expID}},
		{"main_sessions", `SELECT COUNT(1) FROM main_sessions WHERE exploration_id = ?`, []any{expID}},
		{"side_question_sessions", `SELECT COUNT(1) FROM side_question_sessions WHERE task_id = ? OR exploration_id = ?`, []any{taskID, expID}},
		{"findings", `SELECT COUNT(1) FROM findings WHERE task_id = ?`, []any{taskID}},
	}

	for _, c := range checks {
		var cnt int64
		if err := q.QueryRowContext(ctx, c.query, c.args...).Scan(&cnt); err != nil {
			return err
		}
		if cnt > 0 {
			return fmt.Errorf("%w: found %d orphan records in %s for task=%d exp=%d", ErrDanglingReferenceRemaining, cnt, c.table, taskID, expID)
		}
	}
	return nil
}

// AssertNoOrphansForCompany 断言数据库中无任何孤儿残留引用该 Company
func AssertNoOrphansForCompany(ctx context.Context, q DBTX, companyID uint64) error {
	checks := []struct {
		table string
		query string
	}{
		{"company_scope", `SELECT COUNT(1) FROM company_scope WHERE company_id = ?`},
		{"task_scope", `SELECT COUNT(1) FROM task_scope WHERE company_id = ?`},
		{"assets", `SELECT COUNT(1) FROM assets WHERE company_id = ?`},
		{"tasks", `SELECT COUNT(1) FROM tasks WHERE company_id = ?`},
	}
	for _, c := range checks {
		var cnt int64
		if err := q.QueryRowContext(ctx, c.query, companyID).Scan(&cnt); err != nil {
			return err
		}
		if cnt > 0 {
			return fmt.Errorf("%w: found %d orphan records in %s for company=%d", ErrDanglingReferenceRemaining, cnt, c.table, companyID)
		}
	}
	return nil
}

// AssertNoOrphansForConversation 断言数据库中无任何孤儿残留引用该 Conversation
func AssertNoOrphansForConversation(ctx context.Context, q DBTX, convID uint64) error {
	checks := []struct {
		table string
		query string
	}{
		{"conversation_activities", `SELECT COUNT(1) FROM conversation_activities WHERE conversation_id = ?`},
		{"intercept_pending", `SELECT COUNT(1) FROM intercept_pending WHERE conversation_id = ?`},
		{"side_question_sessions", `SELECT COUNT(1) FROM side_question_sessions WHERE conversation_id = ?`},
		{"finding_retests", `SELECT COUNT(1) FROM finding_retests WHERE conversation_id = ?`},
	}
	for _, c := range checks {
		var cnt int64
		if err := q.QueryRowContext(ctx, c.query, convID).Scan(&cnt); err != nil {
			return err
		}
		if cnt > 0 {
			return fmt.Errorf("%w: found %d orphan records in %s for conversation=%d", ErrDanglingReferenceRemaining, cnt, c.table, convID)
		}
	}
	return nil
}
