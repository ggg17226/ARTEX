package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
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

// ExecuteTaskCascade 在事务内按逆拓扑序执行 Task 级联清理
func ExecuteTaskCascade(ctx context.Context, tx *sql.Tx, closure *TaskCascadeClosure) (*CascadeExecutionAudit, error) {
	if err := closure.Validate(); err != nil {
		return nil, err
	}
	audit := newAudit("task", closure.TaskID)

	// Step 1: SET NULL 弱关联字段，解除外键依赖
	// 1.1 置空 findings.node_id
	if len(closure.NodeIDs) > 0 {
		res, err := tx.ExecContext(ctx, `UPDATE findings SET node_id = NULL WHERE node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...)
		if err != nil {
			return nil, fmt.Errorf("nullify findings.node_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["findings.node_id"] = rows
	}
	// 1.2 置空 findings.task_id
	{
		res, err := tx.ExecContext(ctx, `UPDATE findings SET task_id = NULL WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("nullify findings.task_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["findings.task_id"] = rows
	}
	// 1.3 置空 activity.node_id
	if len(closure.NodeIDs) > 0 {
		res, err := tx.ExecContext(ctx, `UPDATE activity SET node_id = NULL WHERE node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...)
		if err != nil {
			return nil, fmt.Errorf("nullify activity.node_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["activity.node_id"] = rows
	}
	// 1.4 置空 task_asset_links.source_node_id
	if len(closure.NodeIDs) > 0 {
		res, err := tx.ExecContext(ctx, `UPDATE task_asset_links SET source_node_id = NULL WHERE source_node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...)
		if err != nil {
			return nil, fmt.Errorf("nullify task_asset_links.source_node_id: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesUpdated["task_asset_links.source_node_id"] = rows
	}

	// Step 2: 逆拓扑删除底层叶子
	// 2.1 side_question_requests
	if len(closure.SideSessionKeys) > 0 {
		res, err := tx.ExecContext(ctx, `DELETE FROM side_question_requests WHERE session_key IN (`+closurePlaceholders(len(closure.SideSessionKeys))+`)`, stringArgs(closure.SideSessionKeys)...)
		if err != nil {
			return nil, fmt.Errorf("delete side_question_requests: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["side_question_requests"] = rows
	}
	// 2.2 side_question_sessions
	if len(closure.SideSessionKeys) > 0 {
		res, err := tx.ExecContext(ctx, `DELETE FROM side_question_sessions WHERE session_key IN (`+closurePlaceholders(len(closure.SideSessionKeys))+`)`, stringArgs(closure.SideSessionKeys)...)
		if err != nil {
			return nil, fmt.Errorf("delete side_question_sessions: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["side_question_sessions"] = rows
	}
	// 2.3 exploration_anchors
	if len(closure.NodeIDs) > 0 {
		res, err := tx.ExecContext(ctx, `DELETE FROM exploration_anchors WHERE node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...)
		if err != nil {
			return nil, fmt.Errorf("delete exploration_anchors: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["exploration_anchors"] = rows
	}
	// 2.4 exploration_edges
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM exploration_edges WHERE exploration_id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete exploration_edges: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["exploration_edges"] = rows
	}
	// 2.5 task_constraints
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_constraints WHERE exploration_id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete task_constraints: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_constraints"] = rows
	}
	// 2.6 activity
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM activity WHERE exploration_id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete activity: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["activity"] = rows
	}
	// 2.7 main_sessions
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM main_sessions WHERE exploration_id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete main_sessions: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["main_sessions"] = rows
	}
	// 2.8 task_relations
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_relations WHERE task_id = ? OR source_task_id = ?`, closure.TaskID, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_relations: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_relations"] = rows
	}
	// 2.9 task_asset_links
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_asset_links WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_asset_links: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_asset_links"] = rows
	}
	// 2.10 task_llm_profiles
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_llm_profiles WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_llm_profiles: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_llm_profiles"] = rows
	}
	// 2.11 task_scope
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_scope WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_scope: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_scope"] = rows
	}
	// 2.12 task_intercept_rules
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_intercept_rules WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_intercept_rules: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_intercept_rules"] = rows
	}
	// 2.13 task_archives
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM task_archives WHERE task_id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete task_archives: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["task_archives"] = rows
	}

	// Step 3: 删除中间节点 exploration_nodes
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM exploration_nodes WHERE exploration_id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete exploration_nodes: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["exploration_nodes"] = rows
	}

	// Step 4: 根节点删除 (tasks -> explorations)
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, closure.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delete tasks: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["tasks"] = rows
	}
	{
		res, err := tx.ExecContext(ctx, `DELETE FROM explorations WHERE id = ?`, closure.ExplorationID)
		if err != nil {
			return nil, fmt.Errorf("delete explorations: %w", err)
		}
		rows, _ := res.RowsAffected()
		audit.EntitiesDeleted["explorations"] = rows
	}

	// Step 5: 防悬空断言
	if err := AssertNoOrphansForTask(ctx, tx, closure.TaskID, closure.ExplorationID); err != nil {
		return nil, err
	}

	return audit, nil
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
