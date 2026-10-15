package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// TaskCascadeClosure 记录待删除 Task 及其所有关联子实体的闭包清单
type TaskCascadeClosure struct {
	TaskID              uint64
	ExplorationID       uint64
	ArchiveIDs          []uint64
	RelationIDs         []uint64 // task_relations.row_id
	AssetLinkIDs        []uint64 // task_asset_links.row_id
	LLMProfileLinkIDs   []uint64 // task_llm_profiles.row_id
	ScopeIDs            []uint64 // task_scope.id
	InterceptRuleIDs    []uint64 // task_intercept_rules.id
	SideSessionKeys     []string // side_question_sessions.session_key
	SideRequestOrdinals []uint64 // side_question_requests.ordinal
	EdgeIDs             []uint64 // exploration_edges.row_id
	AnchorIDs           []uint64 // exploration_anchors.row_id
	ConstraintIDs       []uint64 // task_constraints.id
	ActivityIDs         []uint64 // activity.id
	MainSessionIDs      []uint64 // main_sessions.row_id
	NodeIDs             []uint64 // exploration_nodes.id

	// 置空集合 (SET NULL)
	NullifyFindingTaskIDs []uint64 // findings.id WHERE task_id = ?
	NullifyFindingNodeIDs []uint64 // findings.id WHERE node_id IN (nodes)

	// 解绑资产
	AssetIDsToDetach []uint64 // assets.id
}

// TotalCascadeDeleteCount 计算本次闭包将物理删除的实体总条数
func (c *TaskCascadeClosure) TotalCascadeDeleteCount() int {
	total := 2 // Task 与 Exploration
	total += len(c.ArchiveIDs)
	total += len(c.RelationIDs)
	total += len(c.AssetLinkIDs)
	total += len(c.LLMProfileLinkIDs)
	total += len(c.ScopeIDs)
	total += len(c.InterceptRuleIDs)
	total += len(c.SideSessionKeys)
	total += len(c.SideRequestOrdinals)
	total += len(c.EdgeIDs)
	total += len(c.AnchorIDs)
	total += len(c.ConstraintIDs)
	total += len(c.ActivityIDs)
	total += len(c.MainSessionIDs)
	total += len(c.NodeIDs)
	return total
}

// Validate 校验闭包自洽性
func (c *TaskCascadeClosure) Validate() error {
	if c.TaskID == 0 {
		return errors.New("task closure: TaskID is 0")
	}
	if c.ExplorationID == 0 {
		return errors.New("task closure: ExplorationID is 0")
	}
	return nil
}

// ComputeTaskClosure 在事务内枚举并收集 Task 的完整关联闭包
func ComputeTaskClosure(ctx context.Context, tx DBTX, taskID uint64) (*TaskCascadeClosure, error) {
	closure := &TaskCascadeClosure{
		TaskID: taskID,
	}

	// 1. 获取任务绑定的 exploration_id
	err := tx.QueryRowContext(ctx, `SELECT exploration_id FROM tasks WHERE id = ? LIMIT 1`, taskID).Scan(&closure.ExplorationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: task id=%d not found", ErrForeignKeyViolation, taskID)
		}
		return nil, err
	}

	// 2. 收集任务直接关联的子表 ID
	// 2.1 task_archives
	if err := queryUint64s(ctx, tx, &closure.ArchiveIDs, `SELECT id FROM task_archives WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	// 2.2 task_relations (双向关联)
	if err := queryUint64s(ctx, tx, &closure.RelationIDs, `SELECT row_id FROM task_relations WHERE task_id = ? OR source_task_id = ?`, taskID, taskID); err != nil {
		return nil, err
	}
	// 2.3 task_asset_links
	if err := queryUint64s(ctx, tx, &closure.AssetLinkIDs, `SELECT row_id FROM task_asset_links WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	// 2.4 task_llm_profiles
	if err := queryUint64s(ctx, tx, &closure.LLMProfileLinkIDs, `SELECT row_id FROM task_llm_profiles WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	// 2.5 task_scope
	if err := queryUint64s(ctx, tx, &closure.ScopeIDs, `SELECT id FROM task_scope WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	// 2.6 task_intercept_rules
	if err := queryUint64s(ctx, tx, &closure.InterceptRuleIDs, `SELECT id FROM task_intercept_rules WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}

	// 3. 收集 exploration 子图实体
	expID := closure.ExplorationID
	// 3.1 exploration_nodes
	if err := queryUint64s(ctx, tx, &closure.NodeIDs, `SELECT id FROM exploration_nodes WHERE exploration_id = ?`, expID); err != nil {
		return nil, err
	}
	// 3.2 exploration_edges
	if err := queryUint64s(ctx, tx, &closure.EdgeIDs, `SELECT row_id FROM exploration_edges WHERE exploration_id = ?`, expID); err != nil {
		return nil, err
	}
	// 3.3 exploration_anchors
	if len(closure.NodeIDs) > 0 {
		if err := queryUint64s(ctx, tx, &closure.AnchorIDs, `SELECT row_id FROM exploration_anchors WHERE node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...); err != nil {
			return nil, err
		}
	}
	// 3.4 task_constraints
	if err := queryUint64s(ctx, tx, &closure.ConstraintIDs, `SELECT id FROM task_constraints WHERE exploration_id = ?`, expID); err != nil {
		return nil, err
	}
	// 3.5 activity
	if err := queryUint64s(ctx, tx, &closure.ActivityIDs, `SELECT id FROM activity WHERE exploration_id = ?`, expID); err != nil {
		return nil, err
	}
	// 3.6 main_sessions
	if err := queryUint64s(ctx, tx, &closure.MainSessionIDs, `SELECT row_id FROM main_sessions WHERE exploration_id = ?`, expID); err != nil {
		return nil, err
	}

	// 4. 收集关联的 side_question_sessions 与 requests
	// 来自 task_id 或 exploration_id 或 intent_id (nodeIDs)
	sideSessQuery := `SELECT session_key FROM side_question_sessions WHERE task_id = ? OR exploration_id = ?`
	args := []any{taskID, expID}
	if len(closure.NodeIDs) > 0 {
		sideSessQuery += ` OR intent_id IN (` + closurePlaceholders(len(closure.NodeIDs)) + `)`
		args = append(args, uint64Args(closure.NodeIDs)...)
	}
	if err := queryStrings(ctx, tx, &closure.SideSessionKeys, sideSessQuery, args...); err != nil {
		return nil, err
	}
	if len(closure.SideSessionKeys) > 0 {
		reqQuery := `SELECT ordinal FROM side_question_requests WHERE session_key IN (` + closurePlaceholders(len(closure.SideSessionKeys)) + `)`
		if err := queryUint64s(ctx, tx, &closure.SideRequestOrdinals, reqQuery, stringArgs(closure.SideSessionKeys)...); err != nil {
			return nil, err
		}
	}

	// 5. 收集需要 SET NULL 的 findings
	if err := queryUint64s(ctx, tx, &closure.NullifyFindingTaskIDs, `SELECT id FROM findings WHERE task_id = ?`, taskID); err != nil {
		return nil, err
	}
	if len(closure.NodeIDs) > 0 {
		if err := queryUint64s(ctx, tx, &closure.NullifyFindingNodeIDs, `SELECT id FROM findings WHERE node_id IN (`+closurePlaceholders(len(closure.NodeIDs))+`)`, uint64Args(closure.NodeIDs)...); err != nil {
			return nil, err
		}
	}

	return closure, closure.Validate()
}

// 辅助查询助手函数
func queryUint64s(ctx context.Context, tx DBTX, dest *[]uint64, query string, args ...any) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		*dest = append(*dest, v)
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
