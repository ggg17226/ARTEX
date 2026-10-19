package db

// =====================================================================
// 任务级资产拦截规则（P08-02 Task 2.3）
//
// 方言：占位符 `?`；TiDB 无 RETURNING → 写入用 LastInsertId()，随后**同一事务内**
// 回读整行。写者协议：task_intercept_rules 是任务的子实体，任何新增/改写都必须先
// 经 `runTaskChildWriter`（业务门禁 → 锁父任务行 → 核验存活），正在被删除的任务
// 不会长出新的规则。
// =====================================================================

import (
	"context"
	"database/sql"
	"fmt"
)

// TaskInterceptRuleInput is one task-level rule supplied at task creation.
// Action: 'block'=拦截 'allow'=允许(白名单)；空视为 'block'。
type TaskInterceptRuleInput struct {
	Enabled bool   `json:"enabled"`
	Action  string `json:"action"`
	Kind    string `json:"kind"`
	Pattern string `json:"pattern"`
	Note    string `json:"note"`
}

const taskInterceptRuleCols = `id, enabled, action, kind, pattern, note, created_at, updated_at`

func scanTaskInterceptRule(row interface{ Scan(...any) error }) (AssetInterceptRule, error) {
	var r AssetInterceptRule
	err := row.Scan(&r.ID, &r.Enabled, &r.Action, &r.Kind, &r.Pattern, &r.Note, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

// readTaskInterceptRule 回读一条规则；不存在时返回 sql.ErrNoRows（与原 RETURNING
// 空结果集语义一致）。
func readTaskInterceptRule(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, taskID, ruleID int64) (AssetInterceptRule, error) {
	return scanTaskInterceptRule(q.QueryRow(
		`SELECT `+taskInterceptRuleCols+` FROM task_intercept_rules WHERE id = ? AND task_id = ?`, ruleID, taskID))
}

// ListTaskInterceptRules returns a task's rules (both block and allow) as
// AssetInterceptRule (Builtin always false; Action carries block/allow). taskID
// <= 0 returns nothing.
func (s *AssetStore) ListTaskInterceptRules(taskID int64) ([]AssetInterceptRule, error) {
	if taskID <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT `+taskInterceptRuleCols+` FROM task_intercept_rules WHERE task_id=? ORDER BY action, id`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AssetInterceptRule
	for rows.Next() {
		r, err := scanTaskInterceptRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TaskInterceptRulesSplit loads a task's rules and splits them into block and
// allow sets, for the enforcement gate.
func (s *AssetStore) TaskInterceptRulesSplit(taskID int64) (block, allow []AssetInterceptRule, err error) {
	rules, err := s.ListTaskInterceptRules(taskID)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range rules {
		if r.Action == "allow" {
			allow = append(allow, r)
		} else {
			block = append(block, r)
		}
	}
	return block, allow, nil
}

func normalizeRuleAction(action string) string {
	if action == "allow" {
		return "allow"
	}
	return "block"
}

// CreateTaskInterceptRule inserts a rule under a task.
func (s *AssetStore) CreateTaskInterceptRule(taskID int64, action, kind, pattern, note string, enabled bool) (AssetInterceptRule, error) {
	var created AssetInterceptRule
	err := s.withTaskChildWriter(taskID, func(_ context.Context, tx *sql.Tx) error {
		res, err := tx.Exec(`
INSERT INTO task_intercept_rules(task_id, enabled, action, kind, pattern, note, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, NOW(3), NOW(3))`,
			taskID, boolToInt(enabled), normalizeRuleAction(action), kind, pattern, note)
		if err != nil {
			return fmt.Errorf("insert task intercept rule %q: %w", pattern, err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		created, err = readTaskInterceptRule(tx, taskID, id)
		return err
	})
	if err != nil {
		return AssetInterceptRule{}, err
	}
	return created, nil
}

// UpdateTaskInterceptRule replaces the editable fields of a task's rule (scoped
// by task_id so a rule can only be edited through its owning task).
func (s *AssetStore) UpdateTaskInterceptRule(taskID, ruleID int64, action, kind, pattern, note string, enabled bool) (AssetInterceptRule, error) {
	var updated AssetInterceptRule
	err := s.withTaskChildWriter(taskID, func(_ context.Context, tx *sql.Tx) error {
		if _, err := tx.Exec(`
UPDATE task_intercept_rules
   SET enabled=?, action=?, kind=?, pattern=?, note=?, updated_at=NOW(3)
 WHERE id=? AND task_id=?`,
			boolToInt(enabled), normalizeRuleAction(action), kind, pattern, note, ruleID, taskID); err != nil {
			return err
		}
		var err error
		updated, err = readTaskInterceptRule(tx, taskID, ruleID)
		return err
	})
	if err != nil {
		return AssetInterceptRule{}, err
	}
	return updated, nil
}

// DeleteTaskInterceptRule removes a task's rule. Returns false if not found.
func (s *AssetStore) DeleteTaskInterceptRule(taskID, ruleID int64) (bool, error) {
	var deleted bool
	err := s.withTaskChildWriter(taskID, func(_ context.Context, tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM task_intercept_rules WHERE id=? AND task_id=?`, ruleID, taskID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		deleted = n > 0
		return err
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// ToggleTaskInterceptRule flips the enabled state of a task's rule.
func (s *AssetStore) ToggleTaskInterceptRule(taskID, ruleID int64, enabled bool) error {
	return s.withTaskChildWriter(taskID, func(_ context.Context, tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE task_intercept_rules SET enabled=?, updated_at=NOW(3) WHERE id=? AND task_id=?`,
			boolToInt(enabled), ruleID, taskID)
		return err
	})
}

// insertTaskInterceptRules inserts task-level rules within the task-creation
// transaction (mirrors insertTaskCompanies). 创建事务本身已在写者门禁内，
// 这里只负责落库。
func insertTaskInterceptRules(tx *sql.Tx, taskID int64, rules []TaskInterceptRuleInput) error {
	for _, r := range rules {
		if _, err := tx.Exec(`
INSERT INTO task_intercept_rules(task_id, enabled, action, kind, pattern, note, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, NOW(3), NOW(3))`,
			taskID, boolToInt(r.Enabled), normalizeRuleAction(r.Action), r.Kind, r.Pattern, r.Note); err != nil {
			return fmt.Errorf("insert task intercept rule %q: %w", r.Pattern, err)
		}
	}
	return nil
}
