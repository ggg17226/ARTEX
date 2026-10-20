package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Constraint is one operator-authored operation constraint for a task: kind=allow
// (permitted operations) or kind=deny (forbidden operations), free-text. Stored in
// task_constraints, keyed by exploration_id (cascades with the exploration).
type Constraint struct {
	ID        int64     `json:"id"`
	Kind      string    `json:"kind"` // allow | deny
	Text      string    `json:"text"`
	Origin    string    `json:"origin,omitempty"` // goals | human | system
	CreatedAt time.Time `json:"created_at"`
}

// ListConstraints returns this exploration's constraints, allow before deny, oldest
// first within each group (stable render order for the prompt block + UI).
func (s *ExplorationStore) ListConstraints() ([]Constraint, error) {
	rows, err := s.db.Query(`
SELECT id, kind, text, COALESCE(origin,''), created_at
FROM task_constraints WHERE exploration_id = ?
ORDER BY (kind='deny'), id`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Constraint
	for rows.Next() {
		var c Constraint
		if err := rows.Scan(&c.ID, &c.Kind, &c.Text, &c.Origin, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// AddConstraint inserts one constraint (kind must be allow|deny) and returns its id.
//
// TiDB：`RETURNING id` 没有等价物，改 Exec + LastInsertId（task_constraints 的 id 是
// 自增主键，本语句刚插入的行就是 CURRENT_INSERT_ID）。
func (s *ExplorationStore) AddConstraint(kind, text, origin string) (int64, error) {
	if kind != "allow" && kind != "deny" {
		return 0, fmt.Errorf("kind 必须是 allow 或 deny")
	}
	if origin == "" {
		origin = "system"
	}
	res, err := s.db.Exec(`
INSERT INTO task_constraints(exploration_id, kind, text, origin)
VALUES (?, ?, ?, ?)`, s.expID, kind, text, origin)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateConstraint rewrites a constraint's kind + text; scoped to this exploration.
// Returns an error if no such constraint exists.
//
// M31（changed rows 语义）：PG 的 UPDATE 返回**命中**行数，MySQL/TiDB 的 ROW_COUNT()
// 返回**真正改动**的行数。把 kind/text 原样重写（UI 上"打开编辑再保存"很常见）会得到
// 0，照搬旧判定就会误报"约束不存在"。因此 0 行时必须回读一次确认行是否真的存在；
// 存在 → 视为成功（内容与目标态一致），不存在 → 才是真正的"约束不存在"。
func (s *ExplorationStore) UpdateConstraint(id int64, kind, text string) error {
	if kind != "allow" && kind != "deny" {
		return fmt.Errorf("kind 必须是 allow 或 deny")
	}
	res, err := s.db.Exec(`
UPDATE task_constraints SET kind=?, text=?, updated_at=NOW(3)
WHERE id = ? AND exploration_id = ?`, kind, text, id, s.expID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var exists int64
		err := s.db.QueryRow(`SELECT 1 FROM task_constraints WHERE id = ? AND exploration_id = ? LIMIT 1`, id, s.expID).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("约束不存在")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteConstraint removes a constraint; scoped to this exploration. Returns an
// error if no such constraint exists.
//
// DELETE 的 ROW_COUNT() 与命中行数恒等（删除必然算改动），所以 0 就是不存在，
// 不需要像 UpdateConstraint 那样回读复核。
func (s *ExplorationStore) DeleteConstraint(id int64) error {
	res, err := s.db.Exec(`DELETE FROM task_constraints WHERE id = ? AND exploration_id = ?`, id, s.expID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("约束不存在")
	}
	return nil
}
