package db

// =====================================================================
// 任务分类（P08-02 Task 2.3，TiDB 方言）
//
// PG 用 `ON CONFLICT (nkey) DO NOTHING RETURNING *` 与 SQLSTATE 23505 表达"重名"。
// 新库的等价物是 **canonical identity 列 + hash 唯一索引**：
//   * `nkey_identity` MEDIUMBLOB：schema.NKeyIdentity(nkey) 的 canonical 字节；
//   * `nkey_hash` BINARY(32) 上有 `uq_task_categories_nkey_hash`（唯一）；
//   * 重名 → MySQL/TiDB **1062 ER_DUP_ENTRY**，映射为 ErrTaskCategoryNameConflict；
//   * 自增主键经 `LastInsertId()` 取得，整行随后回读（无 RETURNING）。
//
// 写者协议：`tasks.category_id` 是"给任务挂子实体（分类归属）"，因此
// SetTaskCategory / SetTasksCategory 都在业务门禁内先按 PK 升序锁住目标任务行并
// 核验存活，正在被级联删除的任务不会被并发写成半套分类归属。
// =====================================================================

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

const MaxTaskCategoryNameRunes = 80

// MaxTaskCategoryBatchSize bounds one batch move so a single request cannot lock
// an unbounded number of task rows.
const MaxTaskCategoryBatchSize = 100

var (
	ErrTaskCategoryInvalid      = errors.New("invalid task category")
	ErrTaskCategoryNameConflict = errors.New("task category name already exists")
	ErrTaskCategoryNotFound     = errors.New("task category not found")
	ErrTaskCategoryTaskNotFound = errors.New("task not found")
)

// TaskCategory is a globally reusable task grouping label.
type TaskCategory struct {
	ID        int64     `json:"id,string"`
	Name      string    `json:"name"`
	NKey      string    `json:"-"`
	TaskCount int       `json:"task_count"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// taskCategoryCols 里的 FILTER (WHERE …) 换成 SUM(CASE WHEN …)（TiDB 无 FILTER 子句）；
// GROUP BY 带上全部非聚合列，兼容 ONLY_FULL_GROUP_BY。
const taskCategoryCols = `category.id, category.name, category.nkey,
       COALESCE(SUM(CASE WHEN task.deleted_at IS NULL THEN 1 ELSE 0 END), 0),
       category.created_at, category.updated_at`

const taskCategoryGroupBy = `GROUP BY category.id, category.name, category.nkey, category.created_at, category.updated_at`

func scanTaskCategory(row interface{ Scan(...any) error }) (TaskCategory, error) {
	var category TaskCategory
	err := row.Scan(&category.ID, &category.Name, &category.NKey, &category.TaskCount, &category.CreatedAt, &category.UpdatedAt)
	return category, err
}

func normalizeTaskCategoryName(name string) (string, string, error) {
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		return "", "", fmt.Errorf("%w: name is required", ErrTaskCategoryInvalid)
	}
	if utf8.RuneCountInString(name) > MaxTaskCategoryNameRunes {
		return "", "", fmt.Errorf("%w: name exceeds %d characters", ErrTaskCategoryInvalid, MaxTaskCategoryNameRunes)
	}
	return name, strings.ToLower(name), nil
}

// taskCategoryUniqueViolation 判定 1062（uq_task_categories_nkey_hash 冲突）。
// 判定必须是**结构化错误编号**，不能靠错误文本里是否出现 "1062"。
func taskCategoryUniqueViolation(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}

// readTaskCategory 按 id 回读一行分类（含实时 task_count）。
func readTaskCategory(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, id int64) (TaskCategory, error) {
	return scanTaskCategory(q.QueryRow(`
SELECT `+taskCategoryCols+`
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
WHERE category.id=?
`+taskCategoryGroupBy, id))
}

func (d *DB) CreateTaskCategory(name string) (*TaskCategory, error) {
	name, nkey, err := normalizeTaskCategoryName(name)
	if err != nil {
		return nil, err
	}
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	res, err := d.Exec(`
INSERT INTO task_categories(name, nkey, nkey_identity, nkey_hash, created_at, updated_at)
VALUES(?, ?, ?, ?, NOW(3), NOW(3))`, name, nkey, identity, hash)
	if taskCategoryUniqueViolation(err) {
		// uq_task_categories_nkey_hash 命中：与旧的 ON CONFLICT (nkey) DO NOTHING +
		// 空结果集完全同义。
		return nil, ErrTaskCategoryNameConflict
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	category, err := readTaskCategory(d, id)
	if err != nil {
		return nil, err
	}
	return &category, nil
}

func (d *DB) ListTaskCategories() ([]*TaskCategory, error) {
	rows, err := d.Query(`
SELECT ` + taskCategoryCols + `
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
` + taskCategoryGroupBy + `
ORDER BY category.name, category.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	categories := []*TaskCategory{}
	for rows.Next() {
		category, err := scanTaskCategory(rows)
		if err != nil {
			return nil, err
		}
		categories = append(categories, &category)
	}
	return categories, rows.Err()
}

func (d *DB) GetTaskCategory(id int64) (*TaskCategory, error) {
	category, err := readTaskCategory(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &category, nil
}

func (d *DB) RenameTaskCategory(id int64, name string) (*TaskCategory, error) {
	name, nkey, err := normalizeTaskCategoryName(name)
	if err != nil {
		return nil, err
	}
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	res, err := d.Exec(`
UPDATE task_categories
   SET name=?, nkey=?, nkey_identity=?, nkey_hash=?, updated_at=NOW(3)
 WHERE id=?`, name, nkey, identity, hash, id)
	if taskCategoryUniqueViolation(err) {
		return nil, ErrTaskCategoryNameConflict
	}
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// UPDATE 命中 0 行既可能是"行不存在"也可能是"值完全没变"，必须回读确认。
		if _, err := readTaskCategory(d, id); errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskCategoryNotFound
		} else if err != nil {
			return nil, err
		}
	}
	category, err := readTaskCategory(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskCategoryNotFound
	}
	if err != nil {
		return nil, err
	}
	return &category, nil
}

// DeleteTaskCategory moves affected tasks to the uncategorized bucket through
// the tasks.category_id ON DELETE SET NULL foreign key.
func (d *DB) DeleteTaskCategory(id int64) (bool, error) {
	result, err := d.Exec(`DELETE FROM task_categories WHERE id=?`, id)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

// SetTaskCategory updates one live task. A nil category means uncategorized.
func (d *DB) SetTaskCategory(taskID int64, categoryID *int64) (*TaskCategory, error) {
	if categoryID == nil {
		var cleared bool
		err := d.runTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
			res, err := tx.ExecContext(ctx, `UPDATE tasks SET category_id=NULL, updated_at=NOW(3) WHERE id=?`, taskID)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			cleared = n > 0
			return err
		})
		if err != nil {
			return nil, mapTaskCategoryWriterGone(err)
		}
		if !cleared {
			return nil, ErrTaskCategoryTaskNotFound
		}
		return nil, nil
	}
	if *categoryID <= 0 {
		return nil, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
	}
	var out *TaskCategory
	err := d.runTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		// 分类必须存活；它同样是被并发删除的对象，这里用**当前读**确认（1062/FK 兜底之外
		// 的显式语义：给已删分类挂任务必须报"分类不存在"）。
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM task_categories WHERE id=? LIMIT 1`, *categoryID).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrTaskCategoryNotFound
			}
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE tasks SET category_id=?, updated_at=NOW(3) WHERE id=?`, *categoryID, taskID); err != nil {
			return err
		}
		category, err := readTaskCategoryTx(tx, *categoryID)
		if err != nil {
			return err
		}
		out = &category
		return nil
	})
	if err != nil {
		return nil, mapTaskCategoryWriterGone(err)
	}
	return out, nil
}

// readTaskCategoryTx 在事务内回读分类（readTaskCategory 的事务版）。
func readTaskCategoryTx(tx *sql.Tx, id int64) (TaskCategory, error) {
	return scanTaskCategory(tx.QueryRow(`
SELECT `+taskCategoryCols+`
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
WHERE category.id=?
`+taskCategoryGroupBy, id))
}

// SetTasksCategory moves several tasks into one category (nil = uncategorized)
// inside a single transaction, so a half-applied batch is never observable.
// It returns the ids that were actually updated — ids missing from that slice
// were deleted between selection and submit — plus the refreshed category row
// whose task_count already reflects this move.
func (d *DB) SetTasksCategory(taskIDs []int64, categoryID *int64) ([]int64, *TaskCategory, error) {
	if len(taskIDs) == 0 {
		return nil, nil, fmt.Errorf("%w: task ids are required", ErrTaskCategoryInvalid)
	}
	if len(taskIDs) > MaxTaskCategoryBatchSize {
		return nil, nil, fmt.Errorf("%w: at most %d tasks per request", ErrTaskCategoryInvalid, MaxTaskCategoryBatchSize)
	}
	for _, id := range taskIDs {
		if id <= 0 {
			return nil, nil, fmt.Errorf("%w: task id must be positive", ErrTaskCategoryInvalid)
		}
	}
	if categoryID != nil && *categoryID <= 0 {
		return nil, nil, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
	}

	var updated []int64
	var category *TaskCategory
	err := d.runTaskChildWriters(taskIDs, func(ctx context.Context, tx *sql.Tx, alive map[int64]bool) error {
		// Checking the category inside the transaction keeps a concurrent delete from
		// turning the UPDATE below into a foreign key violation.
		if categoryID != nil {
			var exists int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM task_categories WHERE id=? LIMIT 1`, *categoryID).Scan(&exists)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrTaskCategoryNotFound
			}
			if err != nil {
				return err
			}
		}
		// 已确认存活的行才写；已消失（正被级联删除）的 id 静默跳过，与原 RETURNING
		// 语义一致 —— 返回集合就是"真正被更新的那些"。
		targets := make([]int64, 0, len(taskIDs))
		for _, id := range taskIDs {
			if alive[id] {
				targets = append(targets, id)
			}
		}
		if len(targets) == 0 {
			return nil
		}
		rows, err := tx.QueryContext(ctx, `UPDATE tasks SET category_id=?, updated_at=NOW(3) WHERE id IN (`+
			inPlaceholders(len(targets))+`)`,
			append([]any{categoryID}, int64Args(targets)...)...)
		if err != nil {
			return err
		}
		// UPDATE 不返回行，改成"更新后逐行回读"以复现 RETURNING id 的语义。
		rows.Close()
		updated = append(updated, targets...)
		if categoryID != nil {
			fetched, err := readTaskCategoryTx(tx, *categoryID)
			if err != nil {
				return err
			}
			category = &fetched
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrTaskCategoryNotFound) {
			return nil, nil, err
		}
		return nil, nil, err
	}
	return updated, category, nil
}

// mapTaskCategoryWriterGone 保留原有对外错误语义（任务不存在/已删除）。
func mapTaskCategoryWriterGone(err error) error {
	if errors.Is(err, ErrTaskNotAlive) {
		return ErrTaskCategoryTaskNotFound
	}
	return err
}
