package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

const (
	MaxTaskTemplateNameRunes = 120
	MaxTaskTemplateTextRunes = 16000
)

var (
	ErrTaskTemplateInvalid      = errors.New("invalid task template")
	ErrTaskTemplateNameConflict = errors.New("task template name already exists")
	ErrTaskTemplateNotFound     = errors.New("task template not found")
)

// TaskTemplate is a reusable task preset (description/goal + optional category
// and task-level intercept/allow rules).
type TaskTemplate struct {
	ID             int64                    `json:"id,string"`
	Name           string                   `json:"name"`
	NKey           string                   `json:"-"`
	Description    string                   `json:"description"`
	Goal           string                   `json:"goal"`
	CategoryID     *int64                   `json:"category_id,string"`
	InterceptRules []TaskInterceptRuleInput `json:"intercept_rules"`
	CreatedAt      time.Time                `json:"created_at"`
	UpdatedAt      time.Time                `json:"updated_at"`
}

// TaskTemplateInput is the create/update payload after normalization.
type TaskTemplateInput struct {
	Name           string
	Description    string
	Goal           string
	CategoryID     *int64
	InterceptRules []TaskInterceptRuleInput
}

// TaskTemplatePatch changes only fields flagged as set. Name/Description/Goal use
// non-nil pointers; CategoryID/InterceptRules use explicit Set flags (so a nil
// CategoryID can mean "clear" when SetCategoryID is true).
type TaskTemplatePatch struct {
	Name              *string
	Description       *string
	Goal              *string
	CategoryID        *int64
	SetCategoryID     bool
	InterceptRules    []TaskInterceptRuleInput
	SetInterceptRules bool
}

const taskTemplateCols = `id, name, nkey, description, goal, category_id, intercept_rules, created_at, updated_at`

func scanTaskTemplate(row interface{ Scan(...any) error }) (TaskTemplate, error) {
	var t TaskTemplate
	var rulesRaw []byte
	if err := row.Scan(&t.ID, &t.Name, &t.NKey, &t.Description, &t.Goal, &t.CategoryID, &rulesRaw, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return t, err
	}
	t.InterceptRules = []TaskInterceptRuleInput{}
	if len(rulesRaw) > 0 {
		if err := json.Unmarshal(rulesRaw, &t.InterceptRules); err != nil {
			return t, err
		}
		if t.InterceptRules == nil {
			t.InterceptRules = []TaskInterceptRuleInput{}
		}
	}
	return t, nil
}

// marshalTemplateRules serializes a template's rule snapshot to JSONB text,
// always producing a JSON array (never null).
func marshalTemplateRules(rules []TaskInterceptRuleInput) ([]byte, error) {
	if rules == nil {
		rules = []TaskInterceptRuleInput{}
	}
	return json.Marshal(rules)
}

// taskTemplateName normalizes display whitespace while preserving the user's case.
func taskTemplateName(name string) string { return strings.Join(strings.Fields(name), " ") }

// taskTemplateNKey is the case-insensitive identity used by the unique index.
func taskTemplateNKey(name string) string { return strings.ToLower(taskTemplateName(name)) }

func normalizeTaskTemplateInput(in TaskTemplateInput) (TaskTemplateInput, string, error) {
	in.Name = taskTemplateName(in.Name)
	in.Description = strings.TrimSpace(in.Description)
	in.Goal = strings.TrimSpace(in.Goal)
	switch {
	case in.Name == "":
		return in, "", fmt.Errorf("%w: name is required", ErrTaskTemplateInvalid)
	case utf8.RuneCountInString(in.Name) > MaxTaskTemplateNameRunes:
		return in, "", fmt.Errorf("%w: name exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateNameRunes)
	case in.Description == "":
		return in, "", fmt.Errorf("%w: description is required", ErrTaskTemplateInvalid)
	case utf8.RuneCountInString(in.Description) > MaxTaskTemplateTextRunes:
		return in, "", fmt.Errorf("%w: description exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateTextRunes)
	case in.Goal == "":
		return in, "", fmt.Errorf("%w: goal is required", ErrTaskTemplateInvalid)
	case utf8.RuneCountInString(in.Goal) > MaxTaskTemplateTextRunes:
		return in, "", fmt.Errorf("%w: goal exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateTextRunes)
	}
	return in, taskTemplateNKey(in.Name), nil
}

func normalizeTaskTemplatePatch(patch TaskTemplatePatch) (TaskTemplatePatch, *string, error) {
	if patch.Name == nil && patch.Description == nil && patch.Goal == nil && !patch.SetCategoryID && !patch.SetInterceptRules {
		return patch, nil, fmt.Errorf("%w: no fields supplied", ErrTaskTemplateInvalid)
	}
	var nkey *string
	if patch.Name != nil {
		name := taskTemplateName(*patch.Name)
		if name == "" {
			return patch, nil, fmt.Errorf("%w: name is required", ErrTaskTemplateInvalid)
		}
		if utf8.RuneCountInString(name) > MaxTaskTemplateNameRunes {
			return patch, nil, fmt.Errorf("%w: name exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateNameRunes)
		}
		key := taskTemplateNKey(name)
		patch.Name = &name
		nkey = &key
	}
	if patch.Description != nil {
		description := strings.TrimSpace(*patch.Description)
		if description == "" {
			return patch, nil, fmt.Errorf("%w: description is required", ErrTaskTemplateInvalid)
		}
		if utf8.RuneCountInString(description) > MaxTaskTemplateTextRunes {
			return patch, nil, fmt.Errorf("%w: description exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateTextRunes)
		}
		patch.Description = &description
	}
	if patch.Goal != nil {
		goal := strings.TrimSpace(*patch.Goal)
		if goal == "" {
			return patch, nil, fmt.Errorf("%w: goal is required", ErrTaskTemplateInvalid)
		}
		if utf8.RuneCountInString(goal) > MaxTaskTemplateTextRunes {
			return patch, nil, fmt.Errorf("%w: goal exceeds %d characters", ErrTaskTemplateInvalid, MaxTaskTemplateTextRunes)
		}
		patch.Goal = &goal
	}
	return patch, nkey, nil
}

// taskTemplateUniqueViolation 判定 MySQL/TiDB 1062：新库用
// uq_task_templates_nkey_hash（nkey_identity + nkey_hash）表达"模板名唯一"，
// PG 的 ON CONFLICT (nkey) / SQLSTATE 23505 在 TiDB 上没有对应物。
// 判定只看**结构化错误编号**，不做错误文本匹配。
func taskTemplateUniqueViolation(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}

// readTaskTemplate 按 id 回读一行模板（TiDB 无 RETURNING）。
func readTaskTemplate(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, id int64) (TaskTemplate, error) {
	return scanTaskTemplate(q.QueryRow(`SELECT `+taskTemplateCols+` FROM task_templates WHERE id=?`, id))
}

// CreateTaskTemplate inserts one globally reusable preset.
func (d *DB) CreateTaskTemplate(in TaskTemplateInput) (*TaskTemplate, error) {
	in, nkey, err := normalizeTaskTemplateInput(in)
	if err != nil {
		return nil, err
	}
	rulesJSON, err := marshalTemplateRules(in.InterceptRules)
	if err != nil {
		return nil, err
	}
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	res, err := d.Exec(`
INSERT INTO task_templates(name, nkey, nkey_identity, nkey_hash, description, goal, category_id, intercept_rules, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, NOW(3), NOW(3))`,
		in.Name, nkey, identity, hash, in.Description, in.Goal, in.CategoryID, rulesJSON)
	if taskTemplateUniqueViolation(err) {
		return nil, ErrTaskTemplateNameConflict
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	t, err := readTaskTemplate(d, id)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// ListTaskTemplates returns the most recently maintained templates first.
func (d *DB) ListTaskTemplates() ([]*TaskTemplate, error) {
	rows, err := d.Query(`SELECT ` + taskTemplateCols + ` FROM task_templates ORDER BY updated_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*TaskTemplate{}
	for rows.Next() {
		t, err := scanTaskTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

// GetTaskTemplate returns nil when id does not exist.
func (d *DB) GetTaskTemplate(id int64) (*TaskTemplate, error) {
	t, err := readTaskTemplate(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// UpdateTaskTemplate replaces the editable fields of one preset.
func (d *DB) UpdateTaskTemplate(id int64, in TaskTemplateInput) (*TaskTemplate, error) {
	in, _, err := normalizeTaskTemplateInput(in)
	if err != nil {
		return nil, err
	}
	return d.PatchTaskTemplate(id, TaskTemplatePatch{
		Name: &in.Name, Description: &in.Description, Goal: &in.Goal,
	})
}

// PatchTaskTemplate atomically changes only the supplied fields. Keeping the
// merge in one UPDATE prevents concurrent disjoint PATCH requests from losing
// each other's changes.
func (d *DB) PatchTaskTemplate(id int64, patch TaskTemplatePatch) (*TaskTemplate, error) {
	patch, nkey, err := normalizeTaskTemplatePatch(patch)
	if err != nil {
		return nil, err
	}
	rulesJSON, err := marshalTemplateRules(patch.InterceptRules)
	if err != nil {
		return nil, err
	}
	// 名字变了才重算 canonical identity；未变的列由 CASE 原样保留，
	// 单条 UPDATE 完成合并，避免并发 disjoint PATCH 互相覆盖。
	var identity, hash any
	if nkey != nil {
		identity, hash = canonicalIdentityPair(schema.NKeyIdentity(*nkey))
	}
	_, err = d.Exec(`UPDATE task_templates
SET name = CASE WHEN ? = 1 THEN ? ELSE name END,
    nkey = CASE WHEN ? = 1 THEN ? ELSE nkey END,
    nkey_identity = CASE WHEN ? = 1 THEN ? ELSE nkey_identity END,
    nkey_hash = CASE WHEN ? = 1 THEN ? ELSE nkey_hash END,
    description = CASE WHEN ? = 1 THEN ? ELSE description END,
    goal = CASE WHEN ? = 1 THEN ? ELSE goal END,
    category_id = CASE WHEN ? = 1 THEN ? ELSE category_id END,
    intercept_rules = CASE WHEN ? = 1 THEN ? ELSE intercept_rules END,
    updated_at = NOW(3)
WHERE id = ?`,
		boolToInt(patch.Name != nil), patch.Name,
		boolToInt(nkey != nil), nkey,
		boolToInt(nkey != nil), identity,
		boolToInt(nkey != nil), hash,
		boolToInt(patch.Description != nil), patch.Description,
		boolToInt(patch.Goal != nil), patch.Goal,
		boolToInt(patch.SetCategoryID), patch.CategoryID,
		boolToInt(patch.SetInterceptRules), rulesJSON,
		id)
	if taskTemplateUniqueViolation(err) {
		return nil, ErrTaskTemplateNameConflict
	}
	if err != nil {
		return nil, err
	}
	// 无 RETURNING：0 affected rows 既可能是"行不存在"也可能是"值没变"，必须回读判定。
	t, err := readTaskTemplate(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskTemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// DeleteTaskTemplate deletes one preset and reports whether it existed.
func (d *DB) DeleteTaskTemplate(id int64) (bool, error) {
	result, err := d.Exec(`DELETE FROM task_templates WHERE id=?`, id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n > 0, err
}
