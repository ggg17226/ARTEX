package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// InterceptRule is one row of intercept_rules.
type InterceptRule struct {
	ID             int64     `json:"id,string"` // P03 R02：64 位实体 ID 对外 string
	Name           string    `json:"name"`
	Enabled        bool      `json:"enabled"`
	Priority       int       `json:"priority"`
	MatchTarget    string    `json:"match_target"`
	MatchType      string    `json:"match_type"`
	Pattern        string    `json:"pattern"`
	Action         string    `json:"action"`
	Message        string    `json:"message"`
	TimeoutEnabled bool      `json:"timeout_enabled"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	TimeoutAction  string    `json:"timeout_action"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// InterceptPending is one row of intercept_pending.
//
// P03 R03：id / rule_id / conversation_id 是 64 位实体 ID，对外 string。
// 两个 *int64 没有 omitempty，nil 仍序列化为 null（语义未变）。
// InterceptApprovalRow 嵌入本类型，自动继承同样的 tag。
type InterceptPending struct {
	ID             int64           `json:"id,string"`
	RuleID         *int64          `json:"rule_id,string"`
	ConversationID *int64          `json:"conversation_id,string"`
	TaskID         *string         `json:"task_id"`
	AgentName      string          `json:"agent_name"`
	ToolName       string          `json:"tool_name"`
	ToolInput      json.RawMessage `json:"tool_input"`
	Status         string          `json:"status"`
	DecisionSource string          `json:"decision_source"`
	Reason         string          `json:"reason"` // 规则 message 或模型判定理由(前缀 [模型])
	DecidedAt      *time.Time      `json:"decided_at"`
	CreatedAt      time.Time       `json:"created_at"`
}

const interceptRuleCols = `id, name, enabled, priority, match_target, match_type, pattern, action, message, timeout_enabled, timeout_seconds, timeout_action, created_at, updated_at`

func scanInterceptRule(row interface{ Scan(...any) error }) (InterceptRule, error) {
	var r InterceptRule
	err := row.Scan(&r.ID, &r.Name, &r.Enabled, &r.Priority,
		&r.MatchTarget, &r.MatchType, &r.Pattern, &r.Action, &r.Message,
		&r.TimeoutEnabled, &r.TimeoutSeconds, &r.TimeoutAction,
		&r.CreatedAt, &r.UpdatedAt)
	return r, err
}

// ListInterceptRules returns all rules ordered by priority DESC then id.
func (d *DB) ListInterceptRules() ([]InterceptRule, error) {
	rows, err := d.Query(`SELECT ` + interceptRuleCols + ` FROM intercept_rules ORDER BY priority DESC, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InterceptRule
	for rows.Next() {
		r, err := scanInterceptRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CreateInterceptRule inserts a new rule.
//
// 原生 TiDB 没有 INSERT … RETURNING：自增 id 由 LastInsertId 取得，再按 id 回读整行
// （created_at/updated_at 由 DDL 默认值生成，回读保证返回体与库内一致）。
func (d *DB) CreateInterceptRule(name, matchTarget, matchType, pattern, action, message string, priority int, enabled bool, timeoutEnabled bool, timeoutSeconds int, timeoutAction string) (InterceptRule, error) {
	res, err := d.Exec(`
INSERT INTO intercept_rules(name, enabled, priority, match_target, match_type, pattern, action, message, timeout_enabled, timeout_seconds, timeout_action)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, enabled, priority, matchTarget, matchType, pattern, action, message, timeoutEnabled, timeoutSeconds, timeoutAction)
	if err != nil {
		return InterceptRule{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return InterceptRule{}, err
	}
	return d.getInterceptRule(id)
}

// UpdateInterceptRule replaces all editable fields of an existing rule.
func (d *DB) UpdateInterceptRule(id int64, name, matchTarget, matchType, pattern, action, message string, priority int, enabled bool, timeoutEnabled bool, timeoutSeconds int, timeoutAction string) (InterceptRule, error) {
	if _, err := d.Exec(`
UPDATE intercept_rules
   SET name=?, enabled=?, priority=?, match_target=?,
       match_type=?, pattern=?, action=?, message=?,
       timeout_enabled=?, timeout_seconds=?, timeout_action=?
 WHERE id=?`,
		name, enabled, priority, matchTarget, matchType, pattern, action, message, timeoutEnabled, timeoutSeconds, timeoutAction, id); err != nil {
		return InterceptRule{}, err
	}
	return d.getInterceptRule(id)
}

// getInterceptRule 按主键回读一条规则；行不存在时返回 sql.ErrNoRows，
// 与旧的 INSERT/UPDATE … RETURNING 行为一致（调用方据此区分"没这条规则"）。
func (d *DB) getInterceptRule(id int64) (InterceptRule, error) {
	return scanInterceptRule(d.QueryRow(`SELECT `+interceptRuleCols+` FROM intercept_rules WHERE id=?`, id))
}

// ErrInterceptRuleNotFound 表示按 id 指定的 intercept_rules 行不存在。
// 审批写入侧（CreateInterceptPending / CreateDecidedIntercept）拿它区分"父规则已被
// 删除"与真实数据库错误；此时不再写悬空 rule_id，而是降级成无父规则的记录。
var ErrInterceptRuleNotFound = errors.New("intercept rule not found")

// lockInterceptRuleForReference 锁定并校验 intercept_rules 父行（M23 / 审查 F5）。
//
// 基线不带外键，"删除规则"与"写审批记录"之间没有任何数据库约束兜底：审批写入方必须
// 与 DeleteInterceptRule 抢同一把父行锁，才能在同一事务里断言"这一条规则现在还在"。
// 规则行很少成为热点（每次命中工具规则才写一条审批），这把行锁的等待代价可以接受。
func lockInterceptRuleForReference(tx *sql.Tx, ruleID int64) error {
	var id int64
	err := tx.QueryRow(`SELECT id FROM intercept_rules WHERE id=? FOR UPDATE`, ruleID).Scan(&id)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: %d", ErrInterceptRuleNotFound, ruleID)
	}
	return err
}

// resolveInterceptRuleParent 在写入方的事务里判定 rule_id 这一列最终写什么：
//   - ruleID == 0：本来就是无父规则（LLM 兜底判定）→ (nil, 0)；
//   - 父规则存活：写 ruleID，decision_source 仍记 'rule'；
//   - 父规则已被删除（陈旧请求）：写 NULL，并按"无规则"重算 decision_source，
//     与 DeleteInterceptRule 的 SET NULL 语义一致 —— 审计记录要留下，但绝不能
//     留下指向不存在规则的悬空引用让前端误判"规则还在"。
func resolveInterceptRuleParent(tx *sql.Tx, ruleID int64) (*int64, int64, error) {
	if ruleID == 0 {
		return nil, 0, nil
	}
	if err := lockInterceptRuleForReference(tx, ruleID); err != nil {
		if errors.Is(err, ErrInterceptRuleNotFound) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	id := ruleID
	return &id, id, nil
}

// DeleteInterceptRule removes a rule.
//
// 旧库里 intercept_pending.rule_id 通过 ON DELETE SET NULL 与规则脱钩；基线不带
// 外键，必须显式把待审/已审记录的 rule_id 置空，否则审批行会挂在一条已经不存在的
// 规则上（decision_source 仍写着 rule，前端会误判规则还在）。
//
// 并发协议（F5）：先锁规则行，再做置空 + 删除。审批写入方（CreateInterceptPending /
// CreateDecidedIntercept）在同一把锁后面校验父行存活，所以删除提交之后不会有新的
// 审批行带着已删除的 rule_id 插进来。规则本就不存在时保持幂等（等价 DELETE 0 行）。
func (d *DB) DeleteInterceptRule(id int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var lockedID int64
	switch err := tx.QueryRow(`SELECT id FROM intercept_rules WHERE id=? FOR UPDATE`, id).Scan(&lockedID); err {
	case nil:
	case sql.ErrNoRows:
		// 幂等：规则本来就不存在（重复删除），保持旧的 "DELETE 0 行" 语义。
		return tx.Commit()
	default:
		return err
	}
	if _, err := tx.Exec(`UPDATE intercept_pending SET rule_id=NULL WHERE rule_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM intercept_rules WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// ToggleInterceptRule flips the enabled state of a rule.
func (d *DB) ToggleInterceptRule(id int64, enabled bool) error {
	_, err := d.Exec(`UPDATE intercept_rules SET enabled=? WHERE id=?`, enabled, id)
	return err
}

// interceptPendingInsertCols 是待审记录的两套插入列（是否已定论）。
// status 在基线里是 MEDIUMTEXT NOT NULL 且没有 DDL 默认值，必须由写入方显式给出。
const interceptPendingInsertCols = `rule_id, conversation_id, task_id, agent_name, tool_name, tool_input, status, reason, decided_at, decision_source, audit`

// CreateInterceptPending inserts a pending approval record and returns its ID.
// convID == 0 → conversation_id stored as NULL (background task).
// taskID == "" → task_id stored as NULL.
//
// ruleID != 0 时先在同一事务里锁定并校验父规则（F5）：父规则已被删除就把 rule_id
// 降级成 NULL，绝不写悬空引用。
func (d *DB) CreateInterceptPending(ruleID, convID int64, taskID, agentName, toolName string, input []byte, reason string, audits ...*InterceptAudit) (int64, error) {
	raw := json.RawMessage(input)
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	var convIDPtr *int64
	if convID != 0 {
		convIDPtr = &convID
	}
	var taskIDPtr *string
	if taskID != "" {
		taskIDPtr = &taskID
	}
	tx, err := d.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// ruleID == 0 → NULL: the LLM fallback judge has no owning rule.
	// ruleID != 0 但父规则已消失 → 同样写 NULL，decision_source 随之退回 model/unknown。
	ruleIDPtr, sourceRuleID, err := resolveInterceptRuleParent(tx, ruleID)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(`
INSERT INTO intercept_pending(`+interceptPendingInsertCols+`)
VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?)`,
		ruleIDPtr, convIDPtr, taskIDPtr, agentName, toolName, raw, reason, interceptSource(sourceRuleID, reason), firstAudit(audits))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

// DecideInterceptPending updates a pending record's status (allowed/denied/timeout).
//
// 审批状态机的单向推进由调用方保证；NOW(3) 走会话时区，DSN 已强制 +00:00，
// 与 created_at 同一时间基准。
func (d *DB) DecideInterceptPending(id int64, status string) error {
	_, err := d.Exec(`UPDATE intercept_pending SET status=?, decided_at=NOW(3) WHERE id=?`, status, id)
	return err
}

// CreateDecidedIntercept inserts an intercept_pending row ALREADY in a final state
// (status = 'allowed' | 'denied'), decided_at stamped now. Used to log allow/deny
// rule matches for observability — they don't block and need no user action, so unlike
// CreateInterceptPending (which starts 'pending') this records the outcome directly.
//
// 父规则协议与 CreateInterceptPending 完全一致（锁定 + 存活校验 + 缺失降级为 NULL）。
func (d *DB) CreateDecidedIntercept(ruleID, convID int64, taskID, agentName, toolName string, input []byte, status, reason string, audits ...*InterceptAudit) (int64, error) {
	raw := json.RawMessage(input)
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	var convIDPtr *int64
	if convID != 0 {
		convIDPtr = &convID
	}
	var taskIDPtr *string
	if taskID != "" {
		taskIDPtr = &taskID
	}
	tx, err := d.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// ruleID == 0 → NULL: the LLM fallback judge has no owning rule.
	ruleIDPtr, sourceRuleID, err := resolveInterceptRuleParent(tx, ruleID)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(`
INSERT INTO intercept_pending(`+interceptPendingInsertCols+`)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(3), ?, ?)`,
		ruleIDPtr, convIDPtr, taskIDPtr, agentName, toolName, raw, status, reason, interceptSource(sourceRuleID, reason), firstAudit(audits))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

const interceptPendingCols = `id, rule_id, conversation_id, task_id, agent_name, tool_name, tool_input, status, reason, decided_at, created_at, decision_source`

func scanInterceptPending(s interface{ Scan(...any) error }, p *InterceptPending) error {
	return s.Scan(&p.ID, &p.RuleID, &p.ConversationID, &p.TaskID, &p.AgentName,
		&p.ToolName, &p.ToolInput, &p.Status, &p.Reason, &p.DecidedAt, &p.CreatedAt, &p.DecisionSource)
}

// ListPendingIntercepts returns all unresolved approval requests, newest first.
func (d *DB) ListPendingIntercepts() ([]InterceptPending, error) {
	rows, err := d.Query(`SELECT ` + interceptPendingCols + ` FROM intercept_pending WHERE status='pending' ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InterceptPending
	for rows.Next() {
		var p InterceptPending
		if err := scanInterceptPending(rows, &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetInterceptPending returns one pending record (nil if absent).
func (d *DB) GetInterceptPending(id int64) (*InterceptPending, error) {
	var p InterceptPending
	err := scanInterceptPending(
		d.QueryRow(`SELECT `+interceptPendingCols+` FROM intercept_pending WHERE id=?`, id),
		&p,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &p, err
}

// InterceptApprovalRow is intercept_pending enriched with conversation and rule info.
type InterceptApprovalRow struct {
	InterceptPending
	ConvTitle    string `json:"conv_title"`
	ConvAgentKey string `json:"conv_agent_key"`
	RuleName     string `json:"rule_name"`
}

func scanInterceptApprovalRow(rows interface{ Scan(...any) error }, r *InterceptApprovalRow) error {
	return rows.Scan(
		&r.ID, &r.RuleID, &r.ConversationID, &r.TaskID, &r.AgentName,
		&r.ToolName, &r.ToolInput, &r.Status, &r.Reason, &r.DecidedAt, &r.CreatedAt,
		&r.DecisionSource, &r.ConvTitle, &r.ConvAgentKey, &r.RuleName,
	)
}

// Keep legacy rows without decision_source consistent with their displayed source.
const approvalDecisionSource = `COALESCE(NULLIF(ip.decision_source,''), CASE
 WHEN ip.rule_id IS NOT NULL THEN 'rule'
 WHEN ip.reason LIKE '[模型]%' THEN 'model' ELSE 'unknown' END)`

const approvalRowColumns = `ip.id, ip.rule_id, ip.conversation_id, ip.task_id, ip.agent_name,
       ip.tool_name, ip.tool_input, ip.status, ip.reason, ip.decided_at, ip.created_at, ` + approvalDecisionSource + `,
       COALESCE(c.title,'') AS conv_title,
       COALESCE(c.agent_key,'') AS conv_agent_key,
       COALESCE(ir.name,'') AS rule_name`

const approvalRowJoins = `
FROM intercept_pending ip
LEFT JOIN conversations c ON c.id = ip.conversation_id
LEFT JOIN intercept_rules ir ON ir.id = ip.rule_id`

const approvalRowSelect = `SELECT ` + approvalRowColumns + approvalRowJoins
const approvalRowSelectWithAudit = `SELECT ` + approvalRowColumns + `, ip.audit` + approvalRowJoins

func interceptSource(ruleID int64, reason string) string {
	if ruleID != 0 {
		return "rule"
	}
	if strings.HasPrefix(reason, "[模型]") {
		return "model"
	}
	return "unknown"
}

func firstAudit(audits []*InterceptAudit) any {
	if len(audits) == 0 || audits[0] == nil {
		return nil
	}
	raw, err := json.Marshal(audits[0])
	if err != nil {
		return nil
	}
	return raw
}

// ListAllIntercepts returns up to limit intercept_pending rows (newest first)
// joined with conversation and rule info.
func (d *DB) ListAllIntercepts(limit int) ([]InterceptApprovalRow, error) {
	rows, err := d.Query(approvalRowSelect+` ORDER BY ip.created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InterceptApprovalRow
	for rows.Next() {
		var r InterceptApprovalRow
		if err := scanInterceptApprovalRow(rows, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InterceptApprovalFilter combines exact status and decision-source filters.
// Empty fields include all values.
type InterceptApprovalFilter struct {
	Status         string
	DecisionSource string
}

// ListAllInterceptsPage returns one 1-based page and the total matching count.
func (d *DB) ListAllInterceptsPage(page, size int, filter InterceptApprovalFilter) ([]InterceptApprovalRow, int, error) {
	return d.listInterceptsPage("", page, size, filter)
}

// ListTaskIntercepts returns all intercept_pending rows for a specific task (newest first).
func (d *DB) ListTaskIntercepts(taskID string) ([]InterceptApprovalRow, error) {
	rows, err := d.Query(approvalRowSelect+` WHERE ip.task_id=? ORDER BY ip.created_at DESC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InterceptApprovalRow
	for rows.Next() {
		var r InterceptApprovalRow
		if err := scanInterceptApprovalRow(rows, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListTaskInterceptsPage is the paginated variant of ListTaskIntercepts.
func (d *DB) ListTaskInterceptsPage(taskID string, page, size int, filter InterceptApprovalFilter) ([]InterceptApprovalRow, int, error) {
	return d.listInterceptsPage(taskID, page, size, filter)
}

func (d *DB) listInterceptsPage(taskID string, page, size int, filter InterceptApprovalFilter) ([]InterceptApprovalRow, int, error) {
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	offset := (page - 1) * size

	conditions := []string{}
	args := []any{}
	add := func(column, value string) {
		if value != "" {
			// 位置占位符：值按出现顺序追加，无需再算 $n 的下标。
			args = append(args, value)
			conditions = append(conditions, column+"= ?")
		}
	}
	add("ip.task_id", taskID)
	add("ip.status", filter.Status)
	add(approvalDecisionSource, filter.DecisionSource)
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	var total int
	if err := d.QueryRow("SELECT COUNT(*) FROM intercept_pending ip"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	dataQ := approvalRowSelect + where + " ORDER BY ip.created_at DESC, ip.id DESC LIMIT ? OFFSET ?"
	args = append(args, size, offset)
	rows, err := d.Query(dataQ, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []InterceptApprovalRow{}
	for rows.Next() {
		var r InterceptApprovalRow
		if err := scanInterceptApprovalRow(rows, &r); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}
