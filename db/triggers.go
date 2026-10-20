package db

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

// AgentTrigger is one P3 trigger attached to a custom agent. Six trigger
// conditions can be on at once: interval / on_finding / on_goal_met /
// on_task_timeout / on_tool_call / on_task_create. ToolNames scopes the tool-call
// trigger to a non-empty set of tool keys (empty is rejected at the API layer for
// on_tool_call).
//
// P03 R21：id 是 64 位实体 ID，对外 string。tool_names 列虽存 JSON 文本，但只有
// []string 会被 Marshal/Unmarshal（见 marshalToolNames / scanTrigger），tag 变化不影响。
type AgentTrigger struct {
	ID                 int64      `json:"id,string"`
	AgentKey           string     `json:"agent_key"`
	Enabled            bool       `json:"enabled"`
	IntervalSec        int        `json:"interval_sec"`
	OnFinding          bool       `json:"on_finding"`
	OnGoalMet          bool       `json:"on_goal_met"`
	OnTaskTimeout      bool       `json:"on_task_timeout"`
	OnToolCall         bool       `json:"on_tool_call"`
	OnTaskCreate       bool       `json:"on_task_create"`
	IntervalMessage    string     `json:"interval_message"` // 各触发条件的独立用户消息
	FindingMessage     string     `json:"finding_message"`
	GoalMessage        string     `json:"goal_message"`
	TaskTimeoutMessage string     `json:"task_timeout_message"`
	ToolCallMessage    string     `json:"tool_call_message"`
	TaskCreateMessage  string     `json:"task_create_message"`
	ToolNames          []string   `json:"tool_names"` // 选中的工具 key 列表(DB 存 JSON 文本)
	LastFire           *time.Time `json:"last_fire,omitempty"`
}

const triggerCols = `id, agent_key, enabled, interval_sec, on_finding, on_goal_met, on_task_timeout, on_tool_call, on_task_create, interval_message, finding_message, goal_message, task_timeout_message, tool_call_message, task_create_message, tool_names, last_fire`

// marshalToolNames encodes the tool-key list as JSON text for the tool_names column.
// A nil/empty list stores "" (not "null"/"[]") so the column default stays clean.
func marshalToolNames(names []string) string {
	if len(names) == 0 {
		return ""
	}
	b, err := json.Marshal(names)
	if err != nil {
		return ""
	}
	return string(b)
}

func scanTrigger(sc interface{ Scan(...any) error }) (*AgentTrigger, error) {
	var t AgentTrigger
	var lf sql.NullTime
	var toolNames string
	if err := sc.Scan(&t.ID, &t.AgentKey, &t.Enabled, &t.IntervalSec, &t.OnFinding, &t.OnGoalMet, &t.OnTaskTimeout, &t.OnToolCall, &t.OnTaskCreate,
		&t.IntervalMessage, &t.FindingMessage, &t.GoalMessage, &t.TaskTimeoutMessage, &t.ToolCallMessage, &t.TaskCreateMessage, &toolNames, &lf); err != nil {
		return nil, err
	}
	t.ToolNames = []string{}
	if toolNames != "" {
		_ = json.Unmarshal([]byte(toolNames), &t.ToolNames)
	}
	if lf.Valid {
		t.LastFire = &lf.Time
	}
	return &t, nil
}

// CreateTrigger inserts a trigger for agentKey and returns it.
//
// TiDB：无 INSERT … RETURNING，改 Exec + LastInsertId 定位自增 id，再按 id 回读整行。
// 六个 message 列与 tool_names 都是 MEDIUMTEXT NOT NULL，而 TiDB 不给 TEXT 族声明
// DDL 默认值，因此必须逐列显式写值（空串就是它们的语义默认值），否则 strict 模式 1364。
func (d *DB) CreateTrigger(t *AgentTrigger) (*AgentTrigger, error) {
	res, err := d.Exec(`
INSERT INTO agent_triggers(agent_key, enabled, interval_sec, on_finding, on_goal_met, on_task_timeout, on_tool_call, on_task_create, interval_message, finding_message, goal_message, task_timeout_message, tool_call_message, task_create_message, tool_names)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.AgentKey, t.Enabled, t.IntervalSec, t.OnFinding, t.OnGoalMet, t.OnTaskTimeout, t.OnToolCall, t.OnTaskCreate,
		t.IntervalMessage, t.FindingMessage, t.GoalMessage, t.TaskTimeoutMessage, t.ToolCallMessage, t.TaskCreateMessage, marshalToolNames(t.ToolNames))
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return scanTrigger(d.QueryRow(`SELECT `+triggerCols+` FROM agent_triggers WHERE id = ?`, id))
}

// UpdateTrigger updates a trigger's fields (not last_fire).
func (d *DB) UpdateTrigger(t *AgentTrigger) error {
	_, err := d.Exec(`UPDATE agent_triggers SET enabled=?, interval_sec=?, on_finding=?, on_goal_met=?, on_task_timeout=?, on_tool_call=?, on_task_create=?, interval_message=?, finding_message=?, goal_message=?, task_timeout_message=?, tool_call_message=?, task_create_message=?, tool_names=? WHERE id=?`,
		t.ID, t.Enabled, t.IntervalSec, t.OnFinding, t.OnGoalMet, t.OnTaskTimeout, t.OnToolCall, t.OnTaskCreate,
		t.IntervalMessage, t.FindingMessage, t.GoalMessage, t.TaskTimeoutMessage, t.ToolCallMessage, t.TaskCreateMessage, marshalToolNames(t.ToolNames))
	return err
}

// DeleteTrigger removes a trigger.
func (d *DB) DeleteTrigger(id int64) error {
	_, err := d.Exec(`DELETE FROM agent_triggers WHERE id = ?`, id)
	return err
}

// ListTriggersFor returns an agent's triggers.
func (d *DB) ListTriggersFor(agentKey string) ([]*AgentTrigger, error) {
	return d.queryTriggers(`SELECT `+triggerCols+` FROM agent_triggers WHERE agent_key = ? ORDER BY id`, agentKey)
}

// ListEnabledTriggers returns all enabled triggers (for the scheduler).
func (d *DB) ListEnabledTriggers() ([]*AgentTrigger, error) {
	// 布尔列显式 = 1：TiDB 的 sql_mode 不保证接受裸列当谓词，且 r4 审计口径统一要求显式比较。
	return d.queryTriggers(`SELECT ` + triggerCols + ` FROM agent_triggers WHERE enabled = 1 ORDER BY id`)
}

func (d *DB) queryTriggers(q string, args ...any) ([]*AgentTrigger, error) {
	rows, err := d.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*AgentTrigger{}
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// TouchTriggerFire records an interval trigger's fire time (now).
func (d *DB) TouchTriggerFire(id int64) error {
	_, err := d.Exec(`UPDATE agent_triggers SET last_fire=NOW(3) WHERE id = ?`, id)
	return err
}

// DeleteTriggersForAgent removes all triggers of an agent (custom agent delete).
func (d *DB) DeleteTriggersForAgent(agentKey string) error {
	_, err := d.Exec(`DELETE FROM agent_triggers WHERE agent_key = ?`, agentKey)
	return err
}

// ---------- scheduler_state (kv watermarks) ----------

// GetSchedState reads one scheduler watermark. `key` / `value` 都是 MySQL/TiDB
// 关键字，必须反引号（裸写 WHERE key=? 直接 1064）。
func (d *DB) GetSchedState(key string) (string, error) {
	var v string
	err := d.QueryRow("SELECT `value` FROM scheduler_state WHERE `key` = ?", key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// SetSchedState upserts one scheduler watermark (M13)。
//
// scheduler_state 在 TiDB 基线里是 hash 唯一键表：自然键 `key` 是 MEDIUMTEXT，TEXT 族
// 不能直接建唯一索引，唯一性完全由 uq_scheduler_state_key_hash(key_hash) 承载。因此
// 每条 INSERT 都必须同时写 key_identity（canonical 载荷）与 key_hash（其 SHA-256）：
// 漏写等于把唯一键退化成 NULL，重复水位再也挡不住。ON DUPLICATE KEY UPDATE 的冲突
// 判定就落在那条 hash 唯一索引上，更新分支只碰 `value`（身份不可变）。
//
// 身份载荷沿用 schema.SettingsKeyIdentity —— 该 builder 的契约就是「settings/tools/
// scheduler_state 共用的分支 key」，另起分支会让同一张表出现两套 identity 编码。
func (d *DB) SetSchedState(key, value string) error {
	identity, hash := canonicalIdentityPair(schema.SettingsKeyIdentity(key))
	_, err := d.Exec("INSERT INTO scheduler_state(`key`, key_identity, key_hash, `value`) VALUES (?, ?, ?, ?)"+
		" ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)", key, identity, hash, value)
	return err
}

// ---------- event queries (cross-exploration, for the scheduler) ----------

// TaskEvent is a finding/goal event carrying the owning task's info, used to
// compose the trigger message context.
type TaskEvent struct {
	NodeID     int64  `json:"node_id"`
	TaskID     int64  `json:"task_id"`
	TaskDesc   string `json:"task_description"`
	TaskGoal   string `json:"task_goal"`
	Summary    string `json:"summary"`     // finding summary / goal text
	VulnClass  string `json:"vulnclass"`   // finding only
	Severity   string `json:"severity"`    // finding only
	Tool       string `json:"tool"`        // tool-call only: tool name
	ToolInput  string `json:"tool_input"`  // tool-call only: 入参(JSON 文本)
	ToolOutput string `json:"tool_output"` // tool-call only: 返回内容
	ToolIsErr  bool   `json:"tool_is_err"` // tool-call only: 工具返回是否为错误
}

// NewFindingsSince returns findings with node id > lastID across all live tasks,
// ordered by id (monotonic watermark → no double-fire).
func (d *DB) NewFindingsSince(lastID int64) ([]TaskEvent, error) {
	rows, err := d.Query(`
SELECT n.id, t.id, t.description, t.goal, n.payload
FROM exploration_nodes n JOIN tasks t ON t.exploration_id = n.exploration_id
WHERE n.kind='finding' AND n.id > ? AND t.deleted_at IS NULL
ORDER BY n.id`, lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskEvent{}
	for rows.Next() {
		var e TaskEvent
		var payload []byte
		if err := rows.Scan(&e.NodeID, &e.TaskID, &e.TaskDesc, &e.TaskGoal, &payload); err != nil {
			return nil, err
		}
		var p struct{ Summary, Vulnclass, Severity string }
		_ = json.Unmarshal(payload, &p)
		e.Summary, e.VulnClass, e.Severity = p.Summary, p.Vulnclass, p.Severity
		out = append(out, e)
	}
	return out, rows.Err()
}

// TimedOutTasksSince returns tasks that reached status='timeout' with id > lastID,
// ordered by id (monotonic watermark → no double-fire across restarts).
func (d *DB) TimedOutTasksSince(lastID int64) ([]TaskEvent, error) {
	rows, err := d.Query(`
SELECT id, description, goal FROM tasks
WHERE status='timeout' AND deleted_at IS NULL AND id > ?
ORDER BY id`, lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskEvent{}
	for rows.Next() {
		var e TaskEvent
		if err := rows.Scan(&e.NodeID, &e.TaskDesc, &e.TaskGoal); err != nil {
			return nil, err
		}
		e.TaskID = e.NodeID // task id doubles as NodeID for the watermark
		e.Summary = e.TaskGoal
		out = append(out, e)
	}
	return out, rows.Err()
}

// NewTasksSince returns tasks created with id > lastID (excluding deleted),
// ordered by id (monotonic watermark → no double-fire across restarts). Triggered
// agent runs are conversations, not tasks, so this never fires on its own output.
func (d *DB) NewTasksSince(lastID int64) ([]TaskEvent, error) {
	rows, err := d.Query(`
SELECT id, description, goal FROM tasks
WHERE deleted_at IS NULL AND id > ?
ORDER BY id`, lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskEvent{}
	for rows.Next() {
		var e TaskEvent
		if err := rows.Scan(&e.NodeID, &e.TaskDesc, &e.TaskGoal); err != nil {
			return nil, err
		}
		e.TaskID = e.NodeID // task id doubles as NodeID for the watermark
		e.Summary = e.TaskGoal
		out = append(out, e)
	}
	return out, rows.Err()
}

// NewToolCallsSince returns completed tool calls (a tool_result row) with activity
// id > lastID across all live tasks, ordered by id (monotonic watermark → no
// double-fire). It is driven by tool_result rows (the tool finished, so both input
// and output are available) and joins back to the paired tool_use row for the input.
// Only task-execution activity is scanned — triggered agent runs are conversations
// (conversation_activities), so a tool-call trigger never fires on its own output.
func (d *DB) NewToolCallsSince(lastID int64) ([]TaskEvent, error) {
	rows, err := d.Query(`
SELECT r.id, t.id, t.description, t.goal, r.tool, COALESCE(u.detail,''), COALESCE(r.detail,''), r.is_error
FROM activity r
JOIN tasks t ON t.exploration_id = r.exploration_id
LEFT JOIN activity u ON u.exploration_id = r.exploration_id AND u.tool_use_id = r.tool_use_id AND u.kind='tool_use'
WHERE r.kind='tool_result' AND r.id > ? AND r.tool <> '' AND t.deleted_at IS NULL
ORDER BY r.id`, lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskEvent{}
	for rows.Next() {
		var e TaskEvent
		if err := rows.Scan(&e.NodeID, &e.TaskID, &e.TaskDesc, &e.TaskGoal, &e.Tool, &e.ToolInput, &e.ToolOutput, &e.ToolIsErr); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MetGoals returns all met goals across live tasks (the scheduler filters out the
// ones it already fired for via the persisted fired-set).
func (d *DB) MetGoals() ([]TaskEvent, error) {
	rows, err := d.Query(`
SELECT n.id, t.id, t.description, t.goal, n.payload
FROM exploration_nodes n JOIN tasks t ON t.exploration_id = n.exploration_id
WHERE n.kind='goal' AND n.state='met' AND t.deleted_at IS NULL
ORDER BY n.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskEvent{}
	for rows.Next() {
		var e TaskEvent
		var payload []byte
		if err := rows.Scan(&e.NodeID, &e.TaskID, &e.TaskDesc, &e.TaskGoal, &payload); err != nil {
			return nil, err
		}
		var p struct{ Text, Summary string }
		_ = json.Unmarshal(payload, &p)
		if p.Text != "" {
			e.Summary = p.Text
		} else {
			e.Summary = p.Summary
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
