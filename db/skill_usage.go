package db

import (
	"database/sql"
	"strings"
	"time"
)

// SkillUsage is one Skill() invocation — the always-on skill call ledger. Written
// from the Skill meta-tool's OnInvoke hook (server/assembly.go), one row per load.
// Carries only dimensions (which skill, which agent, which task/session), never the
// caller's args text — args_len is kept so an "empty vs substantial context" split
// is still possible without storing prompt content, mirroring llm_usage.
//
// Rows deliberately outlive their task: skill_usage has no foreign keys, so deleting
// a task keeps its skill statistics intact (same rationale as llm_usage).
type SkillUsage struct {
	Skill         string `json:"skill"`          // skill directory name (matches agent_skill_visibility.skill_name)
	AgentKey      string `json:"agent_key"`      // worker / planner / mainagent / custom agent key
	TaskID        int64  `json:"task_id"`        // 0 for non-task runs (chat sessions)
	ExplorationID int64  `json:"exploration_id"` // 0 when unknown
	IntentID      int64  `json:"intent_id"`      // worker's intent node; 0 for planner/mainagent/chat
	SessionID     string `json:"session_id"`     // chat conversation id; empty for task runs
	ArgsLen       int    `json:"args_len"`
	Found         bool   `json:"found"` // false = the model named a skill that does not exist
}

// InsertSkillUsage appends one ledger row. Best-effort: callers log and continue on
// error (a lost metering row must never break a skill invocation).
func (d *DB) InsertSkillUsage(u *SkillUsage) error {
	_, err := d.Exec(`
INSERT INTO skill_usage(skill, agent_key, task_id, exploration_id, intent_id, session_id, args_len, found)
VALUES (?, NULLIF(?,''), ?, ?, ?, NULLIF(?,''), ?, ?)`,
		u.Skill, u.AgentKey, nullIfZero(u.TaskID), nullIfZero(u.ExplorationID),
		nullIfZero(u.IntentID), u.SessionID, u.ArgsLen, u.Found)
	return err
}

func nullIfZero(v int64) any {
	if v > 0 {
		return v
	}
	return nil
}

// SkillStat is one skill's aggregate usage, for the skills page.
type SkillStat struct {
	Skill    string     `json:"skill"`
	Calls    int        `json:"calls"`
	Tasks    int        `json:"tasks"`     // distinct tasks that loaded it (chat runs excluded)
	Agents   []string   `json:"agents"`    // agent keys that loaded it, most-used first
	LastUsed *time.Time `json:"last_used"` // nil when never called
}

// SkillStats aggregates the whole ledger grouped by skill, most-used first. Skills
// that were never invoked are absent — callers merge against the skill list on disk.
// Only resolved calls count; misses are reported separately by MissingSkillStats.
func (d *DB) SkillStats() ([]SkillStat, error) {
	// agent keys come back as one comma-joined string rather than a real array:
	// 迁移前用 PG 的 STRING_AGG，TiDB 侧对应 GROUP_CONCAT(DISTINCT … SEPARATOR ',')。
	// 逗号连接之所以无歧义，是因为 agent key 的字符集是 [a-z0-9_-]，永远不含逗号；
	// 消费方（mergeAgentKeys）按逗号切分，格式必须保持不变。
	rows, err := d.Query(`
SELECT skill, COUNT(*) AS calls,
       COUNT(DISTINCT task_id) AS tasks,
       COALESCE(GROUP_CONCAT(DISTINCT agent_key SEPARATOR ','), '') AS agents,
       MAX(ts) AS last_used
FROM skill_usage
WHERE found = 1
GROUP BY skill
ORDER BY COUNT(*) DESC, skill`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SkillStat{}
	for rows.Next() {
		var s SkillStat
		var agents string
		var lastUsed sql.NullTime
		if err := rows.Scan(&s.Skill, &s.Calls, &s.Tasks, &agents, &lastUsed); err != nil {
			return nil, err
		}
		s.Agents = []string{}
		if agents != "" {
			s.Agents = strings.Split(agents, ",")
		}
		if lastUsed.Valid {
			t := lastUsed.Time
			s.LastUsed = &t
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	archived, err := d.archivedTaskAggregates()
	if err != nil {
		return nil, err
	}
	return mergeArchivedSkillStats(out, archived, false), nil
}

// MissingSkillStats returns the skill names agents asked for that do not exist,
// most-requested first — the "wished it existed" gap list. Names come from the model
// so they are shown as-is (already length-capped at insert time).
func (d *DB) MissingSkillStats(limit int) ([]SkillStat, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	rows, err := d.Query(`
SELECT skill, COUNT(*) AS calls,
       COALESCE(GROUP_CONCAT(DISTINCT agent_key SEPARATOR ','), '') AS agents,
       MAX(ts) AS last_used
FROM skill_usage
WHERE found = 0
GROUP BY skill
ORDER BY COUNT(*) DESC, skill`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SkillStat{}
	for rows.Next() {
		var s SkillStat
		var agents string
		var lastUsed sql.NullTime
		if err := rows.Scan(&s.Skill, &s.Calls, &agents, &lastUsed); err != nil {
			return nil, err
		}
		s.Agents = []string{}
		if agents != "" {
			s.Agents = strings.Split(agents, ",")
		}
		if lastUsed.Valid {
			t := lastUsed.Time
			s.LastUsed = &t
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	archived, err := d.archivedTaskAggregates()
	if err != nil {
		return nil, err
	}
	out = mergeArchivedSkillStats(out, archived, true)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// SkillCall is one row of a skill's recent-call list (detail panel).
//
// P03 R19：task_id 是 64 位任务 ID，对外 string（无任务时 SQL COALESCE 出的 0
// 变成 "0"，与其它 ID 字段的零值口径一致）。
type SkillCall struct {
	TS        time.Time `json:"ts"`
	AgentKey  string    `json:"agent_key"`
	TaskID    int64     `json:"task_id,string"`
	SessionID string    `json:"session_id"`
	ArgsLen   int       `json:"args_len"`
}

// RecentSkillCalls returns the most recent invocations of one skill, newest first.
func (d *DB) RecentSkillCalls(skill string, limit int) ([]SkillCall, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := d.Query(`
SELECT ts, COALESCE(agent_key,''), COALESCE(task_id,0), COALESCE(session_id,''), args_len
FROM skill_usage
WHERE skill = ? AND found = 1
ORDER BY ts DESC
LIMIT ?`, skill, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SkillCall{}
	for rows.Next() {
		var c SkillCall
		if err := rows.Scan(&c.TS, &c.AgentKey, &c.TaskID, &c.SessionID, &c.ArgsLen); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// SkillCallsByTask counts a task's skill loads, most-used first. Powers a per-task
// view of which procedures its agents actually reached for.
func (d *DB) SkillCallsByTask(taskID int64) ([]SkillStat, error) {
	rows, err := d.Query(`
SELECT skill, COUNT(*) AS calls, MAX(ts) AS last_used
FROM skill_usage
WHERE task_id = ? AND found = 1
GROUP BY skill
ORDER BY COUNT(*) DESC, skill`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SkillStat{}
	for rows.Next() {
		var s SkillStat
		var lastUsed sql.NullTime
		if err := rows.Scan(&s.Skill, &s.Calls, &lastUsed); err != nil {
			return nil, err
		}
		s.Agents = []string{}
		if lastUsed.Valid {
			t := lastUsed.Time
			s.LastUsed = &t
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
