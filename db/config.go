package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// ---------- canonical identity 写入辅助（M08）----------
//
// 基线（db/schema 65 表）把配置域所有自然键唯一约束都换成了 SHA-256 hash 唯一
// 索引：llm_profiles.name_hash、agents.key_hash、mcp_servers.name_hash、
// mcp_tools_cache/agent_visibility/agent_skill_visibility 的 identity_hash。
// 每一处写入都必须把 <col>_identity（canonical 载荷）与 <col>_hash（SHA-256）
// 成对补齐，否则唯一键退化为 NULL、重复行再也挡不住；身份列在更新分支里同样
// 要随自然键一起重算（自然键本身可改，改完之后身份必须自洽）。

func profileNameIdentity(name string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.LLMProfileNameIdentity(name))
}

func agentKeyIdentity(key string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.AgentKeyIdentity(key))
}

func mcpServerNameIdentity(name string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.MCPServerNameIdentity(name))
}

func mcpToolCacheIdentity(serverID int64, toolName string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.MCPToolCacheIdentity(uint64(serverID), toolName))
}

func skillVisibilityIdentity(agentID int64, skillName string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.AgentSkillVisibilityIdentity(uint64(agentID), skillName))
}

// agentVisibilityIdentity 是 agent_visibility 4 元组身份；本文件只写 mcp 资源，
// mcp_tool_name 取基线默认值 ""（与列默认严格一致，否则身份与库内行不自洽）。
func agentVisibilityIdentity(agentID int64, kind string, resourceID int64) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.AgentVisibilityIdentity(uint64(agentID), kind, uint64(resourceID), ""))
}

// ---------- 共享父行锁协议（M23 / 审查 F5）----------
//
// 基线不带外键，"先清子表、再删父表"只覆盖了顺序删除；并发下的保证必须来自**父子
// 两端共用的同一把行锁**：
//
//	写者　　：锁父行（FOR UPDATE）→ 校验父行存活 → 才允许写子表；
//	删除者：锁父行（FOR UPDATE）→ 清子表 → 删父行。
//
// 两侧都在碰任何子表行**之前**拿到父行锁，因此"父对象刚被删除、请求还在路上"这种
// 陈旧写入要么在锁上等待、要么醒来发现父行消失而整体失败，不会再留下指向不存在父行
// 的孤儿子行；固定"先父后子"的顺序也让删除者与写者之间形成不了锁序倒置。
var (
	ErrAgentNotFound     = errors.New("agent not found")
	ErrMCPServerNotFound = errors.New("MCP server not found")
)

// lockAgentForReference 锁定并校验 agent 父行。调用方必须已开启事务，且在它返回
// 之前不得写 agent_prompts / agent_prompt_vars / agent_visibility /
// agent_skill_visibility。
func lockAgentForReference(tx *sql.Tx, agentID int64) error {
	var id int64
	err := tx.QueryRow(`SELECT id FROM agents WHERE id=? FOR UPDATE`, agentID).Scan(&id)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: %d", ErrAgentNotFound, agentID)
	}
	return err
}

// lockMCPServerForReference 同上，父行是 mcp_servers：mcp_tools_cache 的写者
// （SaveMCPTools）与 agent_visibility 的 mcp 侧写者都必须先持有它。
func lockMCPServerForReference(tx *sql.Tx, serverID int64) error {
	var id int64
	err := tx.QueryRow(`SELECT id FROM mcp_servers WHERE id=? FOR UPDATE`, serverID).Scan(&id)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: %d", ErrMCPServerNotFound, serverID)
	}
	return err
}

// ---------- LLM profiles ----------

type LLMProfile struct {
	ID            int64   `json:"id,string"`
	Name          string  `json:"name"`
	Format        string  `json:"format"`
	BaseURL       string  `json:"base_url,omitempty"`
	Proxy         string  `json:"proxy,omitempty"` // LLM 出站代理(http/https/socks5);空=用环境变量
	Model         string  `json:"model"`
	APIKey        string  `json:"-"` // never serialized to UI
	APIKeyHint    string  `json:"api_key_hint,omitempty"`
	RatePerSecond float64 `json:"rate_per_second"`
	RatePerMinute float64 `json:"rate_per_minute"`
	// ContextWindowK is the model's context window in K tokens, used to size
	// compaction thresholds. 0 = use a 200K default; capped at 1000 (1M).
	ContextWindowK int `json:"context_window_k"`
	// ThinkingType 独立控制思考「开关」(thinking.type):"" = 不发送(默认);
	// "disabled" = 显式关闭; "enabled" = 开启. 与 ReasoningEffort 解耦.
	ThinkingType string `json:"thinking_type"`
	// ReasoningEffort 独立控制思考「强度」:"" = 不发送(默认);
	// "low"/"medium"/"high"/"xhigh"/"max" = 对应强度. 见 agent.Config.NewProvider.
	ReasoningEffort string `json:"reasoning_effort"`
	IsDefault       bool   `json:"is_default"`
	// Priority orders the failover chain: higher goes first. The ACTIVE profile
	// (IsDefault) always heads the chain regardless of this value.
	Priority int `json:"priority"`
	// PoolExclude=true keeps this profile out of the failover chain — it stays
	// usable when an agent/task binds it explicitly, it just never gets picked up
	// as a fallback target.
	PoolExclude bool `json:"pool_exclude"`
	// Streaming selects the wire protocol: true (default) = streaming (SSE);
	// false = real non-streaming (stream:false, single JSON response via
	// Provider.Complete). Non-streaming sidesteps flaky gateway SSE at the cost of
	// live in-run progress. Maps to agent.Config.Stream.
	Streaming bool `json:"streaming"`
	// MaxTokens caps a single reply's output in tokens. 0 = send no cap and let
	// the endpoint's own default apply (the historical behaviour). Unlike
	// ContextWindowK — the model's total capacity, used locally to size compaction
	// — this value travels with every request.
	MaxTokens int `json:"max_tokens"`
	// MaxTokensField picks the request key carrying MaxTokens, for format
	// "openai" only: "" = max_tokens (default); "max_completion_tokens" = the
	// newer key, which OpenAI's reasoning models require and whose budget covers
	// reasoning tokens plus visible output. Ignored by anthropic and
	// openai-responses, which name the field themselves.
	MaxTokensField string `json:"max_tokens_field"`
	// SessionHeaderKey, when non-empty, names a custom HTTP header sent on every
	// request built from this profile; its value is the current run's session id
	// (chat conversation / worker intent). For gateways that key prompt caching
	// or sticky routing off a session-id header. "" = not sent. Maps to
	// agent.Config.SessionHeaderKey.
	SessionHeaderKey string `json:"session_header_key"`
	// Retry overrides this profile's share of the retry ladder. Zero value =
	// inherit the global policy (LLMRetryPolicy), so an untouched profile behaves
	// exactly as before. See RetryOverride.
	Retry RetryOverride `json:"retry"`
}

// RetryOverride is one profile's optional override of the three retry layers
// that are per-endpoint: 建连(connect) / 空响应(empty) / 同 provider 安全窗口
// (stream). Each rule's zero value means "inherit the global policy"; see
// RetryRule for the -1 / 0 / >0 semantics.
type RetryOverride struct {
	Connect RetryRule `json:"connect"`
	Empty   RetryRule `json:"empty"`
	Stream  RetryRule `json:"stream"`
}

// profileCols is the read column list (hint variant, no api key) shared by the
// list query; profileColsKey is the same with api_key for the single-row loads.
const profileRetryCols = `COALESCE(retry_connect_attempts,0),COALESCE(retry_connect_interval_ms,0),COALESCE(retry_empty_attempts,0),COALESCE(retry_empty_interval_ms,0),COALESCE(retry_stream_attempts,0),COALESCE(retry_stream_interval_ms,0)`
const profileCols = `id,name,format,COALESCE(base_url,''),COALESCE(proxy,''),model,COALESCE(api_key_hint,''),rate_per_second,rate_per_minute,context_window_k,COALESCE(reasoning_effort,''),is_default,priority,pool_exclude,COALESCE(thinking_type,''),COALESCE(streaming,true),COALESCE(max_tokens,0),COALESCE(max_tokens_field,''),COALESCE(session_header_key,''),` + profileRetryCols
const profileColsKey = `id,name,format,COALESCE(base_url,''),COALESCE(proxy,''),model,COALESCE(api_key,''),rate_per_second,rate_per_minute,context_window_k,COALESCE(reasoning_effort,''),is_default,priority,pool_exclude,COALESCE(thinking_type,''),COALESCE(streaming,true),COALESCE(max_tokens,0),COALESCE(max_tokens_field,''),COALESCE(session_header_key,''),` + profileRetryCols

// scanProfile reads one row in the profileCols / profileColsKey column order. The
// 7th column lands in APIKeyHint or APIKey depending on which list the caller used.
func scanProfile(sc interface{ Scan(...any) error }, into *string, p *LLMProfile) error {
	return sc.Scan(&p.ID, &p.Name, &p.Format, &p.BaseURL, &p.Proxy, &p.Model, into,
		&p.RatePerSecond, &p.RatePerMinute, &p.ContextWindowK, &p.ReasoningEffort, &p.IsDefault, &p.Priority, &p.PoolExclude, &p.ThinkingType, &p.Streaming,
		&p.MaxTokens, &p.MaxTokensField, &p.SessionHeaderKey,
		&p.Retry.Connect.Attempts, &p.Retry.Connect.IntervalMS,
		&p.Retry.Empty.Attempts, &p.Retry.Empty.IntervalMS,
		&p.Retry.Stream.Attempts, &p.Retry.Stream.IntervalMS)
}

func (d *DB) ListProfiles() ([]*LLMProfile, error) {
	rows, err := d.Query(`SELECT ` + profileCols + ` FROM llm_profiles ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*LLMProfile
	for rows.Next() {
		var p LLMProfile
		if err := scanProfile(rows, &p.APIKeyHint, &p); err != nil {
			return nil, err
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

// ActiveProfile returns the default (active) profile with its api key, or nil.
func (d *DB) ActiveProfile() (*LLMProfile, error) {
	var p LLMProfile
	err := scanProfile(d.QueryRow(`SELECT `+profileColsKey+` FROM llm_profiles WHERE is_default LIMIT 1`), &p.APIKey, &p)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &p, err
}

// ProfileByID returns one profile with its api key by id, or nil if not found.
// Used to run a task on a specific (non-default) LLM profile.
func (d *DB) ProfileByID(id int64) (*LLMProfile, error) {
	var p LLMProfile
	err := scanProfile(d.QueryRow(`SELECT `+profileColsKey+` FROM llm_profiles WHERE id=?`, id), &p.APIKey, &p)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &p, err
}

// PoolProfiles returns the failover chain in run order, api keys included: the
// active profile first, then every other keyed profile that isn't excluded, by
// priority DESC (id ASC to stay stable). Profiles without an api key can't serve
// a request, so they never enter the chain. The ordering IS the policy — callers
// walk the slice front to back.
func (d *DB) PoolProfiles() ([]*LLMProfile, error) {
	rows, err := d.Query(`SELECT ` + profileColsKey + ` FROM llm_profiles
WHERE COALESCE(api_key,'') <> '' AND (is_default OR NOT pool_exclude)
ORDER BY is_default DESC, priority DESC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*LLMProfile
	for rows.Next() {
		var p LLMProfile
		if err := scanProfile(rows, &p.APIKey, &p); err != nil {
			return nil, err
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

// SaveProfile inserts (id==0) or updates a profile. Empty apiKey on update keeps existing.
func (d *DB) SaveProfile(p *LLMProfile) (int64, error) {
	hint := p.APIKeyHint
	if len(p.APIKey) >= 4 {
		hint = "…" + p.APIKey[len(p.APIKey)-4:]
	}
	r := p.Retry.Clamped()
	if p.ID == 0 {
		identity, hash := profileNameIdentity(p.Name)
		res, err := d.Exec(`INSERT INTO llm_profiles(name,name_identity,name_hash,format,base_url,proxy,model,api_key,api_key_hint,rate_per_second,rate_per_minute,context_window_k,reasoning_effort,priority,pool_exclude,thinking_type,streaming,max_tokens,max_tokens_field,session_header_key,retry_connect_attempts,retry_connect_interval_ms,retry_empty_attempts,retry_empty_interval_ms,retry_stream_attempts,retry_stream_interval_ms)
VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			p.Name, identity, hash, p.Format, p.BaseURL, p.Proxy, p.Model, p.APIKey, hint, p.RatePerSecond, p.RatePerMinute, p.ContextWindowK, p.ReasoningEffort, p.Priority, p.PoolExclude, p.ThinkingType, p.Streaming, p.MaxTokens, p.MaxTokensField, p.SessionHeaderKey,
			r.Connect.Attempts, r.Connect.IntervalMS, r.Empty.Attempts, r.Empty.IntervalMS, r.Stream.Attempts, r.Stream.IntervalMS)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	}
	identity, hash := profileNameIdentity(p.Name)
	if p.APIKey == "" {
		_, err := d.Exec(`UPDATE llm_profiles SET name=?,name_identity=?,name_hash=?,format=?,base_url=NULLIF(?,''),proxy=NULLIF(?,''),model=?,rate_per_second=?,rate_per_minute=?,context_window_k=?,reasoning_effort=?,priority=?,pool_exclude=?,thinking_type=?,streaming=?,max_tokens=?,max_tokens_field=?,session_header_key=?,retry_connect_attempts=?,retry_connect_interval_ms=?,retry_empty_attempts=?,retry_empty_interval_ms=?,retry_stream_attempts=?,retry_stream_interval_ms=? WHERE id=?`,
			p.Name, identity, hash, p.Format, p.BaseURL, p.Proxy, p.Model, p.RatePerSecond, p.RatePerMinute, p.ContextWindowK, p.ReasoningEffort, p.Priority, p.PoolExclude, p.ThinkingType, p.Streaming, p.MaxTokens, p.MaxTokensField, p.SessionHeaderKey,
			r.Connect.Attempts, r.Connect.IntervalMS, r.Empty.Attempts, r.Empty.IntervalMS, r.Stream.Attempts, r.Stream.IntervalMS, p.ID)
		return p.ID, err
	}
	_, err := d.Exec(`UPDATE llm_profiles SET name=?,name_identity=?,name_hash=?,format=?,base_url=NULLIF(?,''),proxy=NULLIF(?,''),model=?,api_key=?,api_key_hint=?,rate_per_second=?,rate_per_minute=?,context_window_k=?,reasoning_effort=?,priority=?,pool_exclude=?,thinking_type=?,streaming=?,max_tokens=?,max_tokens_field=?,session_header_key=?,retry_connect_attempts=?,retry_connect_interval_ms=?,retry_empty_attempts=?,retry_empty_interval_ms=?,retry_stream_attempts=?,retry_stream_interval_ms=? WHERE id=?`,
		p.Name, identity, hash, p.Format, p.BaseURL, p.Proxy, p.Model, p.APIKey, hint, p.RatePerSecond, p.RatePerMinute, p.ContextWindowK, p.ReasoningEffort, p.Priority, p.PoolExclude, p.ThinkingType, p.Streaming, p.MaxTokens, p.MaxTokensField, p.SessionHeaderKey,
		r.Connect.Attempts, r.Connect.IntervalMS, r.Empty.Attempts, r.Empty.IntervalMS, r.Stream.Attempts, r.Stream.IntervalMS, p.ID)
	return p.ID, err
}

var (
	ErrActiveLLMProfileDelete      = errors.New("cannot delete the active LLM profile; activate another profile first")
	ErrLLMProfileReferencesChanged = errors.New("LLM profile references changed while deleting; retry the request")
	ErrLLMProfileNotFound          = errors.New("LLM profile not found")
)

func (d *DB) DeleteProfile(id int64) error {
	return d.DeleteProfileContext(context.Background(), id)
}

// DeleteProfileContext removes a non-default profile while preserving the task
// failover cursor. Reference changes are retried because a task, agent, or
// conversation may start pointing at the profile between the initial scan and
// the profile row lock. A bound prevents a continuously changing workload from
// keeping an HTTP request alive forever.
func (d *DB) DeleteProfileContext(ctx context.Context, id int64) error {
	const (
		maxAttempts = 8
		maxDuration = 15 * time.Second
	)
	ctx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		retry, err := d.deleteProfile(ctx, id)
		if err != nil || !retry {
			return err
		}
	}
	return fmt.Errorf("%w: profile %d", ErrLLMProfileReferencesChanged, id)
}

// 三条"引用了本 profile 的行"查询。初扫（加锁）与 profile 锁到手后的漂移复查共用
// 它们：同一谓词、同一 ORDER BY、同一 FOR UPDATE，保证两次的锁集合可直接比较。
const (
	profileTaskRefsForUpdate = `SELECT t.id
FROM tasks t
WHERE t.llm_profile_id=?
   OR t.active_llm_profile_id=?
   OR EXISTS (
       SELECT 1 FROM task_llm_profiles x
       WHERE x.task_id=t.id AND x.profile_id=?
   )
ORDER BY t.id
FOR UPDATE`
	profileAgentRefsForUpdate = `SELECT id FROM agents
WHERE llm_profile_id=?
ORDER BY id
FOR UPDATE`
	profileConversationRefsForUpdate = `SELECT id FROM conversations
WHERE llm_profile_id=?
ORDER BY id
FOR UPDATE`
)

func (d *DB) deleteProfile(ctx context.Context, id int64) (bool, error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Reference setters lock their task/agent/conversation row before an
	// llm_profiles row can be locked by a foreign-key check. Keep deletion in the
	// same order: the old profile -> child order deadlocked with a setter's child
	// -> profile order. Sorting also gives concurrent profile deletions a stable
	// order when their chains overlap multiple tasks.
	//
	// TiDB 没有 `FOR UPDATE OF tbl`（PG 方言）：这里只 SELECT tasks 主键，加锁范围
	// 就是 t 本身；EXISTS 子查询走一致性读，不额外持有 task_llm_profiles 行锁。
	//
	// 三条查询与下面的"漂移复查"共用同一组常量：同一个谓词、同一个取锁顺序，
	// 差别只在读的时刻（见 profileReferenceDrifted）。
	lockedTasks, err := lockProfileReferenceRows(ctx, tx, profileTaskRefsForUpdate, id, id, id)
	if err != nil {
		return false, err
	}
	lockedAgents, err := lockProfileReferenceRows(ctx, tx, profileAgentRefsForUpdate, id)
	if err != nil {
		return false, err
	}
	lockedConversations, err := lockProfileReferenceRows(ctx, tx, profileConversationRefsForUpdate, id)
	if err != nil {
		return false, err
	}

	var isDefault bool
	if err := tx.QueryRowContext(ctx, `SELECT is_default FROM llm_profiles WHERE id=? FOR UPDATE`, id).Scan(&isDefault); err != nil {
		if err == sql.ErrNoRows {
			return false, ErrLLMProfileNotFound
		}
		return false, err
	}
	if isDefault {
		return false, ErrActiveLLMProfileDelete
	}

	type affectedTask struct {
		id        int64
		position  int
		wasActive bool
	}

	// 初扫之后、profile 锁到手之前，别的事务仍可能把某个 task/agent/conversation
	// 绑到本 profile 并提交。这条漂移必须**当前读**才看得见：TiDB 默认
	// REPEATABLE-READ，普通 SELECT 读的是事务起点的快照（官方
	// 《TiDB Pessimistic Transaction Mode / Behaviors》：只有 SELECT … FOR UPDATE
	// 与 DML 是 current read），用普通 SELECT 复查等于什么都没查。而函数后面的范围
	// DELETE/UPDATE 恰恰是当前读，会把这条新引用顺手清掉，却不会为它跑
	// successor/cursor/revision —— 任务游标就这样丢了（审查 F4 的第 3、4 步）。
	//
	// 复查因此复用与初扫完全相同的三条 FOR UPDATE 语句：读的是最新已提交集合，取锁
	// 顺序也仍是 task → agent → conversation。一旦发现当前集合里有任何一行不在初扫
	// 的锁集合中，就整体回滚重试，绝不带着"锁序外的新行"继续往下改（不做逆序补锁）。
	if drifted, err := profileReferenceDrifted(ctx, tx, id, lockedTasks, lockedAgents, lockedConversations); err != nil || drifted {
		return drifted, err
	}

	// 同样是当前读：受影响链和 active 游标都可能在我们等待 task 行锁期间被改写过，
	// 快照读会拿到"等待开始前"的旧游标位置，successor 就会选错（F4 的双连接场景）。
	// FOR UPDATE 在这里只补锁 task_llm_profiles 链行；tasks 行本来就在锁集合里。
	rows, err := tx.QueryContext(ctx, `SELECT x.task_id, x.position, COALESCE(t.active_llm_profile_id=?, FALSE)
FROM task_llm_profiles x
JOIN tasks t ON t.id=x.task_id
WHERE x.profile_id=?
ORDER BY x.task_id
FOR UPDATE`, id, id)
	if err != nil {
		return false, err
	}
	var affected []affectedTask
	for rows.Next() {
		var task affectedTask
		if err := rows.Scan(&task.id, &task.position, &task.wasActive); err != nil {
			rows.Close()
			return false, err
		}
		affected = append(affected, task)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM llm_profiles WHERE id=?`, id); err != nil {
		return false, err
	}
	// Only a successor after the deleted cursor is eligible. If there is none,
	// clear the explicit chain so the task falls back to its Agent/global provider;
	// profiles before a manually selected cursor must never be revived.
	for _, task := range affected {
		if !task.wasActive {
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET llm_chain_revision=llm_chain_revision+1 WHERE id=?`, task.id); err != nil {
				return false, err
			}
			continue
		}
		var next int64
		// `position` 是 MySQL 保留字（POSITION()），列名一律反引号。
		// FOR UPDATE：当前读 + 锁住候选 successor 行，避免"等锁期间链被改写"后
		// 把游标切到一个已经不存在 / 已经不是 ready 的位置。
		err := tx.QueryRowContext(ctx, `SELECT profile_id FROM task_llm_profiles
WHERE task_id=? AND `+"`position`"+`>? AND status='ready'
ORDER BY `+"`position`"+`
LIMIT 1 FOR UPDATE`, task.id, task.position).Scan(&next)
		if err != nil && err != sql.ErrNoRows {
			return false, err
		}
		if err == sql.ErrNoRows {
			if _, err := tx.ExecContext(ctx, `DELETE FROM task_llm_profiles WHERE task_id=?`, task.id); err != nil {
				return false, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE tasks
SET active_llm_profile_id=NULL, llm_profile_id=NULL, llm_chain_revision=llm_chain_revision+1
WHERE id=?`, task.id); err != nil {
				return false, err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks
SET active_llm_profile_id=?, llm_profile_id=?, llm_chain_revision=llm_chain_revision+1
WHERE id=?`, next, next, task.id); err != nil {
			return false, err
		}
	}
	// 基线里没有外键（P02 目录红线：无 FK），旧库靠 ON DELETE SET NULL / CASCADE
	// 自动收拾的引用现在必须显式清理，否则 agent / conversation / tasks /
	// task_llm_profiles / llm_profile_health 会指向一个已被删除的 profile。
	// 引用行在事务开头已按 task→agent→conversation→profile 的顺序加锁，这里不再
	// 触碰新的行集合，锁序保持不变。
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_llm_profiles WHERE profile_id=?`, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM llm_profile_health WHERE profile_id=?`, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agents SET llm_profile_id=NULL WHERE llm_profile_id=?`, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversations SET llm_profile_id=NULL WHERE llm_profile_id=?`, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET llm_profile_id=NULL WHERE llm_profile_id=?`, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET active_llm_profile_id=NULL WHERE active_llm_profile_id=?`, id); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

func lockProfileReferenceRows(ctx context.Context, tx *sql.Tx, query string, args ...any) (map[int64]struct{}, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	locked := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		locked[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return locked, nil
}

// profileReferenceDrifted 在 profile 行锁到手之后，用**当前读**（FOR UPDATE）重取
// 三类引用行，并检查它们是否全部落在初扫已经锁住的集合里。返回 true 表示引用集合
// 已经漂移：调用方必须整体回滚重试，不能继续往下改，也不能就地补锁后接着走。
//
// 为什么必须加锁读：TiDB 默认 REPEATABLE-READ，普通 SELECT 是事务起点快照读，看不见
// 初扫之后提交的绑定；而删除/置空的 DML 是当前读，两者一起用就会"漏判新引用、却把
// 它的链删掉"。锁等待 / 死锁说明集合正被并发写者改动，同样按漂移处理，交给
// DeleteProfileContext 的有界重试环。
func profileReferenceDrifted(ctx context.Context, tx *sql.Tx, profileID int64, tasks, agents, conversations map[int64]struct{}) (bool, error) {
	// 顺序保持 task → agent → conversation，与初扫一致，不额外触碰其它表。
	for _, scan := range []struct {
		locked map[int64]struct{}
		query  string
		args   []any
	}{
		{tasks, profileTaskRefsForUpdate, []any{profileID, profileID, profileID}},
		{agents, profileAgentRefsForUpdate, []any{profileID}},
		{conversations, profileConversationRefsForUpdate, []any{profileID}},
	} {
		current, err := lockProfileReferenceRows(ctx, tx, scan.query, scan.args...)
		if err != nil {
			if tidb.IsRetryable(err) {
				return true, nil
			}
			return false, err
		}
		for rowID := range current {
			if _, ok := scan.locked[rowID]; !ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// SetActiveProfile makes one profile the global default (single-default invariant).
func (d *DB) SetActiveProfile(id int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE llm_profiles SET is_default=false WHERE is_default`); err != nil {
		return err
	}
	res, err := tx.Exec(`UPDATE llm_profiles SET is_default=true WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrLLMProfileNotFound
	}
	return tx.Commit()
}

// ---------- Agents / prompts ----------

type Agent struct {
	ID               int64  `json:"id"`
	Key              string `json:"key"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	Role             string `json:"role"`
	Builtin          bool   `json:"builtin"`
	Enabled          bool   `json:"enabled"`
	LLMProfileID     *int64 `json:"llm_profile_id"`    // 绑定的 LLM 配置;nil=跟随任务/会话 pin,再回退全局激活
	MaxTurns         int    `json:"max_turns"`         // 单次运行最大轮次;0=不限制
	RunSecs          int    `json:"run_seconds"`       // worker 单次运行墙钟上限(秒);0=不限制
	WebSearch        bool   `json:"web_search"`        // 是否启用网络搜索(受系统全局开关门控)
	InteractiveShell bool   `json:"interactive_shell"` // 是否启用交互式 shell(持久 PTY 会话工具族)
	WrapupPrompt     string `json:"wrapup_prompt"`     // 收尾提示词(超时/步数耗尽时的 settlement 提示);空=用代码内置默认
	WrapupMaxTurns   int    `json:"wrapup_max_turns"`  // 收尾阶段自身的轮数预算;0=用代码内置默认(按 agent)
	// 任务级超时收尾词(与 per-run 两套;仅 worker/planner 用);空/0=用代码内置默认。
	TaskTimeoutWrapupPrompt   string `json:"task_timeout_wrapup_prompt"`
	TaskTimeoutWrapupMaxTurns int    `json:"task_timeout_wrapup_max_turns"`
	// P3 触发后处理策略(仅自定义 agent 有意义):
	// TriggerRunMode  serial|parallel — 串行排队 / 每次触发各自并发一个会话
	// TriggerMergeMode by_task|all|none — 仅 serial 用:同任务合并 / 全部合并 / 不合并
	// TriggerMaxParallel — 仅 parallel 用的每 agent 并发上限;0=不限
	TriggerRunMode     string `json:"trigger_run_mode"`
	TriggerMergeMode   string `json:"trigger_merge_mode"`
	TriggerMaxParallel int    `json:"trigger_max_parallel"`
}

// agentCols 是 agents 表的共享读取列清单（ListAgents / GetAgentByKey /
// CreateAgent 回读都走它）。
//
// `key` 是 MySQL/TiDB 保留字：WHERE 与 INSERT 侧一直都有反引号，唯独清单里的裸
// `key` 会生成 `SELECT id,key,name,…`，在 TiDB 上是语法错误——列表/单条查询直接
// 报错，CreateAgent 更是 INSERT 已提交、回读失败（行已存在却返回错误）。清单是
// 唯一的拼串入口，所以转义只在这里做一次。
const agentCols = "id,`key`,name," + `COALESCE(description,''),role,builtin,enabled,COALESCE(max_turns,0),COALESCE(run_seconds,600),COALESCE(web_search,false),COALESCE(interactive_shell,false),COALESCE(wrapup_prompt,''),COALESCE(wrapup_max_turns,0),COALESCE(task_timeout_wrapup_prompt,''),COALESCE(task_timeout_wrapup_max_turns,0),COALESCE(trigger_run_mode,'serial'),COALESCE(trigger_merge_mode,'all'),COALESCE(trigger_max_parallel,5),llm_profile_id`

func scanAgent(sc interface{ Scan(...any) error }) (*Agent, error) {
	var a Agent
	var prof sql.NullInt64 // llm_profile_id 可空:未绑定时为 NULL
	err := sc.Scan(&a.ID, &a.Key, &a.Name, &a.Description, &a.Role, &a.Builtin, &a.Enabled, &a.MaxTurns, &a.RunSecs, &a.WebSearch, &a.InteractiveShell, &a.WrapupPrompt, &a.WrapupMaxTurns, &a.TaskTimeoutWrapupPrompt, &a.TaskTimeoutWrapupMaxTurns, &a.TriggerRunMode, &a.TriggerMergeMode, &a.TriggerMaxParallel, &prof)
	if err == nil && prof.Valid {
		v := prof.Int64
		a.LLMProfileID = &v
	}
	return &a, err
}

func (d *DB) ListAgents() ([]*Agent, error) {
	rows, err := d.Query(`SELECT ` + agentCols + ` FROM agents ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Agent
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (d *DB) GetAgentByKey(key string) (*Agent, error) {
	a, err := scanAgent(d.QueryRow("SELECT "+agentCols+" FROM agents WHERE `key`=?", key))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return a, nil
}

// AgentBindingCounts returns per-agent binding counts in a few grouped queries
// (NO N+1): visible MCP servers and skills keyed by agent id, and bound tools
// keyed by agent key (tools.agents is a JSON array of agent keys). Missing keys
// mean zero. Used to show "MCP N · Skill N · 工具 N" on the agent cards.
//
// 工具计数不能在 SQL 里展开 JSON 数组（TiDB 8.5 不支持 JSON_TABLE，见下面的注释），
// 所以 tools 侧是"一条全表查询 + Go 内存聚合"，仍然只有三条 SQL，没有 N+1。
func (d *DB) AgentBindingCounts() (mcp map[int64]int, skill map[int64]int, tools map[string]int, err error) {
	mcp, skill, tools = map[int64]int{}, map[int64]int{}, map[string]int{}
	byID := func(q string, into map[int64]int) error {
		rows, e := d.Query(q)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var n int
			if e := rows.Scan(&id, &n); e != nil {
				return e
			}
			into[id] = n
		}
		return rows.Err()
	}
	if err = byID(`SELECT agent_id, count(DISTINCT resource_id) FROM agent_visibility WHERE resource_kind='mcp' AND enabled GROUP BY agent_id`, mcp); err != nil {
		return
	}
	if err = byID(`SELECT agent_id, count(*) FROM agent_skill_visibility WHERE enabled GROUP BY agent_id`, skill); err != nil {
		return
	}
	// tools.agents 是 JSON 数组（原 PG JSONB）。TiDB 8.5 明确**不支持** JSON_TABLE()
	// ——官方 release-8.5《JSON Functions》把它列在 Unsupported functions 里，
	// 用它只会得到 "Unsupported function JSON_TABLE"。这里只把数组原样读回来，
	// 展开与计数放到 Go 内存里做，语义与原来的 JSON_TABLE + GROUP BY 一致：
	// 数组里每个元素各计一次（同一行内的重复元素重复计数），NULL 与空数组不贡献
	// 计数，没出现的 agent key 仍按"缺失即 0"处理。
	rows, e := d.Query(`SELECT agents FROM tools WHERE agents IS NOT NULL`)
	if e != nil {
		err = e
		return
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if e := rows.Scan(&raw); e != nil {
			err = e
			return
		}
		var keys []string
		if e := json.Unmarshal(raw, &keys); e != nil {
			// 显式报错而不是静默跳过：调用方是 best-effort overlay，静默只会把
			// "统计坏了"伪装成"这个 agent 一个工具都没绑"。
			err = fmt.Errorf("decode tools.agents JSON array: %w", e)
			return
		}
		for _, k := range keys {
			tools[k]++
		}
	}
	err = rows.Err()
	return
}

// CreateAgent inserts a custom (builtin=false) conversational agent with role
// 'assistant'. Returns the new row. Callers validate key/name upstream; the DB
// enforces key uniqueness through uq_agents_key_hash.
//
// 四个 MEDIUMTEXT NOT NULL 列（wrapup_prompt/task_timeout_wrapup_prompt/
// trigger_run_mode/trigger_merge_mode）没有 DDL 默认值，必须显式写入应用层默认值。
func (d *DB) CreateAgent(key, name, description string) (*Agent, error) {
	identity, hash := agentKeyIdentity(key)
	res, err := d.Exec(`
INSERT INTO agents(`+"`key`"+`, key_identity, key_hash, name, description, role, builtin, enabled,
                           wrapup_prompt, task_timeout_wrapup_prompt, trigger_run_mode, trigger_merge_mode)
VALUES (?, ?, ?, ?, NULLIF(?,''), 'assistant', FALSE, TRUE, '', '', 'serial', 'all')`, key, identity, hash, name, description)
	if err != nil {
		return nil, err
	}
	// 原生 TiDB 没有 INSERT … RETURNING：自增 id 走 LastInsertId，再按 id 回读整行。
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	a, err := scanAgent(d.QueryRow(`SELECT `+agentCols+` FROM agents WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return a, nil
}

// UpdateAgentMeta updates a custom agent's display name + description. Built-in
// agents are left untouched (guarded by the caller / the builtin flag).
func (d *DB) UpdateAgentMeta(key, name, description string) error {
	_, err := d.Exec(`UPDATE agents SET name=?, description=NULLIF(?,'') WHERE `+"`key`"+`=? AND builtin=FALSE`, name, description, key)
	return err
}

// DeleteAgent removes a custom agent. Built-in agents are protected by the
// builtin=false guard.
//
// 旧库里 agent_prompts / agent_prompt_vars / agent_visibility /
// agent_skill_visibility 靠 ON DELETE CASCADE 随父行消失；基线不再带外键，
// 这些子行必须在同一事务里显式删除，否则会留下指向不存在 agent 的孤儿。
//
// 并发协议（F5）：父行用 FOR UPDATE 锁定后才清子表。SavePrompt /
// SetAgentSkillVisibility / SetAgentVisibilityKind 等子写者都在各自事务里先锁同一条
// agents 行再写子表，所以"删除已提交、陈旧写入才到"的窗口被这把锁关掉——写者醒来
// 发现父行消失（ErrAgentNotFound）整体失败，不会插回孤儿。
func (d *DB) DeleteAgent(key string) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 父锁必须先于任何子表 DELETE：SavePrompt / SetAgentSkillVisibility /
	// SetAgentVisibilityKind 这些子写者都在各自事务里先锁同一行 agents 再写子表，
	// 于是"删除已完成、陈旧写入还在路上"的窗口被这把锁关掉（写者要么排队等我们提交，
	// 醒来发现父行消失而整体失败）。
	var agentID int64
	err = tx.QueryRow(`SELECT id FROM agents WHERE `+"`key`"+`=? AND builtin=FALSE FOR UPDATE`, key).Scan(&agentID)
	if err == sql.ErrNoRows {
		// 内置 agent 或未知 key：保持旧的 "DELETE 影响 0 行" 语义。
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	for _, q := range []string{
		`DELETE FROM agent_prompt_vars WHERE agent_id=?`,
		`DELETE FROM agent_skill_visibility WHERE agent_id=?`,
		`DELETE FROM agent_visibility WHERE agent_id=?`,
		`DELETE FROM agent_prompts WHERE agent_id=?`,
	} {
		if _, err := tx.Exec(q, agentID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM agents WHERE id=?`, agentID); err != nil {
		return err
	}
	return tx.Commit()
}

// SetAgentMaxTurns updates an agent's max_turns (0 = unlimited).
func (d *DB) SetAgentMaxTurns(key string, maxTurns int) error {
	if maxTurns < 0 {
		maxTurns = 0
	}
	_, err := d.Exec(`UPDATE agents SET max_turns=? WHERE `+"`key`"+`=?`, maxTurns, key)
	return err
}

// SetAgentLLMProfile binds an agent to a specific LLM profile (id != nil), or clears
// the binding (id == nil) so the agent follows the task/conversation pin, else the
// global active profile. Precedence at runtime: agent binding → task/conv pin → active.
func (d *DB) SetAgentLLMProfile(key string, id *int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// DeleteProfile locks reference rows before the profile row. Keep the same
	// order here so a concurrent rebind cannot form a child/profile deadlock.
	var agentID int64
	if err := tx.QueryRow(`SELECT id FROM agents WHERE `+"`key`"+`=? FOR UPDATE`, key).Scan(&agentID); err != nil {
		if err == sql.ErrNoRows {
			// Preserve the previous UPDATE semantics: an unknown key is a no-op.
			return tx.Commit()
		}
		return err
	}
	if err := lockLLMProfileForReference(tx, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE agents SET llm_profile_id=? WHERE id=?`, id, agentID); err != nil {
		return err
	}
	return tx.Commit()
}

// lockLLMProfileForReference makes the profile side of the shared lock-order
// protocol explicit. Callers must already own the referencing child row.
//
// 为什么是 FOR UPDATE 而不是 FOR SHARE：TiDB 默认压根没有"共享行锁"这一层——
// 官方 release-8.5《TiDB System Variables》里 tidb_enable_shared_lock_promotion
// 默认 OFF，打开是把共享锁**升级成排他锁**；另一个 noop 兼容开关则是直接把锁忽略
// 成不加锁。也就是说 FOR SHARE 既不提供"与 FOR UPDATE 互斥、又允许并发绑定"的
// 契约，也不是可验收的等价改写（R4 审计 K 项同样禁止这么糊过去）。绑定侧因此统一
// 取 TiDB 真正支持的 FOR UPDATE：与 DeleteProfile 的 profile 行锁天然互斥，引用
// 集合在删除事务持锁期间不可能再增长。代价是"并发绑定同一个 profile"会串行化——
// 这是配置页的人工写入频率，换来的是删除/绑定互斥这件事有数据库保证。
func lockLLMProfileForReference(tx *sql.Tx, profileID *int64) error {
	if profileID == nil {
		return nil
	}
	var lockedID int64
	if err := tx.QueryRow(`SELECT id FROM llm_profiles WHERE id=? FOR UPDATE`, *profileID).Scan(&lockedID); err != nil {
		if err == sql.ErrNoRows {
			return ErrLLMProfileNotFound
		}
		return err
	}
	return nil
}

// SetAgentWebSearch toggles whether an agent uses network search (still gated by
// the global web-search master switch + backend/key config).
func (d *DB) SetAgentWebSearch(key string, on bool) error {
	_, err := d.Exec("UPDATE agents SET web_search=? WHERE `key`=?", on, key)
	return err
}

// SetAgentInteractiveShell toggles whether an agent gets the interactive shell
// (持久 PTY 会话) tool family + Bash 提示词联动(见 docs/交互式shell设计.md §14.2).
func (d *DB) SetAgentInteractiveShell(key string, on bool) error {
	_, err := d.Exec("UPDATE agents SET interactive_shell=? WHERE `key`=?", on, key)
	return err
}

// SetAgentWrapupPrompt stores an agent's wrap-up (settlement) prompt. Empty string
// means "use the code built-in default" — resolved at runtime by resolveWrapup.
func (d *DB) SetAgentWrapupPrompt(key, prompt string) error {
	_, err := d.Exec("UPDATE agents SET wrapup_prompt=? WHERE `key`=?", prompt, key)
	return err
}

// SetAgentWrapupMaxTurns stores the wrap-up phase's own turn budget. 0 means "use
// the code built-in default" — resolved at runtime by resolveWrapupTurns.
func (d *DB) SetAgentWrapupMaxTurns(key string, n int) error {
	_, err := d.Exec("UPDATE agents SET wrapup_max_turns=? WHERE `key`=?", n, key)
	return err
}

// SetAgentTaskTimeoutWrapup stores an agent's task-timeout wrap-up prompt (empty =
// use code built-in default; only worker/planner have one) and its turn budget
// (0 = default). Resolved at runtime by resolveTaskTimeoutWrapup / …Turns.
func (d *DB) SetAgentTaskTimeoutWrapup(key, prompt string, maxTurns int) error {
	_, err := d.Exec("UPDATE agents SET task_timeout_wrapup_prompt=?, task_timeout_wrapup_max_turns=? WHERE `key`=?", prompt, maxTurns, key)
	return err
}

// SetAgentRunSeconds updates an agent's run_seconds wall-clock budget (0 = unlimited).
func (d *DB) SetAgentRunSeconds(key string, runSecs int) error {
	if runSecs < 0 {
		runSecs = 0
	}
	_, err := d.Exec("UPDATE agents SET run_seconds=? WHERE `key`=?", runSecs, key)
	return err
}

// SetAgentTriggerBehavior stores an agent's P3 trigger post-processing策略:
// runMode(serial|parallel) / mergeMode(by_task|all|none) / maxParallel(parallel 用,0=不限)。
// 枚举做白名单校验,非法值回落默认,避免脏数据把调度 pump 带偏。
func (d *DB) SetAgentTriggerBehavior(key, runMode, mergeMode string, maxParallel int) error {
	switch runMode {
	case "serial", "parallel":
	default:
		runMode = "serial"
	}
	switch mergeMode {
	case "by_task", "all", "none":
	default:
		mergeMode = "by_task"
	}
	if maxParallel < 0 {
		maxParallel = 0
	}
	_, err := d.Exec("UPDATE agents SET trigger_run_mode=?, trigger_merge_mode=?, trigger_max_parallel=? WHERE `key`=?",
		runMode, mergeMode, maxParallel, key)
	return err
}

type PromptVar struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Example     string `json:"example"`
	Source      string `json:"source"`
}

func (d *DB) PromptVars(agentID int64) ([]PromptVar, error) {
	rows, err := d.Query(`SELECT var_name,COALESCE(description,''),COALESCE(example,''),source FROM agent_prompt_vars WHERE agent_id=? ORDER BY var_name`, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PromptVar{}
	for rows.Next() {
		var v PromptVar
		if err := rows.Scan(&v.Name, &v.Description, &v.Example, &v.Source); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// CurrentPrompt returns the agent's active template text ("" if none set yet).
func (d *DB) CurrentPrompt(agentID int64) (string, error) {
	var tmpl sql.NullString
	err := d.QueryRow(`SELECT p.template_text FROM agents a JOIN agent_prompts p ON p.id=a.current_prompt_id WHERE a.id=?`, agentID).Scan(&tmpl)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return tmpl.String, err
}

// SeedPromptIfEmpty writes the code-default template as the agent's first prompt
// version ONLY when it has none yet (current_prompt_id IS NULL). Mirrors
// SeedTool's first-insert-only philosophy: a user's edited prompt is never
// clobbered on restart. Idempotent — a no-op once any version exists.
func (d *DB) SeedPromptIfEmpty(agentID int64, tmpl string) error {
	var cur sql.NullInt64
	if err := d.QueryRow(`SELECT current_prompt_id FROM agents WHERE id=?`, agentID).Scan(&cur); err != nil {
		return err
	}
	if cur.Valid {
		return nil // already seeded or user-edited → leave it
	}
	_, err := d.SavePrompt(agentID, tmpl, "内置默认", "system")
	return err
}

// ResetPromptToDefault appends the code-default template as a new version and
// points current at it — the explicit "恢复为内置默认" action.
func (d *DB) ResetPromptToDefault(agentID int64, tmpl string) (int, error) {
	return d.SavePrompt(agentID, tmpl, "恢复为内置默认", "system")
}

// SavePrompt appends a new version and points current_prompt_id at it.
//
// 先锁 agent 父行并校验存活（F5）：DeleteAgent 走同一把锁，父行已消失时这里返回
// ErrAgentNotFound，不会留下指向不存在 agent 的 agent_prompts 行。
func (d *DB) SavePrompt(agentID int64, template, note, by string) (int, error) {
	tx, err := d.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := lockAgentForReference(tx, agentID); err != nil {
		return 0, err
	}
	var ver int
	if err := tx.QueryRow(`SELECT COALESCE(max(version),0)+1 FROM agent_prompts WHERE agent_id=?`, agentID).Scan(&ver); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT INTO agent_prompts(agent_id,version,template_text,note,updated_by) VALUES (?,?,?,NULLIF(?,''),NULLIF(?,''))`,
		agentID, ver, template, note, by)
	if err != nil {
		return 0, err
	}
	pid, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`UPDATE agents SET current_prompt_id=? WHERE id=?`, pid, agentID); err != nil {
		return 0, err
	}
	return ver, tx.Commit()
}

type PromptVersion struct {
	Version   int       `json:"version"`
	Template  string    `json:"template_text"`
	Note      string    `json:"note,omitempty"`
	CreatedAt time.Time `json:"ts"`
}

func (d *DB) ListPromptVersions(agentID int64) ([]PromptVersion, error) {
	rows, err := d.Query(`SELECT version,template_text,COALESCE(note,''),created_at FROM agent_prompts WHERE agent_id=? ORDER BY version DESC`, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PromptVersion{}
	for rows.Next() {
		var v PromptVersion
		if err := rows.Scan(&v.Version, &v.Template, &v.Note, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ---------- MCP servers ----------

// MCPServer 是 mcp_servers 行。
//
// P03 R26：id 是 64 位实体 ID，对外 string。本类型不是请求体解码目标，也不是持久化
// JSON 回读目标（SaveMCP / ListMCP 全走 SQL 列）；POST /api/mcp 的请求体由
// server.saveMCPRequest 承载，它以外层 ID json.RawMessage 屏蔽本字段（浅层优先），
// 因此加 ,string 后请求解码仍不受影响（有离线回归测试锁定）。
type MCPServer struct {
	ID        int64           `json:"id,string"`
	Name      string          `json:"name"`
	Transport string          `json:"transport"`
	Command   string          `json:"command,omitempty"`
	Args      json.RawMessage `json:"args"`
	Env       json.RawMessage `json:"env"`
	URL       string          `json:"url,omitempty"`
	Enabled   bool            `json:"enabled"`
	Insecure  bool            `json:"insecure"`        // http: skip TLS cert verification (self-signed servers, issue #108)
	Tools     []string        `json:"tools,omitempty"` // cached tool names (mcp_tools_cache)
}

func (d *DB) ListMCP() ([]*MCPServer, error) {
	rows, err := d.Query(`SELECT id,name,transport,COALESCE(command,''),args,env,COALESCE(url,''),enabled,insecure FROM mcp_servers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var out []*MCPServer
	for rows.Next() {
		var m MCPServer
		var args, env []byte
		if err := rows.Scan(&m.ID, &m.Name, &m.Transport, &m.Command, &args, &env, &m.URL, &m.Enabled, &m.Insecure); err != nil {
			rows.Close()
			return nil, err
		}
		m.Args, m.Env = json.RawMessage(args), json.RawMessage(env)
		out = append(out, &m)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close() // free the connection before the per-server tool-cache queries below
	// Attach each server's cached tool names (best-effort; empty until discovered).
	for _, m := range out {
		m.Tools, _ = d.MCPToolNames(m.ID)
	}
	return out, nil
}

// MCPToolNames returns the cached tool names for a server (empty until discovered).
func (d *DB) MCPToolNames(serverID int64) ([]string, error) {
	rows, err := d.Query(`SELECT tool_name FROM mcp_tools_cache WHERE server_id=? ORDER BY tool_name`, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// MCPTool is one cached tool of an MCP server (name + description).
type MCPTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// MCPToolsDetailed returns the cached tools (name + description) for a server.
func (d *DB) MCPToolsDetailed(serverID int64) ([]MCPTool, error) {
	rows, err := d.Query(`SELECT tool_name, COALESCE(description,'') FROM mcp_tools_cache WHERE server_id=? ORDER BY tool_name`, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MCPTool
	for rows.Next() {
		var t MCPTool
		if err := rows.Scan(&t.Name, &t.Description); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// SaveMCPTools replaces the cached tool list for a server (called after discovery).
//
// MCP 发现是异步的：用户完全可能在 discovery 进行中途删掉 server。子写者必须先持有
// 父行锁并校验存活，否则 DeleteMCP 提交后这里仍会插出一份挂在已删除 server 上的缓存
// （基线没有外键兜底）。父行已消失 → ErrMCPServerNotFound，调用方（发现流程）记日志
// 丢弃这次结果即可。
func (d *DB) SaveMCPTools(serverID int64, tools []MCPTool) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockMCPServerForReference(tx, serverID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM mcp_tools_cache WHERE server_id=?`, serverID); err != nil {
		return err
	}
	for _, t := range tools {
		identity, hash := mcpToolCacheIdentity(serverID, t.Name)
		if _, err := tx.Exec(`INSERT INTO mcp_tools_cache(server_id, tool_name, identity, identity_hash, description)
VALUES (?,?,?,?,?)
ON DUPLICATE KEY UPDATE description=VALUES(description)`, serverID, t.Name, identity, hash, t.Description); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) SaveMCP(m *MCPServer) (int64, error) {
	args, env := string(m.Args), string(m.Env)
	if args == "" {
		args = "[]"
	}
	if env == "" {
		env = "{}"
	}
	identity, hash := mcpServerNameIdentity(m.Name)
	if m.ID == 0 {
		res, err := d.Exec(`INSERT INTO mcp_servers(name,name_identity,name_hash,transport,command,args,env,url,enabled,insecure) VALUES (?,?,?,?,NULLIF(?,''),?,?,NULLIF(?,''),?,?)`,
			m.Name, identity, hash, m.Transport, m.Command, args, env, m.URL, m.Enabled, m.Insecure)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	}
	_, err := d.Exec(`UPDATE mcp_servers SET name=?,name_identity=?,name_hash=?,transport=?,command=NULLIF(?,''),args=?,env=?,url=NULLIF(?,''),enabled=?,insecure=? WHERE id=?`,
		m.Name, identity, hash, m.Transport, m.Command, args, env, m.URL, m.Enabled, m.Insecure, m.ID)
	return m.ID, err
}

// DeleteMCP removes a server together with its cached tools and every
// visibility row pointing at it.
//
// mcp_tools_cache 在旧库里靠 ON DELETE CASCADE 随父行消失；基线没有外键，
// 这里必须显式删掉，否则缓存会挂在一条已经不存在的 server 上。
//
// 并发协议（F5）：先锁 mcp_servers 父行，再清子表。SaveMCPTools 与 agent_visibility
// 的 mcp 侧写者走的是同一把父行锁，所以"清完子表 → 删父行"这中间不可能有陈旧写入
// 把缓存重新插回来。父行本就不存在时保持旧的幂等语义（等价 DELETE 影响 0 行）。
func (d *DB) DeleteMCP(id int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var locked int64
	switch err := tx.QueryRow(`SELECT id FROM mcp_servers WHERE id=? FOR UPDATE`, id).Scan(&locked); err {
	case nil:
	case sql.ErrNoRows:
		return tx.Commit()
	default:
		return err
	}
	if _, err := tx.Exec(`DELETE FROM mcp_tools_cache WHERE server_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM agent_visibility WHERE resource_kind='mcp' AND resource_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM mcp_servers WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// ---------- Skill visibility (agent × skill_name) ----------

// AgentSkillNames returns the skill directory names visible to an agent.
func (d *DB) AgentSkillNames(agentID int64) ([]string, error) {
	rows, err := d.Query(`SELECT skill_name FROM agent_skill_visibility WHERE agent_id=? AND enabled ORDER BY skill_name`, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// SkillAgents returns the agent IDs that can see a skill.
func (d *DB) SkillAgents(skillName string) ([]int64, error) {
	rows, err := d.Query(`SELECT agent_id FROM agent_skill_visibility WHERE skill_name=? AND enabled`, skillName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetAgentSkillVisibility replaces all skill visibility for an agent.
func (d *DB) SetAgentSkillVisibility(agentID int64, names []string) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 子写者的父锁：见 lockAgentForReference 的协议说明。
	if err := lockAgentForReference(tx, agentID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM agent_skill_visibility WHERE agent_id=?`, agentID); err != nil {
		return err
	}
	for _, name := range names {
		if _, err := tx.Exec(skillVisibilityUpsertSQL, skillVisibilityArgs(agentID, name)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// skillVisibilityUpsertSQL 依赖 uq_agent_skill_visibility_identity 判定冲突；
// identity 由 (agent_id, skill_name) 唯一决定，更新分支只把 enabled 拉回 TRUE。
const skillVisibilityUpsertSQL = `INSERT INTO agent_skill_visibility(agent_id,skill_name,identity,identity_hash,enabled)
VALUES (?,?,?,?,true)
ON DUPLICATE KEY UPDATE enabled=true`

func skillVisibilityArgs(agentID int64, skillName string) []any {
	identity, hash := skillVisibilityIdentity(agentID, skillName)
	return []any{agentID, skillName, identity, hash}
}

// ToggleSkillVisibility sets one (agent, skill_name) visibility on/off.
// 与批量版一样先锁 agent 父行，否则"agent 已删除 + 前端陈旧 toggle 请求"会插回孤儿行。
func (d *DB) ToggleSkillVisibility(agentID int64, skillName string, on bool) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockAgentForReference(tx, agentID); err != nil {
		return err
	}
	if on {
		if _, err := tx.Exec(skillVisibilityUpsertSQL, skillVisibilityArgs(agentID, skillName)...); err != nil {
			return err
		}
	} else if _, err := tx.Exec(`DELETE FROM agent_skill_visibility WHERE agent_id=? AND skill_name=?`, agentID, skillName); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteSkillVisibility removes all visibility rows for a skill (called on skill delete).
func (d *DB) DeleteSkillVisibility(skillName string) error {
	_, err := d.Exec(`DELETE FROM agent_skill_visibility WHERE skill_name=?`, skillName)
	return err
}

// ---------- Visibility (agent × mcp) ----------

// AgentVisible returns the resource ids of a kind visible to an agent.
func (d *DB) AgentVisible(agentID int64, kind string) ([]int64, error) {
	rows, err := d.Query(`SELECT resource_id FROM agent_visibility WHERE agent_id=? AND resource_kind=? AND enabled`, agentID, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ResourceAgents returns the agent ids that can see a resource.
func (d *DB) ResourceAgents(kind string, resourceID int64) ([]int64, error) {
	rows, err := d.Query(`SELECT agent_id FROM agent_visibility WHERE resource_kind=? AND resource_id=? AND enabled`, kind, resourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetAgentVisibilityKind replaces the full set of visible resources of a kind for
// an agent (agent-side bulk write). Bidirectional with the resource-side view —
// both read/write the same agent_visibility rows.
//
// 父锁顺序固定 agents → mcp_servers → 子表行：先锁 agent，再按 id 升序锁本批 mcp
// 资源，**全部父锁到手之后**才开始写 agent_visibility。这样本函数在排队等父锁时不
// 持有任何子表行锁，DeleteMCP（锁 mcp_servers → 清子表）与 DeleteAgent（锁 agents →
// 清子表）都构造不出环；同批内升序取锁也排除了两个并发写者反序抢同一组 MCP 的死锁。
func (d *DB) SetAgentVisibilityKind(agentID int64, kind string, resourceIDs []int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockAgentForReference(tx, agentID); err != nil {
		return err
	}
	if err := lockMCPResourceParents(tx, kind, resourceIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM agent_visibility WHERE agent_id=? AND resource_kind=?`, agentID, kind); err != nil {
		return err
	}
	for _, rid := range resourceIDs {
		if _, err := tx.Exec(agentVisibilityUpsertSQL, agentVisibilityArgs(agentID, kind, rid)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// visibilityKindMCP 是 agent_visibility.resource_kind 里唯一带数据库父表
// （mcp_servers）的取值，写者必须先持有它的行锁。
const visibilityKindMCP = "mcp"

// lockMCPResourceParents 按 id 升序锁定 resource_kind='mcp' 的资源父行；必须在
// 写入任何 agent_visibility 行之前调用（协议见 SetAgentVisibilityKind）。
func lockMCPResourceParents(tx *sql.Tx, kind string, resourceIDs []int64) error {
	if kind != visibilityKindMCP {
		return nil
	}
	ids := append([]int64(nil), resourceIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, rid := range ids {
		if err := lockMCPServerForReference(tx, rid); err != nil {
			return err
		}
	}
	return nil
}

// agentVisibilityUpsertSQL 依赖 uq_agent_visibility_identity（4 元组 hash）判定冲突。
// 本文件的写入从不带 mcp_tool_name（列默认 ”），identity 必须按同一个 ” 计算，
// 否则同一行会因为身份算错而重复插入。
const agentVisibilityUpsertSQL = `INSERT INTO agent_visibility(agent_id,resource_kind,resource_id,mcp_tool_name,identity,identity_hash,enabled)
VALUES (?,?,?,'',?,?,true)
ON DUPLICATE KEY UPDATE enabled=true`

func agentVisibilityArgs(agentID int64, kind string, resourceID int64) []any {
	identity, hash := agentVisibilityIdentity(agentID, kind, resourceID)
	return []any{agentID, kind, resourceID, identity, hash}
}

// ToggleVisibility sets one (agent, kind, resource) visibility on/off (idempotent).
// 与批量版共用同一套父锁协议（agents → mcp_servers → 子表行）。
func (d *DB) ToggleVisibility(agentID int64, kind string, resourceID int64, on bool) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockAgentForReference(tx, agentID); err != nil {
		return err
	}
	if err := lockMCPResourceParents(tx, kind, []int64{resourceID}); err != nil {
		return err
	}
	if on {
		if _, err := tx.Exec(agentVisibilityUpsertSQL, agentVisibilityArgs(agentID, kind, resourceID)...); err != nil {
			return err
		}
	} else if _, err := tx.Exec(`DELETE FROM agent_visibility WHERE agent_id=? AND resource_kind=? AND resource_id=?`, agentID, kind, resourceID); err != nil {
		return err
	}
	return tx.Commit()
}
