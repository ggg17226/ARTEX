package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/server/idalloc"
	"github.com/Autumn-27/artex/traffic"
)

// DTO/serialization layer: each handler emits EXACTLY the frontend's spec shapes
// (artex/web/src/lib/types.ts). These reshape db package structs so the
// internal DB model never leaks over the API. The db structs and the frontend are
// the canonical contracts; this file maps one onto the other.

func i64s(v int64) string { return strconv.FormatInt(v, 10) }

func rfc3339(t time.Time) string { return t.Format(time.RFC3339) }

// rawString stringifies a json.RawMessage, returning "" for empty/nil so omitempty
// fields drop out.
func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	return string(raw)
}

// ---- Task (frontend "Task") ---- created_at as RFC3339, plus a derived status.
type TaskDTO struct {
	ID                 string             `json:"id"`
	ExplorationID      string             `json:"exploration_id"`
	Name               string             `json:"name"` // 可选任务名称;空=未命名
	CategoryID         *string            `json:"category_id,omitempty"`
	CategoryName       string             `json:"category_name,omitempty"`
	Pinned             bool               `json:"pinned"`
	PinnedAt           string             `json:"pinned_at,omitempty"`
	Description        string             `json:"description"`
	Goal               string             `json:"goal"`
	Status             string             `json:"status"` // created | running | paused | done | failed
	CreatedAt          string             `json:"created_at"`
	CreatedUnix        int64              `json:"created_unix"`       // created_at as unix seconds (for run-duration calc)
	CompletedAt        string             `json:"completed_at"`       // RFC3339 finish time (done/failed); "" if unfinished
	CompletedUnix      int64              `json:"completed_unix"`     // completed_at as unix seconds (0 if unfinished)
	LastActivity       int64              `json:"last_activity_unix"` // unix seconds of the last activity (0 if none)
	Paused             bool               `json:"paused"`
	Queued             bool               `json:"queued"`
	Tokens             TokenTotalDTO      `json:"tokens"` // whole-task token consumption
	GoalsTotal         int                `json:"goals_total"`
	GoalsMet           int                `json:"goals_met"`
	InFlight           int                `json:"in_flight"`                // 运行中 Worker 数（state=running 的意图）
	Findings           FindingSeverityDTO `json:"findings"`                 // 该任务已登记的漏洞数（findings 表，按严重度分档）
	LLMProfileID       *string            `json:"llm_profile_id,omitempty"` // LLM profile used for this task; nil = default
	LLMProfileIDs      []string           `json:"llm_profile_ids"`
	ActiveLLMProfileID *string            `json:"active_llm_profile_id,omitempty"`
	LLMFailoverState   string             `json:"llm_failover_state"`
	LLMFailoverReason  string             `json:"llm_failover_reason,omitempty"`
	SourceTaskIDs      []string           `json:"source_task_ids"`
	ArchiveBlockedBy   string             `json:"archive_blocked_by_task_id,omitempty"`
	CompanyIDs         []string           `json:"company_ids"`
	CoverageEnabled    bool               `json:"coverage_enabled"` // 资产覆盖度功能开关(创建时定)
}

// FindingSeverityDTO 是任务列表里按严重度分档的漏洞计数（严重/高/中/低）。
type FindingSeverityDTO struct {
	Critical int `json:"critical"`
	High     int `json:"high"`
	Medium   int `json:"medium"`
	Low      int `json:"low"`
}

func applyTaskArchiveBlocker(dto *TaskDTO, blockers map[int64]int64) {
	if dto == nil || len(blockers) == 0 {
		return
	}
	taskID, err := strconv.ParseInt(dto.ID, 10, 64)
	if err != nil {
		return
	}
	if dependentID := blockers[taskID]; dependentID > 0 {
		dto.ArchiveBlockedBy = strconv.FormatInt(dependentID, 10)
	}
}

// TokenTotalDTO is a whole-task (all agents) token aggregate.
type TokenTotalDTO struct {
	InputTokens      int `json:"input_tokens"`
	OutputTokens     int `json:"output_tokens"`
	CacheReadTokens  int `json:"cache_read_tokens"`
	CacheWriteTokens int `json:"cache_write_tokens"`
}

func tokenTotalDTO(u db.TokenUsage) TokenTotalDTO {
	return TokenTotalDTO{
		InputTokens:      u.InputTokens,
		OutputTokens:     u.OutputTokens,
		CacheReadTokens:  u.CacheReadTokens,
		CacheWriteTokens: u.CacheWriteTokens,
	}
}

func taskDTO(t *Task, status string) TaskDTO {
	lifecycle := t.lifecycleSnapshot()
	llmState := t.llmStateSnapshot()
	sourceIDs := make([]string, 0, len(lifecycle.SourceTaskIDs))
	for _, id := range lifecycle.SourceTaskIDs {
		sourceIDs = append(sourceIDs, i64s(id))
	}
	return TaskDTO{
		ID:                 t.ID,
		ExplorationID:      i64s(t.ExpID),
		Name:               lifecycle.Name,
		CategoryID:         i64Ptr(lifecycle.CategoryID),
		CategoryName:       lifecycle.CategoryName,
		Pinned:             lifecycle.PinnedAt > 0,
		PinnedAt:           completedRFC(lifecycle.PinnedAt),
		Description:        t.Description,
		Goal:               t.Goal,
		Status:             status,
		CreatedAt:          rfc3339(time.Unix(t.CreatedAt, 0)),
		CreatedUnix:        t.CreatedAt,
		CompletedAt:        completedRFC(lifecycle.CompletedAt),
		CompletedUnix:      lifecycle.CompletedAt,
		Paused:             lifecycle.Paused,
		Queued:             lifecycle.Queued,
		LLMProfileID:       i64Ptr(llmState.ProfileID),
		LLMProfileIDs:      idStrings(llmState.ProfileIDs),
		ActiveLLMProfileID: i64Ptr(llmState.ActiveID),
		LLMFailoverState:   llmState.FailoverState,
		LLMFailoverReason:  llmState.FailoverReason,
		SourceTaskIDs:      sourceIDs,
		CompanyIDs:         idStrings(lifecycle.CompanyIDs),
		CoverageEnabled:    t.CoverageEnabled,
	}
}

// completedRFC renders a unix completion time as RFC3339, or "" when unset (0).
func completedRFC(unix int64) string {
	if unix == 0 {
		return ""
	}
	return rfc3339(time.Unix(unix, 0))
}

// ---- TrafficExchange (frontend "TrafficExchange") ---- ts as RFC3339.
type TrafficExchangeDTO struct {
	ID          string `json:"id"`
	TS          string `json:"ts"`
	Host        string `json:"host"`
	Method      string `json:"method"`
	URL         string `json:"url"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	RespLen     int    `json:"resp_len"`
}

func trafficDTOs(ex []traffic.ExchangeMeta) []TrafficExchangeDTO {
	out := make([]TrafficExchangeDTO, 0, len(ex))
	for _, e := range ex {
		out = append(out, TrafficExchangeDTO{
			ID:          e.ID,
			TS:          rfc3339(time.Unix(e.TS, 0)),
			Host:        e.Host,
			Method:      e.Method,
			URL:         e.URL,
			Status:      e.Status,
			ContentType: e.ContentType,
			RespLen:     e.RespLen,
		})
	}
	return out
}

// ---- TaskNode (frontend "TaskNode") — frontier, intents, exploration graph nodes ----

type TaskNodeDTO struct {
	ID           string `json:"id"`
	Type         string `json:"type"` // db Node.Kind
	Payload      string `json:"payload,omitempty"`
	Priority     int    `json:"priority"`
	State        string `json:"state"`
	Origin       string `json:"origin"`
	TS           string `json:"ts"`
	SourceTaskID string `json:"source_task_id,omitempty"`
	Inherited    bool   `json:"inherited,omitempty"`
	DeleteReason string `json:"delete_reason,omitempty"` // 意图假删除(state='deleted')时的删除原因
}

func taskNodeDTO(n *db.Node) TaskNodeDTO {
	d := TaskNodeDTO{
		ID:           i64s(n.ID),
		Type:         n.Kind,
		Payload:      rawString(n.Payload),
		DeleteReason: n.DeleteReason,
		Priority:     n.Priority,
		State:        n.State,
		Origin:       n.Origin,
		TS:           rfc3339(n.CreatedAt),
	}
	if n.SourceTaskID > 0 {
		d.SourceTaskID = i64s(n.SourceTaskID)
	}
	d.Inherited = n.Inherited
	return d
}

// GoalDTO is a goal node with its payload unpacked into text/vulnclass — the shape
// the 总览「目标管理」UI works with (vs TaskNodeDTO which carries raw payload JSON).
type GoalDTO struct {
	ID        string `json:"id"`
	Text      string `json:"text"`
	VulnClass string `json:"vulnclass,omitempty"`
	State     string `json:"state"`
	Origin    string `json:"origin,omitempty"`
	TS        string `json:"ts"`
}

func goalDTO(n *db.Node) GoalDTO {
	var p struct {
		Text      string `json:"text"`
		VulnClass string `json:"vulnclass"`
	}
	_ = json.Unmarshal(n.Payload, &p)
	return GoalDTO{
		ID:        i64s(n.ID),
		Text:      p.Text,
		VulnClass: p.VulnClass,
		State:     n.State,
		Origin:    n.Origin,
		TS:        rfc3339(n.CreatedAt),
	}
}

func goalDTOs(in []*db.Node) []GoalDTO {
	out := make([]GoalDTO, 0, len(in))
	for _, n := range in {
		out = append(out, goalDTO(n))
	}
	return out
}

// ConstraintDTO is one operation constraint (allow/deny) for the 总览「约束管理」UI.
type ConstraintDTO struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"` // allow | deny
	Text   string `json:"text"`
	Origin string `json:"origin,omitempty"`
	TS     string `json:"ts,omitempty"`
}

func taskNodeDTOs(in []*db.Node) []TaskNodeDTO {
	out := make([]TaskNodeDTO, 0, len(in))
	for _, n := range in {
		out = append(out, taskNodeDTO(n))
	}
	return out
}

// ---- Edge (frontend "Edge") — exploration edges and asset edges ----

type EdgeDTO struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
	Rel string `json:"rel"`
}

func edgeDTO(e db.Edge) EdgeDTO {
	return EdgeDTO{Src: i64s(e.From), Dst: i64s(e.To), Rel: e.Rel}
}

func edgeDTOs(in []db.Edge) []EdgeDTO {
	out := make([]EdgeDTO, 0, len(in))
	for _, e := range in {
		out = append(out, edgeDTO(e))
	}
	return out
}

// ---- Coverage asset references ----

type CoverageAssetRefDTO struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	State        string `json:"state"`
	Summary      string `json:"summary"`
	SourceTaskID string `json:"source_task_id,omitempty"`
	Inherited    bool   `json:"inherited,omitempty"`
}

func coverageAssetRefDTO(ref db.AssetRef) CoverageAssetRefDTO {
	out := CoverageAssetRefDTO{
		ID: i64s(ref.ID), Kind: ref.Kind, State: ref.State, Summary: ref.Summary, Inherited: ref.Inherited,
	}
	if ref.SourceTaskID > 0 {
		out.SourceTaskID = i64s(ref.SourceTaskID)
	}
	return out
}

// ---- Finding (frontend "Finding") ----

type FindingDTO struct {
	TrafficCount          int                        `json:"traffic_count"`
	EvidenceVersion       int64                      `json:"evidence_version"`
	ReportEvidenceVersion int64                      `json:"report_evidence_version"`
	ReportStale           bool                       `json:"report_stale"`
	TrafficBindings       []db.FindingTrafficBinding `json:"traffic_bindings,omitempty"`

	ID        string `json:"id"`
	FindingID string `json:"finding_id,omitempty"` // standalone findings-table id — the handle for status updates
	VulnClass string `json:"vulnclass"`
	Name      string `json:"name,omitempty"` // 漏洞名称;为空时前端回退展示 vulnclass
	Severity  string `json:"severity"`       // critical | high | medium | low
	Status    string `json:"status"`         // pending | in_progress | confirmed | resolved | fixed | false_positive | ignored | duplicate | risk_accepted
	Summary   string `json:"summary"`
	Evidence  string `json:"evidence"`
	Report    string `json:"report,omitempty"` // 详细报告(Markdown);仅详情接口返回,列表为空

	IntentID        string            `json:"intent_id,omitempty"`
	ParamID         string            `json:"param_id,omitempty"`
	TaskID          string            `json:"task_id,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	SourceTaskID    string            `json:"source_task_id,omitempty"`
	Inherited       bool              `json:"inherited,omitempty"`
	Assets          []FindingAssetDTO `json:"assets,omitempty"`
	TS              string            `json:"ts"`
}

// FindingAssetDTO is one asset a finding is anchored to, pre-labelled for display.
type FindingAssetDTO struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

// assetLabel renders an asset's most identifying field for compact display.
func assetLabel(a *db.Asset) string {
	switch {
	case a.URL != "":
		if a.Method != "" {
			return a.Method + " " + a.URL
		}
		return a.URL
	case a.Domain != "":
		if a.Port != nil && *a.Port > 0 {
			return fmt.Sprintf("%s:%d", a.Domain, *a.Port)
		}
		return a.Domain
	case a.IP != "":
		if a.Port != nil && *a.Port > 0 {
			return fmt.Sprintf("%s:%d", a.IP, *a.Port)
		}
		return a.IP
	case a.AppName != "":
		return a.AppName
	case a.BundleID != "":
		return a.BundleID
	case a.ServiceName != "":
		return a.ServiceName
	case a.RootDomain != "":
		return a.RootDomain
	default:
		return "#" + i64s(a.ID)
	}
}

// findingPayload mirrors the JSON written by the worker's report_finding tool
// (agent/tools.go addFinding): {vulnclass, severity, summary, evidence:{by,poc}}.
type findingPayload struct {
	VulnClass string          `json:"vulnclass"`
	Name      string          `json:"name"`
	Severity  string          `json:"severity"`
	Summary   string          `json:"summary"`
	Evidence  json.RawMessage `json:"evidence"`
}

func findingDTO(n *db.Node) FindingDTO {
	var p findingPayload
	_ = json.Unmarshal(n.Payload, &p)
	d := FindingDTO{
		ID:        i64s(n.ID),
		VulnClass: p.VulnClass,
		Name:      p.Name,
		Severity:  p.Severity,
		Status:    db.FindingPending,
		Summary:   p.Summary,
		Evidence:  rawString(p.Evidence),
		TS:        rfc3339(n.CreatedAt),
	}
	if n.SourceTaskID > 0 {
		d.SourceTaskID = i64s(n.SourceTaskID)
	}
	d.Inherited = n.Inherited
	return d
}

// findingDTOsForTask converts a task's finding nodes to DTOs, stamping each with
// the owning task's id/description so the global 发现 page can group across tasks.
// meta maps node id → the standalone findings row (id + status + asset ids), so the
// per-task view shows the same triage state and anchored assets as the global page;
// nodes with no row keep the 'pending' default and no finding_id (not editable).
// assets pre-resolves the anchored asset rows for label rendering.
func findingDTOsForTask(t *Task, in []*db.Node, meta map[int64]db.FindingMeta, assets map[int64]*db.Asset) []FindingDTO {
	return findingDTOsForOwner(t.ID, t.Description, in, meta, assets)
}

func findingDTOsForOwner(taskID, description string, in []*db.Node, meta map[int64]db.FindingMeta, assets map[int64]*db.Asset) []FindingDTO {
	out := make([]FindingDTO, 0, len(in))
	for _, n := range in {
		d := findingDTO(n)
		d.TaskID = taskID
		d.TaskDescription = description
		if m, ok := meta[n.ID]; ok {
			d.FindingID = i64s(m.ID)
			d.Status = m.Status
			d.TrafficCount = m.TrafficCount
			d.Assets = findingAssetDTOs(m.AssetIDs, assets)
		}
		out = append(out, d)
	}
	return out
}

// findingAssetDTOs maps anchored asset ids to display DTOs, skipping ids whose
// asset row is missing (e.g. deleted).
func findingAssetDTOs(ids []int64, assets map[int64]*db.Asset) []FindingAssetDTO {
	var out []FindingAssetDTO
	for _, aid := range ids {
		if a := assets[aid]; a != nil {
			out = append(out, FindingAssetDTO{ID: i64s(a.ID), Type: a.Type, Label: assetLabel(a)})
		}
	}
	return out
}

// findingFromDB converts a standalone DBFinding row to a FindingDTO. task_id and
// task_description are empty when the originating task has been deleted (NULL).
func findingFromDB(f *db.DBFinding, assets map[int64]*db.Asset) FindingDTO {
	status := f.Status
	if status == "" {
		status = db.FindingPending
	}
	d := FindingDTO{
		ID:           i64s(f.ID),
		FindingID:    i64s(f.ID),
		TrafficCount: f.TrafficCount, EvidenceVersion: f.EvidenceVersion, ReportEvidenceVersion: f.ReportEvidenceVersion,
		ReportStale: f.Report != "" && f.EvidenceVersion != f.ReportEvidenceVersion, TrafficBindings: f.TrafficBindings,
		VulnClass: f.VulnClass,
		Name:      f.Name,
		Severity:  f.Severity,
		Status:    status,
		Summary:   f.Summary,
		Evidence:  f.Evidence,
		Report:    f.Report,
		TS:        rfc3339(f.CreatedAt),
	}
	d.Assets = findingAssetDTOs(f.AssetIDs, assets)
	if f.TaskID != nil {
		d.TaskID = i64s(*f.TaskID)
		d.TaskDescription = f.TaskDescription
	}
	if f.NodeID != nil {
		d.IntentID = "" // node_id is the finding node, not the intent; keep IntentID empty
	}
	return d
}

// ---- Activity (frontend "Activity") ----

type ActivityDTO struct {
	Seq          string          `json:"seq"`                 // db Activity.ID（P03：BIGINT 主键别名，wire 一律 string）
	IntentID     string          `json:"intent_id,omitempty"` // db NodeID
	Worker       string          `json:"worker"`
	TS           string          `json:"ts"` // db CreatedAt
	Kind         string          `json:"kind"`
	Tool         string          `json:"tool,omitempty"`
	ToolUseID    string          `json:"tool_use_id,omitempty"`
	IsError      bool            `json:"is_error"`
	Summary      string          `json:"summary"`
	Detail       string          `json:"detail,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	SourceTaskID string          `json:"source_task_id,omitempty"`
	Inherited    bool            `json:"inherited,omitempty"`
	MainSeg      *int            `json:"main_seg,omitempty"` // main-agent conversation segment (nil for non-mainagent rows)
	// token usage (set only on kind='result'); used for per-session token totals.
	InputTokens      *int `json:"input_tokens,omitempty"`
	OutputTokens     *int `json:"output_tokens,omitempty"`
	CacheReadTokens  *int `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens *int `json:"cache_write_tokens,omitempty"`
}

func activityDTO(a db.Activity) ActivityDTO {
	intent := ""
	if a.NodeID != nil {
		intent = i64s(*a.NodeID)
	}
	d := ActivityDTO{
		Seq:              i64s(a.ID),
		IntentID:         intent,
		Worker:           a.Worker,
		TS:               rfc3339(a.CreatedAt),
		Kind:             a.Kind,
		Tool:             a.Tool,
		ToolUseID:        a.ToolUseID,
		IsError:          a.IsError,
		Summary:          a.Summary,
		Detail:           a.Detail, // list endpoint leaves this empty (lazy)
		Metadata:         a.Metadata,
		InputTokens:      a.InputTokens,
		OutputTokens:     a.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens,
		MainSeg:          a.MainSeg,
	}
	if a.SourceTaskID > 0 {
		d.SourceTaskID = i64s(a.SourceTaskID)
	}
	d.Inherited = a.Inherited
	return d
}

func activityDTOs(in []db.Activity) []ActivityDTO {
	out := make([]ActivityDTO, 0, len(in))
	for _, a := range in {
		out = append(out, activityDTO(a))
	}
	return out
}

// ---- Agent (frontend "Agent") — string id ----

type AgentDTO struct {
	ID               string  `json:"id"`
	Key              string  `json:"key"`
	Name             string  `json:"name"`
	Description      string  `json:"description"`
	Role             string  `json:"role"`
	Builtin          bool    `json:"builtin"`
	Enabled          bool    `json:"enabled"`
	MaxTurns         int     `json:"max_turns"`
	RunSecs          int     `json:"run_seconds"`
	WebSearch        bool    `json:"web_search"`
	InteractiveShell bool    `json:"interactive_shell"`
	LLMProfileID     *string `json:"llm_profile_id"` // 绑定的 LLM 配置;null=跟随任务/全局
	// P3 触发后处理策略(仅自定义 agent 有意义)。
	TriggerRunMode     string `json:"trigger_run_mode"`
	TriggerMergeMode   string `json:"trigger_merge_mode"`
	TriggerMaxParallel int    `json:"trigger_max_parallel"`
	// binding counts (populated only by the list endpoint) — shown on agent cards.
	McpCount   int `json:"mcp_count"`
	SkillCount int `json:"skill_count"`
	ToolCount  int `json:"tool_count"`
}

func agentDTO(a *db.Agent) AgentDTO {
	return AgentDTO{
		ID:                 i64s(a.ID),
		Key:                a.Key,
		Name:               a.Name,
		Description:        a.Description,
		Role:               a.Role,
		Builtin:            a.Builtin,
		Enabled:            a.Enabled,
		MaxTurns:           a.MaxTurns,
		RunSecs:            a.RunSecs,
		WebSearch:          a.WebSearch,
		InteractiveShell:   a.InteractiveShell,
		LLMProfileID:       i64Ptr(a.LLMProfileID),
		TriggerRunMode:     a.TriggerRunMode,
		TriggerMergeMode:   a.TriggerMergeMode,
		TriggerMaxParallel: a.TriggerMaxParallel,
	}
}

func agentDTOs(in []*db.Agent) []AgentDTO {
	out := make([]AgentDTO, 0, len(in))
	for _, a := range in {
		out = append(out, agentDTO(a))
	}
	return out
}

// ---- LLMProfile (frontend "LLMProfile") — string id ----

type LLMProfileDTO struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	Format          string  `json:"format"`
	BaseURL         string  `json:"base_url,omitempty"`
	Proxy           string  `json:"proxy,omitempty"`
	Model           string  `json:"model"`
	APIKeyHint      string  `json:"api_key_hint,omitempty"`
	RatePerSecond   float64 `json:"rate_per_second"`
	RatePerMinute   float64 `json:"rate_per_minute"`
	ContextWindowK  int     `json:"context_window_k"`
	ThinkingType    string  `json:"thinking_type"`
	ReasoningEffort string  `json:"reasoning_effort"`
	IsDefault       bool    `json:"is_default"`
	// 轮询(故障转移)参数：priority 越大越先被选中(激活配置恒为链首)；
	// pool_exclude=true 则不作为故障转移目标，但仍可被 agent/任务显式绑定。
	Priority    int  `json:"priority"`
	PoolExclude bool `json:"pool_exclude"`
	// 收发模式：true=流式(SSE) | false=非流式。没有 omitempty —— false 必须出现在
	// 响应里，否则前端读不到「非流式」，开关会回落成默认的流式。
	Streaming bool `json:"streaming"`
	// 单次回复输出上限(0=不发送，由服务端默认值决定)，以及它用哪个请求字段名
	// (''=max_tokens | 'max_completion_tokens'，仅 openai 格式有意义)。
	MaxTokens      int    `json:"max_tokens"`
	MaxTokensField string `json:"max_tokens_field"`
	// 自定义会话头名：非空时每次请求带该 HTTP 头，头值=当前会话/意图的 session id。
	// ''=不发送。用于按 session-id 头做提示缓存/粘性路由的网关。
	SessionHeaderKey string `json:"session_header_key"`
	// 本配置对重试的覆盖(建连/空响应/同 provider 安全窗口)。每项 attempts:
	// 0=继承全局策略 | -1=关闭该层重试 | >0=次数；interval_ms: 0=用默认指数退避 |
	// >0=改用该固定毫秒间隔。全 0 = 完全跟随全局，即历史行为。
	Retry db.RetryOverride `json:"retry"`
}

func llmProfileDTO(p *db.LLMProfile) LLMProfileDTO {
	return LLMProfileDTO{
		ID:               i64s(p.ID),
		Name:             p.Name,
		Format:           p.Format,
		BaseURL:          p.BaseURL,
		Proxy:            p.Proxy,
		Model:            p.Model,
		APIKeyHint:       p.APIKeyHint,
		RatePerSecond:    p.RatePerSecond,
		RatePerMinute:    p.RatePerMinute,
		ContextWindowK:   p.ContextWindowK,
		ThinkingType:     p.ThinkingType,
		ReasoningEffort:  p.ReasoningEffort,
		IsDefault:        p.IsDefault,
		Priority:         p.Priority,
		PoolExclude:      p.PoolExclude,
		Streaming:        p.Streaming,
		MaxTokens:        p.MaxTokens,
		MaxTokensField:   p.MaxTokensField,
		SessionHeaderKey: p.SessionHeaderKey,
		Retry:            p.Retry,
	}
}

func llmProfileDTOs(in []*db.LLMProfile) []LLMProfileDTO {
	out := make([]LLMProfileDTO, 0, len(in))
	for _, p := range in {
		out = append(out, llmProfileDTO(p))
	}
	return out
}

// idStrings formats a slice of int64 ids as strings (for visibility agent ids).
func idStrings(in []int64) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, i64s(v))
	}
	return out
}

// i64Ptr 把内部 *int64 指针转为 wire *string（nil → nil，保持 omitempty 语义）。
func i64Ptr(v *int64) *string {
	if v == nil {
		return nil
	}
	s := strconv.FormatInt(*v, 10)
	return &s
}

// parseIDField 严格解析入口 string ID（path value / body 必填 ID）：
// 必须为十进制整数且落在 1..math.MaxInt64（spec §2.4.1；逐端点，不是全局 >0）。
// 空串/非数字/负数/0/小数/>MaxInt64 → 错误，调用方回 400。
func parseIDField(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("id 不能为空")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("id 非法: %q", s)
	}
	if v <= 0 {
		return 0, fmt.Errorf("id 非法（必须为 1..MaxInt64 正整数）: %s", s)
	}
	return v, nil
}

// parseIDFieldLenient 用于 query-param 过滤器（如 commandTaskFilter）：
// 空串/不可解析/越界 → nil（视为无过滤，不 400）；合法 1..MaxInt64 → &v。
func parseIDFieldLenient(s string) *int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v > math.MaxInt64 {
		return nil
	}
	return &v
}

// queryFilterID 是 parseIDFieldLenient 的 int64/0 哨兵包装，供 store 侧签名是
// `id int64`（0 = 不过滤）的可选 query 过滤器使用（P03 E10：asset 列表的
// company_id / task_id、counts 的 task_id、notify 投递历史的 channel_id 等）。
//
// 语义与 lenient 完全一致：未提供 / 非数字 / 小数 / 0 / 负数 / > MaxInt64 溢出 → 0
// （视为未指定，不 400）。关键是**溢出不得变成 MaxInt64 过滤值**：旧的
// `id, _ := strconv.ParseInt(...)` 在超上限时返回 math.MaxInt64，会让一个写错的
// task_id 静默变成「按 MaxInt64 过滤」的空结果，而不是「未过滤」。
func queryFilterID(s string) int64 {
	if v := parseIDFieldLenient(s); v != nil {
		return *v
	}
	return 0
}

// parseCursorID 解析「DB 主键游标」类入参（P03 E10）：activity 的 since/before、
// logs 历史的 before(db_id)、旁路历史的 before、conversation messages 的 since/before、
// intents 的 before 等都是 activity_logs / task_archives / side_question 序列的 BIGINT 主键，
// 不是分页页码，因此必须显式 64 位解析并正确处理 error。
//   - ""（未提供）与 "0" → 0，保留「从头 / 最新一页」哨兵（逐端点约定，不是全局放宽）；
//   - 合法正十进制且 ≤ MaxInt64 → 该值；
//   - 其余（非数字、小数、负数、"+1"、> MaxInt64）→ error，调用方回 400。
//
// 与 parseIDField 的差别只是允许 0 哨兵；与 parseIDFieldLenient 的差别是非法值必须报错：
// 游标写错若静默退化成 0，客户端会把「参数错了」误读成「从头重拉」，既丢增量语义又放大流量。
// 溢出必须报错，绝不能像旧的 `n, _ := ParseInt` 那样把 >MaxInt64 变成 MaxInt64 过滤值。
func parseCursorID(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cursor 非法: %q", s)
	}
	if v <= 0 {
		return 0, fmt.Errorf("cursor 非法（必须为 0 或 1..MaxInt64 正整数）: %s", s)
	}
	return v, nil
}

// parseIDListField 严格解析请求体里的 64 位 ID 字符串数组（P03：ID 对外一律 string）。
// 逐项经 parseIDField 校验（1..math.MaxInt64）；任一项非法即整体返回错误，调用方回 400。
// 未提供 / 空数组返回 nil，「未指定」语义由调用方决定。
func parseIDListField(in []string) ([]int64, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]int64, 0, len(in))
	for i, raw := range in {
		v, err := parseIDField(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("ids[%d]: %w", i, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// parseOptionalIDString 解析请求体里可选的 string ID：未提供（""）与 "0" 保持 0 哨兵
// 语义（如 insertAssets 的 task_id=0 表示不绑定任务，逐端点保留，不是全局放宽）；
// 其余经 parseIDField 严格解析（1..math.MaxInt64），非数字/负数/越界返回错误。
func parseOptionalIDString(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	return parseIDField(s)
}

// parseRequiredIDRaw 严格解析必填的 ID（支持 string 或 number 兼容格式）。
// 必须为 1..math.MaxInt64 正整数。未提供、null、空串、0、负数、小数、非数字、越界均报错。
func parseRequiredIDRaw(raw json.RawMessage) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, errors.New("id is required")
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return 0, fmt.Errorf("id 非法: %w", err)
		}
		return parseIDField(strings.TrimSpace(s))
	}
	var num uint64
	if err := json.Unmarshal(trimmed, &num); err != nil {
		return 0, fmt.Errorf("id 非法: %w", err)
	}
	if err := idalloc.ValidateUint64(num); err != nil {
		return 0, err
	}
	return int64(num), nil
}

// parseOptionalIDRaw 解析可选 ID（如 /api/llm/test 的 ProfileID 等）。
// 未提供、null 返回 nil, nil；非空时必须为合法 1..MaxInt64 正整数。
func parseOptionalIDRaw(raw json.RawMessage) (*int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	id, err := parseRequiredIDRaw(raw)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// parseProfileSaveID 解析 profile 保存请求中的 ID（支持新建 0 / 空串 与编辑更新）。
// 允许新建：未传、null、""、"0"、0 均解析为 0；
// 若传非空字符串且非 "0"，经 parseIDField 校验解析；
// 若传数字 >0，经 idalloc.ValidateUint64 校验；
// 负数、小数、非数字、>MaxInt64 均返回错误。
func parseProfileSaveID(raw json.RawMessage) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, nil
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return 0, fmt.Errorf("id 非法: %w", err)
		}
		s = strings.TrimSpace(s)
		if s == "" || s == "0" {
			return 0, nil
		}
		return parseIDField(s)
	}
	var num uint64
	if err := json.Unmarshal(trimmed, &num); err != nil {
		return 0, fmt.Errorf("id 非法: %w", err)
	}
	if num == 0 {
		return 0, nil
	}
	if err := idalloc.ValidateUint64(num); err != nil {
		return 0, err
	}
	return int64(num), nil
}

// parseJudgeProfileID 解析 judge 审查配置中的 profile_id（支持 "0" / 0 跟随激活 sentinel）。
// 未传、null、"0"、0 均保持为 0；
// 若传非空字符串且非 "0"，经 parseIDField 校验解析；传空串 "" 经 parseIDField 报错；
// 若传数字 >0，经 idalloc.ValidateUint64 校验；
// 负数、小数、非数字、>MaxInt64、空串均返回错误。
func parseJudgeProfileID(raw json.RawMessage) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, nil
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return 0, fmt.Errorf("profile_id 非法: %w", err)
		}
		s = strings.TrimSpace(s)
		if s == "0" {
			return 0, nil
		}
		return parseIDField(s)
	}
	var num uint64
	if err := json.Unmarshal(trimmed, &num); err != nil {
		return 0, fmt.Errorf("profile_id 非法: %w", err)
	}
	if num == 0 {
		return 0, nil
	}
	if err := idalloc.ValidateUint64(num); err != nil {
		return 0, err
	}
	return int64(num), nil
}
