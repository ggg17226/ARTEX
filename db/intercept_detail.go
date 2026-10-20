package db

import (
	"database/sql"
	"encoding/json"
	"time"
)

// InterceptContextEntry is a bounded, recorded session event, not model reasoning.
type InterceptContextEntry struct {
	Kind      string `json:"kind"`
	Tool      string `json:"tool,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	Text      string `json:"text"`
	IsError   bool   `json:"is_error,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

// InterceptAudit is captured at review time. It is deliberately excluded from
// polling/list responses; old rows have no audit instead of reconstructed data.
type InterceptAudit struct {
	RunID            string                  `json:"run_id,omitempty"`
	ToolUseID        string                  `json:"tool_use_id,omitempty"`
	Correlation      string                  `json:"correlation"` // exact | ambiguous | unavailable
	InputDigest      string                  `json:"input_digest"`
	UserMessage      string                  `json:"user_message"`
	UserTruncated    bool                    `json:"user_truncated,omitempty"`
	Context          []InterceptContextEntry `json:"context"`
	ContextTruncated bool                    `json:"context_truncated,omitempty"`
	CapturedAt       time.Time               `json:"captured_at"`
	ModelFallback    bool                    `json:"model_fallback,omitempty"`
	ModelInput       json.RawMessage         `json:"model_input,omitempty"`
	ModelInputDigest string                  `json:"model_input_digest,omitempty"`
	InitialAction    string                  `json:"initial_action"`
	InitialReason    string                  `json:"initial_reason"`
	EffectiveAction  string                  `json:"effective_action,omitempty"`
	DecisionReason   string                  `json:"decision_reason,omitempty"`
	RuleName         string                  `json:"rule_name,omitempty"`
	ConfigDigest     string                  `json:"config_digest,omitempty"`
	ProfileID        int64                   `json:"profile_id,omitempty"`
	ExecutionStatus  string                  `json:"execution_status"`
	Output           string                  `json:"output,omitempty"`
	OutputTruncated  bool                    `json:"output_truncated,omitempty"`
	ExecutionEndedAt *time.Time              `json:"execution_ended_at,omitempty"`
}

type InterceptDetail struct {
	InterceptApprovalRow
	Audit *InterceptAudit `json:"audit"`
}

func (d *DB) GetInterceptDetail(id int64) (*InterceptDetail, error) {
	var out InterceptDetail
	var raw []byte
	err := d.QueryRow(approvalRowSelectWithAudit+` WHERE ip.id = ?`, id).Scan(
		&out.ID, &out.RuleID, &out.ConversationID, &out.TaskID, &out.AgentName,
		&out.ToolName, &out.ToolInput, &out.Status, &out.Reason, &out.DecidedAt, &out.CreatedAt,
		&out.DecisionSource, &out.ConvTitle, &out.ConvAgentKey, &out.RuleName, &raw,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.Audit); err != nil {
			return nil, err
		}
	}
	return &out, nil
}

// ResolveIntercept atomically settles a pending request. A timeout cannot
// overwrite a human decision and repeat decisions cannot rewrite history.
func (d *DB) ResolveIntercept(id int64, status, action, reason string) (bool, error) {
	execution := "not_executed"
	if action == "allow" {
		execution = "awaiting_result"
	}
	patch, err := json.Marshal(map[string]any{
		"effective_action": action, "decision_reason": reason, "execution_status": execution,
	})
	if err != nil {
		return false, err
	}
	// TiDB 侧的等价改写（逐条对应，语义不变）：
	//   - `audit || patch::jsonb` → `JSON_MERGE_PATCH(audit, CAST(patch AS JSON))`：
	//     两个对象合并、右值覆盖左值，与 jsonb 的 || 一致；`||` 在 MySQL 是逻辑或，
	//     照抄会把布尔值写进 audit；
	//   - `audit->>'correlation' IS DISTINCT FROM 'exact'` → `NOT (取值 <=> 'exact')`：
	//     必须用 NULL 安全等值，否则"字段缺失/脏行"在 PG 里判 TRUE（→ 执行结果未知），
	//     在 MySQL 的三值逻辑里会变成 NULL 而被 CASE 当成 FALSE，把"无法确认执行结果"
	//     误记成"执行成功"；
	//   - `CASE WHEN audit IS NULL THEN NULL` 保留：老行是"没有审计"而不是"空审计"，
	//     绝不能凭空造一个 {} 出来；
	//   - NOW() → NOW(3)：本库 DATETIME(3) 精度的时间列一律由 NOW(3) 提供。
	r, err := d.Exec(`UPDATE intercept_pending SET status=?, decided_at=NOW(3),
		audit=CASE WHEN audit IS NULL THEN NULL ELSE JSON_MERGE_PATCH(JSON_MERGE_PATCH(audit, CAST(? AS JSON)),
        CASE WHEN ?='allow' AND NOT (`+expJSONTextCond("audit", "$.correlation")+` <=> 'exact')
        THEN CAST('{"execution_status":"unknown"}' AS JSON) ELSE CAST('{}' AS JSON) END) END
        WHERE id = ? AND status='pending'`, status, patch, action, id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// CompleteIntercept only updates the exact recorded call after it was allowed.
// A blocked tool_result must never be presented as a failed execution.
func (d *DB) CompleteIntercept(id int64, runID, toolUseID, status, output string, truncated bool) error {
	patch, err := json.Marshal(map[string]any{
		"execution_status": status, "output": output, "output_truncated": truncated,
		"execution_ended_at": time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	// 回填只打在"当初真正放行、且正在等结果"的那一次调用上：四个条件全部走
	// JSON_VALID 守卫的取值口径（同 expJSONTextCond），脏行按不匹配处理，绝不让
	// 一条历史脏数据把整条 UPDATE 打成 Error 3141。
	_, err = d.Exec(`UPDATE intercept_pending SET audit=JSON_MERGE_PATCH(audit, CAST(? AS JSON))
		WHERE id = ? AND `+expJSONTextCond("audit", "$.run_id")+` = ? AND `+expJSONTextCond("audit", "$.tool_use_id")+` = ?
		AND `+expJSONTextCond("audit", "$.effective_action")+`='allow' AND `+expJSONTextCond("audit", "$.execution_status")+`='awaiting_result'`,
		patch, id, runID, toolUseID)
	return err
}
