package server

// W4b-2c（P03 输出侧 string 化 · 第三批）离线 wire 断言：R01–R05、R15–R22、R25、R26
// 以及并入的 R21 findings（O-1 sendWorkerMessage 响应）。
//
// 口径（与 W4b-2a / W4b-2b 一致）：
//  1. 大于 2^53 的 64 位 ID 必须序列化为**带引号的 string**，且十进制数值不失真
//     （9007199254740993 / 9223372036854775807 原样回读）；
//  2. 非 ID 字段（priority / timeout_seconds / interval_sec / latency_ms / token 计数 /
//     progress / format_version / original_size / remaining_timeout_seconds / total /
//     page / size / args_len / count 等）保持 JSON number；
//  3. nil / omitempty 语义不变：无 omitempty 的 *int64 nil 仍是 null，
//     有 omitempty 的 *int64 nil 仍被省略（不会变成 "0"）；
//  4. R22 的 source_task_ids 是**字符串数组**（外层同名字段屏蔽嵌入的 []int64，只出现一次），
//     nil / 空切片的 null / [] 语义不变；
//  5. R04 的 db.InterceptAudit 是**持久化 JSON 回读目标**，其 profile_id 必须仍是 number
//     （加 ,string 会让历史行解码失败），只有 server 输出侧的 interceptAuditWire 是 string；
//  6. R26 的 saveMCPRequest 仍必须屏蔽 db.MCPServer.ID 的解码（string / number / 缺省都不报错，
//     且 MCPServer.ID 保持 0，由 parseProfileSaveID 统一解析）。
//
// 证据边界：这些是**结构体 / map 字面量级**的 Marshal 断言，不是 handler 级覆盖
// （handler 需要真实数据库，离线全部 t.Skip）；handler 侧的转换由代码审查 + 本文件的
// 同形 map 断言佐证。不依赖数据库，纯 json.Marshal / json.Unmarshal。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

// strPtr 只在本测试里用：TaskID 对外本来就是 *string，这里造大 ID 指针。
func strPtr(v string) *string { return &v }

// R01：AssetInterceptRule.ID → string；enabled / builtin 仍是 bool。
// 返回链：assetInterceptListRules / CreateRule / UpdateRule（map{"rules":[]} 与裸 rule）。
func TestIDOutputWireR01AssetInterceptRule(t *testing.T) {
	rule := db.AssetInterceptRule{ID: bigID, Enabled: true, Kind: "cidr", Pattern: "10.0.0.0/8", Note: "n", Builtin: true}
	raw := mustMarshal(t, rule)
	assertStringID(t, raw, "id", bigIDStr)
	roundTrip(t, rule)

	rawMax := mustMarshal(t, db.AssetInterceptRule{ID: maxInt64, Kind: "exact_domain", Pattern: "a.com"})
	assertStringID(t, rawMax, "id", maxIDStr)

	// 列表 map 形状（handler 直接放 db 切片，tag 生效）。
	rawList := mustMarshal(t, map[string]any{"rules": []db.AssetInterceptRule{{ID: bigID}, {ID: 1}}})
	assertStringID(t, rawList, "id", bigIDStr)
	assertStringID(t, rawList, "id", "1")

	// 零值 ID 仍是带引号的 "0"（无 omitempty），不会退回 number。
	rawZero := mustMarshal(t, db.AssetInterceptRule{ID: 0})
	assertStringID(t, rawZero, "id", "0")
	// action 仅任务级规则有值，仍按 omitempty 省略。
	assertOmitted(t, rawZero, "action")
}

// R02：InterceptRule.ID → string；priority / timeout_seconds 是配置数值，保持 number。
func TestIDOutputWireR02InterceptRule(t *testing.T) {
	rule := db.InterceptRule{
		ID: bigID, Name: "r", Enabled: true, Priority: 90, MatchTarget: "tool",
		MatchType: "regex", Pattern: ".*", Action: "ask", Message: "m",
		TimeoutEnabled: true, TimeoutSeconds: 300, TimeoutAction: "deny",
	}
	raw := mustMarshal(t, rule)
	assertStringID(t, raw, "id", bigIDStr)
	assertNumberField(t, raw, "priority", 90)
	assertNumberField(t, raw, "timeout_seconds", 300)
	roundTrip(t, rule)

	rawList := mustMarshal(t, map[string]any{"rules": []db.InterceptRule{{ID: maxInt64, Priority: 1}}})
	assertStringID(t, rawList, "id", maxIDStr)
	assertNumberField(t, rawList, "priority", 1)
}

// R03：InterceptPending 的 id / rule_id / conversation_id → string；
// 两个 *int64 无 omitempty，nil 仍是 null（不会变成 "0"，也不会被 omit）；
// task_id 仍是 *string；嵌入它的 InterceptApprovalRow 自动继承。
func TestIDOutputWireR03InterceptPendingAndApprovalRow(t *testing.T) {
	conv := maxInt64
	rule := bigID
	pending := db.InterceptPending{
		ID: bigID, RuleID: &rule, ConversationID: &conv, TaskID: strPtr("9007199254740993"),
		AgentName: "planner", ToolName: "bash", ToolInput: json.RawMessage(`{"cmd":"id"}`),
		Status: "pending", DecisionSource: "rule", Reason: "r",
	}
	raw := mustMarshal(t, pending)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "rule_id", bigIDStr)
	assertStringID(t, raw, "conversation_id", maxIDStr)
	assertStringID(t, raw, "task_id", bigIDStr)
	roundTrip(t, pending)

	// nil 指针：仍是 null（不是 "0"、不是 omit）。
	rawNil := mustMarshal(t, db.InterceptPending{ID: 1})
	assertStringID(t, rawNil, "id", "1")
	assertNullField(t, rawNil, "rule_id")
	assertNullField(t, rawNil, "conversation_id")
	assertNullField(t, rawNil, "task_id")
	roundTrip(t, db.InterceptPending{ID: 1})

	// 审批列表行：嵌入字段继承同样的 tag。
	row := db.InterceptApprovalRow{
		InterceptPending: pending,
		ConvTitle:        "t", ConvAgentKey: "planner", RuleName: "r",
	}
	rawRow := mustMarshal(t, row)
	assertStringID(t, rawRow, "id", bigIDStr)
	assertStringID(t, rawRow, "rule_id", bigIDStr)
	assertStringID(t, rawRow, "conversation_id", maxIDStr)
	// 嵌入不产生重复 key。
	assertKeyCount(t, rawRow, "id", 1)
	roundTrip(t, row)
}

// R04：审批详情。
//
//	(a) 回归锁：db.InterceptAudit 会被 Marshal 进 intercept_pending.audit 列并被
//	    GetInterceptDetail json.Unmarshal 回读，所以它的 profile_id **必须仍是 number**，
//	    且历史数字形态必须仍能解码；
//	(b) server 输出侧 interceptAuditWire 把 profile_id 覆盖成 string（0 时仍 omit）；
//	(c) 嵌套 InterceptDetail 的审批行 ID（R03）同样是 string；audit 缺失时仍是 null。
func TestIDOutputWireR04InterceptDetailAuditProfileID(t *testing.T) {
	audit := &db.InterceptAudit{
		RunID: "run-1", ToolUseID: "tu-1", Correlation: "exact", InputDigest: "d",
		UserMessage: "hi", Context: []db.InterceptContextEntry{{Kind: "user_message", Text: "hi"}},
		InitialAction: "ask", InitialReason: "r", EffectiveAction: "allow",
		DecisionReason: "dr", RuleName: "rule", ConfigDigest: "cfg",
		ProfileID: bigID, ExecutionStatus: "completed", Output: "out",
	}
	// (a) db 结构体本身仍是 number —— 持久化 JSON 回读依赖这一点。
	rawDBAudit := mustMarshal(t, audit)
	assertNumberField(t, rawDBAudit, "profile_id", bigID)
	var back db.InterceptAudit
	if err := json.Unmarshal([]byte(`{"profile_id":`+bigIDStr+`,"correlation":"exact"}`), &back); err != nil {
		t.Fatalf("历史数字 profile_id 必须仍能解码（否则详情接口 500）: %v", err)
	}
	if back.ProfileID != bigID {
		t.Fatalf("profile_id 回读失真: %d", back.ProfileID)
	}

	conv := maxInt64
	detail := &db.InterceptDetail{
		InterceptApprovalRow: db.InterceptApprovalRow{
			InterceptPending: db.InterceptPending{ID: bigID, ConversationID: &conv, Status: "allowed"},
			ConvTitle:        "t", RuleName: "r",
		},
		Audit: audit,
	}

	// (b) + (c)：handler 实际写出的形状。
	raw := mustMarshal(t, interceptDetailDTO(detail))
	assertStringID(t, raw, "profile_id", bigIDStr)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "conversation_id", maxIDStr)
	assertKeyCount(t, raw, "profile_id", 1)
	// items 之外的非 ID 计数没有出现在本结构里；只断言 audit 仍在且是对象。
	if !strings.Contains(raw, `"audit":{`) {
		t.Fatalf("audit 应仍是对象: %s", raw)
	}

	// ProfileID == 0：内层与外层都 omitempty → 整个 key 消失（语义未变）。
	zero := *audit
	zero.ProfileID = 0
	dz := *detail
	dz.Audit = &zero
	assertOmitted(t, mustMarshal(t, interceptDetailDTO(&dz)), "profile_id")

	// audit 缺失：字段仍是 null（无 omitempty），不是被 omit。
	na := *detail
	na.Audit = nil
	rawNA := mustMarshal(t, interceptDetailDTO(&na))
	assertNullField(t, rawNA, "audit")
	assertStringID(t, rawNA, "id", bigIDStr)

	// nil 详情仍是 null（handler 走 404 分支，不会到这里，只锁转换函数语义）。
	if got := mustMarshal(t, interceptDetailDTO(nil)); got != "null" {
		t.Fatalf("interceptDetailDTO(nil)=%s", got)
	}
}

// R05：InterceptExecution 的 conversation_id / seq（activity 主键别名）→ string；
// 以及 interceptExecution 的 **map 响应**（db tag 不生效，必须在 map 里转）。
func TestIDOutputWireR05InterceptExecution(t *testing.T) {
	conv := maxInt64
	target := &db.InterceptExecution{ConversationID: &conv, TaskID: strPtr("42"), Session: "main:0", Seq: bigID}

	// db 结构体自身（tag 生效）。
	raw := mustMarshal(t, target)
	assertStringID(t, raw, "conversation_id", maxIDStr)
	assertStringID(t, raw, "seq", bigIDStr)
	assertStringID(t, raw, "task_id", "42")
	// Items 是 json:"-"，绝不上 wire。
	if strings.Contains(raw, `"items"`) {
		t.Fatalf("items 不应出现在输出: %s", raw)
	}
	roundTrip(t, target)

	// omitempty：nil conversation_id 仍被省略，不会变成 "0"。
	rawNil := mustMarshal(t, db.InterceptExecution{Seq: 1})
	assertOmitted(t, rawNil, "conversation_id")
	assertStringID(t, rawNil, "seq", "1")

	// handler 的 map 形状（interceptExecution 用 i64Ptr / i64s 显式转换）。
	rawMap := mustMarshal(t, map[string]any{
		"conversation_id": i64Ptr(target.ConversationID),
		"task_id":         target.TaskID,
		"session":         target.Session,
		"seq":             i64s(target.Seq),
		"items":           activityDTOs(target.Items),
	})
	assertStringID(t, rawMap, "conversation_id", maxIDStr)
	assertStringID(t, rawMap, "seq", bigIDStr)
	if !strings.Contains(rawMap, `"items":[]`) {
		t.Fatalf("items 应是数组: %s", rawMap)
	}

	// nil conversation_id 在 map 里仍是 null（不是 "0"、不是 0）。
	rawMapNil := mustMarshal(t, map[string]any{
		"conversation_id": i64Ptr(nil),
		"seq":             i64s(maxInt64),
	})
	assertNullField(t, rawMapNil, "conversation_id")
	assertStringID(t, rawMapNil, "seq", maxIDStr)
}

// R15 / R16：CommandRecord.id + exploration_id、LLMRecord.id → string；
// latency_ms / token 计数保持 number，TaskID / SessionID 仍是 string。
func TestIDOutputWireR15R16CommandsAndLLMRecord(t *testing.T) {
	cmd := db.CommandRecord{ID: bigID, ExpID: maxInt64, Worker: "w", Tool: "bash", Command: "id", Output: "uid", IsError: false}
	raw := mustMarshal(t, cmd)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "exploration_id", maxIDStr)
	roundTrip(t, cmd)

	// pgListCommands 的 map 形状：total 是计数，保持 number（即便数值很大）。
	rawList := mustMarshal(t, map[string]any{"commands": []db.CommandRecord{cmd}, "total": maxInt64})
	assertStringID(t, rawList, "id", bigIDStr)
	assertNumberField(t, rawList, "total", maxInt64)

	rec := db.LLMRecord{
		ID: bigID, Model: "m", ProfileName: "p", SessionID: "s", TaskID: maxIDStr, Worker: "w",
		LatencyMs: 12, InputTokens: 1200, OutputTokens: 340, CacheRead: 11, CacheWrite: 22,
		Status: "ok", Error: "", RequestBody: "{}", ResponseBody: "{}",
	}
	rawRec := mustMarshal(t, rec)
	assertStringID(t, rawRec, "id", bigIDStr)
	assertStringID(t, rawRec, "task_id", maxIDStr)
	assertNumberField(t, rawRec, "latency_ms", 12)
	assertNumberField(t, rawRec, "input_tokens", 1200)
	assertNumberField(t, rawRec, "output_tokens", 340)
	assertNumberField(t, rawRec, "cache_read", 11)
	assertNumberField(t, rawRec, "cache_write", 22)
	roundTrip(t, rec)

	// pgListLLMRecords 的 map 形状。
	rawRecords := mustMarshal(t, map[string]any{"records": []db.LLMRecord{rec}, "total": 1})
	assertStringID(t, rawRecords, "id", bigIDStr)
	assertNumberField(t, rawRecords, "total", 1)
}

// R17 / R18：FindingRetest 与 ActiveFindingRetest 的 id / finding_id / conversation_id → string；
// ActiveFindingRetest.FindingID 早已合规（回归锁）；conversation_id 无 omitempty，nil 仍是 null。
func TestIDOutputWireR17R18FindingRetests(t *testing.T) {
	conv := maxInt64
	retest := db.FindingRetest{
		ID: bigID, FindingID: maxInt64, ConversationID: &conv, Status: "running",
		Verdict: "reproduced", Notes: "n", Snapshot: json.RawMessage(`{"a":1}`),
		Summary: "s", Evidence: "e", Error: "",
	}
	raw := mustMarshal(t, retest)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "finding_id", maxIDStr)
	assertStringID(t, raw, "conversation_id", maxIDStr)
	roundTrip(t, retest)

	// 未关联会话：conversation_id 仍是 null（无 omitempty）。
	rawNil := mustMarshal(t, db.FindingRetest{ID: 1, FindingID: 2})
	assertNullField(t, rawNil, "conversation_id")
	assertStringID(t, rawNil, "id", "1")
	// snapshot 空时仍按 omitempty 省略。
	assertOmitted(t, rawNil, "snapshot")

	// listFindingRetests / startFindingRetest 的 map 形状。
	rawMap := mustMarshal(t, map[string]any{"retests": []db.FindingRetest{retest}, "created": true})
	assertStringID(t, rawMap, "id", bigIDStr)
	assertStringID(t, rawMap, "finding_id", maxIDStr)

	active := db.ActiveFindingRetest{ID: bigID, FindingID: maxInt64, ConversationID: bigID, Status: "running"}
	rawActive := mustMarshal(t, active)
	assertStringID(t, rawActive, "id", bigIDStr)
	assertStringID(t, rawActive, "finding_id", maxIDStr)
	assertStringID(t, rawActive, "conversation_id", bigIDStr)
	assertKeyCount(t, rawActive, "id", 1)
	roundTrip(t, active)

	rawActiveMap := mustMarshal(t, map[string]any{"retests": []db.ActiveFindingRetest{active}})
	assertStringID(t, rawActiveMap, "conversation_id", bigIDStr)
}

// R19 / R20：SkillCall.task_id、ConvTokenSummary.llm_profile_id → string；
// args_len 与 token 计数保持 number；llm_profile_id 无 omitempty，nil 仍是 null。
func TestIDOutputWireR19R20SkillCallAndConvTokens(t *testing.T) {
	call := db.SkillCall{AgentKey: "planner", TaskID: bigID, SessionID: "s", ArgsLen: 4096}
	raw := mustMarshal(t, call)
	assertStringID(t, raw, "task_id", bigIDStr)
	assertNumberField(t, raw, "args_len", 4096)
	roundTrip(t, call)

	// 无任务时 SQL COALESCE 出的 0 变成 "0"（与其它 ID 字段零值口径一致）。
	rawZero := mustMarshal(t, db.SkillCall{TaskID: 0})
	assertStringID(t, rawZero, "task_id", "0")

	// fsSkillUsage 的 map 形状。
	rawMap := mustMarshal(t, map[string]any{"calls": []db.SkillCall{call}})
	assertStringID(t, rawMap, "task_id", bigIDStr)

	profile := maxInt64
	sum := db.ConvTokenSummary{LLMProfileID: &profile, CreatedAt: "2026-10-18", InputTokens: 100, OutputTokens: 20, CacheReadTokens: 7, CacheWriteTokens: 3}
	rawSum := mustMarshal(t, sum)
	assertStringID(t, rawSum, "llm_profile_id", maxIDStr)
	assertNumberField(t, rawSum, "input_tokens", 100)
	assertNumberField(t, rawSum, "output_tokens", 20)
	assertNumberField(t, rawSum, "cache_read_tokens", 7)
	assertNumberField(t, rawSum, "cache_write_tokens", 3)
	roundTrip(t, sum)

	// 未绑定 profile：仍是 null（无 omitempty），不是 "0"。
	rawSumNil := mustMarshal(t, db.ConvTokenSummary{InputTokens: 1})
	assertNullField(t, rawSumNil, "llm_profile_id")
	assertNumberField(t, rawSumNil, "input_tokens", 1)
}

// R21：AgentTrigger.ID → string；interval_sec 是配置数值，保持 number；
// tool_names 是名称数组（不是 ID），last_fire 为 nil 时仍 omit。
func TestIDOutputWireR21AgentTrigger(t *testing.T) {
	tr := db.AgentTrigger{ID: bigID, AgentKey: "planner", Enabled: true, IntervalSec: 3600,
		IntervalMessage: "hi", ToolNames: []string{"bash", "http"}}
	raw := mustMarshal(t, tr)
	assertStringID(t, raw, "id", bigIDStr)
	assertNumberField(t, raw, "interval_sec", 3600)
	if !strings.Contains(raw, `"tool_names":["bash","http"]`) {
		t.Fatalf("tool_names 应仍是名称数组: %s", raw)
	}
	assertOmitted(t, raw, "last_fire")
	roundTrip(t, tr)

	// pgListTriggers / pgCreateTrigger 的 map 形状。
	rawMap := mustMarshal(t, map[string]any{"triggers": []db.AgentTrigger{{ID: maxInt64, IntervalSec: 60}}})
	assertStringID(t, rawMap, "id", maxIDStr)
	assertNumberField(t, rawMap, "interval_sec", 60)
}

// R22：TaskArchive 的 HTTP 输出。db 字段类型保持 []int64（由 source_task_ids 列的
// JSON 数字数组回读），只有 taskArchiveWire / taskArchivePageWire 把它覆盖成字符串数组。
func TestIDOutputWireR22TaskArchiveWire(t *testing.T) {
	category := maxInt64
	archive := db.TaskArchive{
		ID: bigID, TaskID: maxInt64, State: "ready", Phase: "done", Progress: 100,
		Warnings: json.RawMessage(`[]`), FormatVersion: 2, SHA256: "abc",
		OriginalSize: maxInt64, CompressedSize: 123, TaskName: "t", OriginalStatus: "done",
		CategoryIDSnapshot:      &category,
		CategoryNameSnapshot:    "cat",
		SourceTaskIDs:           []int64{bigID, 1, maxInt64},
		RemainingTimeoutSeconds: maxInt64,
		DataCounts:              json.RawMessage(`{"findings":3}`),
		AggregateStats:          json.RawMessage(`{}`),
	}

	// 回归锁：db 层仍是数字数组（[]int64 上的 ,string 会被 encoding/json 忽略，不能靠 tag）。
	rawDB := mustMarshal(t, archive)
	if !strings.Contains(rawDB, `"source_task_ids":[`+bigIDStr+`,1,`+maxIDStr+`]`) {
		t.Fatalf("db.TaskArchive.source_task_ids 必须仍是数字数组（DB JSON 回读依赖）: %s", rawDB)
	}
	roundTrip(t, archive)

	// wire：字符串数组 + 只出现一次（外层屏蔽嵌入字段）。
	wire := taskArchiveDTO(&archive)
	rawWire := mustMarshal(t, wire)
	assertKeyCount(t, rawWire, "source_task_ids", 1)
	assertStringArray(t, rawWire, "source_task_ids", bigIDStr, "1", maxIDStr)
	assertStringID(t, rawWire, "id", bigIDStr)
	assertStringID(t, rawWire, "task_id", maxIDStr)
	assertStringID(t, rawWire, "category_id", maxIDStr)
	// 非 ID 数值保持 number，即便超过 2^53。
	assertNumberField(t, rawWire, "progress", 100)
	assertNumberField(t, rawWire, "format_version", 2)
	assertNumberField(t, rawWire, "original_size", maxInt64)
	assertNumberField(t, rawWire, "compressed_size", 123)
	assertNumberField(t, rawWire, "remaining_timeout_seconds", maxInt64)

	// 分页：逐项转换；total / page / size 是计数，保持 number。
	page := db.TaskArchivePage{Items: []db.TaskArchive{archive, archive}, Total: 2, Page: 1, Size: 20}
	rawPage := mustMarshal(t, taskArchivePageDTO(page))
	assertStringArray(t, rawPage, "source_task_ids", bigIDStr, "1", maxIDStr)
	assertNumberField(t, rawPage, "total", 2)
	assertNumberField(t, rawPage, "page", 1)
	assertNumberField(t, rawPage, "size", 20)
	if strings.Count(rawPage, `"source_task_ids":["`) != 2 {
		t.Fatalf("分页每一项都应是字符串数组: %s", rawPage)
	}

	// nil / 空切片语义：nil → null，[] → []；空分页 Items 仍是 null（ListTaskArchives 空结果是 nil）。
	rawNil := mustMarshal(t, taskArchiveDTO(&db.TaskArchive{ID: 1, TaskID: 1}))
	assertNullField(t, rawNil, "source_task_ids")
	assertOmitted(t, rawNil, "category_id")
	rawEmpty := mustMarshal(t, taskArchiveDTO(&db.TaskArchive{ID: 1, TaskID: 1, SourceTaskIDs: []int64{}}))
	if !strings.Contains(rawEmpty, `"source_task_ids":[]`) {
		t.Fatalf("空 source_task_ids 必须仍是 []: %s", rawEmpty)
	}
	rawPageNil := mustMarshal(t, taskArchivePageDTO(db.TaskArchivePage{Total: 0, Page: 1, Size: 20}))
	assertNullField(t, rawPageNil, "items")
	if got := mustMarshal(t, taskArchiveDTO(nil)); got != "null" {
		t.Fatalf("taskArchiveDTO(nil)=%s", got)
	}
}

// R25：ChatMention.ID → string（ChatMentionPage.Items 嵌套）；next_cursor 是
// base64 不透明字符串（内部 chatMentionCursor 的数字 ID 不外露），无值时仍 omit。
func TestIDOutputWireR25ChatMentionPage(t *testing.T) {
	page := db.ChatMentionPage{
		Items: []db.ChatMention{
			{Kind: "finding", ID: bigID, Label: "L", Description: "D"},
			{Kind: "asset", ID: maxInt64, Label: "a.com", Description: ""},
		},
		NextCursor: "eyJpZCI6MX0",
	}
	raw := mustMarshal(t, page)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "id", maxIDStr)
	assertStringID(t, raw, "next_cursor", "eyJpZCI6MX0")
	roundTrip(t, page)

	// 无下一页：next_cursor 仍按 omitempty 省略；空 items 是 []（db 侧 make 过）。
	rawNoCursor := mustMarshal(t, db.ChatMentionPage{Items: []db.ChatMention{}})
	assertOmitted(t, rawNoCursor, "next_cursor")
	if !strings.Contains(rawNoCursor, `"items":[]`) {
		t.Fatalf("空 items 应仍是 []: %s", rawNoCursor)
	}

	// 内部 cursor 类型不受影响：它被 base64 成不透明字符串，不是前端数字字段。
	blob, err := json.Marshal(struct {
		ID int64 `json:"id"`
	}{ID: bigID})
	if err != nil || !strings.Contains(string(blob), `"id":`+bigIDStr) {
		t.Fatalf("内部 cursor 形态不应改变（base64 前）: %s err=%v", blob, err)
	}
}

// R26：MCPServer.ID → string（pgListMCP 的 map{"servers":[]}）；
// 同时锁住 W4b-1 的 saveMCPRequest 屏蔽：加 ,string 后请求体解码仍不受影响。
func TestIDOutputWireR26MCPServerAndSaveRequest(t *testing.T) {
	srv := db.MCPServer{ID: bigID, Name: "s", Transport: "stdio", Command: "npx",
		Args: json.RawMessage(`["-y"]`), Env: json.RawMessage(`{"K":"V"}`), URL: "http://x",
		Enabled: true, Insecure: false, Tools: []string{"t1"}}
	raw := mustMarshal(t, srv)
	assertStringID(t, raw, "id", bigIDStr)
	roundTrip(t, srv)

	rawMax := mustMarshal(t, db.MCPServer{ID: maxInt64, Name: "m"})
	assertStringID(t, rawMax, "id", maxIDStr)

	// pgListMCP 的 map 形状。
	rawList := mustMarshal(t, map[string]any{"servers": []*db.MCPServer{&srv}})
	assertStringID(t, rawList, "id", bigIDStr)

	// 请求体：外层 ID json.RawMessage 屏蔽 db.MCPServer.ID 的 int64 解码，
	// 因此 string / number / 缺省 / null 都不会因 ,string 而报错，且 MCPServer.ID 恒为 0。
	for _, body := range []string{
		`{"id":"` + maxIDStr + `","name":"s","transport":"stdio"}`,
		`{"id":12,"name":"s","transport":"stdio"}`,
		`{"id":0,"name":"s","transport":"stdio"}`,
		`{"id":null,"name":"s","transport":"stdio"}`,
		`{"name":"s","transport":"stdio"}`,
	} {
		var req saveMCPRequest
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("saveMCPRequest 解码 %s 失败: %v", body, err)
		}
		if req.MCPServer.ID != 0 {
			t.Fatalf("db.MCPServer.ID 必须被外层字段屏蔽，得到 %d（body=%s）", req.MCPServer.ID, body)
		}
		id, err := parseProfileSaveID(req.ID)
		if err != nil {
			t.Fatalf("parseProfileSaveID(%s) 失败: %v", body, err)
		}
		switch body {
		case `{"id":"` + maxIDStr + `","name":"s","transport":"stdio"}`:
			if id != maxInt64 {
				t.Fatalf("大 ID 解析失真: %d", id)
			}
		case `{"id":12,"name":"s","transport":"stdio"}`:
			if id != 12 {
				t.Fatalf("数字 id 兼容失败: %d", id)
			}
		default:
			if id != 0 {
				t.Fatalf("新建哨兵应为 0，得到 %d", id)
			}
		}
	}

	// 非法 ID 仍被拒绝（负数 / 非数字 / 小数）。
	for _, body := range []string{`{"id":-1}`, `{"id":"abc"}`, `{"id":1.5}`, `{"id":"` + maxIDStr + `0"}`} {
		var req saveMCPRequest
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			continue // 解码阶段就失败也算拒绝
		}
		if _, err := parseProfileSaveID(req.ID); err == nil {
			t.Fatalf("非法 id 应被拒绝: %s", body)
		}
	}
}

// 并入的 R21 findings（O-1）：sendWorkerMessage 响应的 intent ID → string；
// request_id 是客户端幂等键（string），accepted / state 是状态字段。
func TestIDOutputWireR21FindingsSendWorkerMessage(t *testing.T) {
	raw := mustMarshal(t, map[string]any{
		"id":         i64s(bigID),
		"state":      "running",
		"accepted":   true,
		"request_id": "req-1",
	})
	assertStringID(t, raw, "id", bigIDStr)
	if !strings.Contains(raw, `"request_id":"req-1"`) || !strings.Contains(raw, `"accepted":true`) {
		t.Fatalf("非 ID 字段形态不应改变: %s", raw)
	}

	// 同族 intent 响应（S03 已修，回归锁：批量 reopened 计数仍是 number）。
	rawBatch := mustMarshal(t, map[string]any{"reopened": 3, "queued": true})
	assertNumberField(t, rawBatch, "reopened", 3)
}

// 嵌套回归：FindingStats.Tasks（R14，W4b-2b）在本批之后仍是 string，
// 且 total / count 等计数保持 number。
func TestIDOutputWireNestedFindingStatsStillString(t *testing.T) {
	stats := db.FindingStats{
		Total: 7, Pending: 2, Critical: 1, High: 2, Medium: 3, Low: 1,
		VulnClasses: []string{"sqli"},
		Tasks:       []db.FindingTaskOption{{ID: bigID, Name: "t", Description: "d", Count: 7}},
	}
	raw := mustMarshal(t, stats)
	assertStringID(t, raw, "id", bigIDStr)
	assertNumberField(t, raw, "total", 7)
	assertNumberField(t, raw, "count", 7)
	roundTrip(t, stats)
}
