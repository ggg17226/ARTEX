package server

// W4b-2a（P03 输出侧 string 化 · 第一批）离线 wire 断言：
// 覆盖 S01–S08 与 R23/R24 中被改的 server 自建响应 / DTO / map / 旁路输出。
// 口径：
//  1. 大于 2^53 的 64 位 ID 必须序列化为**带引号的 string**，且十进制数值不失真；
//  2. 非 ID 计数（内存 seq、sequence、批量 reopened、attempts 等）保持 JSON number；
//  3. nil / 0 哨兵的 omit 行为不变（BatchID=nil、DBID 空串、ProfileID=nil）。
//
// 不依赖数据库，纯 json.Marshal 断言。

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/sidequestion"
)

const (
	bigID     = int64(9007199254740993) // 2^53 + 1：JS Number 精度之外
	maxInt64  = int64(math.MaxInt64)    // 9223372036854775807
	bigIDStr  = "9007199254740993"
	maxIDStr  = "9223372036854775807"
	oneBigStr = `"9007199254740993"`
)

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return string(b)
}

// assertStringID 断言 raw 中 key 的值是带引号的十进制 string 且数值不失真。
func assertStringID(t *testing.T, raw, key, want string) {
	t.Helper()
	wantToken := `"` + key + `":"` + want + `"`
	if !strings.Contains(raw, wantToken) {
		t.Fatalf("expected %s in %s", wantToken, raw)
	}
	// 数字形态（不带引号）不得同时出现。
	if strings.Contains(raw, `"`+key+`:`+want) {
		t.Fatalf("key %s still emitted as number in %s", key, raw)
	}
}

// assertNumberField 断言 key 的值是 JSON number（无引号）。
func assertNumberField(t *testing.T, raw, key string, want int64) {
	t.Helper()
	if !strings.Contains(raw, `"`+key+`":`+i64s(want)) {
		t.Fatalf("expected numeric %s=%d in %s", key, want, raw)
	}
	if strings.Contains(raw, `"`+key+`":"`) {
		t.Fatalf("key %s unexpectedly stringified in %s", key, raw)
	}
}

// assertOmitted 断言 key 完全不出现在输出里。
func assertOmitted(t *testing.T, raw, key string) {
	t.Helper()
	if strings.Contains(raw, `"`+key+`"`) {
		t.Fatalf("key %s should be omitted in %s", key, raw)
	}
}

// S01：ActivityDTO.Seq 承载 db.Activity.ID（BIGINT 主键别名）→ wire string；
// token 计数保持 number；activity cursor 响应 string。
func TestIDOutputWireS01ActivitySeqAndCursor(t *testing.T) {
	raw := mustMarshal(t, activityDTO(db.Activity{ID: bigID, Worker: "w", Kind: "result", InputTokens: ptrInt(7)}))
	assertStringID(t, raw, "seq", bigIDStr)
	assertNumberField(t, raw, "input_tokens", 7)

	rawMax := mustMarshal(t, activityDTO(db.Activity{ID: maxInt64}))
	assertStringID(t, rawMax, "seq", maxIDStr)

	// handler 写出的 cursor map 形态（server.go activity / activityHistory）。
	rawCursor := mustMarshal(t, map[string]any{"items": []ActivityDTO{}, "cursor": i64s(bigID)})
	assertStringID(t, rawCursor, "cursor", bigIDStr)
	rawHist := mustMarshal(t, map[string]any{"snapshot_cursor": i64s(bigID), "earliest_cursor": i64s(0), "has_more": true})
	assertStringID(t, rawHist, "snapshot_cursor", bigIDStr)
	assertStringID(t, rawHist, "earliest_cursor", "0")
}

func ptrInt(v int) *int { return &v }

// S02：LogLine.DBID（server_logs.id）→ string，空串 omit；LogLine.Seq 内存计数保持 number。
func TestIDOutputWireS02LogLineDBID(t *testing.T) {
	raw := mustMarshal(t, LogLine{Seq: 42, DBID: i64s(bigID), TS: "t", Level: "info", Tag: "pg", Text: "x"})
	assertStringID(t, raw, "db_id", bigIDStr)
	assertNumberField(t, raw, "seq", 42)

	rawEmpty := mustMarshal(t, LogLine{Seq: 1, Text: "pre-persistence"})
	assertOmitted(t, rawEmpty, "db_id")
	assertNumberField(t, rawEmpty, "seq", 1)
}

// S03：intentControlResult.ID → string；rerun 响应 reopened 是意图 ID → string；
// 批量 reopened 计数保持 number。
func TestIDOutputWireS03IntentControl(t *testing.T) {
	raw := mustMarshal(t, intentControlResult{ID: i64s(bigID), State: "paused"})
	assertStringID(t, raw, "id", bigIDStr)

	rawRerun := mustMarshal(t, map[string]any{"id": "123", "reopened": i64s(bigID), "queued": false})
	assertStringID(t, rawRerun, "reopened", bigIDStr)

	rawBatch := mustMarshal(t, map[string]any{"id": "123", "reopened": 3, "queued": true})
	assertNumberField(t, rawBatch, "reopened", 3)
}

// S04：taskLLMResolution.ProfileID → *string；nil 仍 omit。
func TestIDOutputWireS04TaskLLMResolution(t *testing.T) {
	id := i64s(bigID)
	raw := mustMarshal(t, taskLLMResolution{ProfileID: &id, Name: "p", Source: "task_chain", Available: true})
	assertStringID(t, raw, "profile_id", bigIDStr)

	rawNil := mustMarshal(t, taskLLMResolution{Source: "global", Reason: "none"})
	assertOmitted(t, rawNil, "profile_id")
}

// S05：llmAuditProfile.ID → string；经 ActivityDTO.Metadata（RawMessage）直达
// HTTP/SSE 的 previous.id / next.id 必须是 string。
func TestIDOutputWireS05LLMAuditMetadata(t *testing.T) {
	meta := llmActivityMetadata{LLMTransition: llmTransitionAudit{
		Mode:     "automatic",
		Reason:   "quota",
		Previous: &llmAuditProfile{ID: i64s(bigID), Name: "prev"},
		Next:     &llmAuditProfile{ID: i64s(maxInt64), Name: "next", Format: "openai", Model: "m"},
	}}
	raw := mustMarshal(t, meta)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "id", maxIDStr)

	// 该结构只 marshal、不经 unmarshal 回读；确认 metadata 嵌入 ActivityDTO 后仍是 string。
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	rawDTO := mustMarshal(t, activityDTO(db.Activity{ID: 1, Metadata: rawMeta}))
	if !strings.Contains(rawDTO, oneBigStr) {
		t.Fatalf("metadata id lost string form: %s", rawDTO)
	}

	if label := llmAuditProfileLabel(&llmAuditProfile{ID: i64s(bigID)}); label != "配置 #"+bigIDStr {
		t.Fatalf("label=%q", label)
	}
}

// S06：旁路 wire DTO：ordinal / model.profile_id → string；sequence 计数保持
// number；next_cursor → string。
func TestIDOutputWireS06SideQuestionWire(t *testing.T) {
	e := &sidequestion.Exchange{
		ID: "req-1", ClientID: "c1", Question: "q", Answer: "a", Status: "completed",
		Model:    sidequestion.Model{ProfileID: bigID, Name: "prof", Format: "openai", Model: "m", Identity: "idn", Streaming: true, WindowTokens: 128},
		Sequence: 7, Ordinal: bigID,
		Context: sidequestion.ContextInfo{Phase: "direct", RecentExchanges: 2},
	}
	raw := mustMarshal(t, sideExchangeWireOf(e))
	assertStringID(t, raw, "ordinal", bigIDStr)
	assertStringID(t, raw, "profile_id", bigIDStr)
	assertNumberField(t, raw, "sequence", 7)
	assertNumberField(t, raw, "recent_exchanges", 2)
	if !strings.Contains(raw, `"id":"req-1"`) {
		t.Fatalf("exchange id must stay string: %s", raw)
	}

	// GET 响应形态：items / current / next_cursor / snapshot.model。
	rawGet := mustMarshal(t, map[string]any{
		"items":       sideExchangeWireOfSlice([]sidequestion.Exchange{*e}),
		"current":     sideExchangeWireOf(e),
		"next_cursor": i64s(bigID),
		"snapshot":    map[string]any{"model": sideModelWireOf(e.Model), "available": true},
	})
	assertStringID(t, rawGet, "next_cursor", bigIDStr)
	assertStringID(t, rawGet, "ordinal", bigIDStr)
	assertStringID(t, rawGet, "profile_id", bigIDStr)

	// current=nil 仍是 JSON null（omit 语义不变）。
	rawNil := mustMarshal(t, map[string]any{"current": sideExchangeWireOf(nil)})
	if !strings.Contains(rawNil, `"current":null`) {
		t.Fatalf("nil current must stay null: %s", rawNil)
	}

	// 持久化结构 tag 未被污染：sidequestion.Exchange 仍按原 int64 tag 序列化
	// （db/side_questions.go 从 JSON 列 unmarshal 回读依赖这一点）。
	rawPersist := mustMarshal(t, *e)
	if !strings.Contains(rawPersist, `"ordinal":`+bigIDStr) || strings.Contains(rawPersist, `"ordinal":"`) {
		t.Fatalf("persisted Exchange.ordinal must remain number: %s", rawPersist)
	}
}

// S07：局部 map / result 的数值 ID → string；相邻计数保持 number。
func TestIDOutputWireS07LocalMaps(t *testing.T) {
	// createCompany / notify create-update / pgSaveMCP / ScopeSentry datasource。
	rawCompany := mustMarshal(t, map[string]any{"id": i64s(bigID), "created": true, "scope_added": 3, "scope_skipped": 1, "scope_invalid": 0})
	assertStringID(t, rawCompany, "id", bigIDStr)
	assertNumberField(t, rawCompany, "scope_added", 3)

	rawMCP := mustMarshal(t, map[string]any{"id": i64s(maxInt64), "enabled": true})
	assertStringID(t, rawMCP, "id", maxIDStr)

	// detachTaskAsset：detached 是 asset ID，不是数量。
	rawDetach := mustMarshal(t, map[string]any{"detached": i64s(bigID)})
	assertStringID(t, rawDetach, "detached", bigIDStr)

	// insertAssets 局部 result。
	type result struct {
		Index int    `json:"index"`
		ID    string `json:"id"`
		Type  string `json:"type"`
	}
	rawIns := mustMarshal(t, result{Index: 0, ID: i64s(bigID), Type: "ip"})
	assertStringID(t, rawIns, "id", bigIDStr)
	assertNumberField(t, rawIns, "index", 0)

	// agent detail llm_profiles[].id。
	rawProf := mustMarshal(t, map[string]any{"id": i64s(bigID), "name": "p", "model": "m", "is_default": true})
	assertStringID(t, rawProf, "id", bigIDStr)
}

// S08：visibility.mcp []int64 → idStrings；skill 名称不动。
func TestIDOutputWireS08Visibility(t *testing.T) {
	raw := mustMarshal(t, map[string]any{"mcp": idStrings([]int64{bigID, 1, maxInt64}), "skill": []string{"recon"}})
	if !strings.Contains(raw, `"mcp":["`+bigIDStr+`","1","`+maxIDStr+`"]`) {
		t.Fatalf("visibility.mcp must be string array: %s", raw)
	}
	if !strings.Contains(raw, `"skill":["recon"]`) {
		t.Fatalf("skill names must stay strings: %s", raw)
	}
	// 空列表序列化成 []，不是 null。
	if got := mustMarshal(t, idStrings(nil)); got != "[]" {
		t.Fatalf("idStrings(nil)=%s", got)
	}
}

// R23/R24：notify DTO `,string` tag；BatchID=nil omit、非 nil string；计数保持 number。
func TestIDOutputWireR23R24NotifyDTO(t *testing.T) {
	rawCh := mustMarshal(t, notifyChannelDTO{ID: bigID, Name: "c", Kind: "webhook", RatePerMin: 60})
	assertStringID(t, rawCh, "id", bigIDStr)
	assertNumberField(t, rawCh, "rate_per_min", 60)

	rawNilBatch := mustMarshal(t, notifyDeliveryDTO{ID: bigID, FindingID: maxInt64, ChannelID: bigID, State: "pending", Attempts: 2})
	assertStringID(t, rawNilBatch, "id", bigIDStr)
	assertStringID(t, rawNilBatch, "channel_id", bigIDStr)
	assertStringID(t, rawNilBatch, "finding_id", maxIDStr)
	assertOmitted(t, rawNilBatch, "batch_id")
	assertNumberField(t, rawNilBatch, "attempts", 2)

	batch := maxInt64
	rawBatch := mustMarshal(t, notifyDeliveryDTO{ID: 1, ChannelID: 1, BatchID: &batch})
	assertStringID(t, rawBatch, "batch_id", maxIDStr)
}
