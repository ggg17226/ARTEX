package server

// W4b-1 输入侧契约测试（P03：64 位 ID 对外一律 string）。
//
// 这些请求体所在 handler 的前置依赖（Manager / pg 连接）在离线测试里不可用
// （仓库内 DB handler 测试统一 t.Skip），因此这里直接对「wire 结构体 + 严格解析」
// 这两层做断言：
//   1. string 形式的 ID（含 >2^53 的大 ID）能解码并解析成内部 int64；
//   2. 非法字符串（"0"、"-1"、"abc"、""、小数、>MaxInt64）解析报错，handler 据此回 400；
//   3. llm_profile_id 三态（缺省 / null / 字符串 ID）互不混淆。
// 数字兼容的过渡口径（R20 M-1 修正，原文声称「每个用例都断言数字写法不再被接受」，
// 对 E04/E07 不成立）：
//   - E01 / E02 / E03 / E05 / E06 是 **string-only**：JSON number 解码即失败，用例显式钉住；
//   - E04（POST /api/mcp 的 save-ID）与 E07（agent 的 llm_profile_id）走
//     parseProfileSaveID / parseOptionalIDRaw，**过渡期仍接受 string|number**。
//     这不是松懈：它们的输出侧（MCPServer.ID、profile ID）在 W4b-2 之前仍以数字上线，
//     前端回传的就是 number；先切 string-only 会把可用的编辑流程打成 400。
//     待输出侧全量 string 化（W4b-2 已落地）后，才可在后续批次收紧这两处。

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestParseIDListField(t *testing.T) {
	t.Run("string ids incl. big id", func(t *testing.T) {
		got, err := parseIDListField([]string{"1", "9007199254740993", "9223372036854775807"})
		if err != nil {
			t.Fatalf("want ok, got %v", err)
		}
		want := []int64{1, 9007199254740993, math.MaxInt64}
		if len(got) != len(want) {
			t.Fatalf("got %v want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	})
	t.Run("absent and empty are nil", func(t *testing.T) {
		if got, err := parseIDListField(nil); got != nil || err != nil {
			t.Fatalf("nil input: got %v, err %v", got, err)
		}
		if got, err := parseIDListField([]string{}); got != nil || err != nil {
			t.Fatalf("empty input: got %v, err %v", got, err)
		}
	})
	t.Run("surrounding spaces tolerated", func(t *testing.T) {
		got, err := parseIDListField([]string{" 7 "})
		if err != nil || len(got) != 1 || got[0] != 7 {
			t.Fatalf("got %v, err %v", got, err)
		}
	})
	for _, bad := range []string{"", " ", "0", "-1", "1.5", "abc", "9223372036854775808", "99999999999999999999"} {
		t.Run("reject "+bad, func(t *testing.T) {
			if _, err := parseIDListField([]string{"1", bad}); err == nil {
				t.Fatalf("want error for %q", bad)
			}
		})
	}
	t.Run("error names the offending index", func(t *testing.T) {
		_, err := parseIDListField([]string{"1", "abc", "3"})
		if err == nil || !strings.Contains(err.Error(), "ids[1]") {
			t.Fatalf("want ids[1] in error, got %v", err)
		}
	})
}

func TestParseOptionalIDStringKeepsZeroSentinel(t *testing.T) {
	for _, s := range []string{"", "0", " 0 "} {
		if v, err := parseOptionalIDString(s); v != 0 || err != nil {
			t.Fatalf("parseOptionalIDString(%q) = %d, %v; want 0, nil", s, v, err)
		}
	}
	for _, tc := range []struct {
		in   string
		want int64
	}{{"1", 1}, {"9007199254740993", 9007199254740993}, {"9223372036854775807", math.MaxInt64}} {
		if v, err := parseOptionalIDString(tc.in); err != nil || v != tc.want {
			t.Fatalf("parseOptionalIDString(%q) = %d, %v; want %d", tc.in, v, err, tc.want)
		}
	}
	for _, bad := range []string{"-1", "-0", "00", "abc", "1.5", "9223372036854775808"} {
		if _, err := parseOptionalIDString(bad); err == nil {
			t.Fatalf("want error for %q", bad)
		}
	}
}

// --- POST /api/tasks/{id}/assets (E01) ---

func TestAttachTaskAssetsRequestAcceptsStringAssetIDs(t *testing.T) {
	// R20 M-3：大 ID 用 2^53+1 与 MaxInt64（2^53 本身 float64 可精确表示，不能单独作 >2^53 证据）。
	var req attachTaskAssetsRequest
	if err := json.Unmarshal([]byte(`{"asset_ids":["1","9007199254740993","9223372036854775807"],"source_summary":" s "}`), &req); err != nil {
		t.Fatalf("string asset_ids must decode: %v", err)
	}
	ids, err := req.assetIDs()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 9007199254740993 || ids[2] != math.MaxInt64 {
		t.Fatalf("got %v", ids)
	}
}

func TestAttachTaskAssetsRequestRejectsBadAssetIDs(t *testing.T) {
	t.Run("invalid string id", func(t *testing.T) {
		for _, body := range []string{`{"asset_ids":["abc"]}`, `{"asset_ids":["1","0"]}`, `{"asset_ids":["-1"]}`, `{"asset_ids":["1.5"]}`, `{"asset_ids":[""]}`} {
			var req attachTaskAssetsRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if _, err := req.assetIDs(); err == nil {
				t.Fatalf("want parse error for %s", body)
			}
		}
	})
	t.Run("numeric ids are out of contract", func(t *testing.T) {
		var req attachTaskAssetsRequest
		if err := json.Unmarshal([]byte(`{"asset_ids":[1,2]}`), &req); err == nil {
			t.Fatal("P03 wire is string[]; number[] must not decode")
		}
	})
	t.Run("scope only", func(t *testing.T) {
		var req attachTaskAssetsRequest
		if err := json.Unmarshal([]byte(`{"scope":["example.com"]}`), &req); err != nil {
			t.Fatal(err)
		}
		if ids, err := req.assetIDs(); ids != nil || err != nil {
			t.Fatalf("got %v, %v", ids, err)
		}
		if len(req.Scope) != 1 {
			t.Fatalf("scope lost: %+v", req.Scope)
		}
	})
}

// --- DELETE /api/assets (E02) ---

func TestDeleteAssetsRequestAcceptsStringIDs(t *testing.T) {
	// R20 M-3：补 2^53+1 与 MaxInt64。
	var req deleteAssetsRequest
	if err := json.Unmarshal([]byte(`{"ids":["1","9007199254740993","9223372036854775807"]}`), &req); err != nil {
		t.Fatalf("string ids must decode: %v", err)
	}
	ids, err := req.parsedIDs()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ids) != 3 || ids[1] != 9007199254740993 || ids[2] != math.MaxInt64 {
		t.Fatalf("got %v", ids)
	}
}

func TestDeleteAssetsRequestRejectsBadIDs(t *testing.T) {
	for _, body := range []string{`{"ids":["0"]}`, `{"ids":["abc"]}`, `{"ids":["9223372036854775808"]}`} {
		var req deleteAssetsRequest
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if _, err := req.parsedIDs(); err == nil {
			t.Fatalf("want parse error for %s", body)
		}
	}
	var req deleteAssetsRequest
	if err := json.Unmarshal([]byte(`{"ids":[1]}`), &req); err == nil {
		t.Fatal("P03 wire is string[]; number[] must not decode")
	}
}

// --- POST /api/assets (E03) ---

func TestInsertAssetsRequestTaskID(t *testing.T) {
	t.Run("string task id", func(t *testing.T) {
		// R20 M-3：2^53+1 与 MaxInt64 各测一次（2^53 本身不算 >2^53 证据）。
		for _, want := range []int64{9007199254740993, math.MaxInt64} {
			var req insertAssetsRequest
			body := `{"task_id":"` + i64s(want) + `","assets":[{"type":"ip","ip":"1.2.3.4"}]}`
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("string task_id must decode: %v", err)
			}
			id, err := req.parsedTaskID()
			if err != nil || id != want {
				t.Fatalf("got %d, %v", id, err)
			}
			if len(req.Assets) != 1 || req.Assets[0].IP != "1.2.3.4" {
				t.Fatalf("assets lost: %+v", req.Assets)
			}
		}
	})
	t.Run("absent and zero keep the no-task sentinel", func(t *testing.T) {
		for _, body := range []string{`{"assets":[]}`, `{"task_id":"","assets":[]}`, `{"task_id":"0","assets":[]}`} {
			var req insertAssetsRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if id, err := req.parsedTaskID(); id != 0 || err != nil {
				t.Fatalf("%s: got %d, %v", body, id, err)
			}
		}
	})
	t.Run("invalid task id", func(t *testing.T) {
		for _, body := range []string{`{"task_id":"-1"}`, `{"task_id":"abc"}`, `{"task_id":"1.5"}`, `{"task_id":"9223372036854775808"}`} {
			var req insertAssetsRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if _, err := req.parsedTaskID(); err == nil {
				t.Fatalf("want parse error for %s", body)
			}
		}
	})
	t.Run("numeric task id is out of contract", func(t *testing.T) {
		var req insertAssetsRequest
		if err := json.Unmarshal([]byte(`{"task_id":123}`), &req); err == nil {
			t.Fatal("P03 wire is string; number must not decode")
		}
	})
}

// --- PUT /api/agents/{key}/config (E07, 三态) ---

func TestAgentConfigRequestLLMProfileIDTriState(t *testing.T) {
	t.Run("absent means no change", func(t *testing.T) {
		var req agentConfigRequest
		if err := json.Unmarshal([]byte(`{"max_turns":3}`), &req); err != nil {
			t.Fatal(err)
		}
		if req.LLMProfileID != nil {
			t.Fatalf("absent field must stay nil, got %s", req.LLMProfileID)
		}
	})
	t.Run("explicit null means unbind", func(t *testing.T) {
		var req agentConfigRequest
		if err := json.Unmarshal([]byte(`{"llm_profile_id":null}`), &req); err != nil {
			t.Fatal(err)
		}
		if req.LLMProfileID == nil {
			t.Fatal("explicit null must stay distinguishable from absent")
		}
		id, err := parseOptionalIDRaw(req.LLMProfileID)
		if err != nil || id != nil {
			t.Fatalf("null must resolve to unbind (nil, nil), got %v, %v", id, err)
		}
	})
	t.Run("string id binds", func(t *testing.T) {
		for _, tc := range []struct {
			body string
			want int64
		}{
			{`{"llm_profile_id":"5"}`, 5},
			{`{"llm_profile_id":"9007199254740993"}`, 9007199254740993}, // 2^53+1（R20 M-3）
			{`{"llm_profile_id":"9223372036854775807"}`, math.MaxInt64},
			{`{"llm_profile_id":5}`, 5}, // 历史数字写法仍兼容
		} {
			var req agentConfigRequest
			if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
				t.Fatalf("decode %s: %v", tc.body, err)
			}
			id, err := parseOptionalIDRaw(req.LLMProfileID)
			if err != nil {
				t.Fatalf("%s: %v", tc.body, err)
			}
			if id == nil || *id != tc.want {
				t.Fatalf("%s: got %v, want %d", tc.body, id, tc.want)
			}
		}
	})
	t.Run("zero and garbage never impersonate null", func(t *testing.T) {
		for _, body := range []string{
			`{"llm_profile_id":0}`, `{"llm_profile_id":"0"}`, `{"llm_profile_id":-1}`,
			`{"llm_profile_id":"-1"}`, `{"llm_profile_id":""}`, `{"llm_profile_id":"abc"}`,
			`{"llm_profile_id":1.5}`, `{"llm_profile_id":"9223372036854775808"}`,
		} {
			var req agentConfigRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if req.LLMProfileID == nil {
				t.Fatalf("%s: field must be present", body)
			}
			if id, err := parseOptionalIDRaw(req.LLMProfileID); err == nil {
				t.Fatalf("want error for %s, got %v", body, id)
			}
		}
	})
}

// --- PUT /api/agents/{key}/visibility (E05) ---

func TestAgentVisibilityRequestAcceptsStringMCPIDs(t *testing.T) {
	// R20 M-3：补 2^53+1 与 MaxInt64。
	var req agentVisibilityRequest
	if err := json.Unmarshal([]byte(`{"mcp":["1","9007199254740993","9223372036854775807"],"skill":["nmap"]}`), &req); err != nil {
		t.Fatalf("string mcp ids must decode: %v", err)
	}
	ids, err := req.parsedMCP()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ids) != 3 || ids[1] != 9007199254740993 || ids[2] != math.MaxInt64 {
		t.Fatalf("got %v", ids)
	}
	if len(req.Skill) != 1 || req.Skill[0] != "nmap" {
		t.Fatalf("skill lost: %+v", req.Skill)
	}
}

func TestAgentVisibilityRequestRejectsBadMCPIDs(t *testing.T) {
	for _, body := range []string{`{"mcp":["0"]}`, `{"mcp":["abc"]}`, `{"mcp":["1","-2"]}`} {
		var req agentVisibilityRequest
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if _, err := req.parsedMCP(); err == nil {
			t.Fatalf("want parse error for %s", body)
		}
	}
	t.Run("absent and empty clear the set", func(t *testing.T) {
		for _, body := range []string{`{}`, `{"mcp":[]}`} {
			var req agentVisibilityRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatal(err)
			}
			if ids, err := req.parsedMCP(); ids != nil || err != nil {
				t.Fatalf("%s: got %v, %v", body, ids, err)
			}
		}
	})
	t.Run("numeric ids are out of contract", func(t *testing.T) {
		var req agentVisibilityRequest
		if err := json.Unmarshal([]byte(`{"mcp":[1]}`), &req); err == nil {
			t.Fatal("P03 wire is string[]; number[] must not decode")
		}
	})
}

// --- POST /api/visibility/toggle (E06) ---

func TestToggleVisibilityRequestStringIDs(t *testing.T) {
	t.Run("string ids", func(t *testing.T) {
		// R20 M-3：resource_id 用 2^53+1 与 MaxInt64 各测一次。
		for _, want := range []int64{9007199254740993, math.MaxInt64} {
			var req toggleVisibilityRequest
			body := `{"agent_id":"7","kind":"mcp","resource_id":"` + i64s(want) + `","visible":true}`
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("string ids must decode: %v", err)
			}
			agentID, err := parseIDField(req.AgentID)
			if err != nil || agentID != 7 {
				t.Fatalf("agent_id: %d, %v", agentID, err)
			}
			resourceID, err := parseIDField(req.ResourceID)
			if err != nil || resourceID != want {
				t.Fatalf("resource_id: %d, %v", resourceID, err)
			}
			if !req.Visible {
				t.Fatal("visible lost")
			}
		}
	})
	t.Run("invalid ids", func(t *testing.T) {
		for _, body := range []string{
			`{"agent_id":"0","kind":"mcp","resource_id":"1"}`,
			`{"agent_id":"","kind":"mcp","resource_id":"1"}`,
			`{"agent_id":"7","kind":"mcp","resource_id":"0"}`,
			`{"agent_id":"7","kind":"mcp","resource_id":"-1"}`,
			`{"agent_id":"7","kind":"mcp","resource_id":"abc"}`,
			`{"kind":"mcp","resource_id":"1"}`,
		} {
			var req toggleVisibilityRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			_, errA := parseIDField(req.AgentID)
			_, errR := parseIDField(req.ResourceID)
			if errA == nil && errR == nil {
				t.Fatalf("want parse error for %s", body)
			}
		}
	})
	t.Run("numeric ids are out of contract", func(t *testing.T) {
		for _, body := range []string{
			`{"agent_id":7,"kind":"mcp","resource_id":"1"}`,
			`{"agent_id":"7","kind":"mcp","resource_id":1}`,
		} {
			var req toggleVisibilityRequest
			if err := json.Unmarshal([]byte(body), &req); err == nil {
				t.Fatalf("P03 wire is string; number must not decode: %s", body)
			}
		}
	})
}

// --- POST /api/mcp (E04) ---

func TestSaveMCPRequestAcceptsStringID(t *testing.T) {
	t.Run("string id edits", func(t *testing.T) {
		var req saveMCPRequest
		if err := json.Unmarshal([]byte(`{"id":"12","name":"demo","transport":"stdio","enabled":true}`), &req); err != nil {
			t.Fatalf("string id must decode: %v", err)
		}
		id, err := parseProfileSaveID(req.ID)
		if err != nil || id != 12 {
			t.Fatalf("got %d, %v", id, err)
		}
		if req.Name != "demo" || req.Transport != "stdio" || !req.Enabled {
			t.Fatalf("embedded fields lost: %+v", req.MCPServer)
		}
		if req.MCPServer.ID != 0 {
			t.Fatalf("embedded int64 ID must stay untouched (shadowed by RawMessage), got %d", req.MCPServer.ID)
		}
	})
	t.Run("new keeps zero", func(t *testing.T) {
		for _, body := range []string{`{"name":"demo"}`, `{"id":null,"name":"demo"}`, `{"id":"","name":"demo"}`, `{"id":"0","name":"demo"}`, `{"id":0,"name":"demo"}`} {
			var req saveMCPRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if id, err := parseProfileSaveID(req.ID); id != 0 || err != nil {
				t.Fatalf("%s: got %d, %v", body, id, err)
			}
		}
	})
	t.Run("invalid id", func(t *testing.T) {
		for _, body := range []string{`{"id":"-1","name":"demo"}`, `{"id":"abc","name":"demo"}`, `{"id":"9223372036854775808","name":"demo"}`} {
			var req saveMCPRequest
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				t.Fatalf("decode %s: %v", body, err)
			}
			if _, err := parseProfileSaveID(req.ID); err == nil {
				t.Fatalf("want parse error for %s", body)
			}
		}
	})
}
