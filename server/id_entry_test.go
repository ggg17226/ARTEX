package server

// E08–E10 入口失败语义契约测试（P03：64 位 ID 的 path / query / cursor 三类入参）。
//
// 全部离线，不连数据库（不设 ARTEX_TIDB_DSN / ARTEX_TEST_TIDB_DSN）：
//   1. 严格实体 ID（pathInt / parseIDField）：只接受 1..MaxInt64；
//   2. DB 主键游标（parseCursorID）：""/"0" 保留哨兵，非法 / 溢出报错；
//   3. 可选 query 过滤器（queryFilterID / parseIDFieldLenient）：非法 / 溢出 = 未指定，
//      绝不退化成 MaxInt64 过滤值；
//   4. handler 级 400：只对「入口校验发生在 store 访问之前」的 handler 离线断言
//      （文件末尾 TestIDE08_*HandlerRejects*）；先查 store（s.pg → 503）或先解析
//      task（ResolveTask → 404）的 handler 在离线环境到不了 ID 分支，边界见其注释。
//
// 大 ID 一律用 2^53+1（9007199254740993，JS Number 精度之外）与 MaxInt64；
// 2^53 本身 float64 可精确表示，不能单独作为「>2^53」证据（R20 M-3）。

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pgdb "github.com/Autumn-27/artex/db"
)

// bigIDStr / maxIDStr 复用 id_output_wire_test.go 里的同名常量（2^53+1 与 MaxInt64）。
const (
	overIDStr  = "9223372036854775808" // MaxInt64 + 1
	negOverStr = "-9223372036854775809"
)

// pathRequest 构造一个带路径参数的请求（等价于 mux 路由匹配后 handler 看到的 r）。
func pathRequest(name, value string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.SetPathValue(name, value)
	return r
}

// --- E08：pathInt 严格实体 ID ---

func TestPathIntStrictEntityID(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"1", 1},
		{"7", 7},
		{bigIDStr, 9007199254740993},
		{maxIDStr, math.MaxInt64},
	} {
		got, ok := pathInt(pathRequest("id", tc.in), "id")
		if !ok || got != tc.want {
			t.Fatalf("pathInt(%q) = (%d, %v), want (%d, true)", tc.in, got, ok, tc.want)
		}
	}
	// 0 / 负数 / 空 / 非数字 / 小数 / 溢出：全部 false（调用方回 400）。
	for _, bad := range []string{"", "0", "-0", "-1", "-7", " 1", "1 ", "1.5", "abc", "0x10", "1e3", "92233720368547758070", overIDStr, negOverStr} {
		if got, ok := pathInt(pathRequest("id", bad), "id"); ok {
			t.Fatalf("pathInt(%q) = (%d, true), want rejected", bad, got)
		}
	}
	// 前导 + / 前导 0 由 strconv.ParseInt 接受（值仍落在 1..MaxInt64）。这与 W4b-1
	// 已提交的 parseIDField 一致，本批不收紧（改了会同时动请求体解析，越界）。
	for _, tc := range []struct {
		in   string
		want int64
	}{{"+1", 1}, {"007", 7}} {
		if got, ok := pathInt(pathRequest("id", tc.in), "id"); !ok || got != tc.want {
			t.Fatalf("pathInt(%q) = (%d, %v), want (%d, true)", tc.in, got, ok, tc.want)
		}
	}
	// 缺失路径参数（mux 未匹配）等价空串 → 拒绝。
	if _, ok := pathInt(httptest.NewRequest(http.MethodGet, "/x", nil), "id"); ok {
		t.Fatal("missing path value must be rejected")
	}
}

// --- E10：DB 主键游标 ---

func TestParseCursorIDSentinelAndBounds(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"", 0},    // 未提供 = 从头 / 最新一页
		{"0", 0},   // 显式 0 哨兵
		{" 0 ", 0}, // 前后空白容忍（与 helper 的 TrimSpace 一致）
		{"1", 1},
		{bigIDStr, 9007199254740993},
		{maxIDStr, math.MaxInt64},
	} {
		got, err := parseCursorID(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("parseCursorID(%q) = (%d, %v), want (%d, nil)", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"-1", "-0", "abc", "1.5", "0x1", overIDStr, negOverStr, "99999999999999999999"} {
		if got, err := parseCursorID(bad); err == nil {
			t.Fatalf("parseCursorID(%q) = (%d, nil), want error", bad, got)
		}
	}
	// 与 parseIDField 一致：ParseInt 接受前导 + / 前导 0，值合法即放行。
	if got, err := parseCursorID("+7"); err != nil || got != 7 {
		t.Fatalf(`parseCursorID("+7") = (%d, %v), want (7, nil)`, got, err)
	}
}

// --- E10：可选 query 过滤器（lenient） ---

func TestQueryFilterIDNeverBecomesMaxInt64(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"", 0}, // 未提供 = 不过滤
		{"0", 0},
		{"-1", 0},
		{"abc", 0},
		{"1.5", 0},
		{overIDStr, 0},  // 关键：溢出必须是「未指定」，不是 MaxInt64
		{negOverStr, 0}, // 负向溢出同理，不是 MinInt64
		{"1", 1},
		{bigIDStr, 9007199254740993},
		{maxIDStr, math.MaxInt64},
	} {
		if got := queryFilterID(tc.in); got != tc.want {
			t.Fatalf("queryFilterID(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	// 与 parseIDFieldLenient 一致：非法 → nil（不是 &0、不是 &MaxInt64）。
	for _, bad := range []string{"", "0", "-1", "abc", overIDStr} {
		if got := parseIDFieldLenient(bad); got != nil {
			t.Fatalf("parseIDFieldLenient(%q) = %v, want nil", bad, *got)
		}
	}
	if got := parseIDFieldLenient(bigIDStr); got == nil || *got != 9007199254740993 {
		t.Fatalf("parseIDFieldLenient(2^53+1) = %v", got)
	}
}

// --- E09：activity session 里嵌的 intent 实体 ID 必须严格 ---

func TestParseActivitySessionIntentIDStrict(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want *int64
	}{
		{"intent:1", ptrOf(int64(1))},
		{"intent:" + bigIDStr, ptrOf(int64(9007199254740993))},
		{"intent:" + maxIDStr, ptrOf(int64(math.MaxInt64))},
		{"main", nil},
		{"plan", nil},
		{"", nil},
	} {
		filter, ok := parseActivitySession(tc.in)
		if !ok {
			t.Fatalf("parseActivitySession(%q) rejected a valid session", tc.in)
		}
		if tc.want == nil && filter.NodeID != nil {
			t.Fatalf("parseActivitySession(%q) leaked NodeID=%v", tc.in, *filter.NodeID)
		}
		if tc.want != nil && (filter.NodeID == nil || *filter.NodeID != *tc.want) {
			t.Fatalf("parseActivitySession(%q) NodeID = %v, want %v", tc.in, filter.NodeID, *tc.want)
		}
	}
	// 0 / 负数 / 非数字 / 溢出：session 非法（handler 回 400），不得变成 NodeID=0 查询。
	for _, bad := range []string{"intent:0", "intent:-1", "intent:abc", "intent:1.5", "intent:" + overIDStr, "intent:", "main:-1", "main:abc", "unknown"} {
		if filter, ok := parseActivitySession(bad); ok {
			t.Fatalf("parseActivitySession(%q) accepted, filter=%+v", bad, filter)
		}
	}
	// main:<seg> 是内存分段编号（不是 64 位实体 ID）：0 合法，保持原语义。
	if filter, ok := parseActivitySession("main:0"); !ok || filter.MainSeg == nil || *filter.MainSeg != 0 {
		t.Fatalf(`parseActivitySession("main:0") = %+v, %v; want MainSeg=0`, filter, ok)
	}
}

func ptrOf(v int64) *int64 { return &v }

// --- R20 M-2：skill 可见性开关的 agent_id 必须严格正数 ---

func TestToggleSkillVisibilityAgentIDStrict(t *testing.T) {
	for _, body := range []string{
		`{"agent_id":"0","skill_name":"nmap","visible":true}`,
		`{"agent_id":"-1","skill_name":"nmap","visible":true}`,
		`{"agent_id":"","skill_name":"nmap","visible":true}`,
		`{"agent_id":"abc","skill_name":"nmap","visible":true}`,
		`{"agent_id":"1.5","skill_name":"nmap","visible":true}`,
		`{"agent_id":"` + overIDStr + `","skill_name":"nmap","visible":true}`,
		`{"skill_name":"nmap","visible":true}`,
		`{"agent_id":7,"skill_name":"nmap","visible":true}`, // wire 是 string，number 不合规
	} {
		var req toggleSkillVisibilityRequest
		// 数字写法在 decode 阶段就失败；字符串写法在 parseIDField 阶段失败。
		if err := json.Unmarshal([]byte(body), &req); err == nil {
			if _, err := parseIDField(req.AgentID); err == nil {
				t.Fatalf("want rejection for %s", body)
			}
		}
	}
	for _, tc := range []struct {
		body string
		want int64
	}{
		{`{"agent_id":"7","skill_name":"nmap","visible":true}`, 7},
		{`{"agent_id":"` + bigIDStr + `","skill_name":"nmap","visible":true}`, 9007199254740993},
		{`{"agent_id":"` + maxIDStr + `","skill_name":"nmap","visible":true}`, math.MaxInt64},
	} {
		var req toggleSkillVisibilityRequest
		if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
			t.Fatalf("decode %s: %v", tc.body, err)
		}
		if got, err := parseIDField(req.AgentID); err != nil || got != tc.want {
			t.Fatalf("%s: got (%d, %v), want %d", tc.body, got, err, tc.want)
		}
	}
}

// --- R23 M-3：归档分页 wire 嵌入后 total/page/size 自动跟随，items 仍是 string ID ---

// pgTaskArchivePageForTest 造一页归档（含大 ID 与 nil→[] 语义的 source_task_ids）。
func pgTaskArchivePageForTest() pgdb.TaskArchivePage {
	return pgdb.TaskArchivePage{
		Items: []pgdb.TaskArchive{{
			ID:            9007199254740993,
			TaskID:        9007199254740993,
			State:         "archived",
			SourceTaskIDs: []int64{1, 2},
		}},
		Total: 7,
		Page:  2,
		Size:  13,
	}
}

func TestTaskArchivePageWireEmbedsPageCounts(t *testing.T) {
	page := taskArchivePageDTO(pgTaskArchivePageForTest())
	raw, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{`"total":7`, `"page":2`, `"size":13`, `"task_id":"` + bigIDStr + `"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("wire %s missing %s", body, want)
		}
	}
	// 外层 Items 必须屏蔽嵌入的 []pgdb.TaskArchive：source_task_ids 是字符串数组。
	if strings.Contains(body, `"source_task_ids":[1,2]`) {
		t.Fatalf("source_task_ids leaked as numbers: %s", body)
	}
	if !strings.Contains(body, `"source_task_ids":["1","2"]`) {
		t.Fatalf("source_task_ids not stringified: %s", body)
	}
	// 反向解码：id / task_id 都是 string（R22 wire 形状）。
	var back struct {
		Items []struct {
			ID            string   `json:"id"`
			TaskID        string   `json:"task_id"`
			SourceTaskIDs []string `json:"source_task_ids"`
		} `json:"items"`
		Total int `json:"total"`
	}
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("wire must decode into string ids: %v (%s)", err, body)
	}
	if back.Total != 7 || len(back.Items) != 1 || back.Items[0].TaskID != bigIDStr {
		t.Fatalf("unexpected round trip: %+v (%s)", back, body)
	}
}

// --- handler 级 400（可离线断言的那批：入口校验先于 store 访问） ---

// doHandler 直接调用 handler（不经 mux），路径参数用 SetPathValue 注入。
func doHandler(h func(http.ResponseWriter, *http.Request), path string, values map[string]string, body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(http.MethodGet, path, nil)
	} else {
		r = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	}
	for k, v := range values {
		r.SetPathValue(k, v)
	}
	rec := httptest.NewRecorder()
	h(rec, r)
	return rec
}

// offlineServer 只填 Manager（pg 为 nil）。用它跑「ID 非法必须在碰 store 之前返回」的 handler。
func offlineServer() *Server { return &Server{m: &Manager{}} }

func TestIDE08HandlersRejectBadEntityIDs(t *testing.T) {
	cases := []struct {
		name string
		call func(*testing.T, *Server, string) *httptest.ResponseRecorder
		bad  []string
	}{
		{
			name: "task_archive_get",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.getTaskArchive, "/api/task-archives/"+id, map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", "1.5", overIDStr, ""},
		},
		{
			name: "task_archive_restore",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.queueTaskArchiveRestore, "/api/task-archives/"+id+"/restore", map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "task_archive_delete",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.queueTaskArchiveDelete, "/api/task-archives/"+id+"/delete", map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "intercept_decide",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.interceptDecide, "/api/intercept/pending/"+id+"/decide", map[string]string{"id": id}, `{"decision":"allowed"}`)
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "task_category_delete",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.pgDeleteTaskCategory, "/api/task-categories/"+id, map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "finding_get",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.getFinding, "/api/findings/"+id, map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", "1.5", overIDStr},
		},
		{
			name: "finding_lineage",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.findingLineage, "/api/findings/"+id+"/lineage", map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "finding_delete",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.deleteFinding, "/api/findings/"+id, map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "finding_deepen",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.deepenFinding, "/api/findings/"+id+"/deepen", map[string]string{"id": id}, `{"description":"x"}`)
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
		{
			name: "llm_record_get",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.pgGetLLMRecord, "/api/llm/records/"+id, map[string]string{"id": id}, "")
			},
			bad: []string{"0", "-1", "abc", overIDStr},
		},
	}
	for _, tc := range cases {
		for _, bad := range tc.bad {
			rec := tc.call(t, offlineServer(), bad)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s with id=%q: status=%d body=%s, want 400", tc.name, bad, rec.Code, rec.Body.String())
			}
		}
	}
}

// TestIDE08HandlersAcceptBigIDs 钉住「合法大 ID 不被入口层拒绝」：这些 handler 在离线
// 环境下会带着合法 ID 继续往下走（store 为 nil → 500/503 或 panic），因此只断言
// 「不是 400」。pg 为 nil 的 store 调用可能 panic，这里用 recover 把它当成「已过入口」。
func TestIDE08HandlersAcceptBigIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*testing.T, *Server, string) *httptest.ResponseRecorder
	}{
		{
			name: "task_archive_get",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.getTaskArchive, "/api/task-archives/"+id, map[string]string{"id": id}, "")
			},
		},
		{
			name: "finding_get",
			call: func(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
				return doHandler(s.getFinding, "/api/findings/"+id, map[string]string{"id": id}, "")
			},
		},
	} {
		for _, id := range []string{"1", bigIDStr, maxIDStr} {
			rec := callIgnoringPanic(t, tc.call, offlineServer(), id)
			if rec != nil && rec.Code == http.StatusBadRequest {
				t.Fatalf("%s with valid big id=%q got 400: %s", tc.name, id, rec.Body.String())
			}
		}
	}
}

func callIgnoringPanic(t *testing.T, call func(*testing.T, *Server, string) *httptest.ResponseRecorder, s *Server, id string) (rec *httptest.ResponseRecorder) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			// store 未初始化：说明入口校验已放行（正是本用例要确认的）。
			rec = nil
		}
	}()
	return call(t, s, id)
}

// --- 离线覆盖不到的边界（说明，不是遗漏） ---
//
// 以下改造点的 handler 在 ID 校验**之前**先取 store 或先解析 task，离线环境（pg=nil、
// 无注册任务）会在 503 / 404 / 200 空响应处提前返回，因此只能靠上面的 helper 级断言
// 覆盖解析语义，handler 级 400 留给 W5 真实库回归：
//   - pathInt 的其余调用点（intercept / notify / trigger / template / asset_intercept /
//     task_intercept / task_assets / conversations / server_mgmt MCP）：先 s.pg(w) → 503；
//   - activity / activityHistory / streamActivity / intents / activityDetail：先
//     ResolveTask → 404（或 activity 的空 items 200）；
//   - pgConversationMessages / pgConversationMsgDetail：先 convByID → 503；
//   - listAssets / assetCounts（lenient filter）与 notifyListDeliveries：先 store → 503，
//     且合法/非法都返回 200（lenient 不产生 400）；
//   - handleSideQuestions 的 before：先 side 服务与 pg → 503。
