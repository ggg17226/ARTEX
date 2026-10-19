package db

// =====================================================================
// P08-03 Task 3.1 —— 探索引擎与会话图谱方言重构测试（M21）
//
// 三层守卫，缺一层都会漏：
//  1. 源码级守卫（TestExploration_SourceIsTiDBDialectOnly）：连"写出来但当前没有
//     调用到"的 PG 片段都能抓到，是 97 处 Blocker 的清零判据；
//  2. 语句级替身（expMock*）：断言真实经过 database/sql 的语句形状 —— 占位符一律
//     `?` 且与参数逐位对齐、自增主键一律 LastInsertId、时序列由 Go 端算好后显式
//     写入、删除路径显式清理 edges/anchors 并置空弱关联；
//  3. TiDB 真机集成（ARTEX_TIDB_DSN 未设置时显式 t.Skip，绝不当作通过）。
// =====================================================================

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// =====================================================================
// 1) 计划 Step 1 —— TiDB 真机节点创建（无 DSN 时显式 Skip）
// =====================================================================

func TestExploration_CreateNode_UnmigratedDialect(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
	}
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})

	tName := fmt.Sprintf("P08 Exp Task %d", time.Now().UnixNano())
	task, err := d.CreateTaskWithOptions(tName, "goal", TaskCreateOptions{})
	if err != nil {
		t.Fatalf("CreateTaskWithOptions failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.DeleteTaskCascade(task.ID, false, false); err != nil {
			t.Errorf("cleanup task failed: %v", err)
		}
		// 独立逆拓扑兜底清理，杜绝孤儿残留
		if _, err := d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID); err != nil {
			t.Errorf("cleanup residual exploration_nodes failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", task.ExplorationID); err != nil {
			t.Errorf("cleanup residual exploration failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", task.ID); err != nil {
			t.Errorf("cleanup residual task root failed: %v", err)
		}
		var nCount, eCount, tCount int
		if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID).Scan(&nCount); err != nil {
			t.Errorf("query residual exploration_nodes failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM explorations WHERE id = ?", task.ExplorationID).Scan(&eCount); err != nil {
			t.Errorf("query residual explorations failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", task.ID).Scan(&tCount); err != nil {
			t.Errorf("query residual tasks failed: %v", err)
		}
		if nCount != 0 || eCount != 0 || tCount != 0 {
			t.Errorf("residual exploration fixtures found after cleanup: nodes=%d, explorations=%d, tasks=%d", nCount, eCount, tCount)
		}
	})

	nodeID, err := d.Exploration(task.ExplorationID).AddNode("intent", map[string]any{"label": "test"}, 1, "pending", "user", nil)
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}
	if nodeID <= 0 {
		t.Fatalf("expected positive nodeID, got %d", nodeID)
	}

	// 边关联 + 活动追加：三者覆盖 RETURNING → LastInsertId 的全部写入路径。
	if err := d.Exploration(task.ExplorationID).Link(nodeID, RelYields, nodeID); err != nil {
		t.Fatalf("Link failed: %v", err)
	}
	activityID, err := d.Exploration(task.ExplorationID).AppendActivity(Activity{Worker: "w1", Kind: "tool_use", Summary: "hello"})
	if err != nil {
		t.Fatalf("AppendActivity failed: %v", err)
	}
	if activityID <= 0 {
		t.Fatalf("expected positive activityID, got %d", activityID)
	}
}

// =====================================================================
// 2) 源码级方言守卫（M21：97 处 Blocker 必须清零）
// =====================================================================

// TestExploration_SourceIsTiDBDialectOnly 扫描两个产品文件的**可执行代码**（去掉行
// 注释），任何 PostgreSQL 片段都判红。清单覆盖计划 Step 3 的全部改造项：`$n` 占位符、
// RETURNING、advisory lock、`ON CONFLICT`、JSONB 转型、`ILIKE`、`FILTER (WHERE`、
// LATERAL、递归 CTE、`TO_CHAR`/`DATE_TRUNC`/`AT TIME ZONE`/`EXTRACT(EPOCH` 时间函数。
func TestExploration_SourceIsTiDBDialectOnly(t *testing.T) {
	pgFragments := []string{
		"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9",
		"RETURNING", "pg_advisory", "ON CONFLICT",
		"::jsonb", "::text", "::bigint", "::int", "::json",
		"array_to_json", "to_jsonb", "jsonb_array_elements", "unnest(", "= ANY(", "ANY(",
		"ILIKE", "DISTINCT ON", "FILTER (WHERE", "FILTER(WHERE",
		"LATERAL", "WITH RECURSIVE", "TO_CHAR(", "DATE_TRUNC", "AT TIME ZONE",
		"EXTRACT(EPOCH", "->>", "NOW() -", "SERIAL",
	}
	for _, file := range []string{"exploration.go", "exploration_sources.go"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			code, _, _ := strings.Cut(line, "//")
			for _, frag := range pgFragments {
				if strings.Contains(code, frag) {
					t.Errorf("%s:%d: PostgreSQL fragment %q in executable code: %s", file, i+1, frag, strings.TrimSpace(code))
				}
			}
		}
	}
}

// TestExploration_QueriesUseOrdinalPlaceholders 把"每条语句 `?` 数 == 实参数数"变成
// 硬约束：`$n` 机械替换成 `?` 最常见的事故就是重复引用的那个参数少绑一位，之后所有
// 条件整体错位。替身记录每条真实语句，逐条比对。
func TestExploration_QueriesUseOrdinalPlaceholders(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	es := d.Exploration(501)

	eng.on("SELECT COUNT(*)", []string{"n"}, []driver.Value{int64(0)})
	eng.on("FROM exploration_nodes", expNodeColumns())
	if _, _, err := es.NodesPage(NodeFilter{Kinds: []string{"intent", "fact"}, States: []string{"open", "running"}, Query: "#12"}, 3, 5); err != nil {
		t.Fatalf("NodesPage: %v", err)
	}
	if _, _, err := es.ActivityPage(ActivitySessionFilter{Main: true, MainSeg: expPtr(4)}, 9, 5); err != nil {
		t.Fatalf("ActivityPage: %v", err)
	}
	if _, err := es.listByKindPageFiltered("intent", 40, 10, "keyword"); err != nil {
		t.Fatalf("listByKindPageFiltered: %v", err)
	}
	if _, err := es.countByKindFiltered("intent", "keyword"); err != nil {
		t.Fatalf("countByKindFiltered: %v", err)
	}
	if _, err := es.ActivityTraceSearch(expPtr(int64(7)), "curl", 20); err != nil {
		t.Fatalf("ActivityTraceSearch: %v", err)
	}
	if _, err := es.ActivityTraceSearch(nil, "curl", 20); err != nil {
		t.Fatalf("ActivityTraceSearch(all): %v", err)
	}
	if _, err := es.ActivityByIDs([]int64{3, 4, 5}); err != nil {
		t.Fatalf("ActivityByIDs: %v", err)
	}
	if _, err := es.NodesByIDs([]int64{3, 4}); err != nil {
		t.Fatalf("NodesByIDs: %v", err)
	}
	if _, err := es.EdgesTouching([]int64{3, 4}); err != nil {
		t.Fatalf("EdgesTouching: %v", err)
	}

	calls := eng.snapshotCalls()
	if len(calls) < 10 {
		t.Fatalf("expected at least 10 statements, got %d", len(calls))
	}
	for _, c := range calls {
		expAssertOrdinal(t, c)
		if strings.Contains(c.Query, "ILIKE") || strings.Contains(c.Query, "::") {
			t.Errorf("PostgreSQL fragment in emitted statement: %s", c.Query)
		}
	}

	// EdgesTouching 的 id 列表在 src/dst 两个谓词里各出现一次：`?` 无编号复用能力，
	// 参数必须整段绑两遍，只绑一遍会让 dst 侧静默拿到空集合。
	var edgeCall *expMockCall
	for i := range calls {
		if strings.Contains(calls[i].Query, "FROM exploration_edges") && strings.Contains(calls[i].Query, "dst_id IN") {
			edgeCall = &calls[i]
		}
	}
	if edgeCall == nil {
		t.Fatal("EdgesTouching must probe exploration_edges on both endpoints")
	}
	if want := 1 + 2*2; len(edgeCall.Args) != want {
		t.Fatalf("EdgesTouching args=%d, want %d (exploration id + the id list bound twice): %v", len(edgeCall.Args), want, edgeCall.Args)
	}
}

// =====================================================================
// 3) 节点写入：RETURNING → LastInsertId
// =====================================================================

func TestExploration_AddNodeUsesLastInsertID(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(9001)

	nodeID, err := d.Exploration(501).AddNode("intent", map[string]any{"summary": "probe"}, 7, "open", "planner", []int64{11, 22})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if nodeID != 9001 {
		t.Fatalf("nodeID=%d, want the LastInsertId 9001", nodeID)
	}

	inserts := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO exploration_nodes")
	})
	if len(inserts) != 1 {
		t.Fatalf("expected 1 node INSERT, got %d", len(inserts))
	}
	stmt := inserts[0]
	expAssertOrdinal(t, stmt)
	if strings.Contains(strings.ToUpper(stmt.Query), "RETURNING") {
		t.Fatalf("TiDB has no INSERT … RETURNING: %s", stmt.Query)
	}
	want := []driver.Value{int64(501), "intent", `{"summary":"probe"}`, int64(7), "open", "planner"}
	if len(stmt.Args) != len(want) {
		t.Fatalf("args=%v, want %v", stmt.Args, want)
	}
	for i := range want {
		if fmt.Sprint(stmt.Args[i]) != fmt.Sprint(want[i]) {
			t.Fatalf("arg %d = %v, want %v (positional alignment broken)", i, stmt.Args[i], want[i])
		}
	}

	anchors := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO exploration_anchors")
	})
	if len(anchors) != 2 {
		t.Fatalf("expected 2 anchor INSERTs, got %d", len(anchors))
	}
	for i, a := range anchors {
		expAssertOrdinal(t, a)
		if strings.Contains(a.Query, "ON CONFLICT") {
			t.Fatalf("ON CONFLICT is PostgreSQL-only: %s", a.Query)
		}
		if !strings.Contains(a.Query, "ON DUPLICATE KEY") {
			t.Fatalf("anchor INSERT must stay idempotent on uq_exploration_anchors: %s", a.Query)
		}
		if fmt.Sprint(a.Args[0]) != "9001" || fmt.Sprint(a.Args[1]) != fmt.Sprint([]int64{11, 22}[i]) {
			t.Fatalf("anchor %d args=%v, want [9001 %d]", i, a.Args, []int64{11, 22}[i])
		}
	}

	if commits := eng.txCounts().Commits; commits != 1 {
		t.Fatalf("commits=%d, want 1", commits)
	}
}

// TestExploration_AddNodeRejectsInvalidLastInsertID 校验自增主键必须落在
// [1, MaxInt64]：驱动/中间件回 0 时不得把 0 当作合法节点 id 交还给调用方。
func TestExploration_AddNodeRejectsInvalidLastInsertID(t *testing.T) {
	for _, bad := range []int64{0, -1} {
		eng := newExpMockEngine()
		d := openExpMockDB(t, eng)
		eng.setNextInsertID(bad)
		if _, err := d.Exploration(501).AddNode("fact", map[string]any{"summary": "x"}, 0, "confirmed", "w", nil); err == nil {
			t.Fatalf("LastInsertId=%d must be rejected", bad)
		}
		counts := eng.txCounts()
		if counts.Commits != 0 {
			t.Fatalf("LastInsertId=%d must never commit (commits=%d)", bad, counts.Commits)
		}
		if counts.Rollbacks == 0 {
			t.Fatalf("LastInsertId=%d must roll the attempt back", bad)
		}
	}
}

// TestExploration_AddNodeRejectsOverflowedID 用 MaxInt64 边界确认上界校验存在。
func TestExploration_AddNodeRejectsOverflowedID(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(math.MaxInt64)
	id, err := d.Exploration(501).AddNode("fact", map[string]any{"summary": "x"}, 0, "confirmed", "w", nil)
	if err != nil {
		t.Fatalf("MaxInt64 is the inclusive upper bound: %v", err)
	}
	if id != math.MaxInt64 {
		t.Fatalf("id=%d", id)
	}
}

// =====================================================================
// 4) 边关联
// =====================================================================

func TestExploration_LinkIsIdempotentOnUniqueKey(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	if err := d.Exploration(501).Link(31, RelYields, 77); err != nil {
		t.Fatalf("Link: %v", err)
	}
	calls := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO exploration_edges")
	})
	if len(calls) != 1 {
		t.Fatalf("expected 1 edge INSERT, got %d", len(calls))
	}
	stmt := calls[0]
	expAssertOrdinal(t, stmt)
	if strings.Contains(stmt.Query, "ON CONFLICT") {
		t.Fatalf("ON CONFLICT is PostgreSQL-only: %s", stmt.Query)
	}
	if !strings.Contains(stmt.Query, "ON DUPLICATE KEY") {
		t.Fatalf("Link must stay idempotent on uq_exploration_edges: %s", stmt.Query)
	}
	want := []string{"501", "31", "yields", "77"}
	for i, w := range want {
		if fmt.Sprint(stmt.Args[i]) != w {
			t.Fatalf("edge arg %d = %v, want %s", i, stmt.Args[i], w)
		}
	}
}

// =====================================================================
// 5) 活动追加
// =====================================================================

func TestExploration_AppendActivityUsesLastInsertID(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(424242)

	nodeID := int64(88)
	in := Activity{
		NodeID:   &nodeID,
		Worker:   "work\x00#1",
		Kind:     "tool_use",
		Summary:  "ran curl\x00",
		Metadata: json.RawMessage(`{"k":"v1"}`),
		IsError:  true,
		MainSeg:  expPtr(3),
	}
	id, err := d.Exploration(501).AppendActivity(in)
	if err != nil {
		t.Fatalf("AppendActivity: %v", err)
	}
	if id != 424242 {
		t.Fatalf("id=%d, want LastInsertId 424242", id)
	}
	calls := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO activity")
	})
	if len(calls) != 1 {
		t.Fatalf("expected 1 activity INSERT, got %d", len(calls))
	}
	stmt := calls[0]
	expAssertOrdinal(t, stmt)
	if strings.Contains(strings.ToUpper(stmt.Query), "RETURNING") {
		t.Fatalf("TiDB has no INSERT … RETURNING: %s", stmt.Query)
	}
	if len(stmt.Args) != 15 {
		t.Fatalf("activity INSERT must bind 15 columns, got %d: %v", len(stmt.Args), stmt.Args)
	}
	if fmt.Sprint(stmt.Args[0]) != "501" || fmt.Sprint(stmt.Args[1]) != "88" {
		t.Fatalf("activity args[0..1]=%v %v, want 501 88", stmt.Args[0], stmt.Args[1])
	}
	if worker := fmt.Sprint(stmt.Args[2]); strings.Contains(worker, "\x00") {
		t.Fatalf("NUL must stay stripped for TEXT columns: %q", worker)
	}
	if summary := fmt.Sprint(stmt.Args[7]); strings.Contains(summary, "\x00") {
		t.Fatalf("NUL must stay stripped: %q", summary)
	}
	// metadata 必须以驱动可绑定的类型（string / []byte）下发：json.RawMessage 会被
	// database/sql 的默认转换器直接拒绝（unsupported type json.RawMessage）。
	switch stmt.Args[9].(type) {
	case string, []byte:
	default:
		t.Fatalf("metadata must bind as string or []byte, got %T", stmt.Args[9])
	}
	if meta := fmt.Sprint(stmt.Args[9]); meta != `{"k":"v1"}` {
		t.Fatalf("metadata=%s", meta)
	}

	// 空 metadata 落 "{}"，与 NOT NULL 列语义一致。
	eng.resetCalls()
	if _, err := d.Exploration(501).AppendActivity(Activity{Kind: "note"}); err != nil {
		t.Fatalf("AppendActivity(empty metadata): %v", err)
	}
	stmt = eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO activity")
	})[0]
	if meta := fmt.Sprint(stmt.Args[9]); meta != "{}" {
		t.Fatalf("empty metadata must persist as {}, got %s", meta)
	}
}

func TestExploration_AppendActivityRejectsInvalidLastInsertID(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(0)
	if _, err := d.Exploration(501).AppendActivity(Activity{Kind: "note"}); err == nil {
		t.Fatal("activity id 0 must be rejected")
	}
}

// =====================================================================
// 6) 时序列：Go 端内存计算后显式写入
// =====================================================================

// TestExploration_SetIntentStateStampsCompletedAtInGo 断言 completed_at 不再由 SQL 的
// `CASE WHEN $n THEN now() ELSE NULL END` 决定，而是 Go 端算好后作为参数显式写入。
func TestExploration_SetIntentStateStampsCompletedAtInGo(t *testing.T) {
	cases := []struct {
		state    string
		terminal bool
	}{
		{"done", true}, {"blocked", true}, {"exhausted", true}, {"stopped", true},
		{"open", false}, {"running", false}, {"paused", false},
	}
	for _, tc := range cases {
		eng := newExpMockEngine()
		d := openExpMockDB(t, eng)
		if err := d.Exploration(501).SetIntentState(12, tc.state); err != nil {
			t.Fatalf("SetIntentState(%s): %v", tc.state, err)
		}
		calls := eng.statements(func(c expMockCall) bool {
			return strings.Contains(c.Query, "UPDATE exploration_nodes")
		})
		if len(calls) != 1 {
			t.Fatalf("state=%s: expected 1 UPDATE, got %d", tc.state, len(calls))
		}
		stmt := calls[0]
		expAssertOrdinal(t, stmt)
		if strings.Contains(strings.ToUpper(stmt.Query), "CASE WHEN") {
			t.Fatalf("state=%s: timing must be computed in Go, not SQL: %s", tc.state, stmt.Query)
		}
		var stamp driver.Value
		for _, a := range stmt.Args {
			if _, ok := a.(time.Time); ok {
				stamp = a
			}
		}
		if tc.state == "paused" {
			if stamp != nil {
				t.Fatalf("state=paused must not stamp completed_at, got %v", stamp)
			}
			continue
		}
		if tc.terminal && stamp == nil {
			t.Fatalf("terminal state %s must stamp completed_at with a Go-side timestamp: %v", tc.state, stmt.Args)
		}
		if !tc.terminal && stamp != nil {
			t.Fatalf("non-terminal state %s must clear completed_at, got %v", tc.state, stamp)
		}
	}
}

func TestExploration_CompareAndSetIntentStateAlignsArgs(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	won, err := d.Exploration(501).CompareAndSetIntentState(12, "running", "done")
	if err != nil {
		t.Fatalf("CompareAndSetIntentState: %v", err)
	}
	if !won {
		t.Fatal("mock reports 1 affected row, expected won=true")
	}
	stmt := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "UPDATE exploration_nodes")
	})[0]
	expAssertOrdinal(t, stmt)
	if !strings.Contains(stmt.Query, "state = ?") && !strings.Contains(stmt.Query, "state=?") {
		t.Fatalf("compare-and-set must guard on the expected state: %s", stmt.Query)
	}
	if fmt.Sprint(stmt.Args[0]) != "done" {
		t.Fatalf("first arg must be the new state, got %v", stmt.Args[0])
	}
	var expectedSeen bool
	for _, a := range stmt.Args {
		if fmt.Sprint(a) == "running" {
			expectedSeen = true
		}
	}
	if !expectedSeen {
		t.Fatalf("expected state must stay bound: %v", stmt.Args)
	}
}

// TestExploration_SoftDeleteIntentStampsInGoAndCleansSideSession
func TestExploration_SoftDeleteIntentStampsInGoAndCleansSideSession(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM exploration_nodes", []string{"state", "payload"},
		[]driver.Value{"running", `{"summary":"drop me"}`})

	summary, err := d.Exploration(501).SoftDeleteIntent(12, "不再需要")
	if err != nil {
		t.Fatalf("SoftDeleteIntent: %v", err)
	}
	if summary != "drop me" {
		t.Fatalf("summary=%q", summary)
	}
	calls := eng.snapshotCalls()
	for _, c := range calls {
		expAssertOrdinal(t, c)
	}
	var stamped bool
	for _, c := range calls {
		if !strings.Contains(c.Query, "UPDATE exploration_nodes") {
			continue
		}
		if !strings.Contains(c.Query, "state='deleted'") && !strings.Contains(c.Query, "state = 'deleted'") {
			t.Fatalf("soft delete must flip state to deleted: %s", c.Query)
		}
		for _, a := range c.Args {
			if _, ok := a.(time.Time); ok {
				stamped = true
			}
		}
		if strings.Contains(strings.ToUpper(c.Query), "CASE WHEN") {
			t.Fatalf("completed_at must be written explicitly, not via SQL CASE: %s", c.Query)
		}
	}
	if !stamped {
		t.Fatalf("completed_at must carry a Go-side timestamp: %v", calls)
	}
	var sideCleaned bool
	for _, c := range calls {
		if strings.Contains(c.Query, "DELETE FROM side_question_sessions") {
			sideCleaned = true
		}
	}
	if !sideCleaned {
		t.Fatal("soft delete must drop the side-question session atomically")
	}
}

// =====================================================================
// 7) 关联删除与弱引用置空（M19 同源要求：新库没有 FK，CASCADE 不存在）
// =====================================================================

func TestExploration_CancelIntentCleansEdgesAnchorsAndWeakRefs(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)

	// 图：intent#10 --yields--> fact#20 / finding#30；intent#40 与 origin 永不入删除集。
	eng.on("SELECT 1 FROM exploration_nodes", []string{"one"}, []driver.Value{int64(1)})
	eng.on("SELECT id, kind, state FROM exploration_nodes", []string{"id", "kind", "state"},
		[]driver.Value{int64(10), "intent", "running"},
		[]driver.Value{int64(20), "fact", "confirmed"},
		[]driver.Value{int64(30), "finding", "confirmed"},
		[]driver.Value{int64(40), "intent", "open"},
	)
	eng.on("SELECT src_id, rel, dst_id FROM exploration_edges", []string{"src_id", "rel", "dst_id"},
		[]driver.Value{int64(10), "yields", int64(20)},
		[]driver.Value{int64(10), "yields", int64(30)},
	)

	cleanup, err := d.Exploration(501).CancelIntent(10)
	if err != nil {
		t.Fatalf("CancelIntent: %v", err)
	}
	if cleanup.Intents != 1 || cleanup.Facts != 1 || cleanup.Findings != 1 {
		t.Fatalf("unexpected cleanup counts: %+v", cleanup)
	}

	calls := eng.snapshotCalls()
	for _, c := range calls {
		expAssertOrdinal(t, c)
		if strings.Contains(strings.ToUpper(c.Query), "RETURNING") {
			t.Fatalf("no RETURNING in the cleanup path: %s", c.Query)
		}
	}

	deleteNodes := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "DELETE FROM exploration_nodes")
	})
	if len(deleteNodes) != 3 {
		t.Fatalf("expected 3 node DELETEs (intent + fact + finding), got %d", len(deleteNodes))
	}

	// 新库无 FK：边与锚点必须显式删除，activity/finding/source_node 必须显式置空。
	for _, want := range []string{
		"DELETE FROM exploration_edges",
		"DELETE FROM exploration_anchors",
		"UPDATE activity",
		"UPDATE findings",
		"UPDATE task_asset_links",
	} {
		var hit bool
		for _, c := range calls {
			if strings.Contains(c.Query, want) {
				hit = true
				if want == "UPDATE activity" && !strings.Contains(c.Query, "node_id=NULL") && !strings.Contains(c.Query, "node_id = NULL") {
					t.Fatalf("activity weak link must be nulled, not deleted: %s", c.Query)
				}
				if want == "UPDATE findings" && !strings.Contains(c.Query, "node_id=NULL") && !strings.Contains(c.Query, "node_id = NULL") {
					t.Fatalf("findings weak link must be nulled: %s", c.Query)
				}
				if want == "UPDATE task_asset_links" && !strings.Contains(c.Query, "source_node_id=NULL") && !strings.Contains(c.Query, "source_node_id = NULL") {
					t.Fatalf("task_asset_links weak link must be nulled: %s", c.Query)
				}
			}
		}
		if !hit {
			var seen []string
			for _, c := range calls {
				seen = append(seen, strings.TrimSpace(strings.SplitN(strings.TrimSpace(c.Query), "\n", 2)[0]))
			}
			t.Fatalf("cleanup must emit %q; statements were: %v", want, seen)
		}
	}

	// 删除边/锚点必须发生在删除节点之前（否则弱关联写回会命中已消失的行）。
	firstEdgeDelete, firstNodeDelete := -1, -1
	for i, c := range calls {
		if firstEdgeDelete < 0 && strings.Contains(c.Query, "DELETE FROM exploration_edges") {
			firstEdgeDelete = i
		}
		if firstNodeDelete < 0 && strings.Contains(c.Query, "DELETE FROM exploration_nodes") {
			firstNodeDelete = i
		}
	}
	if firstEdgeDelete > firstNodeDelete {
		t.Fatalf("edges/anchors must be cleared before the nodes: edge=%d node=%d", firstEdgeDelete, firstNodeDelete)
	}
	if counts := eng.txCounts(); counts.Commits != 1 {
		t.Fatalf("commits=%d, want the whole cleanup in one transaction", counts.Commits)
	}
}

func TestExploration_CancelIntentMissingIntentFailsWithoutCommit(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("SELECT 1 FROM exploration_nodes", []string{"one"}) // 行不存在
	if _, err := d.Exploration(501).CancelIntent(10); err == nil {
		t.Fatal("cancelling a missing intent must fail")
	}
	counts := eng.txCounts()
	if counts.Commits != 0 {
		t.Fatalf("commits=%d, want 0", counts.Commits)
	}
}

func TestExploration_DeleteGoalCleansEdgesAndNullifiesWeakRefs(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM exploration_nodes", []string{"id"}, []driver.Value{int64(7)})

	if err := d.Exploration(501).DeleteGoal(7); err != nil {
		t.Fatalf("DeleteGoal: %v", err)
	}
	calls := eng.snapshotCalls()
	for _, c := range calls {
		expAssertOrdinal(t, c)
	}
	for _, want := range []string{
		"DELETE FROM exploration_edges",
		"DELETE FROM exploration_anchors",
		"DELETE FROM exploration_nodes",
		"UPDATE activity",
		"UPDATE findings",
		"UPDATE task_asset_links",
	} {
		var hit bool
		for _, c := range calls {
			if strings.Contains(c.Query, want) {
				hit = true
			}
		}
		if !hit {
			t.Fatalf("DeleteGoal must emit %q", want)
		}
	}
	if !strings.Contains(strings.Join(queriesOf(calls), " "), "kind='goal'") {
		t.Fatal("goal delete must stay scoped to kind='goal' so an intent/fact can never drop by id")
	}
}

func TestExploration_DeleteGoalMissingReportsNotFound(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM exploration_nodes", []string{"c"}) // 0 行 → 目标不存在
	err := d.Exploration(501).DeleteGoal(7)
	if err == nil {
		t.Fatal("expected an error for a missing goal")
	}
	if counts := eng.txCounts(); counts.Commits != 0 {
		t.Fatalf("commits=%d, want 0", counts.Commits)
	}
}

// =====================================================================
// 8) 递归血缘：TiDB 无递归 CTE → Go 端 BFS
// =====================================================================

func TestExploration_FindingLineageWalksAncestorsInGo(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)

	// origin#1 → intent#10 --yields--> finding#30 --proves--> goal#99（下游必须排除）
	eng.on("FROM exploration_edges", []string{"src_id", "rel", "dst_id"},
		[]driver.Value{int64(1), "spawns", int64(10)},
		[]driver.Value{int64(10), "yields", int64(30)},
		[]driver.Value{int64(30), "proves", int64(99)},
	)
	eng.on("FROM exploration_nodes", expNodeColumns(),
		expNodeRow(1, "fact", "origin"),
		expNodeRow(10, "intent", "done"),
		expNodeRow(30, "finding", "confirmed"),
	)

	nodes, edges, err := d.Exploration(501).FindingLineage(30)
	if err != nil {
		t.Fatalf("FindingLineage: %v", err)
	}
	got := map[int64]bool{}
	for _, n := range nodes {
		got[n.ID] = true
	}
	if !got[1] || !got[10] || !got[30] {
		t.Fatalf("lineage must span origin → intent → finding, got %+v", nodes)
	}
	if got[99] {
		t.Fatal("lineage must exclude the downstream goal (proves edge points away from the finding)")
	}
	if len(edges) == 0 {
		t.Fatal("lineage must carry the internal edges")
	}
	for _, e := range edges {
		if e.To == 99 || e.From == 99 {
			t.Fatalf("edge touching the downstream goal leaked into the lineage: %+v", e)
		}
	}
	for _, c := range eng.snapshotCalls() {
		expAssertOrdinal(t, c)
		if strings.Contains(strings.ToUpper(c.Query), "WITH RECURSIVE") {
			t.Fatalf("TiDB baseline must not rely on recursive CTEs: %s", c.Query)
		}
	}
}

// =====================================================================
// 9) 聚合与时间口径
// =====================================================================

func TestExploration_TokenDailyAllComputesWindowInGo(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM activity", []string{"day", "i", "o", "r"},
		[]driver.Value{"2026-10-19", int64(1), int64(2), int64(3)})

	buckets, err := d.TokenDailyAll(7)
	if err != nil {
		t.Fatalf("TokenDailyAll: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Day != "2026-10-19" {
		t.Fatalf("buckets=%+v", buckets)
	}
	stmt := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "FROM activity")
	})[0]
	expAssertOrdinal(t, stmt)
	if strings.Contains(stmt.Query, "INTERVAL") || strings.Contains(stmt.Query, "$1") {
		t.Fatalf("the day window must be computed in Go and bound as a timestamp: %s", stmt.Query)
	}
	if _, ok := stmt.Args[0].(time.Time); !ok {
		t.Fatalf("window bound as %T, want time.Time", stmt.Args[0])
	}
	if time.Since(stmt.Args[0].(time.Time)) < 6*24*time.Hour {
		t.Fatalf("window start %v is not ~7 days back", stmt.Args[0])
	}
}

func TestExploration_ListMetricsAggregatesWithoutLateral(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("GROUP BY", []string{"c"})
	eng.on("FROM tasks", []string{"exploration_id"}, []driver.Value{int64(501)})

	metrics, err := d.TaskListMetricsAll()
	if err != nil {
		t.Fatalf("TaskListMetricsAll: %v", err)
	}
	if _, ok := metrics[501]; !ok {
		t.Fatalf("every live task must get a metrics row, got %+v", metrics)
	}
	for _, c := range eng.snapshotCalls() {
		expAssertOrdinal(t, c)
		upper := strings.ToUpper(c.Query)
		if strings.Contains(upper, "LATERAL") {
			t.Fatalf("TiDB rejects LATERAL joins; split the aggregates: %s", c.Query)
		}
		if strings.Contains(upper, "FILTER (WHERE") || strings.Contains(upper, "FILTER(WHERE") {
			t.Fatalf("COUNT(*) FILTER is PostgreSQL-only; use COUNT(CASE WHEN …): %s", c.Query)
		}
		if strings.Contains(upper, "EXTRACT(EPOCH") {
			t.Fatalf("use UNIX_TIMESTAMP for epoch reads: %s", c.Query)
		}
	}
}

func TestExploration_GoalCountsAllUsesCaseAggregation(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM exploration_nodes", []string{"exploration_id", "total", "met"},
		[]driver.Value{int64(501), int64(3), int64(1)})

	counts, err := d.GoalCountsAll()
	if err != nil {
		t.Fatalf("GoalCountsAll: %v", err)
	}
	if counts[501].Total != 3 || counts[501].Met != 1 {
		t.Fatalf("counts=%+v", counts[501])
	}
	stmt := eng.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "FROM exploration_nodes")
	})[0]
	if !strings.Contains(strings.ToUpper(stmt.Query), "CASE WHEN") {
		t.Fatalf("expected COUNT(CASE WHEN … THEN 1 END): %s", stmt.Query)
	}
	expAssertOrdinal(t, stmt)
}

func TestExploration_HasActiveIntentAndHasOpenGoalMapPresenceToBool(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*DB) (bool, error)
	}{
		{"HasActiveIntent", func(d *DB) (bool, error) { return d.Exploration(501).HasActiveIntent() }},
		{"HasOpenGoal", func(d *DB) (bool, error) { return d.Exploration(501).HasOpenGoal() }},
	} {
		for _, present := range []bool{true, false} {
			eng := newExpMockEngine()
			d := openExpMockDB(t, eng)
			if present {
				eng.on("FROM exploration_nodes", []string{"one"}, []driver.Value{int64(1)})
			} else {
				eng.on("FROM exploration_nodes", []string{"one"})
			}
			got, err := tc.call(d)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got != present {
				t.Fatalf("%s present=%v got=%v", tc.name, present, got)
			}
			for _, c := range eng.snapshotCalls() {
				expAssertOrdinal(t, c)
			}
		}
	}
}

func TestExploration_NewMainSessionSequencesInGo(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	stamp := time.Date(2026, 10, 19, 3, 4, 5, 0, time.UTC)
	eng.on("ORDER BY seq DESC", []string{"seq"}, []driver.Value{int64(4)})
	eng.on("SELECT created_at FROM main_sessions", []string{"created_at"}, []driver.Value{stamp})

	session, err := d.Exploration(501).NewMainSession()
	if err != nil {
		t.Fatalf("NewMainSession: %v", err)
	}
	if session.Seq != 5 {
		t.Fatalf("seq=%d, want the Go-computed 4+1", session.Seq)
	}
	if !session.CreatedAt.Equal(stamp) {
		t.Fatalf("created_at=%v, want the persisted %v", session.CreatedAt, stamp)
	}
	calls := eng.snapshotCalls()
	for _, c := range calls {
		expAssertOrdinal(t, c)
		if strings.Contains(strings.ToUpper(c.Query), "RETURNING") {
			t.Fatalf("TiDB has no RETURNING: %s", c.Query)
		}
	}
	var inserted bool
	for _, c := range calls {
		if strings.Contains(c.Query, "INSERT INTO main_sessions") {
			inserted = true
			if strings.Contains(strings.ToUpper(c.Query), "SELECT") && strings.Contains(strings.ToUpper(c.Query), "MAX(") {
				t.Fatalf("seq must be computed in Go and inserted explicitly: %s", c.Query)
			}
		}
	}
	if !inserted {
		t.Fatal("expected an explicit main_sessions INSERT")
	}
	if counts := eng.txCounts(); counts.Commits != 1 {
		t.Fatalf("commits=%d, want the read-compute-insert under one transaction", counts.Commits)
	}
}

// =====================================================================
// 既有集成用例（迁移到 TiDB 方言；无 DSN 时由 testDSN 显式 Skip）
// =====================================================================

func TestExplorationFlow(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()

	expID, err := d.CreateExploration("test", "拿下测试目标")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=?`, expID) // cascades nodes/edges/activity
	es := d.Exploration(expID)

	// goal node + two intents
	goal, err := es.AddGoal(map[string]any{"text": "getadmin", "vulnclass": "authz"}, "human")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := es.AddIntent(map[string]any{"summary": "enumerate endpoints"}, 5, nil, "planner"); err != nil {
		t.Fatal(err)
	}
	i2, err := es.AddIntent(map[string]any{"summary": "test idor"}, 8, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}

	// frontier ordered by priority desc → i2(8) before i1(5)
	fr, err := es.Frontier(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fr) != 2 || fr[0].ID != i2 {
		t.Fatalf("frontier order wrong: %+v", fr)
	}

	// atomic claim: first wins, second on same id fails
	ok, err := es.ClaimIntent(i2, "worker-1")
	if err != nil || !ok {
		t.Fatalf("claim i2: ok=%v err=%v", ok, err)
	}
	ok2, _ := es.ClaimIntent(i2, "worker-2")
	if ok2 {
		t.Fatalf("double-claim should fail")
	}

	// finding yields from intent, proves goal
	find, err := es.AddNode("finding", map[string]any{"vulnclass": "idor", "severity": "high"}, 9, "confirmed", "worker-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := es.Link(i2, "yields", find); err != nil {
		t.Fatal(err)
	}
	if err := es.Link(find, "proves", goal); err != nil {
		t.Fatal(err)
	}
	if err := es.SetNodeState(goal, "met"); err != nil {
		t.Fatal(err)
	}

	// lineage: ancestors of the finding traced backward — here {i2, find} joined by
	// the yields edge. The proves→goal edge is DOWNSTREAM (goal must be excluded),
	// and the unrelated intent i1 is not on any path to the finding (excluded too).
	lnNodes, lnEdges, err := es.FindingLineage(find)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for _, n := range lnNodes {
		got[n.ID] = true
	}
	if len(lnNodes) != 2 || !got[find] || !got[i2] {
		t.Fatalf("lineage nodes: want {i2,find}, got %+v", lnNodes)
	}
	if got[goal] {
		t.Fatalf("lineage must exclude the proved goal (it is downstream of the finding)")
	}
	if len(lnEdges) != 1 || lnEdges[0].From != i2 || lnEdges[0].To != find || lnEdges[0].Rel != "yields" {
		t.Fatalf("lineage edges: want i2-yields->find, got %+v", lnEdges)
	}

	// activity poll by id cursor
	id1, err := es.AppendActivity(Activity{Worker: "worker-1", Kind: "tool_use", Tool: "Bash", Summary: "ran curl", Detail: "full output"})
	if err != nil {
		t.Fatal(err)
	}
	items, cursor, err := es.ActivityList(nil, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || cursor != id1 {
		t.Fatalf("activity list: items=%d cursor=%d", len(items), cursor)
	}
	det, _ := es.ActivityDetail(id1)
	if det != "full output" {
		t.Fatalf("detail want 'full output', got %q", det)
	}
	// incremental: nothing new after cursor
	items2, _, _ := es.ActivityList(nil, cursor, 100)
	if len(items2) != 0 {
		t.Fatalf("incremental poll should be empty, got %d", len(items2))
	}

	// stats
	st, _ := es.Stats()
	if st["intent"] != 2 || st["goal"] != 1 || st["finding"] != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestIntentPauseResumeAndCancelCleanup(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()

	expID, err := d.CreateExploration("test", "worker control cleanup")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=?`, expID)
	es := d.Exploration(expID)

	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{
		Domain: fmt.Sprintf("cancel-intent-%d.invalid", expID),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, assetID)

	otherIntent, err := es.AddIntent(map[string]any{"summary": "keep intent"}, 1, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	intentID, err := es.AddIntent(map[string]any{"summary": "cancel intent"}, 10, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := es.ClaimIntent(intentID, "worker-control-test"); err != nil || !claimed {
		t.Fatalf("initial claim: claimed=%v err=%v", claimed, err)
	}
	if err := es.SetIntentState(intentID, "paused"); err != nil {
		t.Fatal(err)
	}

	frontier, err := es.Frontier(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range frontier {
		if node.ID == intentID {
			t.Fatalf("paused intent %d must not enter frontier", intentID)
		}
	}
	if claimed, err := es.ClaimIntent(intentID, "worker-while-paused"); err != nil || claimed {
		t.Fatalf("paused intent claim: claimed=%v err=%v", claimed, err)
	}
	if err := es.SetIntentState(intentID, "open"); err != nil {
		t.Fatal(err)
	}
	if claimed, err := es.ClaimIntent(intentID, "worker-after-resume"); err != nil || !claimed {
		t.Fatalf("resumed intent claim: claimed=%v err=%v", claimed, err)
	}
	if err := es.SetIntentState(intentID, "paused"); err != nil {
		t.Fatal(err)
	}

	directFact, err := es.AddNode("fact", map[string]any{"summary": "remove fact"}, 0, "confirmed", "worker", []int64{assetID})
	if err != nil {
		t.Fatal(err)
	}
	directFinding, err := es.AddNode("finding", map[string]any{"summary": "remove finding"}, 0, "confirmed", "worker", nil)
	if err != nil {
		t.Fatal(err)
	}
	keptFact, err := es.AddNode("fact", map[string]any{"summary": "keep fact"}, 0, "confirmed", "other-worker", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := es.Link(intentID, RelYields, directFact); err != nil {
		t.Fatal(err)
	}
	if err := es.Link(intentID, RelYields, directFinding); err != nil {
		t.Fatal(err)
	}
	findingRowID, err := es.AddStandaloneFinding(0, directFinding, "test", "cancelled finding", SeverityHigh, "summary", "evidence", "worker", []int64{assetID})
	if err != nil {
		t.Fatal(err)
	}
	activityID, err := es.AppendActivity(Activity{NodeID: &intentID, Worker: "worker", Kind: "result", Summary: "remove activity"})
	if err != nil {
		t.Fatal(err)
	}
	keptActivityID, err := es.AppendActivity(Activity{NodeID: &otherIntent, Worker: "other-worker", Kind: "result", Summary: "keep activity"})
	if err != nil {
		t.Fatal(err)
	}

	cleanup, err := es.CancelIntent(intentID)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup.Intents != 1 || cleanup.Facts != 1 || cleanup.Findings != 1 || cleanup.Activities != 1 {
		t.Fatalf("unexpected cleanup counts: %+v", cleanup)
	}
	for _, nodeID := range []int64{intentID, directFact, directFinding} {
		node, err := es.GetNode(nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if node != nil {
			t.Fatalf("node %d survived intent cancellation", nodeID)
		}
	}
	for _, nodeID := range []int64{otherIntent, keptFact} {
		node, err := es.GetNode(nodeID)
		if err != nil || node == nil {
			t.Fatalf("unrelated node %d removed: node=%v err=%v", nodeID, node, err)
		}
	}

	assertCount := func(query string, want int, args ...any) {
		t.Helper()
		var got int
		if err := d.QueryRow(query, args...).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("query count=%d, want %d: %s", got, want, query)
		}
	}
	assertCount(`SELECT COUNT(*) FROM findings WHERE id=?`, 0, findingRowID)
	assertCount(`SELECT COUNT(*) FROM activity WHERE id=?`, 0, activityID)
	assertCount(`SELECT COUNT(*) FROM activity WHERE id=?`, 1, keptActivityID)
	assertCount(`SELECT COUNT(*) FROM exploration_edges WHERE exploration_id=? AND (src_id=? OR dst_id=?)`, 0, expID, intentID)
	assertCount(`SELECT COUNT(*) FROM exploration_anchors WHERE node_id=?`, 0, directFact)
	assertCount(`SELECT COUNT(*) FROM assets WHERE id=?`, 1, assetID)
}

// TestNodesPageQueryMatchesID verifies the 播报板 search filters on node id (both
// the bare number and the「#id」form the UI shows) in addition to payload/origin.
func TestNodesPageQueryMatchesID(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()

	expID, err := d.CreateExploration("test", "id 搜索")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=?`, expID)
	es := d.Exploration(expID)

	target, err := es.AddNode("fact", map[string]any{"summary": "needle-alpha"}, 0, "confirmed", "worker-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := es.AddNode("fact", map[string]any{"summary": "unrelated-beta"}, 0, "confirmed", "worker-b", nil)
	if err != nil {
		t.Fatal(err)
	}

	onlyTarget := func(label, q string) {
		t.Helper()
		nodes, total, err := es.NodesPage(NodeFilter{Query: q}, 1, 50)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if total != 1 || len(nodes) != 1 || nodes[0].ID != target {
			t.Fatalf("%s: q=%q total=%d nodes=%+v, want single node %d", label, q, total, nodes, target)
		}
	}

	onlyTarget("bare id", fmt.Sprint(target))
	onlyTarget("hash id", "#"+fmt.Sprint(target))
	onlyTarget("payload still works", "needle-alpha")

	// A non-matching numeric id returns nothing (and does not accidentally match other).
	if nodes, total, err := es.NodesPage(NodeFilter{Query: fmt.Sprint(target + other + 1000)}, 1, 50); err != nil {
		t.Fatal(err)
	} else if total != 0 || len(nodes) != 0 {
		t.Fatalf("non-existent id: total=%d nodes=%+v, want empty", total, nodes)
	}
}

// =====================================================================
// 语句级替身驱动（离线）
// =====================================================================

// expMockCall 是一条真实经过 database/sql 的语句。
type expMockCall struct {
	Kind  string // "query" | "exec"
	Query string
	Args  []driver.Value
}

type expMockRule struct {
	substr string
	cols   []string
	rows   [][]driver.Value
}

type expTxCounts struct{ Commits, Rollbacks int }

// expMockEngine 记录语句并按子串命中返回结果集；INSERT 的自增主键由 nextID 控制，
// 用于把 LastInsertId 的取值（含 0 / 负数等坏值）变成可注入的输入。
type expMockEngine struct {
	mu        sync.Mutex
	calls     []expMockCall
	rules     []expMockRule
	nextID    int64
	commits   int
	rollbacks int
}

func newExpMockEngine() *expMockEngine {
	return &expMockEngine{nextID: 1001}
}

func (e *expMockEngine) on(substr string, cols []string, rows ...[]driver.Value) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, expMockRule{substr: substr, cols: cols, rows: rows})
}

func (e *expMockEngine) setNextInsertID(id int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextID = id
}

func (e *expMockEngine) nextInsertID() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := e.nextID
	e.nextID++
	return id
}

func (e *expMockEngine) record(c expMockCall) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, c)
}

func (e *expMockEngine) snapshotCalls() []expMockCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]expMockCall, len(e.calls))
	copy(out, e.calls)
	return out
}

func (e *expMockEngine) resetCalls() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = nil
}

func (e *expMockEngine) statements(pred func(expMockCall) bool) []expMockCall {
	var out []expMockCall
	for _, c := range e.snapshotCalls() {
		if pred(c) {
			out = append(out, c)
		}
	}
	return out
}

func (e *expMockEngine) txCounts() expTxCounts {
	e.mu.Lock()
	defer e.mu.Unlock()
	return expTxCounts{Commits: e.commits, Rollbacks: e.rollbacks}
}

func (e *expMockEngine) answer(query string) (expMockRule, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.rules {
		if r.substr == "" || strings.Contains(query, r.substr) {
			return r, true
		}
	}
	return expMockRule{}, false
}

type expMockRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *expMockRows) Columns() []string { return r.cols }
func (r *expMockRows) Close() error      { return nil }
func (r *expMockRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

type expMockResult struct {
	id   int64
	rows int64
}

func (r expMockResult) LastInsertId() (int64, error) { return r.id, nil }
func (r expMockResult) RowsAffected() (int64, error) { return r.rows, nil }

type expMockConn struct{ eng *expMockEngine }

func (c *expMockConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("expMock: prepared statements disabled, all SQL must flow through Queryer/Execer")
}
func (c *expMockConn) Close() error { return nil }
func (c *expMockConn) Begin() (driver.Tx, error) {
	return expMockTx{eng: c.eng}, nil
}
func (c *expMockConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return expMockTx{eng: c.eng}, nil
}

func (c *expMockConn) expArgs(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, 0, len(args))
	for _, a := range args {
		out = append(out, a.Value)
	}
	return out
}

func (c *expMockConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := c.expArgs(args)
	c.eng.record(expMockCall{Kind: "query", Query: query, Args: values})
	if rule, ok := c.eng.answer(query); ok {
		cols := rule.cols
		if cols == nil {
			cols = []string{"_"}
		}
		return &expMockRows{cols: cols, rows: rule.rows}, nil
	}
	// 未预置规则的读：返回一列空集，调用方 Scan 会因列数不匹配立刻暴露。
	return &expMockRows{cols: []string{"_"}}, nil
}

func (c *expMockConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := c.expArgs(args)
	c.eng.record(expMockCall{Kind: "exec", Query: query, Args: values})
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "INSERT") {
		return expMockResult{id: c.eng.nextInsertID(), rows: 1}, nil
	}
	return expMockResult{id: 0, rows: 1}, nil
}

type expMockTx struct{ eng *expMockEngine }

func (t expMockTx) Commit() error   { t.eng.commit(); return nil }
func (t expMockTx) Rollback() error { t.eng.rollback(); return nil }

func (t expMockTx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// 事务内的读与外层同连接，直接复用同一份实现。
	return (&expMockConn{eng: t.eng}).QueryContext(ctx, query, args)
}

func (t expMockTx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return (&expMockConn{eng: t.eng}).ExecContext(ctx, query, args)
}

func (e *expMockEngine) commit()   { e.mu.Lock(); e.commits++; e.mu.Unlock() }
func (e *expMockEngine) rollback() { e.mu.Lock(); e.rollbacks++; e.mu.Unlock() }

type expMockDriver struct{}

func (expMockDriver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("expMock: dsn unused") }

type expMockConnector struct{ conn *expMockConn }

func (c expMockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c expMockConnector) Driver() driver.Driver                        { return expMockDriver{} }

// openExpMockDB 构造一个不落任何真实连接的 *DB：单连接保证语句顺序与事务计数可读。
func openExpMockDB(t *testing.T, eng *expMockEngine) *DB {
	t.Helper()
	sqlDB := sql.OpenDB(expMockConnector{conn: &expMockConn{eng: eng}})
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return &DB{sqlDB}
}

// expPGOrdinalRe 只认 `$` 紧跟数字的 PG 序号占位符；JSON 路径（'$.summary'）里的
// `$` 是合法 TiDB 文本，不能误判。
var expPGOrdinalRe = regexp.MustCompile(`\$\d`)

// expAssertOrdinal 是本文件的核心守卫：`?` 个数必须与实参数逐位相等。
func expAssertOrdinal(t *testing.T, c expMockCall) {
	t.Helper()
	if got, want := strings.Count(c.Query, "?"), len(c.Args); got != want {
		t.Fatalf("placeholder/arg mismatch (%d `?` vs %d args) in %s %s: %v", got, want, c.Kind, c.Query, c.Args)
	}
	if expPGOrdinalRe.MatchString(c.Query) {
		t.Fatalf("PostgreSQL ordinal placeholder survived in %s: %s", c.Kind, c.Query)
	}
}

func queriesOf(calls []expMockCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.Query)
	}
	return out
}

func expPtr[T any](v T) *T {
	out := v
	return &out
}

func expNodeColumns() []string {
	return []string{"id", "kind", "payload", "priority", "state", "origin", "owner", "blocked_reason", "delete_reason", "created_at"}
}

func expNodeRow(id int64, kind, state string) []driver.Value {
	return []driver.Value{
		id, kind, []byte(`{"summary":"n"}`), int64(0), state, "worker", "", "", "",
		time.Date(2026, 10, 19, 1, 2, 3, 0, time.UTC),
	}
}
