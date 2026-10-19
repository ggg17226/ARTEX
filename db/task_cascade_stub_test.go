package db

// =====================================================================
// P08-02 Task 2.3 修复轮次 2 —— 离线语句级替身驱动
//
// 交付审查（R1 / R4 / R5）要的是**控制流与语句事实**：
//   * Fresh Locator 是否真的收齐 finding 子表的物理 PK 并纳入同源锁计划；
//   * 持锁后的成员复核是否**一条 FOR UPDATE 都不发**（只发现、不补锁）；
//   * 发现计划外成员时是否整体回滚重放，而不是在原事务里逆序补锁；
//   * prepare 暂存的外部工作是否在重试退避**之前**被撤销（不重入死锁）。
//
// 本文件用一个可编程的 `database/sql/driver` 替身把这些事实变成可断言的观测：
// 记录每一条真实经过 database/sql 的语句、按"第几次命中 + 哪条连接"返回不同
// 结果集（制造"只有复核连接才看见的新成员"），并对指定语句注入 1213 死锁。
// 它证明的是"实现发出了什么 SQL / 走了哪条分支"，不冒充 TiDB 真机锁行为。
// =====================================================================

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Autumn-27/artex/db/lockorder"
)

// ── 结构化错误替身 ───────────────────────────────────────────────────────────

// stubMySQLErr 是 *mysql.MySQLError 的替身：lockorder.sqlErrorNumber 除原生类型外，
// 也接受实现 `Number() uint16` 的包装类型（R2 I6 的结构化提取规范）。
type stubMySQLErr struct {
	number uint16
	msg    string
}

func (e stubMySQLErr) Error() string  { return fmt.Sprintf("stub sql err %d: %s", e.number, e.msg) }
func (e stubMySQLErr) Number() uint16 { return e.number }

// ── 可编程应答 ───────────────────────────────────────────────────────────────

// stubRule 按语句匹配返回结果集。hit 是该规则的第几次命中（1 起），conn 是发出该
// 语句的连接序号（1 = 外层事务连接，≥2 = 复核用的第二条连接）。
type stubRule struct {
	match func(query string) bool
	rows  func(hit int, conn int64) [][]driver.Value
}

type stubCall struct {
	Kind  string // "query" | "exec"
	Conn  int64
	Query string
	Args  []driver.Value
}

type stubState struct {
	mu       sync.Mutex
	calls    []stubCall
	rules    []stubRule
	fail     func(call stubCall) error // 对指定 exec 注入错误
	lockRead int
}

func newStubState(rules ...stubRule) *stubState {
	return &stubState{rules: rules}
}

func (s *stubState) record(c stubCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, c)
	if strings.Contains(strings.ToUpper(c.Query), "FOR UPDATE") {
		s.lockRead++
	}
}

func (s *stubState) snapshotCalls() []stubCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]stubCall, len(s.calls))
	copy(out, s.calls)
	return out
}

func (s *stubState) lockReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockRead
}

// statements 返回命中谓词的语句（顺序保持）。
func (s *stubState) statements(pred func(stubCall) bool) []stubCall {
	var out []stubCall
	for _, c := range s.snapshotCalls() {
		if pred(c) {
			out = append(out, c)
		}
	}
	return out
}

// answer 计算一条查询的应答。锁读（含 FOR UPDATE）一律回显请求的主键，
// 等价于"该行存在且已被本事务锁住"，否则 LockEntitiesInOrder 会按实体缺失报错。
func (s *stubState) answer(query string, args []driver.Value, conn int64) [][]driver.Value {
	s.mu.Lock()
	defer s.mu.Unlock()
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "FOR UPDATE") {
		// 实体锁读：`SELECT <pk> FROM <t> WHERE <pk> = ? FOR UPDATE` → 回显主键。
		if len(args) > 0 {
			return [][]driver.Value{{args[len(args)-1]}}
		}
		// 全局门禁行：`SELECT name FROM locks WHERE name = 'business_relations' FOR UPDATE`
		// 名字直接内联、无绑定参数 → 命中即可，值不参与判定。
		return [][]driver.Value{{""}}
	}
	for i := range s.rules {
		if s.rules[i].match != nil && s.rules[i].match(query) {
			hits := 1
			for _, c := range s.calls {
				if c.Kind == "query" && s.rules[i].match(c.Query) {
					hits++
				}
			}
			return s.rules[i].rows(hits, conn)
		}
	}
	return nil
}

func (s *stubState) failure(c stubCall) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail == nil {
		return nil
	}
	return s.fail(c)
}

// ── driver 实现 ─────────────────────────────────────────────────────────────

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("stub: dsn unused") }

type stubConnector struct {
	state *stubState
	conns *int64
}

func (c *stubConnector) Driver() driver.Driver { return stubDriver{} }

func (c *stubConnector) Connect(context.Context) (driver.Conn, error) {
	return &stubConn{state: c.state, id: atomic.AddInt64(c.conns, 1)}, nil
}

type stubConn struct {
	state *stubState
	id    int64
}

func (c *stubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("stub: prepared statements unused")
}
func (c *stubConn) Close() error              { return nil }
func (c *stubConn) Begin() (driver.Tx, error) { return stubTx{}, nil }

// BeginTx 一律放行（含 READ ONLY）：成员复核走只读定位事务，替身驱动必须支持，
// 否则 database/sql 在进入任何语句之前就报 "does not support read-only transactions"。
func (c *stubConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return stubTx{}, nil
}

func (c *stubConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := namedValues(args)
	c.state.record(stubCall{Kind: "query", Conn: c.id, Query: query, Args: values})
	return &stubRows{rows: c.state.answer(query, values, c.id)}, nil
}

func (c *stubConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	call := stubCall{Kind: "exec", Conn: c.id, Query: query, Args: namedValues(args)}
	c.state.record(call)
	if err := c.state.failure(call); err != nil {
		return nil, err
	}
	return stubResult{}, nil
}

type stubTx struct{}

func (stubTx) Commit() error   { return nil }
func (stubTx) Rollback() error { return nil }

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 1, nil }
func (stubResult) RowsAffected() (int64, error) { return 1, nil }

type stubRows struct {
	rows [][]driver.Value
	pos  int
}

func (r *stubRows) Columns() []string {
	width := 1
	if len(r.rows) > 0 {
		width = len(r.rows[0])
	}
	cols := make([]string, width)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d", i)
	}
	return cols
}
func (r *stubRows) Close() error { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

func namedValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// openStubDB 打开替身数据库。连接上限刻意保持默认（>1）：成员复核会向**第二条
// 连接**取只读视图，MaxOpenConns(1) 会把那条连接饿死。
func openStubDB(t *testing.T, state *stubState) *DB {
	t.Helper()
	sqlDB := sql.OpenDB(&stubConnector{state: state, conns: new(int64)})
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close stub db: %v", err)
		}
	})
	return &DB{DB: sqlDB}
}

// ── 剧本：task 101 / exploration 201 / nodes {301,302} / findings {801,802} ─────

const (
	stubTask        = 101
	stubExploration = 201
)

func stubRowsAlways(ids ...uint64) func(int, int64) [][]driver.Value {
	return func(int, int64) [][]driver.Value {
		rows := make([][]driver.Value, 0, len(ids))
		for _, id := range ids {
			rows = append(rows, []driver.Value{int64(id)})
		}
		return rows
	}
}

func stubMatch(sub string) func(string) bool {
	return func(q string) bool { return strings.Contains(q, sub) }
}

// stubCascadeRules 是级联主路径的默认剧本。finding 子表只有开启 DeleteFindings 时
// 才会被枚举，剧本对关闭情形同样返回行（由实现的分支决定是否发问）。
func stubCascadeRules() []stubRule {
	return []stubRule{
		// 收尾对账 AssertNoOrphansForTask 的一组 SELECT COUNT(1)：一律 0 条残留。
		// 必须排在最前面，否则会被下面按表名匹配的枚举规则误捕。
		{match: stubMatch("COUNT("), rows: func(int, int64) [][]driver.Value {
			return [][]driver.Value{{int64(0)}}
		}},
		{match: stubMatch("SELECT exploration_id FROM tasks"), rows: stubRowsAlways(stubExploration)},
		{match: stubMatch("FROM exploration_nodes WHERE exploration_id"), rows: stubRowsAlways(301, 302)},
		{match: stubMatch("FROM findings WHERE task_id"), rows: stubRowsAlways(801)},
		{match: stubMatch("FROM findings WHERE node_id IN"), rows: stubRowsAlways(802)},
		{match: stubMatch("FROM finding_assets WHERE finding_id IN"), rows: stubRowsAlways(9001, 9002)},
		{match: stubMatch("FROM finding_retests WHERE finding_id IN"), rows: stubRowsAlways(9101)},
		{match: stubMatch("FROM finding_traffic_bindings WHERE finding_id IN"), rows: stubRowsAlways(9201)},
		{match: stubMatch("SELECT row_id FROM task_asset_links WHERE task_id"), rows: stubRowsAlways(501)},
		{match: stubMatch("SELECT id FROM activity WHERE node_id IN"), rows: stubRowsAlways(402)},
		{match: stubMatch("SELECT row_id FROM task_asset_links WHERE source_node_id IN"), rows: stubRowsAlways(502)},
	}
}

func newCascadeStub() *stubState { return newStubState(stubCascadeRules()...) }

// ── R1：Fresh Locator 收齐 finding 子表物理 PK 并纳入同源锁计划 ────────────────

func TestTask_ClosureLocator_EnumeratesFindingChildPKs(t *testing.T) {
	state := newCascadeStub()
	d := openStubDB(t, state)

	closure, err := ComputeTaskClosureWithOptions(context.Background(), d.DB, stubTask,
		TaskClosureOptions{DeleteFindings: true})
	if err != nil {
		t.Fatalf("compute closure failed: %v", err)
	}
	for name, got := range map[string][]uint64{
		"finding_assets":           closure.FindingAssetIDs,
		"finding_retests":          closure.FindingRetestIDs,
		"finding_traffic_bindings": closure.FindingTrafficBindingIDs,
	} {
		if len(got) == 0 {
			t.Fatalf("%s physical PKs were not enumerated by the locator", name)
		}
	}
	if got := fmt.Sprint(closure.FindingAssetIDs); got != "[9001 9002]" {
		t.Fatalf("finding_assets PKs = %s, want [9001 9002]", got)
	}

	plan, err := closure.LockPlan()
	if err != nil {
		t.Fatalf("lock plan: %v", err)
	}
	members, err := closure.MembershipKeys()
	if err != nil {
		t.Fatalf("membership keys: %v", err)
	}
	if len(plan) != len(members) {
		t.Fatalf("LockPlan (%d) and MembershipKeys (%d) must be the same source set", len(plan), len(members))
	}
	if !lockorder.IsSorted(plan) {
		t.Fatalf("lock plan is not strictly sorted:%v", plan)
	}
	for _, table := range []string{"finding_assets", "finding_retests", "finding_traffic_bindings", "findings"} {
		meta, err := lockorder.LookupTableName(table)
		if err != nil {
			t.Fatalf("%s is not in the lock-order catalog: %v", table, err)
		}
		order := meta.Order
		var got []uint64
		for _, key := range plan {
			if key.Table == order {
				got = append(got, key.PK)
			}
		}
		if len(got) == 0 {
			t.Fatalf("%s physical PKs are absent from the lock plan:%v", table, plan)
		}
		for i := 1; i < len(got); i++ {
			if got[i-1] >= got[i] {
				t.Fatalf("%s PKs are not strictly ascending in the lock plan: %v", table, got)
			}
		}
	}
	if err := closure.Validate(); err != nil {
		t.Fatalf("closure validate: %v", err)
	}
	// 定位阶段必须零加锁（Fresh Locator 契约）。
	if n := state.lockReads(); n != 0 {
		t.Fatalf("locator must not take a single row lock, got %d FOR UPDATE statements", n)
	}
}

// TestTask_ClosureLocator_CrossRootPhysicalIDsEnumerated 测试 R6：
// 证明真实 ComputeTaskClosure 能够端到端收集到跨根 activity 与跨根 task_asset_links，
// 且若这些跨根查询被破坏或漏收，测试必将判红。
func TestTask_ClosureLocator_CrossRootPhysicalIDsEnumerated(t *testing.T) {
	state := newCascadeStub()
	d := openStubDB(t, state)

	closure, err := ComputeTaskClosureWithOptions(context.Background(), d.DB, stubTask,
		TaskClosureOptions{DeleteFindings: true})
	if err != nil {
		t.Fatalf("compute closure failed: %v", err)
	}
	if len(closure.CrossRootActivityIDs) == 0 {
		t.Fatal("locator failed to collect CrossRootActivityIDs from other explorations")
	}
	if len(closure.CrossRootLinkIDs) == 0 {
		t.Fatal("locator failed to collect CrossRootLinkIDs from other tasks")
	}
	if fmt.Sprint(closure.CrossRootActivityIDs) != "[402]" {
		t.Fatalf("CrossRootActivityIDs = %v, want [402]", closure.CrossRootActivityIDs)
	}
	if fmt.Sprint(closure.CrossRootLinkIDs) != "[502]" {
		t.Fatalf("CrossRootLinkIDs = %v, want [502]", closure.CrossRootLinkIDs)
	}
}

func TestTask_ClosureLocator_FindingChildrenRequireDeleteFindings(t *testing.T) {
	state := newCascadeStub()
	d := openStubDB(t, state)

	closure, err := ComputeTaskClosureWithOptions(context.Background(), d.DB, stubTask, TaskClosureOptions{})
	if err != nil {
		t.Fatalf("compute closure failed: %v", err)
	}
	if len(closure.FindingAssetIDs)+len(closure.FindingRetestIDs)+len(closure.FindingTrafficBindingIDs) != 0 {
		t.Fatal("finding children must stay uncollected while DeleteFindings is off")
	}
	// 关闭删除选项却带着子行集合 = 越界写集，Validate 必须拒绝。
	closure.FindingAssetIDs = []uint64{9001}
	if err := closure.Validate(); err == nil {
		t.Fatal("Validate must reject finding child rows enumerated without DeleteFindings")
	}
	for _, c := range state.statements(stubMatchCall("finding_assets")) {
		t.Fatalf("finding_assets must not be read while DeleteFindings is off: %s", c.Query)
	}
}

// ── R1：executor 的写集全部落在审计里，且严格绑在已锁物理 PK 上 ────────────────

// auditedExecer 同时记录"底层实际收到的每一条 DML"和"审计钩子看到的每一条 DML"。
// 两者必须一一对应：任何绕过 narrowWriter 的裸 SQL 都会让两者数量不等（R1 判据）。
type auditedExecer struct {
	mu      sync.Mutex
	raw     []stubCall
	hooked  []CascadeStatementRecord
	hookErr error
}

func (a *auditedExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = driver.Value(fmt.Sprint(arg))
	}
	a.raw = append(a.raw, stubCall{Kind: "exec", Query: query, Args: values})
	return fakeResult{rows: 1}, nil
}

func (a *auditedExecer) record(rec CascadeStatementRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hooked = append(a.hooked, rec)
}

func TestTask_Cascade_FindingChildWritesAreAuditedAndNarrow(t *testing.T) {
	closure := &TaskCascadeClosure{
		TaskID:                fixtureTaskA,
		ExplorationID:         fixtureExploration,
		Options:               TaskClosureOptions{DeleteFindings: true},
		NodeIDs:               []uint64{fixtureNode1},
		FindingIDs:            []uint64{fixtureFindingTask, fixtureFindingNode},
		NullifyFindingTaskIDs: []uint64{fixtureFindingTask},
		NullifyFindingNodeIDs: []uint64{fixtureFindingNode},

		FindingAssetIDs:          []uint64{9001, 9002},
		FindingRetestIDs:         []uint64{9101},
		FindingTrafficBindingIDs: []uint64{9201},
	}
	if _, err := closure.LockPlan(); err != nil {
		t.Fatalf("lock plan: %v", err)
	}

	execer := &auditedExecer{}
	previous := CascadeStatementHook
	hookCalls := 0
	CascadeStatementHook = func(rec CascadeStatementRecord) {
		hookCalls++
		execer.record(rec)
	}
	defer func() { CascadeStatementHook = previous }()

	audit, err := executeTaskCascade(context.Background(), execer, closure, nil)
	if err != nil {
		t.Fatalf("execute cascade: %v", err)
	}
	if hookCalls != len(execer.raw) {
		t.Fatalf("SQL audit must cover every DML: hook=%d executed=%d", hookCalls, len(execer.raw))
	}
	for _, table := range []string{"finding_assets", "finding_retests", "finding_traffic_bindings"} {
		if _, err := lockorder.LookupTableName(table); err != nil {
			t.Fatalf("%s is not in the lock-order catalog: %v", table, err)
		}
		deletes := execer.statements(table)
		if len(deletes) == 0 {
			t.Fatalf("%s was not deleted by physical PK:%v", table, execer.raw)
		}
		for _, c := range deletes {
			upper := strings.ToUpper(c.Query)
			if strings.Contains(upper, "FINDING_ID") {
				t.Fatalf("%s must be cleaned by its own physical PK, not by an open finding_id range: %s", table, c.Query)
			}
			if !strings.Contains(upper, "WHERE ID IN (") {
				t.Fatalf("%s delete is not bound to a physical PK list: %s", table, c.Query)
			}
		}
	}
	// 逆拓扑：三条子表删除必须早于 findings 本体。
	findingsDeleteAt, childAt := -1, -1
	for i, c := range execer.raw {
		upper := strings.ToUpper(c.Query)
		if strings.Contains(upper, "FROM FINDINGS") && findingsDeleteAt < 0 {
			findingsDeleteAt = i
		}
		if strings.Contains(upper, "FINDING_ASSETS") {
			childAt = i
		}
	}
	if !(childAt >= 0 && findingsDeleteAt >= 0 && childAt < findingsDeleteAt) {
		t.Fatalf("finding children must be deleted before their parent findings (child=%d findings=%d)", childAt, findingsDeleteAt)
	}
	for _, table := range []string{"finding_assets", "finding_retests", "finding_traffic_bindings"} {
		if audit.EntitiesDeleted[table] == 0 {
			t.Fatalf("audit must count %s deletions, got %d", table, audit.EntitiesDeleted[table])
		}
	}
}

func (a *auditedExecer) statements(table string) []stubCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []stubCall
	for _, c := range a.raw {
		if strings.Contains(strings.ToUpper(c.Query), "DELETE FROM "+strings.ToUpper(table)) {
			out = append(out, c)
		}
	}
	return out
}

// ── R5：成员复核只发现、绝不补锁 ─────────────────────────────────────────────

func stubMatchCall(sub string) func(stubCall) bool {
	return func(c stubCall) bool { return strings.Contains(c.Query, sub) }
}

func TestTask_MembershipRecheckNeverLocks(t *testing.T) {
	state := newCascadeStub()
	d := openStubDB(t, state)

	keys, err := RelocateTaskClosureMembers(context.Background(), d.DB, stubTask,
		TaskClosureOptions{DeleteFindings: true})
	if err != nil {
		t.Fatalf("relocate members: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("relocator returned an empty membership set")
	}
	locks := state.statements(func(c stubCall) bool {
		return strings.Contains(strings.ToUpper(c.Query), "FOR UPDATE")
	})
	if len(locks) != 0 {
		t.Fatalf("membership recheck must not issue a single FOR UPDATE (that is backwards lock insertion); got %d, first: %s",
			len(locks), locks[0].Query)
	}
	if len(state.statements(stubMatchCall("finding_assets"))) == 0 {
		t.Fatal("recheck must re-enumerate finding children with the same membership SQL")
	}
}

// TestTask_MembershipDriftIsDiscoveredWithoutExtraLock 让**只有复核连接**看得见
// 一条更低 PK 的新成员 300：实现必须报 ErrMembershipDrift、整体回滚重放，
// 且绝不为这条计划外成员补锁。
func TestTask_MembershipDriftIsDiscoveredWithoutExtraLock(t *testing.T) {
	driftNodes := stubRule{
		match: stubMatch("FROM exploration_nodes WHERE exploration_id"),
		rows: func(_ int, conn int64) [][]driver.Value {
			if conn >= 2 {
				return [][]driver.Value{{int64(300)}, {int64(301)}, {int64(302)}}
			}
			return [][]driver.Value{{int64(301)}, {int64(302)}}
		},
	}
	// 漂移规则排在最前，优先于默认枚举剧本。
	state := newStubState(append([]stubRule{driftNodes}, stubCascadeRules()...)...)
	d := openStubDB(t, state)

	_, err := d.DeleteTaskCascadePrepared(stubTask, false, true, false, nil)
	if err == nil {
		t.Fatal("a member discovered after locking must fail the attempt")
	}
	// 错误链必须可分类：既看得到"重试耗尽"，也看得到真正的失败原因是成员漂移
	// （R2 I6：结构化提取不许退化成字符串匹配）。
	if !errors.Is(err, lockorder.ErrRetryExhausted) {
		t.Fatalf("drift must exhaust the retry budget, got %v", err)
	}
	if !errors.Is(err, lockorder.ErrMembershipDrift) {
		t.Fatalf("drift must surface as ErrMembershipDrift through the wrapper chain, got %v", err)
	}
	// 复核确实跑在第二条连接上（这是"看见新提交"的来源）。
	recheckOnSecondConn := false
	for _, c := range state.statements(stubMatchCall("FROM exploration_nodes WHERE exploration_id")) {
		if c.Conn >= 2 {
			recheckOnSecondConn = true
		}
	}
	if !recheckOnSecondConn {
		t.Fatal("membership recheck must run on an independent second connection")
	}
	// 计划外的新成员 300 绝不能被补锁。
	for _, c := range state.statements(func(c stubCall) bool {
		return strings.Contains(strings.ToUpper(c.Query), "FOR UPDATE")
	}) {
		for _, a := range c.Args {
			if v, ok := a.(int64); ok && v == 300 {
				t.Fatalf("implementation locked the unplanned member 300 (backwards lock insertion): %s", c.Query)
			}
		}
	}
	// 漂移必须在任何级联 DML 之前被发现：整个流程零业务写入。
	for _, c := range state.statements(func(c stubCall) bool {
		return c.Kind == "exec" && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(c.Query)), "DELETE FROM")
	}) {
		t.Fatalf("membership drift must roll back before any cascade DML, saw: %s", c.Query)
	}
}

// ── R4：prepare 的外部暂存必须在重试退避之前被撤销 ─────────────────────────────

// stubAttemptStage 复刻 traffic.HostDeleteStage 的关键性质：一次暂存持有**同一把
// 不可重入**的写锁。这里用"上一轮仍持锁就立刻报错"代替真死锁，
// 让重入回归在测试里可见，而不是把测试挂死。
type stubAttemptStage struct {
	mu      sync.Mutex
	held    bool
	staged  int32
	undone  int32
	live    int32
	maxLive int32
}

func (s *stubAttemptStage) prepare(TaskDeletePreparation) (TaskDeleteUndo, error) {
	attempt := atomic.AddInt32(&s.staged, 1)
	s.mu.Lock()
	if s.held {
		s.mu.Unlock()
		return nil, fmt.Errorf("re-entered staging while attempt %d still holds the external writer lock", attempt)
	}
	s.held = true
	s.mu.Unlock()

	live := atomic.AddInt32(&s.live, 1)
	if live > atomic.LoadInt32(&s.maxLive) {
		atomic.StoreInt32(&s.maxLive, live)
	}
	var once sync.Once
	return func() error {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.held {
				s.held = false
				atomic.AddInt32(&s.undone, 1)
				atomic.AddInt32(&s.live, -1)
			}
		})
		return nil
	}, nil
}

func TestTask_PrepareStagingIsUnwoundBeforeRetry(t *testing.T) {
	state := newCascadeStub()
	// 第一轮尝试的第一条级联 DELETE 返回 1213 死锁；之后的尝试放行。
	var deletes int32
	state.fail = func(call stubCall) error {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(call.Query)), "DELETE FROM") {
			return nil
		}
		if atomic.AddInt32(&deletes, 1) == 1 {
			return stubMySQLErr{number: 1213, msg: "stub deadlock"}
		}
		return nil
	}
	d := openStubDB(t, state)

	stage := &stubAttemptStage{}
	if _, err := d.DeleteTaskCascadePrepared(stubTask, false, true, false, stage.prepare); err != nil {
		t.Fatalf("cascade must succeed after the retryable deadlock: %v", err)
	}
	if got := atomic.LoadInt32(&stage.staged); got < 2 {
		t.Fatalf("prepare must run again on the replayed attempt, staged=%d", got)
	}
	if got := atomic.LoadInt32(&stage.undone); got != 1 {
		t.Fatalf("exactly the failed attempt must be unwound exactly once: undone=%d staged=%d", got, atomic.LoadInt32(&stage.staged))
	}
	// 成功的那一轮把 stage 交给调用方收尾（由 server/manager.go 在外部副作用落地后
	// finalize/rollback），因此终态恰好只剩这 1 个 stage；失败的那轮必须已被清零。
	if got := atomic.LoadInt32(&stage.live); got != 1 {
		t.Fatalf("exactly the committed attempt's stage may stay live, live=%d", got)
	}
	if got := atomic.LoadInt32(&stage.maxLive); got > 1 {
		t.Fatalf("two attempts must never hold a stage at the same time, maxLive=%d", got)
	}

	// 语句顺序事实：第二轮尝试重新取得业务门禁之前，第一轮已经发过并败在那条 DELETE 上。
	calls := state.snapshotCalls()
	secondGate := -1
	gateSeen := 0
	firstDelete := -1
	deleteCount := 0
	for i, c := range calls {
		if c.Kind == "exec" && strings.Contains(c.Query, "INSERT IGNORE INTO locks") {
			gateSeen++
			if gateSeen == 2 {
				secondGate = i
			}
		}
		if c.Kind == "exec" && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(c.Query)), "DELETE FROM") {
			deleteCount++
			if firstDelete < 0 {
				firstDelete = i
			}
		}
	}
	if secondGate < 0 {
		t.Fatalf("the replayed attempt must re-acquire the writer gate, gates=%d", gateSeen)
	}
	if !(firstDelete >= 0 && firstDelete < secondGate) {
		t.Fatalf("the failed attempt must have issued its DELETE before the replay re-entered the gate (firstDelete=%d secondGate=%d)", firstDelete, secondGate)
	}
	if deleteCount < 2 {
		t.Fatalf("the replayed attempt must run the cascade DML again, deletes=%d", deleteCount)
	}
}

// TestTask_PrepareUndoFailureStopsRetryAndIsFatal 测试 R4-a：当外部暂存撤销失败时，
// 绝不能继续自动重试或静默吞掉，必须立即中止重试并作为致命错误返回。
func TestTask_PrepareUndoFailureStopsRetryAndIsFatal(t *testing.T) {
	state := newCascadeStub()
	state.fail = func(call stubCall) error {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(call.Query)), "DELETE FROM") {
			return stubMySQLErr{number: 1213, msg: "stub deadlock"}
		}
		return nil
	}
	d := openStubDB(t, state)

	stagedCalls := 0
	undoCalls := 0
	expectedUndoErr := errors.New("external restoreTrees failed")
	failingPrepare := func(TaskDeletePreparation) (TaskDeleteUndo, error) {
		stagedCalls++
		return func() error {
			undoCalls++
			return expectedUndoErr
		}, nil
	}

	_, err := d.DeleteTaskCascadePrepared(stubTask, false, true, false, failingPrepare)
	if err == nil {
		t.Fatal("cascade must fail when undo fails")
	}
	if !errors.Is(err, ErrTaskStageUndoFailed) {
		t.Fatalf("error must wrap ErrTaskStageUndoFailed, got: %v", err)
	}
	// 验证原因链完整保留（F3）：上层必须能够通过 errors.Is 识别出原始外部撤销错误
	if !errors.Is(err, expectedUndoErr) {
		t.Fatalf("error must wrap original undo error (%v), got: %v", expectedUndoErr, err)
	}
	if stagedCalls != 1 {
		t.Fatalf("must stop retrying immediately on undo failure, stagedCalls=%d", stagedCalls)
	}
	if undoCalls != 1 {
		t.Fatalf("undo must be called exactly once, undoCalls=%d", undoCalls)
	}
}

// TestTask_PreparePanicUnwindsStagedLock 测试 R4-b：prepare 成功后若发生 panic，
// 必须在展开前执行撤销释放外部写锁，杜绝写锁泄漏。
func TestTask_PreparePanicUnwindsStagedLock(t *testing.T) {
	state := newCascadeStub()
	state.fail = func(call stubCall) error {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(call.Query)), "DELETE FROM") {
			panic("unexpected internal panic during DML")
		}
		return nil
	}
	d := openStubDB(t, state)

	stage := &stubAttemptStage{}
	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("expected panic to propagate")
		}
		if got := atomic.LoadInt32(&stage.undone); got != 1 {
			t.Fatalf("panic must unwind staged external lock, undone=%d", got)
		}
		stage.mu.Lock()
		held := stage.held
		stage.mu.Unlock()
		if held {
			t.Fatal("external lock must be released after panic")
		}
	}()

	_, _ = d.DeleteTaskCascadePrepared(stubTask, false, true, false, stage.prepare)
}
