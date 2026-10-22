package server

// P08 TiDB 迁移收尾 —— Server.seedFindingRetester 的离线初始化回归测试。
//
// 这条路径过去整段还是 PostgreSQL 方言（pg_advisory_xact_lock / $n 占位符 /
// ON CONFLICT … DO NOTHING RETURNING / INSERT … RETURNING），而生产运行时只有
// 原生 TiDB 8.5：任何一条语句都会在驱动边界上直接 1064/1054 失败，进而让
// server 启动期的 retester seed 整体报错。
//
// 这里不连真实库（真实库由 Lead 独占复验），而是在 database/sql 的 fake driver
// 上装一道「TiDB 方言网关」：凡是 TiDB 8.5 不能原样执行的语句，网关就按 TiDB 的
// 行为返回错误（方言不识别 = 语句失败），并记录完整语句序列与事务边界。
// 被测代码走的是真实入口 s.seedFindingRetester() 与真实 *db.DB（sql.OpenDB 包装），
// 因此红/绿都落在生产路径上，而不是对某个字符串的事后断言。
//
// 网关检查的每条规则都对应 TiDB 侧的真实约束，不是自证式的模式匹配：
//  1. `$1` 位置占位符 —— go-sql-driver 不认，TiDB 直接 1064；TiDB 只有 `?`；
//  2. pg_advisory_xact_lock / ON CONFLICT / INSERT … RETURNING / ::cast / jsonb
//     运算符与函数 / DISTINCT ON / ILIKE —— TiDB 没有的 PG 专有语法与函数；
//  3. `?` 个数必须等于实参个数 —— TiDB 侧参数错位的唯一离线可判信号；
//  4. agents / settings 是 hash 唯一键表（uq_agents_key_hash /
//     uq_settings_key_hash）：INSERT 漏掉 <col>_identity/<col>_hash 等于把唯一键
//     退化成 NULL，重复行再也挡不住（db/config.go、db/settings.go 的硬约束）；
//  5. agents 的 wrapup_prompt / task_timeout_wrapup_prompt / trigger_run_mode /
//     trigger_merge_mode 是 NOT NULL TEXT，TiDB 不允许给 TEXT 列声明 DDL 默认值，
//     省略列名在 STRICT_TRANS_TABLES 下是 1364；
//  6. `key` / `schema` 是 MySQL/TiDB 保留字，未加反引号的裸标识符是 1064。

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

// —— fake driver 与 TiDB 方言网关 ——————————————————————————————

type seedGateCall struct {
	op    string // begin | commit | rollback | exec | query
	query string
	args  []driver.Value
	err   error // 网关按 TiDB 行为判定该语句是否可执行的结果
}

type seedGate struct {
	mu    sync.Mutex
	calls []seedGateCall
	// respond 按语句给出 fake 结果（模拟 TiDB 的数据面）；可为 nil。
	respond func(query string, args []driver.Value) (driver.Result, driver.Rows, error)
}

func (g *seedGate) record(op, query string, args []driver.Value, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, seedGateCall{op: op, query: query, args: args, err: err})
}

// sequence 返回语句类调用（忽略 begin/commit/rollback）的快照。
func (g *seedGate) sequence() []seedGateCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]seedGateCall, 0, len(g.calls))
	for _, c := range g.calls {
		if c.op == "exec" || c.op == "query" {
			out = append(out, c)
		}
	}
	return out
}

// ops 返回全部调用（含事务边界）的 op 序列。
func (g *seedGate) ops() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.calls))
	for _, c := range g.calls {
		out = append(out, c.op)
	}
	return out
}

// failures 返回网关判定为「TiDB 不可执行」的语句。
func (g *seedGate) failures() []seedGateCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]seedGateCall, 0)
	for _, c := range g.calls {
		if c.err != nil {
			out = append(out, c)
		}
	}
	return out
}

// indexOf 返回第一条命中 predicate 的语句下标，未命中返回 -1。
func (g *seedGate) indexOf(predicate func(string) bool) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, c := range g.calls {
		if (c.op == "exec" || c.op == "query") && predicate(c.query) {
			return i
		}
	}
	return -1
}

type seedGateDriver struct{}

func (seedGateDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("seedGateDriver: use OpenDB")
}

type seedGateConnector struct{ gate *seedGate }

func (c seedGateConnector) Connect(context.Context) (driver.Conn, error) {
	return &seedGateConn{gate: c.gate}, nil
}
func (c seedGateConnector) Driver() driver.Driver { return seedGateDriver{} }

type seedGateConn struct{ gate *seedGate }

func (c *seedGateConn) check(query string, args []driver.Value) error {
	return tidbDialectCheck(query, len(args))
}

func (c *seedGateConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := namedValues(args)
	err := c.check(query, values)
	c.gate.record("exec", query, values, err)
	if err != nil {
		return nil, err
	}
	if c.gate.respond != nil {
		res, _, rerr := c.gate.respond(query, values)
		return res, rerr
	}
	return driver.ResultNoRows, nil
}

func (c *seedGateConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := namedValues(args)
	err := c.check(query, values)
	c.gate.record("query", query, values, err)
	if err != nil {
		return nil, err
	}
	if c.gate.respond != nil {
		_, rows, rerr := c.gate.respond(query, values)
		if rows == nil && rerr == nil {
			// 数据面漏答一条 SELECT 时也必须返回空结果集：返回 nil driver.Rows 会让
			// database/sql 在 Rows.Next 里 panic，把"漏答"伪装成崩溃。
			rows = &seedGateRows{cols: []string{"x"}}
		}
		return rows, rerr
	}
	return &seedGateRows{cols: []string{"x"}}, nil
}

func (c *seedGateConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("seedGateConn: Prepare not implemented")
}
func (c *seedGateConn) Close() error { return nil }
func (c *seedGateConn) Begin() (driver.Tx, error) {
	c.gate.record("begin", "", nil, nil)
	return &seedGateTx{gate: c.gate}, nil
}

type seedGateTx struct{ gate *seedGate }

func (t *seedGateTx) Commit() error {
	t.gate.record("commit", "", nil, nil)
	return nil
}
func (t *seedGateTx) Rollback() error {
	t.gate.record("rollback", "", nil, nil)
	return nil
}

func namedValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, 0, len(args))
	for _, a := range args {
		out = append(out, a.Value)
	}
	return out
}

type seedGateRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *seedGateRows) Columns() []string { return r.cols }
func (r *seedGateRows) Close() error      { return nil }
func (r *seedGateRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

// newSeedGateDB 造出一条 fake 连接上的 *db.DB；MaxOpenConns(1) 保证 seed 全程
// 复用同一条连接，事务边界与语句顺序可精确断言。
func newSeedGateDB(t *testing.T, respond func(string, []driver.Value) (driver.Result, driver.Rows, error)) (*db.DB, *seedGate) {
	t.Helper()
	gate := &seedGate{respond: respond}
	sqlDB := sql.OpenDB(seedGateConnector{gate: gate})
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return &db.DB{DB: sqlDB}, gate
}

// —— 方言检查本身 ——————————————————————————————————————————————

var (
	pgPlaceholderRe = regexp.MustCompile(`\$\d+`)
	// 反引号标识符与单引号字面量：先摘掉，剩下的才是「裸标识符 + 语法关键字」。
	backtIdentRe = regexp.MustCompile("`[^`]*`")
	quotaLiteral = regexp.MustCompile("'(?:[^']|'')*'")
	bareKeyRe    = regexp.MustCompile(`(?i)\bkey\b`)
	bareSchemaRe = regexp.MustCompile(`(?i)\bschema\b`)
)

// pgOnlyTokens 是 TiDB 8.5 完全不存在、出现即 1064/1305 的 PG 专有写法。
var pgOnlyTokens = []string{
	"pg_advisory_xact_lock", "pg_advisory_lock", "pg_try_advisory",
	" on conflict", " returning ", "::", "jsonb", "distinct on", "ilike",
}

// tidbDialectCheck 判断一条 SQL 能否原样交给 TiDB 8.5 执行。
// 返回的 error 文本即「TiDB 为什么拒绝这条语句」。
func tidbDialectCheck(query string, argCount int) error {
	lower := strings.ToLower(query)
	if pgPlaceholderRe.MatchString(query) {
		return fmt.Errorf("TiDB 不支持 PG 位置占位符 $n: %s", oneLine(query))
	}
	for _, token := range pgOnlyTokens {
		if strings.Contains(lower, token) {
			return fmt.Errorf("TiDB 不支持的 PostgreSQL 写法 %q: %s", strings.TrimSpace(token), oneLine(query))
		}
	}
	// 摘掉反引号标识符与字符串字面量之后，剩下的文本用于占位符计数与保留字判定。
	bare := backtIdentRe.ReplaceAllString(query, " ID ")
	bare = quotaLiteral.ReplaceAllString(bare, " LITERAL ")
	if n := strings.Count(bare, "?"); n != argCount {
		return fmt.Errorf("占位符 %d 个与实参 %d 个不符: %s", n, argCount, oneLine(query))
	}
	if strings.Contains(strings.ToLower(query), "insert into") {
		// hash 唯一键表：身份列缺失等于唯一键退化成 NULL。
		if strings.Contains(lower, "insert into agents") {
			for _, col := range []string{"key_identity", "key_hash"} {
				if !strings.Contains(lower, col) {
					return fmt.Errorf("agents INSERT 缺 %s（uq_agents_key_hash 会退化成 NULL）: %s", col, oneLine(query))
				}
			}
			// NOT NULL TEXT 列没有 DDL 默认值，strict 模式省略即 1364。
			for _, col := range []string{"wrapup_prompt", "task_timeout_wrapup_prompt", "trigger_run_mode", "trigger_merge_mode"} {
				if !strings.Contains(lower, col) {
					return fmt.Errorf("agents INSERT 缺 NOT NULL TEXT 列 %s（strict 模式 1364）: %s", col, oneLine(query))
				}
			}
		}
		if strings.Contains(lower, "insert into settings") {
			for _, col := range []string{"key_identity", "key_hash"} {
				if !strings.Contains(lower, col) {
					return fmt.Errorf("settings INSERT 缺 %s（uq_settings_key_hash 会退化成 NULL）: %s", col, oneLine(query))
				}
			}
		}
	}
	// 保留字必须反引号：ON DUPLICATE KEY UPDATE 里的 KEY 是语法关键字，先摘掉。
	noDupKey := regexp.MustCompile(`(?i)duplicate key`).ReplaceAllString(bare, " ")
	if loc := bareKeyRe.FindStringIndex(noDupKey); loc != nil {
		return fmt.Errorf("保留字 key 未加反引号（TiDB 1064）: %s", oneLine(query))
	}
	if loc := bareSchemaRe.FindStringIndex(bare); loc != nil {
		return fmt.Errorf("保留字 schema 未加反引号（TiDB 1064）: %s", oneLine(query))
	}
	return nil
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// —— 被测路径 ——————————————————————————————————————————————

// newSeedOnlyServer 造出只用来跑启动 seed 的 Server（不启会话、不启 worker）。
func newSeedOnlyServer(t *testing.T, pg *db.DB) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Server{
		ctx:        ctx,
		m:          &Manager{pg: pg, dir: t.TempDir(), tasks: map[string]*Task{}},
		chatBusy:   map[string]bool{},
		chatCancel: map[string]context.CancelCauseFunc{},
	}
}

// seedRespond 是 fake 的 TiDB **数据面**：按语句形状给出与真实表一致的结果，
// 并统计写入次数。它必须与生产的新语句集合一一对应（Business Gate、maintenance
// 锚点、canonical hash 定位 + 物理主键锁 + 身份三元组 + 写前仲裁）。
//
// 身份/哈希由 schema.AgentKeyIdentity / schema.SettingsKeyIdentity 独立算出（不是把
// 生产写入的值原样回吐），因此"自然键 ↔ 持久 identity 自洽""同 hash 异 identity"
// 这类判定在这里同样是外部约束。
type seedRespond struct {
	flagSet      bool  // settings 里已有 flag=true 的行
	agentExists  bool  // agents 里已有 canonical 的 retester 行
	anchorAbsent bool  // maintenance 专属锚点行缺失（触发自愈 INSERT IGNORE）

	agentID    int64
	agentRowID int64 // agents 按 key_hash 回读的 id；0 表示用 agentID
	promptID   int64

	agentInserts int
	promptInsert int
	settingsUp   int
	toolsInsert  int
	agentUpdates int
	settingsUpd  int

	insertedAgent             bool   // 本轮 agents INSERT 已落库（或 1062 的赢家行已存在）
	agentIdentityDrift      bool   // agents 里同 hash 的行其实是**别人**（同 hash 异 identity）
	agentInsert1062Index    string // 非空 → 下一次 agents INSERT 返回该索引上的 1062
	settingsInsert1062Index string // 非空 → 下一次 settings INSERT 返回该索引上的 1062
}

// canonical 返回某张 hash 唯一键表的 (自然键, identity, hash) 三元组。
func seedCanonicalAgent() (string, []byte, []byte) {
	identity := schema.AgentKeyIdentity(db.FindingRetestAgentKey)
	sum := sha256.Sum256(identity)
	return db.FindingRetestAgentKey, identity, sum[:]
}

func seedCanonicalFlag() (string, []byte, []byte) {
	identity := schema.SettingsKeyIdentity(db.FindingRetesterSeedFlag)
	sum := sha256.Sum256(identity)
	return db.FindingRetesterSeedFlag, identity, sum[:]
}

// hasAgent 报告 fake 里是否已有一行 canonical 的 retester agent。
func (r *seedRespond) hasAgent() bool { return r.agentExists || r.insertedAgent }

const (
	seedAgentRowID   = int64(7001)
	seedFlagRowID    = int64(7002)
	seedBusinessGate = "business_relations"
)

func (r *seedRespond) call(query string, args []driver.Value) (driver.Result, driver.Rows, error) {
	q := strings.ToLower(query)
	one := func(cols []string, vals ...driver.Value) (driver.Result, driver.Rows, error) {
		return nil, &seedGateRows{cols: cols, rows: [][]driver.Value{vals}}, nil
	}
	none := func(cols ...string) (driver.Result, driver.Rows, error) {
		return nil, &seedGateRows{cols: cols}, nil
	}
	switch {
	// —— Business Gate（统一 TxRunner 的第一道 DB 锁）——
	case strings.Contains(q, "from `locks`") || strings.Contains(q, "from locks"):
		return one([]string{"name"}, seedBusinessGate)

	// —— maintenance 专属锚点 ——
	case strings.Contains(q, "from maintenance"):
		if r.anchorAbsent {
			return none("row_id")
		}
		return one([]string{"row_id"}, int64(1))

	// —— settings：身份三元组 / 仲裁 / 定位 / 主键锁 / 取值 ——
	case strings.Contains(q, "from `settings`") && strings.Contains(q, "`key_identity`, `key_hash`"):
		if !r.flagSet {
			return nil, nil, sql.ErrNoRows
		}
		_, identity, hash := seedCanonicalFlag()
		return one([]string{"key", "key_identity", "key_hash"}, db.FindingRetesterSeedFlag, identity, hash)
	case strings.Contains(q, "from `settings`") && strings.Contains(q, "`key_identity` from `settings`"):
		if !r.flagSet {
			return nil, nil, sql.ErrNoRows
		}
		_, identity, _ := seedCanonicalFlag()
		return one([]string{"row_id", "key_identity"}, seedFlagRowID, identity)
	case strings.Contains(q, "from `settings`") && strings.Contains(q, "key_hash"):
		if !r.flagSet {
			return none("row_id")
		}
		return one([]string{"row_id"}, seedFlagRowID)
	case strings.Contains(q, "from `settings` where `row_id` = ?"):
		if !r.flagSet {
			return nil, nil, sql.ErrNoRows
		}
		return one([]string{"row_id"}, seedFlagRowID)
	case strings.Contains(q, "`value` from settings"):
		if !r.flagSet {
			return none("value")
		}
		return one([]string{"value"}, "true")

	// —— agents：身份三元组 / 仲裁 / 定位 / 主键锁 / 自增 id 回读 ——
	case strings.Contains(q, "from `agents`") && strings.Contains(q, "`key_identity`, `key_hash`"):
		if !r.hasAgent() {
			return nil, nil, sql.ErrNoRows
		}
		key, identity, hash := seedCanonicalAgent()
		return one([]string{"key", "key_identity", "key_hash"}, key, identity, hash)
	case strings.Contains(q, "from `agents`") && strings.Contains(q, "`key_identity` from `agents`"):
		if !r.hasAgent() {
			return nil, nil, sql.ErrNoRows
		}
		// schema.VerifyHashArbitration 的仲裁读：identity 不同即身份碰撞。
		identity := schema.AgentKeyIdentity("someone-else")
		if !r.agentIdentityDrift {
			_, identity, _ = seedCanonicalAgent()
		}
		return one([]string{"id", "key_identity"}, seedAgentRowID, identity)
	case strings.Contains(q, "from `agents` where `id` = ?"):
		if !r.hasAgent() {
			return nil, nil, sql.ErrNoRows
		}
		return one([]string{"id"}, seedAgentRowID)
	case strings.Contains(q, "from `agents`") && strings.Contains(q, "key_hash"):
		// 写前仲裁的定位（带反引号的规范形状）。
		if !r.hasAgent() {
			return none("id")
		}
		return one([]string{"id"}, seedAgentRowID)
	case strings.Contains(q, "from agents where key_hash"):
		// 自增 id 落库后按 hash 的回读：必须给出"这一行现在是谁"。
		id := r.agentRowID
		if id == 0 {
			id = r.agentID
		}
		return one([]string{"id"}, id)

	// —— 写入 ——
	case strings.Contains(q, "insert into agents"):
		r.agentInserts++
		if r.agentInsert1062Index != "" {
			idx := r.agentInsert1062Index
			r.agentInsert1062Index = ""
			// 撞键意味着**赢家的那一行**已经在库里。
			r.insertedAgent = true
			return nil, nil, &mysql.MySQLError{Number: 1062,
				Message: "Duplicate entry 'x' for key '" + idx + "'"}
		}
		r.insertedAgent = true
		return fakeResult{rowsAffected: 1, lastInsertID: r.agentID}, nil, nil
	case strings.Contains(q, "insert into agent_prompts"):
		r.promptInsert++
		return fakeResult{rowsAffected: 1, lastInsertID: r.promptID}, nil, nil
	case strings.Contains(q, "update agents"):
		r.agentUpdates++
		return driver.RowsAffected(1), nil, nil
	case strings.Contains(q, "update settings"):
		r.settingsUpd++
		return driver.RowsAffected(1), nil, nil
	case strings.Contains(q, "insert into settings"):
		r.settingsUp++
		if r.settingsInsert1062Index != "" {
			idx := r.settingsInsert1062Index
			r.settingsInsert1062Index = ""
			return nil, nil, &mysql.MySQLError{Number: 1062,
				Message: "Duplicate entry 'y' for key '" + idx + "'"}
		}
		return driver.RowsAffected(1), nil, nil
	case strings.Contains(q, "insert into tools"):
		r.toolsInsert++
		return driver.RowsAffected(1), nil, nil
	case strings.HasPrefix(q, "select"):
		// 其余只读语句（tools 定位等）：空结果集，绝不允许返回 nil Rows。
		return none("x")
	}
	return driver.ResultNoRows, nil, nil
}

type fakeResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r fakeResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// errFakeDuplicateKey 与 TiDB 真实回包同型（*mysql.MySQLError 1062），
// tidb.IsDuplicateKey 认它；用普通 error 包装等于把「撞键」这条真实路径测歪。
var errFakeDuplicateKey error = &mysql.MySQLError{
	Number:  1062,
	Message: "Duplicate entry 'x' for key 'uq_agents_key_hash'",
}

// TestSeedFindingRetesterEmitsTiDBCompatibleSQL 是这条启动路径的总闸：
// seedFindingRetester 的每条语句都必须能被 TiDB 8.5 原样执行，
// 且整段 seed 落在同一个事务里、以一次 commit 收尾。
func TestSeedFindingRetesterEmitsTiDBCompatibleSQL(t *testing.T) {
	state := &seedRespond{agentID: 4242, promptID: 777}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err != nil {
		t.Fatalf("seedFindingRetester: %v", err)
	}
	if bad := gate.failures(); len(bad) > 0 {
		var b strings.Builder
		for _, c := range bad {
			fmt.Fprintf(&b, "\n  %v", c.err)
		}
		t.Fatalf("seedFindingRetester 仍在向 TiDB 下发不可执行的语句:%s", b.String())
	}

	ops := gate.ops()
	begin, commits, rollbacks := 0, 0, 0
	for _, op := range ops {
		switch op {
		case "begin":
			begin++
		case "commit":
			commits++
		case "rollback":
			rollbacks++
		}
	}
	// 工具 seed 是逐条 autocommit（首插入语义落在 1062 上），业务 seed 必须是事务。
	if begin != 1 || commits != 1 {
		t.Fatalf("seed 事务边界 = begin %d / commit %d / rollback %d, want 1/1/0 (ops=%v)", begin, commits, rollbacks, ops)
	}

	// prompt 与 flag 必须同事务：任何一条落到 commit 之外都是原子性破口。
	commitAt := -1
	for i, op := range ops {
		if op == "commit" {
			commitAt = i
			break
		}
	}
	if commitAt < 0 {
		t.Fatal("没有 commit")
	}
	for _, want := range []string{"insert into agents", "insert into agent_prompts", "update agents", "insert into settings"} {
		at := -1
		gate.mu.Lock()
		for i, c := range gate.calls {
			if (c.op == "exec" || c.op == "query") && strings.Contains(strings.ToLower(c.query), want) {
				at = i
				break
			}
		}
		gate.mu.Unlock()
		if at < 0 {
			t.Fatalf("缺少语句 %q；实际序列=%v", want, ops)
		}
		if at > commitAt {
			t.Fatalf("%q 落在 commit 之后（prompt/flag 原子性破口）；实际序列=%v", want, ops)
		}
	}
	if state.agentInserts != 1 || state.promptInsert != 1 || state.settingsUp != 1 {
		t.Fatalf("seed 写入次数 agents=%d prompts=%d settings=%d, want 1/1/1", state.agentInserts, state.promptInsert, state.settingsUp)
	}
	if state.toolsInsert != 2 {
		t.Fatalf("复测工具 seed 次数 = %d, want 2", state.toolsInsert)
	}
}

// TestSeedFindingRetesterOrderingLockFirstFlagLast 锁定迁移不变量的顺序：
// 并发初始化互斥锁必须在任何读写之前，flag 必须最后落（否则中途崩溃会留下
// 「flag=true 但 prompt 缺失」的半套状态）。
func TestSeedFindingRetesterOrderingLockFirstFlagLast(t *testing.T) {
	state := &seedRespond{agentID: 4242, promptID: 777}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err != nil {
		t.Fatalf("seedFindingRetester: %v", err)
	}
	// 第一个 begin 之后必须立刻抢锁。
	ops := gate.ops()
	beginAt := -1
	for i, op := range ops {
		if op == "begin" {
			beginAt = i
			break
		}
	}
	if beginAt < 0 {
		t.Fatal("没有 begin")
	}
	lockAt := gate.indexOf(func(q string) bool {
		return strings.Contains(strings.ToLower(q), "from maintenance") && strings.Contains(strings.ToUpper(q), "FOR UPDATE")
	})
	if lockAt < 0 {
		t.Fatal("缺少事务级互斥锁（TiDB 无 advisory lock，等价原语是 maintenance 单例行 FOR UPDATE）")
	}
	if lockAt < beginAt {
		t.Fatalf("互斥锁必须在事务内获取；实际序列=%v", ops)
	}
	// 全序第 0 层：统一 TxRunner 的 Business Gate 必须**先于** maintenance 锚点。
	gateAt := gate.indexOf(func(q string) bool {
		lower := strings.ToLower(q)
		return strings.Contains(lower, "from locks") && strings.Contains(strings.ToUpper(q), "FOR UPDATE")
	})
	if gateAt < 0 {
		t.Fatal("缺少 Business Gate 行锁（locks.business_relations FOR UPDATE）：seed 未接入统一 TxRunner")
	}
	if gateAt > lockAt {
		t.Fatalf("Business Gate 必须先于 maintenance 锚点，否则与其它写同一批实体的事务形成反向锁序；gate=%d anchor=%d", gateAt, lockAt)
	}
	// flag 判定读：定位 + 取值都必须是 settings 上的**当前读**（FOR UPDATE）。
	// TiDB 默认悲观 RR，普通 SELECT 读的是事务开始前的旧快照，会把"赢家刚提交的
	// flag=true"看成"从未 seed"，从而把用户删掉的 retester 连模板一起复活。
	flagSelect := gate.indexOf(func(q string) bool {
		lower := strings.ToLower(strings.TrimSpace(q))
		return strings.HasPrefix(lower, "select") && strings.Contains(lower, "settings") &&
			strings.Contains(strings.ToUpper(q), "FOR UPDATE")
	})

	flagWrite := gate.indexOf(func(q string) bool {
		return strings.Contains(strings.ToLower(q), "insert into settings")
	})
	agentInsert := gate.indexOf(func(q string) bool { return strings.Contains(strings.ToLower(q), "insert into agents") })
	promptInsert := gate.indexOf(func(q string) bool { return strings.Contains(strings.ToLower(q), "insert into agent_prompts") })
	for _, step := range []struct {
		name string
		at   int
	}{
		{"maintenance lock", lockAt},
		{"flag read", flagSelect},
		{"agent insert", agentInsert},
		{"prompt insert", promptInsert},
		{"flag write", flagWrite},
	} {
		if step.at < 0 {
			t.Fatalf("缺少步骤 %s；实际序列=%v", step.name, ops)
		}
	}
	if !(lockAt < flagSelect && flagSelect < agentInsert && agentInsert < promptInsert && promptInsert < flagWrite) {
		t.Fatalf("步骤顺序错误 lock=%d flagRead=%d agent=%d prompt=%d flagWrite=%d；序列=%v",
			lockAt, flagSelect, agentInsert, promptInsert, flagWrite, ops)
	}
}

// TestSeedFindingRetesterFlaggedSkipsAllWrites：flag 已置位（= 早已 seed 过）时，
// 一个字的 agent/agent_prompts/settings 都不许碰 —— 这是「编辑后重启不覆盖、
// 用户删掉 retester 不复活」的根保证。
func TestSeedFindingRetesterFlaggedSkipsAllWrites(t *testing.T) {
	state := &seedRespond{flagSet: true, agentID: 4242, promptID: 777}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err != nil {
		t.Fatalf("seedFindingRetester: %v", err)
	}
	if bad := gate.failures(); len(bad) > 0 {
		t.Fatalf("seedFindingRetester 下发不可执行语句: %v", bad[0].err)
	}
	if state.agentInserts != 0 || state.promptInsert != 0 || state.settingsUp != 0 {
		t.Fatalf("flag=true 仍发生写入 agents=%d prompts=%d settings=%d, want 0/0/0",
			state.agentInserts, state.promptInsert, state.settingsUp)
	}
	if at := gate.indexOf(func(q string) bool { return strings.Contains(strings.ToLower(q), "update agents") }); at >= 0 {
		t.Fatal("flag=true 仍改写 agents.current_prompt_id")
	}
}

// TestSeedFindingRetesterExistingAgentNotRewritten：agent key 已存在（1062）时，
// 既不覆写 agent 行，也不追加 prompt 版本，只补落 flag。
func TestSeedFindingRetesterExistingAgentNotRewritten(t *testing.T) {
	state := &seedRespond{agentExists: true, agentID: 4242, promptID: 777}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err != nil {
		t.Fatalf("seedFindingRetester（agent 已存在）: %v", err)
	}
	if state.promptInsert != 0 {
		t.Fatalf("已有 agent 仍被追加 prompt 版本（prompt 漂移）: %d", state.promptInsert)
	}
	if at := gate.indexOf(func(q string) bool { return strings.Contains(strings.ToLower(q), "update agents") }); at >= 0 {
		t.Fatal("已有 agent 仍被改写 current_prompt_id")
	}
	if state.settingsUp != 1 {
		t.Fatalf("flag 未落库 settings=%d, want 1（否则每次重启都重跑迁移）", state.settingsUp)
	}
}

// TestSeedFindingRetesterRollsBackWhenFlagWriteFails：flag 写入失败必须整体回滚，
// 绝不允许「prompt 已落、flag 未落」或反之的半套状态。
func TestSeedFindingRetesterRollsBackWhenFlagWriteFails(t *testing.T) {
	state := &seedRespond{agentID: 4242, promptID: 777}
	pg, gate := newSeedGateDB(t, func(q string, args []driver.Value) (driver.Result, driver.Rows, error) {
		if strings.Contains(strings.ToLower(q), "insert into settings") {
			return nil, nil, &mysql.MySQLError{Number: 1062, Message: "Duplicate entry for key 'uq_settings_key_hash'"}
		}
		return state.call(q, args)
	})
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err == nil {
		t.Fatal("flag 写入失败被吞掉，prompt 与 flag 不再同生共死")
	}
	ops := gate.ops()
	commits, rollbacks := 0, 0
	for _, op := range ops {
		switch op {
		case "commit":
			commits++
		case "rollback":
			rollbacks++
		}
	}
	if commits != 0 {
		t.Fatalf("flag 写入失败仍有 %d 次 commit；序列=%v", commits, ops)
	}
	if rollbacks == 0 {
		t.Fatalf("失败路径未回滚；序列=%v", ops)
	}
}

// TestSeedFindingRetesterStrictIDReadback：agent 自增 id 与 key_hash 回读必须严格
// 一致，prompt LastInsertId 必须可信 —— 拿 0 或别人的 id 继续写，等于把
// current_prompt_id 指到别人的模板上。
func TestSeedFindingRetesterStrictIDReadback(t *testing.T) {
	cases := map[string]func(*seedRespond){
		"回读 id 与自增 id 不符": func(r *seedRespond) { r.agentRowID = 9999 },
		"自增 id 非法":        func(r *seedRespond) { r.agentID = 0 },
		"prompt 自增 id 非法": func(r *seedRespond) { r.promptID = 0 },
	}
	for name, respond := range cases {
		t.Run(name, func(t *testing.T) {
			state := &seedRespond{agentID: 4242, promptID: 777}
			respond(state)
			pg, gate := newSeedGateDB(t, state.call)
			s := newSeedOnlyServer(t, pg)

			if err := s.seedFindingRetester(); err == nil {
				t.Fatal("ID 异常被静默接受")
			}
			for _, op := range gate.ops() {
				if op == "commit" {
					t.Fatalf("ID 异常仍提交；序列=%v", gate.ops())
				}
			}
		})
	}
}

// TestSeedFindingRetesterFailsClosedOnForeignIdentity：hash 相同但持久 identity 是
// **别人**（自然键不同）时，seed 必须 fail closed 并整体回滚：既不能把自己的 identity
// 覆盖上去（那等于抢走别人的行），也不能把冲突当成"已存在，跳过"。
func TestSeedFindingRetesterFailsClosedOnForeignIdentity(t *testing.T) {
	state := &seedRespond{agentID: 4242, promptID: 777, agentExists: true, agentIdentityDrift: true}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err == nil {
		t.Fatal("同 hash 异 identity 被当成 seed 成功")
	} else if !errors.Is(err, db.ErrRetesterSeedHashCollision) {
		t.Fatalf("错误必须是身份碰撞哨兵，实际: %v", err)
	}
	if state.agentInserts != 0 || state.promptInsert != 0 || state.settingsUp != 0 || state.settingsUpd != 0 {
		t.Fatalf("身份碰撞仍发生写入 agents=%d prompts=%d settings=%d updates=%d",
			state.agentInserts, state.promptInsert, state.settingsUp, state.settingsUpd)
	}
	for _, op := range gate.ops() {
		if op == "commit" {
			t.Fatalf("身份碰撞仍提交；序列=%v", gate.ops())
		}
	}
}

// TestSeedFindingRetesterFailsClosedOnUnexpectedIndex：1062 落在预期之外的唯一索引上
// 时不得被解释成"同 key 已存在"，必须 fail closed + 回滚（否则任何一条无关唯一键都能
// 让 seed 静默跳过，或让半套状态被提交）。
func TestSeedFindingRetesterFailsClosedOnUnexpectedIndex(t *testing.T) {
	state := &seedRespond{agentID: 4242, promptID: 777, agentInsert1062Index: "uq_agents_something_else"}
	pg, gate := newSeedGateDB(t, state.call)
	s := newSeedOnlyServer(t, pg)

	if err := s.seedFindingRetester(); err == nil {
		t.Fatal("意外索引上的 1062 被吞掉")
	} else if !errors.Is(err, db.ErrRetesterSeedUnexpectedConflict) {
		t.Fatalf("错误必须是意外索引哨兵，实际: %v", err)
	}
	if state.promptInsert != 0 || state.settingsUp != 0 {
		t.Fatalf("意外索引冲突后仍继续写入 prompts=%d settings=%d", state.promptInsert, state.settingsUp)
	}
	for _, op := range gate.ops() {
		if op == "commit" {
			t.Fatalf("意外索引冲突仍提交；序列=%v", gate.ops())
		}
	}
}
