package db

// =====================================================================
// SeedFindingRetester 的回归测试（P08 TiDB 迁移收尾）
//
// 分两层，严格区分「已验证」与「未验证」：
//
//  1. 离线层（永远执行，不连任何库）：在 database/sql 的 fake driver 上驱动真实的
//     SeedFindingRetester。fake 不是"万能成功"的桩，而是一个**带提交语义的最小
//     数据面**（committed/overlay 两态，rollback 丢弃 overlay），因此能真的回答
//     "失败路径到底留下了什么"：
//      a. 写者协议：Business Gate → maintenance 锚点 → settings flag（当前读）→
//         agents（当前读定位 → 物理 PK 锁 → 身份仲裁）→ prompt → current_prompt_id
//         → settings flag 落库 → 单次 commit；任一环节失败必须整体回滚；
//      b. 写前身份仲裁：同 hash 异 identity、自然键不自洽、意外索引 1062、
//         指定索引 1062 的整体回滚重放，四类调度全部在这里有确定性用例；
//      c. 写入内容：按 INSERT 的**列清单 ↔ VALUES 实参**逐位对齐，校验 hash 唯一键
//         的身份列确实是 schema 包算出的 canonical 值、prompt 正文逐字透传。
//     fake 按 TiDB 的回包形态给出结果（*mysql.MySQLError 1062 带 `for key '…'`、
//     LastInsertId、RowsAffected），生产代码里的索引归因走的是真实判定。
//
//  2. 真机层（ARTEX_RETESTER_SEED_TEST_DSN 门控，未配置时显式 t.Skip = 未验证）：
//     **只在专用的空库上运行**（默认拒绝业务库，见 retesterSeedIsolatedSchema）：
//     连接后先证明"当前 schema 对象数恰好为 0"，再用严格 CREATE TABLE（无 IF NOT
//     EXISTS）逐张建表并登记所有权，结束时只删除本轮登记的表，终态 BASE TABLE 必须
//     回到 0。真机层验收 DDL 可执行、RR/RC 两级下的"删除不复活"确定性调度、
//     4 worker 冷锚点重叠、以及持久碰撞注入下的零污染。
//
// 本文件不再以任何固定自然键去删除"可能是别人的"数据：真机用例的 fixture 库是
// 本轮自己建起来的 schema，行级清理只针对本轮物理主键（retesterSeedRowGuard）。
// =====================================================================

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
)

// —— 离线 fake：带提交语义的最小数据面 ————————————————————————

// seedFakeRow 是 settings / agents 两张 hash 唯一键表里的最小行形状。
type seedFakeRow struct {
	pk       int64
	key      string
	identity []byte
	hash     []byte
	value    string
}

type seedFakePrompt struct {
	pk, agentID int64
	version     int
	text        string
}

// seedFakeState 是 fake 的数据面 + 故障注入 + 观测计数。
type seedFakeState struct {
	mu sync.Mutex
	// committed = 已提交；overlay = 本事务未提交的写入（commit 合并，rollback 丢弃）。
	committed map[string]*seedFakeRow
	overlay   map[string]*seedFakeRow
	// 行集按 "表名:\x00hash十六进制" 索引，正是 hash 唯一键的语义。
	promptsCommitted []seedFakePrompt
	promptsOverlay   []seedFakePrompt
	anchor           bool
	anchorOverlay    bool

	nextAgent   int64
	nextPrompt  int64
	nextSetting int64

	// —— 故障注入（一次性的，命中即失效）——
	agentInsert1062Index    string // 非空 → 下一次 agents INSERT 返回该索引上的 1062
	agentReadbackID         int64  // >0 → agents 按 hash 回读时返回这个 id（模拟 id 漂移）
	hideAgentRounds         int    // >0 → 本轮"定位看不见赢家行、INSERT 才发现赢家行"（模拟定位快照过期的并发赢家）
	promptIDZero            bool   // true → agent_prompts INSERT 的 LastInsertId 为 0
	settingsInsert1062Index string
	failOnce                map[string]error

	// —— 观测 ——
	agentInserts    int
	promptInserts   int
	settingsInserts int
	agentUpdates    int
	settingsUpdates int
	anchorSeeds     int
}

func (st *seedFakeState) row(table string, hash []byte) *seedFakeRow {
	key := table + "\x00" + hex.EncodeToString(hash)
	if r, ok := st.overlay[key]; ok {
		return r
	}
	if r, ok := st.committed[key]; ok {
		return r
	}
	return nil
}

func (st *seedFakeState) rowByPK(table string, pk int64) *seedFakeRow {
	prefix := table + "\x00"
	for k, r := range st.overlay {
		if strings.HasPrefix(k, prefix) && r.pk == pk {
			return r
		}
	}
	for k, r := range st.committed {
		if strings.HasPrefix(k, prefix) && r.pk == pk {
			return r
		}
	}
	return nil
}

func (st *seedFakeState) allRows(table string) []*seedFakeRow {
	out := make([]*seedFakeRow, 0)
	for k, r := range st.committed {
		if strings.HasPrefix(k, table+"\x00") {
			out = append(out, r)
		}
	}
	for k, r := range st.overlay {
		if strings.HasPrefix(k, table+"\x00") {
			out = append(out, r)
		}
	}
	return out
}

type seedFake struct {
	mu    sync.Mutex
	calls []seedCall
	state *seedFakeState
}

type seedCall struct {
	op    string // begin | commit | rollback | exec | query
	query string
	args  []driver.Value
}

func (f *seedFake) record(op, query string, args []driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, seedCall{op: op, query: query, args: args})
}

func (f *seedFake) snapshot() []seedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seedCall(nil), f.calls...)
}

// statements 返回语句类调用（忽略事务边界）。
func (f *seedFake) statements() []seedCall {
	out := make([]seedCall, 0)
	for _, c := range f.snapshot() {
		if c.op == "exec" || c.op == "query" {
			out = append(out, c)
		}
	}
	return out
}

// count 返回命中子串的语句数。
func (f *seedFake) count(sub string) int {
	n := 0
	for _, c := range f.statements() {
		if strings.Contains(strings.ToLower(c.query), strings.ToLower(sub)) {
			n++
		}
	}
	return n
}

// find 返回第一条命中子串的语句。
func (f *seedFake) find(sub string) (seedCall, bool) {
	for _, c := range f.statements() {
		if strings.Contains(strings.ToLower(c.query), strings.ToLower(sub)) {
			return c, true
		}
	}
	return seedCall{}, false
}

// txOps 返回事务边界序列。
func (f *seedFake) txOps() []string {
	out := make([]string, 0)
	for _, c := range f.snapshot() {
		if c.op == "begin" || c.op == "commit" || c.op == "rollback" {
			out = append(out, c.op)
		}
	}
	return out
}

func (f *seedFake) commits() int {
	n := 0
	for _, op := range f.txOps() {
		if op == "commit" {
			n++
		}
	}
	return n
}

func (f *seedFake) rollbacks() int {
	n := 0
	for _, op := range f.txOps() {
		if op == "rollback" {
			n++
		}
	}
	return n
}

// dup1062 按 TiDB 的真实错误形态造 1062（消息里带 `for key '<index>'`）。
func dup1062(index string) error {
	return &mysql.MySQLError{Number: 1062,
		Message: fmt.Sprintf("Duplicate entry 'x' for key '%s'", index)}
}

// handle 按语句给出 TiDB 形态的结果。
func (f *seedFake) handle(query string, args []driver.Value) (driver.Result, driver.Rows, error) {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	st := f.state
	lower := strings.ToLower(regexp.MustCompile(`\s+`).ReplaceAllString(query, " "))

	for sub, err := range st.failOnce {
		if strings.Contains(lower, strings.ToLower(sub)) {
			delete(st.failOnce, sub)
			return nil, nil, err
		}
	}
	arg := func(i int) driver.Value {
		if i < len(args) {
			return args[i]
		}
		return nil
	}
	asBytes := func(v driver.Value) []byte {
		switch t := v.(type) {
		case []byte:
			return t
		case string:
			return []byte(t)
		case int64:
			return []byte(fmt.Sprint(t))
		}
		return nil
	}
	asInt := func(v driver.Value) int64 {
		switch t := v.(type) {
		case int64:
			return t
		}
		return 0
	}
	rows1 := func(cols []string, vals ...driver.Value) driver.Rows {
		r := &seedFakeRows{cols: cols}
		if len(vals) > 0 {
			r.rows = [][]driver.Value{vals}
		}
		return r
	}

	switch {
	// —— TxRunner 的 Business Gate（DB 侧）——
	case strings.Contains(lower, "insert ignore into locks"):
		return driver.RowsAffected(1), nil, nil
	case strings.Contains(lower, "from locks") && strings.Contains(lower, "for update"):
		return nil, rows1([]string{"name"}, "business_relations"), nil

	// —— maintenance 专属锚点 ——
	case strings.Contains(lower, "from maintenance") && strings.Contains(lower, "for update"):
		if st.anchor || st.anchorOverlay {
			return nil, rows1([]string{"row_id"}, int64(1)), nil
		}
		return nil, rows1([]string{"row_id"}), nil
	case strings.Contains(lower, "insert ignore into maintenance"):
		st.anchorSeeds++
		st.anchorOverlay = true
		return driver.RowsAffected(1), nil, nil

	// —— settings：身份三元组 / 仲裁 / 定位 / 取值 ——
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `settings`") &&
		strings.Contains(lower, "`key_identity`, `key_hash`"):
		r := st.rowByPK("settings", asInt(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"key", "key_identity", "key_hash"}, r.key, r.identity, r.hash), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `settings`") &&
		strings.Contains(lower, "`key_identity` from `settings`"):
		r := st.row("settings", asBytes(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"row_id", "key_identity"}, r.pk, r.identity), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `settings`") &&
		strings.Contains(lower, "key_hash"):
		r := st.row("settings", asBytes(arg(0)))
		if r == nil {
			return nil, rows1([]string{"row_id"}), nil
		}
		return nil, rows1([]string{"row_id"}, r.pk), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `settings` where `row_id` = ?"):
		r := st.rowByPK("settings", asInt(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"row_id"}, r.pk), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "`value` from settings"):
		r := st.rowByPK("settings", asInt(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"value"}, r.value), nil

	// —— agents：身份三元组 / 仲裁 / 定位 / PK 锁 ——
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `agents`") &&
		strings.Contains(lower, "`key_identity`, `key_hash`"):
		if st.hideAgentRounds > 0 {
			return nil, nil, sql.ErrNoRows
		}
		r := st.rowByPK("agents", asInt(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"key", "key_identity", "key_hash"}, r.key, r.identity, r.hash), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `agents`") &&
		strings.Contains(lower, "`key_identity` from `agents`"):
		if st.hideAgentRounds > 0 {
			return nil, nil, sql.ErrNoRows
		}
		r := st.row("agents", asBytes(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"id", "key_identity"}, r.pk, r.identity), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "from `agents`") &&
		strings.Contains(lower, "where `id` = ?"):
		r := st.rowByPK("agents", asInt(arg(0)))
		if r == nil {
			return nil, nil, sql.ErrNoRows
		}
		return nil, rows1([]string{"id"}, r.pk), nil
	case strings.HasPrefix(lower, "select") && strings.Contains(lower, "agents") &&
		strings.Contains(lower, "key_hash"):
		if st.hideAgentRounds > 0 {
			// 本事务的定位快照里还没有赢家行（并发赢家刚提交）。
			return nil, rows1([]string{"id"}), nil
		}
		if st.agentReadbackID > 0 {
			return nil, rows1([]string{"id"}, st.agentReadbackID), nil
		}
		r := st.row("agents", asBytes(arg(0)))
		if r == nil {
			return nil, rows1([]string{"id"}), nil
		}
		return nil, rows1([]string{"id"}, r.pk), nil

	// —— 写入 ——
	case strings.Contains(lower, "insert into agents"):
		st.agentInserts++
		if st.hideAgentRounds > 0 {
			st.hideAgentRounds--
			return nil, nil, dup1062("uq_agents_key_hash")
		}
		if st.agentInsert1062Index != "" {
			idx := st.agentInsert1062Index
			st.agentInsert1062Index = ""
			return nil, nil, dup1062(idx)
		}
		key, identity, hash := asStr(arg(0)), asBytes(arg(1)), asBytes(arg(2))
		if r := st.row("agents", hash); r != nil {
			return nil, nil, dup1062("uq_agents_key_hash")
		}
		st.nextAgent++
		r := &seedFakeRow{pk: st.nextAgent + 100, key: key, identity: identity, hash: hash}
		st.overlay["agents\x00"+hex.EncodeToString(hash)] = r
		return seedFakeResult{lastInsertID: r.pk, rowsAffected: 1}, nil, nil
	case strings.Contains(lower, "insert into agent_prompts"):
		st.promptInserts++
		st.nextPrompt++
		pk := st.nextPrompt + 500
		if st.promptIDZero {
			pk = 0
		}
		st.promptsOverlay = append(st.promptsOverlay, seedFakePrompt{
			pk: pk, agentID: asInt(arg(0)), version: int(asInt(arg(1))), text: asStr(arg(2))})
		return seedFakeResult{lastInsertID: pk, rowsAffected: 1}, nil, nil
	case strings.Contains(lower, "update agents"):
		st.agentUpdates++
		return driver.RowsAffected(1), nil, nil
	case strings.Contains(lower, "insert into settings"):
		st.settingsInserts++
		if st.settingsInsert1062Index != "" {
			idx := st.settingsInsert1062Index
			st.settingsInsert1062Index = ""
			return nil, nil, dup1062(idx)
		}
		key, identity, hash, value := asStr(arg(0)), asBytes(arg(1)), asBytes(arg(2)), asStr(arg(3))
		if r := st.row("settings", hash); r != nil {
			return nil, nil, dup1062("uq_settings_key_hash")
		}
		st.nextSetting++
		r := &seedFakeRow{pk: st.nextSetting + 900, key: key, identity: identity, hash: hash, value: value}
		st.overlay["settings\x00"+hex.EncodeToString(hash)] = r
		return seedFakeResult{lastInsertID: r.pk, rowsAffected: 1}, nil, nil
	case strings.Contains(lower, "update settings"):
		st.settingsUpdates++
		// 生产语句是 UPDATE settings SET `value` = ?, updated_at = NOW(3) WHERE row_id = ?
		value, pk := asStr(arg(0)), asInt(arg(1))
		for _, r := range st.allRows("settings") {
			if r.pk == pk {
				r.value = value
			}
		}
		return driver.RowsAffected(1), nil, nil
	}
	return nil, nil, fmt.Errorf("seedFake: 未预期语句: %s", oneLineFor(query))
}

func asStr(v driver.Value) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	}
	return fmt.Sprint(v)
}

type seedFakeDriver struct{}

func (seedFakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("seedFakeDriver: use OpenDB")
}

type seedFakeConnector struct{ fake *seedFake }

func (c seedFakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &seedFakeConn{fake: c.fake}, nil
}
func (c seedFakeConnector) Driver() driver.Driver { return seedFakeDriver{} }

type seedFakeConn struct{ fake *seedFake }

func (c *seedFakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := seedDriverValues(args)
	c.fake.record("exec", query, values)
	res, _, err := c.fake.handle(query, values)
	return res, err
}

func (c *seedFakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := seedDriverValues(args)
	c.fake.record("query", query, values)
	_, rows, err := c.fake.handle(query, values)
	return rows, err
}

func (c *seedFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("seedFakeConn: no Prepare")
}
func (c *seedFakeConn) Close() error { return nil }
func (c *seedFakeConn) Begin() (driver.Tx, error) {
	return &seedFakeTx{fake: c.fake}, nil
}

type seedFakeTx struct{ fake *seedFake }

// Commit 把 overlay 合并进 committed（真提交语义）。
func (t *seedFakeTx) Commit() error {
	t.fake.record("commit", "", nil)
	st := t.fake.state
	st.mu.Lock()
	defer st.mu.Unlock()
	for k, v := range st.overlay {
		st.committed[k] = v
	}
	st.overlay = map[string]*seedFakeRow{}
	st.promptsCommitted = append(st.promptsCommitted, st.promptsOverlay...)
	st.promptsOverlay = nil
	st.anchor = st.anchor || st.anchorOverlay
	st.anchorOverlay = false
	return nil
}

// Rollback **丢弃** overlay：失败路径必须留下零残留，这是断言"零污染"的基础。
func (t *seedFakeTx) Rollback() error {
	t.fake.record("rollback", "", nil)
	st := t.fake.state
	st.mu.Lock()
	defer st.mu.Unlock()
	st.overlay = map[string]*seedFakeRow{}
	st.promptsOverlay = nil
	st.anchorOverlay = false
	return nil
}

func seedDriverValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, 0, len(args))
	for _, a := range args {
		out = append(out, a.Value)
	}
	return out
}

type seedFakeRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *seedFakeRows) Columns() []string { return r.cols }
func (r *seedFakeRows) Close() error      { return nil }
func (r *seedFakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

type seedFakeResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r seedFakeResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r seedFakeResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

func newSeedFakeDB(t *testing.T, mutate func(*seedFakeState)) (*DB, *seedFake) {
	t.Helper()
	st := &seedFakeState{
		committed: map[string]*seedFakeRow{},
		overlay:   map[string]*seedFakeRow{},
	}
	if mutate != nil {
		mutate(st)
	}
	fake := &seedFake{state: st}
	sqlDB := sql.OpenDB(seedFakeConnector{fake: fake})
	// TxRunner 的准入 + 串行重放都在同一条 fake 连接上，事务边界与顺序可精确断言。
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return &DB{sqlDB}, fake
}

// seedFakeSeedRetesterRow 在 fake 库里预置一行 agents（可控身份三元组）。
func seedFakeSeedRetesterRow(st *seedFakeState, key string, identity, hash []byte, pk int64) {
	st.committed["agents\x00"+hex.EncodeToString(hash)] = &seedFakeRow{pk: pk, key: key, identity: identity, hash: hash}
}

// seedFakeSeedSettingRow 在 fake 库里预置一行 settings（可控身份三元组与取值）。
func seedFakeSeedSettingRow(st *seedFakeState, key string, identity, hash []byte, value string, pk int64) {
	st.committed["settings\x00"+hex.EncodeToString(hash)] = &seedFakeRow{
		pk: pk, key: key, identity: identity, hash: hash, value: value}
}

// —— 「列 ↔ 实参」解析 ————————————————————————————————————————

// insertColumnsRe 抓 INSERT 的目标表与列清单（反引号标识符也在字符类里）。
var insertColumnsRe = regexp.MustCompile("(?is)INSERT\\s+INTO\\s+(`?\\w+?`?)\\s*\\(([^()]*)\\)\\s*VALUES")

// nullifArgRe 是 `NULLIF(?, ”)` 包裹的可空实参。
var nullifArgRe = regexp.MustCompile(`(?is)^NULLIF\(\s*\?\s*,\s*''\s*\)$`)

// columnArgs 按列清单把 VALUES 的每一项映射成 列名 → 值：
// `?` 与 `NULLIF(?, ”)` 取下一个实参，其余（”、FALSE、常量 1、NOW(3) …）取字面量文本。
// 这是「参数与列错位」这类 bug 的唯一权威判据，所以写入内容断言全部走它。
func columnArgs(t *testing.T, query string, args []driver.Value) map[string]any {
	t.Helper()
	m := insertColumnsRe.FindStringSubmatch(query)
	if m == nil {
		t.Fatalf("无法解析 INSERT 列清单: %s", oneLineFor(query))
	}
	items := splitTopLevel(m[2])
	values := splitTopLevel(valuesClause(t, query))
	if len(items) != len(values) {
		t.Fatalf("列数 %d 与 VALUES 项数 %d 不符: %s", len(items), len(values), oneLineFor(query))
	}
	next := 0
	out := make(map[string]any, len(items))
	for i, item := range items {
		col := normalizeColName(item)
		v := strings.TrimSpace(values[i])
		switch {
		case v == "?":
			if next >= len(args) {
				t.Fatalf("列 %s 需要实参但实参已用尽: %s", col, oneLineFor(query))
			}
			out[col] = args[next]
			next++
		case nullifArgRe.MatchString(v):
			if next >= len(args) {
				t.Fatalf("列 %s 需要实参但实参已用尽: %s", col, oneLineFor(query))
			}
			out[col] = args[next]
			next++
		default:
			out[col] = strings.Trim(v, "'")
		}
	}
	if next != len(args) {
		t.Fatalf("实参 %d 个只匹配到 %d 个: %s", len(args), next, oneLineFor(query))
	}
	return out
}

// valuesClause 取出 VALUES (...) 括号内的文本。
func valuesClause(t *testing.T, query string) string {
	t.Helper()
	i := regexp.MustCompile(`(?is)\bVALUES\b`).FindStringIndex(query)
	if i == nil {
		t.Fatalf("语句没有 VALUES 子句: %s", oneLineFor(query))
	}
	rest := query[i[1]:]
	start := strings.Index(rest, "(")
	if start < 0 {
		t.Fatalf("VALUES 后没有括号: %s", oneLineFor(query))
	}
	depth, inQuote := 0, false
	for j := start; j < len(rest); j++ {
		switch rest[j] {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote {
				depth--
				if depth == 0 {
					return rest[start+1 : j]
				}
			}
		}
	}
	t.Fatalf("VALUES 括号未闭合: %s", oneLineFor(query))
	return ""
}

// splitTopLevel 按最外层逗号切分。
func splitTopLevel(s string) []string {
	var out []string
	depth, inQuote, start := 0, false, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if len(strings.TrimSpace(s[start:])) > 0 {
		out = append(out, strings.TrimSpace(s[start:]))
	}
	return out
}

func normalizeColName(s string) string {
	return strings.NewReplacer("`", "", " ", "", "\n", "", "\t", "").Replace(s)
}

func oneLineFor(s string) string { return strings.Join(strings.Fields(s), " ") }

func assertBytes(t *testing.T, what string, got any, want []byte) {
	t.Helper()
	switch v := got.(type) {
	case []byte:
		if string(v) != string(want) {
			t.Fatalf("%s = %s, want %s", what, hex.EncodeToString(v), hex.EncodeToString(want))
		}
	case string:
		if v != string(want) {
			t.Fatalf("%s = %q, want %s", what, v, hex.EncodeToString(want))
		}
	default:
		t.Fatalf("%s 类型 %T，不是 BLOB/BINARY", what, got)
	}
}

// —— 离线用例 ————————————————————————————————————————————————

// TestSeedFindingRetesterWritesCanonicalIdentity 校验「列 ↔ 实参」逐位对齐：
// hash 唯一键的身份列必须是 schema 包算出的 canonical 载荷与 SHA-256，
// prompt 正文必须逐字透传，flag 也必须带身份对；并且 flag 落库**不得**再用
// ON DUPLICATE KEY UPDATE（I3：更新分支不读持久身份，会污染同 hash 的异 identity 行）。
func TestSeedFindingRetesterWritesCanonicalIdentity(t *testing.T) {
	const prompt = "你是复测 agent\n第二行"
	d, fake := newSeedFakeDB(t, nil)

	if err := d.SeedFindingRetester(context.Background(), prompt); err != nil {
		t.Fatalf("SeedFindingRetester: %v", err)
	}
	if fake.commits() != 1 {
		t.Fatalf("commit 次数 = %d, want 1；tx=%v", fake.commits(), fake.txOps())
	}

	wantAgentIdentity, wantAgentHash := agentKeyIdentity(FindingRetestAgentKey)
	wantFlagIdentity, wantFlagHash := settingsKeyIdentity(FindingRetesterSeedFlag)

	agentCall, ok := fake.find("insert into agents")
	if !ok {
		t.Fatal("缺少 agents INSERT")
	}
	promptCall, ok := fake.find("insert into agent_prompts")
	if !ok {
		t.Fatal("缺少 agent_prompts INSERT")
	}
	flagCall, ok := fake.find("insert into settings")
	if !ok {
		t.Fatal("缺少 settings INSERT")
	}

	agentArgs := columnArgs(t, agentCall.query, agentCall.args)
	if got := fmt.Sprint(agentArgs["key"]); got != FindingRetestAgentKey {
		t.Fatalf("agents.key = %q, want %q", got, FindingRetestAgentKey)
	}
	assertBytes(t, "agents.key_identity", agentArgs["key_identity"], wantAgentIdentity)
	assertBytes(t, "agents.key_hash", agentArgs["key_hash"], wantAgentHash)
	if got := fmt.Sprint(agentArgs["role"]); got != "assistant" {
		t.Fatalf("agents.role = %q, want assistant", got)
	}
	if got := fmt.Sprint(agentArgs["name"]); got == "" {
		t.Fatal("agents.name 为空")
	}
	// NOT NULL TEXT 列在 TiDB 上没有 DDL 默认值，必须显式给应用层默认值。
	for col, want := range map[string]string{
		"wrapup_prompt":              "",
		"task_timeout_wrapup_prompt": "",
		"trigger_run_mode":           "serial",
		"trigger_merge_mode":         "all",
	} {
		if got := fmt.Sprint(agentArgs[col]); got != want {
			t.Fatalf("agents.%s = %q, want %q", col, got, want)
		}
	}
	if got := strings.ToUpper(fmt.Sprint(agentArgs["builtin"])); got != "FALSE" {
		t.Fatalf("agents.builtin = %v, want FALSE（必须是可编辑 agent）", agentArgs["builtin"])
	}
	if got := strings.ToUpper(fmt.Sprint(agentArgs["enabled"])); got != "TRUE" {
		t.Fatalf("agents.enabled = %v, want TRUE", agentArgs["enabled"])
	}

	promptArgs := columnArgs(t, promptCall.query, promptCall.args)
	if got := fmt.Sprint(promptArgs["agent_id"]); got != "101" {
		t.Fatalf("agent_prompts.agent_id = %q, want 101", got)
	}
	if got := fmt.Sprint(promptArgs["template_text"]); got != prompt {
		t.Fatalf("agent_prompts.template_text 未逐字透传: %q", got)
	}
	if got := fmt.Sprint(promptArgs["version"]); got != "1" {
		t.Fatalf("agent_prompts.version = %q, want 1", got)
	}
	if got := fmt.Sprint(promptArgs["note"]); got != "内置默认" {
		t.Fatalf("agent_prompts.note = %q", got)
	}

	flagArgs := columnArgs(t, flagCall.query, flagCall.args)
	if got := fmt.Sprint(flagArgs["key"]); got != FindingRetesterSeedFlag {
		t.Fatalf("settings.key = %q, want %q", got, FindingRetesterSeedFlag)
	}
	assertBytes(t, "settings.key_identity", flagArgs["key_identity"], wantFlagIdentity)
	assertBytes(t, "settings.key_hash", flagArgs["key_hash"], wantFlagHash)
	if got := fmt.Sprint(flagArgs["value"]); got != "true" {
		t.Fatalf("settings.value = %q, want true", got)
	}
	// I3：flag 落库必须是**不带更新分支**的严格 INSERT。
	if strings.Contains(strings.ToLower(flagCall.query), "on duplicate key update") {
		t.Fatalf("settings flag 仍在使用 ON DUPLICATE KEY UPDATE（会覆盖同 hash 的异 identity 行）: %s",
			oneLineFor(flagCall.query))
	}
}

// TestSeedFindingRetesterUsesBusinessGate：本写者必须走统一 TxRunner 的全局门禁，
// 并且门禁必须先于本 seed 的专属锚点与任何实体锁（spec §3.2.2 / I4）。
func TestSeedFindingRetesterUsesBusinessGate(t *testing.T) {
	d, fake := newSeedFakeDB(t, nil)
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("SeedFindingRetester: %v", err)
	}
	gateInsert, ok := fake.find("insert ignore into locks")
	if !ok {
		t.Fatal("缺少 Business Gate 的 locks('business_relations') 插入")
	}
	gateLock, ok := fake.find("from locks")
	if !ok || !strings.Contains(strings.ToUpper(gateLock.query), "FOR UPDATE") {
		t.Fatalf("Business Gate 必须对 locks 行 FOR UPDATE；实际=%v", oneLineFor(gateLock.query))
	}
	anchor, ok := fake.find("from maintenance")
	if !ok {
		t.Fatal("缺少 maintenance 锚点锁")
	}
	// 顺序：Gate（insert + 锁读）→ 锚点 → settings → agents → prompt → flag。
	stmts := fake.statements()
	idx := func(c seedCall) int {
		for i, s := range stmts {
			if s.query == c.query {
				return i
			}
		}
		return -1
	}
	if !(idx(gateInsert) < idx(anchor) && idx(gateLock) < idx(anchor)) {
		t.Fatalf("Business Gate 必须早于 maintenance 锚点；实际序列=%v", stmtQueries(stmts))
	}
	anchorAt := idx(anchor)
	for _, want := range []string{"from `settings`", "insert into agents", "insert into agent_prompts", "insert into settings"} {
		c, ok := fake.find(want)
		if !ok {
			t.Fatalf("缺少语句 %q；实际=%v", want, stmtQueries(stmts))
		}
		if !(anchorAt < idx(c)) {
			t.Fatalf("锚点必须早于 %q；实际=%v", want, stmtQueries(stmts))
		}
	}
}

func stmtQueries(calls []seedCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, oneLineFor(c.query))
	}
	return out
}

// TestSeedFindingRetesterDecisionReadsAreCurrentReads（I1 的结构面）：
// 每一个"决定是否写入"的读都必须是锁读（FOR UPDATE）。RR 下普通 SELECT 读旧快照，
// 会把"赢家已提交 flag → 用户删除 → 已排队者"这条调度判成"从未 seed"，复活用户删掉的 agent。
func TestSeedFindingRetesterDecisionReadsAreCurrentReads(t *testing.T) {
	d, fake := newSeedFakeDB(t, nil)
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("SeedFindingRetester: %v", err)
	}
	for _, c := range fake.statements() {
		q := strings.ToLower(c.query)
		if !strings.HasPrefix(q, "select") {
			continue
		}
		if !strings.Contains(q, "for update") {
			t.Fatalf("决策路径上出现非锁读（RR 下会读旧快照）: %s", oneLineFor(c.query))
		}
	}
	// flag 定位必须是对 key_hash 的当前读（否则看不到并发赢家刚提交的 flag）。
	if n := fake.count("from `settings` where `key_hash`"); n == 0 {
		t.Fatal("flag 定位必须是对 key_hash 的当前读（否则看不到并发赢家刚提交的 flag）")
	}

	// 热路径（flag 行存在）：取值同样必须是当前读。
	warmIdentity, warmHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	dWarm, fakeWarm := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedSettingRow(st, FindingRetesterSeedFlag, warmIdentity, warmHash, "true", 1)
	})
	if err := dWarm.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("热路径 seed: %v", err)
	}
	flagRead, ok := fakeWarm.find("`value` from settings")
	if !ok || !strings.Contains(strings.ToUpper(flagRead.query), "FOR UPDATE") {
		t.Fatalf("幂等 flag 取值必须 FOR UPDATE；实际=%q", oneLineFor(flagRead.query))
	}
	// 物理 PK 锁必须先于身份仲裁（先持锁、后仲裁，不能用输入自证）。
	// 冷库里没有候选行，仲裁不会发生，所以这里用「已有 retester」的库观察仲裁序列。
	agentIdentity, agentHash := agentKeyIdentity(FindingRetestAgentKey)
	dWarm2, fakeWarm2 := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedRetesterRow(st, FindingRetestAgentKey, agentIdentity, agentHash, 77)
	})
	if err := dWarm2.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("已有 agent 路径 seed: %v", err)
	}
	lock, ok := fakeWarm2.find("from `agents` where `id` = ? for update")
	if !ok {
		t.Fatalf("缺少 agents 物理 PK 锁；实际=%v", stmtQueries(fakeWarm2.statements()))
	}
	pkAt := indexOfQuery(fakeWarm2.statements(), lock.query)
	arb, ok := fakeWarm2.find("`id`, `key_identity` from `agents`")
	if !ok {
		t.Fatalf("缺少 schema.VerifyHashArbitration 的仲裁读；实际=%v", stmtQueries(fakeWarm2.statements()))
	}
	arbAt := indexOfQuery(fakeWarm2.statements(), arb.query)
	if !(pkAt >= 0 && arbAt > pkAt) {
		t.Fatalf("仲裁必须发生在物理 PK 锁之后；实际=%v", stmtQueries(fakeWarm2.statements()))
	}
}

// indexOfQuery 返回某条语句第一次出现的下标。
func indexOfQuery(calls []seedCall, query string) int {
	for i, c := range calls {
		if c.query == query {
			return i
		}
	}
	return -1
}

// TestSeedFindingRetesterAnchorMissingSelfHealsOnce：锚点 SELECT 首次无行 →
// INSERT IGNORE 自愈 → 再次 SELECT 抢锁；只多一条 INSERT，最终仍然 commit。
func TestSeedFindingRetesterAnchorMissingSelfHealsOnce(t *testing.T) {
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.failOnce = map[string]error{"from maintenance": sql.ErrNoRows}
	})
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("锚点自愈后应仍能完成 seed: %v", err)
	}
	if st := fake.state; st.anchorSeeds != 1 {
		t.Fatalf("锚点自愈 INSERT 次数 = %d, want 1", st.anchorSeeds)
	}
	if got := fake.count("insert ignore into maintenance"); got != 1 {
		t.Fatalf("锚点 INSERT 语句数 = %d, want 1", got)
	}
	if n := fake.count("from maintenance"); n != 2 {
		t.Fatalf("锚点 SELECT 次数 = %d, want 2（无行→自愈→重抢）", n)
	}
	if fake.commits() != 1 {
		t.Fatalf("锚点自愈路径 commit 次数 = %d, want 1", fake.commits())
	}
}

// TestSeedFindingRetesterFlagTrueDoesNotTouchBusinessTables：flag 已置位时，
// 除门禁、锚点与 flag 的锁读之外不得有任何语句 —— 编辑不覆盖、删除不复活。
func TestSeedFindingRetesterFlagTrueDoesNotTouchBusinessTables(t *testing.T) {
	wantIdentity, wantHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedSettingRow(st, FindingRetesterSeedFlag, wantIdentity, wantHash, "true", 1)
	})
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("SeedFindingRetester: %v", err)
	}
	st := fake.state
	if st.agentInserts != 0 || st.promptInserts != 0 || st.settingsInserts != 0 || st.settingsUpdates != 0 {
		t.Fatalf("flag=true 仍写入 agents=%d prompts=%d settings insert=%d update=%d",
			st.agentInserts, st.promptInserts, st.settingsInserts, st.settingsUpdates)
	}
	for _, c := range fake.statements() {
		q := strings.ToLower(c.query)
		if strings.Contains(q, "insert into agents") || strings.Contains(q, "insert into agent_prompts") ||
			strings.Contains(q, "insert into settings") || strings.Contains(q, "update agents") ||
			strings.Contains(q, "update settings") {
			t.Fatalf("flag=true 仍下发业务写语句: %s", oneLineFor(c.query))
		}
	}
}

// TestSeedFindingRetesterExistingAgentKeepsPrompt：自然键已存在（经写前仲裁确认）→
// 不插 agent、不追加 prompt、不改 current_prompt_id，但 flag 必须落库。
func TestSeedFindingRetesterExistingAgentKeepsPrompt(t *testing.T) {
	wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedRetesterRow(st, FindingRetestAgentKey, wantIdentity, wantHash, 77)
	})
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("SeedFindingRetester: %v", err)
	}
	st := fake.state
	if st.agentInserts != 0 {
		t.Fatalf("已存在 agent 仍尝试 INSERT = %d, want 0", st.agentInserts)
	}
	if st.promptInserts != 0 {
		t.Fatalf("已有 agent 仍追加 prompt 版本 = %d, want 0", st.promptInserts)
	}
	if st.agentUpdates != 0 {
		t.Fatalf("已有 agent 仍被改写（UPDATE agents %d 次）", st.agentUpdates)
	}
	if st.settingsInserts != 1 {
		t.Fatalf("flag 落库次数 = %d, want 1", st.settingsInserts)
	}
	if fake.commits() != 1 {
		t.Fatalf("commit 次数 = %d, want 1", fake.commits())
	}
	// 既有行必须逐字未动。
	row := fake.state.committed["agents\x00"+hex.EncodeToString(wantHash)]
	if row == nil || row.pk != 77 || row.key != FindingRetestAgentKey {
		t.Fatalf("既有 agent 行被改动: %+v", row)
	}
}

// TestSeedFindingRetesterAgentHashCollisionFailsClosed（I2）：
// 库里存在 key='other'、identity=canonical(other)、但 hash 等于 retester hash 的行
// ——这是"同 hash 异 identity"的持久碰撞注入。1062/命中的 hash 都不足以认定同一对象，
// 必须 fail closed：零业务写入、flag 不落库、碰撞行零改动。
func TestSeedFindingRetesterAgentHashCollisionFailsClosed(t *testing.T) {
	_, retesterHash := agentKeyIdentity(FindingRetestAgentKey)
	otherIdentity, _ := agentKeyIdentity("other")
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedRetesterRow(st, "other", otherIdentity, retesterHash, 31)
		// 顺带放一条**不相关**的配置，用来证明失败路径零污染。
		otherKeyIdentity, otherKeyHash := settingsKeyIdentity("unrelated.setting")
		seedFakeSeedSettingRow(st, "unrelated.setting", otherKeyIdentity, otherKeyHash, "keep-me", 55)
	})

	err := d.SeedFindingRetester(context.Background(), "p")
	if err == nil {
		t.Fatal("同 hash 异 identity 的碰撞被吞成成功")
	}
	if !errors.Is(err, ErrRetesterSeedHashCollision) {
		t.Fatalf("错误必须是身份碰撞哨兵，实际: %v", err)
	}
	if errors.Is(err, lockorder.ErrApplicationRetry) || errors.Is(err, lockorder.ErrRetryExhausted) {
		t.Fatal("身份碰撞不得被归类成可重试冲突")
	}
	st := fake.state
	if st.agentInserts != 0 || st.promptInserts != 0 || st.settingsInserts != 0 || st.settingsUpdates != 0 {
		t.Fatalf("碰撞路径仍有写入意图 agents=%d prompts=%d settings=%d/%d",
			st.agentInserts, st.promptInserts, st.settingsInserts, st.settingsUpdates)
	}
	if fake.commits() != 0 {
		t.Fatalf("碰撞路径仍 commit；tx=%v", fake.txOps())
	}
	if fake.rollbacks() == 0 {
		t.Fatalf("碰撞路径未回滚；tx=%v", fake.txOps())
	}
	// 终态：agents 里只有那条碰撞行（且逐字未动），settings 里**没有** flag，不相关配置原样。
	collided := st.committed["agents\x00"+hex.EncodeToString(retesterHash)]
	if collided == nil || collided.key != "other" || string(collided.identity) != string(otherIdentity) || collided.pk != 31 {
		t.Fatalf("碰撞行被改写: %+v", collided)
	}
	if len(st.allRows("agents")) != 1 {
		t.Fatalf("碰撞路径多插了 agent 行: %d", len(st.allRows("agents")))
	}
	if _, ok := st.committed["settings\x00"+hex.EncodeToString(hashOf(FindingRetesterSeedFlag))]; ok {
		t.Fatal("碰撞失败后仍落了 seed flag")
	}
	unrelatedHash := hashOf("unrelated.setting")
	if r := st.committed["settings\x00"+hex.EncodeToString(unrelatedHash)]; r == nil || r.value != "keep-me" {
		t.Fatalf("不相关 settings 被污染: %+v", r)
	}
}

// TestSeedFindingRetesterSettingsHashCollisionFailsClosed（I3）：
// settings 里存在 key='other'、identity=canonical(other)、hash 等于 seed flag hash 的行。
// 旧实现的 ODKU 会把这一行改成 true（污染无关配置）。新协议必须在写 value **之前**
// 用持久身份仲裁 fail closed：那一行零改动，agent 不插，flag 不落。
func TestSeedFindingRetesterSettingsHashCollisionFailsClosed(t *testing.T) {
	_, flagHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	otherIdentity, _ := settingsKeyIdentity("other")
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		seedFakeSeedSettingRow(st, "other", otherIdentity, flagHash, "do-not-touch", 12)
	})

	err := d.SeedFindingRetester(context.Background(), "p")
	if err == nil {
		t.Fatal("settings 同 hash 异 identity 的碰撞被吞成成功")
	}
	if !errors.Is(err, ErrRetesterSeedHashCollision) {
		t.Fatalf("错误必须是身份碰撞哨兵，实际: %v", err)
	}
	st := fake.state
	row := st.committed["settings\x00"+hex.EncodeToString(flagHash)]
	if row == nil || row.value != "do-not-touch" || row.key != "other" {
		t.Fatalf("无关配置被 ODKU 式改写: %+v", row)
	}
	if st.agentInserts != 0 || st.promptInserts != 0 || st.settingsUpdates != 0 {
		t.Fatalf("碰撞路径仍有写入意图 agents=%d prompts=%d settingsUpdate=%d",
			st.agentInserts, st.promptInserts, st.settingsUpdates)
	}
	if fake.commits() != 0 {
		t.Fatal("碰撞路径仍 commit")
	}
}

// TestSeedFindingRetesterNaturalKeyMustBeSelfConsistent：
// 持久 identity 恰好等于本 seed 的 identity，但自然键是别的值（identity 与自然键不自洽）
// ——同样不得认定"这就是本 seed 的 agent"。
func TestSeedFindingRetesterNaturalKeyMustBeSelfConsistent(t *testing.T) {
	wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		// identity/hash 都是 retester 的，但自然键写了别的值。
		seedFakeSeedRetesterRow(st, "retester-typo", wantIdentity, wantHash, 41)
	})
	err := d.SeedFindingRetester(context.Background(), "p")
	if err == nil {
		t.Fatal("自然键不自洽的行被当成合法 retester")
	}
	if !errors.Is(err, ErrRetesterSeedHashCollision) {
		t.Fatalf("错误必须是身份不自洽哨兵，实际: %v", err)
	}
	st := fake.state
	if st.promptInserts != 0 || st.settingsInserts != 0 {
		t.Fatalf("不自洽路径仍写入 prompts=%d settings=%d", st.promptInserts, st.settingsInserts)
	}
	if fake.commits() != 0 {
		t.Fatal("不自洽路径仍 commit")
	}
}

// TestSeedFindingRetesterExpectedIndexConflictRollsBackAndReplays（I2 的 1062 处置）：
// 定位快照看不到并发赢家 → INSERT 撞 uq_agents_key_hash → 本轮**必须整体回滚**，
// 由 TxRunner 在全新事务里重新定位仲裁；重放轮次不得再插第二个 agent 或第二个 prompt。
func TestSeedFindingRetesterExpectedIndexConflictRollsBackAndReplays(t *testing.T) {
	wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		// 并发赢家已经落库，但**第一轮尝试的定位快照看不见它**：INSERT 才发现并撞
		// uq_agents_key_hash。重放轮必须重新定位并按身份仲裁处理。
		st.committed = map[string]*seedFakeRow{
			"agents\x00" + hex.EncodeToString(wantHash): {pk: 3001, key: FindingRetestAgentKey, identity: wantIdentity, hash: wantHash},
		}
		st.hideAgentRounds = 1
	})
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		t.Fatalf("指定索引上的 1062 应经回滚重放后成功: %v", err)
	}
	st := fake.state
	if st.agentInserts != 1 {
		t.Fatalf("agents INSERT 次数 = %d, want 1（重放轮必须走仲裁分支）", st.agentInserts)
	}
	if st.promptInserts != 0 {
		t.Fatalf("重放轮给赢家 agent 追加了 prompt 版本 = %d, want 0", st.promptInserts)
	}
	if fake.commits() != 1 || fake.rollbacks() < 1 {
		t.Fatalf("必须至少一次回滚 + 恰好一次提交；tx=%v", fake.txOps())
	}
	rows := st.allRows("agents")
	if len(rows) != 1 || rows[0].pk != 3001 {
		t.Fatalf("重放后 agent 行数/主键异常: %+v", rows)
	}
	if len(st.promptsCommitted) != 0 {
		t.Fatalf("重放后多出 prompt: %+v", st.promptsCommitted)
	}
	flag := st.committed["settings\x00"+hex.EncodeToString(hashOf(FindingRetesterSeedFlag))]
	if flag == nil || flag.value != "true" {
		t.Fatalf("重放后 flag 未落库: %+v", flag)
	}
}

// TestSeedFindingRetesterUnexpectedIndexConflictFailsClosed（I2 的意外 1062）：
// 1062 落在别的索引上时，既不能当成"同 key 已存在"，也不能当成可重试冲突反复重放。
func TestSeedFindingRetesterUnexpectedIndexConflictFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, index string }{
		{"主键冲突", "PRIMARY"},
		{"其他唯一键", "uq_agents_name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
				st.anchor = true
				st.agentInsert1062Index = tc.index
			})
			err := d.SeedFindingRetester(context.Background(), "p")
			if err == nil {
				t.Fatal("意外索引上的 1062 被吞掉")
			}
			if !errors.Is(err, ErrRetesterSeedUnexpectedConflict) {
				t.Fatalf("错误必须是意外索引哨兵，实际: %v", err)
			}
			st := fake.state
			if st.agentInserts != 1 {
				t.Fatalf("意外索引冲突不得重放，INSERT 次数 = %d, want 1", st.agentInserts)
			}
			if st.promptInserts != 0 || st.settingsInserts != 0 {
				t.Fatalf("意外索引冲突仍写入 prompts=%d settings=%d", st.promptInserts, st.settingsInserts)
			}
			if fake.commits() != 0 {
				t.Fatal("意外索引冲突仍 commit")
			}
			if _, ok := st.committed["settings\x00"+hex.EncodeToString(hashOf(FindingRetesterSeedFlag))]; ok {
				t.Fatal("意外索引冲突后仍落了 seed flag")
			}
		})
	}
}

// TestSeedFindingRetesterSettingsLockTimeoutRetries（I5）：settings 幂等标记 INSERT 命中
// **非 1062** 错误（这里注入 1205 锁等待超时）时，处置必须与 agents INSERT 完全一致 ——
// 用 %w 原样上抛、交给 TxRunner 按结构化编号判定为可重试冲突 → 整笔回滚 + 退避后在全新
// 事务里重放；**绝不**包装成不可重试的 ErrRetesterSeedUnexpectedConflict（旧实现对任意
// ExecContext 错误都调 retesterSeedConflictOperation，把瞬时锁超时误判成"1062 无法归因"
// 且用 %s 丢弃错误码 → Runner 判死、启动失败）。
//
// 判据：注入一次性的 1205（failOnce 命中即失效）后，第二轮在全新事务里成功提交：
//   - SeedFindingRetester 最终返回 nil（重放成功）；
//   - 错误绝不是 ErrRetesterSeedUnexpectedConflict；
//   - 至少一次回滚 + 恰好一次提交（1205 那轮回滚，重放轮提交）；
//   - flag 最终落库为 true、retester agent 恰好一行。
//
// 若回退到旧的错误分类（对任意错误 fail closed），本用例会变红：SeedFindingRetester 返回
// ErrRetesterSeedUnexpectedConflict、commits()==0、flag 不落库。
func TestSeedFindingRetesterSettingsLockTimeoutRetries(t *testing.T) {
	d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
		st.anchor = true
		// 只在 settings INSERT 上下一次 1205；命中即失效，重放轮可正常落库。
		st.failOnce = map[string]error{
			"insert into settings": &mysql.MySQLError{
				Number:  1205,
				Message: "Lock wait timeout exceeded; try restarting transaction",
			},
		}
	})
	if err := d.SeedFindingRetester(context.Background(), "p"); err != nil {
		if errors.Is(err, ErrRetesterSeedUnexpectedConflict) {
			t.Fatalf("I5：settings INSERT 的 1205 被误判成不可重试的意外冲突: %v", err)
		}
		t.Fatalf("settings INSERT 的 1205 应经回滚重放后成功: %v", err)
	}
	if fake.rollbacks() < 1 {
		t.Fatalf("1205 那轮必须整笔回滚；tx=%v", fake.txOps())
	}
	if fake.commits() != 1 {
		t.Fatalf("重放轮必须恰好一次提交；tx=%v", fake.txOps())
	}
	st := fake.state
	// 提交只发生在重放轮：本轮 agents 恰好一行。
	if rows := st.allRows("agents"); len(rows) != 1 {
		t.Fatalf("最终 agent 行数 = %d, want 1；rows=%+v", len(rows), rows)
	}
	flag := st.committed["settings\x00"+hex.EncodeToString(hashOf(FindingRetesterSeedFlag))]
	if flag == nil || flag.value != "true" {
		t.Fatalf("重放后 seed flag 未落库或未置 true: %+v", flag)
	}
}

// TestSeedFindingRetesterAnyStepFailureRollsBack：任一写入失败都必须整体回滚，
// 绝不允许 commit（prompt 与 flag 的原子性），并且库里不留半套状态。
func TestSeedFindingRetesterAnyStepFailureRollsBack(t *testing.T) {
	for _, step := range []string{"insert into agents", "select id from agents", "insert into agent_prompts",
		"update agents", "insert into settings"} {
		t.Run(step, func(t *testing.T) {
			d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
				st.anchor = true
				st.failOnce = map[string]error{step: fmt.Errorf("boom: %s", step)}
			})
			if err := d.SeedFindingRetester(context.Background(), "p"); err == nil {
				t.Fatal("写入失败被吞掉")
			}
			if fake.commits() != 0 {
				t.Fatalf("失败路径仍 commit；tx=%v", fake.txOps())
			}
			if fake.rollbacks() == 0 {
				t.Fatalf("失败路径未回滚；tx=%v", fake.txOps())
			}
			st := fake.state
			if len(st.committed) != 0 || len(st.promptsCommitted) != 0 {
				t.Fatalf("失败路径留下半套状态: rows=%d prompts=%d", len(st.committed), len(st.promptsCommitted))
			}
		})
	}
}

// TestSeedFindingRetesterStrictIDs：自增 id 与 key_hash 回读必须严格一致，
// 且 id 未确认之前不得产生任何子表写入（否则留下孤儿 prompt）。
func TestSeedFindingRetesterStrictIDs(t *testing.T) {
	t.Run("agents 回读 id 与自增 id 不符", func(t *testing.T) {
		d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
			st.anchor = true
			// 自增 id 之后，按 hash 回读却拿到**别人**的 id。
			st.agentReadbackID = 999999
		})
		if err := d.SeedFindingRetester(context.Background(), "p"); err == nil {
			t.Fatal("id 漂移被接受")
		}
		if fake.commits() != 0 {
			t.Fatal("id 漂移仍提交")
		}
		if fake.state.promptInserts != 0 {
			t.Fatal("id 漂移时仍写 prompt（会产生指向别人 agent 的模板）")
		}
	})
	t.Run("agents 回读无行", func(t *testing.T) {
		d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
			st.anchor = true
			st.failOnce = map[string]error{"select id from agents": sql.ErrNoRows}
		})
		if err := d.SeedFindingRetester(context.Background(), "p"); err == nil {
			t.Fatal("回读无行被接受")
		}
		if fake.state.promptInserts != 0 {
			t.Fatal("agent id 尚未确认就先写 prompt")
		}
		if fake.commits() != 0 {
			t.Fatal("回读失败仍提交")
		}
	})
	t.Run("prompt 自增 id 为 0", func(t *testing.T) {
		d, fake := newSeedFakeDB(t, func(st *seedFakeState) {
			st.anchor = true
			st.promptIDZero = true
		})
		if err := d.SeedFindingRetester(context.Background(), "p"); err == nil {
			t.Fatal("agent_prompts 自增 id=0 被接受")
		}
		if fake.commits() != 0 {
			t.Fatal("prompt id 异常仍提交")
		}
		if got := fake.state.agentUpdates; got != 0 {
			t.Fatalf("prompt id 异常仍把 current_prompt_id 指过去（%d 条 UPDATE）", got)
		}
	})
	t.Run("空模板拒绝", func(t *testing.T) {
		d, fake := newSeedFakeDB(t, nil)
		if err := d.SeedFindingRetester(context.Background(), ""); err == nil {
			t.Fatal("空模板被接受")
		}
		if got := len(fake.statements()); got != 0 {
			t.Fatalf("空模板不应下发任何语句，实际 %d 条", got)
		}
	})
}

// TestRetesterSeedDuplicateIndexAttribution：索引归因只认结构化 1062 + `for key '…'`。
// 消息形态取 TiDB 8.5 的真实写法（含 `表名.索引名`），解析不出来必须返回 ok=false。
func TestRetesterSeedDuplicateIndexAttribution(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		index string
		ok    bool
	}{
		{"TiDB 纯索引名", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'uq_agents_key_hash'"}, "uq_agents_key_hash", true},
		{"TiDB 表名.索引名", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'agents.uq_agents_key_hash'"}, "uq_agents_key_hash", true},
		{"主键", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'PRIMARY'"}, "PRIMARY", true},
		{"无 for key 片段", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}, "", false},
		{"非 1062", &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}, "", false},
		{"非结构化错误", errors.New("1062 Duplicate entry for key 'uq_agents_key_hash'"), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index, ok := retesterSeedDuplicateIndex(tc.err)
			if ok != tc.ok || index != tc.index {
				t.Fatalf("retesterSeedDuplicateIndex() = (%q, %v), want (%q, %v)", index, ok, tc.index, tc.ok)
			}
		})
	}
}

// —— 工具 ————————————————————————————————————————————————————

// hashOf 是 settings 自然键的 canonical hash（断言索引用）。
func hashOf(key string) []byte {
	_, h := settingsKeyIdentity(key)
	return h
}

// =====================================================================
// 真机层（ARTEX_RETESTER_SEED_TEST_DSN 门控）
// =====================================================================
//
// C1 的硬要求：**固定自然键的破坏性 seed 测试绝不允许指向业务库**。本层的隔离措施：
//
//  1. 独立开关：只认 ARTEX_RETESTER_SEED_TEST_DSN。没配 → 显式 t.Skip（未验证），
//     绝不借用 ARTEX_TIDB_DSN（那是业务库），两者相等更是直接 t.Fatal。
//  2. 库名黑名单：artex_dev / artex-dev / artex_prod 这类业务 schema 直接拒绝。
//  3. 建表前必须证明**当前 schema 对象数恰好为 0**（information_schema.TABLES 全量
//     计数，含视图）；非空即 fail closed（0 DDL / 0 DROP / 不清空别人的 schema）。
//  4. 只用**严格 CREATE TABLE**（无 IF NOT EXISTS）逐张建表，逐张登记所有权；
//     结束时只 DROP 本轮登记成功的表（逆序），绝不 DROP DATABASE、绝不 DROP 未登记对象。
//  5. 终态判据：本轮登记的表全部删除后，BASE TABLE 必须恰好回到 0，否则测试标红。
//  6. 行级清理只针对"本轮新建的物理主键"（before/after 快照差集），before 里已存在的
//     行一条都不许碰；本轮 fixture 库是自己刚建起来的空 schema，因此 retester 相关行
//     必然是本轮产生的 —— 这一点由快照差集证明，而不是靠固定自然键去猜。
//
// 真机层覆盖：DDL 可执行、三条不得回退的幂等语义、RR/RC 两级下的"删除不复活"
// 确定性调度、4 worker 冷锚点重叠、持久碰撞注入下的零污染、意外索引 1062。
//
// 本层不执行 SET GLOBAL、不改任何全局配置；探针会话只在自己的连接上 SET SESSION
// innodb_lock_wait_timeout，用完立即关闭该连接。

const (
	retesterSeedTestDSNEnv = "ARTEX_RETESTER_SEED_TEST_DSN"
	// retesterSeedForbiddenSchemas 是业务/生产 schema 黑名单。
	retesterSeedForbiddenSchemas = "artex_dev,artex-dev,artex_prod"
)

type retesterSeedFixture struct {
	d      *DB
	dsn    string
	raw    *sql.DB
	mu     sync.Mutex
	owned  []string
	before retesterSeedRowSet
}

// retesterSeedRowSet 是四张相关表的物理主键快照。
type retesterSeedRowSet struct {
	agents      map[int64]string // id -> key
	prompts     map[int64]int64  // id -> agent_id
	settings    map[int64]string // row_id -> key
	maintenance map[int64]string // row_id -> singleton_key
}

func retesterSeedIsolatedSchema(t *testing.T) *retesterSeedFixture {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv(retesterSeedTestDSNEnv))
	if dsn == "" {
		t.Skipf("未验证：%s 未配置，本用例诚实跳过（不代表通过）。仅允许指向独占空库。", retesterSeedTestDSNEnv)
	}
	// 1. 绝不允许与业务 DSN 同源。
	if biz := strings.TrimSpace(os.Getenv("ARTEX_TIDB_DSN")); biz != "" && biz == dsn {
		t.Fatalf("%s 不得等于 ARTEX_TIDB_DSN（业务库）：固定键破坏性 seed 测试必须在独占空库上运行", retesterSeedTestDSNEnv)
	}

	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open 隔离库: %v", err)
	}
	raw.SetMaxOpenConns(4)
	f := &retesterSeedFixture{dsn: dsn, raw: raw, before: retesterSeedRowSet{
		agents: map[int64]string{}, prompts: map[int64]int64{},
		settings: map[int64]string{}, maintenance: map[int64]string{},
	}}
	ctx := context.Background()

	// 2. 必须是真 TiDB、必须选定了库、库名不得是业务库。
	var version string
	if err := raw.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("读 VERSION(): %v", err)
	}
	if !schema.IsRealTiDB(version) {
		t.Fatalf("目标库不是原生 TiDB 8.5：%q", version)
	}
	var dbName string
	if err := raw.QueryRowContext(ctx, "SELECT COALESCE(DATABASE(),'')").Scan(&dbName); err != nil {
		t.Fatalf("读 DATABASE(): %v", err)
	}
	if strings.TrimSpace(dbName) == "" {
		t.Fatal("隔离 DSN 必须选定一个库（SELECT DATABASE() 为空）")
	}
	for _, banned := range strings.Split(retesterSeedForbiddenSchemas, ",") {
		if strings.EqualFold(dbName, banned) {
			t.Fatalf("拒绝在业务/生产 schema %q 上运行固定键 seed 测试", dbName)
		}
	}

	// 3. 建表前必须恰好 0 个对象（含视图）；非空 fail closed。
	var objects int
	if err := raw.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()").Scan(&objects); err != nil {
		t.Fatalf("统计 schema 对象数: %v", err)
	}
	if objects != 0 {
		t.Fatalf("隔离 schema %q 不是空库（现存 %d 个对象）：拒绝建表，更拒绝清空别人的 schema", dbName, objects)
	}

	t.Cleanup(func() { f.close(t) })

	// 4. 严格建表（无 IF NOT EXISTS），逐张登记所有权。
	for _, tbl := range schema.AllTables() {
		stmt, err := schema.GenerateDDL(tbl)
		if err != nil {
			t.Fatalf("生成 %s 的 DDL: %v", tbl.Name, err)
		}
		const prefix = "CREATE TABLE IF NOT EXISTS "
		if !strings.HasPrefix(stmt, prefix) {
			t.Fatalf("%s 的 DDL 前缀异常: %.60s", tbl.Name, stmt)
		}
		strict := "CREATE TABLE " + stmt[len(prefix):]
		if _, err := raw.ExecContext(ctx, strict); err != nil {
			// 失败的那张表**没有**创建成功，因此不登记，cleanup 也绝不 DROP 它。
			t.Fatalf("建表 %s: %v", tbl.Name, err)
		}
		f.mu.Lock()
		f.owned = append(f.owned, tbl.Name)
		f.mu.Unlock()
	}
	for _, stmt := range schema.PrepareInfraSQL() {
		if _, err := raw.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("基础设施 seed %q: %v", stmt, err)
		}
	}
	if err := tidb.EnsureLockInfrastructure(ctx, raw); err != nil {
		t.Fatalf("maintenance 基础设施: %v", err)
	}

	// 5. 用生产入口打开（VerifyBaseline + seedBuiltins），确认 fixture 与真实启动路径同构。
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open 隔离库: %v", err)
	}
	f.d = d

	// 6. 起点必须没有任何 retester 痕迹（固定自然键、canonical hash 都查）。
	_, wantAgentHash := agentKeyIdentity(FindingRetestAgentKey)
	_, wantFlagHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	var n int
	if err := d.QueryRowContext(ctx, "SELECT count(*) FROM agents WHERE key_hash = ? OR `key` = ?", wantAgentHash, FindingRetestAgentKey).Scan(&n); err != nil {
		t.Fatalf("起点检查 agents: %v", err)
	}
	if n != 0 {
		t.Fatalf("隔离库起点已存在 retester agent（%d 行）：隔离失败", n)
	}
	if err := d.QueryRowContext(ctx, "SELECT count(*) FROM settings WHERE key_hash = ? OR `key` = ?", wantFlagHash, FindingRetesterSeedFlag).Scan(&n); err != nil {
		t.Fatalf("起点检查 settings: %v", err)
	}
	if n != 0 {
		t.Fatalf("隔离库起点已存在 seed flag（%d 行）：隔离失败", n)
	}
	f.before = f.snapshot(t)
	return f
}

func (f *retesterSeedFixture) snapshot(t *testing.T) retesterSeedRowSet {
	t.Helper()
	set := retesterSeedRowSet{agents: map[int64]string{}, prompts: map[int64]int64{},
		settings: map[int64]string{}, maintenance: map[int64]string{}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scan := func(q string, fn func(id int64, extra string)) {
		rows, err := f.d.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("快照 %s: %v", q, err)
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var extra string
			if err := rows.Scan(&id, &extra); err != nil {
				t.Fatalf("快照扫描: %v", err)
			}
			fn(id, extra)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("快照游标: %v", err)
		}
	}
	scanInt := func(q string, fn func(id, extra int64)) {
		rows, err := f.d.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("快照 %s: %v", q, err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, extra int64
			if err := rows.Scan(&id, &extra); err != nil {
				t.Fatalf("快照扫描: %v", err)
			}
			fn(id, extra)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("快照游标: %v", err)
		}
	}
	scan("SELECT id, `key` FROM agents", func(id int64, extra string) { set.agents[id] = extra })
	scanInt("SELECT id, COALESCE(agent_id, -1) FROM agent_prompts", func(id, agentID int64) {
		set.prompts[id] = agentID
	})
	scan("SELECT row_id, `key` FROM settings", func(id int64, extra string) { set.settings[id] = extra })
	scan("SELECT row_id, singleton_key FROM maintenance", func(id int64, extra string) { set.maintenance[id] = extra })
	return set
}

// cleanup 只做两件事：按物理主键删除"本轮新增"的行（依赖生产连接 f.d），然后逆序
// 删除本轮建的表（只依赖探针连接 f.raw）。before 里已经存在的行一条都不碰（并复核它们确实还在）。
//
// I6：行级清理需要 f.d；但 setup 若在 Open 之前失败（65 张表 DDL 中途、PrepareInfraSQL、
// EnsureLockInfrastructure），f.d 仍是 nil，此时却可能已经建好并登记了若干表。表级清理
// dropOwnedTables 只依赖 f.raw，**绝不能再被 f.d==nil 挡在门外**，否则已登记表残留，
// 破坏"测试后空库回到 0 表"的终态判据，下一次运行还会被"对象数必须为 0"守卫挡住、无法自愈。
// 因此这里只把行级清理（含 before 复核）收进 f.d != nil，表级清理无条件执行。
func (f *retesterSeedFixture) cleanup(t *testing.T) {
	t.Helper()
	if f.d != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		now := f.snapshot(t)

		for id := range now.agents {
			if _, existed := f.before.agents[id]; existed {
				continue
			}
			for _, q := range []string{
				`DELETE FROM agent_prompt_vars WHERE agent_id = ?`,
				`DELETE FROM agent_skill_visibility WHERE agent_id = ?`,
				`DELETE FROM agent_visibility WHERE agent_id = ?`,
				`DELETE FROM agent_prompts WHERE agent_id = ?`,
			} {
				if _, err := f.d.ExecContext(ctx, q, id); err != nil {
					t.Errorf("清理 agent %d 的子表: %v", id, err)
				}
			}
			if _, err := f.d.ExecContext(ctx, `DELETE FROM agents WHERE id = ?`, id); err != nil {
				t.Errorf("清理 agent %d: %v", id, err)
			}
		}
		for id := range now.settings {
			if _, existed := f.before.settings[id]; existed {
				continue
			}
			if _, err := f.d.ExecContext(ctx, `DELETE FROM settings WHERE row_id = ?`, id); err != nil {
				t.Errorf("清理 settings row_id=%d: %v", id, err)
			}
		}
		for id := range now.maintenance {
			if _, existed := f.before.maintenance[id]; existed {
				continue
			}
			if _, err := f.d.ExecContext(ctx, `DELETE FROM maintenance WHERE row_id = ?`, id); err != nil {
				t.Errorf("清理 maintenance row_id=%d: %v", id, err)
			}
		}

		// before 的行必须一条不少（C1：原数据完整保护）。
		after := f.snapshot(t)
		for table, want := range map[string]map[int64]string{
			"agents":      f.before.agents,
			"settings":    f.before.settings,
			"maintenance": f.before.maintenance,
		} {
			var have map[int64]string
			switch table {
			case "agents":
				have = after.agents
			case "settings":
				have = after.settings
			default:
				have = after.maintenance
			}
			for id, key := range want {
				if got, ok := have[id]; !ok || got != key {
					t.Errorf("fixture 越界改动了既有 %s 行 row_id/id=%d（%q → %v）", table, id, key, got)
				}
			}
		}
	}

	// 表级清理无条件执行：只依赖 f.raw，不依赖 f.d（I6）。
	f.dropOwnedTables(t)
}

// dropOwnedTables 只删除本轮 CREATE TABLE 成功的表（逆序），最后断言空库回到 0 表。
func (f *retesterSeedFixture) dropOwnedTables(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	f.mu.Lock()
	owned := append([]string(nil), f.owned...)
	f.mu.Unlock()
	for i := len(owned) - 1; i >= 0; i-- {
		name := owned[i]
		if _, err := f.raw.ExecContext(ctx, "DROP TABLE `"+name+"`"); err != nil {
			if !strings.Contains(err.Error(), "timeout") {
				// TiDB 的 DDL 队列是串行的：一条被未收束的事务卡住，后面全部排队超时。
				t.Errorf("DROP 本轮建的表 %q: %v", name, err)
			} else {
				t.Errorf("DROP 本轮建的表 %q: %v（检查是否有未收束的 worker 事务）", name, err)
			}
			continue
		}
		f.mu.Lock()
		for j, n := range f.owned {
			if n == name {
				f.owned = append(f.owned[:j], f.owned[j+1:]...)
				break
			}
		}
		f.mu.Unlock()
	}
	var left int
	if err := f.raw.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'").Scan(&left); err != nil {
		t.Errorf("终态检查 BASE TABLE 数: %v", err)
		return
	}
	if left != 0 {
		t.Errorf("终态判据失败：隔离库仍残留 %d 张表（必须恰好回到 0）", left)
	}
}

// close 的顺序：先按物理主键清行 + 只删本轮建的表（还开着连接），
// 然后关生产连接，最后关探针连接。
func (f *retesterSeedFixture) close(t *testing.T) {
	t.Helper()
	f.cleanup(t)
	if f.d != nil {
		if err := f.d.Close(); err != nil {
			t.Errorf("关闭生产连接: %v", err)
		}
	}
	if err := f.raw.Close(); err != nil {
		t.Errorf("关闭探针连接: %v", err)
	}
}

// —— 调度屏障 ————————————————————————————————————————————————

// retesterSeedBarrier 把生产代码里的 (stage, worker) 阶段变成可等待 / 可放行的屏障。
//
// 它替代固定 sleep：所有等待都是事件驱动的。语义：
//   - 测试**必须先 Expect(stage, worker) 登记挂点**，再启动被测 goroutine；
//     登记过的 (stage, worker) 到达时挂起，等到 Release；
//   - 未登记的 (stage, worker) 直接放行 —— 同一个钩子对同一次调度里其它阶段零成本，
//     也不会把没打算观察的阶段挂死；
//   - 放行是**幂等且可提前登记**的：先 Release 后到达的 worker 也立刻通过（不会漏放行）。
type retesterSeedBarrier struct {
	mu        sync.Mutex
	slots     map[string]*retesterSeedSlot
	free      map[string]bool
	freeStage map[string]bool
	arrivals  map[string]chan string // stage -> 到达过的 worker（缓冲，便于 WaitAny）
}

type retesterSeedSlot struct {
	arrived    chan struct{}
	release    chan struct{}
	released   bool
	arrivedOne sync.Once
	releaseOne sync.Once
}

func retesterSeedInstallBarrier(t *testing.T) *retesterSeedBarrier {
	t.Helper()
	b := &retesterSeedBarrier{
		slots:     map[string]*retesterSeedSlot{},
		free:      map[string]bool{},
		freeStage: map[string]bool{},
		arrivals:  map[string]chan string{},
	}
	prev := retesterSeedTestHook
	retesterSeedTestHook = b.enter
	t.Cleanup(func() { retesterSeedTestHook = prev })
	return b
}

func (b *retesterSeedBarrier) enter(stage, worker string) {
	key := worker + "\x00" + stage
	b.mu.Lock()
	if b.free[key] || b.freeStage[stage] {
		b.mu.Unlock()
		return
	}
	slot, ok := b.slots[key]
	if !ok {
		// 没有登记过的阶段不挂起：一次 seed 会经过多个阶段，只挂测试观察的那一个。
		b.mu.Unlock()
		return
	}
	ch := b.arrivals[stage]
	if ch == nil {
		ch = make(chan string, 64)
		b.arrivals[stage] = ch
	}
	b.mu.Unlock()

	slot.arrivedOne.Do(func() { close(slot.arrived) })
	select {
	case ch <- worker:
	default:
	}
	<-slot.release
}

// Expect 预先登记一个挂点（必须在启动被测 goroutine 之前调用）。
func (b *retesterSeedBarrier) Expect(stage, worker string) {
	b.slotFor(worker + "\x00" + stage)
}

// Wait 等到指定 worker 到达指定阶段（到达后它仍挂起，直到 Release）。
func (b *retesterSeedBarrier) Wait(t *testing.T, stage, worker string) {
	t.Helper()
	slot := b.slotFor(worker+"\x00"+stage)
	select {
	case <-slot.arrived:
	case <-time.After(90 * time.Second):
		t.Fatalf("等待 worker %q 到达阶段 %q 超时（90s）", worker, stage)
	}
}

// WaitAny 等到**任意** worker 到达指定阶段，并返回它的名字。
func (b *retesterSeedBarrier) WaitAny(t *testing.T, stage string) string {
	t.Helper()
	b.mu.Lock()
	ch, ok := b.arrivals[stage]
	if !ok {
		ch = make(chan string, 64)
		b.arrivals[stage] = ch
	}
	b.mu.Unlock()
	select {
	case w := <-ch:
		return w
	case <-time.After(90 * time.Second):
		t.Fatalf("等待任意 worker 到达阶段 %q 超时（90s）", stage)
	}
	return ""
}

func (b *retesterSeedBarrier) slotFor(key string) *retesterSeedSlot {
	b.mu.Lock()
	defer b.mu.Unlock()
	slot, ok := b.slots[key]
	if !ok {
		slot = &retesterSeedSlot{arrived: make(chan struct{}), release: make(chan struct{})}
		b.slots[key] = slot
	}
	return slot
}

// Release 放行 (stage, worker)；worker 尚未到达时也被登记为已放行。
func (b *retesterSeedBarrier) Release(stage, worker string) {
	key := worker + "\x00" + stage
	slot := b.slotFor(key)
	b.mu.Lock()
	b.free[key] = true
	b.mu.Unlock()
	slot.releaseOne.Do(func() { close(slot.release) })
}

// ReleaseAll 放行全部阶段：用例失败（t.Fatalf 直接 Goexit）时必须先解开所有挂起的
// worker，否则它们会带着锚点 / Gate 行锁活过 fixture 清理阶段，把 DDL 拖死。
func (b *retesterSeedBarrier) ReleaseAll() {
	b.mu.Lock()
	slots := make([]*retesterSeedSlot, 0, len(b.slots))
	for k, slot := range b.slots {
		_ = k
		b.free[k] = true
		slots = append(slots, slot)
	}
	for stage := range b.arrivals {
		b.freeStage[stage] = true
	}
	b.mu.Unlock()
	for _, slot := range slots {
		slot.releaseOne.Do(func() { close(slot.release) })
	}
}

// ReleaseStages 放行某阶段上的**所有** worker（含之后才到达的）。
func (b *retesterSeedBarrier) ReleaseStages(stage string) {
	b.mu.Lock()
	b.freeStage[stage] = true
	suffix := "\x00" + stage
	slots := make([]*retesterSeedSlot, 0, len(b.slots))
	for k, slot := range b.slots {
		if strings.HasSuffix(k, suffix) {
			slots = append(slots, slot)
		}
	}
	b.mu.Unlock()
	for _, slot := range slots {
		slot.releaseOne.Do(func() { close(slot.release) })
	}
}
// retesterSeedAssertWriteConflictHeld 在**独立会话**上重放一条与持有者**写写冲突**的
// INSERT，它必须因锁等待超时报 1205。
//
// 为什么用"写冲突探针"而不是"再 FOR UPDATE 一次"：在本集群（TiDB 8.5.8，
// @@tidb_txn_mode=pessimistic）实测得到的锁矩阵是 ——
//
//	持有者未提交             另一会话的语句            结果
//	SELECT … FOR UPDATE     SELECT … FOR UPDATE      不等待（2ms 返回）
//	SELECT … FOR UPDATE     UPDATE 同一行            等 → 1205
//	SELECT … FOR UPDATE     INSERT 同一主键          等 → 1205
//	UPDATE 一行             SELECT … FOR UPDATE      不等待
//	INSERT 新行（未提交）    UPDATE 该新行            不等待
//	INSERT 新行（未提交）    INSERT 同一唯一键        等 → 1205
//
// 也就是说 TiDB 的悲观锁**只在写写冲突时互斥**（且没有 gap lock）；"两个会话互相 FOR
// UPDATE"根本探不出活锁。真正的互斥点是**唯一键上的写入**：seed 的锚点 INSERT IGNORE、
// settings/agents 的 canonical hash 唯一键，全都是这个机制 —— 输家等到赢家提交，然后拿到
// 1062，再走身份仲裁。探针因此必须用 INSERT。
//
// 探针只在自己的连接上 SET SESSION innodb_lock_wait_timeout（会话级、用完即关），
// 不改任何全局配置；它期望被锁挡住，正常情况下**不会**写入任何数据。
func retesterSeedAssertWriteConflictHeld(t *testing.T, dsn, label, stmt string, args ...any) {
	t.Helper()
	probe, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("打开探针连接: %v", err)
	}
	defer probe.Close()
	probe.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := probe.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 5"); err != nil {
		t.Fatalf("设置探针会话锁等待超时: %v", err)
	}
	start := time.Now()
	_, err = probe.ExecContext(ctx, stmt, args...)
	cost := time.Since(start).Round(time.Millisecond)
	if err == nil {
		t.Fatalf("%s 探针竟然写入成功：没有任何事务锁住这一行，并发重叠没有建立", label)
	}
	if strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("%s 探针立刻拿到 1062（%v）而不是锁等待：说明持有者的锁不在这一行上（%v）", label, cost, err)
	}
	if msg := strings.ToLower(err.Error()); !strings.Contains(msg, "lock wait timeout") {
		t.Fatalf("%s 探针未因锁等待超时失败（耗时 %v），实际错误: %v", label, cost, err)
	}
}

// —— 真机查询 helper ————————————————————————————————————————

type retesterSeedCounts struct {
	agents  int
	prompts int
	current sql.NullInt64
	flag    sql.NullString
	orphans int
	anchors int
}

func (f *retesterSeedFixture) counts(t *testing.T) retesterSeedCounts {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var c retesterSeedCounts
	var agentID sql.NullInt64
	// 按**自然键**数：注入的碰撞行 key='other' 但 key_hash 与本 seed 相同，
	// 按 hash 数会把"别人的行"误算成 seed 产物。
	if err := f.d.QueryRowContext(ctx, "SELECT id FROM agents WHERE `key` = ?", FindingRetestAgentKey).Scan(&agentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("读 retester agent: %v", err)
	}
	if err := f.d.QueryRowContext(ctx, "SELECT count(*) FROM agents WHERE `key` = ?", FindingRetestAgentKey).Scan(&c.agents); err != nil {
		t.Fatalf("数 agent: %v", err)
	}
	if agentID.Valid {
		if err := f.d.QueryRowContext(ctx, "SELECT count(*) FROM agent_prompts WHERE agent_id = ?", agentID.Int64).Scan(&c.prompts); err != nil {
			t.Fatalf("数 prompt: %v", err)
		}
		if err := f.d.QueryRowContext(ctx, "SELECT current_prompt_id FROM agents WHERE id = ?", agentID.Int64).Scan(&c.current); err != nil {
			t.Fatalf("读 current_prompt_id: %v", err)
		}
	}
	if err := f.d.QueryRowContext(ctx, "SELECT count(*) FROM agent_prompts p LEFT JOIN agents a ON a.id = p.agent_id WHERE a.id IS NULL").Scan(&c.orphans); err != nil {
		t.Fatalf("数 prompt 孤儿: %v", err)
	}
	if err := f.d.QueryRowContext(ctx, "SELECT `value` FROM settings WHERE `key` = ?", FindingRetesterSeedFlag).Scan(&c.flag); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("读 seed flag: %v", err)
	}
	if err := f.d.QueryRowContext(ctx, "SELECT count(*) FROM maintenance WHERE singleton_key = ?", retesterSeedLockSingletonKey).Scan(&c.anchors); err != nil {
		t.Fatalf("数锚点: %v", err)
	}
	return c
}

// assertCurrentPromptPointsToSeedAgent（M1）严格校验 current_prompt_id 指针的**目标**：
// 不仅要求非空，还必须回查该 id 对应的 agent_prompts 行确实归属于本 seed 的 agent
// （按自然键 FindingRetestAgentKey 定位）。仅断言 Valid 会让"指针指向不存在的 id 或
// 别的 prompt"蒙混过关，所以 ColdAnchorOverlap 与 ConcurrentEntry 共用本判据，消除两条
// 并发路径断言严密性的差异。
func (f *retesterSeedFixture) assertCurrentPromptPointsToSeedAgent(t *testing.T, c retesterSeedCounts) {
	t.Helper()
	if !c.current.Valid {
		t.Fatal("current_prompt_id 为空：agent 没有指向任何模板")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var pointerAgent int64
	if err := f.d.QueryRowContext(ctx,
		"SELECT agent_id FROM agent_prompts WHERE id = ?", c.current.Int64).Scan(&pointerAgent); err != nil {
		t.Fatalf("读 current prompt(id=%d) 归属: %v", c.current.Int64, err)
	}
	var seedAgent int64
	if err := f.d.QueryRowContext(ctx,
		"SELECT id FROM agents WHERE `key` = ?", FindingRetestAgentKey).Scan(&seedAgent); err != nil {
		t.Fatalf("读 seed agent id: %v", err)
	}
	if pointerAgent != seedAgent {
		t.Fatalf("current prompt(id=%d) 指向 agent %d，seed agent 是 %d（指针未指向本 seed agent 的模板）",
			c.current.Int64, pointerAgent, seedAgent)
	}
}

// —— 真机用例 ————————————————————————————————————————————————

// TestSeedFindingRetesterRealTiDBIdempotency 在真实 TiDB 8.5 上验收三条不得回退的
// 语义，以及 hash 身份列、current_prompt_id 指针、锚点行数与孤儿判据。
func TestSeedFindingRetesterRealTiDBIdempotency(t *testing.T) {
	f := retesterSeedIsolatedSchema(t)
	ctx := context.Background()
	d := f.d
	const prompt = "第一行\n\t第二行带引号\"x\""

	if err := d.SeedFindingRetester(ctx, prompt); err != nil {
		t.Fatalf("首次 seed: %v", err)
	}
	c := f.counts(t)
	if c.agents != 1 {
		t.Fatalf("seed 后 agent 行数 = %d, want 恰好 1", c.agents)
	}
	if c.prompts != 1 {
		t.Fatalf("seed 后 prompt 行数 = %d, want 恰好 1", c.prompts)
	}
	if c.orphans != 0 {
		t.Fatalf("孤儿 prompt = %d, want 0", c.orphans)
	}
	if c.flag.String != "true" {
		t.Fatalf("seed flag = %q, want true", c.flag.String)
	}
	if c.anchors != 1 {
		t.Fatalf("maintenance 锚点行数 = %d, want 1", c.anchors)
	}
	if !c.current.Valid || c.current.Int64 < 1 {
		t.Fatalf("current_prompt_id = %v, want 指向真实 prompt 行", c.current)
	}

	var agentID, promptAgentID int64
	var version int
	var text, note, updatedBy string
	if err := d.QueryRowContext(ctx,
		"SELECT agent_id, version, template_text, COALESCE(note,''), COALESCE(updated_by,'') FROM agent_prompts WHERE id = ?",
		c.current.Int64).Scan(&promptAgentID, &version, &text, &note, &updatedBy); err != nil {
		t.Fatalf("读 current prompt: %v", err)
	}
	if err := d.QueryRowContext(ctx, "SELECT id FROM agents WHERE `key` = ?", FindingRetestAgentKey).Scan(&agentID); err != nil {
		t.Fatalf("读 agent id: %v", err)
	}
	if promptAgentID != agentID {
		t.Fatalf("current prompt 指向 agent %d，本 seed 的 agent 是 %d", promptAgentID, agentID)
	}
	if version != 1 || text != prompt {
		t.Fatalf("current prompt = (v%d, %q), want (v1, %q)", version, text, prompt)
	}
	if note != retesterPromptNote || updatedBy != retesterPromptUpdatedBy {
		t.Fatalf("prompt note/updated_by = (%q,%q)", note, updatedBy)
	}

	// hash 唯一键的身份列必须是 canonical 值，且自然键自洽。
	wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
	var storedIdentity, storedHash []byte
	var storedKey string
	if err := d.QueryRowContext(ctx, "SELECT `key`, key_identity, key_hash FROM agents WHERE id = ?", agentID).
		Scan(&storedKey, &storedIdentity, &storedHash); err != nil {
		t.Fatalf("读 agents 身份列: %v", err)
	}
	if storedKey != FindingRetestAgentKey || string(storedIdentity) != string(wantIdentity) || string(storedHash) != string(wantHash) {
		t.Fatalf("agents 身份列不正确：key=%q identity=%x hash=%x", storedKey, storedIdentity, storedHash)
	}
	if got, _ := agentKeyIdentity(storedKey); string(got) != string(storedIdentity) {
		t.Fatal("自然键与持久 identity 不自洽")
	}

	// 用户编辑后再 seed：不覆盖正文、不追加版本、不动 current。
	if _, err := d.SavePrompt(agentID, "用户改过的模板", "手工编辑", "tester"); err != nil {
		t.Fatalf("SavePrompt: %v", err)
	}
	if err := d.SeedFindingRetester(ctx, prompt); err != nil {
		t.Fatalf("编辑后 seed: %v", err)
	}
	afterEdit := f.counts(t)
	if afterEdit.prompts != 2 {
		t.Fatalf("编辑后 prompt 行数 = %d, want 2（seed 不得追加第三个版本）", afterEdit.prompts)
	}
	if afterEdit.current.Int64 != c.current.Int64+1 {
		t.Fatalf("current_prompt_id 被 seed 改回旧版：%d", afterEdit.current.Int64)
	}
	var edited string
	if err := d.QueryRowContext(ctx, "SELECT template_text FROM agent_prompts WHERE id = ?", afterEdit.current.Int64).Scan(&edited); err != nil {
		t.Fatalf("读编辑后的模板: %v", err)
	}
	if edited != "用户改过的模板" {
		t.Fatalf("seed 覆盖了用户编辑的模板: %q", edited)
	}

	// 用户删除（flag 保留）后再 seed：绝不复活。
	if err := d.DeleteAgent(FindingRetestAgentKey); err != nil {
		t.Fatalf("DeleteAgent: %v", err)
	}
	if err := d.SeedFindingRetester(ctx, prompt); err != nil {
		t.Fatalf("删除后 seed: %v", err)
	}
	afterDelete := f.counts(t)
	if afterDelete.agents != 0 || afterDelete.prompts != 0 {
		t.Fatalf("用户删除后被复活：agents=%d prompts=%d", afterDelete.agents, afterDelete.prompts)
	}
	if afterDelete.flag.String != "true" {
		t.Fatalf("删除后 seed flag 丢失: %q", afterDelete.flag.String)
	}

	// 库里已有同自然键的"用户自建" agent（flag 缺失）：原样保留，不追加 prompt，只补 flag。
	if _, err := d.ExecContext(ctx, "DELETE FROM settings WHERE `key` = ?", FindingRetesterSeedFlag); err != nil {
		t.Fatalf("清 flag: %v", err)
	}
	if _, err := d.ExecContext(ctx, `
INSERT INTO agents(`+"`key`"+`, key_identity, key_hash, name, role, builtin, enabled,
                   wrapup_prompt, task_timeout_wrapup_prompt, trigger_run_mode, trigger_merge_mode)
VALUES (?, ?, ?, '用户自己的复测', 'assistant', FALSE, TRUE, '', '', 'serial', 'all')`,
		FindingRetestAgentKey, wantIdentity, wantHash); err != nil {
		t.Fatalf("预置同 key agent: %v", err)
	}
	var userAgentID int64
	if err := d.QueryRowContext(ctx, "SELECT id FROM agents WHERE `key` = ?", FindingRetestAgentKey).Scan(&userAgentID); err != nil {
		t.Fatalf("读预置 agent id: %v", err)
	}
	for _, v := range []int{1, 2} {
		var pid int64
		if _, err := d.ExecContext(ctx, `INSERT INTO agent_prompts(agent_id,version,template_text,note,updated_by)
VALUES (?, ?, ?, '自建', 'tester')`, userAgentID, v, fmt.Sprintf("自建模板 v%d", v)); err != nil {
			t.Fatalf("预置 prompt v%d: %v", v, err)
		}
		if err := d.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&pid); err != nil {
			t.Fatalf("读 prompt id: %v", err)
		}
		if _, err := d.ExecContext(ctx, "UPDATE agents SET current_prompt_id = ? WHERE id = ?", pid, userAgentID); err != nil {
			t.Fatalf("指向自建模板: %v", err)
		}
	}
	before := f.counts(t)
	if err := d.SeedFindingRetester(ctx, prompt); err != nil {
		t.Fatalf("已有同 key agent 时 seed: %v", err)
	}
	kept := f.counts(t)
	if kept.agents != 1 || kept.prompts != 2 {
		t.Fatalf("已有 agent 被追加/复制：agents=%d prompts=%d", kept.agents, kept.prompts)
	}
	if kept.current.Int64 != before.current.Int64 {
		t.Fatalf("current_prompt_id 被改写：%d → %d", before.current.Int64, kept.current.Int64)
	}
	var keptName string
	if err := d.QueryRowContext(ctx, "SELECT name FROM agents WHERE id = ?", userAgentID).Scan(&keptName); err != nil {
		t.Fatalf("读 name: %v", err)
	}
	if keptName != "用户自己的复测" {
		t.Fatalf("既有 agent 的 name 被改写: %q", keptName)
	}
	if kept.flag.String != "true" {
		t.Fatalf("既有 agent 场景下 flag 未补落: %q", kept.flag.String)
	}
}

// TestSeedFindingRetesterRealTiDBDeleteNoResurrect（I1 的确定性调度验收）。
//
// 调度（每一步都由屏障推进，没有 sleep）：
//  1. 赢家 A 走**生产入口**，挂在"锚点已到手、什么都没写"的阶段：它持有 Business Gate
//     与 maintenance 锚点的行锁，事务开着；独立会话探针证明这把锁是活的。
//  2. 输家 B 先在自己的事务里跑一条**普通读**把 RR 快照钉死在 A 提交之前，再进 payload，
//     于是必然卡在 A 持有的锚点上。
//  3. 放行 A → A 写入 agent + prompt + flag=true 并提交。
//  4. B 越过锚点，停在"flag 当前读之前"；此时测试用**生产删除路径**删掉 A 刚建的 agent
//     （flag 保留）。
//  5. 放行 B → B 必须读到 A 刚提交的 true（当前读），直接返回，绝不重建 agent。
//
// 该用例同时跑 RR（生产默认）与 RC 两档隔离级别。旧实现这里的 flag 普通 SELECT 在 RR
// 下读到 A 提交前的"没有 flag"，加上 agent 已被用户删除不再撞 1062，就会复活。
func TestSeedFindingRetesterRealTiDBDeleteNoResurrect(t *testing.T) {
	for _, tc := range []struct {
		name  string
		level sql.IsolationLevel
	}{
		{"repeatable-read（TiDB 默认，生产档位）", sql.LevelDefault},
		{"read-committed", sql.LevelReadCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := retesterSeedIsolatedSchema(t)
			bar := retesterSeedInstallBarrier(t)
			ctx := context.Background()
			bar.Expect(retesterSeedStageAnchorHeld, "A")
			bar.Expect(retesterSeedStageBeforeFlagRead, "B")

			winner := make(chan error, 1)
			loser := make(chan error, 1)
			var loserTx *sql.Tx
			var winnerTaken, loserTaken, loserStarted bool
			// 用例失败时的收束顺序（t.Cleanup 是 LIFO，本条最后注册 ⇒ 最先执行）：
			// 先解开所有挂起点，再等两个 worker 结束并回滚输家事务，最后才让 fixture
			// 去 DROP TABLE —— 否则挂起的 worker 会带着锚点 / Gate 行锁把 DDL 队列卡死。
			t.Cleanup(func() {
				bar.ReleaseAll()
				for _, w := range []struct {
					ch    chan error
					taken *bool
					who   string
				}{{winner, &winnerTaken, "赢家"}, {loser, &loserTaken, "输家"}} {
					if *w.taken || (w.ch == loser && !loserStarted) {
						continue
					}
					select {
					case <-w.ch:
					case <-time.After(3 * time.Minute):
						t.Errorf("%s worker 在放行后 3 分钟内未收束", w.who)
					}
				}
				if loserTx != nil {
					_ = loserTx.Rollback()
				}
			})

			go func() {
				winner <- f.d.SeedFindingRetester(retesterSeedWithWorker(ctx, "A"), "赢家模板")
			}()
			bar.Wait(t, retesterSeedStageAnchorHeld, "A")
			// Gate 行此刻确实被赢家的事务锁着：独立会话插入同一条 locks 记录必须被挡到锁超时。
			retesterSeedAssertWriteConflictHeld(t, f.dsn, "Business Gate",
				"INSERT INTO locks (name) VALUES ('business_relations')")

			// 输家事务：先把快照钉在赢家提交之前，再进 payload（会卡在锚点上）。
			loserCtx, cancelLoser := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancelLoser()
			tx, err := f.d.BeginTx(loserCtx, &sql.TxOptions{Isolation: tc.level})
			if err != nil {
				t.Fatalf("开输家事务: %v", err)
			}
			loserTx = tx
			var pinned int
			if err := tx.QueryRowContext(loserCtx, "SELECT count(*) FROM settings").Scan(&pinned); err != nil {
				t.Fatalf("钉住输家快照: %v", err)
			}
			loserStarted = true
			go func() {
				loser <- f.d.seedFindingRetesterAttempt(retesterSeedWithWorker(loserCtx, "B"), tx, "输家模板")
			}()

			// 放行赢家 → 它写入并提交；输家随后停在 flag 读之前。
			bar.Release(retesterSeedStageAnchorHeld, "A")
			winnerTaken = true
			if err := <-winner; err != nil {
				t.Fatalf("赢家 seed: %v", err)
			}
			bar.Wait(t, retesterSeedStageBeforeFlagRead, "B")
			if got := f.counts(t); got.agents != 1 || got.flag.String != "true" {
				t.Fatalf("赢家应先留下 1 个 agent 与 flag： %+v", got)
			}

			// 用户删除赢家刚创建的 agent（flag 保留）。
			if err := f.d.DeleteAgent(FindingRetestAgentKey); err != nil {
				t.Fatalf("用户删除 agent: %v", err)
			}

			bar.Release(retesterSeedStageBeforeFlagRead, "B")
			loserTaken = true
			if err := <-loser; err != nil {
				t.Fatalf("输家 seed 应因 flag=true 直接返回: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("提交输家事务: %v", err)
			}
			loserTx = nil

			final := f.counts(t)
			if final.agents != 0 {
				t.Errorf("删除不复活被破坏：并发输家重建了 %d 个 agent", final.agents)
			}
			if final.prompts != 0 {
				t.Errorf("删除不复活被破坏：并发输家重建了 %d 个 prompt", final.prompts)
			}
			if final.orphans != 0 {
				t.Errorf("输家留下孤儿 prompt = %d", final.orphans)
			}
			if final.flag.String != "true" {
				t.Errorf("输家把 seed flag 改掉了: %q", final.flag.String)
			}
		})
	}
}

// TestSeedFindingRetesterRealTiDBColdAnchorOverlap（M1）：4 个 worker 在**冷锚点**
// （maintenance 里还没有专属行）上确定性地重叠。
//
// 重叠为什么是确定的（不是"起了 4 个 goroutine"）：
//  1. 4 个 worker 各自先开好**自己的事务**（各自占住一条连接），再进入 payload；
//  2. 每个 worker 都停在锚点锁读之前的阶段，测试逐一确认它们都已进入后一次性放行；
//  3. 第一个拿到锚点的 worker 停在"锚点已到手"，其余 3 个 worker 的事务此刻**已经打开**，
//     它们的下一条语句就是同一个锚点，且必须等赢家提交后才能越过它；因此在赢家提交之前，
//     4 个事务必然同时存活（重叠是构造出来的，不靠 goroutine 抢跑碰运气）。
//     互斥点（真机实测，见 retesterSeedAssertWriteConflictHeld 的锁矩阵）：TiDB 悲观锁只在
//     写写冲突时互斥、且没有 gap lock，所以冷锚点的真正串行点是 uq_maintenance_singleton
//     上的 INSERT IGNORE，而不是锚点的 FOR UPDATE；测试用同键 INSERT 写冲突探针证明这把锁
//     是活的。
//  4. 赢家提交后才放行锚点：其余 worker 只能在赢家提交之后取锁，并必须靠 flag 当前读
//     被挡住。冷锚点上输家可能先拿到 1205/1062（TiDB 在锚点唯一键写入处排队），生产由
//     统一 TxRunner 整体回滚 + 重放；本用例按 IsRetryableConflict 做同样的**有界重放**，
//     重放不改变上面的重叠构造（屏障只作用于第一轮，判据是最终落库结果）。
//
// 判据（全部是"数出来的"，不是 JOIN 隐藏）：恰好 1 个 agent、恰好 1 条 prompt、
// current_prompt_id 指向那条 prompt、孤儿 prompt = 0、锚点行 = 1、flag = true。
func TestSeedFindingRetesterRealTiDBColdAnchorOverlap(t *testing.T) {
	f := retesterSeedIsolatedSchema(t)
	bar := retesterSeedInstallBarrier(t)
	const workers = 4
	const maxRounds = 12

	type outcome struct {
		err    error
		rounds int
	}
	outs := make([]chan outcome, workers)
	done := make([]bool, workers)
	// 失败收束（LIFO）：先解挂 + 收敛 worker，再回滚可能残留的事务。
	t.Cleanup(func() {
		bar.ReleaseAll()
		for i := range outs {
			if outs[i] == nil || done[i] {
				continue
			}
			select {
			case <-outs[i]:
			case <-time.After(3 * time.Minute):
				t.Errorf("worker %d 放行后 3 分钟内未收束", i)
			}
		}
	})

	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("W%d", i)
		bar.Expect(retesterSeedStageBeforeAnchor, worker)
		bar.Expect(retesterSeedStageAnchorHeld, worker)
		outs[i] = make(chan outcome, 1)
		go func(idx int, worker, tmpl string) {
			var last error
			for round := 0; round < maxRounds; round++ {
				// 每轮一个**自己的事务**（各自占住一条连接）；BeginTx 就在 payload 之前，
				// 因此 worker 到达 before-anchor 屏障即证明它的事务已经打开。
				tx, err := f.d.BeginTx(context.Background(), nil)
				if err != nil {
					outs[idx] <- outcome{err: err, rounds: round + 1}
					return
				}
				err = f.d.seedFindingRetesterAttempt(retesterSeedWithWorker(context.Background(), worker), tx, tmpl)
				if err == nil {
					if cerr := tx.Commit(); cerr != nil {
						outs[idx] <- outcome{err: cerr, rounds: round + 1}
						return
					}
					outs[idx] <- outcome{rounds: round + 1}
					return
				}
				_ = tx.Rollback()
				// 冷锚点上输家会在锚点的唯一键写入处拿到 1205/1062；生产里统一 TxRunner
				// 把它整体回滚后重放，这里按同一判据（结构化编号）做有界重放。
				if !lockorder.IsRetryableConflict(err) {
					outs[idx] <- outcome{err: err, rounds: round + 1}
					return
				}
				last = err
				runtime.Gosched()
			}
			outs[idx] <- outcome{err: fmt.Errorf("重放 %d 轮仍冲突: %w", maxRounds, last), rounds: maxRounds}
		}(i, worker, "模板-"+worker)
		// 事务已开、payload 已进入：4 个事务因此必然同时存活。
		bar.Wait(t, retesterSeedStageBeforeAnchor, worker)
	}
	bar.ReleaseStages(retesterSeedStageBeforeAnchor)

	holder := bar.WaitAny(t, retesterSeedStageAnchorHeld)
	// 冷锚点此刻被持有者插着（未提交）：独立会话插入同一个 singleton_key 必须被挡到锁超时
	// —— 这正是其余 worker 卡住的地方，重叠是数据库层面的事实。
	retesterSeedAssertWriteConflictHeld(t, f.dsn, "冷锚点",
		"INSERT INTO maintenance (singleton_key, op_id, mode, revision, updated_at) VALUES (?, NULL, 0, 0, CURRENT_TIMESTAMP(3))",
		retesterSeedLockSingletonKey)
	bar.ReleaseStages(retesterSeedStageAnchorHeld)

	rounds := make([]int, workers)
	for i := 0; i < workers; i++ {
		done[i] = true
		select {
		case o := <-outs[i]:
			if o.err != nil {
				t.Fatalf("worker %d seed 失败（%d 轮）: %v", i, o.rounds, o.err)
			}
			rounds[i] = o.rounds
		case <-time.After(5 * time.Minute):
			t.Fatalf("worker %d 未在 5 分钟内完成", i)
		}
	}

	c := f.counts(t)
	if c.agents != 1 {
		t.Fatalf("4 worker 并发后 agent 行数 = %d, want 恰好 1（重叠锚点失效）", c.agents)
	}
	if c.prompts != 1 {
		t.Fatalf("4 worker 并发后 prompt 行数 = %d, want 恰好 1（不允许 0 条或多条）", c.prompts)
	}
	// M1：current_prompt_id 指针目标断言抽成共用 helper，与 ConcurrentEntry 同一判据。
	f.assertCurrentPromptPointsToSeedAgent(t, c)
	if c.orphans != 0 {
		t.Fatalf("孤儿 prompt = %d, want 0", c.orphans)
	}
	if c.flag.String != "true" {
		t.Fatalf("seed flag = %q, want true", c.flag.String)
	}
	if c.anchors != 1 {
		t.Fatalf("锚点行数 = %d, want 1（uq_maintenance_singleton 必须挡住并发 seed）", c.anchors)
	}
	t.Logf("冷锚点重叠：holder=%s 各 worker 尝试轮数=%v；agents=%d prompts=%d orphans=%d anchors=%d",
		holder, rounds, c.agents, c.prompts, c.orphans, c.anchors)
}

// TestSeedFindingRetesterRealTiDBConcurrentEntry（M1 的生产入口版）：4 个 worker 同时
// 走 SeedFindingRetester 入口（带 Business Gate 的统一 TxRunner）。
// 判据同上：恰好 1 agent、恰好 1 prompt、current 指对、孤儿 0。
func TestSeedFindingRetesterRealTiDBConcurrentEntry(t *testing.T) {
	f := retesterSeedIsolatedSchema(t)
	bar := retesterSeedInstallBarrier(t)
	const workers = 4
	ctx := context.Background()
	t.Cleanup(bar.ReleaseAll)

	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("E%d", i)
		bar.Expect(retesterSeedStageEnter, worker)
		go func(worker string) {
			errs <- f.d.SeedFindingRetester(retesterSeedWithWorker(ctx, worker), "入口模板-"+worker)
		}(worker)
		bar.Wait(t, retesterSeedStageEnter, worker)
	}
	// 4 条调用全部在飞之后才放行：此刻它们在 Gate/锚点上互相排队，而不是串行启动。
	bar.ReleaseStages(retesterSeedStageEnter)
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("并发入口 seed 失败: %v", err)
		}
	}
	c := f.counts(t)
	if c.agents != 1 || c.prompts != 1 {
		t.Fatalf("并发后 agents=%d prompts=%d, want 恰好 1/1", c.agents, c.prompts)
	}
	if c.orphans != 0 {
		t.Fatalf("并发后孤儿 prompt = %d, want 0", c.orphans)
	}
	// M1：补齐 current_prompt_id 目标断言，与 ColdAnchorOverlap 复用同一 helper，
	// 确保指针确实指向本 seed agent 的模板，而不是只判 Valid。
	f.assertCurrentPromptPointsToSeedAgent(t, c)
	if c.flag.String != "true" || c.anchors != 1 {
		t.Fatalf("flag=%q anchors=%d", c.flag.String, c.anchors)
	}
}

// TestSeedFindingRetesterRealTiDBCollisionsFailClosed（I2/I3 的持久碰撞注入）：
// 在生产 schema 上**真的插入**同 hash 异 identity、自然键不自洽、settings 同 hash 异
// identity、以及"1062 落在别的唯一索引"四类故障行，逐条验收：
//   - 必须 fail closed（错误可归因，不是"成功"也不是无限重放）；
//   - 碰撞行零改动、不相关配置零污染、retester 与 flag 一条都不落库。
func TestSeedFindingRetesterRealTiDBCollisionsFailClosed(t *testing.T) {
	ctx := context.Background()

	// —— 注入 helper ——
	insertAgent := func(t *testing.T, f *retesterSeedFixture, key string, identity, hash []byte, name string) int64 {
		t.Helper()
		if _, err := f.d.ExecContext(ctx, `
INSERT INTO agents(`+"`key`"+`, key_identity, key_hash, name, role, builtin, enabled,
                   wrapup_prompt, task_timeout_wrapup_prompt, trigger_run_mode, trigger_merge_mode)
VALUES (?, ?, ?, ?, 'assistant', FALSE, TRUE, '', '', 'serial', 'all')`,
			key, identity, hash, name); err != nil {
			t.Fatalf("注入 agents 行 key=%q: %v", key, err)
		}
		var id int64
		if err := f.d.QueryRowContext(ctx, "SELECT id FROM agents WHERE `key` = ?", key).Scan(&id); err != nil {
			t.Fatalf("读注入行 id: %v", err)
		}
		return id
	}
	insertSetting := func(t *testing.T, f *retesterSeedFixture, key string, identity, hash []byte, value string) {
		t.Helper()
		if _, err := f.d.ExecContext(ctx, `
INSERT INTO settings(`+"`key`"+`, key_identity, key_hash, `+"`value`"+`, updated_at)
VALUES (?, ?, ?, ?, NOW(3))`, key, identity, hash, value); err != nil {
			t.Fatalf("注入 settings 行 key=%q: %v", key, err)
		}
	}
	agentRow := func(t *testing.T, f *retesterSeedFixture, id int64) (string, []byte, []byte, string) {
		t.Helper()
		var key, name string
		var identity, hash []byte
		if err := f.d.QueryRowContext(ctx,
			"SELECT `key`, key_identity, key_hash, name FROM agents WHERE id = ?", id).
			Scan(&key, &identity, &hash, &name); err != nil {
			t.Fatalf("读 agents 行 %d: %v", id, err)
		}
		return key, identity, hash, name
	}
	settingRow := func(t *testing.T, f *retesterSeedFixture, key string) (string, []byte, []byte, bool) {
		t.Helper()
		var value string
		var identity, hash []byte
		err := f.d.QueryRowContext(ctx,
			"SELECT `value`, key_identity, key_hash FROM settings WHERE `key` = ?", key).
			Scan(&value, &identity, &hash)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, nil, false
		}
		if err != nil {
			t.Fatalf("读 settings 行 %q: %v", key, err)
		}
		return value, identity, hash, true
	}

	// —— 用例 1：agents 同 hash 异 identity（I2 反例）——
	t.Run("agents 同 hash 异 identity", func(t *testing.T) {
		f := retesterSeedIsolatedSchema(t)
		_, retesterHash := agentKeyIdentity(FindingRetestAgentKey)
		otherIdentity, _ := agentKeyIdentity("other")
		decoyID := insertAgent(t, f, "other", otherIdentity, retesterHash, "别人的 agent")
		// 不相关配置：失败路径必须一条都不碰。
		unrelatedIdentity, unrelatedHash := settingsKeyIdentity("unrelated.setting")
		insertSetting(t, f, "unrelated.setting", unrelatedIdentity, unrelatedHash, "keep-me")

		err := f.d.SeedFindingRetester(ctx, "模板")
		if err == nil {
			t.Fatal("同 hash 异 identity 的碰撞被当成成功")
		}
		if !errors.Is(err, ErrRetesterSeedHashCollision) {
			t.Fatalf("错误必须是身份碰撞哨兵，实际: %v", err)
		}
		if errors.Is(err, lockorder.ErrRetryExhausted) {
			t.Fatal("身份碰撞不得被当成可重试冲突反复重放")
		}
		key, identity, hash, name := agentRow(t, f, decoyID)
		if key != "other" || string(identity) != string(otherIdentity) ||
			string(hash) != string(retesterHash) || name != "别人的 agent" {
			t.Fatalf("碰撞行被改动: key=%q name=%q identity=%x hash=%x", key, name, identity, hash)
		}
		c := f.counts(t)
		if c.agents != 0 {
			t.Errorf("碰撞失败后仍插了 retester agent（%d 行）", c.agents)
		}
		if c.flag.Valid {
			t.Errorf("碰撞失败后仍落了 seed flag: %q", c.flag.String)
		}
		if v, _, _, ok := settingRow(t, f, "unrelated.setting"); !ok || v != "keep-me" {
			t.Errorf("不相关 settings 被污染: %q", v)
		}
	})

	// —— 用例 2：identity 与本 seed 相同但自然键不自洽 ——
	t.Run("自然键与持久 identity 不自洽", func(t *testing.T) {
		f := retesterSeedIsolatedSchema(t)
		wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
		decoyID := insertAgent(t, f, "retester-typo", wantIdentity, wantHash, "名字很像的别人")

		err := f.d.SeedFindingRetester(ctx, "模板")
		if err == nil || !errors.Is(err, ErrRetesterSeedHashCollision) {
			t.Fatalf("自然键不自洽必须 fail closed，实际: %v", err)
		}
		key, _, _, _ := agentRow(t, f, decoyID)
		if key != "retester-typo" {
			t.Fatalf("不自洽行被改动: %q", key)
		}
		if c := f.counts(t); c.agents != 0 || c.flag.Valid {
			t.Errorf("不自洽路径仍落了数据: %+v", c)
		}
	})

	// —— 用例 3：settings 同 hash 异 identity（I3 反例：ODKU 会把它改成 true）——
	t.Run("settings 同 hash 异 identity", func(t *testing.T) {
		f := retesterSeedIsolatedSchema(t)
		_, flagHash := settingsKeyIdentity(FindingRetesterSeedFlag)
		otherIdentity, _ := settingsKeyIdentity("other")
		insertSetting(t, f, "other", otherIdentity, flagHash, "do-not-touch")

		err := f.d.SeedFindingRetester(ctx, "模板")
		if err == nil || !errors.Is(err, ErrRetesterSeedHashCollision) {
			t.Fatalf("settings 碰撞必须 fail closed，实际: %v", err)
		}
		v, identity, hash, ok := settingRow(t, f, "other")
		if !ok || v != "do-not-touch" || string(identity) != string(otherIdentity) ||
			string(hash) != string(flagHash) {
			t.Fatalf("无关配置被 ODKU 式改写: value=%q identity=%x hash=%x", v, identity, hash)
		}
		if c := f.counts(t); c.agents != 0 || c.flag.Valid {
			t.Errorf("settings 碰撞路径仍落了 agent/flag: %+v", c)
		}
	})

	// —— 用例 4：1062 落在预期之外的唯一索引 ——
	t.Run("1062 落在其他唯一索引", func(t *testing.T) {
		f := retesterSeedIsolatedSchema(t)
		// 本轮自己的表上加一条复合唯一索引，再放一条与本 seed 完全同值 (builtin, enabled)
		// 的诱饵行：agents INSERT 必然撞在这条**非自然键**索引上。
		if _, err := f.raw.ExecContext(ctx,
			// name/role 都是 NOT NULL TEXT，MySQL/TiDB 要求前缀长度。
			"ALTER TABLE `agents` ADD UNIQUE KEY `uq_agents_seed_probe` (`name`(48), `role`(24))"); err != nil {
			t.Fatalf("注入额外唯一索引: %v", err)
		}
		// 诱饵行占住本 seed 将要写入的 (name, role)：agents INSERT 必然撞在
		// uq_agents_seed_probe 这条**非自然键**索引上。
		decoyIdentity, decoyHash := agentKeyIdentity("decoy")
		insertAgent(t, f, "decoy", decoyIdentity, decoyHash, retesterAgentName)
		if _, err := f.raw.ExecContext(ctx, "UPDATE `agents` SET `role` = ? WHERE `key` = ?", retesterAgentRole, "decoy"); err != nil {
			t.Fatalf("对齐诱饵 role: %v", err)
		}

		err := f.d.SeedFindingRetester(ctx, "模板")
		if err == nil {
			t.Fatal("意外索引上的 1062 被吞掉")
		}
		if !errors.Is(err, ErrRetesterSeedUnexpectedConflict) {
			t.Fatalf("错误必须是意外索引哨兵，实际: %v", err)
		}
		if !strings.Contains(err.Error(), "uq_agents_seed_probe") {
			t.Fatalf("错误里没有索引归因: %v", err)
		}
		if c := f.counts(t); c.agents != 0 || c.prompts != 0 || c.flag.Valid || c.orphans != 0 {
			t.Errorf("意外索引冲突后仍落了数据: %+v", c)
		}
	})
}
