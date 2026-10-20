package db

// P08-04 Task 4.1 —— 通知投递队列的两阶段 CAS 租约状态机与代次 Fencing 保护（M33，闭环 F05）。
//
// 本文件从两个层面钉住同一条契约：
//
//  1. **离线语句级替身（必跑）**：`ndqEngine` 是一个只认识本任务四条语句契约的
//     `notification_deliveries` 状态机替身。它按真实 TiDB 的行为求值 WHERE
//     （含 DATETIME(3) 毫秒舍入）并回报 changed rows，于是「迟到 ACK 影响 0 行」
//     是**求值出来的结果**，不是测试自己写死的常量。任何丢掉
//     `WHERE state = 'sending' AND next_attempt_at = ?` 谓词的实现都会被它
//     直接判红（F05 判红口径）。
//  2. **真机集成（设 ARTEX_TIDB_DSN 时跑）**：计划 Step 1 规定的领取与四类回填
//     完整时序，外加并发 worker 不重复领取。未设 DSN 时明确 SKIP，不计真机 PASS。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/notify"
)

// =====================================================================
// 离线语句级替身：notification_deliveries 租约状态机
// =====================================================================

const (
	ndqChannelID int64 = 7000001
	ndqEventID   int64 = 9000001
	ndqFindingID int64 = 4242
)

// ndqRow 是一行 notification_deliveries 的真实列（对齐 db/schema 基线：
// id/event_id/channel_id/state/attempts/next_attempt_at/last_error/batch_id/
// created_at/sent_at；不存在 PG 时代的 scheduled_at）。
type ndqRow struct {
	id        int64
	eventID   int64
	channelID int64
	state     string
	attempts  int
	next      time.Time
	lastError string
	batchID   *int64
	created   time.Time
	sentAt    *time.Time
}

type ndqChannelRow struct {
	id      int64
	name    string
	kind    string
	enabled bool
	mode    string
}

type ndqCall struct {
	Kind  string // "query" | "exec"
	Query string
	Args  []driver.Value
}

type ndqEngine struct {
	mu        sync.Mutex
	rows      map[int64]*ndqRow
	channels  map[int64]*ndqChannelRow
	calls     []ndqCall
	commits   int
	rollbacks int
}

func newNDQEngine() *ndqEngine {
	return &ndqEngine{
		rows:     map[int64]*ndqRow{},
		channels: map[int64]*ndqChannelRow{},
	}
}

// ndqNow 与 ndqRound 一起模拟 TiDB 的 DATETIME(3)：毫秒精度。
// 生产代码若把带亚毫秒噪声的时间写进 next_attempt_at，落库会被舍入，
// 之后拿原始值当 fencing 令牌就永远匹配不上——替身必须能复现这个坑。
func ndqNow() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func ndqRound(t time.Time) time.Time { return t.UTC().Truncate(time.Millisecond) }

func (e *ndqEngine) seedChannel(id int64, mode string, enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.channels[id] = &ndqChannelRow{id: id, name: "P08-04 替身渠道", kind: "dingtalk", enabled: enabled, mode: mode}
}

func (e *ndqEngine) setChannel(id int64, mode string, enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.channels[id]; c != nil {
		c.mode, c.enabled = mode, enabled
	}
}

func (e *ndqEngine) seedDelivery(id, channelID int64, state string, next time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows[id] = &ndqRow{
		id: id, eventID: ndqEventID, channelID: channelID, state: state,
		next: ndqRound(next), created: ndqNow().Add(-time.Hour),
	}
}

// expireLease 把租约截止时间搬到过去，等价于集成测试里那句手工 UPDATE：
// 模拟 worker 处理过慢、租约已到期。
func (e *ndqEngine) expireLease(id int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r := e.rows[id]; r != nil {
		r.next = ndqNow().Add(-2 * time.Minute)
	}
}

func (e *ndqEngine) snapshot(id int64) ndqRow {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r := e.rows[id]; r != nil {
		return *r
	}
	return ndqRow{}
}

func (e *ndqEngine) txCounts() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commits, e.rollbacks
}

func (e *ndqEngine) record(c ndqCall) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, c)
}

func (e *ndqEngine) statementList() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.calls))
	for _, c := range e.calls {
		out = append(out, c.Kind+": "+c.Query)
	}
	return out
}

// mustFind 取出**最后一条**命中 substr 的语句，取不到直接判红。
func (e *ndqEngine) mustFind(t *testing.T, kind, substr string) ndqCall {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := len(e.calls) - 1; i >= 0; i-- {
		if e.calls[i].Kind == kind && strings.Contains(e.calls[i].Query, substr) {
			return e.calls[i]
		}
	}
	t.Fatalf("未发出 %s 语句 %q；实际语句：\n%s", kind, substr, strings.Join(func() []string {
		out := make([]string, 0, len(e.calls))
		for _, c := range e.calls {
			out = append(out, c.Kind+": "+c.Query)
		}
		return out
	}(), "\n"))
	return ndqCall{}
}

// countMatching 统计命中 substr 的语句条数。
func (e *ndqEngine) countMatching(kind, substr string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int
	for _, c := range e.calls {
		if c.Kind == kind && strings.Contains(c.Query, substr) {
			n++
		}
	}
	return n
}

// 三条语句契约。锚定全句的正则是刻意的：语句形状漂移（换列名、丢谓词、
// 丢 FOR UPDATE）必须让替身拒绝服务，而不是静默返回一个「看起来对」的结果。
var (
	ndqClaimSelectRe = regexp.MustCompile(
		`^SELECT id, channel_id, attempts FROM notification_deliveries ` +
			`WHERE channel_id = \? ` +
			`AND \(\(state = 'pending' AND next_attempt_at <= NOW\(3\)\) OR \(state = 'sending' AND next_attempt_at < NOW\(3\)\)\) ` +
			`ORDER BY id ASC LIMIT \? FOR UPDATE$`)
	ndqClaimUpdateRe = regexp.MustCompile(
		`^UPDATE notification_deliveries SET state = 'sending', next_attempt_at = \?, attempts = attempts \+ 1 ` +
			`WHERE id IN \(\?(?:, \?)*\)$`)
	// 回填语句：SET 段以 state 字面量开头，fence 谓词紧跟 WHERE，id 列表收尾。
	ndqBackfillRe = regexp.MustCompile(
		`^UPDATE notification_deliveries SET state = '(\w+)'(.*?) WHERE state = 'sending' AND next_attempt_at = \? AND id IN \(\?(?:, \?)*\)$`)
	ndqFenceWhereRe = regexp.MustCompile(`WHERE state = 'sending' AND next_attempt_at = \?`)
)

func ndqNorm(q string) string { return strings.Join(strings.Fields(q), " ") }

// validate 是所有语句的公共闸门：TiDB 方言（只认 `?`）、占位符与实参逐位对齐。
func (e *ndqEngine) validate(kind, query string, args []driver.Value) (string, error) {
	n := ndqNorm(query)
	if strings.Contains(n, "$") {
		return "", fmt.Errorf("TiDB 方言清除未完成：%s 语句仍带 PostgreSQL 序号占位符：%s", kind, n)
	}
	if got, want := strings.Count(n, "?"), len(args); got != want {
		return "", fmt.Errorf("%s 语句的 `?` 个数(%d)与实参个数(%d)不符：%s args=%v", kind, got, want, n, args)
	}
	e.record(ndqCall{Kind: kind, Query: n, Args: args})
	return n, nil
}

func (e *ndqEngine) query(query string, args []driver.Value) (driver.Rows, error) {
	n, err := e.validate("query", query, args)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case strings.HasPrefix(n, "SELECT enabled, mode FROM notification_channels WHERE id = ?"):
		return e.gateRows(args)
	case strings.HasPrefix(n, "SELECT id, channel_id, attempts FROM notification_deliveries"):
		return e.claimSelect(n, args)
	case strings.Contains(n, "FROM notification_deliveries d JOIN notification_events e"):
		return e.loadRows(args)
	}
	return nil, fmt.Errorf("ndq: 未预期的查询语句：%s", n)
}

func (e *ndqEngine) exec(query string, args []driver.Value) (driver.Result, error) {
	n, err := e.validate("exec", query, args)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !strings.Contains(n, "notification_deliveries") {
		return nil, fmt.Errorf("ndq: 未预期的写语句：%s", n)
	}
	switch {
	case strings.Contains(n, "SET state = 'sending'"):
		return e.claimCAS(n, args)
	case strings.HasPrefix(n, "UPDATE notification_deliveries SET state = '"):
		// F05 判红：任何推进投递状态机的回填都必须带代次 Fencing 谓词。
		if !ndqFenceWhereRe.MatchString(n) {
			return nil, fmt.Errorf("F05 判红：回填 UPDATE 缺少代次 Fencing 谓词 "+
				"`WHERE state = 'sending' AND next_attempt_at = ?`，迟到 ACK 会覆盖新租约：%s", n)
		}
		return e.fencedBackfill(n, args)
	case strings.Contains(n, "SET batch_id = COALESCE(batch_id, ?)"):
		return e.stampBatchID(n, args)
	}
	return nil, fmt.Errorf("ndq: 未预期的投递写语句：%s", n)
}

// gateRows 回答「这个渠道现在能不能领」：启用 + 模式匹配都在 Go 侧判定，
// 于是阶段 1 的抢占语句可以保持单表 FOR UPDATE。
func (e *ndqEngine) gateRows(args []driver.Value) (driver.Rows, error) {
	id, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("ndq: 渠道闸门 id 实参必须是 int64，实际 %T", args[0])
	}
	c, exists := e.channels[id]
	if !exists {
		return &ndqRows{cols: []string{"enabled", "mode"}}, nil
	}
	return &ndqRows{cols: []string{"enabled", "mode"}, rows: [][]driver.Value{{c.enabled, c.mode}}}, nil
}

// claimSelect 是阶段 1：单表有界候选锁定 + NOW(3) 两支到期谓词。
func (e *ndqEngine) claimSelect(n string, args []driver.Value) (driver.Rows, error) {
	if strings.Contains(n, "SKIP LOCKED") {
		return nil, fmt.Errorf("F05 判红：阶段 1 CAS 抢占仍带 SKIP LOCKED：%s", n)
	}
	if !strings.Contains(n, "FOR UPDATE") {
		return nil, fmt.Errorf("阶段 1 CAS 抢占必须用 FOR UPDATE 锁定候选行：%s", n)
	}
	if !ndqClaimSelectRe.MatchString(n) {
		return nil, fmt.Errorf("阶段 1 CAS 抢占语句偏离契约"+
			"（要求：单表 SELECT id, channel_id, attempts + channel_id = ? + "+
			"(state = 'pending' AND next_attempt_at <= NOW(3)) OR (state = 'sending' AND next_attempt_at < NOW(3)) + "+
			"ORDER BY id ASC + LIMIT ? + FOR UPDATE）：%s", n)
	}
	channelID, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("ndq: 渠道 id 实参必须是 int64，实际 %T", args[0])
	}
	limit, ok := args[1].(int64)
	if !ok {
		return nil, fmt.Errorf("ndq: LIMIT 实参必须是 int64，实际 %T", args[1])
	}
	now := ndqNow()
	out := [][]driver.Value{}
	for _, id := range e.sortedIDsLocked() {
		r := e.rows[id]
		if r.channelID != channelID {
			continue
		}
		due := (r.state == NotifyStatePending && !r.next.After(now)) ||
			(r.state == NotifyStateSending && r.next.Before(now))
		if !due {
			continue
		}
		out = append(out, []driver.Value{r.id, r.channelID, int64(r.attempts)})
		if int64(len(out)) >= limit {
			break
		}
	}
	return &ndqRows{cols: []string{"id", "channel_id", "attempts"}, rows: out}, nil
}

// claimCAS 是阶段 2：写死本次分配的租约截止时间，作为后续回填的代次令牌。
func (e *ndqEngine) claimCAS(n string, args []driver.Value) (driver.Result, error) {
	if !ndqClaimUpdateRe.MatchString(n) {
		return nil, fmt.Errorf("阶段 2 CAS 抢占更新偏离契约"+
			"（要求：SET state = 'sending', next_attempt_at = ?, attempts = attempts + 1 WHERE id IN (...)）：%s", n)
	}
	deadline, ok := args[0].(time.Time)
	if !ok {
		return nil, fmt.Errorf("阶段 2 的第一个实参必须是 Go 侧算好的租约截止时间，实际 %T", args[0])
	}
	var changed int64
	for _, a := range args[1:] {
		id, ok := a.(int64)
		if !ok {
			return nil, fmt.Errorf("ndq: id 实参必须是 int64，实际 %T", a)
		}
		r, exists := e.rows[id]
		if !exists {
			continue
		}
		r.state = NotifyStateSending
		r.next = ndqRound(deadline)
		r.attempts++
		changed++
	}
	return ndqResult{rows: changed}, nil
}

// fencedBackfill 按真实 TiDB 的 changed rows 语义求值回填：
// 只有「仍在 sending **且**租约截止时间与代次令牌逐位相等」的行才算改动。
func (e *ndqEngine) fencedBackfill(n string, args []driver.Value) (driver.Result, error) {
	m := ndqBackfillRe.FindStringSubmatch(n)
	if m == nil {
		return nil, fmt.Errorf("回填语句偏离契约"+
			"（要求：UPDATE notification_deliveries SET state = '<目标态>' … "+
			"WHERE state = 'sending' AND next_attempt_at = ? AND id IN (...)）：%s", n)
	}
	target, setTail := m[1], m[2]
	setCount := strings.Count(setTail, "?")
	if setCount+1 > len(args) {
		return nil, fmt.Errorf("回填语句 SET 段需要 %d 个实参，实参总数只有 %d：%s", setCount, len(args), n)
	}
	setVals := args[:setCount]
	fence, ok := args[setCount].(time.Time)
	if !ok {
		return nil, fmt.Errorf("代次 fencing 令牌（租约截止时间）必须是 time.Time 实参，实际 %T：%s", args[setCount], n)
	}
	ids := args[setCount+1:]
	now := ndqNow()
	var changed int64
	for _, a := range ids {
		id, ok := a.(int64)
		if !ok {
			return nil, fmt.Errorf("ndq: id 实参必须是 int64，实际 %T", a)
		}
		r, exists := e.rows[id]
		if !exists {
			continue
		}
		// 迟到 ACK 的两道防线：状态已不是 sending，或租约已被新 worker 换成新截止时间。
		if r.state != NotifyStateSending || !r.next.Equal(ndqRound(fence)) {
			continue
		}
		r.state = target
		if strings.Contains(setTail, "sent_at = NOW(3)") {
			sent := now
			r.sentAt = &sent
		}
		if strings.Contains(setTail, "last_error = ''") {
			r.lastError = ""
		}
		for _, v := range setVals {
			switch v := v.(type) {
			case time.Time:
				r.next = ndqRound(v)
			case string:
				r.lastError = v
			}
		}
		if strings.Contains(setTail, "attempts - 1") && r.attempts > 0 {
			r.attempts--
		}
		changed++
	}
	return ndqResult{rows: changed}, nil
}

func (e *ndqEngine) stampBatchID(n string, args []driver.Value) (driver.Result, error) {
	batchID, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("ndq: batch_id 实参必须是 int64，实际 %T", args[0])
	}
	var changed int64
	for _, a := range args[1:] {
		id, ok := a.(int64)
		if !ok {
			return nil, fmt.Errorf("ndq: id 实参必须是 int64，实际 %T", a)
		}
		r, exists := e.rows[id]
		if !exists {
			continue
		}
		if r.batchID != nil {
			continue // COALESCE：批次号一旦写下不再改，重试也保持「这批是一起发的」。
		}
		v := batchID
		r.batchID = &v
		changed++
	}
	return ndqResult{rows: changed}, nil
}

func (e *ndqEngine) sortedIDsLocked() []int64 {
	ids := make([]int64, 0, len(e.rows))
	for id := range e.rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// loadRows 回读领取后的完整投递行（投递 + 事件快照 + 渠道配置），
// 列序必须与 joinedDeliveryQuery 严格一致。
func (e *ndqEngine) loadRows(args []driver.Value) (driver.Rows, error) {
	out := [][]driver.Value{}
	for _, a := range args {
		id, ok := a.(int64)
		if !ok {
			return nil, fmt.Errorf("ndq: id 实参必须是 int64，实际 %T", a)
		}
		r, exists := e.rows[id]
		if !exists {
			continue
		}
		ch := e.channels[r.channelID]
		if ch == nil {
			ch = &ndqChannelRow{id: r.channelID, name: "缺失渠道", kind: "dingtalk", enabled: true, mode: NotifyModeRealtime}
		}
		var batch any
		if r.batchID != nil {
			batch = *r.batchID
		}
		var sentAt any
		if r.sentAt != nil {
			sentAt = *r.sentAt
		}
		out = append(out, []driver.Value{
			r.id, r.eventID, r.channelID, r.state, int64(r.attempts), r.next,
			r.lastError, batch, r.created, sentAt,
			[]byte(`{"severity":"high","kind":"finding_created"}`), "finding_created", ndqFindingID,
			ch.id, ch.name, ch.kind, ch.enabled, []byte(`{"webhook":"http://127.0.0.1:1"}`), ch.mode, []byte(`{}`), int64(20),
		})
	}
	return &ndqRows{cols: []string{
		"id", "event_id", "channel_id", "state", "attempts", "next_attempt_at",
		"last_error", "batch_id", "created_at", "sent_at",
		"snapshot", "kind", "finding_id",
		"c_id", "c_name", "c_kind", "c_enabled", "c_config", "c_mode", "c_filter", "c_rate",
	}, rows: out}, nil
}

// ---------- driver 适配 ----------

type ndqRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *ndqRows) Columns() []string { return r.cols }
func (r *ndqRows) Close() error      { return nil }
func (r *ndqRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

type ndqResult struct{ id, rows int64 }

func (r ndqResult) LastInsertId() (int64, error) { return r.id, nil }
func (r ndqResult) RowsAffected() (int64, error) { return r.rows, nil }

type ndqConn struct{ eng *ndqEngine }

func (c *ndqConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("ndq: 禁用预处理语句，所有 SQL 必须走 Queryer/Execer")
}
func (c *ndqConn) Close() error { return nil }
func (c *ndqConn) Begin() (driver.Tx, error) {
	return ndqTx{eng: c.eng}, nil
}
func (c *ndqConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return ndqTx{eng: c.eng}, nil
}

func ndqValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, 0, len(args))
	for _, a := range args {
		out = append(out, a.Value)
	}
	return out
}

func (c *ndqConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.eng.query(query, ndqValues(args))
}

func (c *ndqConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.eng.exec(query, ndqValues(args))
}

type ndqTx struct{ eng *ndqEngine }

func (t ndqTx) Commit() error {
	t.eng.mu.Lock()
	defer t.eng.mu.Unlock()
	t.eng.commits++
	return nil
}

func (t ndqTx) Rollback() error {
	t.eng.mu.Lock()
	defer t.eng.mu.Unlock()
	t.eng.rollbacks++
	return nil
}

func (t ndqTx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return (&ndqConn{eng: t.eng}).QueryContext(ctx, query, args)
}

func (t ndqTx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return (&ndqConn{eng: t.eng}).ExecContext(ctx, query, args)
}

type ndqDriver struct{}

func (ndqDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("ndq: 请用 OpenDB 构造连接")
}

type ndqConnector struct{ conn *ndqConn }

func (c ndqConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c ndqConnector) Driver() driver.Driver                        { return ndqDriver{} }

// openNDQMockDB 构造一个不落任何真实连接的 *DB。单连接保证语句顺序与事务计数可读。
func openNDQMockDB(t *testing.T, eng *ndqEngine) *DB {
	t.Helper()
	sqlDB := sql.OpenDB(ndqConnector{conn: &ndqConn{eng: eng}})
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return &DB{sqlDB}
}

// =====================================================================
// 离线用例 1：两阶段 CAS 抢占
// =====================================================================

func TestNotification_ClaimTwoPhaseCAS_ClaimsOnlyDueRowsAndStampsLease(t *testing.T) {
	eng := newNDQEngine()
	eng.seedChannel(ndqChannelID, NotifyModeRealtime, true)
	eng.seedDelivery(101, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Minute))
	eng.seedDelivery(102, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Second))
	eng.seedDelivery(103, ndqChannelID, NotifyStatePending, ndqNow().Add(time.Hour))      // 未到重试时间
	eng.seedDelivery(104, ndqChannelID, NotifyStateSent, ndqNow().Add(-time.Hour))        // 终态不参与
	eng.seedDelivery(105, ndqChannelID, NotifyStateSending, ndqNow().Add(30*time.Second)) // 别人租约未到期
	eng.seedDelivery(106, ndqChannelID, NotifyStateSending, ndqNow().Add(-time.Minute))   // 租约过期 → 崩溃自愈
	d := openNDQMockDB(t, eng)
	ctx := context.Background()

	got, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, 90*time.Second)
	if err != nil {
		t.Fatalf("ClaimRealtimeDeliveries failed: %v", err)
	}
	var ids []int64
	for _, dl := range got {
		ids = append(ids, dl.ID)
	}
	if len(ids) != 3 || ids[0] != 101 || ids[1] != 102 || ids[2] != 106 {
		t.Fatalf("阶段 1 只应领到到期 pending 与租约过期 sending（101/102/106），实得 %v", ids)
	}
	for _, dl := range got {
		if dl.State != NotifyStateSending {
			t.Fatalf("领取后状态必须是 sending，得到 %s", dl.State)
		}
		if dl.Attempts != 1 {
			t.Fatalf("领取即计一次尝试，得到 attempts=%d", dl.Attempts)
		}
		if dl.NextAttemptAt.Nanosecond()%int(time.Millisecond) != 0 {
			t.Fatalf("租约截止时间必须对齐 DATETIME(3) 毫秒精度，否则回填的 fencing 等值比较永远落空：%v", dl.NextAttemptAt)
		}
		if !dl.NextAttemptAt.After(time.Now().UTC()) {
			t.Fatalf("租约截止时间必须落在未来，得到 %v", dl.NextAttemptAt)
		}
		if dl.Channel == nil || dl.FindingID != ndqFindingID {
			t.Fatalf("渲染上下文缺失：channel=%v finding_id=%d", dl.Channel, dl.FindingID)
		}
		seen := eng.snapshot(dl.ID)
		if seen.next.UTC().Truncate(time.Millisecond) != dl.NextAttemptAt.UTC().Truncate(time.Millisecond) {
			t.Fatalf("返回行的租约截止时间必须来自落库值：got %v want %v", dl.NextAttemptAt, seen.next)
		}
	}

	// 阶段 1 语句契约：真实表名 + 真实列名 + 两支 NOW(3) 到期谓词 + FOR UPDATE，且无 SKIP LOCKED。
	phase1 := eng.mustFind(t, "query", "FROM notification_deliveries WHERE channel_id = ?")
	for _, want := range []string{
		"SELECT id, channel_id, attempts FROM notification_deliveries",
		"(state = 'pending' AND next_attempt_at <= NOW(3))",
		"(state = 'sending' AND next_attempt_at < NOW(3))",
		"ORDER BY id ASC",
		"LIMIT ?",
		"FOR UPDATE",
	} {
		if !strings.Contains(phase1.Query, want) {
			t.Fatalf("阶段 1 缺少 %q：%s", want, phase1.Query)
		}
	}
	if strings.Contains(phase1.Query, "SKIP LOCKED") {
		t.Fatalf("F05 要求彻底废除 SKIP LOCKED：%s", phase1.Query)
	}
	if strings.Contains(phase1.Query, "scheduled_at") {
		t.Fatalf("真实列名是 next_attempt_at，不存在 scheduled_at：%s", phase1.Query)
	}

	// 阶段 2 语句契约：租约截止时间由 Go 侧算好后作为第一个实参写入。
	phase2 := eng.mustFind(t, "exec", "SET state = 'sending', next_attempt_at = ?, attempts = attempts + 1")
	if len(phase2.Args) != 4 {
		t.Fatalf("阶段 2 实参必须是 [截止时间, 3 个 id]，实得 %v", phase2.Args)
	}
	deadline, ok := phase2.Args[0].(time.Time)
	if !ok {
		t.Fatalf("阶段 2 第一个实参必须是 assignedDeadline，实际 %T", phase2.Args[0])
	}
	if deadline.Nanosecond()%int(time.Millisecond) != 0 {
		// next_attempt_at 是 DATETIME(3)：带亚毫秒噪声写进去会被舍入，
		// 而这份截止时间同时是回填的 fencing 令牌，必须写入侧与比较侧逐位一致。
		t.Fatalf("assignedDeadline 必须已对齐到毫秒（DATETIME(3)），实得 %v（.nanosecond=%d）",
			deadline, deadline.Nanosecond())
	}
	for _, dl := range got {
		if !dl.NextAttemptAt.UTC().Truncate(time.Millisecond).Equal(deadline.UTC().Truncate(time.Millisecond)) {
			t.Fatalf("返回的租约截止时间必须等于阶段 2 写入值：got %v want %v", dl.NextAttemptAt, deadline)
		}
	}
	if commits, rollbacks := eng.txCounts(); commits != 1 || rollbacks != 0 {
		// 提交成功后 deferred 的 tx.Rollback() 是 no-op，不会二次触达驱动层。
		t.Fatalf("一次领取应恰好提交一次（提交即释放行锁），实得 commits=%d rollbacks=%d", commits, rollbacks)
	}
}

func TestNotification_ClaimRealtimeDeliveries_SkipsDisabledAndWrongModeChannel(t *testing.T) {
	eng := newNDQEngine()
	eng.seedChannel(ndqChannelID, NotifyModeRealtime, false) // 已停用
	eng.seedDelivery(111, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Minute))
	d := openNDQMockDB(t, eng)
	ctx := context.Background()

	got, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, time.Minute)
	if err != nil {
		t.Fatalf("停用渠道不应报错，得到 %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("停用渠道不应领到投递，实得 %d 条", len(got))
	}
	if n := eng.countMatching("query", "FROM notification_deliveries WHERE channel_id = ?"); n != 0 {
		t.Fatalf("渠道闸门未通过时不该发出抢占语句（白烧 attempts），实得 %d 条", n)
	}

	// 模式不匹配：realtime 渠道不能被子路领取。
	eng.setChannel(ndqChannelID, NotifyModeDigest, true)
	if got, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("realtime 领取不得碰 digest 渠道，实得 %d 条 err=%v", len(got), err)
	}
	// digest 渠道可被批次领取。
	eng.setChannel(ndqChannelID, NotifyModeDigest, true)
	if got, err := d.ClaimDigestBatch(ctx, ndqChannelID, 10, time.Minute); err != nil || len(got) != 1 {
		t.Fatalf("digest 渠道应领到 1 条，实得 %d 条 err=%v", len(got), err)
	}
}

func TestNotification_ClaimDigestBatch_WritesStableBatchID(t *testing.T) {
	eng := newNDQEngine()
	eng.seedChannel(ndqChannelID, NotifyModeDigest, true)
	for _, id := range []int64{121, 122, 123} {
		eng.seedDelivery(id, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Minute))
	}
	d := openNDQMockDB(t, eng)
	ctx := context.Background()

	batch, err := d.ClaimDigestBatch(ctx, ndqChannelID, MaxDigestBatchSize, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDigestBatch failed: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("应一次领走 3 条，实得 %d", len(batch))
	}
	if batch[0].BatchID == nil || *batch[0].BatchID != 121 {
		t.Fatalf("批次号必须是集合内最小 id，实得 %v", batch[0].BatchID)
	}
	for _, dl := range batch {
		if dl.BatchID == nil || *dl.BatchID != 121 {
			t.Fatalf("同批必须共享批次号，实得 %v", dl.BatchID)
		}
	}

	// 整批重排 + 租约过期后重领：COALESCE 必须保住原批次号。
	deadline := batch[0].NextAttemptAt
	ids := []int64{121, 122, 123}
	if n, err := d.RescheduleDeliveries(ctx, ids, deadline, time.Second, "模拟整批失败"); err != nil || n != 3 {
		t.Fatalf("整批重排应影响 3 行，实得 n=%d err=%v", n, err)
	}
	for _, id := range ids {
		eng.expireLease(id)
	}
	reclaimed, err := d.ClaimDigestBatch(ctx, ndqChannelID, MaxDigestBatchSize, time.Minute)
	if err != nil {
		t.Fatalf("重领失败: %v", err)
	}
	if len(reclaimed) != 3 {
		t.Fatalf("重领应拿到 3 条，实得 %d", len(reclaimed))
	}
	for _, dl := range reclaimed {
		if dl.BatchID == nil || *dl.BatchID != 121 {
			t.Fatalf("重试后批次号必须保持 %d，实得 %v", 121, dl.BatchID)
		}
	}
}

// =====================================================================
// 离线用例 2：四类回填的代次 Fencing（F05 判红口径）
// =====================================================================

// TestNotification_FencingBackfill_OfflineStaleTokenAffectsZeroRows 是 R51-F07-02
// 整改项的离线版：A 领取 → A 租约到期 → B 重领（新截止时间 + attempts 恰好 +1）
// → A 带旧令牌回填 → 四类回填影响行数必须严格为 0，且 B 的租约分毫不动。
func TestNotification_FencingBackfill_OfflineStaleTokenAffectsZeroRows(t *testing.T) {
	eng := newNDQEngine()
	eng.seedChannel(ndqChannelID, NotifyModeRealtime, true)
	eng.seedDelivery(201, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Minute))
	d := openNDQMockDB(t, eng)
	ctx := context.Background()

	claimedA, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, 10*time.Second)
	if err != nil {
		t.Fatalf("worker A 领取失败: %v", err)
	}
	if len(claimedA) != 1 {
		t.Fatalf("worker A 应领到 1 条，实得 %d", len(claimedA))
	}
	deadlineA, attemptsA := claimedA[0].NextAttemptAt, claimedA[0].Attempts

	eng.expireLease(201)

	claimedB, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, 60*time.Second)
	if err != nil {
		t.Fatalf("worker B 重领失败: %v", err)
	}
	if len(claimedB) != 1 {
		t.Fatalf("worker B 必须重领到 201，实得 %d 条", len(claimedB))
	}
	deadlineB, attemptsB := claimedB[0].NextAttemptAt, claimedB[0].Attempts
	if !deadlineB.After(deadlineA) {
		t.Fatalf("重领必须产生更新的截止时间：%v -> %v", deadlineA, deadlineB)
	}
	if attemptsB != attemptsA+1 {
		t.Fatalf("重领必须让 attempts 恰好 +1：%d -> %d", attemptsA, attemptsB)
	}

	staleCalls := []struct {
		name string
		call func() (int64, error)
	}{
		{"MarkDeliveriesSent", func() (int64, error) { return d.MarkDeliveriesSent(ctx, []int64{201}, deadlineA) }},
		{"RescheduleDeliveries", func() (int64, error) {
			return d.RescheduleDeliveries(ctx, []int64{201}, deadlineA, 30*time.Second, "late ack")
		}},
		{"DeferDeliveries", func() (int64, error) { return d.DeferDeliveries(ctx, []int64{201}, deadlineA, "late defer") }},
		{"FailDeliveries", func() (int64, error) { return d.FailDeliveries(ctx, []int64{201}, deadlineA, "late fail") }},
	}
	for _, stale := range staleCalls {
		affected, err := stale.call()
		if err != nil {
			t.Fatalf("%s 带旧令牌回填不该报错（0 行是正常结果）: %v", stale.name, err)
		}
		if affected != 0 {
			t.Fatalf("F05 判红：%s 带旧令牌回填必须影响 0 行，实得 %d", stale.name, affected)
		}
		now := eng.snapshot(201)
		if now.state != NotifyStateSending {
			t.Fatalf("%s 篡改了 worker B 的状态：%s", stale.name, now.state)
		}
		if !now.next.UTC().Equal(deadlineB.UTC()) {
			t.Fatalf("%s 篡改了 worker B 的租约截止时间：%v (want %v)", stale.name, now.next, deadlineB)
		}
		if now.attempts != attemptsB {
			t.Fatalf("%s 篡改了 worker B 的 attempts：%d (want %d)", stale.name, now.attempts, attemptsB)
		}
		if n := eng.countMatching("exec", "AND next_attempt_at = ? AND id IN"); n == 0 {
			t.Fatalf("%s 的 UPDATE 没带 fencing 谓词", stale.name)
		}
	}

	// 正向路径：B 以当前令牌回填必须成功，证明 fencing 没有误杀当前租约。
	affected, err := d.MarkDeliveriesSent(ctx, []int64{201}, deadlineB)
	if err != nil {
		t.Fatalf("worker B 以当前令牌回填失败: %v", err)
	}
	if affected != 1 {
		t.Fatalf("worker B 以当前令牌回填必须影响 1 行，实得 %d", affected)
	}
	if now := eng.snapshot(201); now.state != NotifyStateSent || now.sentAt == nil {
		t.Fatalf("当前租约下应前进为 sent 并写 sent_at，实得 %+v", now)
	}
}

// TestNotification_FencingBackfill_PositiveTransitions 逐一验证四类回填在
// 正确令牌下的真实状态流转（含 Defer 退还领取时乐观 +1 的那一次尝试）。
func TestNotification_FencingBackfill_PositiveTransitions(t *testing.T) {
	ctx := context.Background()

	newClaimed := func(t *testing.T, id int64) (*DB, *ndqEngine, time.Time) {
		t.Helper()
		eng := newNDQEngine()
		eng.seedChannel(ndqChannelID, NotifyModeRealtime, true)
		eng.seedDelivery(id, ndqChannelID, NotifyStatePending, ndqNow().Add(-time.Minute))
		d := openNDQMockDB(t, eng)
		got, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, time.Minute)
		if err != nil || len(got) != 1 {
			t.Fatalf("领取失败: %v (%d 条)", err, len(got))
		}
		return d, eng, got[0].NextAttemptAt
	}

	t.Run("Reschedule 退回 pending 并推后重试", func(t *testing.T) {
		d, eng, deadline := newClaimed(t, 301)
		affected, err := d.RescheduleDeliveries(ctx, []int64{301}, deadline, 5*time.Second, "网络抖动")
		if err != nil || affected != 1 {
			t.Fatalf("正确令牌重排应影响 1 行，实得 n=%d err=%v", affected, err)
		}
		row := eng.snapshot(301)
		if row.state != NotifyStatePending || row.lastError != "网络抖动" {
			t.Fatalf("重排后应 pending 并记录原因，实得 %+v", row)
		}
		if row.attempts != 1 {
			t.Fatalf("重排是失败重试，attempts 必须保留领取时计的那一次，实得 %d", row.attempts)
		}
		if !row.next.After(ndqNow()) {
			t.Fatalf("重排后 next_attempt_at 必须在未来，实得 %v", row.next)
		}
	})

	t.Run("Defer 退回 pending 并退还尝试", func(t *testing.T) {
		d, eng, deadline := newClaimed(t, 302)
		affected, err := d.DeferDeliveries(ctx, []int64{302}, deadline, "本条消息已达渠道长度上限")
		if err != nil || affected != 1 {
			t.Fatalf("正确令牌分段应影响 1 行，实得 n=%d err=%v", affected, err)
		}
		row := eng.snapshot(302)
		if row.state != NotifyStatePending || row.attempts != 0 {
			t.Fatalf("分段续发不是失败，必须退回 pending 且把 attempts 减回 0，实得 %+v", row)
		}
		if row.next.After(ndqNow().Add(time.Second)) {
			t.Fatalf("分段留待下一批应立即可再领，实得 %v", row.next)
		}
	})

	t.Run("Fail 判最终失败", func(t *testing.T) {
		d, eng, deadline := newClaimed(t, 303)
		affected, err := d.FailDeliveries(ctx, []int64{303}, deadline, "重试耗尽")
		if err != nil || affected != 1 {
			t.Fatalf("正确令牌判失败应影响 1 行，实得 n=%d err=%v", affected, err)
		}
		row := eng.snapshot(303)
		if row.state != NotifyStateFailed || row.lastError != "重试耗尽" {
			t.Fatalf("应落 failed 并记录原因，实得 %+v", row)
		}
	})

	t.Run("Sent 清空历史错误", func(t *testing.T) {
		d, eng, deadline := newClaimed(t, 304)
		if _, err := d.RescheduleDeliveries(ctx, []int64{304}, deadline, time.Second, "第一次失败"); err != nil {
			t.Fatalf("重排失败: %v", err)
		}
		eng.expireLease(304) // 退避时间已过，允许再领
		claimed, err := d.ClaimRealtimeDeliveries(ctx, ndqChannelID, 10, time.Minute)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("重排后重领失败: %v (%d 条)", err, len(claimed))
		}
		if row := eng.snapshot(304); row.lastError != "第一次失败" {
			t.Fatalf("前置条件不成立：last_error 应为历史原因，实得 %q", row.lastError)
		}
		affected, err := d.MarkDeliveriesSent(ctx, []int64{304}, claimed[0].NextAttemptAt)
		if err != nil || affected != 1 {
			t.Fatalf("标记已送达应影响 1 行，实得 n=%d err=%v", affected, err)
		}
		if row := eng.snapshot(304); row.state != NotifyStateSent || row.lastError != "" {
			t.Fatalf("送达后应清掉历史错误，实得 %+v", row)
		}
	})
}

// TestNotification_ClaimDeliveries_EmptyQueueCommitsQuietly 覆盖空队列路径：
// 没有候选行时不该发出阶段 2 更新，也不该报错。
func TestNotification_ClaimDeliveries_EmptyQueueCommitsQuietly(t *testing.T) {
	eng := newNDQEngine()
	eng.seedChannel(ndqChannelID, NotifyModeRealtime, true)
	d := openNDQMockDB(t, eng)

	got, err := d.ClaimRealtimeDeliveries(context.Background(), ndqChannelID, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("空队列应返回 0 条且无错，实得 %d 条 err=%v", len(got), err)
	}
	if n := eng.countMatching("exec", "SET state = 'sending'"); n != 0 {
		t.Fatalf("没有候选行时不该发出阶段 2 更新，实得 %d 条", n)
	}
	if got, err := d.ClaimRealtimeDeliveries(context.Background(), ndqChannelID, 0, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("limit<=0 表示本轮额度用尽，应领 0 条且无错，实得 %d 条 err=%v", len(got), err)
	}
}

// =====================================================================
// 真机集成用例（设 ARTEX_TIDB_DSN 才跑；未设明确 SKIP，不计真机 PASS）
// =====================================================================

// ndqIntegrationFixture 建一个测试渠道 + 一条事件，并注册严格清理。
func ndqIntegrationFixture(t *testing.T, d *DB, evFindingID int64) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	ch := &NotificationChannel{
		Name:       fmt.Sprintf("p08_04_ch_%d", time.Now().UnixNano()),
		Kind:       "dingtalk",
		Mode:       NotifyModeRealtime,
		Config:     json.RawMessage(`{"webhook":"http://127.0.0.1:1"}`),
		Filter:     json.RawMessage(`{}`),
		RatePerMin: 100,
	}
	chID, err := d.SaveNotificationChannel(ctx, ch)
	if err != nil {
		t.Fatalf("SaveNotificationChannel failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel_id = ?", chID); err != nil {
			t.Errorf("cleanup channel deliveries failed: %v", err)
		}
		if _, err := d.ExecContext(ctx, "DELETE FROM notification_channels WHERE id = ?", chID); err != nil {
			t.Errorf("cleanup notification_channels failed: %v", err)
		}
		var delCount, chCount int
		if err := d.QueryRowContext(ctx, "SELECT count(*) FROM notification_deliveries WHERE channel_id = ?", chID).Scan(&delCount); err != nil {
			t.Errorf("query residual channel deliveries failed: %v", err)
		}
		if err := d.QueryRowContext(ctx, "SELECT count(*) FROM notification_channels WHERE id = ?", chID).Scan(&chCount); err != nil {
			t.Errorf("query residual channels failed: %v", err)
		}
		if delCount != 0 || chCount != 0 {
			t.Errorf("residual channel fixtures found: deliveries=%d channels=%d", delCount, chCount)
		}
	})

	evID, err := d.AddNotificationEvent(ctx, notify.EventFindingCreated, evFindingID,
		notify.Snapshot{Severity: "high", VulnClass: "XSS"})
	if err != nil {
		t.Fatalf("AddNotificationEvent failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE event_id = ?", evID); err != nil {
			t.Errorf("cleanup event deliveries failed: %v", err)
		}
		if _, err := d.ExecContext(ctx, "DELETE FROM notification_events WHERE id = ?", evID); err != nil {
			t.Errorf("cleanup notification_events failed: %v", err)
		}
		var delCount, eventCount int
		if err := d.QueryRowContext(ctx, "SELECT count(*) FROM notification_deliveries WHERE event_id = ?", evID).Scan(&delCount); err != nil {
			t.Errorf("query residual event deliveries failed: %v", err)
		}
		if err := d.QueryRowContext(ctx, "SELECT count(*) FROM notification_events WHERE id = ?", evID).Scan(&eventCount); err != nil {
			t.Errorf("query residual events failed: %v", err)
		}
		if delCount != 0 || eventCount != 0 {
			t.Errorf("residual event fixtures found: deliveries=%d events=%d", delCount, eventCount)
		}
	})
	return chID, evID
}

// TestNotification_ClaimRealtimeDeliveries_TiDBDialect 真机跑通「事件 → fan-out →
// 两阶段 CAS 领取」整条 TiDB 语句路径（计划 Step 1 测试①）。
func TestNotification_ClaimRealtimeDeliveries_TiDBDialect(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	chID, evID := ndqIntegrationFixture(t, d, 1001)

	if _, _, err := d.FanOutPendingEvents(ctx, 10); err != nil {
		t.Fatalf("FanOutPendingEvents failed: %v", err)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	deliveries, err := d.ClaimRealtimeDeliveries(ctxTimeout, chID, 10, 10*time.Second)
	if err != nil {
		t.Fatalf("ClaimRealtimeDeliveries failed: %v", err)
	}
	if len(deliveries) == 0 {
		t.Fatalf("expected at least 1 claimed delivery for channel %d", chID)
	}
	var matched bool
	for _, dl := range deliveries {
		if dl.EventID == evID && dl.ChannelID == chID && dl.State == NotifyStateSending {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("expected claimed delivery matching event %d and channel %d with state 'sending', got %+v", evID, chID, deliveries)
	}
}

// TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods 真机版
// 四类回填 Fencing（计划 Step 1 测试②）。
func TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	chID, evID := ndqIntegrationFixture(t, d, 2002)

	if _, _, err := d.FanOutPendingEvents(ctx, 10); err != nil {
		t.Fatalf("FanOutPendingEvents failed: %v", err)
	}

	claimedA, err := d.ClaimRealtimeDeliveries(ctx, chID, 10, 10*time.Second)
	if err != nil {
		t.Fatalf("worker A ClaimRealtimeDeliveries failed: %v", err)
	}
	var targetA *NotificationDelivery
	for _, dl := range claimedA {
		if dl.EventID == evID && dl.ChannelID == chID && dl.State == NotifyStateSending {
			targetA = dl
			break
		}
	}
	if targetA == nil {
		t.Fatalf("worker A must claim the sending delivery for event %d", evID)
	}
	deadlineA, attemptsA := targetA.NextAttemptAt, targetA.Attempts

	// A 的租约到期：把截止时间挪到过去，模拟 worker A 处理过慢。
	if _, err := d.ExecContext(ctx, "UPDATE notification_deliveries SET next_attempt_at = ? WHERE id = ?",
		time.Now().UTC().Add(-2*time.Minute).Truncate(time.Millisecond), targetA.ID); err != nil {
		t.Fatalf("expire worker A lease failed: %v", err)
	}

	claimedB, err := d.ClaimRealtimeDeliveries(ctx, chID, 10, 60*time.Second)
	if err != nil {
		t.Fatalf("worker B ClaimRealtimeDeliveries failed: %v", err)
	}
	var targetB *NotificationDelivery
	for _, dl := range claimedB {
		if dl.ID == targetA.ID {
			targetB = dl
			break
		}
	}
	if targetB == nil {
		t.Fatalf("worker B must reclaim delivery %d after expiry", targetA.ID)
	}
	deadlineB, attemptsB := targetB.NextAttemptAt, targetB.Attempts
	if !deadlineB.After(deadlineA) || attemptsB != attemptsA+1 {
		t.Fatalf("reclaim must issue a newer deadline and increment attempts exactly once: deadline %v -> %v, attempts %d -> %d",
			deadlineA, deadlineB, attemptsA, attemptsB)
	}

	lateCalls := []struct {
		name string
		call func() (int64, error)
	}{
		{"MarkDeliveriesSent", func() (int64, error) { return d.MarkDeliveriesSent(ctx, []int64{targetA.ID}, deadlineA) }},
		{"RescheduleDeliveries", func() (int64, error) {
			return d.RescheduleDeliveries(ctx, []int64{targetA.ID}, deadlineA, 30*time.Second, "late ack")
		}},
		{"DeferDeliveries", func() (int64, error) { return d.DeferDeliveries(ctx, []int64{targetA.ID}, deadlineA, "late defer") }},
		{"FailDeliveries", func() (int64, error) { return d.FailDeliveries(ctx, []int64{targetA.ID}, deadlineA, "late fail") }},
	}
	for _, late := range lateCalls {
		affected, err := late.call()
		if err != nil {
			t.Fatalf("%s with stale fence must not error, got: %v", late.name, err)
		}
		if affected != 0 {
			t.Fatalf("%s with stale fence must affect exactly 0 rows (fencing violated), got %d", late.name, affected)
		}
		var stateNow string
		var deadlineNow time.Time
		var attemptsNow int
		if err := d.QueryRowContext(ctx, "SELECT state, next_attempt_at, attempts FROM notification_deliveries WHERE id = ?",
			targetA.ID).Scan(&stateNow, &deadlineNow, &attemptsNow); err != nil {
			t.Fatalf("query delivery after %s failed: %v", late.name, err)
		}
		if stateNow != NotifyStateSending || !deadlineNow.UTC().Equal(deadlineB.UTC()) || attemptsNow != attemptsB {
			t.Fatalf("%s with stale fence mutated worker B lease: state=%s deadline=%v attempts=%d (want sending/%v/%d)",
				late.name, stateNow, deadlineNow, attemptsNow, deadlineB, attemptsB)
		}
	}

	affected, err := d.MarkDeliveriesSent(ctx, []int64{targetA.ID}, deadlineB)
	if err != nil {
		t.Fatalf("worker B MarkDeliveriesSent with current fence failed: %v", err)
	}
	if affected != 1 {
		t.Fatalf("worker B MarkDeliveriesSent with current fence must affect exactly 1 row, got %d", affected)
	}
	var finalState string
	if err := d.QueryRowContext(ctx, "SELECT state FROM notification_deliveries WHERE id = ?", targetA.ID).Scan(&finalState); err != nil {
		t.Fatalf("query final delivery state failed: %v", err)
	}
	if finalState != NotifyStateSent {
		t.Fatalf("delivery must transition to sent under the current lease token, got %s", finalState)
	}
}

// TestNotification_ClaimRealtimeDeliveries_ConcurrentWorkersNoDoubleClaim 真机并发抢占：
// 多个 worker 同刻抢同一批到期行，任意一行的领取者必须唯一（阶段 1 的 FOR UPDATE
// 与阶段 2 的 CAS 共同保证），且 attempts 总量不得超过领取总次数。
func TestNotification_ClaimRealtimeDeliveries_ConcurrentWorkersNoDoubleClaim(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	chID, evID := ndqIntegrationFixture(t, d, 3003)

	if _, _, err := d.FanOutPendingEvents(ctx, 10); err != nil {
		t.Fatalf("FanOutPendingEvents failed: %v", err)
	}

	const workers = 6
	type verdict struct {
		ids []int64
		err error
	}
	results := make(chan verdict, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			got, err := d.ClaimRealtimeDeliveries(ctx, chID, 10, time.Minute)
			v := verdict{err: err}
			for _, dl := range got {
				v.ids = append(v.ids, dl.ID)
			}
			results <- v
		}()
	}
	close(start)

	seen := map[int64]int{}
	var claimed int
	for i := 0; i < workers; i++ {
		select {
		case v := <-results:
			if v.err != nil {
				t.Fatalf("concurrent ClaimRealtimeDeliveries failed: %v", v.err)
			}
			for _, id := range v.ids {
				seen[id]++
			}
			claimed += len(v.ids)
		case <-time.After(30 * time.Second):
			t.Fatal("concurrent claim did not settle within 30s")
		}
	}
	// 目标事件的那条投递必须恰好被领走一次（其余并发者拿到 0 条）。
	var targetID int64
	if err := d.QueryRowContext(ctx, "SELECT id FROM notification_deliveries WHERE event_id = ? AND channel_id = ?",
		evID, chID).Scan(&targetID); err != nil {
		t.Fatalf("query target delivery failed: %v", err)
	}
	if seen[targetID] != 1 {
		t.Fatalf("同一条投递的并发领取者必须唯一，实得 %d 次（总领取 %d 次）", seen[targetID], claimed)
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("投递 %d 被 %d 个 worker 同时领走，两阶段 CAS 失效", id, n)
		}
	}
	var state string
	var attempts int
	if err := d.QueryRowContext(ctx, "SELECT state, attempts FROM notification_deliveries WHERE id = ?", targetID).
		Scan(&state, &attempts); err != nil {
		t.Fatalf("query target state failed: %v", err)
	}
	if state != NotifyStateSending || attempts != 1 {
		t.Fatalf("并发后目标投递应为 sending/attempts=1，实得 %s/%d", state, attempts)
	}
}
