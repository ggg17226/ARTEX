package tidb

// 离线 fake-driver 覆盖 maintenance 抢锁的「取消后补偿」与「有界退避」两条路径：
// 真实 TiDB 上这两条依赖服务端晚到写与 30s 量级等待，无法在单测里稳定复现，
// 故用 database/sql 的 fake driver 直接编排 Exec/Query 的返回值。
// fake driver 写法参考 db/schema/ddl_apply_test.go。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// —— fake driver 基础设施 ——————————————————————————————————————————————

type lockFakeCall struct {
	op    string // "exec" | "query"
	query string
}

type lockFakeDriver struct{}

func (lockFakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("lockFakeDriver: Open not implemented, use OpenDB")
}

type lockFakeConnector struct{ conn *lockFakeConn }

func (c lockFakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.conn, nil
}

func (c lockFakeConnector) Driver() driver.Driver { return lockFakeDriver{} }

type lockFakeConn struct {
	mu      sync.Mutex
	calls   []lockFakeCall
	execFn  func(ctx context.Context, query string) (driver.Result, error)
	queryFn func(ctx context.Context, query string) (driver.Rows, error)
}

func (c *lockFakeConn) record(op, query string) {
	c.mu.Lock()
	c.calls = append(c.calls, lockFakeCall{op: op, query: query})
	c.mu.Unlock()
}

func (c *lockFakeConn) count(prefixes ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, call := range c.calls {
		for _, p := range prefixes {
			if strings.Contains(call.query, p) {
				n++
				break
			}
		}
	}
	return n
}

func (c *lockFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("lockFakeConn: Prepare not implemented")
}
func (c *lockFakeConn) Close() error { return nil }
func (c *lockFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("lockFakeConn: Begin not implemented")
}

func (c *lockFakeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.record("exec", query)
	if c.execFn != nil {
		return c.execFn(ctx, query)
	}
	return driver.ResultNoRows, nil
}

func (c *lockFakeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.record("query", query)
	if c.queryFn != nil {
		return c.queryFn(ctx, query)
	}
	return &lockFakeRows{cols: []string{"x"}}, nil
}

type lockFakeRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *lockFakeRows) Columns() []string { return r.cols }
func (r *lockFakeRows) Close() error      { return nil }
func (r *lockFakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

// SQL 语句识别（与 lock.go 中的语句文本对齐）
const (
	qAcquireUpdate = "SET op_id = UNHEX("
	qReleaseUpdate = "SET op_id = NULL"
	qOwnerSelect   = "SELECT op_id FROM maintenance"
	qProbeSelect   = "SELECT 1 FROM maintenance"
)

func newLockFakeDB(t *testing.T,
	execFn func(ctx context.Context, query string) (driver.Result, error),
	queryFn func(ctx context.Context, query string) (driver.Rows, error),
) (*sql.DB, *lockFakeConn) {
	t.Helper()
	fc := &lockFakeConn{execFn: execFn, queryFn: queryFn}
	db := sql.OpenDB(lockFakeConnector{conn: fc})
	db.SetMaxOpenConns(1) // 串行复用同一条 fake conn，便于精确断言调用序列
	t.Cleanup(func() { _ = db.Close() })
	return db, fc
}

// probeRows 返回单例行存在（SELECT 1 → 1）。
func probeRows() driver.Rows {
	return &lockFakeRows{cols: []string{"1"}, rows: [][]driver.Value{{int64(1)}}}
}

// ownerRows 返回 maintenance.op_id；nil 表示 NULL。
func ownerRows(opIDHex string) driver.Rows {
	if opIDHex == "" {
		return &lockFakeRows{cols: []string{"op_id"}, rows: [][]driver.Value{{nil}}}
	}
	raw, err := hex.DecodeString(opIDHex)
	if err != nil {
		panic(err)
	}
	return &lockFakeRows{cols: []string{"op_id"}, rows: [][]driver.Value{{raw}}}
}

// withShortReconcile 把补偿预算/轮询间隔调小，测试结束恢复。
func withShortReconcile(t *testing.T, budget, poll time.Duration) {
	t.Helper()
	oldBudget, oldPoll := acquireReconcileBudget, acquireReconcilePoll
	acquireReconcileBudget, acquireReconcilePoll = budget, poll
	t.Cleanup(func() { acquireReconcileBudget, acquireReconcilePoll = oldBudget, oldPoll })
}

// withMaxAcquireWait 把无 deadline ctx 的抢锁总上限调小，测试结束恢复。
func withMaxAcquireWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := maxAcquireWait
	maxAcquireWait = d
	t.Cleanup(func() { maxAcquireWait = old })
}

// —— 用例 ——————————————————————————————————————————————————————

// 取消后补偿成功：抢锁 UPDATE 返回 ctx 错误，复核发现自己就是 owner →
// 条件释放并返回带 "released" 的错误（链含 ctx.Err()）。
func TestAcquireMaintenance_CanceledCompensationReleases(t *testing.T) {
	const opID = "0123456789abcdef0123456789abcdef"
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				// 模拟服务端在行锁上排队：客户端 ctx 到期才返回错误
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(opID), nil // 晚到写已把 owner 写成自己
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, opID)
	if err == nil {
		t.Fatal("canceled acquire must return error")
	}
	if !strings.Contains(err.Error(), "released") {
		t.Errorf("err = %v, want message containing \"released\"", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want chain to contain context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrAcquireUncertain) {
		t.Errorf("confirmed-and-released path must not report ErrAcquireUncertain: %v", err)
	}
	if n := fc.count(qReleaseUpdate); n != 1 {
		t.Errorf("release UPDATE calls = %d, want 1", n)
	}
	if n := fc.count(qOwnerSelect); n < 1 {
		t.Errorf("reconcile must query op_id, got %d owner selects", n)
	}
}

// 取消后未持有：复核看到 op_id IS NULL（预算内始终为空）→ 原样返回 ctx 错误，
// 绝不执行任何释放 UPDATE（不得误清他人/后续 owner）。
func TestAcquireMaintenance_CanceledNotHeldReturnsCtxErr(t *testing.T) {
	const opID = "11111111111111111111111111111111"
	withShortReconcile(t, 200*time.Millisecond, 20*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				t.Error("must not release when op_id is NULL")
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(""), nil // 始终 NULL
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := AcquireMaintenance(ctx, db, opID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrAcquireUncertain) {
		t.Errorf("budget-exhausted-with-NULL-owner is a confirmed not-held case, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("reconcile waited %v, want ~budget 200ms (bounded)", elapsed)
	}
	if n := fc.count(qOwnerSelect); n < 2 {
		t.Errorf("reconcile should poll owner, got %d queries", n)
	}
}

// 取消后他人持有：复核看到别的 opID → 立即返回 ctx 错误，不做释放、不再轮询。
func TestAcquireMaintenance_CanceledHeldByOtherReturnsCtxErr(t *testing.T) {
	const otherOp = "22222222222222222222222222222222"
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				t.Error("must not release another op's ownership")
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(otherOp), nil
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := AcquireMaintenance(ctx, db, "33333333333333333333333333333333")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("other-owner is unambiguous, should return fast, took %v", elapsed)
	}
	if n := fc.count(qOwnerSelect); n != 1 {
		t.Errorf("owner queries = %d, want 1 (early exit)", n)
	}
}

// 无法确认：复核查询本身失败 → ErrAcquireUncertain（调用方需重试或清理）。
func TestAcquireMaintenance_CanceledUncertain(t *testing.T) {
	const opID = "44444444444444444444444444444444"
	sentinel := errors.New("fake: reconcile query failed")
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	db, _ := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return nil, sentinel
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, opID)
	if !errors.Is(err, ErrAcquireUncertain) {
		t.Fatalf("err = %v, want ErrAcquireUncertain", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("uncertain error must still chain the ctx error, got %v", err)
	}
}

// 无 deadline + 他人持有：内部 maxAcquireWait 上限生效，返回 ErrMaintenanceContended，
// 不再像 context.Background() 那样无限退避挂起。
func TestAcquireMaintenance_NoDeadlineBoundedByMaxWait(t *testing.T) {
	withMaxAcquireWait(t, 300*time.Millisecond)

	db, _ := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				return driver.RowsAffected(0), nil // 恒 0 行：他人持有
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				return probeRows(), nil
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	start := time.Now()
	err := AcquireMaintenance(context.Background(), db, "55555555555555555555555555555555")
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMaintenanceContended) {
		t.Fatalf("err = %v, want ErrMaintenanceContended", err)
	}
	if elapsed < 300*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("waited %v, want ~maxAcquireWait 300ms", elapsed)
	}
	if !strings.Contains(err.Error(), "300ms") {
		t.Errorf("err = %v, want message to mention the wait budget", err)
	}
}

// 有 deadline 的 ctx 行为不变：仍按 ctx 到期返回 DeadlineExceeded，
// 不受 maxAcquireWait 影响。上限刻意设得**短于**调用方 deadline（150ms < 400ms）：
// 若实现错误地对所有 ctx 一律施加 cap，会在 150ms 就返回 ErrMaintenanceContended，
// 本用例即失败；正确实现必须等到调用方期限（≈400ms）才返回 DeadlineExceeded。
func TestAcquireMaintenance_DeadlineCtxUnaffectedByCap(t *testing.T) {
	withMaxAcquireWait(t, 150*time.Millisecond)

	db, _ := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				return driver.RowsAffected(0), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				return probeRows(), nil
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := AcquireMaintenance(ctx, db, "66666666666666666666666666666666")
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrMaintenanceContended) {
		t.Errorf("deadline ctx must not be capped by maxAcquireWait, got %v", err)
	}
	if elapsed < 350*time.Millisecond {
		t.Errorf("returned after %v, want ≈caller deadline 400ms (cap 150ms must not apply)", elapsed)
	}
}

// 成功抢锁不触发任何补偿查询（回归：正常路径零额外开销）。
func TestAcquireMaintenance_SuccessNoReconcile(t *testing.T) {
	db, fc := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				return probeRows(), nil
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	if err := AcquireMaintenance(context.Background(), db, "77777777777777777777777777777777"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if n := fc.count(qOwnerSelect); n != 0 {
		t.Errorf("success path must not run reconcile queries, got %d", n)
	}
}

// —— Round 1 修复回归（R8 审查 I1/I2/I3/M1）——————————————————————————

// I3：NULL → own → released 确定性序列。第一次复查看到 NULL（晚到写尚未落库），
// 第二次看到自己的 opID（晚到写已落库）→ 条件释放。回归兜底：若补偿退化为
// 只查一次 SELECT、或删除轮询，本用例必失败（第一次 NULL 就返回将无 released）。
func TestAcquireMaintenance_CanceledNullThenOwnReleases(t *testing.T) {
	const opID = "88888888888888888888888888888888"
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	ownerCalls := 0
	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				ownerCalls++
				if ownerCalls == 1 {
					return ownerRows(""), nil // 晚到写尚未落库
				}
				return ownerRows(opID), nil // 晚到写已把 owner 写成自己
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, opID)
	if err == nil {
		t.Fatal("canceled acquire must return error")
	}
	if !strings.Contains(err.Error(), "canceled after acquiring, released") {
		t.Errorf("err = %v, want message containing %q", err, "canceled after acquiring, released")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want chain to contain context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrAcquireUncertain) {
		t.Errorf("confirmed-and-released path must not report ErrAcquireUncertain: %v", err)
	}
	if n := fc.count(qOwnerSelect); n < 2 {
		t.Errorf("owner queries = %d, want >= 2 (NULL 后必须继续轮询到预算或观察到 own)", n)
	}
	if n := fc.count(qReleaseUpdate); n != 1 {
		t.Errorf("release UPDATE calls = %d, want 1", n)
	}
}

// I1：复核的**第一次** owner 查询自身耗尽预算（等池/查询阻塞到 rctx 到期，
// 从未成功读到过 owner）→ 必须 ErrAcquireUncertain，不得降格为「未持有」的
// 裸 ctx 错误。
func TestAcquireMaintenance_CanceledReconcileFirstQueryTimeoutUncertain(t *testing.T) {
	const opID = "91919191919191919191919191919191"
	withShortReconcile(t, 150*time.Millisecond, 20*time.Millisecond)

	db, _ := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return driver.ResultNoRows, nil
		},
		func(ctx context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				<-ctx.Done() // 模拟查询阻塞到复核预算到期（等池同理）
				return nil, ctx.Err()
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := AcquireMaintenance(ctx, db, opID)
	if !errors.Is(err, ErrAcquireUncertain) {
		t.Fatalf("query never succeeded must be uncertain, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("uncertain error must still chain the ctx error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Errorf("reconcile must stay bounded by its budget, took %v", elapsed)
	}
}

// I1 + M1：先成功读到 NULL，随后一次复核查询返回真实错误（连接/SQL 故障）→
// 仍必须 ErrAcquireUncertain，且错误链保留**真实查询错误**（sentinel 可判）。
func TestAcquireMaintenance_CanceledReconcileQueryFailsAfterNullUncertain(t *testing.T) {
	const opID = "92929292929292929292929292929292"
	queryErr := errors.New("fake: owner query failed after NULL")
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	ownerCalls := 0
	db, _ := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				ownerCalls++
				if ownerCalls == 1 {
					return ownerRows(""), nil
				}
				return nil, queryErr
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, opID)
	if !errors.Is(err, ErrAcquireUncertain) {
		t.Fatalf("err = %v, want ErrAcquireUncertain", err)
	}
	if !errors.Is(err, queryErr) {
		t.Errorf("real query error must be kept in the chain (M1), got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("uncertain error must still chain the ctx error, got %v", err)
	}
}

// M1：复核确认持有但**释放 UPDATE 失败** → ErrAcquireUncertain，错误链必须
// 保留真实释放错误（releaseSentinel 可判），不得只留 sentinel + ctx 错误。
func TestAcquireMaintenance_CanceledReleaseFailsUncertain(t *testing.T) {
	const opID = "93939393939393939393939393939393"
	releaseErr := errors.New("fake: release update failed")
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				return nil, releaseErr
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(opID), nil
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, opID)
	if !errors.Is(err, ErrAcquireUncertain) {
		t.Fatalf("err = %v, want ErrAcquireUncertain", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Errorf("real release error must be kept in the chain (M1), got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("uncertain error must still chain the ctx error, got %v", err)
	}
	if n := fc.count(qReleaseUpdate); n != 1 {
		t.Errorf("release UPDATE calls = %d, want 1", n)
	}
}

// I2：无 deadline 的 maxAcquireWait 上限必须覆盖**单条阻塞的抢锁 UPDATE**
// （服务端行锁/等池场景）：上限到期即返回 ErrMaintenanceContended，而不是
// 继续等驱动/服务端（真机 innodb_lock_wait_timeout=50s），且到期后不得再补发
// UPDATE。
func TestAcquireMaintenance_NoDeadlineCapCoversBlockingUpdate(t *testing.T) {
	withMaxAcquireWait(t, 200*time.Millisecond)
	withShortReconcile(t, 100*time.Millisecond, 20*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done() // 模拟 UPDATE 在服务端行锁上排队
				return nil, ctx.Err()
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(""), nil // 复核窗口内未观察到持有
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	start := time.Now()
	err := AcquireMaintenance(context.Background(), db, "94949494949494949494949494949494")
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMaintenanceContended) {
		t.Fatalf("err = %v, want ErrMaintenanceContended", err)
	}
	if !strings.Contains(err.Error(), "200ms") {
		t.Errorf("err = %v, want message to mention the wait budget", err)
	}
	// 上限 200ms + 补偿复核 ≤100ms + 调度余量；绝不能等满服务端锁超时。
	if elapsed < 200*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("waited %v, want ≈maxAcquireWait 200ms + reconcile budget 100ms", elapsed)
	}
	if n := fc.count(qAcquireUpdate); n != 1 {
		t.Errorf("acquire UPDATE calls = %d, want 1 (截止后不得补发 UPDATE)", n)
	}
}

// I2 补充：上限到期打断的正是排队中的抢锁 UPDATE，且复核观察到 own owner →
// 返回值必须以 ErrMaintenanceContended 为准，同时错误链保留补偿的 "released"
// 结果（上限路径同样不得丢失补偿）。
func TestAcquireMaintenance_NoDeadlineCapCanceledUpdateReconciles(t *testing.T) {
	const opID = "95959595959595959595959595959595"
	withMaxAcquireWait(t, 200*time.Millisecond)
	withShortReconcile(t, 2*time.Second, 10*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(ctx context.Context, query string) (driver.Result, error) {
			switch {
			case strings.Contains(query, qAcquireUpdate):
				<-ctx.Done()
				return nil, ctx.Err()
			case strings.Contains(query, qReleaseUpdate):
				return driver.RowsAffected(1), nil
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			switch {
			case strings.Contains(query, qProbeSelect):
				return probeRows(), nil
			case strings.Contains(query, qOwnerSelect):
				return ownerRows(opID), nil // 晚到写已落库
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	err := AcquireMaintenance(context.Background(), db, opID)
	if !errors.Is(err, ErrMaintenanceContended) {
		t.Fatalf("err = %v, want ErrMaintenanceContended", err)
	}
	if !strings.Contains(err.Error(), "canceled after acquiring, released") {
		t.Errorf("cap path must keep the reconcile result in the chain, got %v", err)
	}
	if n := fc.count(qReleaseUpdate); n != 1 {
		t.Errorf("release UPDATE calls = %d, want 1", n)
	}
}

// —— Round 2 修复回归（R12 复审 N1：普通查询错误不得被误报为 cap 到期）——————

// N1：调用方 ctx 健康且**没有 deadline**（capped=true，内部上限远未到期）时，
// 入口 SELECT 返回普通错误（权限拒绝/连接失败/SQL 语法…）→ 必须原样保留真实
// 原因（errors.Is 可判），**绝不得**包装成 ErrMaintenanceContended，也不得进入
// 退避循环补发 UPDATE。回归兜底：若 capExpired 退回「capped && ctx.Err()==nil」
// 这种恒真谓词，本用例立刻失败（错误会变成 Contended 且丢失 probeErr）。
func TestAcquireMaintenance_InitialProbeQueryErrorPreserved(t *testing.T) {
	const opID = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	probeErr := errors.New("fake: Error 1142 (42000): SELECT command denied to user 'artex' for table 'maintenance'")
	withMaxAcquireWait(t, 5*time.Second) // 远未到期：任何 Contended 都是误报

	db, fc := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				t.Error("入口探测失败后不得再发抢锁 UPDATE")
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				return nil, probeErr // 普通权限错误，非 ctx 到期、非缺行/缺表
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	start := time.Now()
	err := AcquireMaintenance(context.Background(), db, opID)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("入口探测失败必须返回错误")
	}
	if !errors.Is(err, probeErr) {
		t.Errorf("真实查询错误必须保留在错误链上，got %v", err)
	}
	if errors.Is(err, ErrMaintenanceContended) {
		t.Errorf("普通查询错误不得改写成 ErrMaintenanceContended（N1），got %v", err)
	}
	if errors.Is(err, ErrMaintenanceRowMissing) {
		t.Errorf("非缺行/缺表错误不得改写成 ErrMaintenanceRowMissing，got %v", err)
	}
	if elapsed > 1*time.Second {
		t.Errorf("普通错误必须立即返回，took %v", elapsed)
	}
	if n := fc.count(qAcquireUpdate); n != 0 {
		t.Errorf("acquire UPDATE calls = %d, want 0", n)
	}
}

// N1 的另一半：入口 SELECT 一直阻塞到**内部上限真的到期** → 这才是
// ErrMaintenanceContended 的正当场景；且到期后不得再补发抢锁 UPDATE。
func TestAcquireMaintenance_InitialProbeBlockedByCapContended(t *testing.T) {
	withMaxAcquireWait(t, 200*time.Millisecond)

	db, fc := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				t.Error("cap 到期后不得补发抢锁 UPDATE")
			}
			return driver.ResultNoRows, nil
		},
		func(ctx context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				<-ctx.Done() // 入口探测阻塞到内部上限到期（等池同理）
				return nil, ctx.Err()
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	start := time.Now()
	err := AcquireMaintenance(context.Background(), db, "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2")
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMaintenanceContended) {
		t.Fatalf("err = %v, want ErrMaintenanceContended（cap 真的到期）", err)
	}
	if !strings.Contains(err.Error(), "200ms") {
		t.Errorf("err = %v, want message to mention the wait budget", err)
	}
	if elapsed < 200*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("waited %v, want ≈maxAcquireWait 200ms", elapsed)
	}
	if n := fc.count(qAcquireUpdate); n != 0 {
		t.Errorf("acquire UPDATE calls = %d, want 0", n)
	}
}

// N1（循环内探测）：UPDATE 恒 0 行后，循环内探测返回普通错误 → 同样必须原样
// 保留真实原因，不得改写成 Contended，也不得继续退避空转到上限。
func TestAcquireMaintenance_InLoopProbeQueryErrorPreserved(t *testing.T) {
	const opID = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
	probeErr := errors.New("fake: lost connection to maintenance probe")
	withMaxAcquireWait(t, 5*time.Second)
	probeCalls := 0

	db, fc := newLockFakeDB(t,
		func(_ context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, qAcquireUpdate) {
				return driver.RowsAffected(0), nil // 0 行 → 进入循环内探测
			}
			return driver.ResultNoRows, nil
		},
		func(_ context.Context, query string) (driver.Rows, error) {
			if strings.Contains(query, qProbeSelect) {
				probeCalls++
				if probeCalls == 1 {
					return probeRows(), nil // 入口探测成功 → 才会走到循环内探测
				}
				return nil, probeErr // 循环内探测：普通错误（非 ctx、非缺行/缺表）
			}
			return &lockFakeRows{cols: []string{"x"}}, nil
		})

	start := time.Now()
	err := AcquireMaintenance(context.Background(), db, opID)
	elapsed := time.Since(start)
	if !errors.Is(err, probeErr) {
		t.Errorf("真实探测错误必须保留在错误链上，got %v", err)
	}
	if errors.Is(err, ErrMaintenanceContended) {
		t.Errorf("普通探测错误不得改写成 ErrMaintenanceContended（N1），got %v", err)
	}
	if elapsed > 1*time.Second {
		t.Errorf("普通错误必须立即返回（不退避空转），took %v", elapsed)
	}
	if n := fc.count(qAcquireUpdate); n != 1 {
		t.Errorf("acquire UPDATE calls = %d, want 1", n)
	}
	if probeCalls != 2 {
		t.Errorf("probe SELECT calls = %d, want 2（入口 1 次 + 循环内 1 次）", probeCalls)
	}
}
