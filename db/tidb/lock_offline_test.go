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
// 不受 maxAcquireWait 影响。
func TestAcquireMaintenance_DeadlineCtxUnaffectedByCap(t *testing.T) {
	withMaxAcquireWait(t, 5*time.Second)

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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := AcquireMaintenance(ctx, db, "66666666666666666666666666666666")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrMaintenanceContended) {
		t.Errorf("deadline ctx must not be capped by maxAcquireWait, got %v", err)
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
