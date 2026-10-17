package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ---------------------------------------------------------------------------
// 本文件覆盖 R2 审查项 W6c：
//   I2 —— 连接池准入控制（防 hold-and-wait 池死锁）
//   I6 —— 结构化错误分类（禁止错误文本误判）
//   I7 —— 提交协议异常纳入 unknown-outcome（ErrPktSync 等）
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// I6：结构化错误分类
// ---------------------------------------------------------------------------

// codeOnlyError 只实现 Code() uint16 的驱动风格错误（规范提取接口之一）
type codeOnlyError struct {
	code uint16
	msg  string
}

func (e *codeOnlyError) Error() string { return fmt.Sprintf("Error %d: %s", e.code, e.msg) }
func (e *codeOnlyError) Code() uint16  { return e.code }

// TestIsRetryableConflict_StructuredNumbersOnly 原生 *mysql.MySQLError（Number 是字段）
// 必须被精确识别；任何**没有**结构化编号的错误，无论消息里出现 1213/1205/1062/
// Deadlock/Lock wait timeout 多少次，都不得判为可重试冲突（R2 I6）。
func TestIsRetryableConflict_StructuredNumbersOnly(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// —— 原生驱动结构化错误：绝对精确 ——
		{"native 1213 deadlock", &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}, true},
		{"native 1205 lock wait timeout", &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}, true},
		{"native 1062 duplicate entry", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '7' for key 'PRIMARY'"}, true},
		{"wrapped native 1213", fmt.Errorf("exec update: %w", &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}), true},
		{"double wrapped native 1205", fmt.Errorf("lock db namespace ns: %w", fmt.Errorf("select for update: %w", &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout"})), true},

		// —— 结构化编号不在白名单：即使消息含冲突字样也不得重试 ——
		{"native 1146 table missing", &mysql.MySQLError{Number: 1146, Message: "Table 'db.t' doesn't exist"}, false},
		{"native 1064 syntax mentioning 1213", &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax near 1213"}, false},
		{"native 1216 fk mentioning deadlock", &mysql.MySQLError{Number: 1216, Message: "Cannot add or update a child row: Deadlock"}, false},

		// —— 测试替身/自定义结构化接口 ——
		{"Number() method 1213", &mockMySQLError{ErrNumber: 1213, Message: "Deadlock"}, true},
		{"Number() method 1146", &mockMySQLError{ErrNumber: 1146, Message: "no such table"}, false},
		{"Code() method 1205", &codeOnlyError{code: 1205, msg: "Lock wait timeout"}, true},
		{"wrapped Code() 1062", fmt.Errorf("insert: %w", &codeOnlyError{code: 1062, msg: "Duplicate entry"}), true},

		// —— 纯文本业务错误：含编号/关键字一律 false（R2 I6 的核心反例）——
		{"business msg contains 1213", errors.New("task 1213 violates policy"), false},
		{"business msg contains deadlock word", errors.New("Deadlock detected by workflow engine"), false},
		{"business msg contains lock wait timeout", errors.New("lock wait timeout in scheduler queue"), false},
		{"business msg contains 1062 + Duplicate entry", errors.New("Duplicate entry for business ref 1062"), false},
		{"wrapped business text error", fmt.Errorf("validate order: %w", errors.New("order 1205 already shipped")), false},
		{"plain business error", errors.New("amount must be positive"), false},

		// —— 本包结构化哨兵 ——
		{"membership drift", ErrMembershipDrift, true},
		{"entity not found", ErrEntityNotFound, true},
		{"admission timeout", ErrAdmissionTimeout, true},
		{"wrapped admission timeout", fmt.Errorf("fresh locator admission: %w", ErrAdmissionTimeout), true},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"nil", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryableConflict(c.err); got != c.want {
				t.Fatalf("IsRetryableConflict(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// I7：unknown-outcome 分类与提交结局判定
// ---------------------------------------------------------------------------

func TestIsOutcomeUnknown_ProtocolDesyncAndTruncation(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"mysql.ErrPktSync", mysql.ErrPktSync, true},
		{"wrapped mysql.ErrPktSync", fmt.Errorf("write command: %w", mysql.ErrPktSync), true},
		{"double wrapped mysql.ErrPktSync", fmt.Errorf("commit: %w", fmt.Errorf("read response: %w", mysql.ErrPktSync)), true},
		{"mysql.ErrPktSyncMul", mysql.ErrPktSyncMul, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"wrapped io.ErrUnexpectedEOF", fmt.Errorf("read packet: %w", io.ErrUnexpectedEOF), true},
		{"io.EOF", io.EOF, true},
		{"mysql.ErrInvalidConn", mysql.ErrInvalidConn, true},
		{"mysql.ErrMalformPkt", mysql.ErrMalformPkt, true},
		{"driver.ErrBadConn", driver.ErrBadConn, true},
		{"native 2006 gone away", &mysql.MySQLError{Number: 2006, Message: "MySQL server has gone away"}, true},
		{"native 2013 lost", &mysql.MySQLError{Number: 2013, Message: "Lost connection to MySQL server during query"}, true},
		{"net.OpError", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, true},
		{"ErrOutcomeUnknown sentinel", ErrOutcomeUnknown, true},
		{"text broken pipe", errors.New("write tcp 127.0.0.1:4000->127.0.0.1:4000: write: broken pipe"), true},
		{"text commands out of sync", errors.New("Error 10000: commands out of sync. You can't run this command now"), true},

		// 明确不属于 unknown 的错误
		{"plain business error", errors.New("amount must be positive"), false},
		{"native 1213 deadlock", &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}, false},
		{"sql.ErrTxDone", sql.ErrTxDone, false},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isOutcomeUnknown(c.err); got != c.want {
				t.Fatalf("isOutcomeUnknown(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestCommitProvablyNotAttempted 只有"提交请求可证明未送达服务端"的错误才允许
// 按普通失败处理；其余（含协议失步）一律 unknown-outcome（R2 I7）。
func TestCommitProvablyNotAttempted(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"sql.ErrTxDone", sql.ErrTxDone, true},
		{"wrapped sql.ErrTxDone", fmt.Errorf("commit: %w", sql.ErrTxDone), true},
		{"context.Canceled", context.Canceled, true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"wrapped deadline", fmt.Errorf("commit: %w", context.DeadlineExceeded), true},
		{"mysql.ErrPktSync", mysql.ErrPktSync, false},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, false},
		{"native 2013", &mysql.MySQLError{Number: 2013, Message: "Lost connection"}, false},
		{"unknown driver error", errors.New("some weird commit failure"), false},
		{"nil", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := commitProvablyNotAttempted(c.err); got != c.want {
				t.Fatalf("commitProvablyNotAttempted(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// setMockCommitFn 在 globalMockDriver 上安装提交错误钩子，返回清理函数。
func setMockCommitFn(t *testing.T, fn func() error) {
	t.Helper()
	globalMockDriver.mu.Lock()
	globalMockDriver.commitFn = fn
	globalMockDriver.mu.Unlock()
	t.Cleanup(func() {
		globalMockDriver.mu.Lock()
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()
	})
}

func mockDriverRollbackCount() int {
	globalMockDriver.mu.Lock()
	defer globalMockDriver.mu.Unlock()
	return globalMockDriver.txRolledBack
}

// TestTxRunner_CommitPktSync_HoldsSharedGateAndBlocksNextRun
// mysql.ErrPktSync 发生在 Commit：响应 sequence 失步时服务端可能已经提交，
// 必须判为 unknown-outcome；随后的 Rollback 只会返回 sql.ErrTxDone，
// 不得被当作"已安全撤销"。默认门禁（进程级单例）必须继续封锁同一 namespace。
func TestTxRunner_CommitPktSync_HoldsSharedGateAndBlocksNextRun(t *testing.T) {
	ns := "w6c-i7-pktsync-" + t.Name()

	db1 := getMockDB(t)
	defer db1.Close()
	db2 := getMockDB(t)
	defer db2.Close()

	// 用例结束手工归还被刻意 hold 的共享门禁，避免污染同进程其他用例
	// （这是测试夹具行为，不是生产恢复手段，见 ctxMutex 文档）
	t.Cleanup(func() { DefaultGateManager().ReleaseLocalNamespace(ns) })

	setMockCommitFn(t, func() error { return mysql.ErrPktSync })

	rollbacksBefore := mockDriverRollbackCount()

	err1 := NewTxRunner(db1).Run(context.Background(), TxRunnerOptions{
		RequiredNamespaces: []string{ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})
	if !errors.Is(err1, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown for mysql.ErrPktSync at commit, got %v", err1)
	}
	if !errors.Is(err1, mysql.ErrPktSync) {
		t.Errorf("expected wrapped mysql.ErrPktSync in error chain, got %v", err1)
	}

	// hold 分支绝不 Rollback：stdlib 此时只会返回 sql.ErrTxDone，不构成撤销证据
	if got := mockDriverRollbackCount(); got != rollbacksBefore {
		t.Errorf("unknown-outcome must not roll back: rollbacks went %d -> %d", rollbacksBefore, got)
	}

	// 后续 Run 必须被进程级共享门禁封锁（fail closed）
	var payloadCalls int
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	err2 := NewTxRunner(db2).Run(ctx2, TxRunnerOptions{
		RequiredNamespaces: []string{ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		payloadCalls++
		return nil
	})
	if !errors.Is(err2, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded while blocked on held gate, got %v", err2)
	}
	if payloadCalls != 0 {
		t.Fatalf("second Run entered payload %d times while gate was held", payloadCalls)
	}
}

// TestTxRunner_CommitUnexpectedEOF_HoldsGates io.ErrUnexpectedEOF（响应包读到一半断流）
// 同样必须 unknown-outcome 且不释放门禁。
func TestTxRunner_CommitUnexpectedEOF_HoldsGates(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	setMockCommitFn(t, func() error { return fmt.Errorf("read response: %w", io.ErrUnexpectedEOF) })

	cgm := newCountingGateManager()
	err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"w6c-i7-eof"},
		GateManager:          cgm,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown, got %v", err)
	}
	cgm.mu.Lock()
	defer cgm.mu.Unlock()
	if cgm.releaseBusiness != 0 || cgm.releaseNamespaces["w6c-i7-eof"] != 0 {
		t.Errorf("gates must stay held on unknown outcome, got releases business=%d ns=%d",
			cgm.releaseBusiness, cgm.releaseNamespaces["w6c-i7-eof"])
	}
}

// TestTxRunner_CommitUnrecognizedError_DefaultsToUnknownOutcome
// 无法证明"未提交"的提交错误一律按 unknown-outcome 处理（保守默认，
// 不靠有限文本白名单反推"已回滚"）。
func TestTxRunner_CommitUnrecognizedError_DefaultsToUnknownOutcome(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	setMockCommitFn(t, func() error { return errors.New("some weird commit failure") })

	cgm := newCountingGateManager()
	err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		GateManager:          cgm,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown for unrecognized commit error, got %v", err)
	}
	cgm.mu.Lock()
	defer cgm.mu.Unlock()
	if cgm.releaseBusiness != 0 {
		t.Errorf("gates must stay held, got %d releases", cgm.releaseBusiness)
	}
}

// TestTxRunner_CommitPreSendErrors_AreNotUnknownOutcome
// sql.ErrTxDone 与发送前的 ctx 结束可证明提交未送达：按普通失败处理，
// 门禁必须正常释放（否则正常取消也会被永久封锁）。
func TestTxRunner_CommitPreSendErrors_AreNotUnknownOutcome(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"sql.ErrTxDone", sql.ErrTxDone},
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := getMockDB(t)
			defer db.Close()

			setMockCommitFn(t, func() error { return tc.err })

			cgm := newCountingGateManager()
			err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
				RequiresBusinessGate: true,
				RequiredNamespaces:   []string{"w6c-i7-presend"},
				GateManager:          cgm,
			}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				return nil
			})

			if errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("pre-send %v must not be classified as unknown outcome", tc.err)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("expected %v, got %v", tc.err, err)
			}
			cgm.mu.Lock()
			defer cgm.mu.Unlock()
			if cgm.releaseBusiness != 1 || cgm.releaseNamespaces["w6c-i7-presend"] != 1 {
				t.Errorf("gates must be released, got business=%d ns=%d",
					cgm.releaseBusiness, cgm.releaseNamespaces["w6c-i7-presend"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// I2：连接池准入控制
// ---------------------------------------------------------------------------

func TestAdmissionControl_CapsConcurrency(t *testing.T) {
	a := NewAdmissionControl(2, 2*time.Second)

	var (
		cur, peak int64
		wg        sync.WaitGroup
	)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := a.Acquire(context.Background())
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer release()

			n := atomic.AddInt64(&cur, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&cur, -1)
		}()
	}
	wg.Wait()

	if peak > 2 {
		t.Fatalf("admission leaked: peak concurrency %d > capacity 2", peak)
	}
	if peak < 1 {
		t.Fatalf("no work observed, test is vacuous")
	}
	st := a.Stats()
	if st.InUse != 0 || st.Waiting != 0 {
		t.Errorf("expected all permits returned, got %+v", st)
	}
	if a.Available() != 2 {
		t.Errorf("expected 2 available permits, got %d", a.Available())
	}
}

func TestAdmissionControl_TimeoutCancelAndNoop(t *testing.T) {
	t.Run("wait timeout fails fast under Background", func(t *testing.T) {
		a := NewAdmissionControl(1, 60*time.Millisecond)
		release, err := a.Acquire(context.Background())
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}
		defer release()

		start := time.Now()
		_, err = a.Acquire(context.Background())
		if !errors.Is(err, ErrAdmissionTimeout) {
			t.Fatalf("expected ErrAdmissionTimeout, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("admission waited %v, expected ~60ms (no unbounded wait)", elapsed)
		}
		if got := a.Stats().Timeouts; got != 1 {
			t.Errorf("expected 1 timeout, got %d", got)
		}
	})

	t.Run("ctx cancel unblocks waiter", func(t *testing.T) {
		a := NewAdmissionControl(1, 0) // 仅由 ctx 控制等待
		release, err := a.Acquire(context.Background())
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}
		defer release()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := a.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("nil and unlimited are pass-through", func(t *testing.T) {
		var nilCtrl *AdmissionControl
		rel, err := nilCtrl.Acquire(context.Background())
		if err != nil {
			t.Fatalf("nil control acquire: %v", err)
		}
		rel()

		unlimited := NewAdmissionControl(0, 10*time.Millisecond)
		for i := 0; i < 8; i++ {
			r, err := unlimited.Acquire(context.Background())
			if err != nil {
				t.Fatalf("unlimited acquire %d: %v", i, err)
			}
			r()
		}
		if unlimited.Available() != -1 {
			t.Errorf("unlimited control should report -1, got %d", unlimited.Available())
		}
	})

	t.Run("release is idempotent", func(t *testing.T) {
		a := NewAdmissionControl(1, time.Second)
		release, err := a.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		release()
		release() // 重复释放必须是无操作
		if a.Available() != 1 {
			t.Errorf("expected 1 available permit after double release, got %d", a.Available())
		}
		if got := a.Stats().InUse; got != 0 {
			t.Errorf("expected InUse 0, got %d", got)
		}
	})
}

func TestDefaultAdmissions_AreSingletonsAndBelowPoolCapacity(t *testing.T) {
	if DefaultRunnerAdmission() != DefaultRunnerAdmission() {
		t.Error("DefaultRunnerAdmission must be a process-wide singleton")
	}
	if DefaultLocatorAdmission() != DefaultLocatorAdmission() {
		t.Error("DefaultLocatorAdmission must be a process-wide singleton")
	}
	if DefaultRunnerAdmission() == DefaultLocatorAdmission() {
		t.Error("runner 与 locator 必须是两个独立信号量：共用会重新引入嵌套获取的 hold-and-wait")
	}
	// 最坏占用：runner 各持 1 条外层连接 + locator 各持 1 条只读连接，必须留有余量
	if worst := DefaultRunnerAdmissionCapacity + DefaultLocatorAdmissionCapacity; worst >= DefaultPoolCapacity {
		t.Errorf("worst case conns %d must stay below pool capacity %d", worst, DefaultPoolCapacity)
	}
	if DefaultRunnerAdmissionCapacity >= DefaultPoolCapacity || DefaultLocatorAdmissionCapacity >= DefaultPoolCapacity {
		t.Error("admission capacity must be strictly below pool capacity")
	}
}

// TestExecuteFreshLocator_AdmissionTimeoutFailsFast 定位连接拿不到许可时必须快速失败，
// 绝不在连接池上无限排队（context.Background 也不例外）。
func TestExecuteFreshLocator_AdmissionTimeoutFailsFast(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	a := NewAdmissionControl(1, 80*time.Millisecond)
	release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatalf("hold permit: %v", err)
	}

	fn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		return &LocatorSnapshot{}, nil
	}

	start := time.Now()
	_, err = ExecuteFreshLocatorWithAdmission(context.Background(), db, fn, a)
	if !errors.Is(err, ErrAdmissionTimeout) {
		t.Fatalf("expected ErrAdmissionTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("locator waited %v on admission, expected ~80ms", elapsed)
	}

	release()

	if _, err := ExecuteFreshLocatorWithAdmission(context.Background(), db, fn, a); err != nil {
		t.Fatalf("locator after permit released: %v", err)
	}
}

// TestTxRunner_AdmissionSaturatedFailsFast Run 拿不到准入许可时立即返回，
// 不开事务、不进 payload、不排队等死。
func TestTxRunner_AdmissionSaturatedFailsFast(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	a := NewAdmissionControl(1, 60*time.Millisecond)
	release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatalf("hold permit: %v", err)
	}
	defer release()

	var payloadCalls int
	err = NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
		Admission: a,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		payloadCalls++
		return nil
	})

	if !errors.Is(err, ErrAdmissionTimeout) {
		t.Fatalf("expected ErrAdmissionTimeout, got %v", err)
	}
	if payloadCalls != 0 {
		t.Fatalf("payload ran %d times without an admission permit", payloadCalls)
	}
}

// openSizedMockDB 打开一个限定池容量的 mock 连接池（模拟 db/tidb 的 maxOpenConns）。
func openSizedMockDB(t *testing.T, maxOpen int) *sql.DB {
	t.Helper()
	db, err := sql.Open("mock_locator_driver", "pool_test_dsn")
	if err != nil {
		t.Fatalf("open mock db: %v", err)
	}
	db.SetMaxOpenConns(maxOpen)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestTxRunner_AdmissionPreventsPoolHoldAndWait 复现 R2 I2 反例 A 的形状并证明准入有效：
// 池容量 4、8 个并发 Run，每个 Run 先持外层事务连接，再在 payload 内调用 fresh locator
// 向同一个池要第二条连接。准入上限 2/1 保证最坏占用 2+1=3 条连接 < 池容量 4，
// 所有 Run 必须全部完成，不得出现无人能归还连接的 hold-and-wait。
func TestTxRunner_AdmissionPreventsPoolHoldAndWait(t *testing.T) {
	const (
		workers        = 8
		poolSize       = 4
		runnerCapacity = 2
		locatorCap     = 1
		holdOuter      = 25 * time.Millisecond
		holdLocator    = 25 * time.Millisecond
	)

	db := openSizedMockDB(t, poolSize)
	runner := NewTxRunner(db)
	adm := NewAdmissionControl(runnerCapacity, 5*time.Second)
	locAdm := NewAdmissionControl(locatorCap, 5*time.Second)

	var (
		cur, peakOuter int64
		wg             sync.WaitGroup
		errs           = make([]error, workers)
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = runner.Run(context.Background(), TxRunnerOptions{
				Admission: adm,
			}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				// 已持外层事务连接（1 条物理连接）
				n := atomic.AddInt64(&cur, 1)
				for {
					p := atomic.LoadInt64(&peakOuter)
					if n <= p || atomic.CompareAndSwapInt64(&peakOuter, p, n) {
						break
					}
				}
				defer atomic.AddInt64(&cur, -1)

				time.Sleep(holdOuter)

				// 嵌套取第二条连接：无准入时这里就是 hold-and-wait 的阻塞点
				_, err := ExecuteFreshLocatorWithAdmission(ctx, db, func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
					time.Sleep(holdLocator)
					return &LocatorSnapshot{}, nil
				}, locAdm)
				return err
			})
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("hold-and-wait: %d/%d Runs still blocked after 10s (peak outer conns %d, pool %d)",
			workers, workers, atomic.LoadInt64(&peakOuter), poolSize)
	}

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if peak := atomic.LoadInt64(&peakOuter); peak > runnerCapacity {
		t.Fatalf("admission not enforced: %d Runs held outer connections at once (capacity %d)", peak, runnerCapacity)
	}
	if st := adm.Stats(); st.InUse != 0 {
		t.Errorf("admission permits leaked: %+v", st)
	}
	if st := locAdm.Stats(); st.InUse != 0 {
		t.Errorf("locator permits leaked: %+v", st)
	}
}

// TestExecuteFreshLocator_WithoutAdmissionStarvesPool 反面对照（R2 I2 反例 A）：
// 关闭准入后，2 个执行流各持 1 条连接（池容量 2）再各自要第二条，
// 两条 locator 请求只能停在连接池等待队列里 —— 没有任何一方能自行脱身，
// 只有外部释放被占用的连接才能前进（context.Background 下就是永久卡死）。
//
// 判定刻意不依赖 ctx 取消时序：stdlib 在等待者取消时会把"已经为取消者拨好的连接"
// 转交给另一个等待者（database/sql 的瞬时超额），用它断言会不稳定。
func TestExecuteFreshLocator_WithoutAdmissionStarvesPool(t *testing.T) {
	const workers = 2

	db := openSizedMockDB(t, workers)
	unlimited := NewAdmissionControl(0, 0)

	outerHeld := make(chan *sql.Tx, workers)
	failed := make(chan error, workers)
	finished := make(chan error, workers)
	// start 栅栏：所有外层连接都占住之后才放行 locator，
	// 否则先跑的一方能在池还有余量时直接成功，测不到饥饿。
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		go func(idx int) {
			outer, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				failed <- fmt.Errorf("worker %d begin outer: %w", idx, err)
				return
			}
			outerHeld <- outer
			<-start

			_, err = ExecuteFreshLocatorWithAdmission(context.Background(), db,
				func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
					return &LocatorSnapshot{}, nil
				}, unlimited)
			finished <- err
		}(i)
	}

	// 先等两条外层连接都被占住
	outers := make([]*sql.Tx, 0, workers)
	for i := 0; i < workers; i++ {
		select {
		case err := <-failed:
			t.Fatalf("setup failed: %v", err)
		case tx := <-outerHeld:
			outers = append(outers, tx)
		case <-time.After(3 * time.Second):
			t.Fatal("setup timed out waiting for outer transactions")
		}
	}

	if st := db.Stats(); st.InUse < workers {
		t.Fatalf("setup: expected %d connections in use, got %d", workers, st.InUse)
	}
	close(start)

	// 池已被外层占满：观察窗口内 locator 绝不能完成
	select {
	case err := <-finished:
		t.Fatalf("locator completed while the pool was exhausted (err=%v): setup too slow", err)
	case <-time.After(300 * time.Millisecond):
	}
	if wc := db.Stats().WaitCount; wc == 0 {
		t.Error("expected pool waiters: locator requests should be queued behind the held outer connections")
	}

	// 外部干预：释放外层连接 → 排队者立刻前进（证明它们只是被池容量卡住）
	for _, tx := range outers {
		if err := tx.Rollback(); err != nil {
			t.Errorf("release outer tx: %v", err)
		}
	}

	for i := 0; i < workers; i++ {
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("locator after pool freed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("hold-and-wait: locators still blocked after the pool was freed")
		}
	}
}
