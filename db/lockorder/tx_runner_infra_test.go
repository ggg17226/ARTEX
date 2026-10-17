package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// C1：默认 GateManager 进程级共享
// ---------------------------------------------------------------------------

func TestDefaultGateManager_IsProcessWideSingleton(t *testing.T) {
	if DefaultGateManager() != DefaultGateManager() {
		t.Fatal("DefaultGateManager must return the same instance across calls")
	}
}

// TestTxRunner_DefaultGatesSerializeSameNamespace 两个使用默认门禁的并发 Run
// （不同 TxRunner、不同 *sql.DB）必须在同一 namespace 的本地门禁层互斥（R2 C1）。
func TestTxRunner_DefaultGatesSerializeSameNamespace(t *testing.T) {
	ns := "w6a-c1-shared-" + t.Name()

	dbA := getMockDB(t)
	defer dbA.Close()
	dbB := getMockDB(t)
	defer dbB.Close()

	var (
		mu     sync.Mutex
		events []string
		record = func(ev string) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		}
	)

	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	defer wg.Wait() // 先注册：最后执行，确保两个 Run 都退出
	defer release()

	runA := NewTxRunner(dbA)
	runB := NewTxRunner(dbB)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := runA.Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("A:enter")
				close(enteredA)
				<-releaseA
				record("A:exit")
				return nil
			})
		if err != nil {
			t.Errorf("run A failed: %v", err)
		}
	}()

	// 等 A 真正进入 payload（即已持有 ns 门禁）后再启动 B，避免调度顺序造成的假阴性
	select {
	case <-enteredA:
	case <-time.After(5 * time.Second):
		t.Fatal("run A never entered payload")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := runB.Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("B:enter")
				return nil
			})
		if err != nil {
			t.Errorf("run B failed: %v", err)
		}
	}()

	// A 仍持有门禁期间，B 不得进入 payload
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	observed := append([]string(nil), events...)
	mu.Unlock()
	if len(observed) != 1 || observed[0] != "A:enter" {
		t.Fatalf("namespace gate did not serialize two concurrent Runs, events=%v", observed)
	}

	release()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	want := []string{"A:enter", "A:exit", "B:enter"}
	if len(events) != len(want) {
		t.Fatalf("unexpected event sequence: got %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("unexpected event sequence: got %v, want %v", events, want)
		}
	}
}

// TestTxRunner_UnknownCommitHoldsSharedGate_BlocksNextRun unknown-outcome 分支 hold 住的
// 默认门禁必须继续阻塞后续 Run（R2 C1：fail closed 在默认 API 下真实生效）。
func TestTxRunner_UnknownCommitHoldsSharedGate_BlocksNextRun(t *testing.T) {
	ns := "w6a-c1-hold-" + t.Name()

	db1 := getMockDB(t)
	defer db1.Close()
	db2 := getMockDB(t)
	defer db2.Close()

	// 测试结束释放被刻意 hold 的共享门禁，避免污染同进程后续用例
	defer DefaultGateManager().ReleaseLocalNamespace(ns)

	globalMockDriver.mu.Lock()
	globalMockDriver.commitFn = func() error { return driver.ErrBadConn }
	globalMockDriver.mu.Unlock()
	defer func() {
		globalMockDriver.mu.Lock()
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()
	}()

	// Run #1：提交结果未知 → 返回 ErrOutcomeUnknown 且不释放门禁
	err1 := NewTxRunner(db1).Run(context.Background(), TxRunnerOptions{
		RequiredNamespaces: []string{ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})
	if !errors.Is(err1, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown, got %v", err1)
	}

	// Run #2：默认门禁（同一单例）→ 必须阻塞在 ns 门禁上，被 ctx 取消
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

// ---------------------------------------------------------------------------
// I1：panic 路径清理
// ---------------------------------------------------------------------------

// TestTxRunner_PayloadPanic_ReleasesTxAndGates payload panic 后必须回滚外层事务、
// 逆序释放全部门禁；panic 以原值重新抛出（R2 I1）。
func TestTxRunner_PayloadPanic_ReleasesTxAndGates(t *testing.T) {
	ns := "w6a-i1-panic-" + t.Name()

	db := getMockDB(t)
	defer db.Close()

	globalMockDriver.mu.Lock()
	rollbacksBefore := globalMockDriver.txRolledBack
	globalMockDriver.mu.Unlock()

	sentinel := "boom from payload"

	func() {
		defer func() {
			p := recover()
			if p == nil {
				t.Error("expected panic to propagate out of Run after cleanup")
				return
			}
			if s, ok := p.(string); !ok || s != sentinel {
				t.Errorf("expected original panic value %q, got %#v", sentinel, p)
			}
		}()
		err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
			RequiresBusinessGate: true,
			RequiredNamespaces:   []string{ns},
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
			panic(sentinel)
		})
		t.Errorf("expected Run to re-panic, returned err=%v", err)
	}()

	globalMockDriver.mu.Lock()
	rollbacksAfter := globalMockDriver.txRolledBack
	globalMockDriver.mu.Unlock()
	if rollbacksAfter-rollbacksBefore != 1 {
		t.Errorf("expected exactly 1 driver rollback on panic path, got %d", rollbacksAfter-rollbacksBefore)
	}

	// 门禁（business + namespace）必须已全部释放：同 namespace 的后续 Run 应正常完成
	db2 := getMockDB(t)
	defer db2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := NewTxRunner(db2).Run(ctx, TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	}); err != nil {
		t.Fatalf("gates/locks not released after panic: %v", err)
	}
}

// ---------------------------------------------------------------------------
// I3：门禁等待与退避可取消
// ---------------------------------------------------------------------------

func TestMemoryGateManager_WaitIsCancellable(t *testing.T) {
	gm := NewMemoryGateManager()
	bg := context.Background()

	if err := gm.AcquireLocalNamespace(bg, "ns-block"); err != nil {
		t.Fatalf("acquire ns-block: %v", err)
	}
	if err := gm.AcquireLocalBusiness(bg); err != nil {
		t.Fatalf("acquire business: %v", err)
	}

	// 超时取消
	ctxTimeout, cancelTimeout := context.WithTimeout(bg, 80*time.Millisecond)
	defer cancelTimeout()
	start := time.Now()
	err := gm.AcquireLocalNamespace(ctxTimeout, "ns-block")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded from namespace gate, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("namespace gate wait not cancellable, took %v", elapsed)
	}

	// 主动取消（business gate）
	ctxCancel, cancel := context.WithCancel(bg)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start = time.Now()
	err = gm.AcquireLocalBusiness(ctxCancel)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled from business gate, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("business gate wait not cancellable, took %v", elapsed)
	}

	// 取消等待不得破坏持有者语义：持有者释放后新请求可获取
	gm.ReleaseLocalNamespace("ns-block")
	gm.ReleaseLocalBusiness()
	ctx2, cancel2 := context.WithTimeout(bg, 2*time.Second)
	defer cancel2()
	if err := gm.AcquireLocalNamespace(ctx2, "ns-block"); err != nil {
		t.Errorf("re-acquire after release failed: %v", err)
	}
	gm.ReleaseLocalNamespace("ns-block")
}

// TestTxRunner_BackoffSleepIsCancellable 退避休眠期间 ctx 取消必须立即返回（R2 I3）。
func TestTxRunner_BackoffSleepIsCancellable(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db) // 默认 sleepContext，不走 SetSleeper

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := runner.Run(ctx, TxRunnerOptions{
		BackoffSchedule: []time.Duration{30 * time.Second},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return ErrMembershipDrift
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled during backoff, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("backoff sleep not cancellable, took %v", elapsed)
	}
}

// ---------------------------------------------------------------------------
// I4：namespaces 排序去重
// ---------------------------------------------------------------------------

func TestNormalizeNamespaces(t *testing.T) {
	got := normalizeNamespaces([]string{"ns-b", "ns-a", "ns-b", "ns-a", "ns-c", "ns-b"})
	want := []string{"ns-a", "ns-b", "ns-c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if normalizeNamespaces(nil) != nil {
		t.Errorf("expected nil for nil input")
	}
	// 输入切片不得被修改
	src := []string{"z", "a", "z"}
	_ = normalizeNamespaces(src)
	if src[0] != "z" || src[1] != "a" || src[2] != "z" {
		t.Errorf("normalizeNamespaces mutated caller slice: %v", src)
	}
}

// recordingGateManager 记录获取/释放顺序的 GateManager 测试替身
type recordingGateManager struct {
	mu       sync.Mutex
	acquired []string
	released []string
}

func (g *recordingGateManager) AcquireLocalBusiness(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acquired = append(g.acquired, "business")
	return nil
}

func (g *recordingGateManager) ReleaseLocalBusiness() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = append(g.released, "business")
}

func (g *recordingGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acquired = append(g.acquired, "ns:"+ns)
	return nil
}

func (g *recordingGateManager) ReleaseLocalNamespace(ns string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = append(g.released, "ns:"+ns)
}

func TestTxRunner_DuplicateNamespacesDeduped(t *testing.T) {
	rec := &recordingGateManager{}

	db := getMockDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := NewTxRunner(db).Run(ctx, TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"ns-dup", "ns-a", "ns-dup", "ns-dup", "ns-a"},
		GateManager:          rec,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	wantAcq := []string{"business", "ns:ns-a", "ns:ns-dup"}
	wantRel := []string{"ns:ns-dup", "ns:ns-a", "business"}
	if len(rec.acquired) != len(wantAcq) {
		t.Fatalf("unexpected acquisitions: got %v, want %v", rec.acquired, wantAcq)
	}
	for i := range wantAcq {
		if rec.acquired[i] != wantAcq[i] {
			t.Fatalf("unexpected acquisitions: got %v, want %v", rec.acquired, wantAcq)
		}
	}
	if len(rec.released) != len(wantRel) {
		t.Fatalf("unexpected releases: got %v, want %v", rec.released, wantRel)
	}
	for i := range wantRel {
		if rec.released[i] != wantRel[i] {
			t.Fatalf("unexpected releases: got %v, want %v", rec.released, wantRel)
		}
	}
}

// TestTxRunner_DuplicateNamespacesNoSelfLock 真实互斥门禁下重复 namespace 不得自锁（R2 I4）
func TestTxRunner_DuplicateNamespacesNoSelfLock(t *testing.T) {
	ns := "w6a-i4-dup-" + t.Name()
	db := getMockDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := NewTxRunner(db).Run(ctx, TxRunnerOptions{
		RequiredNamespaces: []string{ns, ns, ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	}); err != nil {
		t.Fatalf("duplicate namespaces must be deduped, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// I9：nil DB / nil tx 显式报错
// ---------------------------------------------------------------------------

func TestTxRunner_NilDatabaseRejected(t *testing.T) {
	payloadCalls := 0
	err := NewTxRunner(nil).Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"ns-nil"},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		payloadCalls++
		return nil
	})

	if !errors.Is(err, ErrNilDatabase) {
		t.Fatalf("expected ErrNilDatabase, got %v", err)
	}
	if payloadCalls != 0 {
		t.Errorf("payload must not run with nil db, calls=%d", payloadCalls)
	}
}

func TestLockAPIs_NilTxRejected(t *testing.T) {
	ctx := context.Background()
	keys := []EntityKey{{Table: TableOrderTasks, PK: 1}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{Key: key, Live: true}, nil
	}

	if err := LockEntitiesInOrder(ctx, nil, NewLockContext(), keys); !errors.Is(err, ErrNilTx) {
		t.Errorf("LockEntitiesInOrder: expected ErrNilTx, got %v", err)
	}
	if _, err := LockAndVerify(ctx, nil, NewLockContext(), snapshot, verifier); !errors.Is(err, ErrNilTx) {
		t.Errorf("LockAndVerify: expected ErrNilTx, got %v", err)
	}

	// 锁序前置校验优先于 nil tx 检查（保持既有语义）
	unsorted := []EntityKey{{Table: TableOrderTasks, PK: 20}, {Table: TableOrderTasks, PK: 10}}
	if err := LockEntitiesInOrder(ctx, nil, NewLockContext(), unsorted); !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock precedence preserved, got %v", err)
	}
}
