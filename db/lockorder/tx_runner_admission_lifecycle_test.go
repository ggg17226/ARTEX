package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ---------------------------------------------------------------------------
// R18 C-1 / I-1 / I-2：准入许可生命周期、获取点位置、unknown-outcome 短路
//
// C-1（必须）：许可归还必须由 defer 完成，payload panic / runtime.Goexit 的栈展开
//
//	不得泄漏进程级许可（旧实现是 runAttempt 返回后的普通语句，16 次 panic 即全局自锁）。
//
// I-1（重要）：许可获取点必须在**本地门禁之后、BeginTx 之前**，
//
//	否则 unknown-outcome 永久封锁的 namespace 会把全局准入占满，故障扩散至全局。
//
// I-2（重要）：IsRetryableConflict 必须对 ErrOutcomeUnknown 短路。
// ---------------------------------------------------------------------------

func panicTag(i int) string { return fmt.Sprintf("boom-%d", i) }

// TestTxRunner_PayloadPanic_ReleasesAdmissionPermit R18 C-1 主证据：
// 16 并发、共 20 次 payload panic 之后，准入许可必须完全健康
// （InUse 归零、零超时），后续 Run 绝不得出现 ErrAdmissionTimeout。
//
// 旧实现（acquire → runAttempt → 普通语句 release）在本用例的第 17 次 panic 后
// 就会让 Run 卡在准入等待上 5s 然后返回 ErrAdmissionTimeout。
func TestTxRunner_PayloadPanic_ReleasesAdmissionPermit(t *testing.T) {
	const (
		totalPanics = 20
		workers     = 16
	)

	db := getMockDB(t)
	defer db.Close()

	// 与生产默认同容量（16）：许可若按次泄漏，第 17 次 Run 必然拿不到许可。
	adm := NewAdmissionControl(DefaultRunnerAdmissionCapacity, 5*time.Second)
	runner := NewTxRunner(db)

	tasks := make(chan int, totalPanics)
	for i := 0; i < totalPanics; i++ {
		tasks <- i
	}
	close(tasks)

	var wg sync.WaitGroup
	var panicked, payloadCalls int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				func() {
					defer func() {
						p := recover()
						if p == nil {
							t.Errorf("task %d: expected Run to re-panic", i)
							return
						}
						if s, ok := p.(string); !ok || s != panicTag(i) {
							t.Errorf("task %d: expected original panic value %q, got %#v", i, panicTag(i), p)
						}
						atomic.AddInt64(&panicked, 1)
					}()
					err := runner.Run(context.Background(), TxRunnerOptions{Admission: adm},
						func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
							atomic.AddInt64(&payloadCalls, 1)
							panic(panicTag(i))
						})
					t.Errorf("task %d: expected Run to re-panic, returned err=%v", i, err)
				}()
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&panicked); got != totalPanics {
		t.Fatalf("expected %d re-panicked Runs, got %d", totalPanics, got)
	}
	if got := atomic.LoadInt64(&payloadCalls); got != totalPanics {
		t.Fatalf("expected %d payload entries (no retry on panic), got %d", totalPanics, got)
	}

	st := adm.Stats()
	if st.InUse != 0 {
		t.Errorf("panic 路径泄漏准入许可：InUse=%d（capacity %d）", st.InUse, st.Capacity)
	}
	if st.Waiting != 0 {
		t.Errorf("expected no waiters left, got %+v", st)
	}
	if st.Timeouts != 0 {
		t.Errorf("expected zero admission timeouts, got %d（许可泄漏的直接症状）", st.Timeouts)
	}
	if got := adm.Available(); got != DefaultRunnerAdmissionCapacity {
		t.Errorf("expected all %d permits available, got %d", DefaultRunnerAdmissionCapacity, got)
	}

	// 许可池必须仍然可用：一次正常 Run 必须直接通过
	if err := runner.Run(context.Background(), TxRunnerOptions{Admission: adm},
		func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error { return nil }); err != nil {
		t.Fatalf("20 次 panic 之后 Run 仍应正常放行，got %v (ErrAdmissionTimeout=%v)",
			err, errors.Is(err, ErrAdmissionTimeout))
	}
}

// TestTxRunner_PayloadGoexit_ReleasesAdmissionPermit C-1 的另一半：payload 内
// runtime.Goexit（典型误用：在 payload 里调 t.Fatal）同样不得泄漏许可。
// Goexit 会执行所有 defer 后终止当前 goroutine，因此只有 defer 归还才安全。
func TestTxRunner_PayloadGoexit_ReleasesAdmissionPermit(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	adm := NewAdmissionControl(1, 2*time.Second)
	runner := NewTxRunner(db)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(context.Background(), TxRunnerOptions{Admission: adm},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				runtime.Goexit()
				return nil
			})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("payload Goexit 后 Run 未返回：defer 清理链未执行")
	}

	if st := adm.Stats(); st.InUse != 0 {
		t.Errorf("Goexit 路径泄漏准入许可：InUse=%d, stats=%+v", st.InUse, st)
	}
	if err := runner.Run(context.Background(), TxRunnerOptions{Admission: adm},
		func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error { return nil }); err != nil {
		t.Fatalf("Goexit 之后 Run 仍应正常放行，got %v", err)
	}
}

// TestTxRunner_PayloadError_ReleasesAdmissionPermit 业务错误与 unknown-outcome 两条
// 返回路径都必须归还许可（R18 M6 覆盖缺口）：
//   - payload 返回不可重试业务错误；
//   - Commit 返回协议失步 → unknown-outcome：门禁按设计继续 hold，但许可必须归还
//     （否则 unknown-outcome 越多、准入越紧，正是 I-1 要消除的扩散路径）。
func TestTxRunner_PayloadError_ReleasesAdmissionPermit(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	t.Run("payload business error", func(t *testing.T) {
		adm := NewAdmissionControl(1, time.Second)
		bizErr := errors.New("business rule broken")

		err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{Admission: adm},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				return bizErr
			})
		if !errors.Is(err, bizErr) {
			t.Fatalf("expected %v, got %v", bizErr, err)
		}
		if st := adm.Stats(); st.InUse != 0 {
			t.Errorf("业务错误路径泄漏许可：InUse=%d, stats=%+v", st.InUse, st)
		}
	})

	t.Run("commit outcome unknown holds gates but returns permit", func(t *testing.T) {
		globalMockDriver.mu.Lock()
		globalMockDriver.commitFn = func() error { return mysql.ErrPktSync }
		globalMockDriver.mu.Unlock()
		defer func() {
			globalMockDriver.mu.Lock()
			globalMockDriver.commitFn = nil
			globalMockDriver.mu.Unlock()
		}()

		adm := NewAdmissionControl(1, time.Second)
		cgm := newCountingGateManager()

		err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
			Admission:            adm,
			GateManager:          cgm,
			RequiresBusinessGate: true,
			RequiredNamespaces:   []string{"w6cb-c1-unknown"},
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
			return nil
		})
		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("expected ErrOutcomeUnknown, got %v", err)
		}
		if st := adm.Stats(); st.InUse != 0 {
			t.Errorf("unknown-outcome 路径泄漏许可：InUse=%d, stats=%+v", st.InUse, st)
		}
		// 门禁必须仍被 hold（I7 fail closed 不受本次修复影响）
		cgm.mu.Lock()
		heldBusiness, releasedBusiness := cgm.acquireBusiness, cgm.releaseBusiness
		heldNS, releasedNS := cgm.acquireNamespaces["w6cb-c1-unknown"], cgm.releaseNamespaces["w6cb-c1-unknown"]
		cgm.mu.Unlock()
		if heldBusiness != 1 || releasedBusiness != 0 {
			t.Errorf("business gate must stay held: acquire=%d release=%d", heldBusiness, releasedBusiness)
		}
		if heldNS != 1 || releasedNS != 0 {
			t.Errorf("namespace gate must stay held: acquire=%d release=%d", heldNS, releasedNS)
		}
		// 许可已归还 → 下一个 Run（不同门禁域）必须能拿到许可。
		// 先撤掉 commitFn 注入，否则这次 Run 也会撞上同一个 ErrPktSync。
		globalMockDriver.mu.Lock()
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()
		if err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
			Admission: adm,
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error { return nil }); err != nil {
			t.Fatalf("许可必须在 unknown-outcome 后归还，got %v", err)
		}
	})
}

// orderingGateManager 记录"取得本地门禁的那一刻，准入许可是否已被持有"（R18 I-1 顺序证据）。
type orderingGateManager struct {
	inner     GateManager
	admission *AdmissionControl

	mu               sync.Mutex
	inUseAtAcquire   []int64
	inUseAtRelease   []int64
	acquireNamespace string
}

func (g *orderingGateManager) AcquireLocalBusiness(ctx context.Context) error {
	g.sampleAcquire()
	return g.inner.AcquireLocalBusiness(ctx)
}

func (g *orderingGateManager) ReleaseLocalBusiness() {
	g.sampleRelease()
	g.inner.ReleaseLocalBusiness()
}

func (g *orderingGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	g.sampleAcquire()
	g.mu.Lock()
	g.acquireNamespace = ns
	g.mu.Unlock()
	return g.inner.AcquireLocalNamespace(ctx, ns)
}

func (g *orderingGateManager) ReleaseLocalNamespace(ns string) {
	g.sampleRelease()
	g.inner.ReleaseLocalNamespace(ns)
}

func (g *orderingGateManager) sampleAcquire() {
	g.mu.Lock()
	g.inUseAtAcquire = append(g.inUseAtAcquire, g.admission.Stats().InUse)
	g.mu.Unlock()
}

func (g *orderingGateManager) sampleRelease() {
	g.mu.Lock()
	g.inUseAtRelease = append(g.inUseAtRelease, g.admission.Stats().InUse)
	g.mu.Unlock()
}

func (g *orderingGateManager) snapshot() (acq, rel []int64, ns string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.inUseAtAcquire...), append([]int64(nil), g.inUseAtRelease...), g.acquireNamespace
}

// TestTxRunner_AcquiresAdmissionAfterLocalGates R18 I-1 的顺序证据：
//  1. 取得本地门禁的那一刻，准入许可**尚未**被持有（InUse 必须为 0）；
//  2. payload 执行期间（已持外层连接）许可**必须**被持有（InUse==1）——
//     后移许可不等于放弃对"持连接窗口"的限流；
//  3. 释放本地门禁时许可仍被持有（归还严格晚于回滚与门禁释放）。
func TestTxRunner_AcquiresAdmissionAfterLocalGates(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	adm := NewAdmissionControl(4, time.Second)
	gm := &orderingGateManager{inner: NewMemoryGateManager(), admission: adm}
	ns := "w6cb-i1-order"

	var inUseInPayload int64 = -1
	err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
		Admission:            adm,
		GateManager:          gm,
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{ns},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		atomic.StoreInt64(&inUseInPayload, adm.Stats().InUse)
		return nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	acq, rel, gotNS := gm.snapshot()
	if len(acq) != 2 {
		t.Fatalf("expected 2 local gate acquisitions (business + ns), got %v", acq)
	}
	for i, v := range acq {
		if v != 0 {
			t.Errorf("gate acquire #%d 发生在准入许可之前：InUse=%d（I-1 要求许可后移到门禁之后）", i, v)
		}
	}
	if gotNS != ns {
		t.Errorf("expected namespace %q, got %q", ns, gotNS)
	}
	if got := atomic.LoadInt64(&inUseInPayload); got != 1 {
		t.Errorf("payload 持外层连接期间必须持有准入许可：InUse=%d, want 1", got)
	}
	if len(rel) != 2 {
		t.Fatalf("expected 2 local gate releases, got %v", rel)
	}
	for i, v := range rel {
		if v != 1 {
			t.Errorf("gate release #%d 时许可已提前归还（InUse=%d）：许可必须晚于回滚与门禁释放归还", i, v)
		}
	}
	if st := adm.Stats(); st.InUse != 0 {
		t.Errorf("Run 返回后许可必须归还：InUse=%d, stats=%+v", st.InUse, st)
	}
}

// TestTxRunner_BlockedOnGateConsumesNoAdmissionPermit R18 I-1 的故障扩散反证：
// 某 namespace 因 unknown-outcome 被永久 hold（本用例用真实互斥门禁复现），
// 3 个 context.Background() 请求排在门禁上；准入容量 2、等待上限 150ms。
//
// 旧顺序（许可 → 门禁）下：前 2 个请求各占 1 个许可并永久卡在门禁上，
// 第 3 个请求 150ms 后拿到 ErrAdmissionTimeout —— 无关请求被单个 namespace 拖死。
// 新顺序（门禁 → 许可）下：3 个请求全部卡在门禁上，一个许可都不占、零超时，
// 门禁释放后全部正常完成。
func TestTxRunner_BlockedOnGateConsumesNoAdmissionPermit(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	const (
		waiters       = 3
		admissionCap  = 2
		admissionWait = 150 * time.Millisecond
		observeWindow = 600 * time.Millisecond
	)

	gm := &trackedGateManager{inner: NewMemoryGateManager(), tag: "i1"}
	ns := "w6cb-i1-poisoned"

	// 模拟 unknown-outcome 的永久封锁：门禁已被占用且无人归还
	if err := gm.inner.AcquireLocalNamespace(context.Background(), ns); err != nil {
		t.Fatalf("hold gate: %v", err)
	}

	adm := NewAdmissionControl(admissionCap, admissionWait)
	runner := NewTxRunner(db)

	globalMockDriver.mu.Lock()
	txBefore := globalMockDriver.txStarted
	globalMockDriver.mu.Unlock()

	results := make(chan error, waiters)
	var payloadCalls int64
	for i := 0; i < waiters; i++ {
		go func() {
			results <- runner.Run(context.Background(), TxRunnerOptions{
				Admission:          adm,
				GateManager:        gm,
				RequiredNamespaces: []string{ns},
			}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				atomic.AddInt64(&payloadCalls, 1)
				return nil
			})
		}()
	}

	// 等 3 个请求都抵达门禁等待点
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		acquired, _, _ := gm.snapshot()
		if len(acquired) >= waiters {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	st := adm.Stats()
	if st.InUse != 0 {
		t.Errorf("卡在本地门禁上的请求不得占用准入许可：InUse=%d（I-1）", st.InUse)
	}
	if st.Timeouts != 0 {
		t.Errorf("门禁排队期间不应产生准入超时：Timeouts=%d（I-1 的扩散形态）", st.Timeouts)
	}
	if got := atomic.LoadInt64(&payloadCalls); got != 0 {
		t.Errorf("门禁未到手就进 payload：%d 次", got)
	}
	globalMockDriver.mu.Lock()
	txDuring := globalMockDriver.txStarted
	globalMockDriver.mu.Unlock()
	if txDuring != txBefore {
		t.Errorf("门禁排队期间不得开启事务：txStarted %d → %d", txBefore, txDuring)
	}

	select {
	case err := <-results:
		t.Fatalf("waiter 在门禁被 hold 期间就返回了（旧顺序的 ErrAdmissionTimeout 扩散）：%v", err)
	case <-time.After(observeWindow):
	}

	// 解除封锁：3 个请求必须全部完成，且没有任何准入超时
	gm.inner.ReleaseLocalNamespace(ns)

	for i := 0; i < waiters; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("waiter %d: %v (ErrAdmissionTimeout=%v)", i, err, errors.Is(err, ErrAdmissionTimeout))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d 在门禁释放后仍未完成", i)
		}
	}
	if got := atomic.LoadInt64(&payloadCalls); got != waiters {
		t.Errorf("expected %d payload runs, got %d", waiters, got)
	}
	if st := adm.Stats(); st.InUse != 0 {
		t.Errorf("expected all permits returned, got %+v", st)
	}
}

// ---------------------------------------------------------------------------
// R18 I-2：ErrOutcomeUnknown 一律不可重试
// ---------------------------------------------------------------------------

func TestIsRetryableConflict_OutcomeUnknownNeverRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"bare sentinel", ErrOutcomeUnknown},
		{"unknown + 1213 deadlock", fmt.Errorf("%w: %w", ErrOutcomeUnknown,
			&mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"})},
		{"unknown + 1205 lock wait timeout", fmt.Errorf("%w: %w", ErrOutcomeUnknown,
			&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"})},
		{"unknown + 1062 duplicate entry", fmt.Errorf("%w: commit error not provably pre-send: %w", ErrOutcomeUnknown,
			&mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'PRIMARY'"})},
		{"unknown + 包装过的 1213", fmt.Errorf("commit failed: %w", fmt.Errorf("%w: %w", ErrOutcomeUnknown,
			&mockMySQLError{ErrNumber: 1213, Message: "Deadlock"}))},
		{"unknown + ctx 取消", fmt.Errorf("%w: %w", ErrOutcomeUnknown, context.DeadlineExceeded)},
	}
	for _, tc := range cases {
		if IsRetryableConflict(tc.err) {
			t.Errorf("%s: unknown-outcome 绝不可判为可重试（会导致重复提交）：%v", tc.name, tc.err)
		}
	}

	// 反面对照：不含 ErrOutcomeUnknown 的同类编号仍然可重试（短路没有误伤正常分类）
	for _, num := range []uint16{1062, 1205, 1213} {
		err := fmt.Errorf("exec: %w", &mysql.MySQLError{Number: num, Message: "conflict"})
		if !IsRetryableConflict(err) {
			t.Errorf("plain MySQL %d must stay retryable", num)
		}
	}
	if !IsRetryableConflict(ErrMembershipDrift) || !IsRetryableConflict(ErrAdmissionTimeout) {
		t.Error("漂移/准入哨兵必须保持可重试")
	}
	if IsRetryableConflict(nil) {
		t.Error("nil must not be retryable")
	}
}

// TestRun_OutcomeUnknownIsNotRetriedByRunner 端到端：Commit 返回 1213 时按 I7 判为
// unknown-outcome 并立即返回（门禁 hold），runner 自身绝不重放 payload。
func TestRun_OutcomeUnknownIsNotRetriedByRunner(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	globalMockDriver.mu.Lock()
	globalMockDriver.commitFn = func() error {
		return &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	}
	globalMockDriver.mu.Unlock()
	defer func() {
		globalMockDriver.mu.Lock()
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()
	}()

	var payloadCalls int
	adm := NewAdmissionControl(2, time.Second)
	err := NewTxRunner(db).Run(context.Background(), TxRunnerOptions{
		Admission:   adm,
		GateManager: newCountingGateManager(),
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		payloadCalls++
		return nil
	})

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("expected ErrOutcomeUnknown, got %v", err)
	}
	if payloadCalls != 1 {
		t.Errorf("unknown-outcome 必须立即返回，不得重放 payload：payload 进入 %d 次", payloadCalls)
	}
	if IsRetryableConflict(err) {
		t.Errorf("runner 返回给上层的错误必须被判为不可重试：%v", err)
	}
	if st := adm.Stats(); st.InUse != 0 {
		t.Errorf("expected permits returned, got %+v", st)
	}
}
