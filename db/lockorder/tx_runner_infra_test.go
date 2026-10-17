package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 测试替身：委托型可观察门禁（R11 M-R11-2 握手用）
// ---------------------------------------------------------------------------

// trackedGateManager 把门禁调用原样委托给 inner（默认传 DefaultGateManager()，
// 互斥语义完全由被测实现提供，本类型不参与加锁），只额外暴露：
//   - hook：每次"进入等待 / 等待返回 / 释放"事件，供测试做确定性握手；
//   - acquired/released：获取与释放的顺序；
//   - releaseRollbacks：每次释放时的 driver 回滚计数，用于断言"回滚先于释放"。
type trackedGateManager struct {
	inner GateManager
	tag   string

	// rollbackCount 非 nil 时，release 事件会附带当时的 driver 回滚次数
	rollbackCount func() int
	// hook 非 nil 时在锁外被调用（用于向 channel 推事件）
	hook func(event string)

	mu               sync.Mutex
	acquired         []string
	released         []string
	releaseRollbacks []int
}

func (g *trackedGateManager) emit(event string) {
	if g.hook != nil {
		g.hook(g.tag + ":" + event)
	}
}

func (g *trackedGateManager) AcquireLocalBusiness(ctx context.Context) error {
	g.mu.Lock()
	g.acquired = append(g.acquired, "business")
	g.mu.Unlock()
	g.emit("acquire-start:business")
	err := g.inner.AcquireLocalBusiness(ctx)
	g.emit("acquire-return:business")
	return err
}

func (g *trackedGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	g.mu.Lock()
	g.acquired = append(g.acquired, "ns:"+ns)
	g.mu.Unlock()
	g.emit("acquire-start:ns:" + ns)
	err := g.inner.AcquireLocalNamespace(ctx, ns)
	g.emit("acquire-return:ns:" + ns)
	return err
}

func (g *trackedGateManager) ReleaseLocalBusiness() {
	g.recordRelease("business")
	g.inner.ReleaseLocalBusiness()
}

func (g *trackedGateManager) ReleaseLocalNamespace(ns string) {
	g.recordRelease("ns:" + ns)
	g.inner.ReleaseLocalNamespace(ns)
}

func (g *trackedGateManager) recordRelease(name string) {
	rollbacks := -1
	if g.rollbackCount != nil {
		rollbacks = g.rollbackCount()
	}

	g.mu.Lock()
	g.released = append(g.released, name)
	if rollbacks >= 0 {
		g.releaseRollbacks = append(g.releaseRollbacks, rollbacks)
	}
	g.mu.Unlock()

	g.emit("release:" + name)
}

// snapshot 返回获取序、释放序，以及每次释放时的 driver 回滚计数副本
func (g *trackedGateManager) snapshot() (acquired, released []string, releaseRollbacks []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.acquired...),
		append([]string(nil), g.released...),
		append([]int(nil), g.releaseRollbacks...)
}

// waitGateEvent 在 timeout 内等待指定事件出现（期间出现的其他事件被丢弃）。
// 超时视为测试失败：它只用于证明"被测阶段确实已经发生"，不作为放行手段。
func waitGateEvent(t *testing.T, ch <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ev := <-ch:
			if ev == want {
				return
			}
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for gate event %q", timeout, want)
		}
	}
}

// newGateEventHook 返回事件通道与配套的 hook：通道容量足够本包用例，满则丢弃
func newGateEventHook() (chan string, func(string)) {
	ch := make(chan string, 64)
	return ch, func(ev string) {
		select {
		case ch <- ev:
		default:
		}
	}
}

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

	// 握手（R11 M-R11-2 第 1 点）：两个 Run 各自包一层"委托给默认单例"的门禁管理器。
	// 互斥仍由 DefaultGateManager() 这同一实例提供（被测语义不变），包装只暴露
	// "已到达门禁 / 门禁已返回"事件，用来确定性地证明 B 确实在 A 持有期间到达门禁，
	// 而不是靠 time.Sleep 猜 B 有没有被调度。
	gateEvents, hook := newGateEventHook()
	gmA := &trackedGateManager{inner: DefaultGateManager(), tag: "A", hook: hook}
	gmB := &trackedGateManager{inner: DefaultGateManager(), tag: "B", hook: hook}

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
		err := runA.Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}, GateManager: gmA},
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
	// A 的门禁获取早于 payload 返回，先把这两个事件排掉，后面只观察 B 的事件
	waitGateEvent(t, gateEvents, "A:acquire-return:ns:"+ns, 5*time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := runB.Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}, GateManager: gmB},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("B:enter")
				return nil
			})
		if err != nil {
			t.Errorf("run B failed: %v", err)
		}
	}()

	// 握手 1：B 确实已经到达共享 namespace 门禁（证明它已起跑并正在等待，
	// 而不是"还没被调度"，从而消除旧实现 200ms 窗口的假绿）
	waitGateEvent(t, gateEvents, "B:acquire-start:ns:"+ns, 5*time.Second)

	// 握手 2：A 仍持有门禁期间，B 既不得从门禁返回、也不得进入 payload。
	// 若实现退化为"每次 Run 新建 manager"，B 的 acquire 会立即返回并被这里捕获。
	// 下面 200ms 只是"本应立刻发生的事件"的观测窗口（B 已证明在 select 上等待），
	// 不是让 B 起跑的同步手段。
	select {
	case ev := <-gateEvents:
		t.Fatalf("namespace gate did not serialize two concurrent Runs: got %q while A still held the gate", ev)
	case <-time.After(200 * time.Millisecond):
	}

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
// 逆序释放全部门禁（回滚严格先于释放）、Reset 锁上下文；panic 以原值重新抛出，
// 且不得被退避重试吞掉（R2 I1，R11 M-R11-2 第 3 点补齐三项断言）。
func TestTxRunner_PayloadPanic_ReleasesTxAndGates(t *testing.T) {
	nsA := "w6a-i1-panic-a-" + t.Name()
	nsB := "w6a-i1-panic-b-" + t.Name()

	db := getMockDB(t)
	defer db.Close()

	rollbackCount := func() int {
		globalMockDriver.mu.Lock()
		defer globalMockDriver.mu.Unlock()
		return globalMockDriver.txRolledBack
	}
	rollbacksBefore := rollbackCount()

	sentinel := "boom from payload"

	var (
		panicLockCtx  *LockContext
		historyBefore int
		payloadCalls  int
	)

	// 委托默认单例：门禁真的被持有/释放（保留"后续 Run 能正常进入"这一强断言），
	// 同时可观察获取/释放顺序与"回滚是否先于释放"。
	gm := &trackedGateManager{inner: DefaultGateManager(), tag: "panic", rollbackCount: rollbackCount}

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
			// 故意逆序传入：runner 必须规范化为 nsA → nsB
			RequiredNamespaces: []string{nsB, nsA},
			GateManager:        gm,
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
			payloadCalls++
			panicLockCtx = lockCtx
			historyBefore = len(lockCtx.ReleaseEvents())
			panic(sentinel)
		})
		t.Errorf("expected Run to re-panic, returned err=%v", err)
	}()

	// 1) panic 不得被重试吞掉：payload 恰好进入一次
	if payloadCalls != 1 {
		t.Fatalf("payload must run exactly once on panic path, got %d", payloadCalls)
	}

	// 2) 外层事务必须恰好回滚一次
	if got := rollbackCount() - rollbacksBefore; got != 1 {
		t.Errorf("expected exactly 1 driver rollback on panic path, got %d", got)
	}

	// 3) lockCtx 必须被 Reset：panic 前非空（business + 2 ns 的本地与 DB 门禁事件），
	//    清理后必须为空 —— 删掉 cleanup 里的 lockCtx.Reset() 本断言即失败
	if historyBefore == 0 {
		t.Fatal("payload observed an empty lock history; the Reset assertion below would prove nothing")
	}
	if after := len(panicLockCtx.ReleaseEvents()); after != 0 {
		t.Errorf("lockCtx.Reset() not applied on panic cleanup: history still has %d events", after)
	}

	// 4) 获取顺序（business → nsA → nsB）与释放顺序严格逆序（nsB → nsA → business）
	acquired, released, releaseRollbacks := gm.snapshot()
	wantAcq := []string{"business", "ns:" + nsA, "ns:" + nsB}
	wantRel := []string{"ns:" + nsB, "ns:" + nsA, "business"}
	if len(acquired) != len(wantAcq) || len(released) != len(wantRel) {
		t.Fatalf("unexpected gate ops: acquired=%v want %v; released=%v want %v",
			acquired, wantAcq, released, wantRel)
	}
	for i := range wantAcq {
		if acquired[i] != wantAcq[i] {
			t.Fatalf("unexpected acquire order: got %v, want %v", acquired, wantAcq)
		}
	}
	for i := range wantRel {
		if released[i] != wantRel[i] {
			t.Fatalf("unexpected release order (must be strictly reversed): got %v, want %v", released, wantRel)
		}
	}

	// 5) 回滚严格先于任何一次本地门禁释放
	if len(releaseRollbacks) != len(wantRel) {
		t.Fatalf("expected rollback count sampled on every release, got %v", releaseRollbacks)
	}
	for i, n := range releaseRollbacks {
		if n-rollbacksBefore < 1 {
			t.Fatalf("release #%d (%s) happened before the tx rollback (driver rollbacks=%d, before=%d)",
				i, released[i], n, rollbacksBefore)
		}
	}

	// 6) 门禁（business + 两个 namespace）必须已全部释放：同门禁域的后续 Run 应正常完成
	db2 := getMockDB(t)
	defer db2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := NewTxRunner(db2).Run(ctx, TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{nsA, nsB},
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

	// 主动取消（business gate）。business 门禁此刻已被本测试持有，因此
	// AcquireLocalBusiness 唯一的返回路径就是 ctx 取消：用"起跑握手 → cancel"
	// 取代 time.Sleep 触发取消（R11 M-R11-2），消除调度窗口。
	ctxCancel, cancel := context.WithCancel(bg)
	defer cancel()

	businessErr := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started) // 握手：等待方已起跑
		businessErr <- gm.AcquireLocalBusiness(ctxCancel)
	}()
	<-started

	start = time.Now()
	cancel()

	select {
	case err = <-businessErr:
	case <-time.After(2 * time.Second):
		t.Fatal("business gate wait never returned after cancellation")
	}
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
// R11 M-R11-2 第 2 点：只有确实观察到"已进入退避"后才取消，避免"从未进入 backoff
// 也被入口 ctx.Err() 检查放行"的漏检。
func TestTxRunner_BackoffSleepIsCancellable(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db) // 默认 sleepContext，不走 SetSleeper

	// 握手：包一层 sleepFn，只增加"已进入退避"信号与时长记录，
	// 内部仍调用生产默认 sleepContext（NewTxRunner 已把 sleepFn 指向它），被测对象不变。
	base := runner.sleepFn
	backoffEntered := make(chan time.Duration, 4)
	var (
		enteredMu  sync.Mutex
		enteredDur []time.Duration
	)
	runner.sleepFn = func(ctx context.Context, d time.Duration) error {
		enteredMu.Lock()
		enteredDur = append(enteredDur, d)
		enteredMu.Unlock()
		select {
		case backoffEntered <- d:
		default:
		}
		return base(ctx, d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	// 观察到退避进入之后才取消；若从未进入退避，则由下面的 enteredDur 断言失败暴露
	go func() {
		select {
		case <-backoffEntered:
			cancel()
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()

	payloadCalls := 0
	start := time.Now()
	err := runner.Run(ctx, TxRunnerOptions{
		BackoffSchedule: []time.Duration{30 * time.Second},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		payloadCalls++
		return ErrMembershipDrift
	})
	elapsed := time.Since(start)

	enteredMu.Lock()
	durations := append([]time.Duration(nil), enteredDur...)
	enteredMu.Unlock()

	if len(durations) != 1 || durations[0] != 30*time.Second {
		t.Fatalf("expected exactly 1 backoff sleep of 30s before cancellation, got %v", durations)
	}
	if payloadCalls != 1 {
		t.Errorf("expected exactly 1 payload attempt before the backoff, got %d", payloadCalls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled during backoff, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("backoff sleep not cancellable, took %v", elapsed)
	}
}

// TestSleepContext_CancelsWhileSleeping 直接证明默认退避休眠"真的在等待"，
// 并在 ctx 取消时立即返回：补上 Run 级用例无法区分的"入口 ctx.Err() 早退"与
// "select 等待中被取消"两条路径（R11 M-R11-2 第 2 点）。
func TestSleepContext_CancelsWhileSleeping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const sleepFor = 60 * time.Second

	done := make(chan error, 1)
	go func() { done <- sleepContext(ctx, sleepFor) }()

	// 60s 的休眠不得自行返回：若在取消前就返回，说明它没有真正等待
	// （这里的 100ms 是"60s vs 立刻"的观测窗口，不是被测阶段的同步手段）
	select {
	case err := <-done:
		t.Fatalf("sleepContext(%v) returned before cancellation: %v", sleepFor, err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from sleepContext, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sleepContext did not return after cancellation")
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

// TestLockAPIs_OrderCheckPrecedesNilTx R11 I-R11-1 回归：两个导出执行入口对
// "乱序 / 重复候选 + nil tx" 必须给出同一分类 ErrOutOfOrderLock（且不得命中 ErrNilTx）；
// "有序（含空候选集）+ nil tx" 仍必须 ErrNilTx fail closed。
func TestLockAPIs_OrderCheckPrecedesNilTx(t *testing.T) {
	ctx := context.Background()

	verifierCalls := 0
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		verifierCalls++
		return &EntityState{Key: key, Live: true}, nil
	}

	unsorted := []EntityKey{{Table: TableOrderTasks, PK: 20}, {Table: TableOrderTasks, PK: 10}}
	duplicate := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 10}}
	ordered := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}

	// ① 乱序 / 重复 + nil tx → 两入口一致先报 ErrOutOfOrderLock
	for _, tc := range []struct {
		name string
		keys []EntityKey
	}{
		{"unsorted", unsorted},
		{"duplicate", duplicate},
	} {
		err := LockEntitiesInOrder(ctx, nil, NewLockContext(), tc.keys)
		if !errors.Is(err, ErrOutOfOrderLock) || errors.Is(err, ErrNilTx) {
			t.Errorf("LockEntitiesInOrder(%s): expected ErrOutOfOrderLock only, got %v", tc.name, err)
		}

		_, err = LockAndVerify(ctx, nil, NewLockContext(), &LocatorSnapshot{CandidateKeys: tc.keys}, verifier)
		if !errors.Is(err, ErrOutOfOrderLock) {
			t.Errorf("LockAndVerify(%s): expected ErrOutOfOrderLock, got %v", tc.name, err)
		}
		if errors.Is(err, ErrNilTx) {
			t.Errorf("LockAndVerify(%s): nil tx must not mask the ordering error, got %v", tc.name, err)
		}
	}

	// ② 有序（含空候选集）+ nil tx → 两入口均 ErrNilTx，绝不"离线假成功"
	for _, tc := range []struct {
		name string
		keys []EntityKey
	}{
		{"ordered", ordered},
		{"empty", nil},
	} {
		lc := NewLockContext()
		if err := LockEntitiesInOrder(ctx, nil, lc, tc.keys); !errors.Is(err, ErrNilTx) {
			t.Errorf("LockEntitiesInOrder(%s): expected ErrNilTx, got %v", tc.name, err)
		} else if errors.Is(err, ErrOutOfOrderLock) {
			t.Errorf("LockEntitiesInOrder(%s): sorted keys must not report ordering error, got %v", tc.name, err)
		}
		if n := len(lc.ReleaseEvents()); n != 0 {
			t.Errorf("LockEntitiesInOrder(%s): rejected call must not record locks, history=%d", tc.name, n)
		}

		_, err := LockAndVerify(ctx, nil, NewLockContext(), &LocatorSnapshot{CandidateKeys: tc.keys}, verifier)
		if !errors.Is(err, ErrNilTx) {
			t.Errorf("LockAndVerify(%s): expected ErrNilTx, got %v", tc.name, err)
		} else if errors.Is(err, ErrOutOfOrderLock) {
			t.Errorf("LockAndVerify(%s): sorted keys must not report ordering error, got %v", tc.name, err)
		}
	}

	// ③ 可诊断性：组合入口的 nil tx 错误必须透出真正失败的步骤
	_, err := LockAndVerify(ctx, nil, NewLockContext(), &LocatorSnapshot{CandidateKeys: ordered}, verifier)
	if err == nil || !strings.Contains(err.Error(), "LockEntitiesInOrder") {
		t.Errorf("combined entry must keep the failing step in the nil tx message, got %v", err)
	}

	// ④ 加锁失败路径不得触达 verifier（不会"未加锁先核验"）
	if verifierCalls != 0 {
		t.Errorf("verifier must not be called when locking fails, calls=%d", verifierCalls)
	}

	// ⑤ nil snapshot 仍是组合入口独有的最前置检查，且不被上面两类错误掩盖
	if _, err := LockAndVerify(ctx, nil, NewLockContext(), nil, verifier); err == nil ||
		errors.Is(err, ErrNilTx) || errors.Is(err, ErrOutOfOrderLock) ||
		!strings.Contains(err.Error(), "snapshot") {
		t.Errorf("nil snapshot must be rejected first with its own error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// R11 M-R11-1：ctxMutex 所有权契约固化
// ---------------------------------------------------------------------------

// TestCtxMutex_ReleaseContractIsTokenBased 固化 R11 M-R11-1 记录的所有权契约：
// unlock 只归还"一个令牌"，不校验调用者身份。
//
// 这是**调用方违约场景的边界固化**，不是对用法的推荐：本包 Runner 路径由
// heldNS/heldBusiness 复位保证配对。若将来引入 lease token / owner 校验，
// 本测试会失败并强制同步更新 ctxMutex 的契约文档。
func TestCtxMutex_ReleaseContractIsTokenBased(t *testing.T) {
	m := newCtxMutex()
	bg := context.Background()

	// 未被持有时 unlock 是无操作（不 panic、不产生令牌）
	if m.unlock() {
		t.Fatal("unlock on an unheld ctxMutex must be a noop")
	}

	// 正常配对：获取 → 归还 → 重复归还是 noop
	if err := m.lock(bg); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if !m.unlock() {
		t.Fatal("unlock must take the token back from a held ctxMutex")
	}
	if m.unlock() {
		t.Fatal("second unlock on a free ctxMutex must be a noop")
	}

	// 违约场景：A 获取 → A 正常释放 → B 获取 → A 再次"释放"
	if err := m.lock(bg); err != nil { // A
		t.Fatalf("lock A: %v", err)
	}
	if !m.unlock() { // A 正常释放
		t.Fatal("A's release must take the token back")
	}
	if err := m.lock(bg); err != nil { // B
		t.Fatalf("lock B: %v", err)
	}
	if !m.unlock() { // A 误释放：取走的是 B 的令牌
		t.Fatal("documented contract: a mis-release takes the token held by another owner")
	}

	// 后果：锁已空闲，C 可立即进入 —— 互斥被违约释放破坏，本锁不提供任何检测能力
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	if err := m.lock(ctx); err != nil {
		t.Fatalf("expected the mutex to be free after the mis-release, got %v", err)
	}
	m.unlock()
}
