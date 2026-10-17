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
// 测试替身：委托型可观察门禁（R11 M-R11-2 / R13 M2 握手用）
// ---------------------------------------------------------------------------

// waitSignalContext 是"已抵达等待点"探针（R13 M2 / M-R13-1 的最小修复）。
//
// 依据被测实现的固定调用顺序 —— ctxMutex.lock 与 sleepContext 都是
//
//	① if err := ctx.Err(); err != nil { return err }    // 入口早退
//	② select { case …: case <-ctx.Done(): }              // 真正的等待点
//
// 因此"Err() 已被观测为 nil 之后 Done() 被求值、且此刻 Err() 仍为 nil"
// 等价于"该调用已经越过入口早退分支、正在进入 select 等待"：入口分支若命中，
// 函数早已返回，Done() 根本不会被求值。用它取代"goroutine 一起跑就发信号"，
// 消除"发完信号就被调度暂停"造成的假绿窗口。
//
// 只在首次满足条件的那一次 Done() 求值上回调一次；回调不阻塞、不改变被测行为。
type waitSignalContext struct {
	context.Context
	onWait func()

	mu       sync.Mutex
	errIsNil bool
	fired    bool
}

func newWaitSignalContext(ctx context.Context, onWait func()) *waitSignalContext {
	return &waitSignalContext{Context: ctx, onWait: onWait}
}

func (c *waitSignalContext) Err() error {
	err := c.Context.Err()
	if err == nil {
		c.mu.Lock()
		c.errIsNil = true
		c.mu.Unlock()
	}
	return err
}

func (c *waitSignalContext) Done() <-chan struct{} {
	ch := c.Context.Done()
	c.mu.Lock()
	fire := !c.fired && c.errIsNil && c.Context.Err() == nil
	if fire {
		c.fired = true
	}
	c.mu.Unlock()
	if fire {
		c.onWait()
	}
	return ch
}

// newWaitSignalChan 返回一个容量 1 的信号通道与配套回调（回调满则丢弃，不阻塞被测方）
func newWaitSignalChan() (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	return ch, func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// waitSignal 在 timeout 内等待"已抵达等待点"探针信号。
// 超时视为测试失败：它只用于证明"被测阶段确实已经发生"，不作为放行手段。
func waitSignal(t *testing.T, ch <-chan struct{}, what string, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatalf("timed out after %v waiting for %s", timeout, what)
	}
}

// trackedGateManager 把门禁调用原样委托给 inner（默认传 DefaultGateManager()，
// 互斥语义完全由被测实现提供，本类型不参与加锁），只额外暴露：
//   - hook：每次"抵达 inner 的等待点 / 等待返回 / 释放"事件，供测试做确定性握手；
//     其中 acquire-wait 由 waitSignalContext 探针在 inner **内部**求值 ctx.Done()
//     时发出（R13 M2：不是"调用前发信号"，见该类型文档）；
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

// tracked 在挂了 hook 时把 ctx 包一层"已抵达等待点"探针；未挂 hook 的用例
// （panic 顺序用例）原样透传，行为完全不变。
func (g *trackedGateManager) tracked(ctx context.Context, label string) context.Context {
	if g.hook == nil {
		return ctx
	}
	return newWaitSignalContext(ctx, func() { g.emit("acquire-wait:" + label) })
}

func (g *trackedGateManager) AcquireLocalBusiness(ctx context.Context) error {
	g.mu.Lock()
	g.acquired = append(g.acquired, "business")
	g.mu.Unlock()
	// R13 M2：这里**不发**"起跑"信号。acquire-wait 由探针在 inner 越过入口
	// ctx.Err() 检查、求值 ctx.Done()（即进入 select 等待）时发出。
	err := g.inner.AcquireLocalBusiness(g.tracked(ctx, "business"))
	g.emit("acquire-return:business")
	return err
}

func (g *trackedGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	g.mu.Lock()
	g.acquired = append(g.acquired, "ns:"+ns)
	g.mu.Unlock()
	err := g.inner.AcquireLocalNamespace(g.tracked(ctx, "ns:"+ns), ns)
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
//
// R13 M2 的握手方式（全程事件驱动，不用"窗口内没有事件"当作 B 已在等待的证据）：
//  1. B 的 acquire-wait 事件由探针在 inner 门禁**内部**发出（已越过入口 ctx.Err()
//     早退分支、正在 select 上等待），不是"调用前发个信号就去抢跑"；
//  2. A 仍持有时取消 B，要求 B 以 context.Canceled 结束、payload 恰 0 次 —— 这是
//     正向事件断言：若门禁退化为不互斥，B 会先返回 nil 并进入 payload，必被捕获；
//  3. 全过程不释放 A；释放 A 之后再独立验证同 namespace 的后继 Run 可以进入。
func TestTxRunner_DefaultGatesSerializeSameNamespace(t *testing.T) {
	ns := "w6a-c1-shared-" + t.Name()

	dbA := getMockDB(t)
	defer dbA.Close()
	dbB := getMockDB(t)
	defer dbB.Close()
	dbC := getMockDB(t)
	defer dbC.Close()

	var (
		mu     sync.Mutex
		events []string
		record = func(ev string) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		}
		observed = func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), events...)
		}
	)

	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }

	// 两个 Run 各自包一层"委托给默认单例"的门禁管理器。互斥仍由 DefaultGateManager()
	// 这同一实例提供（被测语义不变），包装只上报"抵达等待点 / 等待返回 / 释放"事件。
	gateEvents, hook := newGateEventHook()
	gmA := &trackedGateManager{inner: DefaultGateManager(), tag: "A", hook: hook}
	gmB := &trackedGateManager{inner: DefaultGateManager(), tag: "B", hook: hook}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	defer wg.Wait() // 先注册：最后执行，确保所有 Run goroutine 都退出
	defer release()

	runA := NewTxRunner(dbA)
	runB := NewTxRunner(dbB)

	aDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		aDone <- runA.Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}, GateManager: gmA},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("A:enter")
				close(enteredA)
				<-releaseA
				record("A:exit")
				return nil
			})
	}()

	// 等 A 真正进入 payload（即已持有 ns 门禁）后再启动 B，避免调度顺序造成的假阴性
	select {
	case <-enteredA:
	case <-time.After(5 * time.Second):
		t.Fatal("run A never entered payload")
	}
	// A 的门禁获取早于 payload，先把 A 自己的事件排掉，后面只观察 B 的事件
	waitGateEvent(t, gateEvents, "A:acquire-return:ns:"+ns, 5*time.Second)

	// —— B：在 A 仍持有门禁时到达同一个共享 namespace 门禁 ——
	bCtx, cancelB := context.WithCancel(ctx)
	defer cancelB()
	bDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		bDone <- runB.Run(bCtx, TxRunnerOptions{RequiredNamespaces: []string{ns}, GateManager: gmB},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("B:enter")
				return nil
			})
	}()

	// 握手 1（明确事件，不是时间窗口）：B 已经越过入口 ctx.Err() 早退分支，
	// 正在共享 namespace 门禁的 select 上等待。
	waitGateEvent(t, gateEvents, "B:acquire-wait:ns:"+ns, 5*time.Second)

	// 握手 2（明确事件，不是时间窗口）：A 仍持有门禁，B 唯一的返回路径是 ctx 结束。
	// 取消 B，要求它以"未取得门禁"结束。若门禁退化为不互斥（例如每次 Run 新建
	// manager），B 的 Acquire 会先返回 nil 并进入 payload，下面的错误分类与
	// payload 断言必有一项失败。
	cancelB()

	var errB error
	select {
	case errB = <-bDone:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Run never returned after its ctx was cancelled while A still held the gate")
	}
	if !errors.Is(errB, context.Canceled) {
		t.Fatalf("namespace gate did not serialize two concurrent Runs: B finished with %v while A still held the gate", errB)
	}

	// A 仍在 payload 内（release 由本测试在下一步才触发）：此刻既不得有 A:exit，
	// 更不得有 B:enter。
	if got := observed(); len(got) != 1 || got[0] != "A:enter" {
		t.Fatalf("namespace gate did not serialize two concurrent Runs, events=%v", got)
	}

	// —— 释放 A：A 正常结束，其门禁由 Run 逆序释放 ——
	release()
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("run A failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run A never returned after release")
	}

	// —— 后继 Run：A 释放后同一 namespace 必须可再次进入（B 的取消没有把门禁留在
	// 持有态）。这里刻意不注入 GateManager，顺带覆盖"为 nil 时选默认单例"的分支。
	cDone := make(chan error, 1)
	go func() {
		cDone <- NewTxRunner(dbC).Run(ctx, TxRunnerOptions{RequiredNamespaces: []string{ns}},
			func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				record("C:enter")
				return nil
			})
	}()
	select {
	case err := <-cDone:
		if err != nil {
			t.Fatalf("successor Run on the same namespace failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("successor Run never entered after A released the namespace gate")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"A:enter", "A:exit", "C:enter"}
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
	// AcquireLocalBusiness 唯一的返回路径就是 ctx 取消：用"已抵达等待点探针 → cancel"
	// 取代 time.Sleep 触发取消（R11 M-R11-2，R13 M2 把探针从"起跑"改为"已进入等待"）。
	ctxCancel, cancel := context.WithCancel(bg)
	defer cancel()

	businessWaiting, notifyBusinessWaiting := newWaitSignalChan()
	businessErr := make(chan error, 1)
	go func() {
		businessErr <- gm.AcquireLocalBusiness(newWaitSignalContext(ctxCancel, notifyBusinessWaiting))
	}()
	// 探针：AcquireLocalBusiness 已越过入口 ctx.Err() 检查、正在 select 上等待
	waitSignal(t, businessWaiting, "AcquireLocalBusiness reached its wait point", 5*time.Second)

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

// TestSleepContext_CancelsWhileSleeping 直接证明默认退避休眠"真的在 select 上等待"，
// 并在 ctx 取消时立即返回（R11 M-R11-2 第 2 点；R13 M-R13-1 补起跑握手）。
//
// 握手语义：探针只在"Err() 已被观测为 nil 之后 Done() 被求值、且此刻 Err() 仍为 nil"
// 时发信号。sleepContext 的顺序是 ①入口 ctx.Err() 早退 ②d<=0 早退 ③select，
// 因此收到信号即证明它已经走过两条早退分支、只剩 select 一条路径可走 ——
// "取消后才被调度、走入口早退"的执行根本不会求值 Done()，也就永远发不出该信号，
// 用例会在握手处超时失败，而不是全绿。
func TestSleepContext_CancelsWhileSleeping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const sleepFor = 60 * time.Second

	waiting, notifyWaiting := newWaitSignalChan()
	done := make(chan error, 1)
	go func() {
		done <- sleepContext(newWaitSignalContext(ctx, notifyWaiting), sleepFor)
	}()

	// 握手：确认已进入 select 等待点（超时即失败，不以"窗口内没返回"作为等待证据）
	waitSignal(t, waiting, "sleepContext reached its select wait point", 5*time.Second)

	// 早退分支已被上面的握手排除：此刻不得返回。
	// （done 的写入严格晚于探针回调，因此这是一次确定性的即时检查，不是观测窗口）
	select {
	case err := <-done:
		t.Fatalf("sleepContext(%v) returned before cancellation: %v", sleepFor, err)
	default:
	}

	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from sleepContext, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sleepContext did not return after cancellation")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("sleepContext not cancellable at its wait point, took %v", elapsed)
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
//
// R13 M-R13-2：断言改为对**每个「入口 × 输入」组合**都保留各自的 LockContext，
// 并在错误分类之后断言 `len(lc.ReleaseEvents()) == 0` —— 失败路径不得登记任何锁事件；
// verifier 总次数断言移到全部调用之后。
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

	// 两个导出执行入口：底层 / 组合
	entries := []struct {
		name string
		call func(context.Context, *sql.Tx, *LockContext, []EntityKey) error
	}{
		{"LockEntitiesInOrder", func(ctx context.Context, tx *sql.Tx, lc *LockContext, keys []EntityKey) error {
			return LockEntitiesInOrder(ctx, tx, lc, keys)
		}},
		{"LockAndVerify", func(ctx context.Context, tx *sql.Tx, lc *LockContext, keys []EntityKey) error {
			_, err := LockAndVerify(ctx, tx, lc, &LocatorSnapshot{CandidateKeys: keys}, verifier)
			return err
		}},
	}

	// ① 每个「入口 × 输入」组合：错误分类互斥且正确 + 被拒调用不得登记任何锁事件
	for _, e := range entries {
		for _, tc := range []struct {
			name string
			keys []EntityKey
			want error
		}{
			{"unsorted", unsorted, ErrOutOfOrderLock},
			{"duplicate", duplicate, ErrOutOfOrderLock},
			{"ordered", ordered, ErrNilTx},
			{"empty", nil, ErrNilTx},
		} {
			other := ErrNilTx
			if tc.want == ErrNilTx {
				other = ErrOutOfOrderLock
			}

			lc := NewLockContext()
			err := e.call(ctx, nil, lc, tc.keys)
			if !errors.Is(err, tc.want) {
				t.Errorf("%s(%s): expected %v, got %v", e.name, tc.name, tc.want, err)
			}
			if errors.Is(err, other) {
				t.Errorf("%s(%s): must not be classified as %v, got %v", e.name, tc.name, other, err)
			}
			if n := len(lc.ReleaseEvents()); n != 0 {
				t.Errorf("%s(%s): rejected call must not record locks, history=%d", e.name, tc.name, n)
			}
		}
	}

	// ② 可诊断性：组合入口的 nil tx 错误必须透出真正失败的步骤（同样不得登记）
	{
		lc := NewLockContext()
		_, err := LockAndVerify(ctx, nil, lc, &LocatorSnapshot{CandidateKeys: ordered}, verifier)
		if err == nil || !strings.Contains(err.Error(), "LockEntitiesInOrder") {
			t.Errorf("combined entry must keep the failing step in the nil tx message, got %v", err)
		}
		if n := len(lc.ReleaseEvents()); n != 0 {
			t.Errorf("combined entry (ordered): rejected call must not record locks, history=%d", n)
		}
	}

	// ③ nil snapshot 仍是组合入口独有的最前置检查，且不被上面两类错误掩盖；
	//    它同样声称"拒绝即不登记"，因此也要独立 lc 检查
	{
		lc := NewLockContext()
		if _, err := LockAndVerify(ctx, nil, lc, nil, verifier); err == nil ||
			errors.Is(err, ErrNilTx) || errors.Is(err, ErrOutOfOrderLock) ||
			!strings.Contains(err.Error(), "snapshot") {
			t.Errorf("nil snapshot must be rejected first with its own error, got %v", err)
		}
		if n := len(lc.ReleaseEvents()); n != 0 {
			t.Errorf("nil snapshot: rejected call must not record locks, history=%d", n)
		}
	}

	// ④ 加锁失败路径不得触达 verifier（不会"未加锁先核验"）——放在全部调用之后统计
	if verifierCalls != 0 {
		t.Errorf("verifier must not be called when locking fails, calls=%d", verifierCalls)
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
