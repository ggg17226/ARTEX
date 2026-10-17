package lockorder

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// 连接池准入控制（R2 I2：防 hold-and-wait 连接池死锁）
// ---------------------------------------------------------------------------
//
// 风险模型（R2 I2）：
//   - 共享 TiDB 池上限固定为 32（db/tidb/open.go 的 maxOpenConns，本包不得反向依赖该包）；
//   - TxRunner.Run 的外层事务先占用 1 条物理连接（Run:db.BeginTx），
//     随后 payload 内的 ExecuteFreshLocator 会向**同一个池**再要 1 条独立只读连接；
//   - 若 32 个并发 Run 都停在"已持外层连接、正等第二条"这一点上，
//     没有任何一方能前进：经典 hold-and-wait。context.Background() 下表现为永久卡死。
//
// 修复策略（分层准入，容量严格小于池容量）：
//
//	runner 准入（默认 16 = 池/2）  → 最多 16 条外层事务连接
//	locator 准入（默认 8 = 池/4）   → 最多 8 条只读定位连接
//	最坏占用 16 + 8 = 24 < 32，剩余容量留给同池的其他使用者。
//
// 两层必须是**不同的**信号量：同一个信号量上嵌套获取（Run 持 1 个许可再等第 2 个）
// 会原样复现 hold-and-wait。等待许可一律可被 ctx 取消，并带默认 5s 的等待上限，
// 因此 context.Background() 也绝不会永久死等，只会得到 ErrAdmissionTimeout。
//
// 该机制只保证 lockorder 自身不会互相饿死；它不是全局调度器，也不保证公平性。

const (
	// DefaultPoolCapacity 与 db/tidb/open.go 的 maxOpenConns 对齐（本包不得反向依赖 db/tidb）。
	DefaultPoolCapacity = 32

	// DefaultRunnerAdmissionCapacity 允许同时进入 Run（各持 1 条外层事务连接）的上限 = 池/2。
	DefaultRunnerAdmissionCapacity = DefaultPoolCapacity / 2 // 16

	// DefaultLocatorAdmissionCapacity 允许同时执行 fresh locator（各持 1 条只读连接）的上限 = 池/4。
	// 取小于 runner 容量：定位连接是短生命周期的纯读快照，排队比占池更划算。
	DefaultLocatorAdmissionCapacity = DefaultPoolCapacity / 4 // 8

	// DefaultAdmissionWaitTimeout 等待准入许可的默认上限，避免 Background ctx 永久死等。
	DefaultAdmissionWaitTimeout = 5 * time.Second
)

// ErrAdmissionTimeout 等待准入许可超过 AdmissionControl 的等待上限。
// 它是**背压信号**而非业务错误：调用方可退避后重试（IsRetryableConflict 已将其归入可重试）。
var ErrAdmissionTimeout = errors.New("lockorder: connection pool admission permit wait timed out")

// AdmissionStats 准入控制的瞬时观测值。
type AdmissionStats struct {
	Capacity int   // 许可总数（<=0 表示不限流）
	InUse    int64 // 当前已发放的许可数
	Waiting  int64 // 正在排队等待许可的调用数（近似值）
	Timeouts int64 // 累计等待超时次数
	Canceled int64 // 累计因 ctx 结束而放弃的次数
}

// AdmissionControl 有界准入信号量。零值不可用，必须经 NewAdmissionControl 创建。
// 并发安全，可跨 TxRunner / *sql.DB 共享。
type AdmissionControl struct {
	capacity    int
	waitTimeout time.Duration

	tokens chan struct{}

	inUse    int64
	waiting  int64
	timeouts int64
	canceled int64
}

// NewAdmissionControl 创建准入信号量。
//   - capacity <= 0：不限流（Acquire 直接放行），仅用于显式关闭该机制；
//   - waitTimeout <= 0：仅由 ctx 控制等待时长（不推荐配合 context.Background 使用）。
//
// capacity 必须严格小于连接池容量，且要为本包之外的同池使用者留出余量：
// lockorder 的嵌套模式（外层事务 + fresh locator）单个 Run 最多占用 2 条连接。
func NewAdmissionControl(capacity int, waitTimeout time.Duration) *AdmissionControl {
	if capacity < 0 {
		capacity = 0
	}
	a := &AdmissionControl{
		capacity:    capacity,
		waitTimeout: waitTimeout,
	}
	if capacity > 0 {
		a.tokens = make(chan struct{}, capacity)
	}
	return a
}

// Capacity 返回许可总数；<=0 表示不限流。
func (a *AdmissionControl) Capacity() int {
	if a == nil {
		return 0
	}
	return a.capacity
}

// Available 返回当前可用许可数；不限流时返回 -1。
func (a *AdmissionControl) Available() int {
	if a == nil || a.capacity <= 0 {
		return -1
	}
	return a.capacity - len(a.tokens)
}

// Stats 返回瞬时观测值快照；计数器为近似值，仅用于观测与测试断言。
func (a *AdmissionControl) Stats() AdmissionStats {
	if a == nil {
		return AdmissionStats{}
	}
	return AdmissionStats{
		Capacity: a.capacity,
		InUse:    atomic.LoadInt64(&a.inUse),
		Waiting:  atomic.LoadInt64(&a.waiting),
		Timeouts: atomic.LoadInt64(&a.timeouts),
		Canceled: atomic.LoadInt64(&a.canceled),
	}
}

// Acquire 获取一个准入许可，返回**幂等**的释放函数。
//
// 等待顺序：先尝试立即取令牌；失败后在 ctx.Done()、等待超时与令牌三者之间 select，
// 因此任何情况下都不会无限阻塞：
//   - ctx 先结束 → 返回 ctx.Err()（调用方语义与库内其他等待点一致）；
//   - 等待超过 waitTimeout → 返回包装 ErrAdmissionTimeout 的错误；
//   - 拿到令牌 → 返回 release（重复调用安全，未持有时为无操作）。
//
// a 为 nil 或 capacity<=0 时视为不限流：立即返回空释放函数。
func (a *AdmissionControl) Acquire(ctx context.Context) (func(), error) {
	if a == nil || a.capacity <= 0 {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		atomic.AddInt64(&a.canceled, 1)
		return nil, err
	}

	// 快路径：有空闲令牌直接拿走，不排队、不计数。
	select {
	case a.tokens <- struct{}{}:
		atomic.AddInt64(&a.inUse, 1)
		return a.releaseFunc(), nil
	default:
	}

	var timeoutCh <-chan time.Time
	var timer *time.Timer
	if a.waitTimeout > 0 {
		timer = time.NewTimer(a.waitTimeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	atomic.AddInt64(&a.waiting, 1)
	defer atomic.AddInt64(&a.waiting, -1)

	select {
	case a.tokens <- struct{}{}:
		atomic.AddInt64(&a.inUse, 1)
		return a.releaseFunc(), nil
	case <-ctx.Done():
		atomic.AddInt64(&a.canceled, 1)
		return nil, ctx.Err()
	case <-timeoutCh:
		atomic.AddInt64(&a.timeouts, 1)
		return nil, fmt.Errorf("%w: capacity %d, waited %s", ErrAdmissionTimeout, a.capacity, a.waitTimeout)
	}
}

// releaseFunc 构造幂等释放闭包。
// 与 ctxMutex 同源的限制：令牌不携带持有者身份，release 只归还"一个令牌"，
// 必须由成功 Acquire 的同一执行流配对调用；未持有时调用是无操作（静默返回）。
func (a *AdmissionControl) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			select {
			case <-a.tokens:
				atomic.AddInt64(&a.inUse, -1)
			default:
				// 令牌已被他人归还（调用方违约）：不 panic，静默失败
			}
		})
	}
}

var (
	defaultRunnerAdmissionOnce  sync.Once
	defaultRunnerAdmissionVal   *AdmissionControl
	defaultLocatorAdmissionOnce sync.Once
	defaultLocatorAdmissionVal  *AdmissionControl
)

// DefaultRunnerAdmission 返回进程级共享的 Run 准入信号量（默认 16 个许可）。
//
// TxRunner.Run 在 TxRunnerOptions.Admission 为 nil 时使用它：准入必须跨 TxRunner
// 实例共享才有意义（与 DefaultGateManager 同一理由）。
// 需要按实际池容量调优时注入 NewAdmissionControl(...) 自定义实例。
func DefaultRunnerAdmission() *AdmissionControl {
	defaultRunnerAdmissionOnce.Do(func() {
		defaultRunnerAdmissionVal = NewAdmissionControl(DefaultRunnerAdmissionCapacity, DefaultAdmissionWaitTimeout)
	})
	return defaultRunnerAdmissionVal
}

// DefaultLocatorAdmission 返回进程级共享的 fresh locator 准入信号量（默认 8 个许可）。
//
// 与 DefaultRunnerAdmission 是**两个独立**信号量：locator 在已持外层连接的执行流内
// 再取第二条连接，若与 runner 共用同一信号量就会形成嵌套获取，重新引入 hold-and-wait。
func DefaultLocatorAdmission() *AdmissionControl {
	defaultLocatorAdmissionOnce.Do(func() {
		defaultLocatorAdmissionVal = NewAdmissionControl(DefaultLocatorAdmissionCapacity, DefaultAdmissionWaitTimeout)
	})
	return defaultLocatorAdmissionVal
}
