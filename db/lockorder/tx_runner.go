package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	// DefaultBackoffSchedule 规格 §4.3、§10.4-T 定义的 100/250/500ms/1s 指数退避阶梯
	DefaultBackoffSchedule = []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1000 * time.Millisecond,
	}

	ErrRetryExhausted = errors.New("lockorder: transaction retry attempts exhausted")
	ErrOutcomeUnknown = errors.New("lockorder: transaction commit outcome unknown, local gates held")

	// ErrNilTx 导出执行入口收到 nil *sql.Tx 时返回：禁止把"没有真实加锁"当离线成功（R2 I9）
	ErrNilTx = errors.New("lockorder: nil transaction handle")
)

// ---------------------------------------------------------------------------
// 错误分类（R2 I6：结构化编号判定；R2 I7：提交结局不确定性判定）
// ---------------------------------------------------------------------------

// retryableMySQLNumbers 服务端明确可重试的错误编号：
// 1062 ER_DUP_ENTRY、1205 ER_LOCK_WAIT_TIMEOUT、1213 ER_LOCK_DEADLOCK。
var retryableMySQLNumbers = map[uint16]struct{}{
	1062: {},
	1205: {},
	1213: {},
}

// unknownOutcomeMySQLNumbers 驱动/客户端侧"连接已不可信"的错误编号：
// 2006 CR_SERVER_GONE_ERROR、2013 CR_SERVER_LOST。
var unknownOutcomeMySQLNumbers = map[uint16]struct{}{
	2006: {},
	2013: {},
}

// sqlErrorNumber 以**结构化**方式提取 MySQL/TiDB 错误编号。
//
// 优先级：
//  1. *mysql.MySQLError —— 原生 go-sql-driver 的真实形态（Number 是**字段**，不是方法），
//     服务端返回的每一个错误、以及驱动侧 2006/2013 都是该类型；
//  2. 实现 Number()/Code() 方法的包装类型或测试替身（规范提取接口）。
//
// ok=false 表示错误链上没有任何结构化编号。调用方**不得**因此改用文本猜测：
// 业务错误消息里出现 "1213"、"deadlock" 之类的字样不构成冲突证据（R2 I6）。
func sqlErrorNumber(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number, true
	}
	type number16 interface{ Number() uint16 }
	type number32 interface{ Number() int32 }
	type numberInt interface{ Number() int }
	type code16 interface{ Code() uint16 }
	type codeInt interface{ Code() int }

	var n16 number16
	if errors.As(err, &n16) {
		return n16.Number(), true
	}
	var n32 number32
	if errors.As(err, &n32) {
		return uint16(n32.Number()), true
	}
	var nInt numberInt
	if errors.As(err, &nInt) {
		return uint16(nInt.Number()), true
	}
	var c16 code16
	if errors.As(err, &c16) {
		return c16.Code(), true
	}
	var cInt codeInt
	if errors.As(err, &cInt) {
		return uint16(cInt.Code()), true
	}
	return 0, false
}

// IsRetryableConflict 判断错误是否为死锁、锁超时、唯一键冲突、成员漂移等可重试冲突。
//
// 判定顺序（R2 I6：完全结构化，绝不做错误文本匹配）：
//  0. **ErrOutcomeUnknown → 一律不可重试（R18 I-2 短路，优先级最高）**；
//  1. 本包自身的结构化哨兵（漂移类 + 准入背压）→ 可重试；
//  2. context.Canceled / DeadlineExceeded → 不可重试（取消不是冲突）；
//  3. 能提取到结构化错误编号 → **只按编号**决定，命中 1062/1205/1213 才可重试；
//  4. 无结构化编号 → 不可重试。
//
// 第 0 步是 I7 fail-closed 语义的**唯一权威入口**（R18 I-2）：unknown-outcome 的错误用
// `fmt.Errorf("%w: %w", ErrOutcomeUnknown, commitErr)` 双包装，内层 commitErr 完全可能
// 恰好是 1213/1205/1062（TiDB 在提交阶段返回死锁/锁超时并非不可能）。若不在这里短路，
// 第 3 步会取到内层编号并返回 true，等于把"结局未知、门禁已封锁"的事务交给上层重放，
// 直接违反 I7 的"绝不自动重放"，可造成重复提交。结局未知时**任何**重放都不安全。
//
// 第 3 步的"只按编号"是关键：一旦拿到编号就不再回退到文本，
// 避免 `errors.New("task 1213 violates policy")` 这类业务错误被误判为死锁，
// 也避免真实 1064 语法错误因消息里含 "1213" 而被无限重试。
func IsRetryableConflict(err error) bool {
	if err == nil {
		return false
	}

	// 0. R18 I-2：提交结局未知 → 永不重试（必须早于编号判定，否则内层 1213/1205/1062
	//    会经 %w 链被取到编号而误判为可重试）。
	if errors.Is(err, ErrOutcomeUnknown) {
		return false
	}

	// 漂移类冲突：一律整体回滚 + 退避后重做非加锁 Locator（P04b Global Constraint 5）。
	// ErrEntityNotFound（R2 I5）：持锁/复核阶段实体物理缺失等同"实体缺失"漂移，
	// 不得当作终态业务错误结束事务，必须退避重试并重新定位快照。
	if errors.Is(err, ErrMembershipDrift) ||
		errors.Is(err, ErrLowerPKDiscovered) ||
		errors.Is(err, ErrNewMemberDiscovered) ||
		errors.Is(err, ErrEntityNotFound) {
		return true
	}

	// 连接池准入背压（R2 I2）：容量暂时不足，退避后重试是正确响应。
	if errors.Is(err, ErrAdmissionTimeout) {
		return true
	}

	// 排除主动取消或超时的上下文错误
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if num, ok := sqlErrorNumber(err); ok {
		_, retryable := retryableMySQLNumbers[num]
		return retryable
	}

	// 无结构化编号：不猜文本，一律不可重试（R2 I6）
	return false
}

// isOutcomeUnknown 判定错误是否意味着"操作结局无法确定"（既不能认定已提交，
// 也不能认定已回滚），因此必须保持门禁封锁（fail closed）。
//
// 覆盖范围（R2 I7）：
//   - 本包 ErrOutcomeUnknown 标记；
//   - 驱动协议失步：mysql.ErrPktSync / mysql.ErrPktSyncMul
//     （go-sql-driver v1.9.3 packets.go readPacket 在响应 sequence 异常时返回；
//     COMMIT 已写出但响应不可信时，服务端可能已经提交）；
//   - 连接失效：driver.ErrBadConn、mysql.ErrInvalidConn、mysql.ErrMalformPkt；
//   - 传输层截断：io.ErrUnexpectedEOF、io.EOF（响应包读到一半断流）；
//   - 驱动侧 2006/2013（server gone / lost connection）；
//   - *net.OpError 及连接类文本特征。
//
// 注意文本兜底只出现在**保守方向**：命中即判 unknown（宁可多封锁、绝不自动重放），
// 它不参与"已安全回滚"的结论（R2 I7 最小建议）。
func isOutcomeUnknown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, mysql.ErrMalformPkt) ||
		errors.Is(err, mysql.ErrPktSync) ||
		errors.Is(err, mysql.ErrPktSyncMul) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) {
		return true
	}
	if num, ok := sqlErrorNumber(err); ok {
		if _, unknown := unknownOutcomeMySQLNumbers[num]; unknown {
			return true
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"broken pipe",
		"connection reset",
		"connection lost",
		"connection refused",
		"server has gone away",
		"unexpected eof",
		"bad connection",
		"invalid connection",
		"commands out of sync",
		"out of sync",
		"malformed packet",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// commitProvablyNotAttempted 判定 tx.Commit() 返回的错误是否**可证明**提交请求
// 从未送达服务端。只有下列两种情况允许按"未提交"处理（R2 I7：其余一律 unknown）：
//
//  1. sql.ErrTxDone：stdlib 在 tx.done 的 CAS 上失败（database/sql sql.go Tx.Commit），
//     本次调用根本没有执行 driver 的 Commit；
//  2. context.Canceled / context.DeadlineExceeded：stdlib Tx.Commit 在 CAS 与
//     driver.Commit **之前**先 select tx.ctx.Done()，而 go-sql-driver v1.9.3 的
//     Commit 自身不监听 ctx、也不注册 watcher（transaction.go），
//     因此 ctx 类错误只可能来自发送前的那次检查。
//
// 其他任何提交错误（协议失步、连接中断、服务端错误、未知驱动错误）都无法证明未提交，
// 必须由调用方按 unknown-outcome 处理：保持门禁封锁，绝不自动重放。
func commitProvablyNotAttempted(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrTxDone) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ---------------------------------------------------------------------------
// 本地门禁（Local Gates）
// ---------------------------------------------------------------------------

// GateManager 本地门禁管理器接口
type GateManager interface {
	AcquireLocalBusiness(ctx context.Context) error
	ReleaseLocalBusiness()
	AcquireLocalNamespace(ctx context.Context, ns string) error
	ReleaseLocalNamespace(ns string)
}

// ctxMutex 可被 context 取消的互斥锁（R2 I3）。
// 容量为 1 的 channel 即令牌：持有者 = 唯一成功投递令牌的一方；等待者可在 ctx 结束时脱身。
//
// 所有权契约（R11 M-R11-1，务必先读）：本锁**不携带、也不校验持有者身份**。
//   - lock 成功 = 取得令牌；unlock 只归还"一个令牌"，无法验证归还者是否为持有者；
//   - 因此 acquire/release 必须由同一执行流严格配对：谁 lock 成功，谁负责 unlock；
//   - 违约示例：A release → B acquire → A 再次 release。第三次调用会取走 **B 的**
//     令牌（并非无操作），使 C 能在 B 仍持有时进入 —— 属调用方违约，本锁不提供兜底；
//   - 仅在锁未被持有（channel 为空）时 unlock 才是无操作：返回 false、不 panic，
//     因此重复释放只会静默失败，不会像 sync.Mutex 那样 panic。
//
// 该缺口与旧的 sync.Mutex 实现同源（sync.Mutex 同样允许跨 goroutine Unlock，
// B 重获后被 A 错误 Unlock 同样会丢失互斥），不是本次改动新引入的漏洞；
// 但默认门禁已改为进程级单例（DefaultGateManager），误释放的影响域从"单次 Run"
// 扩大到"整个进程的门禁域"，故不得把 ReleaseLocalBusiness/ReleaseLocalNamespace
// 当作"安全恢复/强制解封协议"使用。需要可审计的解封能力时，应另行引入带 lease token
// 的 quarantine/reconcile API（本波不做，见 w6a-b-report.md 剩余风险）。
type ctxMutex struct {
	token chan struct{}
}

func newCtxMutex() *ctxMutex {
	return &ctxMutex{token: make(chan struct{}, 1)}
}

// lock 获取互斥锁；ctx 先已取消或等待期间结束均立即返回 ctx.Err()，绝不无限等待。
// ctx.Done() 与"投递成功"同时就绪时 select 可能选择投递成功：此时令牌已被本方持有，
// 由调用方（runAttempt 的 cleanup）正常释放，不会丢失互斥。
func (m *ctxMutex) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.token <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unlock 归还一个令牌：
//   - 锁未被持有（channel 为空）→ 无操作，返回 false，不 panic；
//   - 锁被持有 → 取走令牌并返回 true，**不校验调用者是否为持有者**。
//
// 调用方必须保证与成功的 lock 配对（见 ctxMutex 的所有权契约）；错误调用会释放他人的
// 令牌并破坏互斥，本方法不提供任何检测能力。
func (m *ctxMutex) unlock() bool {
	select {
	case <-m.token:
		return true
	default:
		return false
	}
}

// MemoryGateManager 基于内存互斥锁的本地门禁实现。
// 锁等待可被 ctx 取消（R2 I3）；同一实例必须跨事务共享才具备跨 Run 互斥语义（R2 C1）。
type MemoryGateManager struct {
	business *ctxMutex
	nsMu     sync.Mutex
	nsLocks  map[string]*ctxMutex
}

// NewMemoryGateManager 创建基于内存的门禁管理器（调用方自行决定共享范围；
// 需要跨 Run/跨事务互斥时使用 DefaultGateManager 进程级单例）
func NewMemoryGateManager() *MemoryGateManager {
	return &MemoryGateManager{
		business: newCtxMutex(),
		nsLocks:  make(map[string]*ctxMutex),
	}
}

// AcquireLocalBusiness 获取本地业务关系门禁，等待期间可被 ctx 取消
func (m *MemoryGateManager) AcquireLocalBusiness(ctx context.Context) error {
	return m.business.lock(ctx)
}

// ReleaseLocalBusiness 释放本地业务关系门禁。
//
// 契约同 ctxMutex.unlock：只归还一个令牌，不校验调用者身份。必须由成功
// AcquireLocalBusiness 的同一执行流调用；未持有时调用是无操作（静默返回），
// 他人持有时调用会**误释放他人的门禁**（调用方违约，见 ctxMutex 文档）。
func (m *MemoryGateManager) ReleaseLocalBusiness() {
	m.business.unlock()
}

// getNSMutex 返回（必要时创建）指定 namespace 的互斥锁
func (m *MemoryGateManager) getNSMutex(ns string) *ctxMutex {
	m.nsMu.Lock()
	defer m.nsMu.Unlock()
	mu, ok := m.nsLocks[ns]
	if !ok {
		mu = newCtxMutex()
		m.nsLocks[ns] = mu
	}
	return mu
}

// lookupNSMutex 仅查询已存在的 namespace 锁；释放路径不得凭空创建锁对象
func (m *MemoryGateManager) lookupNSMutex(ns string) (*ctxMutex, bool) {
	m.nsMu.Lock()
	defer m.nsMu.Unlock()
	mu, ok := m.nsLocks[ns]
	return mu, ok
}

// AcquireLocalNamespace 获取指定 namespace 本地门禁，等待期间可被 ctx 取消
func (m *MemoryGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	return m.getNSMutex(ns).lock(ctx)
}

// ReleaseLocalNamespace 释放指定 namespace 本地门禁。
//
// 未持有（含从未出现过的 ns）时为无操作；契约同 ctxMutex.unlock —— 只归还一个令牌、
// 不校验调用者身份，必须由成功 AcquireLocalNamespace 的同一执行流配对调用。
// 在默认单例 DefaultGateManager() 上对**他人正在持有**的 namespace 调用本方法，
// 会破坏该 namespace 的互斥并让 unknown-outcome 的 fail closed 封锁失效：
// 这是调用方违约，不是受支持的恢复手段（R11 M-R11-1）。
func (m *MemoryGateManager) ReleaseLocalNamespace(ns string) {
	if mu, ok := m.lookupNSMutex(ns); ok {
		mu.unlock()
	}
}

var (
	defaultGateManagerOnce sync.Once
	defaultGateManagerVal  *MemoryGateManager
)

// DefaultGateManager 返回进程级共享的本地门禁管理器（R2 C1）。
//
// TxRunner.Run 在 TxRunnerOptions.GateManager 为 nil 时一律使用该单例，从而保证：
//   - 同一 namespace 的并发 Run 在本地门禁层互斥（跨 TxRunner 实例、跨 *sql.DB 实例）；
//   - unknown-outcome 分支刻意 hold 住的门禁在后续 Run 中继续阻塞同一 namespace（fail closed）。
//
// 生命周期：该实例的 namespace 锁表只增不减。注意这与"旧实现一致"的说法并不成立——
// 旧默认路径每次 Run 新建临时管理器（Run 结束即可整体回收），改为单例后，
// 每个出现过的 namespace 的锁对象真正常驻整个进程生命周期，内存增长随 namespace
// 基数线性累积（基数上限/准入/指标属后续工作，见 w6a-b-report.md 剩余风险）。
//
// 误用风险（R11 M-R11-1）：ReleaseLocalBusiness/ReleaseLocalNamespace 不校验持有者，
// 在本单例上手工"解锁"可能释放他人正在持有的门禁、并解除 unknown-outcome 的封锁，
// 因此它不是恢复协议；需要可审计解封时应引入带 lease token 的对账 API。
func DefaultGateManager() *MemoryGateManager {
	defaultGateManagerOnce.Do(func() {
		defaultGateManagerVal = NewMemoryGateManager()
	})
	return defaultGateManagerVal
}

// ---------------------------------------------------------------------------
// 事务执行器
// ---------------------------------------------------------------------------

// TxRunnerOptions 事务运行器配置
type TxRunnerOptions struct {
	RequiresBusinessGate bool
	RequiredNamespaces   []string
	BackoffSchedule      []time.Duration

	// GateManager 本地门禁管理器。为 nil 时使用进程级共享单例 DefaultGateManager()，
	// 以获得跨 Run、跨 TxRunner 的本地互斥与 unknown-outcome fail closed（R2 C1）。
	// 仅测试或确需隔离门禁域时才注入自定义实现。
	GateManager GateManager

	// Admission 连接池准入信号量（R2 I2）。为 nil 时使用进程级共享单例
	// DefaultRunnerAdmission()（默认 16 个许可 = 池容量/2）。
	//
	// 作用：限制同时处于"已持外层事务连接、可能再取 fresh locator 第二条连接"模式的
	// Run 数量，避免 32 个并发 Run 在 maxOpenConns=32 的共享池上形成 hold-and-wait。
	// 获取点在**本地门禁之后、BeginTx 之前**（R18 I-1）：卡在门禁上的请求不占许可，
	// 单个 namespace 的 unknown-outcome 永久封锁不会经许可耗尽扩散成全局拒绝服务。
	// 注入 NewAdmissionControl(0, 0) 可显式关闭限流（仅测试用途）。
	Admission *AdmissionControl
}

// TxPayloadFunc 在全序与门禁保护下的核心事务执行逻辑
type TxPayloadFunc func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error

// TxRunner 全事务回滚与有界退避重试执行器
type TxRunner struct {
	db *sql.DB
	// sleepFn 可取消的退避休眠；默认 sleepContext，测试可经 SetSleeper 注入观测实现
	sleepFn func(ctx context.Context, d time.Duration) error
}

// sleepContext 默认退避休眠：timer + select，ctx 结束立即返回其错误（R2 I3）
func sleepContext(ctx context.Context, d time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewTxRunner 创建事务运行器。db 为 nil 时 Run 直接返回 ErrNilDatabase（R2 I9），
// 不再提供"离线成功"路径。
func NewTxRunner(db *sql.DB) *TxRunner {
	return &TxRunner{
		db:      db,
		sleepFn: sleepContext,
	}
}

// SetSleeper 设置用于单测模拟的休眠函数。
// 注入实现仅用于观测退避时长；取消语义由返回后的 ctx 检查保证，
// 生产路径始终走可取消的 sleepContext。
// 注意：注入的 sleeper 自身不可被 ctx 中断，且本方法与 Run 之间无并发保护，
// 只应在开始并发使用之前完成配置。
func (r *TxRunner) SetSleeper(sleeper func(time.Duration)) {
	if sleeper == nil {
		return
	}
	r.sleepFn = func(ctx context.Context, d time.Duration) error {
		sleeper(d)
		if ctx == nil {
			return nil
		}
		return ctx.Err()
	}
}

// normalizeNamespaces 复制、按名称升序排序并去重（R2 I4）。
// 去重必须在加锁之前完成：同名 namespace 第二次 Acquire 会阻塞在本 goroutine
// 已持有的非重入互斥锁上，永远到不了 LockContext 的 ErrDuplicateLock 检查。
func normalizeNamespaces(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	sorted := make([]string, len(in))
	copy(sorted, in)
	sort.Strings(sorted)

	dedup := make([]string, 0, len(sorted))
	for i, ns := range sorted {
		if i > 0 && ns == sorted[i-1] {
			continue
		}
		dedup = append(dedup, ns)
	}
	return dedup
}

// attemptOutcome 单次尝试的结果分类
type attemptOutcome int

const (
	// outcomeDone 已结束：err==nil 表示已提交成功；否则为不可重试失败
	outcomeDone attemptOutcome = iota
	// outcomeRetry 已整体回滚并释放门禁，可退避重试
	outcomeRetry
	// outcomeHeld 提交结果未知：本地门禁按设计保持 hold，不得再进入同一 namespace
	outcomeHeld
)

// Run 执行带全序门禁与有界退避重试的悲观事务。
//
// 顺序约束保持不变：本地门禁（business → namespace 升序）先于 DB 行锁
// （business_relations → namespace 升序），释放严格逆序。
//
// ctx 契约：准入等待、门禁等待与退避休眠都可被 ctx 取消；payload 自身也必须尊重 ctx。
//
// 资源顺序（R18 I-1 调整后）：本地门禁（business → namespace 升序）→ 连接池准入许可
// （R2 I2）→ 外层事务连接 → DB Gate → 实体锁；释放严格逆序
// （unknown-outcome 例外：本地门禁按设计继续持有）。
//
// 为什么准入许可必须在本地门禁**之后**取得（R18 I-1）：门禁等待只受 ctx 约束，
// context.Background() 下是**无界**等待，而 unknown-outcome 会按设计永久持有门禁。
// 若先取许可再等门禁，16 个卡在门禁上的 Background 请求就会把全局准入占满，
// 让完全无关的 namespace 也开始收到 ErrAdmissionTimeout —— 单个 namespace 的
// fail closed 会经许可耗尽扩散成全局拒绝服务。调整后许可只覆盖"可能持连接"的窗口：
// 卡在门禁上的请求一个许可都不占（见 TestTxRunner_BlockedOnGateConsumesNoAdmissionPermit）。
// 顺序反转不重新引入 hold-and-wait：许可等待有上限（默认 5s）且必然归还，
// 门禁等待方不持许可，因此不存在"持许可等门禁"的环路。
func (r *TxRunner) Run(ctx context.Context, opts TxRunnerOptions, payload TxPayloadFunc) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("%w: TxRunner.Run requires a non-nil *sql.DB (offline success path removed)", ErrNilDatabase)
	}
	if payload == nil {
		return errors.New("lockorder: nil payload function")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	gm := opts.GateManager
	if gm == nil {
		gm = DefaultGateManager()
	}

	// R2 I2：连接池准入。nil → 进程级共享单例，跨 TxRunner 才有全局限流意义。
	admission := opts.Admission
	if admission == nil {
		admission = DefaultRunnerAdmission()
	}

	sleepFn := r.sleepFn
	if sleepFn == nil {
		sleepFn = sleepContext
	}

	schedule := opts.BackoffSchedule
	if len(schedule) == 0 {
		schedule = DefaultBackoffSchedule
	}
	maxAttempts := len(schedule) + 1

	// 预先对 namespace 按名称升序排序并去重，严格保证全序且不会自锁
	sortedNamespaces := normalizeNamespaces(opts.RequiredNamespaces)

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleepFn(ctx, schedule[attempt-1]); err != nil {
				return err
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// R2 I2 + R18 I-1：准入许可的获取点已后移到 runAttempt 内
		// **本地门禁之后、BeginTx 之前**（见 Run 的文档与 runAttempt 的注释）。
		// 许可的归还由 runAttempt 最外层的 defer 负责：正常返回、错误返回、
		// payload panic 的栈展开、以及 payload 内 runtime.Goexit 都 100% 归还（R18 C-1）。
		err, outcome := r.runAttempt(ctx, gm, admission, opts, sortedNamespaces, payload)
		switch outcome {
		case outcomeHeld:
			// unknown outcome：门禁刻意保持持有，交由上层人工/对账介入
			return err
		case outcomeRetry:
			lastErr = err
			continue
		default:
			return err
		}
	}

	return fmt.Errorf("%w: attempts exhausted (last error: %v)", ErrRetryExhausted, lastErr)
}

// runAttempt 执行单次尝试。所有资源（外层事务、本地门禁、准入许可、锁上下文）都在本函数内
// 通过统一的 defer 清理（R2 I1 + R18 C-1）：
//   - 正常错误路径：Rollback → 逆序释放本地门禁 → lockCtx.Reset() → 归还准入许可
//   - panic 路径：同样清理后以原 panic 值重新 panic（保留调用方的 recover 语义与
//     堆栈信息；把 panic 降级成 error 会让上层误判为业务错误并可能被重试逻辑吞掉）
//   - 已提交成功 / unknown-outcome hold：不清理门禁（前者已释放，后者按设计继续封锁），
//     但**准入许可照样归还**
//
// 准入许可的生命周期（R18 C-1 / I-1，务必先读）：
//   - 取得点：本地门禁全部到手之后、BeginTx 之前（I-1：不在无界门禁等待前占许可）；
//   - 归还点：本函数**最外层**的 defer（注册最早 → 执行最晚）。因此归还严格发生在
//     "回滚完成、连接已归还池、门禁已释放"之后，许可的占用窗口不会比连接窗口短一拍；
//   - 该 defer 用闭包变量 releaseAdmission 判定是否已持有许可，注册点早于取得点，
//     所以 payload panic 的栈展开、以及 payload 内 runtime.Goexit（如误用 t.Fatal）
//     都必然经过它 —— 绝不允许改回"runAttempt 返回后再 releaseAdmission()"这种普通语句：
//     普通语句会被栈展开跳过，每次 panic 泄漏 1 个进程级许可，16 次即整个 lockorder
//     被自身准入机制永久拒绝服务（R18 C-1 的原始故障形态）。
//
// 提交结局判定（R2 I7）：Commit 返回错误时，只有 commitProvablyNotAttempted 能证明
// "提交请求未送达"的错误（sql.ErrTxDone / 发送前的 ctx 结束）才按普通失败处理；
// 其余一律 unknown-outcome —— 包括 mysql.ErrPktSync 这类协议失步，
// 以及任何无法归类的驱动错误（保守默认，绝不靠有限白名单反推"已回滚"）。
func (r *TxRunner) runAttempt(
	ctx context.Context,
	gm GateManager,
	admission *AdmissionControl,
	opts TxRunnerOptions,
	namespaces []string,
	payload TxPayloadFunc,
) (error, attemptOutcome) {
	lockCtx := NewLockContext()

	var (
		tx              *sql.Tx
		heldBusiness    bool
		heldNS          []string
		committed       bool
		holdGates       bool
		commitAttempted bool
		// releaseAdmission 非 nil 表示本次尝试已持有连接池准入许可。
		// 只允许由下面第一个 defer 读取并归还（R18 C-1）。
		releaseAdmission func()
	)

	// defer #1（注册最早 → 执行最晚）：无条件归还准入许可。
	// 放在最外层注册的两个目的：
	//  1. 即使 cleanup()/门禁释放自身再次 panic，本 defer 仍会在同一帧的展开中执行；
	//  2. 归还点晚于 cleanup（回滚 + 连接归还 + 门禁释放），许可窗口覆盖完整的持连接窗口。
	defer func() {
		if releaseAdmission != nil {
			releaseAdmission()
			releaseAdmission = nil
		}
	}()

	// 逆序释放本地门禁闭包：namespace 倒序，business 最后；重复调用安全
	releaseLocalGates := func() {
		for i := len(heldNS) - 1; i >= 0; i-- {
			gm.ReleaseLocalNamespace(heldNS[i])
		}
		heldNS = nil

		if heldBusiness {
			gm.ReleaseLocalBusiness()
			heldBusiness = false
		}
	}

	rollbackTx := func() {
		if tx != nil {
			rbErr := tx.Rollback()
			tx = nil
			// R2 I7：一旦 Commit 已被尝试，stdlib 在调用 driver Commit 前就把 tx.done 置位，
			// 此处的 Rollback 只可能返回 sql.ErrTxDone。它**不是**"服务端已撤销"的证据，
			// 结局判定完全由 commitErr 的分类决定；这里只留痕，绝不据此改变门禁决策。
			if commitAttempted && errors.Is(rbErr, sql.ErrTxDone) {
				log.Printf("[lockorder] rollback after commit attempt returned sql.ErrTxDone; " +
					"treated as NOT evidence of rollback")
			}
		}
	}

	cleanup := func() {
		rollbackTx()
		if !holdGates {
			releaseLocalGates()
		}
		lockCtx.Reset()
	}

	defer func() {
		if p := recover(); p != nil {
			cleanup()
			panic(p)
		}
		if committed || holdGates {
			return
		}
		cleanup()
	}()

	// 1. 获取本地门禁（Local Gates）——必须先于任何 DB 锁
	if opts.RequiresBusinessGate {
		if err := gm.AcquireLocalBusiness(ctx); err != nil {
			return err, outcomeDone
		}
		heldBusiness = true
		if err := lockCtx.RecordLocalBusinessGate(); err != nil {
			return err, outcomeDone
		}
	}

	for _, ns := range namespaces {
		if err := gm.AcquireLocalNamespace(ctx, ns); err != nil {
			return err, outcomeDone
		}
		heldNS = append(heldNS, ns)
		if err := lockCtx.RecordLocalNamespaceGate(ns); err != nil {
			return err, outcomeDone
		}
	}

	// 2. 连接池准入许可（R2 I2）——**必须在本地门禁之后、BeginTx 之前**（R18 I-1）。
	//
	// 等待许可期间本方不持有任何连接，因此不构成 hold-and-wait；等待可被 ctx 取消且有上限
	// （默认 5s），context.Background() 也绝不会永久死等。
	// 许可覆盖"外层事务连接 + payload 内 fresh locator 第二条连接"的整个窗口，
	// 归还由本函数最外层的 defer #1 负责（R18 C-1：panic/Goexit 也必然归还）。
	var err error
	releaseAdmission, err = admission.Acquire(ctx)
	if err != nil {
		// 未持有许可：releaseAdmission 保持 nil，defer #1 是无操作。
		return fmt.Errorf("runner admission: %w", err), outcomeDone
	}

	// 3. 开始外层数据事务（Run 已保证 r.db 非 nil）
	tx, err = r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin outer data tx: %w", err), outcomeDone
	}

	// 4. 在外层数据事务内获取 DB Gate 锁
	if err := acquireDBGates(ctx, tx, lockCtx, opts, namespaces); err != nil {
		if IsRetryableConflict(err) {
			return err, outcomeRetry
		}
		return err, outcomeDone
	}

	// 5. 执行业务载荷（包含 fresh locator 与实体加锁重验）
	payloadErr := payload(ctx, tx, lockCtx)
	if payloadErr == nil {
		// 6. 提交事务
		commitAttempted = true
		commitErr := tx.Commit()
		switch {
		case commitErr == nil:
			// 成功提交：逆序释放本地门禁，返回成功
			committed = true
			tx = nil
			releaseLocalGates()
			return nil, outcomeDone

		case commitProvablyNotAttempted(commitErr):
			// 可证明提交请求未送达服务端（ErrTxDone / 发送前的 ctx 结束）：
			// 按普通失败处理，回滚外层事务并逆序释放门禁。
			payloadErr = commitErr

		case isOutcomeUnknown(commitErr):
			holdGates = true
			log.Printf("[lockorder] tx commit outcome unknown: %v; holding local gates", commitErr)
			return fmt.Errorf("%w: %w", ErrOutcomeUnknown, commitErr), outcomeHeld

		default:
			// R2 I7：其余任何提交错误都**无法证明**未提交（服务端可能已经提交），
			// 一律按 unknown-outcome 处理：保持门禁封锁（DefaultGateManager 进程级
			// fail closed），绝不自动重放、绝不用 Rollback 的 ErrTxDone 反推"已撤销"。
			holdGates = true
			log.Printf("[lockorder] tx commit failed with non-provable error, outcome unknown: %v; holding local gates", commitErr)
			return fmt.Errorf("%w: commit error not provably pre-send: %w", ErrOutcomeUnknown, commitErr), outcomeHeld
		}
	}

	// 执行失败或提交失败：整体回滚外层事务，逆序释放全部门禁（由 defer 统一执行）
	if IsRetryableConflict(payloadErr) {
		return payloadErr, outcomeRetry
	}
	// 非可重试错误：立即 fail closed
	return payloadErr, outcomeDone
}

// acquireDBGates 在外层数据事务内按 business_relations → namespace 升序获取 DB 行锁。
// tx 由 runAttempt 保证非 nil（R2 I9：不再存在 nil tx 的"离线"分支）。
func acquireDBGates(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, opts TxRunnerOptions, namespaces []string) error {
	if opts.RequiresBusinessGate {
		if err := lockCtx.RecordDBBusinessGate(); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES ('business_relations')"); err != nil {
			return fmt.Errorf("lock db business_relations: insert ignore: %w", err)
		}
		var name string
		row := tx.QueryRowContext(ctx, "SELECT name FROM locks WHERE name = 'business_relations' FOR UPDATE")
		if err := row.Scan(&name); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lock db business_relations: row not found under lock")
			}
			return fmt.Errorf("lock db business_relations: %w", err)
		}
	}

	for _, ns := range namespaces {
		if err := lockCtx.RecordDBNamespaceGate(ns); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES (?)", ns); err != nil {
			return fmt.Errorf("lock db namespace %s: insert ignore: %w", ns, err)
		}
		var name string
		row := tx.QueryRowContext(ctx, "SELECT name FROM locks WHERE name = ? FOR UPDATE", ns)
		if err := row.Scan(&name); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lock db namespace %s: row not found under lock", ns)
			}
			return fmt.Errorf("lock db namespace %s: %w", ns, err)
		}
	}
	return nil
}
