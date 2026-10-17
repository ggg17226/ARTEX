package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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

// mysqlErrorNumberExtractor 兼容从不同 mysql driver 错误中提取状态码
type mysqlErrorNumberExtractor interface {
	error
	// 部分驱动或自定义包装包含此接口
}

// IsRetryableConflict 判断错误是否为死锁、超时、唯一键冲突或成员漂移等可重试冲突
func IsRetryableConflict(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrMembershipDrift) ||
		errors.Is(err, ErrLowerPKDiscovered) ||
		errors.Is(err, ErrNewMemberDiscovered) {
		return true
	}

	// 排除主动取消或超时的上下文错误
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// 错误编号探测（1062, 1213, 1205）
	type numberer interface {
		Number() uint16
	}
	var numGetter numberer
	if errors.As(err, &numGetter) {
		switch numGetter.Number() {
		case 1062, 1213, 1205:
			return true
		}
	}

	// 结构体直接字段反射匹配测试 mock 与原生驱动
	type numField struct {
		Number uint16
	}
	var nf interface{ Error() string }
	if errors.As(err, &nf) {
		type hasNum interface {
			Number() uint16
		}
		// 检查常规包含 Number uint16 导出的结构
		type errNum interface {
			Number() uint16
		}
		var en errNum
		if errors.As(err, &en) {
			switch en.Number() {
			case 1062, 1213, 1205:
				return true
			}
		}
	}

	// 备选字符串特征匹配（兜底非结构化包装）
	msg := err.Error()
	if containsSubstr(msg, "1213") || containsSubstr(msg, "Deadlock") ||
		containsSubstr(msg, "1205") || containsSubstr(msg, "Lock wait timeout") ||
		containsSubstr(msg, "1062") || containsSubstr(msg, "Duplicate entry") {
		return true
	}

	return false
}

func containsSubstr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && stringSearch(s, substr)))
}

func stringSearch(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// isOutcomeUnknown 识别连接重置、broken pipe、connection lost 等网络/驱动断连类错误，或特定标记
func isOutcomeUnknown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) {
		return true
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 2006, 2013: // CR_SERVER_GONE_ERROR, CR_SERVER_LOST
			return true
		}
	}
	type hasNumMethod interface {
		Number() uint16
	}
	var hnm hasNumMethod
	if errors.As(err, &hnm) {
		switch hnm.Number() {
		case 2006, 2013:
			return true
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection lost") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "server has gone away") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "bad connection") {
		return true
	}
	return false
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
// ctx 契约：门禁等待与退避休眠都可被 ctx 取消；payload 自身也必须尊重 ctx。
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

		err, outcome := r.runAttempt(ctx, gm, opts, sortedNamespaces, payload)
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

// runAttempt 执行单次尝试。所有资源（外层事务、本地门禁、锁上下文）都在本函数内
// 通过统一的 per-attempt defer 清理（R2 I1）：
//   - 正常错误路径：Rollback → 逆序释放本地门禁 → lockCtx.Reset()
//   - panic 路径：同样清理后以原 panic 值重新 panic（保留调用方的 recover 语义与
//     堆栈信息；把 panic 降级成 error 会让上层误判为业务错误并可能被重试逻辑吞掉）
//   - 已提交成功 / unknown-outcome hold：不清理门禁（前者已释放，后者按设计继续封锁）
func (r *TxRunner) runAttempt(
	ctx context.Context,
	gm GateManager,
	opts TxRunnerOptions,
	namespaces []string,
	payload TxPayloadFunc,
) (error, attemptOutcome) {
	lockCtx := NewLockContext()

	var (
		tx           *sql.Tx
		heldBusiness bool
		heldNS       []string
		committed    bool
		holdGates    bool
	)

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
			_ = tx.Rollback()
			tx = nil
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

	// 2. 开始外层数据事务（Run 已保证 r.db 非 nil）
	var err error
	tx, err = r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin outer data tx: %w", err), outcomeDone
	}

	// 3. 在外层数据事务内获取 DB Gate 锁
	if err := acquireDBGates(ctx, tx, lockCtx, opts, namespaces); err != nil {
		if IsRetryableConflict(err) {
			return err, outcomeRetry
		}
		return err, outcomeDone
	}

	// 4. 执行业务载荷（包含 fresh locator 与实体加锁重验）
	payloadErr := payload(ctx, tx, lockCtx)
	if payloadErr == nil {
		// 5. 提交事务
		commitErr := tx.Commit()
		if commitErr == nil {
			// 成功提交：逆序释放本地门禁，返回成功
			committed = true
			tx = nil
			releaseLocalGates()
			return nil, outcomeDone
		}
		if isOutcomeUnknown(commitErr) {
			holdGates = true
			log.Printf("[lockorder] tx commit outcome unknown: %v; holding local gates", commitErr)
			return fmt.Errorf("%w: %v", ErrOutcomeUnknown, commitErr), outcomeHeld
		}
		payloadErr = commitErr
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
