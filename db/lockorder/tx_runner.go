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

// GateManager 本地门禁管理器接口
type GateManager interface {
	AcquireLocalBusiness(ctx context.Context) error
	ReleaseLocalBusiness()
	AcquireLocalNamespace(ctx context.Context, ns string) error
	ReleaseLocalNamespace(ns string)
}

// MemoryGateManager 基于内存读写锁的本地门禁实现
type MemoryGateManager struct {
	businessMu sync.Mutex
	nsMu       sync.Mutex
	nsLocks    map[string]*sync.Mutex
}

// NewMemoryGateManager 创建基于内存的门禁管理器
func NewMemoryGateManager() *MemoryGateManager {
	return &MemoryGateManager{
		nsLocks: make(map[string]*sync.Mutex),
	}
}

func (m *MemoryGateManager) AcquireLocalBusiness(ctx context.Context) error {
	m.businessMu.Lock()
	return nil
}

func (m *MemoryGateManager) ReleaseLocalBusiness() {
	m.businessMu.Unlock()
}

func (m *MemoryGateManager) getNSMutex(ns string) *sync.Mutex {
	m.nsMu.Lock()
	defer m.nsMu.Unlock()
	mu, ok := m.nsLocks[ns]
	if !ok {
		mu = &sync.Mutex{}
		m.nsLocks[ns] = mu
	}
	return mu
}

func (m *MemoryGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	mu := m.getNSMutex(ns)
	mu.Lock()
	return nil
}

func (m *MemoryGateManager) ReleaseLocalNamespace(ns string) {
	mu := m.getNSMutex(ns)
	mu.Unlock()
}

// TxRunnerOptions 事务运行器配置
type TxRunnerOptions struct {
	RequiresBusinessGate bool
	RequiredNamespaces   []string
	BackoffSchedule      []time.Duration
	GateManager          GateManager
}

// TxPayloadFunc 在全序与门禁保护下的核心事务执行逻辑
type TxPayloadFunc func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error

// TxRunner 全事务回滚与退避重试执行器
type TxRunner struct {
	db      *sql.DB
	sleeper func(time.Duration)
}

// NewTxRunner 创建事务运行器
func NewTxRunner(db *sql.DB) *TxRunner {
	return &TxRunner{
		db:      db,
		sleeper: time.Sleep,
	}
}

// SetSleeper 设置用于单测模拟的休眠函数
func (r *TxRunner) SetSleeper(sleeper func(time.Duration)) {
	if sleeper != nil {
		r.sleeper = sleeper
	}
}

// Run 执行带全序门禁与有界退避重试的悲观事务
func (r *TxRunner) Run(ctx context.Context, opts TxRunnerOptions, payload TxPayloadFunc) error {
	if payload == nil {
		return errors.New("lockorder: nil payload function")
	}

	gm := opts.GateManager
	if gm == nil {
		gm = NewMemoryGateManager()
	}

	schedule := opts.BackoffSchedule
	if len(schedule) == 0 {
		schedule = DefaultBackoffSchedule
	}
	maxAttempts := len(schedule) + 1

	// 预先对 namespace 按名称升序排序，严格保证全序
	sortedNamespaces := make([]string, len(opts.RequiredNamespaces))
	copy(sortedNamespaces, opts.RequiredNamespaces)
	sort.Strings(sortedNamespaces)

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			sleepDur := schedule[attempt-1]
			r.sleeper(sleepDur)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// 创建当前尝试独立的锁上下文
		lockCtx := NewLockContext()

		// 1. 获取本地门禁（Local Gates）
		var heldLocalBusiness bool
		var heldLocalNamespaces []string

		acquireLocalGates := func() error {
			if opts.RequiresBusinessGate {
				if err := gm.AcquireLocalBusiness(ctx); err != nil {
					return err
				}
				heldLocalBusiness = true
				if err := lockCtx.RecordLocalBusinessGate(); err != nil {
					return err
				}
			}

			for _, ns := range sortedNamespaces {
				if err := gm.AcquireLocalNamespace(ctx, ns); err != nil {
					return err
				}
				heldLocalNamespaces = append(heldLocalNamespaces, ns)
				if err := lockCtx.RecordLocalNamespaceGate(ns); err != nil {
					return err
				}
			}
			return nil
		}

		// 逆序释放本地门禁闭包
		releaseLocalGates := func() {
			// 先逆序释放 namespace gates
			for i := len(heldLocalNamespaces) - 1; i >= 0; i-- {
				gm.ReleaseLocalNamespace(heldLocalNamespaces[i])
			}
			heldLocalNamespaces = nil

			// 最后释放 business gate
			if heldLocalBusiness {
				gm.ReleaseLocalBusiness()
				heldLocalBusiness = false
			}
		}

		if err := acquireLocalGates(); err != nil {
			releaseLocalGates()
			return err
		}

		// 2. 开始外层数据事务
		var tx *sql.Tx
		var err error
		if r.db != nil {
			tx, err = r.db.BeginTx(ctx, nil)
			if err != nil {
				releaseLocalGates()
				return fmt.Errorf("begin outer data tx: %w", err)
			}
		}

		// 3. 在外层数据事务内获取 DB Gate 锁
		acquireDBGates := func() error {
			if opts.RequiresBusinessGate {
				if err := lockCtx.RecordDBBusinessGate(); err != nil {
					return err
				}
				if tx != nil {
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
			}

			for _, ns := range sortedNamespaces {
				if err := lockCtx.RecordDBNamespaceGate(ns); err != nil {
					return err
				}
				if tx != nil {
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
			}
			return nil
		}

		gateErr := acquireDBGates()
		if gateErr != nil {
			if tx != nil {
				_ = tx.Rollback()
			}
			releaseLocalGates()
			lockCtx.Reset()
			if IsRetryableConflict(gateErr) {
				lastErr = gateErr
				continue
			}
			return gateErr
		}

		// 4. 执行业务载荷（包含 fresh locator 与实体加锁重验）
		payloadErr := payload(ctx, tx, lockCtx)
		if payloadErr == nil {
			// 5. 提交事务
			var commitErr error
			if tx != nil {
				commitErr = tx.Commit()
			}
			if commitErr == nil {
				// 成功提交：逆序释放本地门禁，返回成功
				releaseLocalGates()
				return nil
			}
			if isOutcomeUnknown(commitErr) {
				log.Printf("[lockorder] tx commit outcome unknown: %v; holding local gates", commitErr)
				return fmt.Errorf("%w: %v", ErrOutcomeUnknown, commitErr)
			}
			payloadErr = commitErr
		}

		// 执行失败或提交失败：整体回滚外层事务，逆序释放全部门禁
		if tx != nil {
			_ = tx.Rollback()
		}
		releaseLocalGates()
		lockCtx.Reset()

		if IsRetryableConflict(payloadErr) {
			lastErr = payloadErr
			continue
		}

		// 非可重试错误：立即 fail closed
		return payloadErr
	}

	return fmt.Errorf("%w: attempts exhausted (last error: %v)", ErrRetryExhausted, lastErr)
}
