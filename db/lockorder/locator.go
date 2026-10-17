package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	// MaxLocatorCandidates 单次 locator 快照候选实体数量硬上限，防止无界查询拖垮事务
	MaxLocatorCandidates = 1000
)

var (
	ErrCandidateLimitExceeded = errors.New("lockorder: candidate limit exceeded in locator query")
	ErrNilDatabase            = errors.New("lockorder: nil database handle")
	ErrNilLocatorFunc         = errors.New("lockorder: nil locator function")
)

// LocatorSnapshot 只读 locator 提取的候选集快照
type LocatorSnapshot struct {
	CandidateKeys    []EntityKey
	StableIdentities map[EntityKey]string
	CreatedAt        time.Time
}

// LocatorFunc 在独立只读事务中执行的纯读查询函数
type LocatorFunc func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error)

// ExecuteFreshLocator 使用同一连接池另开独立只读事务，执行非加锁快照查询并安全回收连接。
//
// 使用进程级共享的 DefaultLocatorAdmission() 做连接准入（R2 I2）：
// 本函数会在调用方已持有一条连接（外层事务）的情况下再取第二条，
// 不限流时 32 个并发 Run 会在 maxOpenConns=32 的共享池上形成 hold-and-wait。
// 需要自定义容量时改用 ExecuteFreshLocatorWithAdmission。
func ExecuteFreshLocator(ctx context.Context, db *sql.DB, fn LocatorFunc) (*LocatorSnapshot, error) {
	return ExecuteFreshLocatorWithAdmission(ctx, db, fn, DefaultLocatorAdmission())
}

// ExecuteFreshLocatorWithAdmission 与 ExecuteFreshLocator 相同，但显式指定连接准入信号量。
//
// admission 为 nil 时退回进程级共享单例；admission.Capacity() <= 0 表示不限流。
// 等待许可可被 ctx 取消，并有等待上限（默认 5s）：拿不到许可时返回包装
// ErrAdmissionTimeout 的错误，**不会**在连接池上无限排队（R2 I2）。
func ExecuteFreshLocatorWithAdmission(
	ctx context.Context,
	db *sql.DB,
	fn LocatorFunc,
	admission *AdmissionControl,
) (*LocatorSnapshot, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}
	if fn == nil {
		return nil, ErrNilLocatorFunc
	}
	if admission == nil {
		admission = DefaultLocatorAdmission()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 准入许可必须在 BeginTx **之前**取得、在连接干净归还**之后**才释放。
	// defer 顺序（LIFO）：先注册 release → 最后执行；后注册 rollback → 先执行。
	releaseAdmission, err := admission.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("fresh locator admission: %w", err)
	}
	defer releaseAdmission()

	// 明确开辟只读隔离事务：快照建立在全部门禁已获取之后，排除外层 data TX 老快照漏成员
	locTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("fresh locator begin tx: %w", err)
	}

	closed := false
	defer func() {
		if !closed {
			_ = locTx.Rollback()
		}
	}()

	snapshot, err := fn(ctx, locTx)
	if err != nil {
		return nil, fmt.Errorf("fresh locator query: %w", err)
	}
	if snapshot == nil {
		snapshot = &LocatorSnapshot{}
	}

	if len(snapshot.CandidateKeys) > MaxLocatorCandidates {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrCandidateLimitExceeded, len(snapshot.CandidateKeys), MaxLocatorCandidates)
	}

	// 显式结束独立只读事务，将连接干净交还连接池
	if err := locTx.Commit(); err != nil {
		return nil, fmt.Errorf("fresh locator commit: %w", err)
	}
	closed = true

	// 自动排序去重，保证满足全序实体加锁前置条件
	snapshot.CandidateKeys = SortEntityKeys(snapshot.CandidateKeys)
	if snapshot.StableIdentities == nil {
		snapshot.StableIdentities = make(map[EntityKey]string)
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}

	return snapshot, nil
}
