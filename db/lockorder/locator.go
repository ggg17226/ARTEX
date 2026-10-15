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

// ExecuteFreshLocator 使用同一连接池另开独立只读事务，执行非加锁快照查询并安全回收连接
func ExecuteFreshLocator(ctx context.Context, db *sql.DB, fn LocatorFunc) (*LocatorSnapshot, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}
	if fn == nil {
		return nil, ErrNilLocatorFunc
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

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
