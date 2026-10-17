package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrMembershipDrift     = errors.New("lockorder: membership drift detected during current verification")
	ErrEntityNotFound      = errors.New("lockorder: entity not found under lock")
	ErrLowerPKDiscovered   = errors.New("lockorder: lower PK discovered, backwards locking prohibited")
	ErrNewMemberDiscovered = errors.New("lockorder: new member discovered, full rollback required")
)

// EntityState 实体当前态数据
type EntityState struct {
	Key            EntityKey
	StableIdentity string
	Live           bool
	Version        uint64
}

// QueryRower 查询接口抽象
type QueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// CurrentVerifier 当前态验证函数类型
type CurrentVerifier func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error)

// BuildLockSQL 根据表元信息生成精确的单主键加锁 SQL
func BuildLockSQL(meta TableMeta) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? FOR UPDATE", meta.PKColumn, meta.Name, meta.PKColumn)
}

// LockEntitiesInOrder 外层数据事务按严格 (table-order, 物理 PK 升序) 执行 FOR UPDATE。
//
// tx 为 nil 时返回 ErrNilTx：本函数是导出执行入口，绝不允许把"只登记、未加锁"
// 当作离线成功（R2 I9）。纯锁序校验请直接使用 LockContext / SortEntityKeys。
func LockEntitiesInOrder(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, keys []EntityKey) error {
	if !IsSorted(keys) {
		return fmt.Errorf("%w: candidate keys must be strictly sorted before locking", ErrOutOfOrderLock)
	}
	if tx == nil {
		return fmt.Errorf("%w: LockEntitiesInOrder requires a non-nil *sql.Tx", ErrNilTx)
	}

	for _, key := range keys {
		// 登记到锁上下文，验证未违反全序规则
		if err := lockCtx.RecordEntityLock(key); err != nil {
			return err
		}

		meta, err := LookupTableOrder(key.Table)
		if err != nil {
			return err
		}

		query := BuildLockSQL(meta)
		var lockedPK uint64
		row := tx.QueryRowContext(ctx, query, key.PK)
		if err := row.Scan(&lockedPK); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: table %s pk %d", ErrEntityNotFound, meta.Name, key.PK)
			}
			return fmt.Errorf("lock entity %s: %w", key, err)
		}
	}
	return nil
}

// VerifyCurrentStates 在持有锁后执行 current read，验证逻辑身份与活性
func VerifyCurrentStates(ctx context.Context, q QueryRower, keys []EntityKey, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if verifier == nil {
		return nil, errors.New("lockorder: nil CurrentVerifier")
	}

	states := make([]*EntityState, 0, len(keys))
	for _, key := range keys {
		st, err := verifier(ctx, q, key)
		if err != nil {
			return nil, fmt.Errorf("verify entity %s: %w", key, err)
		}
		if st == nil {
			return nil, fmt.Errorf("%w: entity %s returned nil state", ErrMembershipDrift, key)
		}
		if !st.Live {
			return nil, fmt.Errorf("%w: entity %s is marked inactive or deleted", ErrMembershipDrift, key)
		}

		if snapshot != nil && snapshot.StableIdentities != nil {
			if expectedIdent, ok := snapshot.StableIdentities[key]; ok {
				if st.StableIdentity != expectedIdent {
					return nil, fmt.Errorf("%w: entity %s identity mismatch (expected %s, got %s)",
						ErrMembershipDrift, key, expectedIdent, st.StableIdentity)
				}
			}
		}
		states = append(states, st)
	}
	return states, nil
}

// LockAndVerify 将排序加锁与当前态核验原子组合。
// tx 为 nil 时返回 ErrNilTx（R2 I9）：加锁与核验必须在真实外层事务内发生。
func LockAndVerify(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if snapshot == nil {
		return nil, errors.New("lockorder: nil snapshot")
	}
	if tx == nil {
		return nil, fmt.Errorf("%w: LockAndVerify requires a non-nil *sql.Tx", ErrNilTx)
	}

	// 1. 严格全序加锁
	if err := LockEntitiesInOrder(ctx, tx, lockCtx, snapshot.CandidateKeys); err != nil {
		return nil, err
	}

	// 2. 持锁下立即进行 current read 核验
	states, err := VerifyCurrentStates(ctx, tx, snapshot.CandidateKeys, snapshot, verifier)
	if err != nil {
		// 核验失败直接透出 ErrMembershipDrift 或内部错误，调用层必须整体回滚，严禁追加补锁
		return nil, err
	}
	return states, nil
}
