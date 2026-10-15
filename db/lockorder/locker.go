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

// LockEntitiesInOrder 外层数据事务按严格 (table-order, 物理 PK 升序) 执行 FOR UPDATE
func LockEntitiesInOrder(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, keys []EntityKey) error {
	if !IsSorted(keys) {
		return fmt.Errorf("%w: candidate keys must be strictly sorted before locking", ErrOutOfOrderLock)
	}

	for _, key := range keys {
		// 登记到锁上下文，验证未违反全序规则
		if err := lockCtx.RecordEntityLock(key); err != nil {
			return err
		}

		// 若 tx 为 nil（用于离线单测），仅验证锁序
		if tx == nil {
			continue
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

// LockAndVerify 将排序加锁与当前态核验原子组合
func LockAndVerify(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if snapshot == nil {
		return nil, errors.New("lockorder: nil snapshot")
	}

	// 1. 严格全序加锁
	if err := LockEntitiesInOrder(ctx, tx, lockCtx, snapshot.CandidateKeys); err != nil {
		return nil, err
	}

	// 2. 持锁下立即进行 current read 核验
	var qr QueryRower
	if tx != nil {
		qr = tx
	}
	states, err := VerifyCurrentStates(ctx, qr, snapshot.CandidateKeys, snapshot, verifier)
	if err != nil {
		// 核验失败直接透出 ErrMembershipDrift 或内部错误，调用层必须整体回滚，严禁追加补锁
		return nil, err
	}
	return states, nil
}
