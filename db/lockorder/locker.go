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

// ---------------------------------------------------------------------------
// C2：持锁后的成员漂移核验（P04b Global Constraint 5）
// ---------------------------------------------------------------------------

// ReLocateRower 持锁重定位所需的最小查询接口：在单行查询之外增加多行查询，
// 使 ReLocator 能在**已持有实体锁的外层事务**内枚举当前成员集合。
// *sql.Tx 与 *sql.DB 天然满足本接口。
type ReLocateRower interface {
	QueryRower
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ReLocator 在已持有实体锁的外层事务内重新执行候选集定位（current read），
// 返回**当前**成员集合，供 CompareMembership 与 LocatorSnapshot.CandidateKeys 比对（R2 C2）。
//
// 契约（务必遵守）：
//   - 必须读取当前可见成员：TiDB 下需要显式 current read（锁定读或等价手段），
//     REPEATABLE READ 的普通 SELECT 读的是本事务旧快照，发现不了新成员；
//   - 只做"发现"，绝不做"吸收"：即使发现新成员或更低序实体，也**不得**在本回调内
//     对其追加 FOR UPDATE 或任何补锁动作。发现即由 CompareMembership 返回
//     ErrMembershipDrift，由外层整体 ROLLBACK、逆序释放全部锁与门禁、清空 lockCtx
//     后重做非加锁 Locator —— 此为本包红线（P04b Global Constraint 5：
//     绝对禁止在已持实体集合上反向追加补锁）。
type ReLocator func(ctx context.Context, q ReLocateRower) ([]EntityKey, error)

// membershipDriftError 漂移错误：让同一个错误同时命中 ErrMembershipDrift 与具体原因哨兵
// （ErrNewMemberDiscovered / ErrLowerPKDiscovered / ErrEntityNotFound），
// 调用方既可统一按"漂移 → 整体回滚 + 重做 locator"处理，也能精确区分漂移类别。
type membershipDriftError struct {
	msg   string
	cause error
}

func (e *membershipDriftError) Error() string { return e.msg }

// Unwrap 返回多错误：errors.Is/As 会同时看到 ErrMembershipDrift 与具体原因
func (e *membershipDriftError) Unwrap() []error {
	if e.cause == nil || e.cause == ErrMembershipDrift {
		return []error{ErrMembershipDrift}
	}
	return []error{ErrMembershipDrift, e.cause}
}

// Cause 返回具体漂移原因；纯身份/活性漂移为 nil
func (e *membershipDriftError) Cause() error { return e.cause }

// newMembershipDrift 构造漂移错误。cause 为 nil 表示无附加类别（身份/活性/范围漂移）。
func newMembershipDrift(cause error, format string, args ...any) error {
	return &membershipDriftError{msg: fmt.Sprintf(format, args...), cause: cause}
}

// membershipDiff 成员集合差异结果；指针字段为 nil 表示该类差异不存在
type membershipDiff struct {
	// lowerPK 当前集合中出现序严格低于快照最小成员的成员（backwards locking 风险最高）
	lowerPK *EntityKey
	// newMember 当前集合中出现快照没有的成员（非更低序）
	newMember *EntityKey
	// missing 快照成员在当前集合中缺失
	missing *EntityKey
	// minSnap 快照的最小序成员（仅 hasMin 为真时有效）
	minSnap EntityKey
	hasMin  bool
}

// any 报告是否存在任一差异
func (d membershipDiff) any() bool {
	return d.lowerPK != nil || d.newMember != nil || d.missing != nil
}

// describe 返回差异的简短人类可读描述（用于不附带类别哨兵的范围漂移错误信息）
func (d membershipDiff) describe() string {
	switch {
	case d.lowerPK != nil:
		return fmt.Sprintf("lower-order entity %s present", *d.lowerPK)
	case d.newMember != nil:
		return fmt.Sprintf("unexpected entity %s present", *d.newMember)
	case d.missing != nil:
		return fmt.Sprintf("snapshot entity %s absent", *d.missing)
	}
	return "membership mismatch"
}

// diffMembership 以集合语义（排序无关、自动去重）比对快照成员与当前成员。
//
// 空快照不得短路：快照为空而当前集合非空时，任何成员都是"新成员"漂移。
func diffMembership(snapshot, current []EntityKey) membershipDiff {
	var d membershipDiff

	snapSet := make(map[EntityKey]struct{}, len(snapshot))
	for _, k := range snapshot {
		snapSet[k] = struct{}{}
		if !d.hasMin || CompareEntityKey(k, d.minSnap) < 0 {
			d.minSnap = k
			d.hasMin = true
		}
	}

	seen := make(map[EntityKey]struct{}, len(current))
	for _, k := range current {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if _, ok := snapSet[k]; ok {
			continue
		}
		if d.hasMin && CompareEntityKey(k, d.minSnap) < 0 {
			if d.lowerPK == nil {
				lower := k
				d.lowerPK = &lower
			}
			continue
		}
		if d.newMember == nil {
			fresh := k
			d.newMember = &fresh
		}
	}

	for _, k := range snapshot {
		if _, ok := seen[k]; !ok {
			if d.missing == nil {
				gone := k
				d.missing = &gone
			}
		}
	}

	return d
}

// CompareMembership 比对"快照成员集"与"持锁后重新定位到的当前成员集"（R2 C2）。
//
// 判定四类差异，任一命中即返回可被 errors.Is(err, ErrMembershipDrift) 识别的漂移错误：
//  1. 当前集合出现序更低（同表更小 PK 或更小 table-order）的成员 → 附带 ErrLowerPKDiscovered；
//  2. 当前集合出现快照中没有的新成员（含快照为空而当前非空）→ 附带 ErrNewMemberDiscovered；
//  3. 快照成员在当前集合中缺失（实体被删除）→ 附带 ErrEntityNotFound（R2 I5）；
//  4. 两侧完全一致 → 返回 nil。
//
// 本函数是纯比对：绝不发起任何 SQL、绝不追加补锁。发现漂移后唯一合法动作是
// 整体 ROLLBACK 外层事务并重做非加锁 Locator（P04b Global Constraint 5）。
func CompareMembership(snapshot, current []EntityKey) error {
	d := diffMembership(snapshot, current)

	switch {
	case d.lowerPK != nil:
		return newMembershipDrift(ErrLowerPKDiscovered,
			"membership drift: entity %s discovered under current read at a lower position than every snapshot member (snapshot minimum %s); "+
				"appending locks backwards is prohibited, full rollback and re-locate required",
			*d.lowerPK, d.minSnap)
	case d.newMember != nil:
		return newMembershipDrift(ErrNewMemberDiscovered,
			"membership drift: entity %s discovered under current read but absent from the %d-key snapshot; "+
				"appending locks is prohibited, full rollback and re-locate required",
			*d.newMember, len(snapshot))
	case d.missing != nil:
		return newMembershipDrift(ErrEntityNotFound,
			"membership drift: snapshot entity %s is missing under current read (deleted or never existed); "+
				"full rollback and re-locate required", *d.missing)
	}
	return nil
}

// BuildLockSQL 根据表元信息生成精确的单主键加锁 SQL
func BuildLockSQL(meta TableMeta) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? FOR UPDATE", meta.PKColumn, meta.Name, meta.PKColumn)
}

// LockEntitiesInOrder 外层数据事务按严格 (table-order, 物理 PK 升序) 执行 FOR UPDATE。
//
// tx 为 nil 时返回 ErrNilTx：本函数是导出执行入口，绝不允许把"只登记、未加锁"
// 当作离线成功（R2 I9）。纯锁序校验请直接使用 LockContext / SortEntityKeys。
//
// 参数校验优先级（两个导出执行入口共用，R11 I-R11-1）：
//
//	① 锁序校验（IsSorted）→ ErrOutOfOrderLock
//	② nil tx 校验 → ErrNilTx
//
// 即"乱序/重复候选 + nil tx"同时存在时先报 ErrOutOfOrderLock：调用方需要先修的是
// 加锁全序，而不是句柄。LockAndVerify 直接委托本函数，因此两入口对同一输入给出
// 同一错误分类；修改本顺序会同时改变两个入口的契约。
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
				// R2 I5：持锁阶段实体物理缺失即 P04b Global Constraint 5 的"实体缺失"漂移，
				// 必须整体回滚并重做非加锁 Locator（IsRetryableConflict 已把该类别归入可重试）。
				// 同时保留 ErrEntityNotFound，使调用方仍能精确区分"缺失"与其他漂移类别。
				return newMembershipDrift(ErrEntityNotFound,
					"lock entity %s: not found under FOR UPDATE (treated as membership drift, full rollback and re-locate required)", key)
			}
			return fmt.Errorf("lock entity %s: %w", key, err)
		}
	}
	return nil
}

// VerifyCurrentStates 在持有锁后执行 current read，验证逻辑身份与活性（R2 C2 强化）。
//
// 逐实体核验以下漂移，任一命中即返回 errors.Is(err, ErrMembershipDrift) 的错误，
// 调用方必须整体回滚（严禁在已持实体集合上追加补锁）：
//   - 核验范围与快照成员集不一致（漏验或多验都说明"锁的不是验的、验的不是锁的"）；
//   - verifier 报告实体缺失（sql.ErrNoRows / ErrEntityNotFound）→ 附带 ErrEntityNotFound（R2 I5）；
//   - 返回 nil state、Live=false（软删/非活动）；
//   - 返回的 EntityState.Key 与被请求的 key 不符（身份错位）；
//   - 快照登记了稳定身份但缺该实体条目，或哈希与当前值不一致。
//
// 注意：本函数只覆盖"已锁定成员"的身份/活性，**无法**发现新成员或更低序实体；
// 完整成员复核请使用 VerifyCurrentStatesWithReLocate / LockAndVerifyWithReLocate。
func VerifyCurrentStates(ctx context.Context, q QueryRower, keys []EntityKey, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if verifier == nil {
		return nil, errors.New("lockorder: nil CurrentVerifier")
	}

	// 核验范围必须与快照成员集完全一致：范围差异本身就是成员漂移（R2 C2 "缺身份放行"反例）
	if snapshot != nil {
		if d := diffMembership(snapshot.CandidateKeys, keys); d.any() {
			return nil, newMembershipDrift(nil,
				"verify scope: verification key set differs from snapshot membership (%s); full rollback and re-locate required",
				d.describe())
		}
	}

	states := make([]*EntityState, 0, len(keys))
	for _, key := range keys {
		st, err := verifier(ctx, q, key)
		if err != nil {
			if errors.Is(err, ErrMembershipDrift) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrEntityNotFound) {
				// R2 I5：复核阶段实体不存在归入漂移，并保留"缺失"原因
				return nil, newMembershipDrift(ErrEntityNotFound,
					"verify entity %s: not found under current read (treated as membership drift, full rollback and re-locate required)", key)
			}
			return nil, fmt.Errorf("verify entity %s: %w", key, err)
		}
		if st == nil {
			return nil, newMembershipDrift(nil, "verify entity %s: returned nil state", key)
		}
		if st.Key != key {
			return nil, newMembershipDrift(nil,
				"verify entity %s: verifier returned state for %s (identity mismatch)", key, st.Key)
		}
		if !st.Live {
			return nil, newMembershipDrift(nil, "verify entity %s: marked inactive or deleted", key)
		}

		if snapshot != nil && len(snapshot.StableIdentities) > 0 {
			expectedIdent, ok := snapshot.StableIdentities[key]
			if !ok {
				return nil, newMembershipDrift(nil,
					"verify entity %s: snapshot carries stable identities but has no entry for it (incomplete snapshot)", key)
			}
			if st.StableIdentity != expectedIdent {
				return nil, newMembershipDrift(nil,
					"verify entity %s: stable identity mismatch (expected %s, got %s)", key, expectedIdent, st.StableIdentity)
			}
		}
		states = append(states, st)
	}
	return states, nil
}

// VerifyCurrentStatesWithReLocate 持锁后的完整成员复核（R2 C2）：
//
//	① reLocate != nil 时，在**已持有实体锁的事务内**重新定位当前成员集（current read），
//	  与 snapshot.CandidateKeys 做集合比对 → 新成员 / 更低序实体 / 快照成员缺失 /
//	  空快照发现成员，一律 ErrMembershipDrift；
//	② 比对通过后才逐实体执行 verifier 的身份/活性核验。
//
// 空快照不得短路：reLocate 始终执行，只有确认当前集合同样为空才算无漂移。
//
// 顺序刻意如此：成员集比对先于逐实体核验，否则"旧实体全部健康、集合却多了新成员"
// 的场景会静默通过（R2 C2 反例）。发现漂移只返回错误，绝不追加补锁。
func VerifyCurrentStatesWithReLocate(ctx context.Context, q ReLocateRower, snapshot *LocatorSnapshot, verifier CurrentVerifier, reLocate ReLocator) ([]*EntityState, error) {
	if snapshot == nil {
		return nil, errors.New("lockorder: nil snapshot")
	}
	if verifier == nil {
		return nil, errors.New("lockorder: nil CurrentVerifier")
	}

	if reLocate != nil {
		if q == nil {
			return nil, errors.New("lockorder: nil rower for membership re-location")
		}
		current, err := reLocate(ctx, q)
		if err != nil {
			// 重定位本身失败（SQL 错误等）不是漂移：原样透出，由分类器决定是否重试
			return nil, fmt.Errorf("relocate members under lock: %w", err)
		}
		if err := CompareMembership(snapshot.CandidateKeys, current); err != nil {
			// 红线：只报错，绝不在此处对发现的成员加锁
			return nil, err
		}
	}

	return VerifyCurrentStates(ctx, q, snapshot.CandidateKeys, snapshot, verifier)
}

// LockAndVerifyWithReLocate 是 C2 完整能力的组合入口：严格全序加锁 → 持锁重定位
// 成员集比对 → 逐实体当前态核验。
//
// 参数校验优先级与 LockAndVerify / LockEntitiesInOrder 完全一致（R11 I-R11-1）：
// ① snapshot == nil 立即报错；② 锁序校验（ErrOutOfOrderLock）先于 nil tx（ErrNilTx）。
//
// reLocate 为 nil 时退化为 LockAndVerify 的旧语义（仅身份/活性核验，无法发现新成员），
// 生产路径应始终提供 reLocate；其契约见 ReLocator 文档（发现即报错，禁止补锁）。
func LockAndVerifyWithReLocate(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier, reLocate ReLocator) ([]*EntityState, error) {
	if snapshot == nil {
		return nil, errors.New("lockorder: nil snapshot")
	}

	// 1. 严格全序加锁（锁序校验优先于 nil tx 校验，见 LockEntitiesInOrder 文档）
	if err := LockEntitiesInOrder(ctx, tx, lockCtx, snapshot.CandidateKeys); err != nil {
		return nil, fmt.Errorf("lock and verify: %w", err)
	}

	// 2. 持锁下先做成员集合复核，再做逐实体当前态核验
	states, err := VerifyCurrentStatesWithReLocate(ctx, tx, snapshot, verifier, reLocate)
	if err != nil {
		// 核验失败直接透出 ErrMembershipDrift 或内部错误：调用层必须整体回滚，严禁追加补锁
		return nil, err
	}
	return states, nil
}

// LockAndVerify 将排序加锁与当前态核验原子组合（向后兼容入口）。
//
// 等价于 LockAndVerifyWithReLocate(..., nil)：**只覆盖已锁定成员的身份/活性**，
// 无法发现新成员、更低物理 PK 或空快照变非空（R2 C2）。需要完整成员复核的调用方
// 请直接使用 LockAndVerifyWithReLocate 并提供 ReLocator。
//
// tx 为 nil 时返回 ErrNilTx（R2 I9）：加锁与核验必须在真实外层事务内发生。
//
// 参数校验优先级与 LockEntitiesInOrder 完全一致（R11 I-R11-1）：
//
//	① snapshot == nil → 立即报错（该参数是本函数独有的）
//	② 其余校验全部委托 LockEntitiesInOrder：先锁序校验（ErrOutOfOrderLock），
//	  后 nil tx 校验（ErrNilTx）
//
// 因此本入口**不再重复** nil tx 守卫：组合入口若在排序校验之前判 nil tx，会让
// "乱序候选 + nil tx"从 ErrOutOfOrderLock 变成 ErrNilTx，与底层入口分类不一致，
// 也会掩盖调用方真正的锁序缺陷。委托错误以 "lock and verify: %w" 包装透出，
// errors.Is 判定不变，且错误信息保留失败步骤（LockEntitiesInOrder）的可诊断性。
func LockAndVerify(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	return LockAndVerifyWithReLocate(ctx, tx, lockCtx, snapshot, verifier, nil)
}
