package lockorder

// W6b：C2 成员漂移核验 + I5 实体缺失归入漂移/重试 的离线单元测试。
//
// 覆盖 R2 C2 的四类漂移（新成员 / 更低序实体 / 快照成员缺失 / 空快照变非空）、
// 逻辑身份变化，以及"发现漂移绝不反向补锁"红线（P04b Global Constraint 5），
// 并验证 I5：ErrEntityNotFound 触发指数退避并重新运行快照定位。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 测试替身：可记录 SQL 的独立 mock driver（不与 locator_test.go 的全局 mock 共享状态）
// ---------------------------------------------------------------------------

type driftMockQuery struct {
	query string
	args  []driver.Value
}

// driftMockConfig 记录本实例上执行过的每一条语句。
//   - lockRows：单主键 FOR UPDATE 命中的物理 PK；未命中返回空结果集（→ sql.ErrNoRows）
//   - handle 非 nil 时接管全部查询（用于多行 current-read 重定位场景）
type driftMockConfig struct {
	mu       sync.Mutex
	queries  []driftMockQuery
	lockRows map[uint64]struct{}
	handle   func(query string, args []driver.Value) ([][]driver.Value, error)
}

func driftArgPK(v driver.Value) (uint64, bool) {
	switch n := v.(type) {
	case int64:
		if n >= 0 {
			return uint64(n), true
		}
	case uint64:
		return n, true
	}
	return 0, false
}

func (c *driftMockConfig) record(query string, args []driver.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, driftMockQuery{query: query, args: append([]driver.Value(nil), args...)})
}

func (c *driftMockConfig) run(query string, args []driver.Value) ([][]driver.Value, error) {
	c.mu.Lock()
	c.queries = append(c.queries, driftMockQuery{query: query, args: append([]driver.Value(nil), args...)})
	handle := c.handle
	lockRows := c.lockRows
	c.mu.Unlock()

	if handle != nil {
		return handle(query, args)
	}
	if len(args) == 0 {
		return nil, nil
	}
	pk, ok := driftArgPK(args[0])
	if !ok {
		return nil, nil
	}
	if _, hit := lockRows[pk]; hit {
		return [][]driver.Value{{int64(pk)}}, nil
	}
	return nil, nil
}

// lockingPKs 返回本实例上执行过的所有 FOR UPDATE 语句的主键参数（按发生顺序）。
// 用于断言红线：检测到漂移后不得对任何新实体追加 FOR UPDATE。
func (c *driftMockConfig) lockingPKs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pks []uint64
	for _, q := range c.queries {
		if !strings.Contains(q.query, "FOR UPDATE") || len(q.args) == 0 {
			continue
		}
		if pk, ok := driftArgPK(q.args[0]); ok {
			pks = append(pks, pk)
		}
	}
	return pks
}

type driftMockDriver struct{}

var (
	driftMockRegistry sync.Map
	driftMockSeq      uint64
)

func init() {
	sql.Register("lockorder_drift_driver", driftMockDriver{})
}

func (driftMockDriver) Open(dsn string) (driver.Conn, error) {
	v, ok := driftMockRegistry.Load(dsn)
	if !ok {
		return nil, fmt.Errorf("lockorder drift mock: unknown dsn %q", dsn)
	}
	return &driftMockConn{cfg: v.(*driftMockConfig)}, nil
}

type driftMockConn struct{ cfg *driftMockConfig }

func (c *driftMockConn) Prepare(query string) (driver.Stmt, error) {
	return &driftMockStmt{conn: c, query: query}, nil
}
func (c *driftMockConn) Close() error { return nil }
func (c *driftMockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *driftMockConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &driftMockTx{}, nil
}

type driftMockTx struct{}

func (driftMockTx) Commit() error   { return nil }
func (driftMockTx) Rollback() error { return nil }

type driftMockStmt struct {
	conn  *driftMockConn
	query string
}

func (s *driftMockStmt) Close() error  { return nil }
func (s *driftMockStmt) NumInput() int { return -1 }
func (s *driftMockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.cfg.record(s.query, args)
	return driver.RowsAffected(0), nil
}
func (s *driftMockStmt) Query(args []driver.Value) (driver.Rows, error) {
	data, err := s.conn.cfg.run(s.query, args)
	if err != nil {
		return nil, err
	}
	return &driftMockRows{cols: []string{"id"}, data: data}, nil
}

type driftMockRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *driftMockRows) Columns() []string { return r.cols }
func (r *driftMockRows) Close() error      { return nil }
func (r *driftMockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

func getDriftMockDB(t *testing.T, lockRows ...uint64) (*sql.DB, *driftMockConfig) {
	t.Helper()
	cfg := &driftMockConfig{lockRows: make(map[uint64]struct{}, len(lockRows))}
	for _, pk := range lockRows {
		cfg.lockRows[pk] = struct{}{}
	}
	dsn := fmt.Sprintf("drift-%d", atomic.AddUint64(&driftMockSeq, 1))
	driftMockRegistry.Store(dsn, cfg)

	db, err := sql.Open("lockorder_drift_driver", dsn)
	if err != nil {
		t.Fatalf("open drift mock db: %v", err)
	}
	return db, cfg
}

// driftLiveVerifier 返回恒为 Live、身份取自快照的 verifier
func driftLiveVerifier(snapshot *LocatorSnapshot) CurrentVerifier {
	return func(_ context.Context, _ QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{
			Key:            key,
			StableIdentity: snapshot.StableIdentities[key],
			Live:           true,
			Version:        1,
		}, nil
	}
}

// fakeReLocateRower 离线用例占位句柄：只需满足 ReLocateRower 接口，
// 测试内的 ReLocator 不真正发起 SQL（真实对接见 *_ReLocatorUsesRealTxQuery）。
type fakeReLocateRower struct{}

func (fakeReLocateRower) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }
func (fakeReLocateRower) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("fakeReLocateRower: no rows")
}

func mustBeginTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin mock tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func pkEq(got []uint64, want ...uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// C2 ①：CompareMembership 纯集合比对（四类漂移 + 无漂移）
// ---------------------------------------------------------------------------

func TestCompareMembership_IdenticalMembershipPasses(t *testing.T) {
	snapshot := []EntityKey{
		{Table: TableOrderTasks, PK: 10},
		{Table: TableOrderTasks, PK: 20},
		{Table: TableOrderCompanies, PK: 5},
	}
	// 顺序不同 + 含重复项：集合语义下必须判定为无漂移
	current := []EntityKey{
		{Table: TableOrderCompanies, PK: 5},
		{Table: TableOrderTasks, PK: 20},
		{Table: TableOrderTasks, PK: 10},
		{Table: TableOrderTasks, PK: 10},
	}
	if err := CompareMembership(snapshot, current); err != nil {
		t.Fatalf("expected no drift, got %v", err)
	}
}

func TestCompareMembership_BothEmptyPasses(t *testing.T) {
	if err := CompareMembership(nil, nil); err != nil {
		t.Fatalf("empty vs empty must pass, got %v", err)
	}
}

func TestCompareMembership_NewHigherPKIsDrift(t *testing.T) {
	snapshot := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	current := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}

	err := CompareMembership(snapshot, current)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift, got %v", err)
	}
	if !errors.Is(err, ErrNewMemberDiscovered) {
		t.Errorf("expected ErrNewMemberDiscovered cause, got %v", err)
	}
	if errors.Is(err, ErrLowerPKDiscovered) || errors.Is(err, ErrEntityNotFound) {
		t.Errorf("wrong drift category: %v", err)
	}
}

func TestCompareMembership_NewLowerPKIsDrift(t *testing.T) {
	snapshot := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	current := []EntityKey{{Table: TableOrderTasks, PK: 5}, {Table: TableOrderTasks, PK: 10}}

	err := CompareMembership(snapshot, current)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift, got %v", err)
	}
	if !errors.Is(err, ErrLowerPKDiscovered) {
		t.Errorf("expected ErrLowerPKDiscovered cause, got %v", err)
	}
	if !strings.Contains(err.Error(), "backwards") {
		t.Errorf("message must state backwards locking is prohibited, got %v", err)
	}
}

func TestCompareMembership_LowerTableOrderIsDrift(t *testing.T) {
	// table-order 更小的成员同样属于"更低序"：Tasks 排在 Companies 之前
	snapshot := []EntityKey{{Table: TableOrderCompanies, PK: 100}}
	current := []EntityKey{
		{Table: TableOrderTasks, PK: 1},
		{Table: TableOrderCompanies, PK: 100},
	}

	err := CompareMembership(snapshot, current)
	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrLowerPKDiscovered) {
		t.Fatalf("expected lower-order membership drift, got %v", err)
	}
}

func TestCompareMembership_EmptySnapshotDiscoversMembers(t *testing.T) {
	// 空快照不得盲目跳过验证：当前集合出现任何成员都是漂移
	err := CompareMembership(nil, []EntityKey{{Table: TableOrderTasks, PK: 1}})
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift for empty snapshot, got %v", err)
	}
	if !errors.Is(err, ErrNewMemberDiscovered) {
		t.Errorf("expected ErrNewMemberDiscovered cause, got %v", err)
	}
}

func TestCompareMembership_SnapshotMemberMissingIsDrift(t *testing.T) {
	snapshot := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}
	current := []EntityKey{{Table: TableOrderTasks, PK: 20}}

	err := CompareMembership(snapshot, current)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift, got %v", err)
	}
	// I5：实体缺失保留 ErrEntityNotFound 归因
	if !errors.Is(err, ErrEntityNotFound) {
		t.Errorf("expected ErrEntityNotFound cause, got %v", err)
	}
}

func TestCompareMembership_CurrentEmptyWithNonEmptySnapshotIsMissingDrift(t *testing.T) {
	err := CompareMembership([]EntityKey{{Table: TableOrderTasks, PK: 10}}, nil)
	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("expected missing-entity drift, got %v", err)
	}
}

func TestMembershipDriftError_ExposesCategory(t *testing.T) {
	err := CompareMembership(
		[]EntityKey{{Table: TableOrderTasks, PK: 10}},
		[]EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}},
	)
	var de *membershipDriftError
	if !errors.As(err, &de) {
		t.Fatalf("expected *membershipDriftError, got %T", err)
	}
	if !errors.Is(de.Cause(), ErrNewMemberDiscovered) {
		t.Errorf("expected cause ErrNewMemberDiscovered, got %v", de.Cause())
	}
	if strings.Contains(err.Error(), "\n") {
		t.Errorf("drift error message must stay single-line, got %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// C2 ②：VerifyCurrentStates 身份/活性/范围核验
// ---------------------------------------------------------------------------

func TestVerifyCurrentStates_MissingSnapshotIdentityIsDrift(t *testing.T) {
	keys := []EntityKey{
		{Table: TableOrderTasks, PK: 1},
		{Table: TableOrderTasks, PK: 2},
	}
	// 快照登记了稳定身份，却缺第二个实体的条目 → 不得"缺身份放行"
	snapshot := &LocatorSnapshot{
		CandidateKeys: keys,
		StableIdentities: map[EntityKey]string{
			keys[0]: "hash-1",
		},
	}
	verifier := driftLiveVerifier(snapshot)

	if _, err := VerifyCurrentStates(context.Background(), nil, keys, snapshot, verifier); !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift for missing snapshot identity, got %v", err)
	}
}

func TestVerifyCurrentStates_StateKeyMismatchIsDrift(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 1}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	verifier := func(_ context.Context, _ QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{Key: EntityKey{Table: TableOrderTasks, PK: 999}, Live: true}, nil
	}

	if _, err := VerifyCurrentStates(context.Background(), nil, keys, snapshot, verifier); !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift for state key mismatch, got %v", err)
	}
}

func TestVerifyCurrentStates_VerifierNoRowsIsMissingDrift(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 1}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	verifier := func(_ context.Context, _ QueryRower, _ EntityKey) (*EntityState, error) {
		return nil, sql.ErrNoRows
	}

	_, err := VerifyCurrentStates(context.Background(), nil, keys, snapshot, verifier)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift, got %v", err)
	}
	if !errors.Is(err, ErrEntityNotFound) {
		t.Errorf("expected ErrEntityNotFound cause (I5), got %v", err)
	}
}

func TestVerifyCurrentStates_VerifierBusinessErrorStaysNonDrift(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 1}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	sentinel := errors.New("connection reset by peer")
	verifier := func(_ context.Context, _ QueryRower, _ EntityKey) (*EntityState, error) {
		return nil, sentinel
	}

	_, err := VerifyCurrentStates(context.Background(), nil, keys, snapshot, verifier)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the original error to be preserved, got %v", err)
	}
	if errors.Is(err, ErrMembershipDrift) {
		t.Errorf("a plain verifier failure must not be classified as drift, got %v", err)
	}
}

func TestVerifyCurrentStates_ScopeMismatchIsDrift(t *testing.T) {
	snapshot := &LocatorSnapshot{
		CandidateKeys: []EntityKey{
			{Table: TableOrderTasks, PK: 1},
			{Table: TableOrderTasks, PK: 2},
		},
	}
	// 只核验一半成员：核验范围与快照成员集不一致本身就是漂移
	_, err := VerifyCurrentStates(context.Background(), nil, snapshot.CandidateKeys[:1], snapshot, driftLiveVerifier(snapshot))
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected ErrMembershipDrift for verification scope mismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// C2 ③：VerifyCurrentStatesWithReLocate 持锁重定位
// ---------------------------------------------------------------------------

func TestVerifyCurrentStatesWithReLocate_MatchesSnapshotPasses(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}

	relocateCalls := 0
	verifierCalls := 0
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		relocateCalls++
		return append([]EntityKey(nil), keys...), nil
	}
	inner := driftLiveVerifier(snapshot)
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		verifierCalls++
		return inner(ctx, q, key)
	}

	states, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, snapshot, verifier, relocate)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(states) != 2 || relocateCalls != 1 || verifierCalls != 2 {
		t.Errorf("unexpected counts: states=%d relocate=%d verifier=%d", len(states), relocateCalls, verifierCalls)
	}
}

func TestVerifyCurrentStatesWithReLocate_EmptySnapshotStillRelocates(t *testing.T) {
	snapshot := &LocatorSnapshot{}
	relocateCalls := 0
	verifierCalls := 0
	verifier := func(_ context.Context, _ QueryRower, _ EntityKey) (*EntityState, error) {
		verifierCalls++
		t.Fatal("verifier must not run when membership drift is detected")
		return nil, nil
	}

	// 空快照 + 当前发现成员 → 漂移
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		relocateCalls++
		return []EntityKey{{Table: TableOrderTasks, PK: 7}}, nil
	}
	_, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, snapshot, verifier, relocate)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("empty snapshot must still be verified, got %v", err)
	}
	if relocateCalls != 1 || verifierCalls != 0 {
		t.Errorf("expected relocate once + no verifier, got relocate=%d verifier=%d", relocateCalls, verifierCalls)
	}

	// 空快照 + 当前仍为空 → 无漂移
	relocate = func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return nil, nil
	}
	if _, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, snapshot, verifier, relocate); err != nil {
		t.Fatalf("empty snapshot with empty current set must pass, got %v", err)
	}
}

func TestVerifyCurrentStatesWithReLocate_ReLocateFailureIsNotDrift(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	sentinel := errors.New("current read failed")

	_, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, snapshot,
		driftLiveVerifier(snapshot),
		func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) { return nil, sentinel })

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the re-locate failure to propagate, got %v", err)
	}
	if errors.Is(err, ErrMembershipDrift) {
		t.Errorf("a re-locate SQL failure must not be reported as drift, got %v", err)
	}
}

func TestVerifyCurrentStatesWithReLocate_Guards(t *testing.T) {
	keys := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	snapshot := &LocatorSnapshot{CandidateKeys: keys}
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) { return keys, nil }

	if _, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, nil, driftLiveVerifier(snapshot), relocate); err == nil ||
		!strings.Contains(err.Error(), "snapshot") {
		t.Errorf("nil snapshot must be rejected, got %v", err)
	}
	if _, err := VerifyCurrentStatesWithReLocate(context.Background(), fakeReLocateRower{}, snapshot, nil, relocate); err == nil ||
		!strings.Contains(err.Error(), "CurrentVerifier") {
		t.Errorf("nil verifier must be rejected, got %v", err)
	}
	// 需要重定位却没有可用的查询句柄 → 显式报错，绝不静默跳过核验
	if _, err := VerifyCurrentStatesWithReLocate(context.Background(), nil, snapshot, driftLiveVerifier(snapshot), relocate); err == nil ||
		!strings.Contains(err.Error(), "re-location") {
		t.Errorf("nil rower with re-locator must be rejected, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// C2 ④：真实 *sql.Tx 端到端 —— 红线：发现漂移绝不反向补锁
// ---------------------------------------------------------------------------

func TestLockEntitiesInOrder_MissingRowIsDriftAndStops(t *testing.T) {
	// 物理表存在 pk=10 与 pk=30；按全序加到 pk=20 时未命中
	db, cfg := getDriftMockDB(t, 10, 30)
	defer db.Close()
	tx := mustBeginTx(t, db)

	keys := []EntityKey{
		{Table: TableOrderTasks, PK: 10},
		{Table: TableOrderTasks, PK: 20},
		{Table: TableOrderTasks, PK: 30},
	}
	err := LockEntitiesInOrder(context.Background(), tx, NewLockContext(), keys)

	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("expected missing-entity drift (I5), got %v", err)
	}
	// fail fast：pk=20 未命中即整体返回，pk=30 绝不再加锁
	if got := cfg.lockingPKs(); !pkEq(got, 10, 20) {
		t.Errorf("expected locking to stop right at the missing pk=20, got %v", got)
	}
}

func TestLockAndVerifyWithReLocate_LowerPKDriftNeverBacklocks(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10)
	defer db.Close()
	tx := mustBeginTx(t, db)

	snapshot := &LocatorSnapshot{CandidateKeys: []EntityKey{{Table: TableOrderTasks, PK: 10}}}
	verifierCalls := 0
	inner := driftLiveVerifier(snapshot)
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		verifierCalls++
		return inner(ctx, q, key)
	}
	// 持锁后 current read 发现更小的物理 PK
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return []EntityKey{{Table: TableOrderTasks, PK: 5}, {Table: TableOrderTasks, PK: 10}}, nil
	}

	_, err := LockAndVerifyWithReLocate(context.Background(), tx, NewLockContext(), snapshot, verifier, relocate)

	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrLowerPKDiscovered) {
		t.Fatalf("expected lower-PK membership drift, got %v", err)
	}
	// 红线：FOR UPDATE 只对快照成员 pk=10 发出，绝不出现新发现的 pk=5
	if got := cfg.lockingPKs(); !pkEq(got, 10) {
		t.Errorf("RED LINE: no FOR UPDATE allowed for discovered pk=5, got %v", got)
	}
	if verifierCalls != 0 {
		t.Errorf("identity verification must not run after membership drift, calls=%d", verifierCalls)
	}
}

func TestLockAndVerifyWithReLocate_NewMemberDriftNeverBacklocks(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10, 20)
	defer db.Close()
	tx := mustBeginTx(t, db)

	snapshot := &LocatorSnapshot{CandidateKeys: []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}}
	// 新成员 pk=30 在快照之后出现：不得追加补锁
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return []EntityKey{
			{Table: TableOrderTasks, PK: 10},
			{Table: TableOrderTasks, PK: 20},
			{Table: TableOrderTasks, PK: 30},
		}, nil
	}

	_, err := LockAndVerifyWithReLocate(context.Background(), tx, NewLockContext(), snapshot,
		driftLiveVerifier(snapshot), relocate)

	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrNewMemberDiscovered) {
		t.Fatalf("expected new-member drift, got %v", err)
	}
	if got := cfg.lockingPKs(); !pkEq(got, 10, 20) {
		t.Errorf("RED LINE: only snapshot members may be locked, got %v", got)
	}
}

func TestLockAndVerifyWithReLocate_MissingMemberDrift(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10, 20)
	defer db.Close()
	tx := mustBeginTx(t, db)

	snapshot := &LocatorSnapshot{CandidateKeys: []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}}
	// 加锁阶段 pk=20 存在，重定位却读不到 → 实体缺失漂移
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return []EntityKey{{Table: TableOrderTasks, PK: 10}}, nil
	}

	_, err := LockAndVerifyWithReLocate(context.Background(), tx, NewLockContext(), snapshot,
		driftLiveVerifier(snapshot), relocate)

	if !errors.Is(err, ErrMembershipDrift) || !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("expected missing-member drift, got %v", err)
	}
	if got := cfg.lockingPKs(); !pkEq(got, 10, 20) {
		t.Errorf("expected only the snapshot members locked, got %v", got)
	}
}

func TestLockAndVerifyWithReLocate_ReLocatorUsesRealTxQuery(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10)
	defer db.Close()
	tx := mustBeginTx(t, db)

	// 多行 current-read 结果：证明 ReLocateRower.QueryContext 与 *sql.Tx 可直接对接
	cfg.mu.Lock()
	cfg.handle = func(_ string, _ []driver.Value) ([][]driver.Value, error) {
		return [][]driver.Value{{int64(10)}}, nil
	}
	cfg.mu.Unlock()

	snapshot := &LocatorSnapshot{CandidateKeys: []EntityKey{{Table: TableOrderTasks, PK: 10}}}
	relocate := func(ctx context.Context, q ReLocateRower) ([]EntityKey, error) {
		rows, err := q.QueryContext(ctx, "SELECT id FROM tasks WHERE owner = ? FOR UPDATE", 42)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var keys []EntityKey
		for rows.Next() {
			var pk uint64
			if err := rows.Scan(&pk); err != nil {
				return nil, err
			}
			keys = append(keys, EntityKey{Table: TableOrderTasks, PK: pk})
		}
		return keys, rows.Err()
	}

	states, err := LockAndVerifyWithReLocate(context.Background(), tx, NewLockContext(), snapshot,
		driftLiveVerifier(snapshot), relocate)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}
}

func TestLockAndVerifyWithReLocate_SuccessPath(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10, 20)
	defer db.Close()
	tx := mustBeginTx(t, db)

	keys := []EntityKey{{Table: TableOrderTasks, PK: 10}, {Table: TableOrderTasks, PK: 20}}
	snapshot := &LocatorSnapshot{
		CandidateKeys: keys,
		StableIdentities: map[EntityKey]string{
			keys[0]: "hash-a",
			keys[1]: "hash-b",
		},
	}
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return append([]EntityKey(nil), keys...), nil
	}

	lockCtx := NewLockContext()
	states, err := LockAndVerifyWithReLocate(context.Background(), tx, lockCtx, snapshot,
		driftLiveVerifier(snapshot), relocate)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 states, got %d", len(states))
	}
	if got := cfg.lockingPKs(); !pkEq(got, 10, 20) {
		t.Errorf("expected strict ascending locks on 10,20, got %v", got)
	}
}

func TestLockAndVerifyWithReLocate_IdentityChangeIsDrift(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10)
	defer db.Close()
	tx := mustBeginTx(t, db)

	keys := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	snapshot := &LocatorSnapshot{
		CandidateKeys:    keys,
		StableIdentities: map[EntityKey]string{keys[0]: "old-hash"},
	}
	relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
		return append([]EntityKey(nil), keys...), nil
	}
	verifier := func(_ context.Context, _ QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{Key: key, StableIdentity: "new-hash", Live: true}, nil
	}

	_, err := LockAndVerifyWithReLocate(context.Background(), tx, NewLockContext(), snapshot, verifier, relocate)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Fatalf("expected identity-change drift, got %v", err)
	}
	if got := cfg.lockingPKs(); !pkEq(got, 10) {
		t.Errorf("no extra locks expected, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// I5：ErrEntityNotFound 归入可重试冲突 → 指数退避 + 重做快照定位
// ---------------------------------------------------------------------------

func TestIsRetryableConflict_EntityNotFoundAndDriftCauses(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"ErrEntityNotFound", ErrEntityNotFound, true},
		{"wrapped ErrEntityNotFound", fmt.Errorf("payload: %w", ErrEntityNotFound), true},
		{"missing drift", CompareMembership([]EntityKey{{Table: TableOrderTasks, PK: 1}}, nil), true},
		{"new member drift", CompareMembership(nil, []EntityKey{{Table: TableOrderTasks, PK: 1}}), true},
		{"lower pk drift", CompareMembership([]EntityKey{{Table: TableOrderTasks, PK: 10}}, []EntityKey{{Table: TableOrderTasks, PK: 1}}), true},
		{"business error", errors.New("amount must be positive"), false},
	}
	for _, c := range cases {
		if got := IsRetryableConflict(c.err); got != c.want {
			t.Errorf("%s: IsRetryableConflict=%v, want %v (%v)", c.name, got, c.want, c.err)
		}
	}
}

func TestTxRunner_EntityNotFoundRetriesWithBackoff(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleeps []time.Duration
	runner.SetSleeper(func(d time.Duration) { sleeps = append(sleeps, d) })

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{}, func(_ context.Context, _ *sql.Tx, _ *LockContext) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("payload: %w", ErrEntityNotFound)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if len(sleeps) != 2 || sleeps[0] != 100*time.Millisecond || sleeps[1] != 250*time.Millisecond {
		t.Errorf("expected exponential backoff 100ms/250ms, got %v", sleeps)
	}
}

// TestTxRunner_MembershipDriftRerunsLocator 端到端验证 C2 + I5：
// 持锁重定位发现更低序新成员 → ErrMembershipDrift → 整体回滚 → 指数退避 →
// payload（含快照定位）重新运行；全程不得对新成员追加 FOR UPDATE。
func TestTxRunner_MembershipDriftRerunsLocator(t *testing.T) {
	db, cfg := getDriftMockDB(t, 10)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleeps []time.Duration
	runner.SetSleeper(func(d time.Duration) { sleeps = append(sleeps, d) })

	snapshotKeys := []EntityKey{{Table: TableOrderTasks, PK: 10}}
	var locatorCalls, relocateCalls int

	err := runner.Run(context.Background(), TxRunnerOptions{}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		locatorCalls++
		snapshot := &LocatorSnapshot{CandidateKeys: append([]EntityKey(nil), snapshotKeys...)}

		relocateCalls++
		driftFirstAttempt := relocateCalls == 1
		relocate := func(_ context.Context, _ ReLocateRower) ([]EntityKey, error) {
			if driftFirstAttempt {
				// 首次尝试：出现更低物理 PK 的新成员
				return []EntityKey{{Table: TableOrderTasks, PK: 5}, snapshotKeys[0]}, nil
			}
			return append([]EntityKey(nil), snapshotKeys...), nil
		}

		_, lockErr := LockAndVerifyWithReLocate(ctx, tx, lockCtx, snapshot, driftLiveVerifier(snapshot), relocate)
		return lockErr
	})

	if err != nil {
		t.Fatalf("expected eventual success after re-locate, got %v", err)
	}
	if locatorCalls != 2 || relocateCalls != 2 {
		t.Errorf("expected locator+relocate to run once per attempt, got locator=%d relocate=%d", locatorCalls, relocateCalls)
	}
	if len(sleeps) != 1 || sleeps[0] != 100*time.Millisecond {
		t.Errorf("expected one 100ms backoff, got %v", sleeps)
	}
	// 红线：两次尝试各自只对快照成员 pk=10 加锁，从未对漂移出的 pk=5 加锁
	if got := cfg.lockingPKs(); !pkEq(got, 10, 10) {
		t.Errorf("RED LINE: only pk=10 may ever be locked, got %v", got)
	}
}

func TestTxRunner_MissingEntityDriftExhaustsExplicitly(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	runner.SetSleeper(func(time.Duration) {})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{}, func(_ context.Context, _ *sql.Tx, _ *LockContext) error {
		attempts++
		return fmt.Errorf("payload: %w", ErrEntityNotFound)
	})

	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("expected ErrRetryExhausted, got %v", err)
	}
	if attempts != len(DefaultBackoffSchedule)+1 {
		t.Errorf("expected %d attempts, got %d", len(DefaultBackoffSchedule)+1, attempts)
	}
}
