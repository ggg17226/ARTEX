package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// 模拟 MySQL 驱动错误
type mockMySQLError struct {
	ErrNumber uint16
	Message   string
}

func (e *mockMySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.ErrNumber, e.Message)
}

func (e *mockMySQLError) Number() uint16 {
	return e.ErrNumber
}

func TestIsRetryableConflict(t *testing.T) {
	cases := []struct {
		err      error
		expected bool
	}{
		{&mockMySQLError{ErrNumber: 1213, Message: "Deadlock found"}, true},
		{&mockMySQLError{ErrNumber: 1205, Message: "Lock wait timeout"}, true},
		{&mockMySQLError{ErrNumber: 1062, Message: "Duplicate entry"}, true},
		{ErrMembershipDrift, true},
		{ErrLowerPKDiscovered, true},
		{ErrNewMemberDiscovered, true},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
		{errors.New("some business validation error"), false},
	}

	for _, c := range cases {
		if got := IsRetryableConflict(c.err); got != c.expected {
			t.Errorf("IsRetryableConflict(%v): expected %v, got %v", c.err, c.expected, got)
		}
	}
}

func TestTxRunner_SuccessFirstAttempt(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"ns-1"},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if len(sleepCalls) != 0 {
		t.Errorf("expected 0 backoff sleeps, got %d", len(sleepCalls))
	}
}

func TestTxRunner_RetryOnMembershipDrift(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		if attempts < 3 {
			// 前两次报告成员漂移
			return ErrMembershipDrift
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected final success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if len(sleepCalls) != 2 {
		t.Fatalf("expected 2 sleeps, got %d", len(sleepCalls))
	}
	if sleepCalls[0] != 100*time.Millisecond || sleepCalls[1] != 250*time.Millisecond {
		t.Errorf("unexpected sleep schedule: %v", sleepCalls)
	}
}

func TestTxRunner_RetryExhausted(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return &mockMySQLError{ErrNumber: 1213, Message: "Deadlock"}
	})

	if !errors.Is(err, ErrRetryExhausted) {
		t.Errorf("expected ErrRetryExhausted, got %v", err)
	}
	// 4 档退避 (100, 250, 500, 1000ms)，共 5 次尝试
	if attempts != 5 {
		t.Errorf("expected 5 attempts, got %d", attempts)
	}
	if len(sleepCalls) != 4 {
		t.Fatalf("expected 4 sleeps, got %d", len(sleepCalls))
	}
	expectedSleeps := []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1000 * time.Millisecond,
	}
	for i, s := range sleepCalls {
		if s != expectedSleeps[i] {
			t.Errorf("sleep %d: expected %v, got %v", i, expectedSleeps[i], s)
		}
	}
}

func TestTxRunner_NonRetryableFailsImmediately(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	expectedErr := errors.New("business rule broken")
	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if len(sleepCalls) != 0 {
		t.Errorf("expected 0 sleeps, got %d", len(sleepCalls))
	}
}

func TestMemoryGateManager_ReverseRelease(t *testing.T) {
	gm := NewMemoryGateManager()
	ctx := context.Background()

	if err := gm.AcquireLocalBusiness(ctx); err != nil {
		t.Fatalf("acquire business gate failed: %v", err)
	}
	if err := gm.AcquireLocalNamespace(ctx, "ns-a"); err != nil {
		t.Fatalf("acquire ns-a failed: %v", err)
	}
	if err := gm.AcquireLocalNamespace(ctx, "ns-b"); err != nil {
		t.Fatalf("acquire ns-b failed: %v", err)
	}

	// 逆序释放
	gm.ReleaseLocalNamespace("ns-b")
	gm.ReleaseLocalNamespace("ns-a")
	gm.ReleaseLocalBusiness()

	// 重新获取应成功无阻塞
	if err := gm.AcquireLocalBusiness(ctx); err != nil {
		t.Fatalf("re-acquire business gate failed: %v", err)
	}
	gm.ReleaseLocalBusiness()
}

func TestTxRunner_DBGates_InsertIgnoreAndForUpdate(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	var mu sync.Mutex
	var execStatements []string
	var queryStatements []string

	globalMockDriver.mu.Lock()
	globalMockDriver.execFn = func(query string, args []driver.Value) (driver.Result, error) {
		mu.Lock()
		execStatements = append(execStatements, query)
		mu.Unlock()
		return driver.RowsAffected(1), nil
	}
	globalMockDriver.queryFn = func(query string, args []driver.Value) (driver.Rows, error) {
		mu.Lock()
		queryStatements = append(queryStatements, query)
		mu.Unlock()
		return &mockRows{nameVal: "locked"}, nil
	}
	globalMockDriver.commitFn = nil
	globalMockDriver.mu.Unlock()

	defer func() {
		globalMockDriver.mu.Lock()
		globalMockDriver.execFn = nil
		globalMockDriver.queryFn = nil
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()
	}()

	runner := NewTxRunner(db)
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"ns-test"},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// 验证 business_relations 与 ns-test 均执行了 INSERT IGNORE 与 SELECT FOR UPDATE
	if len(execStatements) != 2 {
		t.Fatalf("expected 2 exec statements, got %d: %v", len(execStatements), execStatements)
	}
	if !strings.Contains(execStatements[0], "INSERT IGNORE INTO locks (name) VALUES ('business_relations')") {
		t.Errorf("unexpected exec[0]: %s", execStatements[0])
	}
	if !strings.Contains(execStatements[1], "INSERT IGNORE INTO locks (name) VALUES (?)") {
		t.Errorf("unexpected exec[1]: %s", execStatements[1])
	}

	if len(queryStatements) != 2 {
		t.Fatalf("expected 2 query statements, got %d: %v", len(queryStatements), queryStatements)
	}
	if !strings.Contains(queryStatements[0], "SELECT name FROM locks WHERE name = 'business_relations' FOR UPDATE") {
		t.Errorf("unexpected query[0]: %s", queryStatements[0])
	}
	if !strings.Contains(queryStatements[1], "SELECT name FROM locks WHERE name = ? FOR UPDATE") {
		t.Errorf("unexpected query[1]: %s", queryStatements[1])
	}
}

func TestTxRunner_DBGates_RowNotFoundFails(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	// 模拟 Scan 遇到 sql.ErrNoRows（mockRows 直接返回 io.EOF，没有扫描到任何行）
	emptyRows := func() (driver.Rows, error) {
		return &mockRows{read: true}, nil
	}

	// 1. 测试 business_relations 行缺失拒绝
	t.Run("business_relations_missing", func(t *testing.T) {
		globalMockDriver.mu.Lock()
		globalMockDriver.execFn = func(query string, args []driver.Value) (driver.Result, error) {
			return driver.RowsAffected(0), nil
		}
		globalMockDriver.queryFn = func(query string, args []driver.Value) (driver.Rows, error) {
			return emptyRows()
		}
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()

		defer func() {
			globalMockDriver.mu.Lock()
			globalMockDriver.execFn = nil
			globalMockDriver.queryFn = nil
			globalMockDriver.commitFn = nil
			globalMockDriver.mu.Unlock()
		}()

		runner := NewTxRunner(db)
		err := runner.Run(context.Background(), TxRunnerOptions{
			RequiresBusinessGate: true,
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
			return nil
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		expectedErrMsg := "lock db business_relations: row not found under lock"
		if !strings.Contains(err.Error(), expectedErrMsg) {
			t.Errorf("expected error containing %q, got %q", expectedErrMsg, err.Error())
		}
	})

	// 2. 测试 namespace 行缺失拒绝
	t.Run("namespace_missing", func(t *testing.T) {
		globalMockDriver.mu.Lock()
		globalMockDriver.execFn = func(query string, args []driver.Value) (driver.Result, error) {
			return driver.RowsAffected(0), nil
		}
		globalMockDriver.queryFn = func(query string, args []driver.Value) (driver.Rows, error) {
			if strings.Contains(query, "business_relations") {
				return &mockRows{nameVal: "business_relations"}, nil
			}
			return emptyRows()
		}
		globalMockDriver.commitFn = nil
		globalMockDriver.mu.Unlock()

		defer func() {
			globalMockDriver.mu.Lock()
			globalMockDriver.execFn = nil
			globalMockDriver.queryFn = nil
			globalMockDriver.commitFn = nil
			globalMockDriver.mu.Unlock()
		}()

		runner := NewTxRunner(db)
		err := runner.Run(context.Background(), TxRunnerOptions{
			RequiresBusinessGate: true,
			RequiredNamespaces:   []string{"ns-missing"},
		}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
			return nil
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		expectedErrMsg := "lock db namespace ns-missing: row not found under lock"
		if !strings.Contains(err.Error(), expectedErrMsg) {
			t.Errorf("expected error containing %q, got %q", expectedErrMsg, err.Error())
		}
	})
}

// countingGateManager 记录 local gate 获取和释放调用次数的 GateManager
type countingGateManager struct {
	mu                sync.Mutex
	acquireBusiness   int
	releaseBusiness   int
	acquireNamespaces map[string]int
	releaseNamespaces map[string]int
}

func newCountingGateManager() *countingGateManager {
	return &countingGateManager{
		acquireNamespaces: make(map[string]int),
		releaseNamespaces: make(map[string]int),
	}
}

func (c *countingGateManager) AcquireLocalBusiness(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquireBusiness++
	return nil
}

func (c *countingGateManager) ReleaseLocalBusiness() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseBusiness++
}

func (c *countingGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquireNamespaces[ns]++
	return nil
}

func (c *countingGateManager) ReleaseLocalNamespace(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseNamespaces[ns]++
}

func TestTxRunner_CommitOutcomeUnknown_HoldsLocalGates(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	unknownErrCases := []struct {
		name string
		err  error
	}{
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"mysql.ErrInvalidConn", mysql.ErrInvalidConn},
		{"mysql 2006 server gone", &mockMySQLError{ErrNumber: 2006, Message: "MySQL server has gone away"}},
		{"mysql 2013 connection lost", &mockMySQLError{ErrNumber: 2013, Message: "Lost connection to MySQL server during query"}},
		{"broken pipe message", errors.New("write: broken pipe")},
		{"connection reset by peer", errors.New("read: connection reset by peer")},
		{"connection lost message", errors.New("connection lost during commit")},
	}

	for _, tc := range unknownErrCases {
		t.Run(tc.name, func(t *testing.T) {
			cgm := newCountingGateManager()

			globalMockDriver.mu.Lock()
			globalMockDriver.execFn = func(query string, args []driver.Value) (driver.Result, error) {
				return driver.RowsAffected(1), nil
			}
			globalMockDriver.queryFn = func(query string, args []driver.Value) (driver.Rows, error) {
				return &mockRows{nameVal: "locked"}, nil
			}
			globalMockDriver.commitFn = func() error {
				return tc.err
			}
			globalMockDriver.mu.Unlock()

			defer func() {
				globalMockDriver.mu.Lock()
				globalMockDriver.execFn = nil
				globalMockDriver.queryFn = nil
				globalMockDriver.commitFn = nil
				globalMockDriver.mu.Unlock()
			}()

			runner := NewTxRunner(db)
			err := runner.Run(context.Background(), TxRunnerOptions{
				RequiresBusinessGate: true,
				RequiredNamespaces:   []string{"ns-guard"},
				GateManager:          cgm,
			}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
				return nil
			})

			if err == nil {
				t.Fatalf("expected ErrOutcomeUnknown error, got nil")
			}
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("expected errors.Is(err, ErrOutcomeUnknown), got %v", err)
			}

			// 断言 local gates 没有被释放（保持锁定）
			cgm.mu.Lock()
			defer cgm.mu.Unlock()

			if cgm.acquireBusiness != 1 {
				t.Errorf("expected 1 acquireBusiness, got %d", cgm.acquireBusiness)
			}
			if cgm.releaseBusiness != 0 {
				t.Errorf("expected 0 releaseBusiness (gates held), got %d", cgm.releaseBusiness)
			}
			if cgm.acquireNamespaces["ns-guard"] != 1 {
				t.Errorf("expected 1 acquire ns-guard, got %d", cgm.acquireNamespaces["ns-guard"])
			}
			if cgm.releaseNamespaces["ns-guard"] != 0 {
				t.Errorf("expected 0 release ns-guard (gates held), got %d", cgm.releaseNamespaces["ns-guard"])
			}
		})
	}
}
