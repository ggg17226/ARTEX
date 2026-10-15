package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

// 模拟 MySQL 驱动错误
type mockMySQLError struct {
	Number  uint16
	Message string
}

func (e *mockMySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Number, e.Message)
}

func TestIsRetryableConflict(t *testing.T) {
	cases := []struct {
		err      error
		expected bool
	}{
		{&mockMySQLError{Number: 1213, Message: "Deadlock found"}, true},
		{&mockMySQLError{Number: 1205, Message: "Lock wait timeout"}, true},
		{&mockMySQLError{Number: 1062, Message: "Duplicate entry"}, true},
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
		return &mockMySQLError{Number: 1213, Message: "Deadlock"}
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
