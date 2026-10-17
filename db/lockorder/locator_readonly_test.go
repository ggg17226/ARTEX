package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// ---------------------------------------------------------------------------
// 真实 TiDB Error 1235（READ ONLY has only noop implementation）适配
//
// 背景（w6d-report.md 第二节）：目标 TiDB 默认 tidb_enable_noop_functions=OFF，
// BeginTx(&sql.TxOptions{ReadOnly:true}) 直接报
//
//	Error 1235 (42000): function READ ONLY has only noop implementation in tidb now
//
// 降级前的实现让 fresh locator 在真实库上 100% 失败。以下用例在**离线 mock** 上
// 复现该服务端行为（mock 必须能观察 TxOptions，见 locator_test.go 的 beginTxFn 钩子），
// 证明：命中 1235 → 安全降级为普通事务并正常返回快照；其他错误 → 原样上抛、绝不降级。
// ---------------------------------------------------------------------------

// tidbReadOnlyNoopErr 真实 TiDB v8.5.8 的原始错误文本。
func tidbReadOnlyNoopErr() error {
	return &mysql.MySQLError{
		Number:  1235,
		Message: "function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions",
	}
}

// clearReadOnlyCapabilityCache 清空按 *sql.DB 缓存的 READ ONLY 能力探测结果。
func clearReadOnlyCapabilityCache() {
	locatorReadOnlyUnsupported.Range(func(k, _ any) bool {
		locatorReadOnlyUnsupported.Delete(k)
		return true
	})
}

// setBeginHook 安装驱动侧 BeginTx 钩子，并在用例结束时清理。
func setBeginHook(t *testing.T, fn func(opts driver.TxOptions) error) {
	t.Helper()
	globalMockDriver.mu.Lock()
	globalMockDriver.beginTxFn = fn
	globalMockDriver.beginAttempts = nil
	globalMockDriver.mu.Unlock()

	clearReadOnlyCapabilityCache()
	t.Cleanup(func() {
		globalMockDriver.resetBeginHook()
		clearReadOnlyCapabilityCache()
	})
}

func simpleLocatorSnapshot(_ context.Context, _ *sql.Tx) (*LocatorSnapshot, error) {
	return &LocatorSnapshot{
		CandidateKeys: []EntityKey{{Table: TableOrderTasks, PK: 2}, {Table: TableOrderTasks, PK: 1}},
	}, nil
}

// TestExecuteFreshLocator_TiDBReadOnlyNoopRejectionFallsBack 核心回归：
// 服务端拒绝 READ ONLY（1235）时，fresh locator 必须自动降级为普通事务并成功返回快照，
// 且降级只发生一次往返（能力缓存后不再重复吃 1235）。
func TestExecuteFreshLocator_TiDBReadOnlyNoopRejectionFallsBack(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	var readOnlyAttempts int
	setBeginHook(t, func(opts driver.TxOptions) error {
		if opts.ReadOnly {
			readOnlyAttempts++
			return tidbReadOnlyNoopErr()
		}
		return nil
	})

	ctx := context.Background()
	snapshot, err := ExecuteFreshLocatorWithAdmission(ctx, db, simpleLocatorSnapshot, NewAdmissionControl(1, 0))
	if err != nil {
		t.Fatalf("READ ONLY 被拒绝时必须降级为普通事务而不是失败，got %v", err)
	}
	if len(snapshot.CandidateKeys) != 2 {
		t.Fatalf("降级后快照内容不符：%+v", snapshot.CandidateKeys)
	}
	// 排序去重仍然生效（降级不得绕过 ExecuteFreshLocator 的后处理）
	if snapshot.CandidateKeys[0].PK != 1 || snapshot.CandidateKeys[1].PK != 2 {
		t.Errorf("expected sorted candidate keys [1 2], got %+v", snapshot.CandidateKeys)
	}

	attempts := globalMockDriver.recordedBeginAttempts()
	if len(attempts) != 2 {
		t.Fatalf("expected 2 driver BeginTx (READ ONLY 尝试 + 普通事务降级), got %d: %+v", len(attempts), attempts)
	}
	if !attempts[0].ReadOnly {
		t.Errorf("first attempt must still declare READ ONLY (capable servers keep the extra guard), got %+v", attempts[0])
	}
	if attempts[1].ReadOnly {
		t.Errorf("fallback attempt must be a plain tx, got %+v", attempts[1])
	}
	if readOnlyAttempts != 1 {
		t.Errorf("expected exactly 1 rejected READ ONLY attempt, got %d", readOnlyAttempts)
	}

	// 能力缓存：同一 *sql.DB 的第二次定位不得再吃一次 1235 往返
	if _, err := ExecuteFreshLocatorWithAdmission(ctx, db, simpleLocatorSnapshot, NewAdmissionControl(1, 0)); err != nil {
		t.Fatalf("second locator call: %v", err)
	}
	if readOnlyAttempts != 1 {
		t.Errorf("capability cache missed: READ ONLY retried %d times after known-unsupported", readOnlyAttempts)
	}
	if got := len(globalMockDriver.recordedBeginAttempts()); got != 3 {
		t.Fatalf("expected 3 total BeginTx (1 rejected + 2 plain), got %d", got)
	}
}

// TestExecuteFreshLocator_MessageOnlyNoopRejectionFallsBack 无结构化编号的包装错误
// （代理层/中间件改写错误类型时）也必须按消息特征降级。
func TestExecuteFreshLocator_MessageOnlyNoopRejectionFallsBack(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	setBeginHook(t, func(opts driver.TxOptions) error {
		if opts.ReadOnly {
			return fmt.Errorf("driver: begin failed: %w",
				errors.New("Error 1235 (42000): function READ ONLY has only noop implementation in tidb now"))
		}
		return nil
	})

	if _, err := ExecuteFreshLocatorWithAdmission(context.Background(), db, simpleLocatorSnapshot, NewAdmissionControl(1, 0)); err != nil {
		t.Fatalf("消息特征命中时必须降级，got %v", err)
	}
}

// TestExecuteFreshLocator_OtherBeginErrorsPropagate 反证：非 1235 的 BeginTx 错误
// 一律原样上抛，绝不"再试一次普通事务"（那会掩盖真实故障并多打一次往返）。
func TestExecuteFreshLocator_OtherBeginErrorsPropagate(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	beginErr := &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax near 1235"}
	setBeginHook(t, func(opts driver.TxOptions) error { return beginErr })

	_, err := ExecuteFreshLocatorWithAdmission(context.Background(), db, simpleLocatorSnapshot, NewAdmissionControl(1, 0))
	if err == nil {
		t.Fatal("expected BeginTx error to propagate")
	}
	if !errors.Is(err, beginErr) {
		t.Errorf("expected wrapped 1064 error, got %v", err)
	}
	if !strings.Contains(err.Error(), "fresh locator begin tx") {
		t.Errorf("expected the original wrapping message, got %q", err.Error())
	}
	if got := globalMockDriver.recordedBeginAttempts(); len(got) != 1 {
		t.Fatalf("must NOT retry with a plain tx on non-1235 errors, got %d BeginTx: %+v", len(got), got)
	}
}

// TestExecuteFreshLocator_ReadOnlyAcceptedWhenSupported 正向路径：支持 READ ONLY 的
// 服务端（MySQL 8 等）必须只开一次事务，且事务声明保持 READ ONLY（不因本次修复退化）。
func TestExecuteFreshLocator_ReadOnlyAcceptedWhenSupported(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	setBeginHook(t, func(opts driver.TxOptions) error { return nil })

	if _, err := ExecuteFreshLocatorWithAdmission(context.Background(), db, simpleLocatorSnapshot, NewAdmissionControl(1, 0)); err != nil {
		t.Fatalf("locator: %v", err)
	}
	got := globalMockDriver.recordedBeginAttempts()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 BeginTx, got %d: %+v", len(got), got)
	}
	if !got[0].ReadOnly {
		t.Errorf("READ ONLY must still be requested when the server accepts it, got %+v", got[0])
	}
}

// TestIsReadOnlyUnsupportedErr 判定边界：只认"READ ONLY 事务声明被拒"这一件事。
func TestIsReadOnlyUnsupportedErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"tidb 1235", tidbReadOnlyNoopErr(), true},
		{"wrapped 1235", fmt.Errorf("begin tx: %w", tidbReadOnlyNoopErr()), true},
		{"message only", errors.New("function READ ONLY has only noop implementation in tidb now"), true},
		{"wrapped message only", fmt.Errorf("driver: %w", errors.New("Function READ ONLY has only noop implementation in tidb now")), true},
		{"1213 deadlock", &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}, false},
		{"1064 mentions read only", &mysql.MySQLError{Number: 1064, Message: "syntax error near READ ONLY"}, false},
		{"text mentions 1235 only", errors.New("task 1235 violates policy"), false},
		{"plain begin failure", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		if got := isReadOnlyUnsupportedErr(tc.err); got != tc.want {
			t.Errorf("%s: isReadOnlyUnsupportedErr(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
