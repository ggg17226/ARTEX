package tidb_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
)

func myErr(code uint16) error {
	return &mysql.MySQLError{Number: code, Message: "x"}
}

func TestClassify_MySQLErrorCodes(t *testing.T) {
	cases := []struct {
		code uint16
		want tidb.Kind
	}{
		{1062, tidb.KindDuplicateKey},
		{1213, tidb.KindDeadlock},
		{1205, tidb.KindLockTimeout},
		{2006, tidb.KindConnectionLost},
		{2013, tidb.KindConnectionLost},
		{1064, tidb.KindUnknown}, // 语法错误不属于任何已知类
		{1452, tidb.KindUnknown}, // 外键违规（本红线禁止出现，出现也不重试）
	}
	for _, tc := range cases {
		if got := tidb.Classify(myErr(tc.code)); got != tc.want {
			t.Errorf("Classify(%d) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestClassify_DriverPreflightError(t *testing.T) {
	base := fmt.Errorf("dial tcp 127.0.0.1:4000: connection refused")
	// v1.9.3 最小偏差：driver 已无 MySQLDriverError 类型，dial 失败实测为 *net.OpError（见 task-4-report.md 顾虑 1）
	wrapped := &net.OpError{Op: "dial", Err: base}
	if got := tidb.Classify(wrapped); got != tidb.KindConnError {
		t.Fatalf("Classify(driver error) = %v, want KindConnError", got)
	}
	if !tidb.IsRetryable(wrapped) {
		t.Fatal("connection-level driver error should be retryable")
	}
}

func TestClassify_DriverBadConnSentinels(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"mysql.ErrInvalidConn", mysql.ErrInvalidConn},
		{"wrapped mysql.ErrInvalidConn", fmt.Errorf("transport failed: %w", mysql.ErrInvalidConn)},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"wrapped driver.ErrBadConn", fmt.Errorf("connection dead: %w", driver.ErrBadConn)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tidb.Classify(tc.err); got != tidb.KindConnError {
				t.Fatalf("Classify(%s) = %v, want KindConnError", tc.name, got)
			}
			if !tidb.IsRetryable(tc.err) {
				t.Fatalf("IsRetryable(%s) = false, want true", tc.name)
			}
		})
	}
}

func TestClassify_Context(t *testing.T) {
	if got := tidb.Classify(context.DeadlineExceeded); got != tidb.KindCancelled {
		t.Fatalf("Classify(DeadlineExceeded) = %v, want KindCancelled", got)
	}
	if got := tidb.Classify(context.Canceled); got != tidb.KindCancelled {
		t.Fatalf("Classify(Canceled) = %v, want KindCancelled", got)
	}
}

func TestClassify_Unknown(t *testing.T) {
	if got := tidb.Classify(errors.New("boom")); got != tidb.KindUnknown {
		t.Fatalf("Classify(generic) = %v, want KindUnknown", got)
	}
}

func TestIsRetryableMatrix(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{myErr(1213), true},  // deadlock
		{myErr(1205), true},  // lock timeout
		{myErr(2006), true},  // server closed
		{myErr(2013), true},  // lost during query
		{myErr(1062), false}, // duplicate key 不重试（语义错误）
		{context.DeadlineExceeded, false},
		{errors.New("x"), false},
	}
	for _, tc := range cases {
		if got := tidb.IsRetryable(tc.err); got != tc.want {
			t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
