package schema

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// mustTestDSN 读取专用测试 DSN；无 DSN 显式 skip 并注明“未验证”（Global Constraint 8）。
// Task 7 复用此 helper，不得重复定义。
func mustTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ARTEX_TEST_TIDB_DSN")
	if dsn == "" {
		t.Skip("skipped: no real TiDB 8.5 LTS cluster (ARTEX_TEST_TIDB_DSN unset), marked unverified / 未验证")
	}
	return dsn
}

// sqlOpen 使用 mysql driver 打开连接（P01 已引入的依赖）。
// Task 7 复用此 helper，不得重复定义。
func sqlOpen(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

func TestSmokeSkipsWithoutDSN(t *testing.T) {
	t.Setenv("ARTEX_TEST_TIDB_DSN", "")
	var childSkipped bool
	var reachedSentinel bool

	t.Run("child_smoke_skip", func(ct *testing.T) {
		ct.Cleanup(func() {
			if ct.Skipped() {
				childSkipped = true
			}
		})
		_ = mustTestDSN(ct)
		reachedSentinel = true
	})

	if !childSkipped {
		t.Fatal("expected child test to be skipped when ARTEX_TEST_TIDB_DSN is unset")
	}
	if reachedSentinel {
		t.Fatal("unreachable sentinel was reached after mustTestDSN; t.Skip did not stop execution")
	}
}

func TestSmokeRealTiDBDDL(t *testing.T) {
	dsn := mustTestDSN(t)
	db, err := sqlOpen(dsn)
	if err != nil {
		t.Fatalf("sqlOpen: %v", err)
	}

	h := newTiDBTestHarness(db)
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), defaultCleanupTimeout)
		defer cancel()
		if err := h.Cleanup(cleanCtx); err != nil {
			t.Errorf("cleanup failed: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("db close failed: %v", err)
		}
	})

	ctx := context.Background()

	// 1. 守卫检查：非 TiDB、空 DB、非空 schema 均在此拒绝
	if err := h.CheckGuards(ctx); err != nil {
		t.Fatalf("CheckGuards failed: %v", err)
	}

	// 2. 首次严格创建（无 IF NOT EXISTS，证明归属）
	if err := h.CreateAllOwnedTables(ctx); err != nil {
		t.Fatalf("CreateAllOwnedTables failed: %v", err)
	}

	// 3. 校验 BASE TABLE 数量为 65
	if err := h.VerifyBaseTableCount(ctx, 65); err != nil {
		t.Fatalf("VerifyBaseTableCount failed: %v", err)
	}

	// 4. 执行基础设施种子数据并验证
	if err := h.ApplyInfraSeed(ctx); err != nil {
		t.Fatalf("ApplyInfraSeed failed: %v", err)
	}
	if err := h.VerifyInfraSeed(ctx); err != nil {
		t.Fatalf("VerifyInfraSeed failed: %v", err)
	}

	// 5. 幂等性：使用原 ApplyDDL (带 IF NOT EXISTS) 再次应用
	stmts, err := GenerateStatements()
	if err != nil {
		t.Fatalf("GenerateStatements failed: %v", err)
	}
	if err := h.ApplyIdempotentDDL(ctx, stmts); err != nil {
		t.Fatalf("ApplyIdempotentDDL (2nd) failed: %v", err)
	}

	// 再次执行种子数据并验证
	if err := h.ApplyInfraSeed(ctx); err != nil {
		t.Fatalf("ApplyInfraSeed (2nd) failed: %v", err)
	}
	if err := h.VerifyInfraSeed(ctx); err != nil {
		t.Fatalf("VerifyInfraSeed (2nd) failed: %v", err)
	}

	// 6. 显式 Cleanup
	cleanCtx, cancel := context.WithTimeout(context.Background(), defaultCleanupTimeout)
	defer cancel()
	if err := h.Cleanup(cleanCtx); err != nil {
		t.Fatalf("explicit Cleanup failed: %v", err)
	}
}
