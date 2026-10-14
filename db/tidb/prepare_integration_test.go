package tidb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
)

func TestExecutePrepare_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := tidb.ExecutePrepare(db); err != nil {
		t.Fatalf("ExecutePrepare: %v", err)
	}
	// 幂等：再跑一遍
	if err := tidb.ExecutePrepare(db); err != nil {
		t.Fatalf("second ExecutePrepare: %v", err)
	}
	ctx := context.Background()
	var version int
	if err := db.QueryRowContext(ctx, "SELECT value FROM schema_meta WHERE `key`='schema_version'").Scan(&version); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if version != 0 {
		t.Fatalf("schema_version = %d, want 0（prepare 只置 0，finalize 才 0→1）", version)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM locks WHERE name='business_relations'").Scan(&n); err != nil {
		t.Fatalf("read locks row: %v", err)
	}
	if n != 1 {
		t.Fatalf("locks.business_relations rows = %d, want 1", n)
	}
	// 红线断言：三张基础设施表 FK/CHECK = 0
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.table_constraints
		WHERE table_schema = DATABASE()
		  AND table_name IN ('schema_meta','maintenance','locks')
		  AND constraint_type IN ('FOREIGN KEY','CHECK')`).Scan(&n); err != nil {
		t.Fatalf("constraint audit: %v", err)
	}
	if n != 0 {
		t.Fatalf("FK/CHECK count = %d, want 0", n)
	}
}

func TestValidatePrepareState_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := tidb.ExecutePrepare(db); err != nil {
		t.Fatalf("ExecutePrepare: %v", err)
	}
	// prepare 状态校验只依赖库内状态，与 dataRoot 无关（dataRoot 路径校验归 P08 ValidateBinding）
	if err := tidb.ValidatePrepareState(db); err != nil {
		t.Fatalf("ValidatePrepareState(valid): %v", err)
	}
}

func TestValidatePrepareState_VersionMismatch_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := tidb.ExecutePrepare(db); err != nil {
		t.Fatalf("ExecutePrepare: %v", err)
	}
	// 模拟 finalize 后版本：validate 必须拒绝
	if _, err := db.Exec("UPDATE schema_meta SET value=1 WHERE `key`='schema_version'"); err != nil {
		t.Fatalf("bump version: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("UPDATE schema_meta SET value=0 WHERE `key`='schema_version'")
	})
	err = tidb.ValidatePrepareState(db)
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("ValidatePrepareState(v1) = %v, want schema_version mismatch error", err)
	}
}
