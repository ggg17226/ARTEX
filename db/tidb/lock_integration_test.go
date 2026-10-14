package tidb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/tidb"
)

func TestAcquireReleaseMaintenance_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := tidb.EnsureLockInfrastructure(ctx, db); err != nil {
		t.Fatalf("EnsureLockInfrastructure: %v", err)
	}
	const opIDA = "0123456789abcdef0123456789abcdef"
	if err := tidb.AcquireMaintenance(ctx, db, opIDA); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// 竞争方在 300ms 截止内必须失败（A 持锁）
	ctxB, cancelB := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelB()
	err = tidb.AcquireMaintenance(ctxB, db, "fedcba9876543210fedcba9876543210")
	if err == nil {
		t.Fatal("second acquire should fail while A holds the lock")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire err = %v, want ctx deadline", err)
	}
	// 错误 opID 不能释放 A 的锁
	released, err := tidb.ReleaseMaintenance(ctx, db, "a1b2c3d4e5f60718a1b2c3d4e5f60718")
	if err != nil || released {
		t.Fatalf("ReleaseMaintenance(wrong) = (%v, %v), want (false, nil)", released, err)
	}
	// 正确 opID 释放
	released, err = tidb.ReleaseMaintenance(ctx, db, opIDA)
	if err != nil || !released {
		t.Fatalf("ReleaseMaintenance(A) = (%v, %v), want (true, nil)", released, err)
	}
	// 释放后可再获取
	const opIDC = "99999999999999999999999999999999"
	if err := tidb.AcquireMaintenance(ctx, db, opIDC); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	_, _ = tidb.ReleaseMaintenance(ctx, db, opIDC)
}

func TestCoordinateMaintenance_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := tidb.EnsureLockInfrastructure(ctx, db); err != nil {
		t.Fatalf("EnsureLockInfrastructure: %v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE maintenance SET op_id=NULL WHERE singleton_key='maintenance'"); err != nil {
		t.Fatalf("reset op_id: %v", err)
	}
	// 长 tx 内 coordinate 后，竞争方获取必须被行锁阻塞
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tidb.CoordinateMaintenance(ctx, db, tx); err != nil {
		t.Fatalf("CoordinateMaintenance: %v", err)
	}
	ctxB, cancelB := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelB()
	if err := tidb.AcquireMaintenance(ctxB, db, "b1b2b3b4b5b6b7b8b9b0c1c2c3c4c5c6"); err == nil {
		_ = tx.Rollback()
		t.Fatal("acquire should be blocked by coordinating tx")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// tx 提交后行锁释放，可再获取
	const opIDAft = "c1c2c3c4c5c6c7c8c9c0d1d2d3d4d5d6"
	if err := tidb.AcquireMaintenance(ctx, db, opIDAft); err != nil {
		t.Fatalf("acquire after commit: %v", err)
	}
	_, _ = tidb.ReleaseMaintenance(ctx, db, opIDAft)
}

func TestEnsureLockInfrastructure_NoFKCheck_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := tidb.EnsureLockInfrastructure(ctx, db); err != nil {
		t.Fatalf("EnsureLockInfrastructure: %v", err)
	}
	// 红线断言：P01 自建表不得引入 FK/CHECK
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.table_constraints
		WHERE table_schema = DATABASE()
		  AND table_name IN ('schema_meta','maintenance','locks')
		  AND constraint_type IN ('FOREIGN KEY','CHECK')`).Scan(&n); err != nil {
		t.Fatalf("constraint audit: %v", err)
	}
	if n != 0 {
		t.Fatalf("FK/CHECK count on P01 infra tables = %d, want 0", n)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM maintenance WHERE singleton_key IN ('main','maintenance')`).Scan(&n); err != nil {
		t.Fatalf("singleton audit: %v", err)
	}
	if n != 2 {
		t.Fatalf("maintenance singleton rows = %d, want 2 (main + maintenance)", n)
	}
	// 幂等：再跑一遍不得报错
	if err := tidb.EnsureLockInfrastructure(ctx, db); err != nil {
		t.Fatalf("second EnsureLockInfrastructure: %v", err)
	}
}
