package tidb_test

import (
	"context"
	"database/sql"
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

// TestAcquireMaintenance_InvalidOpID 纯单测（不依赖 DSN）：opID 校验必须在
// 任何 DB 查询之前 fail closed。DSN 故意指向不可达端口且密码错误——若校验
// 漏过（走到查询层），返回的是 dial/auth 错误而非 ErrInvalidOpID，断言自然
// 失败；合法 opID 的对照组则要求「不是」ErrInvalidOpID（即校验闸门已通过）。
func TestAcquireMaintenance_InvalidOpID(t *testing.T) {
	db, err := sql.Open(tidb.DriverName(), "artex:wrongpass@tcp(127.0.0.1:4000)/artex")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	invalid := []string{
		"zz123456789abcdef0123456789abcdef",    // z 非 hex
		"0123456789abcdef0123456789abcdeg",     // g 非 hex
		"0123456789abcdef0123456789abcde!",     // ! 非 hex
		"0123456789abcdef",                     // 16 位，长度不符
		"0123456789abcdef0123456789abcdef0",    // 33 位，长度不符
		"",                                     // 空串
		"01234567-89ab-cdef-0123-456789abcdeg", // 带连字符 + g 非 hex
	}
	for _, opID := range invalid {
		if err := tidb.AcquireMaintenance(ctx, db, opID); !errors.Is(err, tidb.ErrInvalidOpID) {
			t.Errorf("AcquireMaintenance(%q) err = %v, want ErrInvalidOpID", opID, err)
		}
		if _, err := tidb.ReleaseMaintenance(ctx, db, opID); !errors.Is(err, tidb.ErrInvalidOpID) {
			t.Errorf("ReleaseMaintenance(%q) err = %v, want ErrInvalidOpID", opID, err)
		}
	}

	// 对照组：大写 hex 与带连字符 36 位必须通过校验（到达查询层 →
	// dial/auth 错误，而非 ErrInvalidOpID）。
	valid := []string{
		"0123456789ABCDEF0123456789ABCDEF",
		"0123-4567-89ab-cdef-0123456789abcdef",
	}
	ctxShort, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for _, opID := range valid {
		if err := tidb.AcquireMaintenance(ctxShort, db, opID); errors.Is(err, tidb.ErrInvalidOpID) {
			t.Errorf("AcquireMaintenance(%q) err = %v, should not be ErrInvalidOpID", opID, err)
		}
	}
}

// TestMaintenanceRowMissing_FailClosed_Real：maintenance 单例行被删除后，
// 三个入口都必须返回 ErrMaintenanceRowMissing（fail closed），且
// AcquireMaintenance 须立即返回而不是陷入退避空转。
func TestMaintenanceRowMissing_FailClosed_Real(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, `
DELETE FROM maintenance WHERE singleton_key = 'maintenance'`); err != nil {
		t.Fatalf("delete singleton row: %v", err)
	}
	// 还原单例行（t.Cleanup 按 LIFO：先于上面的 db.Close 执行）
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `
INSERT INTO maintenance (singleton_key, op_id, mode, revision, updated_at)
VALUES ('maintenance', NULL, 0, 0, NOW(3))`)
	})
	const opID = "0123456789abcdef0123456789abcdef"
	ctxTO, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Acquire：fail closed，立即返回，不得退避空转
	start := time.Now()
	err = tidb.AcquireMaintenance(ctxTO, db, opID)
	if !errors.Is(err, tidb.ErrMaintenanceRowMissing) {
		t.Fatalf("AcquireMaintenance (row missing) err = %v, want ErrMaintenanceRowMissing", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("AcquireMaintenance fail-closed took %v, want < 2s (no backoff spin)", elapsed)
	}

	// Release：(false, ErrMaintenanceRowMissing)
	released, err := tidb.ReleaseMaintenance(ctxTO, db, opID)
	if released || !errors.Is(err, tidb.ErrMaintenanceRowMissing) {
		t.Fatalf("ReleaseMaintenance (row missing) = (%v, %v), want (false, ErrMaintenanceRowMissing)", released, err)
	}

	// Coordinate：ErrMaintenanceRowMissing
	tx, err := db.BeginTx(ctxTO, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tidb.CoordinateMaintenance(ctxTO, db, tx); !errors.Is(err, tidb.ErrMaintenanceRowMissing) {
		t.Fatalf("CoordinateMaintenance (row missing) err = %v, want ErrMaintenanceRowMissing", err)
	}
	_ = tx.Rollback()
}

// TestCoordinateMaintenance_LockedByOpID_Real：互斥完整性——A 经
// AcquireMaintenance 持锁（autocommit 已释放行锁，但 op_id 仍非空）后，
// B 事务的 CoordinateMaintenance 必须返回 ErrMaintenanceLocked，
// 不得静默通过；A 释放后 B 可正常协调。
func TestCoordinateMaintenance_LockedByOpID_Real(t *testing.T) {
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
	const opIDA = "000000000000000000000000000000a1"
	if err := tidb.AcquireMaintenance(ctx, db, opIDA); err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	t.Cleanup(func() { _, _ = tidb.ReleaseMaintenance(ctx, db, opIDA) })

	// B 事务必须检测到被占用
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tidb.CoordinateMaintenance(ctx, db, tx); !errors.Is(err, tidb.ErrMaintenanceLocked) {
		_ = tx.Rollback()
		t.Fatalf("CoordinateMaintenance (op_id held) err = %v, want ErrMaintenanceLocked", err)
	}
	_ = tx.Rollback()

	// A 释放后 B 可正常协调
	if _, err := tidb.ReleaseMaintenance(ctx, db, opIDA); err != nil {
		t.Fatalf("release A: %v", err)
	}
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx(2): %v", err)
	}
	if err := tidb.CoordinateMaintenance(ctx, db, tx2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("CoordinateMaintenance after release = %v, want nil", err)
	}
	_ = tx2.Rollback()
}
