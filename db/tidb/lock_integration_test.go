package tidb_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/tidb"
)

// resetMaintenanceOwner 立即清空 maintenance 单例行的 op_id，并注册同名 cleanup。
// 残留 owner 会让后续抢锁退避到超时（R0 挂起根因），故每个 maintenance 用例
// 进出各清一次，用例之间互不污染。
func resetMaintenanceOwner(t *testing.T, db *sql.DB) {
	t.Helper()
	clear := func(why string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "UPDATE maintenance SET op_id=NULL WHERE singleton_key='maintenance'"); err != nil {
			if why == "start" {
				t.Fatalf("reset maintenance op_id: %v", err)
			}
			t.Logf("cleanup reset maintenance op_id: %v", err)
		}
	}
	clear("start")
	t.Cleanup(func() { clear("cleanup") })
}

// maintenanceOwner 返回 maintenance.op_id 的大写 hex；NULL 返回 ""。
func maintenanceOwner(t *testing.T, db *sql.DB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, `
SELECT HEX(op_id) FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&v); err != nil {
		t.Fatalf("read maintenance op_id: %v", err)
	}
	if !v.Valid {
		return ""
	}
	return strings.ToUpper(v.String)
}

// requireNoMaintenanceOwnerSoon 断言 op_id 最终为 NULL：取消补偿是异步收敛的
// （服务端晚到写可能在取消之后才落库），故给最多 2s 的轮询窗口（单次 SELECT
// 另有 5s 独立上限，窗口是尽力值而非硬时限）。
//
// 调用时机必须在被断言的取消抢锁**返回之后**（errB 已到手）。但要注意补偿是
// Acquire 返回前**同步**执行的：晚到写落库 → 复查看到 own owner → 条件释放清空，
// 整段都早于 errB 返回，因此本窗口的轮询**覆盖不到**「落库→清空」这个过程；
// sawOwner=false 是正常结果，也不能反过来证明补偿做过什么（R12 复审 N2 校准，
// 原注释误称本窗口覆盖完整过程）。两条证据彼此独立：
//   - 补偿确实执行（观察到 own owner 并进入条件释放）：由调用方对 errB 的
//     "canceled after acquiring, released" 错误文本断言证明；
//   - 补偿完成后的状态：由本 helper 断言「errB 返回之后 owner 为 NULL」。
//
// 窗口内若真观察到非 NULL owner，只说明另有晚到写正在被清（额外信息，不作断言）。
func requireNoMaintenanceOwnerSoon(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	sawOwner := false
	last := maintenanceOwner(t, db)
	for {
		if last != "" {
			sawOwner = true
		}
		if last == "" || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
		last = maintenanceOwner(t, db)
	}
	if last != "" {
		t.Fatalf("maintenance op_id still %s after 2s, want NULL (canceled competitor left an owner)", last)
	}
	t.Logf("owner poll window done (starts after errB returned): non-NULL observed during window = %v "+
		"(compensation runs synchronously before errB, so this window only verifies the final NULL; "+
		"the reconcile branch itself is proven by errB's error text)", sawOwner)
}

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
	resetMaintenanceOwner(t, db)
	// 抢锁一律用有界 ctx：无 deadline 的调用虽有 maxAcquireWait 兜底，但测试
	// 不应依赖 30s 上限来暴露挂起。
	ctxTO, cancelTO := context.WithTimeout(ctx, 5*time.Second)
	defer cancelTO()
	const opIDA = "0123456789abcdef0123456789abcdef"
	if err := tidb.AcquireMaintenance(ctxTO, db, opIDA); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = tidb.ReleaseMaintenance(cleanCtx, db, opIDA)
	})
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
	// 释放后可再获取（有界 ctx：抢锁一律禁止 context.Background() 裸抢）
	const opIDC = "99999999999999999999999999999999"
	ctxC, cancelC := context.WithTimeout(ctx, 5*time.Second)
	defer cancelC()
	if err := tidb.AcquireMaintenance(ctxC, db, opIDC); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = tidb.ReleaseMaintenance(cleanCtx, db, opIDC)
	})
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
	resetMaintenanceOwner(t, db)
	// 长 tx 内 coordinate 后，竞争方获取必须被行锁阻塞
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tidb.CoordinateMaintenance(ctx, db, tx); err != nil {
		t.Fatalf("CoordinateMaintenance: %v", err)
	}
	// 协调事务在 500ms 提交，竞争者 B 在 300ms 取消：B 的服务端 UPDATE 仍排在
	// maintenance 行的行锁队列里（驱动取消只关 socket、不发 KILL QUERY），事务一
	// 提交就照常把 op_id 写成 B——R0 实测残留的正是这个 opID。把提交点安排在 B
	// 的取消补偿窗口内，用来断言补偿必须把这份晚到残留清掉。
	commitDone := make(chan error, 1)
	time.AfterFunc(500*time.Millisecond, func() { commitDone <- tx.Commit() })

	const opIDB = "b1b2b3b4b5b6b7b8b9b0c1c2c3c4c5c6"
	ctxB, cancelB := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelB()
	start := time.Now()
	errB := tidb.AcquireMaintenance(ctxB, db, opIDB)
	if errB == nil {
		t.Fatal("acquire should be blocked by coordinating tx")
	}
	if !errors.Is(errB, context.DeadlineExceeded) {
		t.Fatalf("blocked acquire err = %v, want ctx deadline", errB)
	}
	if errors.Is(errB, tidb.ErrAcquireUncertain) {
		t.Errorf("canceled acquire reported uncertain outcome: %v", errB)
	}
	// 核心回归断言：B 的错误必须来自「复核观察到 own owner 并条件释放」分支——
	// 仅凭 DeadlineExceeded + 最终 NULL 无法排除「补偿根本没执行、晚到写也没落库
	// 或被别人清掉」的假绿调度；错误文本精确锁定补偿执行路径。
	if !strings.Contains(errB.Error(), "canceled after acquiring, released") {
		t.Fatalf("canceled acquire err = %v, want chain through the reconcile release branch (%q)",
			errB, "canceled after acquiring, released")
	}
	// 补偿在独立预算内收敛，调用不得无限期挂起
	if elapsed := time.Since(start); elapsed >= 5*time.Second {
		t.Errorf("canceled acquire took %v, want well under the reconcile budget", elapsed)
	}
	t.Logf("canceled competitor B returned after %v: %v", time.Since(start).Round(time.Millisecond), errB)

	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}
	// 核心断言（此时 errB 已返回、补偿已同步执行完）：被 300ms 取消的竞争者 B
	// 不得留下 owner（最多 2s 内必须收敛为 NULL）。分工：补偿确实走过「看到 own
	// owner → 条件释放」分支由上面 errB 的 "released" 错误文本证明；本 helper 只
	// 验证补偿完成之后的状态是 NULL，不覆盖也不声称覆盖落库→清空的过程。
	requireNoMaintenanceOwnerSoon(t, db)

	// tx 提交后行锁释放，可再获取（有界 ctx，禁止再用 Background 裸抢）
	const opIDAft = "c1c2c3c4c5c6c7c8c9c0d1d2d3d4d5d6"
	ctxAft, cancelAft := context.WithTimeout(ctx, 5*time.Second)
	defer cancelAft()
	if err := tidb.AcquireMaintenance(ctxAft, db, opIDAft); err != nil {
		t.Fatalf("acquire after commit: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = tidb.ReleaseMaintenance(cleanCtx, db, opIDAft)
	})
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
	resetMaintenanceOwner(t, db)
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
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tidb.CoordinateMaintenance(ctxTO, db, tx); !errors.Is(err, tidb.ErrMaintenanceRowMissing) {
		t.Fatalf("CoordinateMaintenance (row missing) err = %v, want ErrMaintenanceRowMissing", err)
	}
	_ = tx.Rollback()
}

// TestAcquireMaintenance_RowDeletedDuringBackoff_Real：A 持锁期间 B 抢锁进入
// 退避重试，退避窗口内单例行被并发删除——B 必须在下次 0 行尝试时探测到缺行
// 并 fail closed 返回 ErrMaintenanceRowMissing，而不是无限退避直到 ctx 超时
// 误报 DeadlineExceeded（回归：循环内缺行探测）。
func TestAcquireMaintenance_RowDeletedDuringBackoff_Real(t *testing.T) {
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
	resetMaintenanceOwner(t, db)
	const opIDA = "d1d2d3d4d5d6d7d8d9d0e1e2e3e4e5e6"
	ctxA, cancelA := context.WithTimeout(ctx, 5*time.Second)
	defer cancelA()
	if err := tidb.AcquireMaintenance(ctxA, db, opIDA); err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = tidb.ReleaseMaintenance(cleanCtx, db, opIDA)
	})
	// 还原单例行（LIFO：先于 release A 与 db.Close 执行，以 op_id=NULL 新行
	// 重建基础设施，等效于同时清掉 A 的残留持有）
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanCtx, `
INSERT INTO maintenance (singleton_key, op_id, mode, revision, updated_at)
VALUES ('maintenance', NULL, 0, 0, NOW(3))`)
	})

	// B 以 3s 截止抢锁：A 持锁使其 UPDATE 恒 0 行，进入退避重试
	type bres struct {
		err  error
		took time.Duration
	}
	resCh := make(chan bres, 1)
	go func() {
		ctxB, cancelB := context.WithTimeout(ctx, 3*time.Second)
		defer cancelB()
		start := time.Now()
		errB := tidb.AcquireMaintenance(ctxB, db, "e1e2e3e4e5e6e7e8e9f0f1f2f3f4f5f6")
		resCh <- bres{err: errB, took: time.Since(start)}
	}()

	// 等 B 完成首次尝试并进入退避窗口后再删行，确保命中「退避期间删行」路径
	time.Sleep(200 * time.Millisecond)
	if _, err := db.ExecContext(ctx, `
DELETE FROM maintenance WHERE singleton_key = 'maintenance'`); err != nil {
		t.Fatalf("delete singleton row: %v", err)
	}

	res := <-resCh
	if !errors.Is(res.err, tidb.ErrMaintenanceRowMissing) {
		t.Fatalf("B acquire after row deleted = %v, want ErrMaintenanceRowMissing", res.err)
	}
	if errors.Is(res.err, context.DeadlineExceeded) {
		t.Fatalf("B acquire after row deleted returned ctx deadline (%v), want fail-closed ErrMaintenanceRowMissing", res.err)
	}
	if res.took >= 2*time.Second {
		t.Errorf("B fail-closed took %v, want < 2s (no backoff spin)", res.took)
	}
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
	resetMaintenanceOwner(t, db)
	const opIDA = "000000000000000000000000000000a1"
	ctxA, cancelA := context.WithTimeout(ctx, 5*time.Second)
	defer cancelA()
	if err := tidb.AcquireMaintenance(ctxA, db, opIDA); err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = tidb.ReleaseMaintenance(cleanCtx, db, opIDA)
	})

	// B 事务必须检测到被占用
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
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
	t.Cleanup(func() { _ = tx2.Rollback() })
	if err := tidb.CoordinateMaintenance(ctx, db, tx2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("CoordinateMaintenance after release = %v, want nil", err)
	}
	_ = tx2.Rollback()
}
