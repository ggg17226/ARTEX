package idalloc

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// openTiDB 读 env ARTEX_TIDB_DSN（与 P08 同名 env；P03 不依赖 config/）。
// 缺失时打印「未验证」并跳过——不得伪造通过（spec Global Constraints 诚实性）。
func openTiDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skipf("未验证: ARTEX_TIDB_DSN 未配置")
	}
	db, err := sql.Open("mysql", dsn) // P01 注册的 TiDB driver；driver 名以 P01 落地为准
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

// ---- 纯逻辑边界（无门控，必须全量通过）----

func TestBoundary_ZeroAndAboveMaxInt64(t *testing.T) {
	// id==0 是合法 sentinel（before=0 / pool reset 0），但**分配结果** 0 必须失败：
	if err := ValidateUint64(0); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(0) = %v, want ErrOutOfSafeRange", err)
	}
	// id>MaxInt64（§11.1 实测 9223372036854775813/...814）必须失败：
	if err := ValidateUint64(math.MaxInt64 + 1); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(MaxInt64+1) = %v, want ErrOutOfSafeRange", err)
	}
	if err := ValidateUint64(9223372036854775813); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(9223372036854775813) = %v, want ErrOutOfSafeRange", err)
	}
	// 域边界值本身合法：
	if err := ValidateUint64(math.MaxInt64); err != nil {
		t.Fatalf("ValidateUint64(MaxInt64) = %v, want nil", err)
	}
}

// ---- 真实 TiDB 边界（env 门控）----

// §11.1：插入 9223372036854775813（>MaxInt64）后 LAST_INSERT_ID()=0；
// 其后正常自增得 9223372036854775814 仍为 0 → AllocID 必须显式失败，不 fallback。
func TestTiDB_HighValueLastInsertIDZero(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
			t.Errorf("cleanup drop table: %v", err)
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	// 显式插入高值（需带必填 logical_key 满足 STRICT_TRANS_TABLES）
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(9223372036854775813), "high_1"); err != nil {
		t.Fatalf("insert high value: %v", err)
	}
	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID after high-value insert = %v, want ErrReadbackZero（LAST_INSERT_ID()=0 实测）", err)
	}

	// 补齐后续自增插入（logical_key = "high_2"），断言自增获得的 LAST_INSERT_ID() 依然为 0 并被 AllocID 拒绝
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (logical_key) VALUES (?)", "high_2"); err != nil {
		t.Fatalf("insert auto-increment after high value: %v", err)
	}
	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID after subsequent auto-increment = %v, want ErrReadbackZero（LAST_INSERT_ID()=0 实测）", err)
	}
}

// §11.1：MaxUint64 显式插入成功且可回读（roundtrip 合法），
// 随后自增回绕 id=0（另一次 ERROR 1105 auto increment action failed）→ allocator 必须 fail closed。
// 注意：按 Task 2 补充说明强制修正：
// 1. 公开业务 ID 回读 AllocIDByIdentity 对 MaxUint64 必须返回 ErrOutOfSafeRange；
// 2. 物理主键高值回读必须使用 AllocPhysicalRowIDByIdentity 并验证 roundtrip。
func TestTiDB_MaxUint64RoundtripThenWrap(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
			t.Errorf("cleanup drop table: %v", err)
		}
	}()

	// 1) MaxUint64 显式 insert + exact identity 回读 roundtrip 合法：
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(math.MaxUint64), "max"); err != nil {
		t.Fatalf("insert MaxUint64: %v", err)
	}

	// 补充断言：公开业务 ID 回读 helper 严禁放行 >MaxInt64 高值
	if _, err := AllocIDByIdentity(ctx, tx, "SELECT id FROM idalloc_t WHERE logical_key = ?", "max"); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocIDByIdentity for MaxUint64 = %v, want ErrOutOfSafeRange", err)
	}

	// 底层物理主键回读 helper 允许合法高值 roundtrip
	id, err := AllocPhysicalRowIDByIdentity(ctx, tx, "SELECT id FROM idalloc_t WHERE logical_key = ?", "max")
	if err != nil {
		t.Fatalf("identity readback: %v", err)
	}
	if id != math.MaxUint64 {
		t.Fatalf("roundtrip = %d, want MaxUint64", id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 2) 随后自增：回绕 id=0 或 ERROR 1105 → 两种形态都必须 fail closed：
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	defer tx2.Rollback()
	if _, err := tx2.ExecContext(ctx, "INSERT INTO idalloc_t (logical_key) VALUES ('after')"); err != nil {
		// 形态 B：ERROR 1105 auto increment action failed —— 分配器本身拒绝，fail closed 达成
		// 精准断言错误确实源自 allocator 耗尽，杜绝非预期错误假绿
		var mysqlErr *mysql.MySQLError
		isMySQL1105 := errors.As(err, &mysqlErr) && mysqlErr.Number == 1105
		isAutoIncActionFailed := strings.Contains(strings.ToLower(err.Error()), "auto increment action failed")
		if !isMySQL1105 && !isAutoIncActionFailed {
			t.Fatalf("unexpected insert error: %v", err)
		}
		t.Logf("allocator 拒绝自增（fail closed，精准命中 allocator exhaustion）: %v", err)
		return
	}
	// 形态 A：自增回绕 id=0 → LAST_INSERT_ID()=0 或 identity 回读到 0 → 显式错误
	if _, err := AllocID(ctx, tx2); !errors.Is(err, ErrReadbackZero) && !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocID after wrap = %v, want ErrReadbackZero/ErrOutOfSafeRange", err)
	}
	var wrapped uint64
	if err := tx2.QueryRowContext(ctx, "SELECT id FROM idalloc_t WHERE logical_key = 'after'").Scan(&wrapped); err != nil {
		t.Fatalf("scan wrapped: %v", err)
	}
	if wrapped != 0 {
		t.Fatalf("回绕形态未复现：wrapped id = %d（§11.1 预期 0；若 TiDB 版本行为变化需更新本用例并记录）", wrapped)
	}
}

// §11.1：INSERT IGNORE miss → ROW_COUNT()=0、LAST_INSERT_ID()=0 → 必须显式报错，
// 不得把「未插入」当作成功并回读 0。
// 注意：按 Task 2 补充说明强制修正首次插入期望 affected=1。
func TestTiDB_InsertIgnoreMissExplicitError(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
			t.Errorf("cleanup drop table: %v", err)
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "INSERT IGNORE INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(1), "dup")
	if err != nil {
		t.Fatalf("insert ignore: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("首次插入期望 affected=1，实际 got %d", affected)
	}
	// 重复同 logical_key → miss：
	res2, err := tx.ExecContext(ctx, "INSERT IGNORE INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(2), "dup")
	if err != nil {
		t.Fatalf("insert ignore dup: %v", err)
	}
	if got, _ := res2.RowsAffected(); got != 0 {
		t.Fatalf("IGNORE miss RowsAffected = %d, want 0（§11.1 实测）", got)
	}
	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID after IGNORE miss = %v, want ErrReadbackZero（LAST_INSERT_ID()=0 实测）", err)
	}
}
