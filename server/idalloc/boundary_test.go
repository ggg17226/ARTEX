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

// §11.1：插入 9223372036854775813（>MaxInt64）后 LAST_INSERT_ID()=0 → AllocID 必须显式失败，不 fallback。
//
// 计划 §11.1 假设 vs TiDB v8.5.8 实测（192.168.27.79:6000，本用例第二步）：
//   - 假设：显式插入高值后的**后续自增插入**，LAST_INSERT_ID() 仍为 0（→ ErrReadbackZero）。
//   - 实测：TiDB 把自增计数器**接续**到显式高值 +1，
//     SELECT LAST_INSERT_ID() = 9223372036854775814（= 9223372036854775813 + 1）> MaxInt64。
//
// 两步都必须 fail closed，但由**不同层**承担：
//   - 第一步（显式高值插入）：TiDB 不把高值写入 session last_insert_id → 回读 0，
//     fail closed 由 AllocID 的「回读 0」守卫（ErrReadbackZero）承担；
//   - 第二步（后续自增插入）：回读到的是越域高值而非 0，
//     fail closed 由 idalloc 的**兼容域校验** ValidateUint64（ErrOutOfSafeRange）承担。
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

	// 第二步：补齐后续自增插入（logical_key = "high_2"）。
	// 先显式 SELECT LAST_INSERT_ID() 证明 TiDB 的「自增计数器接续显式高值」行为，
	// 再断言 AllocID 以兼容域校验 fail closed（不得放宽为「任意 error 即可」）。
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (logical_key) VALUES (?)", "high_2"); err != nil {
		t.Fatalf("insert auto-increment after high value: %v", err)
	}
	var seqReadback uint64
	if err := tx.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&seqReadback); err != nil {
		t.Fatalf("readback LAST_INSERT_ID after subsequent auto-increment: %v", err)
	}
	// v8.5.8 实测值：9223372036854775814（= 显式高值 9223372036854775813 + 1）。
	// 计划 §11.1 假设此处为 0（→ ErrReadbackZero）；实测为接续高值 +1，
	// 因此本步断言 > MaxInt64，fail closed 由 ValidateUint64 的越域校验（ErrOutOfSafeRange）承担。
	if seqReadback <= math.MaxInt64 {
		t.Fatalf("后续自增 LAST_INSERT_ID() = %d，未复现「接续显式高值 >MaxInt64」形态（v8.5.8 实测 9223372036854775814）；若 TiDB 版本行为变化需更新本用例并记录", seqReadback)
	}
	t.Logf("后续自增 LAST_INSERT_ID() 实测 = %d（> MaxInt64 = %d）；计划 §11.1 的「回读 0」假设在 v8.5.8 不成立", seqReadback, uint64(math.MaxInt64))

	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocID after subsequent auto-increment = %v, want ErrOutOfSafeRange（回读值 %d 越域，fail closed 由兼容域校验承担）", err, seqReadback)
	}
}

// §11.1：MaxUint64 显式插入成功且可回读（roundtrip 合法），
// 随后自增必须 fail closed。
//
// 计划 §11.1 假设 vs TiDB v8.5.8 实测（192.168.27.79:6000）：
//   - 假设形态 A：自增回绕 id=0（LAST_INSERT_ID()=0）→ 由 idalloc 的「回读 0」守卫 fail closed；
//   - 假设形态 B：ERROR 1105 "auto increment action failed"；
//   - **实测形态 B′**：`Error 1467 (HY000): Failed to read auto-increment value from storage engine`
//     —— 本实例不出现 1105，也不回绕到 0，而是由 TiDB 存储层直接拒绝写入。
//
// 即：本实例的 fail closed 由 **MySQL/TiDB SQL 层（errno 1467）** 承担，
// idalloc 层（ErrReadbackZero/ErrOutOfSafeRange）只在形态 A 时才被触发；
// 两种形态都必须被精准识别，非白名单错误一律 t.Fatalf，禁止静默通过。
//
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

	// 2) 随后自增：回绕 id=0 / ERROR 1105 / ERROR 1467 → 各形态都必须 fail closed：
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	defer tx2.Rollback()
	if _, err := tx2.ExecContext(ctx, "INSERT INTO idalloc_t (logical_key) VALUES ('after')"); err != nil {
		// 形态 B：自增计数器耗尽时由 SQL/存储层直接拒绝写入 —— fail closed 由 **TiDB 层**达成。
		// 精准白名单断言错误确实源自 allocator 耗尽，杜绝非预期错误假绿：
		//   - 1467：v8.5.8 实测 "Failed to read auto-increment value from storage engine"
		//   - 1105：计划 §11.1 假设的 errno（本实例未出现，保留跨版本兼容）
		//   - 文本 "auto increment action failed"：计划 §11.1 假设的错误文案（保留兼容）
		var mysqlErr *mysql.MySQLError
		hasMySQL := errors.As(err, &mysqlErr)
		isMySQL1467 := hasMySQL && mysqlErr.Number == 1467
		isMySQL1105 := hasMySQL && mysqlErr.Number == 1105
		isAutoIncActionFailed := strings.Contains(strings.ToLower(err.Error()), "auto increment action failed")
		if !isMySQL1467 && !isMySQL1105 && !isAutoIncActionFailed {
			t.Fatalf("unexpected insert error: %v", err)
		}
		if isMySQL1467 {
			t.Logf("allocator 拒绝自增（fail closed，v8.5.8 实测 errno 1467，由 TiDB 存储层承担）: %v", err)
			return
		}
		t.Logf("allocator 拒绝自增（fail closed，计划 §11.1 假设形态 1105/文案兼容）: %v", err)
		return
	}
	// 形态 A（计划 §11.1 假设分支，v8.5.8 实测**未触达**——本实例走上面的 errno 1467）：
	// 自增回绕 id=0 → LAST_INSERT_ID()=0 或 identity 回读到 0 → 由 idalloc 层显式报错。
	// 保留本分支以覆盖「TiDB 不回错、只回绕」的其它版本形态；断言本身不变。
	if _, err := AllocID(ctx, tx2); !errors.Is(err, ErrReadbackZero) && !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocID after wrap = %v, want ErrReadbackZero/ErrOutOfSafeRange", err)
	}
	var wrapped uint64
	if err := tx2.QueryRowContext(ctx, "SELECT id FROM idalloc_t WHERE logical_key = 'after'").Scan(&wrapped); err != nil {
		t.Fatalf("scan wrapped: %v", err)
	}
	if wrapped != 0 {
		t.Fatalf("回绕形态未复现：wrapped id = %d（计划 §11.1 假设 0；v8.5.8 实测为 errno 1467 直接拒绝，若 TiDB 版本行为变化需更新本用例并记录）", wrapped)
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
