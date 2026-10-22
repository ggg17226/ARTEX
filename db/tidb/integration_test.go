package tidb_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
)

// 两套互不相通的真实库门控：
//   - ARTEX_TIDB_DSN：业务库（已 finalize、schema_version=1、65 张表）。**只允许只读**
//     集成用例使用（OpenTiDB / ProbeCapabilities 一类）。
//   - ARTEX_TIDB_EMPTY_DSN：独占空库，凡是会执行 DDL、写 schema_meta、抢/清
//     maintenance 单例行的用例**必须**走这里。
const (
	envTiDBDSN      = "ARTEX_TIDB_DSN"
	envTiDBEmptyDSN = "ARTEX_TIDB_EMPTY_DSN"
)

// infraTableNames 是本套用例会自己建出来的基础设施表（ExecutePrepare /
// EnsureLockInfrastructure 的产物），清理与「空库」判定都以它为白名单。
var infraTableNames = []string{"schema_meta", "maintenance", "locks"}

// requireTiDB 是**业务库**门控：缺 DSN 时打印「未验证」并 skip。
//
// 只可用于只读用例。任何写库（DDL / schema_meta / maintenance 单例行）的用例
// 一律改用 requireEmptyTiDB —— 把已 finalize 的业务库 schema_version 打回 0
// 会让应用直接拒绝启动（上一轮已实际发生过一次）。
func requireTiDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(envTiDBDSN)
	if dsn == "" {
		t.Skipf("未验证：%s 未设置，跳过真实 TiDB 集成测试", envTiDBDSN)
	}
	return dsn
}

// requireEmptyTiDB 是**独占空库**门控，返回 ARTEX_TIDB_EMPTY_DSN。
//
//   - 未设置 → t.Skip（诚实跳过，绝不回落到 ARTEX_TIDB_DSN 指向的业务库）；
//   - 已设置 → 先过三道护栏，任何一道不过直接 t.Fatalf 拒绝执行：
//     1. DSN 必须能解析且带库名（证明得了目标是谁）；
//     2. 与 ARTEX_TIDB_DSN 指向的物理库不得相同（同主机 + 同库名即同一个库，
//     用户名不参与判定；跨主机允许同名库）；
//     3. 连上去复核目标库里除 schema_meta/maintenance/locks 之外**没有任何基表**，
//     业务库即使换了名字也会被这一条挡下。
//
// 用例结束后把这三张基础设施表 DROP 掉，让空库回到 0 表状态（只 DROP TABLE，
// 绝不 DROP DATABASE）。
func requireEmptyTiDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(envTiDBEmptyDSN)
	if dsn == "" {
		t.Skipf("未验证：%s 未设置，跳过需要独占空库的用例"+
			"（这些用例会执行 DDL 并把 schema_version 写回 0，禁止在业务库 %s 上跑）", envTiDBEmptyDSN, envTiDBDSN)
	}
	empty := parseDSNTarget(t, envTiDBEmptyDSN, dsn)
	if strings.TrimSpace(empty.db) == "" {
		t.Fatalf("%s 未指定库名，无法证明目标是独占空库，拒绝执行", envTiDBEmptyDSN)
	}
	if biz := os.Getenv(envTiDBDSN); biz != "" {
		if samePhysicalLibrary(empty, parseDSNTarget(t, envTiDBDSN, biz)) {
			t.Fatalf("%s 与 %s 指向同一个物理库（net=%s addr=%s db=%s）："+
				"本组用例会把 schema_version 打回 0 并重建基础设施表，拒绝在业务库上执行",
				envTiDBEmptyDSN, envTiDBDSN, empty.net, empty.addr, empty.db)
		}
	}
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("%s 连接失败，无法核验独占空库：%v", envTiDBEmptyDSN, err)
	}
	defer db.Close()
	assertExclusiveEmptyLibrary(t, db)
	t.Cleanup(func() { dropInfraTables(t, dsn) })
	return dsn
}

// dsnTarget 是决定「连到哪一个物理库」的要素；密码刻意排除在外（换密码不改库身份）。
type dsnTarget struct{ net, addr, user, db string }

// parseDSNTarget 解析 DSN 并抽出目标库；解析失败即 Fail Closed（护栏本身
// 出问题时必须拒绝执行，而不是放行）。
func parseDSNTarget(t *testing.T, label, dsn string) dsnTarget {
	t.Helper()
	cfg, err := parseDSNSafe(dsn)
	if err != nil {
		t.Fatalf("%s 解析失败，无法证明它与其他库不是同一个库，拒绝执行：%v", label, err)
	}
	return dsnTarget{net: cfg.Net, addr: cfg.Addr, user: cfg.User, db: cfg.DBName}
}

// parseDSNSafe 包装 mysql.ParseDSN 并吞掉它的 panic（go-sql-driver v1.9.3 对
// 已废弃参数会直接 panic，与生产侧 tidb 包内 safeParseDSN 同一口径）。
func parseDSNSafe(dsn string) (cfg *mysql.Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid DSN: %v", r)
		}
	}()
	return mysql.ParseDSN(dsn)
}

// samePhysicalLibrary 判定两个 DSN 是否指向同一个物理库：协议 + 服务端地址 +
// 库名相同即同一个库（换用户连同一份数据仍是同一个库，故不参与判定）。
func samePhysicalLibrary(a, b dsnTarget) bool {
	return strings.EqualFold(a.db, b.db) && a.net == b.net && a.addr == b.addr
}

// assertExclusiveEmptyLibrary 断言目标库里除了本套用例自建的基础设施表之外没有
// 任何基表 —— 名字像空库、实际是一份业务库（换了库名/换了实例）的误配由这条兜住。
func assertExclusiveEmptyLibrary(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cur string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&cur); err != nil {
		t.Fatalf("read DATABASE() of %s: %v", envTiDBEmptyDSN, err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
  AND table_name NOT IN ('schema_meta','maintenance','locks')`).Scan(&n); err != nil {
		t.Fatalf("audit tables of %s: %v", envTiDBEmptyDSN, err)
	}
	if n != 0 {
		t.Fatalf("%s 指向的库 %s 里存在 %d 张基础设施表之外的基表：这不是独占空库，"+
			"拒绝执行会建表 / 改 schema_version 的用例", envTiDBEmptyDSN, cur, n)
	}
}

// dropInfraTables 清理本套用例在独占空库里建出来的基础设施表，让库回到 0 表状态。
// 只 DROP 这三张由用例自己建出来的表；requireEmptyTiDB 已经证明过目标不是业务库、
// 且不含业务表，因此这里不存在误删业务对象的可能。
func dropInfraTables(t *testing.T, dsn string) {
	t.Helper()
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Errorf("cleanup: 连接 %s 失败，基础设施表可能残留：%v", envTiDBEmptyDSN, err)
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, name := range infraTableNames {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+name+"`"); err != nil {
			t.Errorf("cleanup: drop table %s: %v（独占空库可能残留基础设施表）", name, err)
		}
	}
}

func TestOpenTiDB_EmptyDSNFailsClosed(t *testing.T) {
	t.Setenv(envTiDBDSN, "")
	db, _, err := tidb.OpenTiDB("")
	if err == nil {
		db.Close()
		t.Fatal("expected error for empty DSN, got nil")
	}
}

func TestOpenTiDB_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, caps, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if caps.Version == "" {
		t.Fatal("capabilities.Version empty — 探测未执行")
	}
	if caps.MaxAllowedPacket <= 0 {
		t.Fatalf("capabilities.MaxAllowedPacket = %d, want > 0（dev 实测 64MiB）", caps.MaxAllowedPacket)
	}
	if caps.Timezone != "+00:00" {
		t.Fatalf("session time_zone = %q, want +00:00（DSN 强制参数未生效）", caps.Timezone)
	}
	// 池参数抽查：MaxOpenConns 应为 32
	if db.Stats().MaxOpenConnections != 32 {
		t.Fatalf("MaxOpenConnections = %d, want 32", db.Stats().MaxOpenConnections)
	}
}

func TestProbeCapabilities_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	caps, err := tidb.ProbeCapabilities(context.Background(), db)
	if err != nil {
		t.Fatalf("ProbeCapabilities: %v", err)
	}
	if caps.Isolation == "" || caps.Collation == "" {
		t.Fatalf("incomplete capabilities: %+v", caps)
	}
}
