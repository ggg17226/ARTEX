package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// prepare 阶段的基础设施 DDL（幂等；红线：无 FK、无 CHECK、单列 BIGINT UNSIGNED 物理 PK + AUTO_ID_CACHE=1）。
// 结构逐字对齐 P02 `db/schema` 65 表目录（列名/类型/索引名/表选项与 P02 编译输出一致；
// P02 是全库 65 表 DDL 的唯一真实来源）：
//   - schema_meta: row_id / key VARCHAR(64) UNIQUE / value BIGINT / updated_at（key='schema_version' 行存版本戳）
//   - locks:       row_id / name VARCHAR(64) UNIQUE（'business_relations' 为基础设施命名行）
// 注意：`key` 是 MySQL 保留字，DDL 与所有 SQL 中一律用反引号包裹该列名。
const (
	schemaMetaDDL = `
CREATE TABLE IF NOT EXISTS ` + "`schema_meta`" + ` (
  ` + "`row_id`" + ` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT AUTO_ID_CACHE=1,
  ` + "`key`" + ` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  ` + "`value`" + ` BIGINT NOT NULL,
  ` + "`updated_at`" + ` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`row_id`" + `),
  UNIQUE KEY ` + "`uq_schema_meta_key`" + ` (` + "`key`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`
	locksDDL = `
CREATE TABLE IF NOT EXISTS ` + "`locks`" + ` (
  ` + "`row_id`" + ` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT AUTO_ID_CACHE=1,
  ` + "`name`" + ` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  PRIMARY KEY (` + "`row_id`" + `),
  UNIQUE KEY ` + "`uq_locks_name`" + ` (` + "`name`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`
)

// ExecutePrepare 幂等执行 prepare 阶段基础设施（对齐 P08 `artex db-migrate --prepare`；
// P08 侧实现消费 P02 `db/schema` 全库基线，本原语为三基础设施表的最小等价路径）：
// 建 schema_meta / maintenance / locks 三表、missing-only seed、schema_version 置 0。
// DDL 一律 autocommit（TiDB DDL 自动提交），不进事务；不执行任何业务 seed，
// 不 stamp 目标版本（0→1 由 P08 finalize 在同一事务条件戳）。
func ExecutePrepare(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, schemaMetaDDL); err != nil {
		return fmt.Errorf("create schema_meta: %w", err)
	}
	if err := EnsureLockInfrastructure(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, locksDDL); err != nil {
		return fmt.Errorf("create locks: %w", err)
	}
	// 两条基础设施 seed（与 P02 `schema.PrepareInfraSQL()` 同构；DSN 强制会话 UTC，NOW(3) 即 UTC）
	if _, err := db.ExecContext(ctx, `
INSERT IGNORE INTO schema_meta (` + "`key`" + `, ` + "`value`" + `, updated_at)
VALUES ('schema_version', 0, NOW(3))`); err != nil {
		return fmt.Errorf("seed schema_meta: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT IGNORE INTO locks (name)
VALUES ('business_relations')`); err != nil {
		return fmt.Errorf("seed locks: %w", err)
	}
	return nil
}

// ValidatePrepareState 严格校验 prepare 后的库状态（P08 侧同名函数
// lifecycle.ValidateBinding 是「应用启动前绑定」校验：schema_version==1 +
// business_deployment/traffic_stores 匹配，二者分工详见 Task 6 对齐段）；
// 本函数只依赖库内状态，不含 dataRoot 参数；任一不满足 → 显式错误，fail closed。
func ValidatePrepareState(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("validate prepare state: nil db")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var version int
	err := db.QueryRowContext(ctx, "SELECT value FROM schema_meta WHERE `key`='schema_version'").Scan(&version)
	if err == sql.ErrNoRows {
		return fmt.Errorf("validate prepare state: schema_meta 'schema_version' row missing — run ExecutePrepare first")
	}
	if err != nil {
		return fmt.Errorf("validate prepare state: read schema_version: %w", err)
	}
	if version != 0 {
		return fmt.Errorf("validate prepare state: schema_version = %d, want 0（prepare 阶段只接受 v0；v1 库走 P08 finalize 幂等 noOp 路径）", version)
	}
	var n int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM maintenance WHERE singleton_key IN ('main','maintenance')`).Scan(&n); err != nil {
		return fmt.Errorf("validate prepare state: audit maintenance: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("validate prepare state: maintenance singleton rows = %d, want 2", n)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM locks WHERE name='business_relations'`).Scan(&n); err != nil {
		return fmt.Errorf("validate prepare state: audit locks: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("validate prepare state: locks.business_relations rows = %d, want 1", n)
	}
	return nil
}
