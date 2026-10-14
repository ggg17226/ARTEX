package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ErrMaintenanceRowMissing 表示 maintenance 表或 singleton 行不存在：
// 先由 ExecutePrepare / EnsureLockInfrastructure 建立基础设施。
var ErrMaintenanceRowMissing = errors.New("maintenance row missing: run ExecutePrepare (or EnsureLockInfrastructure) first")

// MaintenanceTableDDL 是 P01 自有的基础设施 DDL（幂等；TiDB DDL 自动提交）。
// 红线：无 FOREIGN KEY、无 CHECK；物理主键单列 BIGINT UNSIGNED + AUTO_ID_CACHE=1。
// 结构逐字对齐 P02 `db/schema` 目录（列名/类型/索引名/表选项与 P02 编译输出一致；
// P02 是全库 65 表 DDL 的唯一真实来源）：删除早期自定义的 holder/schema_version 列 ——
// 维护窗口 owner 归 `op_id`（BINARY(16)，fencing 用 `revision`），版本戳归
// `schema_meta` 的 `key='schema_version'` 行。
const MaintenanceTableDDL = `
CREATE TABLE IF NOT EXISTS ` + "`maintenance`" + ` (
  ` + "`row_id`" + ` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT AUTO_ID_CACHE=1,
  ` + "`singleton_key`" + ` VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  ` + "`op_id`" + ` BINARY(16) NULL,
  ` + "`mode`" + ` TINYINT NOT NULL,
  ` + "`revision`" + ` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  ` + "`updated_at`" + ` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`row_id`" + `),
  UNIQUE KEY ` + "`uq_maintenance_singleton`" + ` (` + "`singleton_key`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

// lockBackoff 序列：100ms 起、上限 1s（对齐既有 applySchemaWithRetry 的节奏）。
var lockBackoff = [...]time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
}

// EnsureLockInfrastructure 在 autocommit 上下文执行幂等 DDL 与单例行 seed。
// TiDB DDL 自动提交，不进事务；重复执行安全（IF NOT EXISTS + INSERT IGNORE）。
func EnsureLockInfrastructure(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, MaintenanceTableDDL); err != nil {
		return fmt.Errorf("ensure maintenance table: %w", err)
	}
	// 两单例行按 P02 目录结构 seed（mode=0 常规态、revision=0；DSN 强制会话 UTC，NOW(3) 即 UTC）。
	if _, err := db.ExecContext(ctx, `
INSERT IGNORE INTO maintenance (singleton_key, op_id, mode, revision, updated_at)
VALUES ('main', NULL, 0, 0, NOW(3)),
       ('maintenance', NULL, 0, 0, NOW(3))`); err != nil {
		return fmt.Errorf("seed maintenance singletons: %w", err)
	}
	return nil
}

// AcquireMaintenance 以条件 UPDATE 原子抢占 maintenance 单例行的 op_id，
// 竞争时按 lockBackoff 退避重试直至 ctx 截止（ctx 错误原样返回，KindCancelled）。
// opID 必须为 16 字节 UUID（32 位 hex 或带连字符 36 位字符串），代表维护窗口 owner；
// 每次成功抢占 `revision` 单调递增，作为 fencing 依据（对齐 P02 目录结构）。
func AcquireMaintenance(ctx context.Context, db *sql.DB, opID string) error {
	op, err := normalizeOpID(opID)
	if err != nil {
		return fmt.Errorf("acquire maintenance: %w", err)
	}
	attempt := 0
	for {
		res, err := db.ExecContext(ctx, `
UPDATE maintenance
SET op_id = UNHEX(?), revision = revision + 1, updated_at = NOW(3)
WHERE singleton_key = 'maintenance' AND op_id IS NULL`, op)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if isTableMissing(err) {
				return ErrMaintenanceRowMissing
			}
			return fmt.Errorf("acquire maintenance: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return nil
		}
		// 被占：退避后重试。递增退避 100ms→250ms→500ms→1s（封顶，见 lockBackoff），
		// 退避用可取消的 timer，ctx 截止立即返回。
		idx := attempt
		if idx >= len(lockBackoff) {
			idx = len(lockBackoff) - 1
		}
		timer := time.NewTimer(lockBackoff[idx])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			attempt++
		}
	}
}

// ReleaseMaintenance 以 opID 条件释放锁；幂等（锁已不在自己手里返回 false）。
// 返回 (是否实际释放, error)。
func ReleaseMaintenance(ctx context.Context, db *sql.DB, opID string) (bool, error) {
	op, err := normalizeOpID(opID)
	if err != nil {
		return false, fmt.Errorf("release maintenance: %w", err)
	}
	res, err := db.ExecContext(ctx, `
UPDATE maintenance
SET op_id = NULL, updated_at = NOW(3)
WHERE singleton_key = 'maintenance' AND op_id = UNHEX(?)`, op)
	if err != nil {
		if isTableMissing(err) {
			return false, ErrMaintenanceRowMissing
		}
		return false, fmt.Errorf("release maintenance: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("release maintenance: %w", err)
	}
	return n == 1, nil
}

// CoordinateMaintenance 在调用方事务内对 maintenance 单例行加 FOR UPDATE 行锁，
// 使该事务与 AcquireMaintenance 抢占方互斥；行锁随 tx commit/rollback 原子释放。
// 语义对齐既有 db.coordinateWithSchemaMigration（PG advisory xact lock，db/db.go:82）。
// 注意：tx 必须持有真实事务；DDL 不得出现在该 tx 内（TiDB DDL 自动提交）。
func CoordinateMaintenance(ctx context.Context, db *sql.DB, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("coordinate maintenance: nil tx")
	}
	var one int
	if err := tx.QueryRowContext(ctx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance' FOR UPDATE`).Scan(&one); err != nil {
		if isTableMissing(err) {
			return ErrMaintenanceRowMissing
		}
		return fmt.Errorf("coordinate maintenance: %w", err)
	}
	return nil
}

// isTableMissing 识别 MySQL/TiDB 1146（表或行所在表不存在）。
func isTableMissing(err error) bool {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1146
	}
	return false
}

// normalizeOpID 把 16 字节 UUID 字符串（32 位 hex 或带连字符 36 位）归一为
// 32 位 hex，供 SQL 端 UNHEX 写入/比对 BINARY(16) 的 op_id 列；长度不符 →
// 显式错误（fail closed，与 P01 其余入口一致）。
func normalizeOpID(opID string) (string, error) {
	s := strings.ReplaceAll(opID, "-", "")
	if len(s) != 32 {
		return "", fmt.Errorf("opID %q: expected 16-byte UUID (32 hex chars, dashes optional)", opID)
	}
	return s, nil
}
