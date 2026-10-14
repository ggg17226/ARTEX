package tidb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ErrMaintenanceRowMissing 表示 maintenance 表或 singleton 行不存在：
// 先由 ExecutePrepare / EnsureLockInfrastructure 建立基础设施。
var ErrMaintenanceRowMissing = errors.New("maintenance row missing: run ExecutePrepare (or EnsureLockInfrastructure) first")

// ErrMaintenanceLocked 表示 maintenance 单例行当前被持有（op_id 非空）。
// CoordinateMaintenance 在事务内检测到该状态时返回：持有方的行锁已随其
// autocommit 释放，互斥只能靠 op_id 判定，故 fail closed 不静默通过，
// 调用方应退避重试或按维护窗口语义处理。
var ErrMaintenanceLocked = errors.New("maintenance lock is held by another op")

// ErrInvalidOpID 表示 opID 不是合法 16 字节 UUID（32 位 hex，连字符可选）。
// Fail closed：阻止非 hex 字符串到达 SQL 端 UNHEX 后隐式得到 NULL 写入。
var ErrInvalidOpID = errors.New("invalid opID")

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
	// Fail closed：单例行缺失（表在但行不在）时条件 UPDATE 恒 0 行，若不探测
	// 会陷入无限退避空转；进入循环前探测覆盖「抢锁前已缺失」，循环内每次
	// 0 行再探测覆盖「等待/退避期间被并发删除」，均返回 ErrMaintenanceRowMissing。
	var one int
	if err := db.QueryRowContext(ctx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
			return ErrMaintenanceRowMissing
		}
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
		// 0 行：快速轻量探测区分两种原因——
		// 行在但 op_id 非空（被其他 owner 占用）→ 继续指数退避重试；
		// 行在等待/退避期间被并发删除（sql.ErrNoRows / 1146）→ fail closed
		// 立即返回 ErrMaintenanceRowMissing，终止重试，避免空转直到 ctx 超时。
		var one int
		if err := db.QueryRowContext(ctx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&one); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
				return ErrMaintenanceRowMissing
			}
			return fmt.Errorf("acquire maintenance: %w", err)
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
// 单例行缺失时返回 ErrMaintenanceRowMissing（fail closed）。
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
	if n == 1 {
		return true, nil
	}
	// 0 行：区分「单例行缺失」（fail closed）与「被他人持有」（幂等 false）。
	var one int
	if err := db.QueryRowContext(ctx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
			return false, ErrMaintenanceRowMissing
		}
		return false, fmt.Errorf("release maintenance: %w", err)
	}
	return false, nil
}

// CoordinateMaintenance 在调用方事务内对 maintenance 单例行加 FOR UPDATE 行锁，
// 并检查 op_id 判定互斥：
//   - 行缺失（含表缺失）→ ErrMaintenanceRowMissing；
//   - op_id 非空（已被 AcquireMaintenance 持有）→ ErrMaintenanceLocked
//     （持有方 autocommit 已释放行锁，互斥只能靠 op_id 判定，fail closed
//     不静默通过，调用方应退避重试）；
//   - op_id 为空 → 加行锁成功，该事务与后续抢占方互斥，行锁随
//     tx commit/rollback 原子释放。
//
// 语义对齐既有 db.coordinateWithSchemaMigration（PG advisory xact lock，db/db.go:82）。
// 注意：tx 必须持有真实事务；DDL 不得出现在该 tx 内（TiDB DDL 自动提交）。
func CoordinateMaintenance(ctx context.Context, db *sql.DB, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("coordinate maintenance: nil tx")
	}
	var opID sql.Null[string]
	if err := tx.QueryRowContext(ctx, `
SELECT op_id FROM maintenance WHERE singleton_key = 'maintenance' FOR UPDATE`).Scan(&opID); err != nil {
		if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
			return ErrMaintenanceRowMissing
		}
		return fmt.Errorf("coordinate maintenance: %w", err)
	}
	if opID.Valid {
		return ErrMaintenanceLocked
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
// 32 位 hex，供 SQL 端 UNHEX 写入/比对 BINARY(16) 的 op_id 列；长度不符或
// 含非 hex 字符 → ErrInvalidOpID（fail closed，与 P01 其余入口一致）。
// 严格用 hex.DecodeString 逐字符校验：非 hex 字符串若漏过，SQL 端 UNHEX
// 会隐式返回 NULL，污染 BINARY(16) 列与比对语义。
func normalizeOpID(opID string) (string, error) {
	s := strings.ReplaceAll(opID, "-", "")
	if len(s) != 32 {
		return "", fmt.Errorf("opID %q: expected 16-byte UUID (32 hex chars, dashes optional): %w", opID, ErrInvalidOpID)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", fmt.Errorf("opID %q: expected 32 hex chars: %w", opID, ErrInvalidOpID)
	}
	return s, nil
}
