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

// ErrAcquireUncertain 表示抢锁 UPDATE 被 ctx 取消后，**无法确认**服务端究竟
// 是否已写入 owner：go-sql-driver v1.9.3 的取消只关闭 socket、不发 KILL QUERY，
// 服务端 UPDATE 仍会在行锁释放后照常提交，故「客户端返回 ctx 错误」既不能证明
// 未持有、也不能证明已持有。补偿复核查询本身失败（含复核预算在查询期间到期、
// 从未成功读到过 owner）或释放 UPDATE 失败时返回本 sentinel，错误链同时保留
// 真实的查询/释放错误与调用方 ctx 错误；调用方应重试或按 opID 显式清理
// （ReleaseMaintenance 幂等，可安全重放）。
var ErrAcquireUncertain = errors.New("acquire outcome uncertain after cancellation")

// ErrMaintenanceContended 表示抢锁在内部上限 maxAcquireWait 内始终未成功。
// 仅在调用方 ctx **没有 deadline** 时返回：退避序列 1s 封顶只约束单次等待、
// 没有总次数上限，`context.Background()` 抢锁会永久挂起（R0 实测 60s+ 卡死），
// 故对无 deadline 的调用施加总等待上限。上限经派生的有效 ctx 覆盖入口探测、
// 连接池等待、单条 SQL（含服务端行锁等待）与退避 sleep；唯一可超出上限的
// 尾项是 UPDATE 被取消后的补偿复核（独立预算 acquireReconcileBudget，默认 2s）。
// 只有该上限**确实到期**（有效 ctx 报 DeadlineExceeded 且调用方 ctx 未到期）才会
// 返回本 sentinel；权限拒绝、连接失败、SQL 语法等普通错误保留原始原因
// （`acquire maintenance: %w`），不会被改写成竞争超限，调用方可 errors.Is/As 归因。
var ErrMaintenanceContended = errors.New("maintenance lock contended beyond max wait")

// MaintenanceTableDDL 是 P01 自有的基础设施 DDL（幂等；TiDB DDL 自动提交）。
// 红线：无 FOREIGN KEY、无 CHECK；物理主键单列 BIGINT UNSIGNED + AUTO_ID_CACHE=1。
// 结构逐字对齐 P02 `db/schema` 目录（列名/类型/索引名/表选项与 P02 编译输出一致；
// P02 是全库 65 表 DDL 的唯一真实来源）：删除早期自定义的 holder/schema_version 列 ——
// 维护窗口 owner 归 `op_id`（BINARY(16)，fencing 用 `revision`），版本戳归
// `schema_meta` 的 `key='schema_version'` 行。
const MaintenanceTableDDL = `
CREATE TABLE IF NOT EXISTS ` + "`maintenance`" + ` (
  ` + "`row_id`" + ` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ` + "`singleton_key`" + ` VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  ` + "`op_id`" + ` BINARY(16) NULL,
  ` + "`mode`" + ` TINYINT NOT NULL,
  ` + "`revision`" + ` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  ` + "`updated_at`" + ` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`row_id`" + `),
  UNIQUE KEY ` + "`uq_maintenance_singleton`" + ` (` + "`singleton_key`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE=1;
`

// lockBackoff 序列：100ms 起、上限 1s（对齐既有 applySchemaWithRetry 的节奏）。
var lockBackoff = [...]time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
}

// maxAcquireWait 是**调用方 ctx 没有 deadline** 时抢锁的总等待上限：退避序列
// 1s 封顶只约束单次 sleep，没有总次数上限，Background 调用会永久挂起。
// 实现为入口派生的有效 ctx 截止（见 AcquireMaintenance），覆盖入口探测、连接池
// 等待、单条 SQL（含服务端行锁等待）与退避 sleep；唯一可超出上限的尾项是 UPDATE
// 被取消后的补偿复核（独立预算 acquireReconcileBudget）。
// 有 deadline 的 ctx 不受影响（仍以 ctx 到期为准）。包级变量而非常量是为了让
// 离线测试能把上限调小，禁止生产路径外的真等 30s。
var maxAcquireWait = 30 * time.Second

// acquireReconcileBudget / acquireReconcilePoll 是取消后补偿（reconcileCanceledAcquire）
// 的独立 ctx 预算与轮询间隔：预算内一旦确认 owner 立即返回，不做满时长等待。
// 同样为包级变量，便于离线测试注入短预算。
var (
	acquireReconcileBudget = 2 * time.Second
	acquireReconcilePoll   = 50 * time.Millisecond
)

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
//
// 两处与「取消 / 无 deadline」相关的必要语义（R0 诊断，真实 TiDB 实测）：
//   - 取消后补偿：抢锁 UPDATE 返回 context.Canceled / DeadlineExceeded 时，服务端
//     语句可能并未被终止（驱动只关 socket、不发 KILL QUERY），行锁一释放就照常
//     提交并把 owner 写成自己。此时用独立 ctx 复核并条件释放，见
//     reconcileCanceledAcquire。
//   - 有界退避：调用方 ctx 没有 deadline 时，入口派生一个截止为 now+maxAcquireWait
//     的内部有效 ctx，入口探测、连接池等待、单条 SQL（含服务端行锁等待）与退避
//     sleep 全部使用有效 ctx；**内部上限真的到期**（有效 ctx 报 DeadlineExceeded
//     且调用方 ctx 仍健康）且错误由该到期造成 → 返回 ErrMaintenanceContended，
//     避免 context.Background() 永久挂起。普通查询/连接错误不受该映射影响，
//     一律以 `%w` 原样保留真实原因，不会被改写成竞争超限。有效 ctx 到期
//     发生在抢锁 UPDATE 上时同样先做补偿复核（晚到写风险与取消路径一致），
//     故总时长最多再超出补偿预算 acquireReconcileBudget。
func AcquireMaintenance(ctx context.Context, db *sql.DB, opID string) error {
	op, err := normalizeOpID(opID)
	if err != nil {
		return fmt.Errorf("acquire maintenance: %w", err)
	}
	// 有界退避：ctx 无 deadline 时派生带 maxAcquireWait 截止的有效 ctx，所有
	// 探测/UPDATE/等待都用它——上限因此覆盖初始探测、连接池等待、单条 SQL 与
	// 退避 sleep，且到期后立即返回，不会再于截止后补发 UPDATE。ctx 有 deadline
	// 时 effCtx == ctx，行为完全不变，仍以调用方 ctx 到期为准。
	effCtx := ctx
	capped := false
	if _, ok := ctx.Deadline(); !ok {
		capped = true
		var cancel context.CancelFunc
		effCtx, cancel = context.WithDeadline(ctx, time.Now().Add(maxAcquireWait))
		defer cancel()
	}
	// capExpired 报告**内部上限是否真的到期**：capped 且调用方 ctx 仍健康，
	// 且有效 ctx 的错误确实是 DeadlineExceeded。三者缺一不可——只判
	// `capped && ctx.Err()==nil` 的话，该谓词从派生 ctx 创建起就恒为 true，
	// 于是入口/循环内探测遇到的任何普通查询错误（权限、连接、SQL 语法…）都会
	// 在健康的无 deadline ctx 下被误报成 ErrMaintenanceContended，真实原因丢失
	// （R12 复审 N1）。普通错误一律原样包装返回，绝不改写成竞争超限。
	capExpired := func() bool {
		return capped && ctx.Err() == nil && errors.Is(effCtx.Err(), context.DeadlineExceeded)
	}
	// capTimeout 是上限到期的返回错误；消息保留上限时长便于调用方定位。
	capTimeout := func() error {
		return fmt.Errorf("%w: waited %s for maintenance lock", ErrMaintenanceContended, maxAcquireWait)
	}
	// Fail closed：单例行缺失（表在但行不在）时条件 UPDATE 恒 0 行，若不探测
	// 会陷入无限退避空转；进入循环前探测覆盖「抢锁前已缺失」，循环内每次
	// 0 行再探测覆盖「等待/退避期间被并发删除」，均返回 ErrMaintenanceRowMissing。
	var one int
	if err := db.QueryRowContext(effCtx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
			return ErrMaintenanceRowMissing
		}
		// 只有「错误本身由 ctx 到期造成」才可能映射 ErrMaintenanceContended：
		// 内部上限打断入口探测 → Contended；调用方 ctx 取消 → 原样返回 ctx 错误。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if capExpired() {
				return capTimeout()
			}
			return err
		}
		// 普通查询错误（权限拒绝、连接失败、SQL 语法…）：真实原因原样保留，
		// 绝不改写成竞争超限（R12 复审 N1）。
		return fmt.Errorf("acquire maintenance: %w", err)
	}
	attempt := 0
	for {
		// 有效 ctx 已到期（退避 timer 与到期同刻竞态时可能先回到循环头）：
		// 立即返回，不再补发 UPDATE。本轮 UPDATE 尚未发出，无晚到写风险，
		// 无需补偿。
		if err := effCtx.Err(); err != nil {
			if capExpired() {
				return capTimeout()
			}
			return err
		}
		res, err := db.ExecContext(effCtx, `
UPDATE maintenance
SET op_id = UNHEX(?), revision = revision + 1, updated_at = NOW(3)
WHERE singleton_key = 'maintenance' AND op_id IS NULL`, op)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if capExpired() {
					// 内部上限到期打断了排队中的 UPDATE：服务端晚到写风险与
					// 调用方取消一致，先做补偿复核，返回值以 ErrMaintenanceContended
					// 为准（复核确认释放/无法确认时错误链保留复核结果）。
					contended := capTimeout()
					if recErr := reconcileCanceledAcquire(db, op, effCtx, err); recErr != effCtx.Err() {
						return fmt.Errorf("%w: %w", contended, recErr)
					}
					return contended
				}
				// 客户端取消 ≠ 服务端取消：复核本次是否其实已落 owner，
				// 若是则条件释放，避免残留 owner 让后续抢锁者无限退避。
				return reconcileCanceledAcquire(db, op, ctx, err)
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
		if err := db.QueryRowContext(effCtx, `
SELECT 1 FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&one); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if capExpired() {
					return capTimeout()
				}
				return err
			}
			if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
				return ErrMaintenanceRowMissing
			}
			// 普通查询错误（权限/连接/SQL…）原样保留真实原因，不改写为 Contended（N1）。
			return fmt.Errorf("acquire maintenance: %w", err)
		}
		// 被占：退避后重试。递增退避 100ms→250ms→500ms→1s（封顶，见 lockBackoff），
		// 退避用可取消的 timer，有效 ctx（调用方 ctx 或内部上限）截止立即返回；
		// timer 与上限到期同刻竞态由循环头的 effCtx.Err() 兜住，不会截止后再发 UPDATE。
		idx := attempt
		if idx >= len(lockBackoff) {
			idx = len(lockBackoff) - 1
		}
		timer := time.NewTimer(lockBackoff[idx])
		select {
		case <-effCtx.Done():
			timer.Stop()
			if capExpired() {
				return capTimeout()
			}
			return ctx.Err()
		case <-timer.C:
			attempt++
		}
	}
}

// reconcileCanceledAcquire 在抢锁 UPDATE 因 ctx 取消/超时失败后复核真实结果。
//
// 背景（R0 诊断 + 真实 TiDB 实测）：go-sql-driver v1.9.3 的 ctx 取消只关闭
// socket、不发 KILL QUERY，服务端 UPDATE 会一直排在 maintenance 单例行的行锁队列里，
// 行锁一释放就照常提交并把 op_id 写成本次 opID——客户端已经返回 ctx 错误，
// owner 却被永久占用，后续抢锁者（尤其 context.Background()）无限退避挂起。
//
// 因此用**独立** ctx（不能复用已取消的 ctx）复核，预算 acquireReconcileBudget：
//   - op_id == 本次 opID → 条件 UPDATE 清空（只清自己，绝不动他人 owner），返回
//     "acquire maintenance: canceled after acquiring, released: %w"（链含 ctx 错误）；
//   - op_id 属于他人 → 本次 UPDATE 的 `WHERE op_id IS NULL` 不可能再命中，立即返回
//     原 ctx 错误；
//   - op_id 为空 → 晚到写可能仍排在行锁队列里，按 acquireReconcilePoll 短轮询到
//     预算耗尽；**成功读到 NULL 且预算在轮询间隔中正常耗尽**才按「窗口内未观察
//     到持有」返回原 ctx 错误（不保证服务端此后不再落 owner）；
//   - 复核查询本身失败——含普通 SQL 错误、连接/池错误、以及**复核预算在查询
//     期间到期（从未成功读到过 owner，或读到 NULL 后下一次查询超时）**——一律
//     无法确认 → 返回 ErrAcquireUncertain，错误链同时保留真实查询错误与 ctx
//     错误（不丢弃任何真实失败原因）；
//   - 释放 UPDATE 失败 → 同样 ErrAcquireUncertain，链含真实释放错误与 ctx 错误。
func reconcileCanceledAcquire(db *sql.DB, op string, ctx context.Context, cause error) error {
	causeErr := ctx.Err()
	if causeErr == nil {
		causeErr = cause
	}
	rctx, cancel := context.WithTimeout(context.Background(), acquireReconcileBudget)
	defer cancel()
	for {
		var cur sql.Null[string]
		err := db.QueryRowContext(rctx, `
SELECT op_id FROM maintenance WHERE singleton_key = 'maintenance'`).Scan(&cur)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || isTableMissing(err) {
				return causeErr // 单例行不存在，谈不上持有
			}
			// 复核查询失败：普通错误与「查询自身耗尽预算」（rctx 到期、等池
			// 失败等）都属无法确认——后者从未成功读到过 owner，不能降格为
			// 「未持有」。真实查询错误与 ctx 错误一并保留在链上。
			return fmt.Errorf("%w: cancel outcome uncertain: %w: %w", ErrAcquireUncertain, causeErr, err)
		}
		if cur.Valid {
			if !strings.EqualFold(hex.EncodeToString([]byte(cur.V)), op) {
				return causeErr // 明确属于他人
			}
			// 本次取消的 UPDATE 其实已落 owner：条件释放（只清自己）。
			if _, rerr := db.ExecContext(rctx, `
UPDATE maintenance
SET op_id = NULL, updated_at = NOW(3)
WHERE singleton_key = 'maintenance' AND op_id = UNHEX(?)`, op); rerr != nil {
				// 已确认持有但没清干净：真实释放错误与 ctx 错误一并保留。
				return fmt.Errorf("%w: canceled after acquiring, release failed: %w: %w", ErrAcquireUncertain, causeErr, rerr)
			}
			return fmt.Errorf("acquire maintenance: canceled after acquiring, released: %w", causeErr)
		}
		// op_id 为空：晚到写可能仍在行锁队列里，短轮询到预算耗尽。
		// 只有「成功读到 NULL + 预算在间隔中正常耗尽」这一条路径按
		// 「窗口内未观察到持有」返回原 ctx 错误。
		if !sleepCtx(rctx, acquireReconcilePoll) {
			return causeErr
		}
	}
}

// sleepCtx 睡 d 后返回 true；ctx 提前结束（含预算耗尽）则立即返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
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
