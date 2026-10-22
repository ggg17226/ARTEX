package db

// =====================================================================
// 「漏洞复测」Agent 的启动 seed（P08 原生 TiDB 8.5 收尾，M08/M12 同口径）
//
// 旧实现是整段 PostgreSQL 方言（pg_advisory_xact_lock、$n 占位符、
// INSERT … ON CONFLICT (key) DO NOTHING RETURNING、INSERT … RETURNING），
// 在原生 TiDB 8.5 上每一条都是 1064/1054，启动期 seed 直接失败。
// 本文件按仓库既有的 TiDB 惯例重写，并且**整段迁移仍在同一个事务里**
// （agents / agent_prompts / settings 三处写入同生共死）。
//
// 写者协议（spec §3.2.2 全写者合同 + §3.2.1 写前仲裁，逐条对应实现）：
//
//  1. 统一 Runner / Business Gate（I4）。本写者不自己 BeginTx，而是走
//     lockorder.TxRunner.Run(TxRunnerOptions{RequiresBusinessGate: true})：
//     本地业务门禁 → 连接池准入 → BeginTx → DB 侧 `locks('business_relations')`
//     FOR UPDATE → 业务载荷 → 单次 Commit，失败整体回滚 + 指数退避整体重放。
//     锚点在全序中的位置（Gate 之后的实际取锁顺序，实测如此）：
//
//     Business Gate（本地 + locks 行）
//       → maintenance 专属锚点 row（本 seed 的 leaf 级互斥，仅本写者持有）
//       → settings 幂等 flag 行（row_id 物理 PK）
//       → agents 行（id 物理 PK）
//       → agent_prompts 追加（父行已在上面持锁，父先子后）
//
//     注意最后一段与 lockorder.TableOrder 的**数值序**是相反的（maintenance=32 <
//     settings=42，但 agents=2）：这是刻意的，不是笔误（R33 合并前审查 Minor-2）。
//     之所以不成环：
//       - 本 seed 是一次性启动写者，整个载荷在 Business Gate 内，Gate 先于任何实体锁；
//         其余写者要么持同一把 Gate（互斥），要么根本不碰 agents / settings /
//         maintenance 这三张表的行；
//       - 要严格升序就只能"先锁 agents 再读 flag"，那会在 flag 已置位的快路径上无故
//         锁一行 agents（启动期热路径），并让"已 seed 过"的判定依赖一次多余的实体锁；
//       - 全仓没有"先锁 agents → 再锁 settings / maintenance"的反向写者，
//         因此 32 ↔ 42 ↔ 2 之间不存在环。
//     若将来真的出现这样的反向路径，正确的修法是重排本 seed 的顺序（agents → settings
//     → maintenance），而不是给 Gate 开例外。
//
//     Gate 先于任何实体锁是 §3.2.2 的硬要求；专属锚点在 Gate **之后**，所以它
//     只用于把「同一 seed 的两次尝试」在 DB 层再串行一次（并覆盖 Runner 重放之间
//     的空档），不承担全局门禁职责，也不与 schema 维护窗口的
//     tidb.CoordinateMaintenance（另一条 maintenance 行 + op_id 租约）互认。
//
//  2. 事务隔离与"当前读"（I1）。Runner 用默认隔离级别（TiDB release-8.5 默认
//     RR，不改任何全局/会话配置）。RR 下**普通 SELECT 读的是本事务开始前的快照**，
//     锁读（FOR UPDATE）与 INSERT/UPDATE/DELETE 才读最新已提交版本。因此本文件里
//     每一个"决定是否要写"的读都必须是**锁读（当前读）**：
//     幂等 flag 的读取、hash 候选行定位、身份三元组回读全部 FOR UPDATE。
//     旧注释所称"锁读沿用本事务快照"是写反的，正确方向恰好相反，见
//     retesterSeedFlagSeededTx 的注释与 db/retester_seed_tidb_test.go 的
//     确定性调度回归（赢家提交 → 用户删除 → 已排队者继续）。
//
//  3. 写前身份仲裁（I2/I3）。agents 与 settings 都是 hash 唯一键表
//     （uq_agents_key_hash / uq_settings_key_hash）：1062 **不是**"同 key 已存在"
//     的证据，任何"已存在"结论都必须在物理 PK 锁保护下，用持久 canonical
//     identity 的完整字节比对得出（schema.VerifyHashArbitration 白名单仲裁器 +
//     自然键自洽校验）。同 hash 异 identity 一律 fail closed，绝不改写那一行。
//     settings 幂等标记因此**不再用 ON DUPLICATE KEY UPDATE**：先仲裁，再按物理
//     PK 精确 UPDATE（只碰 value/updated_at），或严格 INSERT。
//
//  4. 1062 归因与整体回滚重放。INSERT 撞 1062 时必须归因到具体索引：
//     只有落在本 seed 的自然键索引上，才按"定位快照被并发赢家推翻"处理，
//     返回 lockorder.ErrApplicationRetry 让 Runner **整笔回滚后在全新事务里重放**
//     （§3.2.1 路径 B-3）；落在其他索引（主键、别的唯一键）或无法归因时一律
//     fail closed —— 既不吞掉，也不在新事务里反复重放。
//
//  5. 本集群实测锁语义（TiDB 8.5.8，@@tidb_txn_mode=pessimistic；证据见测试里的
//     retesterSeedAssertWriteConflictHeld 与归档记录）。悲观锁**只在写写冲突时互斥**，
//     且没有 gap lock：
//       持有者未提交                另一会话            结果
//       SELECT … FOR UPDATE        SELECT … FOR UPDATE 不等待
//       SELECT … FOR UPDATE        UPDATE / INSERT 同行 等待 → 1205
//       INSERT 新行                UPDATE 该新行        不等待
//       INSERT 新行                INSERT 同一唯一键    等待 → 1205
//     两条推论，直接关系到上面的 Gate/锚点到底在防什么：
//       a) Gate 行与 maintenance 锚点行的 FOR UPDATE **不会**挡住另一个也只做锁读的
//          会话，因此它们不是"读读互斥锁"；真正的 DB 层串行发生在**写入**上：冷锚点靠
//          uq_maintenance_singleton 的 INSERT IGNORE 撞键等待，实体行靠 canonical hash
//          唯一键的写入冲突（等 → 1062 → 归因 → 仲裁/重放）。
//       b) 正因为读读不互斥，"决策读必须是当前读"（第 2 条）不是优化而是**唯一**的正确性
//          来源：两个并发 seed 都可能同时越过锚点，此时只有"锁读读到最新已提交的 flag"
//          + "唯一键写入冲突后整笔回滚重放"能挡住重复 seed 与复活。
//     本文件不依赖读读互斥，也不依赖 gap lock；上述矩阵是本写者正确性论证的一部分。
//
// 语义红线（与迁移前的 PG 版本逐条对齐，不得回退）：
//  1. flag=true 立即返回：编辑过的提示词重启不覆盖、用户删掉的 retester 不复活；
//  2. agents 自然键已存在（经身份仲裁确认）→ 不改写 agent 行、不追加 prompt 版本，只补落 flag；
//  3. prompt 与 flag 同事务：中途任何失败整体回滚，不留半套状态；
//  4. 并发初始化由 Gate + 专属锚点串行化：既不重复插 agent，也不会插出指向
//     不存在 agent 的 agent_prompts 孤儿行。
// =====================================================================

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
)

// FindingRetesterSeedFlag 是「retester 迁移已完成」的幂等标记，落在 settings。
// 与迁移前的 PG 实现同名同值，存量库不会被重跑。
const FindingRetesterSeedFlag = "finding_retester_seed_v1"

const (
	// retesterSeedLockSingletonKey 是本迁移专属的 maintenance 锚点行 key。
	// maintenance.singleton_key 是 VARCHAR(16) ascii，本常量 13 个字符。
	retesterSeedLockSingletonKey = "retester_seed"

	retesterAgentName        = "漏洞复测"
	retesterAgentDescription = "从漏洞详情手动启动，读取原证据并保存独立复测结论。"
	retesterAgentRole        = "assistant"
	retesterPromptNote       = "内置默认"
	retesterPromptUpdatedBy  = "system"
	retesterSeedFlagValue    = "true"

	// retesterSeedAgentIndex / retesterSeedSettingsIndex 是本 seed 唯一**允许**
	// 解释成"自然键已存在"的两条唯一索引；1062 落在别处必须 fail closed。
	retesterSeedAgentIndex    = "uq_agents_key_hash"
	retesterSeedSettingsIndex = "uq_settings_key_hash"
)

// 本 seed 的两类终态失败：
//   - ErrRetesterSeedHashCollision：hash 命中但持久 identity / 自然键不自洽
//     （同 hash 异 identity 的碰撞注入），必须整体回滚且不改动任何既有行；
//   - ErrRetesterSeedUnexpectedConflict：1062 落在预期之外的索引、或无法归因。
//
// 两者都**不可重试**：重放不会改变"这一行不是我们要写的对象"这一事实。
// 因此它们都不能把原始 *mysql.MySQLError 挂进 %w 链 —— 那会让
// lockorder.IsRetryableConflict 按结构化编号 1062 判成可重试冲突。
var (
	ErrRetesterSeedHashCollision      = schema.ErrIdentityHashCollision
	ErrRetesterSeedUnexpectedConflict = errors.New("retester seed: 1062 落在预期之外的唯一索引，拒绝解释成同 key 已存在")
)

// —— 测试专用调度缝 ————————————————————————————————————————————
//
// retesterSeedTestHook 只用于在同包测试里制造**确定性**调度交错（赢家提交 →
// 用户删除 → 已排队者继续；4 worker 冷锚点重叠）。生产进程里它恒为 nil，
// 判空后直接返回，不产生任何语句、等待或分支差异。
// 阶段名是稳定契约，测试按名等待，绝不用固定 sleep 代替屏障。
var retesterSeedTestHook func(stage, worker string)

const (
	// retesterSeedStageEnter 在 SeedFindingRetester 入口（Runner 之前）触发：
	// 到这里就代表这一次 seed 调用已经**在飞**，可与其它 worker 组屏障。
	retesterSeedStageEnter = "enter"
	// retesterSeedStageBeforeAnchor 在 payload 第一条语句（锚点锁读）之前触发。
	retesterSeedStageBeforeAnchor = "before-anchor"
	// retesterSeedStageAnchorHeld 在本事务已持有 maintenance 专属锚点（并持有
	// Business Gate）、尚未读任何业务行时触发：此刻的挂起者正持有一把真实行锁。
	retesterSeedStageAnchorHeld = "anchor-held"
	// retesterSeedStageBeforeFlagRead 在锚点已到手、幂等 flag 当前读**之前**触发。
	retesterSeedStageBeforeFlagRead = "before-flag-read"
	// retesterSeedStageBeforeCommit 在全部写入完成、payload 返回（Runner 提交）前触发。
	retesterSeedStageBeforeCommit = "before-commit"
)

type retesterSeedWorkerKey struct{}

// retesterSeedWithWorker 给一次 seed 打上 worker 标签，仅供测试区分调度对象。
func retesterSeedWithWorker(ctx context.Context, worker string) context.Context {
	return context.WithValue(ctx, retesterSeedWorkerKey{}, worker)
}

func retesterSeedWorker(ctx context.Context) string {
	if w, ok := ctx.Value(retesterSeedWorkerKey{}).(string); ok {
		return w
	}
	return ""
}

func retesterSeedStage(ctx context.Context, stage string) {
	if hook := retesterSeedTestHook; hook != nil {
		hook(stage, retesterSeedWorker(ctx))
	}
}

// SeedFindingRetester 原子地 seed「可编辑的漏洞复测 agent」。
// 不触发任何自动发现；一旦 seed 过（flag 已置位），用户的编辑与删除都能
// 跨重启保留；已存在的同 key agent 原样保留，不被改写。
//
// promptText 是写入 agent_prompts 的默认模板正文（由调用方带进来：模板正文
// 属于 agent 包，db 包不反向依赖它），空串视为调用方错误，fail closed。
//
// 并发/重试：整体走 TxRunner —— 1062 / 1205 / 1213 / 应用级重试信号都触发
// **整笔回滚 + 退避后在全新事务里重放**；碰撞与意外索引冲突 fail closed。
func (d *DB) SeedFindingRetester(ctx context.Context, promptText string) error {
	if promptText == "" {
		return errors.New("retester seed: 默认提示词为空，拒绝 seed 一个没有模板的 agent")
	}
	retesterSeedStage(ctx, retesterSeedStageEnter)
	return d.retesterSeedTxRunner().Run(ctx,
		lockorder.TxRunnerOptions{RequiresBusinessGate: true},
		func(ctx context.Context, tx *sql.Tx, _ *lockorder.LockContext) error {
			return d.seedFindingRetesterAttempt(ctx, tx, promptText)
		})
}

// retesterSeedTxRunner 构造本写者的统一事务运行器（默认退避阶梯 + 默认准入），
// 与 db/tasks.go 的 taskTxRunner 同一口径；每次 Run 都是全新的 *sql.Tx，
// 任何"部分写入 + 原地重试"都被禁止。
func (d *DB) retesterSeedTxRunner() *lockorder.TxRunner {
	return lockorder.NewTxRunner(d.DB)
}

// seedFindingRetesterAttempt 是单次尝试的业务载荷：Runner 已经持好 Business Gate
// （本地 + locks 行）并开好事务，这里只负责按上面的全序取实体锁、仲裁、写入。
//
// 导出给同包测试（真机的确定性调度用例要在**测试自己控制的事务**里重放本载荷，
// 才能把"输家的快照早于赢家提交"这一条件固定下来）；生产入口只有上面那个 Run。
func (d *DB) seedFindingRetesterAttempt(ctx context.Context, tx *sql.Tx, promptText string) error {
	if promptText == "" {
		return errors.New("retester seed: 默认提示词为空，拒绝 seed 一个没有模板的 agent")
	}
	retesterSeedStage(ctx, retesterSeedStageBeforeAnchor)
	if err := lockRetesterSeedTx(ctx, tx); err != nil {
		return err
	}
	retesterSeedStage(ctx, retesterSeedStageAnchorHeld)
	retesterSeedStage(ctx, retesterSeedStageBeforeFlagRead)

	// 红线 1：flag 已置位就立刻返回，绝不碰业务表。
	// 这个读**必须**是当前读（FOR UPDATE）：RR 下普通 SELECT 读的是本事务快照，
	// 会看不见并发赢家刚提交的 flag，进而把用户已经删掉的 retester 重新插回来。
	// 锁读读的是最新已提交版本，RR / RC 两级都成立。
	seeded, err := retesterSeedFlagSeededTx(ctx, tx)
	if err != nil {
		return err
	}
	if seeded {
		retesterSeedStage(ctx, retesterSeedStageBeforeCommit)
		return nil
	}

	agentID, inserted, err := insertRetesterAgentTx(ctx, tx)
	if err != nil {
		return err
	}
	if inserted {
		if err := seedRetesterPromptTx(ctx, tx, agentID, promptText); err != nil {
			return err
		}
	}
	if err := markRetesterSeededTx(ctx, tx); err != nil {
		return err
	}
	retesterSeedStage(ctx, retesterSeedStageBeforeCommit)
	return nil
}

// lockRetesterSeedTx 抢本迁移的事务级互斥锚点。锚点行自愈式 seed：
// EnsureLockInfrastructure 只 seed main/maintenance 两条，并发 seed 被唯一键
// uq_maintenance_singleton 挡下，之后仍然只有一行可锁。
//
// 这把锁只在 Business Gate **之后**获取（§3.2.2 门禁先于实体锁），并且是本
// 事务里第一个实体锁；后续实体锁（settings → agents → agent_prompts）严格
// 排在它后面，任何写者都不会反过来先取实体锁再取它。
func lockRetesterSeedTx(ctx context.Context, tx *sql.Tx) error {
	seek := `SELECT row_id FROM maintenance WHERE singleton_key = ? FOR UPDATE`
	var rowID int64
	err := tx.QueryRowContext(ctx, seek, retesterSeedLockSingletonKey).Scan(&rowID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("retester seed: lock anchor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO maintenance (singleton_key, op_id, mode, revision, updated_at)
VALUES (?, NULL, 0, 0, CURRENT_TIMESTAMP(3))`, retesterSeedLockSingletonKey); err != nil {
		return fmt.Errorf("retester seed: seed anchor: %w", err)
	}
	if err := tx.QueryRowContext(ctx, seek, retesterSeedLockSingletonKey).Scan(&rowID); err != nil {
		return fmt.Errorf("retester seed: lock anchor after seed: %w", err)
	}
	return nil
}

// —— 写前身份仲裁基础设施（§3.2.1 路径 A/B）————————————————————————

// locateHashedRowPKForUpdate 按 hash 唯一索引做**当前读**定位，拿到候选行的物理 PK。
//
// 它只是"定位器"，不是仲裁器：TiDB 里锁读读最新已提交版本，所以并发赢家刚插的行
// 也看得见；真正的"这行是不是我要写的对象"必须在拿到物理 PK 锁之后由
// verifyHashedRowIdentity 按完整字节比对决定（schema.VerifyHashArbitration 的
// 前置条件也正是"调用方已持物理 PK 锁"）。
func locateHashedRowPKForUpdate(ctx context.Context, tx *sql.Tx, table, pkCol, hashCol string, hash []byte) (int64, bool, error) {
	var pk int64
	err := tx.QueryRowContext(ctx,
		"SELECT `"+pkCol+"` FROM `"+table+"` WHERE `"+hashCol+"` = ? FOR UPDATE", hash).Scan(&pk)
	switch {
	case err == nil:
		if pk < 1 {
			return 0, false, fmt.Errorf("retester seed: %s.%s 物理主键 %d 越界", table, pkCol, pk)
		}
		return pk, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("retester seed: locate %s by %s: %w", table, hashCol, err)
	}
}

// lockRowByPhysicalPK 按物理 PK 取行级悲观锁（§3.2.1 路径 A-1 的单元素全序）。
func lockRowByPhysicalPK(ctx context.Context, tx *sql.Tx, table, pkCol string, pk int64) error {
	var got int64
	err := tx.QueryRowContext(ctx,
		"SELECT `"+pkCol+"` FROM `"+table+"` WHERE `"+pkCol+"` = ? FOR UPDATE", pk).Scan(&got)
	if err != nil {
		return fmt.Errorf("retester seed: lock %s %s=%d: %w", table, pkCol, pk, err)
	}
	if got != pk {
		return fmt.Errorf("retester seed: %s 锁回 %d 与目标主键 %d 不一致", table, got, pk)
	}
	return nil
}

// readHashedRowIdentityForUpdate 在物理 PK 锁的保护下当前读该行的持久身份三元组
// （自然键、canonical identity、identity hash）。这就是仲裁的**唯一**判据来源：
// 读的是库里的字节，不是调用方自己传进来的输入。
func readHashedRowIdentityForUpdate(ctx context.Context, tx *sql.Tx, table, pkCol, keyCol, identCol, hashCol string, pk int64) (string, []byte, []byte, error) {
	var naturalKey string
	var identity, hash []byte
	err := tx.QueryRowContext(ctx,
		"SELECT "+keyCol+", `"+identCol+"`, `"+hashCol+"` FROM `"+table+"` WHERE `"+pkCol+"` = ? FOR UPDATE",
		pk).Scan(&naturalKey, &identity, &hash)
	if err != nil {
		return "", nil, nil, fmt.Errorf("retester seed: read identity of %s %s=%d: %w", table, pkCol, pk, err)
	}
	return naturalKey, identity, hash, nil
}

// verifyHashedRowIdentity 写前身份仲裁（§3.2.1 路径 A-2、闭环 F02）：
// 在调用方已持物理 PK 锁的前提下，把持久身份与本 seed 的期望身份做**完整字节**比对，
// 并再核验一次自然键自洽（自然键重算的 canonical identity 必须逐字节等于持久 identity）。
// 三个条件缺一不可，任何不成立都返回 ErrRetesterSeedHashCollision：
// 只凭 hash 命中、或只凭"INSERT 报了 1062"，都不足以认定"这就是同一对象"。
//
// identityOf 是 schema 包里对应的 canonical builder（AgentKeyIdentity /
// SettingsKeyIdentity）；hashOf/expectedHash 用来同时核对 hash 列本身。
func verifyHashedRowIdentity(label, table, pkCol string, pk int64, storedKey string, storedIdentity, storedHash, wantIdentity, wantHash []byte, wantKey string, identityOf func(string) []byte) error {
	if !bytes.Equal(storedIdentity, wantIdentity) {
		return fmt.Errorf("retester seed: %s：%s.%s=%d 的持久 identity 与本 seed 期望不一致（同 hash 异 identity 碰撞）: %w",
			label, table, pkCol, pk, ErrRetesterSeedHashCollision)
	}
	if !bytes.Equal(storedHash, wantHash) {
		return fmt.Errorf("retester seed: %s：%s.%s=%d 的持久 hash 列与 canonical hash 不一致: %w",
			label, table, pkCol, pk, ErrRetesterSeedHashCollision)
	}
	if recomputed := identityOf(storedKey); !bytes.Equal(recomputed, storedIdentity) {
		return fmt.Errorf("retester seed: %s：%s.%s=%d 的自然键 %q 与持久 identity 不自洽: %w",
			label, table, pkCol, pk, storedKey, ErrRetesterSeedHashCollision)
	}
	if storedKey != wantKey {
		return fmt.Errorf("retester seed: %s：%s.%s=%d 的自然键 %q 不是本 seed 的自然键 %q: %w",
			label, table, pkCol, pk, storedKey, wantKey, ErrRetesterSeedHashCollision)
	}
	return nil
}

// arbitrateHashedRowForUpdate 串起 §3.2.1 路径 A 的三步：
// hash 当前读定位 → 物理 PK 持锁 → 持锁下读持久身份并完整字节仲裁。
// found=false 表示库里没有同 hash 的行（调用方走路径 B 的严格 INSERT）。
func arbitrateHashedRowForUpdate(ctx context.Context, tx *sql.Tx, label, table, pkCol, keyCol, identCol, hashCol string,
	wantIdentity, wantHash []byte, wantKey string, identityOf func(string) []byte) (int64, bool, error) {
	pk, found, err := locateHashedRowPKForUpdate(ctx, tx, table, pkCol, hashCol, wantHash)
	if err != nil || !found {
		return 0, false, err
	}
	if err := lockRowByPhysicalPK(ctx, tx, table, pkCol, pk); err != nil {
		return 0, false, err
	}
	// 白名单仲裁器（schema.VerifyHashArbitration）：物理 PK 锁已在手，这里只做
	// 持锁下的完整字节复核，它自身不 commit/rollback/重试。
	var wantHashSum [32]byte
	copy(wantHashSum[:], wantHash)
	if _, err := schema.VerifyHashArbitration(ctx, tx, table, pkCol, identCol, hashCol,
		wantHashSum, wantIdentity); err != nil {
		if errors.Is(err, schema.ErrIdentityHashCollision) {
			return 0, false, fmt.Errorf("retester seed: %s 身份仲裁失败: %w", label, ErrRetesterSeedHashCollision)
		}
		return 0, false, fmt.Errorf("retester seed: %s 仲裁: %w", label, err)
	}
	storedKey, storedIdentity, storedHash, err := readHashedRowIdentityForUpdate(ctx, tx, table, pkCol, keyCol, identCol, hashCol, pk)
	if err != nil {
		return 0, false, err
	}
	if err := verifyHashedRowIdentity(label, table, pkCol, pk, storedKey, storedIdentity, storedHash,
		wantIdentity, wantHash, wantKey, identityOf); err != nil {
		return 0, false, err
	}
	return pk, true, nil
}

// retesterSeedDuplicateIndex 从 1062 里**结构化**取出冲突索引名。
//
// go-sql-driver 的 *mysql.MySQLError 只带 Number/Message/SQLState，索引身份只存在于
// 消息的 `for key '<index>'` 片段里（TiDB 可能写成 `表名.索引名`，取最后一段）。
// 解析不出来时返回 ok=false —— 调用方必须 fail closed，绝不允许把"不知道撞在哪条
// 索引上的 1062"解释成"自然键已存在"（I2 的反例正来自这种吞异常）。
var retesterSeedDupKeyRe = regexp.MustCompile(`(?is)\bfor\s+key\s+['"]([^'"]+)['"]`)

func retesterSeedDuplicateIndex(err error) (string, bool) {
	var myErr *mysql.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1062 {
		return "", false
	}
	m := retesterSeedDupKeyRe.FindStringSubmatch(myErr.Message)
	if m == nil {
		return "", false
	}
	name := m[1]
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	if name == "" {
		return "", false
	}
	return name, true
}

// retesterSeedConflictOutcome 把一次 INSERT 的错误翻译成 seed 的处置：
//   - (retry=true)：1062 明确落在本 seed 的自然键索引 → 整笔回滚 + 新事务重放；
//   - error!=nil：1062 落在别的索引 / 无法归因 / 其他错误 → fail closed。
//
// 归因失败时**不能**把原始 *mysql.MySQLError 挂进 %w（编号 1062 会被 Runner 判成
// 可重试冲突并白白重放 5 次），只保留其文本。
func retesterSeedConflictOperation(action string, err error, wantIndex string) (bool, error) {
	index, ok := retesterSeedDuplicateIndex(err)
	if ok && index == wantIndex {
		return true, nil
	}
	if ok {
		return false, fmt.Errorf("%w（%s 撞在 %q，本 seed 只接受 %q）: %s",
			ErrRetesterSeedUnexpectedConflict, action, index, wantIndex, err.Error())
	}
	return false, fmt.Errorf("%w（%s 的 1062 无法归因到具体唯一索引）: %s",
		ErrRetesterSeedUnexpectedConflict, action, err.Error())
}

// —— 幂等 flag（settings，hash 唯一键 + 隐藏 row_id）——————————————————

// retesterSeedFlagRow 在写前仲裁下定位 settings 里的 flag 行。
// found=false 表示还没有这一行（或并发赢家尚未提交）。
func retesterSeedFlagRow(ctx context.Context, tx *sql.Tx) (int64, bool, error) {
	wantIdentity, wantHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	return arbitrateHashedRowForUpdate(ctx, tx, "settings 幂等标记", "settings", "row_id", "`key`", "key_identity", "key_hash",
		wantIdentity, wantHash, FindingRetesterSeedFlag, func(k string) []byte {
			id, _ := settingsKeyIdentity(k)
			return id
		})
}

// retesterSeedFlagSeededTx 幂等 flag 的**当前读**（I1 的正解）。
//
// 为什么必须 FOR UPDATE：TiDB release-8.5 默认悲观 RR，普通 SELECT 读事务开始前的
// 快照。若这里读的是旧快照，"赢家提交 flag → 用户删除 agent → 已排队者继续"这条
// 调度下，后来者会认定"从未 seed"，再因为它插的 agent 已被用户删除而不再撞 1062，
// 于是把用户删掉的 retester 连同默认模板一起复活。锁读读最新已提交版本，
// 这条窗口在 RR 与 RC 下都被关掉。
func retesterSeedFlagSeededTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	rowID, found, err := retesterSeedFlagRow(ctx, tx)
	if err != nil || !found {
		return false, err
	}
	// 行锁已在手；这一步同样是当前读，读到的就是赢家刚提交的 value。
	var value string
	if err := tx.QueryRowContext(ctx, "SELECT `value` FROM settings WHERE row_id = ? FOR UPDATE", rowID).Scan(&value); err != nil {
		return false, fmt.Errorf("retester seed: read %s: %w", FindingRetesterSeedFlag, err)
	}
	return value == retesterSeedFlagValue, nil
}

// markRetesterSeededTx 落幂等 flag。
//
// 不再用 ON DUPLICATE KEY UPDATE（I3）：ODKU 的更新分支不读持久身份，同 hash 异
// identity 的行会被直接改值，把无关配置污染成 true。正确协议是写前仲裁 + 二选一：
//   - 仲裁命中同一对象 → 按物理 row_id 精确 UPDATE，只碰 value/updated_at，
//     绝不改 key / key_identity / key_hash（身份列不可变）；
//   - 没有同 hash 的行 → 严格 INSERT（不带 ODKU），1062 归因后整体回滚重放。
func markRetesterSeededTx(ctx context.Context, tx *sql.Tx) error {
	rowID, found, err := retesterSeedFlagRow(ctx, tx)
	if err != nil {
		return err
	}
	if found {
		if _, err := tx.ExecContext(ctx,
			"UPDATE settings SET `value` = ?, updated_at = NOW(3) WHERE row_id = ?", retesterSeedFlagValue, rowID); err != nil {
			return fmt.Errorf("retester seed: update %s: %w", FindingRetesterSeedFlag, err)
		}
		return nil
	}
	wantIdentity, wantHash := settingsKeyIdentity(FindingRetesterSeedFlag)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO settings (`+"`key`"+`, key_identity, key_hash, `+"`value`"+`, updated_at)
VALUES (?, ?, ?, ?, NOW(3))`,
		FindingRetesterSeedFlag, wantIdentity, wantHash, retesterSeedFlagValue); err != nil {
		// I5：与 agents INSERT（insertRetesterAgentTx）严格对齐。只有结构化 1062
		// （tidb.IsDuplicateKey）才进入索引归因；1205 锁等待超时 / 1213 死锁 / context
		// 取消等非 1062 一律用 %w **原样上抛**，交给 TxRunner 按结构化编号
		// （lockorder.IsRetryableConflict）分类退避重试。
		// 绝不把可重试错误误包装成 ErrRetesterSeedUnexpectedConflict（旧实现直接对任意
		// 错误调 retesterSeedConflictOperation，且用 %s 丢弃底层错误码，把一次瞬时锁等待
		// 超时误判成"启动失败"，违反本文件顶层合同：1062/1205/1213 都触发整体回滚 + 重放）。
		if tidb.IsDuplicateKey(err) {
			retry, fatal := retesterSeedConflictOperation("settings 幂等标记 INSERT", err, retesterSeedSettingsIndex)
			if fatal != nil {
				return fatal
			}
			if !retry {
				return fmt.Errorf("retester seed: record %s: %w", FindingRetesterSeedFlag, err)
			}
			// 1062 落在 uq_settings_key_hash：定位快照已被并发赢家推翻。
			// 整笔回滚，由 TxRunner 在全新事务里重新定位 + 仲裁（§3.2.1 路径 B-3）。
			return fmt.Errorf("retester seed: %s 撞 %s 1062，整笔回滚重放: %w",
				FindingRetesterSeedFlag, retesterSeedSettingsIndex, lockorder.ErrApplicationRetry)
		}
		// 非 1062：1205/1213/ctx 等交由 TxRunner 分类；其它错误经 IsRetryableConflict
		// 判为不可重试后自然 fail closed（0 commit），语义与 agents INSERT 完全一致。
		return fmt.Errorf("retester seed: record %s: %w", FindingRetesterSeedFlag, err)
	}
	return nil
}

// —— agents / agent_prompts ————————————————————————————————————————

// insertRetesterAgentTx 插入 builtin=false 的可编辑 retester agent。
//
// 返回 inserted=false 表示**经写前身份仲裁确认**自然键已存在：既有行原样保留，
// 既不覆写它的任何列，也不为它追加 prompt 版本。
// 1062 只是并发信号，不是"同 key 已存在"的证据：落在 uq_agents_key_hash 上就整笔
// 回滚重放（下一轮走路径 A 仲裁），落在别处或无法归因一律 fail closed。
func insertRetesterAgentTx(ctx context.Context, tx *sql.Tx) (int64, bool, error) {
	wantIdentity, wantHash := agentKeyIdentity(FindingRetestAgentKey)
	pk, found, err := arbitrateHashedRowForUpdate(ctx, tx, "agents 自然键", "agents", "id", "`key`", "key_identity", "key_hash",
		wantIdentity, wantHash, FindingRetestAgentKey, func(k string) []byte {
			id, _ := agentKeyIdentity(k)
			return id
		})
	if err != nil {
		return 0, false, err
	}
	if found {
		return pk, false, nil
	}

	res, err := tx.ExecContext(ctx, `
INSERT INTO agents(`+"`key`"+`, key_identity, key_hash, name, description, role, builtin, enabled,
                           wrapup_prompt, task_timeout_wrapup_prompt, trigger_run_mode, trigger_merge_mode)
VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, FALSE, TRUE, '', '', 'serial', 'all')`,
		FindingRetestAgentKey, wantIdentity, wantHash, retesterAgentName, retesterAgentDescription, retesterAgentRole)
	if err != nil {
		if tidb.IsDuplicateKey(err) {
			retry, fatal := retesterSeedConflictOperation("agents INSERT", err, retesterSeedAgentIndex)
			if fatal != nil {
				return 0, false, fatal
			}
			if !retry {
				return 0, false, fmt.Errorf("retester seed: insert agent: %w", err)
			}
			return 0, false, fmt.Errorf("retester seed: agents 撞 %s 1062，整笔回滚重放: %w",
				retesterSeedAgentIndex, lockorder.ErrApplicationRetry)
		}
		return 0, false, fmt.Errorf("retester seed: insert agent: %w", err)
	}

	// 自增 id：先校验取值范围，再按唯一键锁读回查（M29 口径：判定依据是回读，
	// 不是 affected 行数），并且回读到的行必须通过同一条身份仲裁。
	// 少了这一步，任何把 LastInsertId 读成 0 或读到别人 id 的实现误差，都会把
	// current_prompt_id 指到别的模板上（agent_prompts 行则成为孤儿）。
	id, err := res.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("retester seed: agents LastInsertId: %w", err)
	}
	if id < 1 {
		return 0, false, fmt.Errorf("retester seed: agents auto-increment id %d out of range", id)
	}
	var readBack int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM agents WHERE key_hash = ? FOR UPDATE`, wantHash).Scan(&readBack); err != nil {
		return 0, false, fmt.Errorf("retester seed: read back agent id: %w", err)
	}
	if readBack != id {
		return 0, false, fmt.Errorf("retester seed: agent id 回读 %d 与自增 id %d 不一致", readBack, id)
	}
	storedKey, storedIdentity, storedHash, err := readHashedRowIdentityForUpdate(ctx, tx, "agents", "id", "`key`", "key_identity", "key_hash", id)
	if err != nil {
		return 0, false, err
	}
	if err := verifyHashedRowIdentity("agents 新插行", "agents", "id", id, storedKey, storedIdentity, storedHash,
		wantIdentity, wantHash, FindingRetestAgentKey, func(k string) []byte {
			got, _ := agentKeyIdentity(k)
			return got
		}); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// seedRetesterPromptTx 追加 version=1 的默认模板并把 agents.current_prompt_id 指过去。
//
// 只在「本事务刚插入 agent 行」之后调用：agent 行是全新的，uq_agent_prompts
// (agent_id,version) 不可能撞键，version 常量 1 是安全的。与 db.SavePrompt 同构，
// 但必须留在本事务内 —— 否则 prompt 与 flag 会分属两个事务（红线 3）。
// 父行就是本事务刚插入并锁过的 agents 行，无需再走 lockAgentForReference。
func seedRetesterPromptTx(ctx context.Context, tx *sql.Tx, agentID int64, promptText string) error {
	res, err := tx.ExecContext(ctx, `INSERT INTO agent_prompts(agent_id,version,template_text,note,updated_by)
VALUES (?, 1, ?, NULLIF(?, ''), NULLIF(?, ''))`, agentID, promptText, retesterPromptNote, retesterPromptUpdatedBy)
	if err != nil {
		return fmt.Errorf("retester seed: insert prompt: %w", err)
	}
	promptID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("retester seed: agent_prompts LastInsertId: %w", err)
	}
	if promptID < 1 {
		return fmt.Errorf("retester seed: agent_prompts auto-increment id %d out of range", promptID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agents SET current_prompt_id = ? WHERE id = ?`, promptID, agentID); err != nil {
		return fmt.Errorf("retester seed: point current_prompt_id: %w", err)
	}
	return nil
}
