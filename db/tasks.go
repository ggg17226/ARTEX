package db

// =====================================================================
// 任务注册表与任务级联删除（P08-02 Task 2.3）
//
// 本文件的三条硬约束：
//  1. **没有任何表锁**：原 PG 的 `LOCK TABLE tasks/assets IN SHARE ROW EXCLUSIVE
//     MODE` 在 TiDB 上不存在，也不需要 —— 并发正确性由
//     `lockorder.TxRunner`（业务门禁 + 指数退避重试）+
//     `lockorder.LockEntitiesInOrder`（`(TableOrder, PK 升序)` 行级悲观锁）+
//     持锁成员漂移复核共同保证（M18 / M27 / M36）。
//  2. **写者协议**：任何"向任务追加子实体"的写者都必须经 `runTaskChildWriter`
//     —— 业务门禁 → 行锁父任务行 → 核验存活 → 才允许写子实体。正在被删除的任务
//     绝不会被并发挂上孤儿（I1）。
//  3. **占位符 `?`、自增主键 `LastInsertId()`**：TiDB 没有 `$n` 与 `RETURNING`。
// =====================================================================

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/Autumn-27/artex/db/schema"
)

// Task is a row in the task registry (1:1 with an exploration).
type Task struct {
	ID            int64      `json:"id"`
	Name          string     `json:"name"` // 可选任务名称;空=未命名
	CategoryID    *int64     `json:"category_id,omitempty"`
	CategoryName  string     `json:"category_name,omitempty"`
	Pinned        bool       `json:"pinned"`
	PinnedAt      *time.Time `json:"pinned_at,omitempty"`
	Description   string     `json:"description"`
	Goal          string     `json:"goal"`
	ExplorationID int64      `json:"exploration_id"`
	Status        string     `json:"status"`
	Paused        bool       `json:"paused"`
	Queued        bool       `json:"queued"`
	QueuedAt      *time.Time `json:"queued_at,omitempty"`
	QueueMode     string     `json:"queue_mode,omitempty"`
	LLMProfileID  *int64     `json:"llm_profile_id,omitempty"`
	// Task-level ordered LLM chain. LLMProfileID remains the compatibility alias
	// for ActiveLLMProfileID while older API clients still send one profile id.
	LLMProfileIDs      []int64    `json:"llm_profile_ids,omitempty"`
	ActiveLLMProfileID *int64     `json:"active_llm_profile_id,omitempty"`
	LLMChainRevision   int64      `json:"-"`
	LLMFailoverState   string     `json:"llm_failover_state,omitempty"`
	LLMFailoverReason  string     `json:"llm_failover_reason,omitempty"`
	SourceTaskIDs      []int64    `json:"source_task_ids,omitempty"`
	CompanyIDs         []int64    `json:"company_ids,omitempty"`
	ParentRef          string     `json:"parent_ref,omitempty"` // 父任务 id(编排 spawn 记录;空=顶层)
	CreatedAt          time.Time  `json:"created_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"` // 进入终态(done/failed/timeout)的时刻;非终态为 nil
	// 任务级超时(见 docs/任务级超时与收尾设计.md)。
	TimeoutSeconds int        `json:"timeout_seconds"`        // 0=不限时
	FirstRunAt     *time.Time `json:"first_run_at,omitempty"` // 首次真正开始运行的时刻(非 created_at);nil=尚未运行
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`  // = first_run_at + timeout_seconds;nil=不限或未运行
	// planner 心跳触发间隔(秒)：距上轮 plan 结束/任务开始满该值且期间无触发 → 触发一轮。
	// 下限=默认=300(5min)，低于一律抬到 300(在 CreateTask 归一)。见 docs/planner-trigger-impl-plan.md
	PlanHeartbeatSeconds int `json:"plan_heartbeat_seconds"`
	// CoverageEnabled 是「资产覆盖度功能」总开关(默认 true)。false 时：不计算/不展示测试
	// 覆盖度、不自动累积 task_scope(source=auto)、不给 agent 开放 add_task_scope/
	// list_untested_assets、态势里不注入 coverage 块(scope 字段仍保留)。company 关联
	// (task_scope kind=company)与此开关无关，永不受影响。见 db/task_scope.go。
	CoverageEnabled bool `json:"coverage_enabled"`
}

// TaskDeleteResult reports optional related-data cleanup performed in the same
// transaction as the task/exploration delete.
type TaskDeleteResult struct {
	AssetsDeleted     int64
	AssetsDetached    int64
	FindingsDeleted   int64
	LLMRecordsDeleted int64
}

// TaskDeletePreparation is produced inside the deletion transaction after the
// cascade has taken every physical lock. Prepare callbacks may use TrafficHosts
// to stage an external traffic deletion before TiDB commits; the locks (business
// gate + row locks on the whole physical side-effect closure) stay held through
// commit, so a host cannot become shared again after its traffic was staged.
type TaskDeletePreparation struct {
	ExplorationID int64
	TrafficHosts  []string
}

// ErrTaskStageUndoFailed 表示外部暂存（如 traffic / files）在单次尝试失败后撤销失败。
// 致命错误：外部写锁或暂存树可能处于不一致状态，严禁继续自动重试或静默吞掉（R4-a）。
var ErrTaskStageUndoFailed = errors.New("task cascade: failed to unwind staged external work after failed attempt")

// StageUndoError 包装外部暂存撤销失败，并实现 fatalUndo 接口通知 TxRunner 永不重试。
type StageUndoError struct {
	Err error
}

func (e *StageUndoError) Error() string          { return e.Err.Error() }
func (e *StageUndoError) Unwrap() error          { return e.Err }
func (e *StageUndoError) Is(target error) bool   { return target == ErrTaskStageUndoFailed }
func (e *StageUndoError) StageUndoFailed() bool { return true }

// TaskDeleteUndo undoes (and releases) the external work staged by **one single
// attempt** of the cascade. TxRunner replays the whole payload on a retryable
// conflict, so a stage that outlives its attempt would be re-entered while still
// holding its non-reentrant writer lock — a self-deadlock (R4).
//
// 契约（Spec §3.2.2、审查 R4）：
//   - 单次失败尝试的 undo 恰好执行一次，在退避重放前执行完毕；
//   - 若 undo 返回错误，必须包装 ErrTaskStageUndoFailed 并立即中止重试（不可重试终态）；
//   - panic 时无论是正常错误还是展开，必须同样触发 undo 释放外部写锁（R4-b）；
//   - 最终成功提交时，undo 自动注销，由调用方负责 Commit 阶段。
type TaskDeleteUndo func() error

// IsTerminal reports whether a task status is a terminal (finished) state.
// 单一真源，替换散落各处的 done/failed 硬编码判定。
func IsTerminal(status string) bool {
	return status == "done" || status == "failed" || status == "timeout"
}

// terminalStatusSQL 是终态集合的唯一字面量来源（TiDB 方言，无 $n）。
const terminalStatusSQL = `('done','failed','timeout')`

// =====================================================================
// 写者协议（Writer Protocol）
// =====================================================================

// ErrTaskNotAlive 表示父任务不存在或已被（软）删除：写者协议的唯一拒绝结果。
// 正在被级联删除的任务，其行锁会被删除事务持有到提交，写者要么排队、要么在它
// 提交后拿到"任务已消失"，两种情形都不可能造出孤儿子实体。
var ErrTaskNotAlive = errors.New("task writer: parent task is missing or deleted")

// ErrTaskDeleteOutcomeUnknown 表示删除事务的**提交结局未知**（连接断流 / 协议失步）。
// 此时门禁仍被封锁、既不能认定已提交也不能认定已回滚：调用方必须挂起进入人工对账，
// **绝不**执行外部补偿（traffic / 文件回滚或重放），否则可能把已删数据的外部证据
// 重新"复活"成不一致状态（I7 fail closed）。
var ErrTaskDeleteOutcomeUnknown = errors.New("task delete: commit outcome unknown, manual reconciliation required; external compensation MUST NOT run")

// taskTxRunner 构造任务写者统一使用的 TxRunner（默认退避阶梯 + 默认准入）。
func (d *DB) taskTxRunner() *lockorder.TxRunner {
	// 退避阶梯与重试上限由 TxRunner 默认值给出（100/250/500/1000ms，整体重放）。
	return lockorder.NewTxRunner(d.DB)
}

// lockTaskRowAlive 在事务内以行锁读取父任务行并核验其存活（写者协议第二步）。
// `FOR UPDATE` 同时是 TiDB 下的当前读：软删标记刚被别的事务写上时也必须看见。
func lockTaskRowAlive(ctx context.Context, tx *sql.Tx, taskID int64) (bool, error) {
	var alive bool
	if err := tx.QueryRowContext(ctx,
		`SELECT deleted_at IS NULL FROM tasks WHERE id = ? FOR UPDATE`, taskID).Scan(&alive); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return alive, nil
}

// runTaskChildWriter 是"向任务追加子实体"（资产、意图、分类、范围、拦截规则…）的
// 统一写者事务（计划 §3.2.2 写者协议 / Task 2.2 I1 屏障的落地入口）：
//
//	TxRunner.Run(RequiresBusinessGate: true)   // 全局业务门禁，排除并发级联删除
//	  → SELECT … FROM tasks WHERE id=? FOR UPDATE  // 行锁父任务
//	  → 核验 deleted_at IS NULL                     // 绝不向已删任务挂孤儿
//	  → fn(tx)                                      // 子实体写入
//
// 冲突（1062 / 1205 / 1213 / 成员漂移 / 应用级重试信号 lockorder.ErrApplicationRetry）
// 由 TxRunner **整体回滚 + 指数退避整体重放**；任何"部分写入 + 原地重试"都被禁止
// （payload 每次都拿到全新的 *sql.Tx）。
func (d *DB) runTaskChildWriter(taskID int64, fn func(ctx context.Context, tx *sql.Tx) error) error {
	if taskID <= 0 {
		return fmt.Errorf("%w: task id must be positive", ErrTaskNotAlive)
	}
	runner := d.taskTxRunner()
	return runner.Run(context.Background(), lockorder.TxRunnerOptions{RequiresBusinessGate: true},
		func(ctx context.Context, tx *sql.Tx, _ *lockorder.LockContext) error {
			alive, err := lockTaskRowAlive(ctx, tx, taskID)
			if err != nil {
				return err
			}
			if !alive {
				return fmt.Errorf("%w: task %d", ErrTaskNotAlive, taskID)
			}
			return fn(ctx, tx)
		})
}

// runTaskWriteTx 在业务门禁内执行一次"任务根"写事务（读改写 / 需要读回的场景）。
// 与 runTaskChildWriter 的区别：不预置父任务存活核验，由 payload 自己决定语义。
func (d *DB) runTaskWriteTx(fn func(ctx context.Context, tx *sql.Tx) error) error {
	runner := d.taskTxRunner()
	return runner.Run(context.Background(), lockorder.TxRunnerOptions{RequiresBusinessGate: true},
		func(ctx context.Context, tx *sql.Tx, _ *lockorder.LockContext) error {
			return fn(ctx, tx)
		})
}

// runTaskChildWriters 是批量版写者协议（分类批量移动等）：门禁 → **按 PK 升序**锁定
// 全部目标任务行 → 把"哪些行仍然存活"交给 payload。
//
// 升序锁是硬要求：两个批量写者以不同顺序锁同一批任务行会交叉等待死锁。
// payload 拿到 alive 后自行决定"消失的 id"是跳过还是报错 —— 与原
// `UPDATE … WHERE id=ANY(...) AND deleted_at IS NULL RETURNING id` 语义一致：
// 消失的行不出现在返回集合里，而不是让整笔批量失败。
func (d *DB) runTaskChildWriters(taskIDs []int64, fn func(ctx context.Context, tx *sql.Tx, alive map[int64]bool) error) error {
	if len(taskIDs) == 0 {
		return fmt.Errorf("%w: no task ids", ErrTaskNotAlive)
	}
	ordered := make([]int64, 0, len(taskIDs))
	seen := make(map[int64]struct{}, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			return fmt.Errorf("%w: task id must be positive", ErrTaskNotAlive)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	runner := d.taskTxRunner()
	return runner.Run(context.Background(), lockorder.TxRunnerOptions{RequiresBusinessGate: true},
		func(ctx context.Context, tx *sql.Tx, _ *lockorder.LockContext) error {
			rows, err := tx.QueryContext(ctx, `SELECT id, deleted_at IS NULL FROM tasks
WHERE id IN (`+inPlaceholders(len(ordered))+`) ORDER BY id FOR UPDATE`, int64Args(ordered)...)
			if err != nil {
				return err
			}
			alive := make(map[int64]bool, len(ordered))
			for rows.Next() {
				var id int64
				var ok bool
				if err := rows.Scan(&id, &ok); err != nil {
					rows.Close()
					return err
				}
				alive[id] = ok
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			return fn(ctx, tx, alive)
		})
}

// =====================================================================
// 创建
// =====================================================================

// CreateTask creates an exploration + task in one transaction and returns the task.
// timeoutSeconds is the task-level wall-clock budget (0 = 不限时); deadline_at is
// stamped later at first real run (see engine), not here.
// MinPlanHeartbeatSeconds 是 planner 心跳间隔的下限 = 默认 = 10min。
// 低于它(含缺省 0 / 负值 / 误配的小值)一律抬到 10min，防止把 planner 打爆。
const MinPlanHeartbeatSeconds = 600

// MaxTaskSourceCount bounds the amount of live inherited context one task can
// pull into every planner/main-agent prompt. Inheritance is intentionally direct
// only; keeping the fan-in bounded also prevents a single create request from
// multiplying graph and asset-context queries without limit.
const MaxTaskSourceCount = 8

// MaxTaskCompanyCount bounds the number of company asset scopes attached to a
// task. Company scopes are prompt context, and their currently attributed
// assets are snapshotted into the task at creation time.
const MaxTaskCompanyCount = 32

const taskCompanyAssetSource = "company"

var (
	ErrTaskCompanyIDsInvalid = errors.New("invalid task company ids")
	ErrTaskCompanyNotFound   = errors.New("task company not found")
)

// NormalizeTaskCompanyIDs validates IDs and removes duplicates while retaining
// the user's first-seen order.
func NormalizeTaskCompanyIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	seen := make(map[int64]struct{}, min(len(ids), MaxTaskCompanyCount))
	normalized := make([]int64, 0, min(len(ids), MaxTaskCompanyCount))
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("%w: company id must be positive", ErrTaskCompanyIDsInvalid)
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
		if len(normalized) > MaxTaskCompanyCount {
			return nil, fmt.Errorf("%w: got more than %d unique companies", ErrTaskCompanyIDsInvalid, MaxTaskCompanyCount)
		}
	}
	return normalized, nil
}

func normalizeHeartbeat(sec int) int {
	if sec < MinPlanHeartbeatSeconds {
		return MinPlanHeartbeatSeconds
	}
	return sec
}

func (d *DB) CreateTask(description, goal string, llmProfileID *int64, timeoutSeconds, planHeartbeatSeconds int) (*Task, error) {
	var ids []int64
	if llmProfileID != nil {
		ids = []int64{*llmProfileID}
	}
	return d.CreateTaskWithOptions(description, goal, TaskCreateOptions{
		LLMProfileIDs: ids, TimeoutSeconds: timeoutSeconds, PlanHeartbeatSeconds: planHeartbeatSeconds,
	})
}

// TaskCreateOptions contains the task data that must be committed atomically
// with the task/exploration row.
type TaskCreateOptions struct {
	Name                 string // 可选任务名称;空=未命名
	CategoryID           *int64
	SourceTaskIDs        []int64
	CompanyIDs           []int64
	LLMProfileIDs        []int64
	TimeoutSeconds       int
	PlanHeartbeatSeconds int
	// CoverageEnabled 是「资产覆盖度功能」开关;nil=默认开(true)，让不关心该开关的创建
	// 路径(编排 spawn、老 API)沿用原行为。仅 web 创建任务时可显式传 false 关闭。
	CoverageEnabled *bool
	// InterceptRules 是任务级资产拦截规则,创建时随任务在同一事务内写入 task_intercept_rules。
	InterceptRules []TaskInterceptRuleInput
}

// CreateTaskWithOptions creates an exploration, task, direct source relations,
// and the ordered task LLM chain in one transaction.
//
// 创建同样是"任务写者"：它与级联删除/子实体挂载共享同一把全局业务门禁，
// 因此不会出现"给一个正在被删除的任务登记 source 关系"这类交错（I1）。
func (d *DB) CreateTaskWithOptions(description, goal string, opts TaskCreateOptions) (*Task, error) {
	if len(opts.SourceTaskIDs) > MaxTaskSourceCount {
		return nil, fmt.Errorf("too many source tasks: got %d, maximum is %d", len(opts.SourceTaskIDs), MaxTaskSourceCount)
	}
	companyIDs, err := NormalizeTaskCompanyIDs(opts.CompanyIDs)
	if err != nil {
		return nil, err
	}
	opts.CompanyIDs = companyIDs
	if opts.TimeoutSeconds < 0 {
		opts.TimeoutSeconds = 0
	}
	opts.PlanHeartbeatSeconds = normalizeHeartbeat(opts.PlanHeartbeatSeconds)

	var created *Task
	if err := d.runTaskWriteTx(func(ctx context.Context, tx *sql.Tx) error {
		task, err := d.createTaskInTx(ctx, tx, description, goal, opts)
		if err != nil {
			return err
		}
		created = task
		return nil
	}); err != nil {
		return nil, err
	}
	return created, nil
}

// createTaskInTx 是单次创建尝试：TiDB 没有 RETURNING，自增主键一律 LastInsertId()，
// 状态/时间等由数据库默认值产出后再回读一次（同一事务内，读到的是自己的写入）。
func (d *DB) createTaskInTx(ctx context.Context, tx *sql.Tx, description, goal string, opts TaskCreateOptions) (*Task, error) {
	res, err := tx.ExecContext(ctx,
		`INSERT INTO explorations(description, goal, status, created_at, updated_at) VALUES(?, ?, 'open', NOW(3), NOW(3))`,
		description, goal)
	if err != nil {
		return nil, err
	}
	expID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	if expID <= 0 {
		return nil, fmt.Errorf("create exploration: invalid auto increment id %d", expID)
	}

	// origin fact: the exploration graph's root, a KindFact node (state='origin')
	// holding the original task description. Goals/intents/findings descend from
	// it, and the seeded target asset is anchored to it as lineage (the asset graph
	// is global and shared, not isolated per task).
	originPayload, _ := json.Marshal(map[string]any{
		"summary":     "任务起点：" + description + "；目标：" + goal,
		"description": description,
		"goal":        goal,
	})
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO exploration_nodes(exploration_id, kind, payload, priority, state, origin, created_at, updated_at)
VALUES(?, 'fact', ?, 0, 'origin', 'system', NOW(3), NOW(3))`, expID, string(originPayload)); err != nil {
		return nil, err
	}

	coverageEnabled := opts.CoverageEnabled == nil || *opts.CoverageEnabled
	var categoryName string
	if opts.CategoryID != nil {
		if *opts.CategoryID <= 0 {
			return nil, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
		}
		if err := tx.QueryRowContext(ctx, `SELECT name FROM task_categories WHERE id = ?`, *opts.CategoryID).
			Scan(&categoryName); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrTaskCategoryNotFound
			}
			return nil, err
		}
	}
	var active *int64
	if len(opts.LLMProfileIDs) > 0 {
		id := opts.LLMProfileIDs[0]
		active = &id
	}

	taskRes, err := tx.ExecContext(ctx, `
INSERT INTO tasks(name, category_id, description, goal, exploration_id, llm_profile_id, active_llm_profile_id,
                  timeout_seconds, plan_heartbeat_seconds, coverage_enabled, status, paused, queued, queue_mode,
                  created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'created', 0, 0, '', NOW(3), NOW(3))`,
		opts.Name, opts.CategoryID, description, goal, expID, active, active,
		opts.TimeoutSeconds, opts.PlanHeartbeatSeconds, boolToInt(coverageEnabled))
	if err != nil {
		return nil, err
	}
	taskID, err := taskRes.LastInsertId()
	if err != nil {
		return nil, err
	}
	if taskID <= 0 {
		return nil, fmt.Errorf("create task: invalid auto increment id %d", taskID)
	}

	t := &Task{
		ID: taskID, Name: opts.Name, CategoryID: opts.CategoryID, CategoryName: categoryName,
		Description: description, Goal: goal, ExplorationID: expID,
		LLMProfileID: active, ActiveLLMProfileID: active,
		LLMProfileIDs:  append([]int64(nil), opts.LLMProfileIDs...),
		SourceTaskIDs:  append([]int64(nil), opts.SourceTaskIDs...),
		CompanyIDs:     append([]int64(nil), opts.CompanyIDs...),
		Status:         "created",
		TimeoutSeconds: opts.TimeoutSeconds, PlanHeartbeatSeconds: opts.PlanHeartbeatSeconds,
		CoverageEnabled: coverageEnabled,
	}
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM tasks WHERE id = ?`, taskID).Scan(&t.CreatedAt); err != nil {
		return nil, err
	}
	if err := insertTaskRelations(ctx, tx, taskID, opts.SourceTaskIDs); err != nil {
		return nil, err
	}
	if err := insertTaskCompanies(ctx, tx, taskID, opts.CompanyIDs); err != nil {
		return nil, err
	}
	if err := insertTaskLLMProfiles(tx, taskID, opts.LLMProfileIDs); err != nil {
		return nil, err
	}
	if err := insertTaskInterceptRules(tx, taskID, opts.InterceptRules); err != nil {
		return nil, err
	}
	if len(opts.LLMProfileIDs) == 0 {
		t.LLMFailoverState = "default"
	} else {
		t.LLMFailoverState = "ready"
	}
	return t, nil
}

// insertTaskCompanies 把企业关联落进 task_scope（kind=company，带 canonical identity
// 唯一键），并完成 **assets.task_ids 与 task_asset_links 的原子双写**（M26 / M30）。
//
// PG 版本靠 `array_append(task_ids, $1)` + 触发器同步 link 表；TiDB 的 task_ids 是
// JSON 文本，双写必须由应用层显式完成：先锁住候选资产行，再"读旧值 → 合并 → 写回"，
// 最后 upsert link 行。任何一步失败整笔事务回滚，绝不留下"JSON 加了、link 没建"的半态。
func insertTaskCompanies(ctx context.Context, tx *sql.Tx, taskID int64, companyIDs []int64) error {
	if len(companyIDs) == 0 {
		return nil
	}
	// Company scope edits rebuild assets.company_id. Serialize the creation-time
	// snapshot with those edits so the task sees one committed attribution state.
	if err := lockCompanyScopeMutation(tx); err != nil {
		return err
	}

	ph := inPlaceholders(len(companyIDs))
	args := int64Args(companyIDs)
	var found int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM companies WHERE id IN (`+ph+`)`, args...).Scan(&found); err != nil {
		return err
	}
	if found != len(companyIDs) {
		return fmt.Errorf("%w: one or more companies do not exist", ErrTaskCompanyNotFound)
	}

	for _, companyID := range companyIDs {
		identity, hash := canonicalIdentityPair(schema.TaskScopeIdentity(
			uint64(taskID), "company", "", "", uint64(companyID), ""))
		if _, err := tx.ExecContext(ctx, `
INSERT INTO task_scope(task_id, kind, company_id, source, reason, created_at, identity, identity_hash)
VALUES(?, 'company', ?, 'manual', '任务创建时关联企业', NOW(3), ?, ?)
ON DUPLICATE KEY UPDATE identity = identity`,
			taskID, companyID, identity, hash); err != nil {
			return fmt.Errorf("insert task company scope %d: %w", companyID, err)
		}
	}

	// 资产双写：候选资产行先锁（与并发 detach / 级联删除互斥），再逐个合并 task_ids。
	rows, err := tx.QueryContext(ctx,
		`SELECT id, COALESCE(task_ids, '[]') FROM assets WHERE company_id IN (`+ph+`) ORDER BY id FOR UPDATE`, args...)
	if err != nil {
		return err
	}
	type assetTaskIDs struct {
		id    int64
		raw   string
		merge string
		added bool
	}
	targets := make([]assetTaskIDs, 0, 16)
	for rows.Next() {
		var id int64
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		merged, err := assetUnionTaskIDs(raw, []int64{taskID})
		if err != nil {
			rows.Close()
			return fmt.Errorf("asset %d task_ids: %w", id, err)
		}
		targets = append(targets, assetTaskIDs{id: id, raw: raw, merge: merged, added: merged != raw})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, target := range targets {
		if target.added {
			if _, err := tx.ExecContext(ctx,
				`UPDATE assets SET task_ids = ?, updated_at = NOW(3) WHERE id = ?`, target.merge, target.id); err != nil {
				return fmt.Errorf("attach asset %d to task %d: %w", target.id, taskID, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO task_asset_links(task_id, asset_id, source, source_summary, created_at, updated_at)
VALUES(?, ?, ?, ?, NOW(3), NOW(3))
ON DUPLICATE KEY UPDATE
    source=VALUES(source),
    source_summary=VALUES(source_summary),
    source_node_id=NULL,
    updated_at=NOW(3)`, taskID, target.id, taskCompanyAssetSource, companyScopeLinkSummary(tx, target.id)); err != nil {
			return fmt.Errorf("link asset %d to task %d: %w", target.id, taskID, err)
		}
	}
	return nil
}

// companyScopeLinkSummary 组装 link 行的来源说明（企业名可能已被并发清理，取不到就留空）。
func companyScopeLinkSummary(tx *sql.Tx, assetID int64) string {
	var name string
	_ = tx.QueryRow(`
SELECT COALESCE(c.name, '')
FROM assets a LEFT JOIN companies c ON c.id = a.company_id
WHERE a.id = ?`, assetID).Scan(&name)
	return "任务创建时关联企业：" + name
}

func insertTaskRelations(ctx context.Context, tx *sql.Tx, taskID int64, sourceIDs []int64) error {
	seen := map[int64]bool{}
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 || sourceID == taskID || seen[sourceID] {
			return fmt.Errorf("invalid or duplicate source task id %d", sourceID)
		}
		seen[sourceID] = true
		// 源任务必须存活；它的行也顺带锁住，杜绝"关系指向一个正在消失的任务"。
		var exists int64
		err := tx.QueryRowContext(ctx,
			`SELECT id FROM tasks WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, sourceID).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("source task %d not found", sourceID)
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO task_relations(task_id, source_task_id, created_at) VALUES(?, ?, NOW(3))`,
			taskID, sourceID); err != nil {
			return err
		}
	}
	return nil
}

func insertTaskLLMProfiles(tx *sql.Tx, taskID int64, profileIDs []int64) error {
	seen := map[int64]bool{}
	for position, profileID := range profileIDs {
		if profileID <= 0 || seen[profileID] {
			return fmt.Errorf("invalid or duplicate LLM profile id %d", profileID)
		}
		seen[profileID] = true
		if _, err := tx.Exec(`
INSERT INTO task_llm_profiles(task_id, profile_id, position, status, created_at, updated_at)
VALUES(?, ?, ?, 'ready', NOW(3), NOW(3))`, taskID, profileID, position); err != nil {
			return err
		}
	}
	return nil
}

// =====================================================================
// 读取与状态机
// =====================================================================

const taskCols = `id, COALESCE(name,''), category_id,
COALESCE((SELECT category.name FROM task_categories category WHERE category.id=tasks.category_id),''),
description, goal, exploration_id, status, paused, queued, queued_at, COALESCE(queue_mode,''), llm_profile_id, active_llm_profile_id, COALESCE(parent_ref,''), pinned_at, created_at, completed_at, COALESCE(timeout_seconds,0), COALESCE(plan_heartbeat_seconds,300), COALESCE(coverage_enabled,1), first_run_at, deadline_at`

func scanTask(sc interface{ Scan(...any) error }) (*Task, error) {
	var t Task
	if err := sc.Scan(&t.ID, &t.Name, &t.CategoryID, &t.CategoryName, &t.Description, &t.Goal, &t.ExplorationID, &t.Status, &t.Paused, &t.Queued, &t.QueuedAt, &t.QueueMode, &t.LLMProfileID, &t.ActiveLLMProfileID, &t.ParentRef, &t.PinnedAt, &t.CreatedAt, &t.CompletedAt, &t.TimeoutSeconds, &t.PlanHeartbeatSeconds, &t.CoverageEnabled, &t.FirstRunAt, &t.DeadlineAt); err != nil {
		return nil, err
	}
	t.Pinned = t.PinnedAt != nil
	return &t, nil
}

// SetParentRef records a task's parent task id (编排 agent spawn_task 关联).
func (d *DB) SetParentRef(id int64, parentRef string) error {
	var nullable any
	if strings.TrimSpace(parentRef) != "" {
		nullable = parentRef
	}
	_, err := d.Exec(`UPDATE tasks SET parent_ref = ?, updated_at = NOW(3) WHERE id = ?`, nullable, id)
	return err
}

// ListTasks returns alive tasks with pinned tasks first, then newest ids.
func (d *DB) ListTasks() ([]*Task, error) {
	// MySQL/TiDB 没有 NULLS LAST：用"未置顶优先 + pinned_at DESC + id DESC"等价表达。
	rows, err := d.Query(`SELECT ` + taskCols + ` FROM tasks WHERE deleted_at IS NULL
ORDER BY (pinned_at IS NULL), pinned_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := d.hydrateTasksContext(out); err != nil {
		return nil, err
	}
	return out, nil
}

// TaskPatch updates task list metadata without changing execution state.
type TaskPatch struct {
	Name   *string
	Pinned *bool
}

// UpdateTask applies a partial task name/pin mutation and returns the updated row.
// Re-pinning an already pinned task preserves its original position.
//
// TiDB 没有 UPDATE … RETURNING：读改写在业务门禁内一次完成（UPDATE + 同事务回读）。
func (d *DB) UpdateTask(id int64, patch TaskPatch) (*Task, error) {
	var updated *Task
	err := d.runTaskWriteTx(func(ctx context.Context, tx *sql.Tx) error {
		var pinnedArg any
		if patch.Pinned != nil {
			pinnedArg = boolToInt(*patch.Pinned)
		}
		res, err := tx.ExecContext(ctx, `
UPDATE tasks SET
	name = CASE WHEN ? = 1 THEN ? ELSE name END,
	pinned_at = CASE
		WHEN ? IS NULL THEN pinned_at
		WHEN ? = 1 THEN COALESCE(pinned_at, NOW(3))
		ELSE NULL
	END,
	updated_at = NOW(3)
WHERE id = ? AND deleted_at IS NULL`,
			boolToInt(patch.Name != nil), patch.Name, pinnedArg, pinnedArg, id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			var exists int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM tasks WHERE id = ?`, id).Scan(&exists); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil // 任务不存在 → 与原契约一致：(nil, nil)
				}
				return err
			}
			return nil // 存在但已软删 → 同样按"未更新"返回
		}
		task, err := scanTask(tx.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ?`, id))
		if err != nil {
			return err
		}
		updated = task
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// GetTask returns one alive task (nil if not found/deleted).
func (d *DB) GetTask(id int64) (*Task, error) {
	t, err := scanTask(d.QueryRow(`SELECT `+taskCols+` FROM tasks WHERE id=? AND deleted_at IS NULL`, id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := d.hydrateTaskContext(t); err != nil {
		return nil, err
	}
	return t, nil
}

// SetPaused persists a task's paused flag.
func (d *DB) SetPaused(id int64, paused bool) error {
	_, err := d.Exec(`UPDATE tasks SET paused=?, updated_at=NOW(3) WHERE id=?`, boolToInt(paused), id)
	return err
}

// Enqueue places a task at the tail of the persistent admission queue. Repeating
// the operation while it is already queued keeps its original FIFO position.
func (d *DB) Enqueue(id int64, mode string) error {
	if mode != "bootstrap" && mode != "resume" {
		return fmt.Errorf("invalid queue mode %q", mode)
	}
	_, err := d.Exec(`
UPDATE tasks
SET queued = 1,
    queued_at = CASE WHEN queued = 1 THEN COALESCE(queued_at, NOW(3)) ELSE NOW(3) END,
    queue_mode = CASE WHEN queue_mode = 'bootstrap' OR ? = 'bootstrap' THEN 'bootstrap' ELSE 'resume' END,
    updated_at = NOW(3)
WHERE id = ?`, mode, id)
	return err
}

// Dequeue removes the concurrency hold. clearMode is false when a user pauses a
// queued task, preserving whether its next admission must bootstrap or resume.
func (d *DB) Dequeue(id int64, clearMode bool) error {
	_, err := d.Exec(`
UPDATE tasks
SET queued = 0,
    queued_at = NULL,
    queue_mode = CASE WHEN ? = 1 THEN '' ELSE queue_mode END,
    updated_at = NOW(3)
WHERE id = ?`, boolToInt(clearMode), id)
	return err
}

// SetQueued is the compatibility helper used by older callers and tests. New
// scheduling code should use Enqueue/Dequeue so FIFO metadata is explicit.
func (d *DB) SetQueued(id int64, queued bool) error {
	if queued {
		return d.Enqueue(id, "bootstrap")
	}
	return d.Dequeue(id, true)
}

// SetStatus updates a task's lifecycle status. Entering a terminal state
// (done/failed/timeout) stamps completed_at once (COALESCE keeps the first stamp
// stable); moving back to a non-terminal state clears it, so a re-run has no stale
// finish time.
func (d *DB) SetStatus(id int64, status string) error {
	_, err := d.Exec(`
UPDATE tasks
   SET status = ?,
       completed_at = CASE WHEN ? IN `+terminalStatusSQL+` THEN COALESCE(completed_at, NOW(3)) ELSE NULL END,
       updated_at = NOW(3)
 WHERE id = ?`, status, status, id)
	return err
}

// SetTerminalStatusGuarded sets a terminal status only when the task is NOT already
// terminal, so a completed↔timeout race resolves to the first writer (won=true).
// Returns won=false (no error) when another terminal status already stuck — the
// caller then leaves it alone. Non-terminal transitions / re-run still use SetStatus.
func (d *DB) SetTerminalStatusGuarded(id int64, status string) (won bool, err error) {
	res, err := d.Exec(`
UPDATE tasks
   SET status = ?,
       completed_at = COALESCE(completed_at, NOW(3)),
       updated_at = NOW(3)
 WHERE id = ? AND status NOT IN `+terminalStatusSQL, status, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// StampFirstRun records a task's first-real-run moment and computes its absolute
// deadline (= now + timeoutSeconds). Idempotent: only stamps when first_run_at is
// still NULL, so restarts / re-entries keep the original clock. timeoutSeconds<=0
// leaves deadline_at NULL (不限时). Returns the resulting deadline (nil = 不限/未变).
func (d *DB) StampFirstRun(id int64, timeoutSeconds int) (*time.Time, error) {
	var deadline *time.Time
	err := d.execInTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
UPDATE tasks
   SET first_run_at = COALESCE(first_run_at, NOW(3)),
       deadline_at = CASE
           WHEN first_run_at IS NOT NULL THEN deadline_at
           WHEN ? > 0 THEN DATE_ADD(NOW(3), INTERVAL ? SECOND)
           ELSE NULL END,
       updated_at = NOW(3)
 WHERE id = ?`, timeoutSeconds, timeoutSeconds, id); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT deadline_at FROM tasks WHERE id = ?`, id).Scan(&deadline)
	})
	return deadline, err
}

// =====================================================================
// 级联删除（四阶段：Fresh Locator → 全序悲观锁 → 成员漂移复核 → 物理逆拓扑清理）
// =====================================================================

// TaskCascadeTestHookAfterSnapshot 是交付给测试的同步注入钩子（默认 nil）：
// 在**闭包快照完成之后、持锁成员复核之前**被同步调用一次。
// 用例借此在第二连接上提交一个"更低 PK 的新成员"，验收本轮尝试必须以
// lockorder.ErrMembershipDrift 整体回滚 + 退避重放（逆序补锁的实现会在此判红）。
// 钩子内不得持有本事务的锁，也不得 panic 吞错。
var TaskCascadeTestHookAfterSnapshot func(taskID uint64)

// DeleteTask preserves the historical behavior: remove the task and its
// exploration subgraph while retaining global assets.
func (d *DB) DeleteTask(id int64) error {
	_, err := d.DeleteTaskCascade(id, false, false)
	return err
}

// DeleteTaskCascade hard-deletes a task, its exploration subgraph and the full
// physical side-effect closure. Assets shared with another live task are only
// detached (task_ids 位剥离 + 本任务 link 删除)；shared rows survive.
func (d *DB) DeleteTaskCascade(id int64, deleteAssets, deleteFindings bool, deleteLLMRecords ...bool) (TaskDeleteResult, error) {
	deleteRecords := len(deleteLLMRecords) > 0 && deleteLLMRecords[0]
	return d.DeleteTaskCascadePrepared(id, deleteAssets, deleteFindings, deleteRecords, nil)
}

// DeleteTaskCascadePrepared coordinates reversible external deletion with the
// TiDB cascade. When prepare is non-nil, host ownership is resolved and prepare
// is invoked inside the deletion transaction **after** the whole physical
// side-effect closure has been locked, so a host cannot become shared after its
// traffic had already been staged.
//
// Attempt-level lifecycle (R4): the Runner replays the entire payload on a
// retryable conflict (1213/1205/1062/membership drift), which means prepare can
// be invoked more than once per call. prepare therefore hands back a
// TaskDeleteUndo that the cascade invokes as soon as — and exactly once for —
// the attempt that staged it failed: the stage is unwound and its writer lock
// released **before** the runner backs off, so the next attempt starts clean.
// The caller only finalizes (Commit) the stage of the attempt that finally
// committed.
//
// A stage is never unwound on the unknown-outcome path: when the returned error
// wraps ErrTaskDeleteOutcomeUnknown the commit result is undetermined, the gates
// stay held and no external compensation may run at all (manual reconciliation
// only). Errors raised by the payload itself can never mean "possibly committed"
// — the transaction never reached Commit — so unwinding there is always safe.
func (d *DB) DeleteTaskCascadePrepared(
	id int64,
	deleteAssets, deleteFindings, deleteLLMRecords bool,
	prepare func(TaskDeletePreparation) (TaskDeleteUndo, error),
) (TaskDeleteResult, error) {
	var result TaskDeleteResult
	if id <= 0 {
		return result, fmt.Errorf("delete task cascade: invalid task id %d", id)
	}
	opts := TaskClosureOptions{
		DeleteAssets:     deleteAssets,
		DeleteFindings:   deleteFindings,
		DeleteLLMRecords: deleteLLMRecords,
	}
	runner := d.taskTxRunner()
	err := runner.Run(context.Background(), lockorder.TxRunnerOptions{RequiresBusinessGate: true},
		func(ctx context.Context, tx *sql.Tx, lockCtx *lockorder.LockContext) error {
			out, err := d.runTaskCascadeAttempt(ctx, tx, lockCtx, id, opts, prepare)
			if err != nil {
				return err
			}
			result = out
			return nil
		})
	if err != nil {
		if errors.Is(err, lockorder.ErrOutcomeUnknown) {
			// 提交结局未知：门禁封锁、绝不重放，也绝不外部补偿。
			return result, fmt.Errorf("%w: %w", ErrTaskDeleteOutcomeUnknown, err)
		}
		return result, err
	}
	return result, nil
}

// runTaskCascadeAttempt 是删除级联的**一次完整尝试**（四阶段）。任何错误都让
// TxRunner 整体回滚，重试从阶段 1 的非加锁 Locator 重新开始。
//
// 本轮尝试若在 prepare 之后失败，defer 会先撤销/释放本轮暂存的外部工作再返回，
// 因此下一轮 attempt 的 prepare 永远面对"零残留"的外部状态（R4）。
func (d *DB) runTaskCascadeAttempt(
	ctx context.Context,
	tx *sql.Tx,
	lockCtx *lockorder.LockContext,
	id int64,
	opts TaskClosureOptions,
	prepare func(TaskDeletePreparation) (TaskDeleteUndo, error),
) (result TaskDeleteResult, err error) {
	var undo TaskDeleteUndo
	attemptCompleted := false
	defer func() {
		// 捕获 panic 并确保在展开前执行撤销释放外部写锁（R4-b）
		if p := recover(); p != nil {
			if undo != nil && !attemptCompleted {
				staged := undo
				undo = nil
				_ = staged()
			}
			panic(p)
		}
		if undo == nil || attemptCompleted {
			return
		}
		staged := undo
		undo = nil // 恰好一次：绝不重复撤销，也绝不留给下一轮
		if uerr := staged(); uerr != nil {
			// 撤销失败必须转化为不可重试的致命错误，中断 Runner 的后续自动重试（R4-a）
			err = &StageUndoError{
				Err: fmt.Errorf("%w: %v (underlying failure: %v)", ErrTaskStageUndoFailed, uerr, err),
			}
		}
	}()

	// ── 阶段 1：Fresh Locator（业务门禁保护下的非加锁闭包快照）──────────────
	snapshot, err := ComputeTaskClosureWithOptions(ctx, tx, uint64(id), opts)
	if err != nil {
		if errors.Is(err, ErrForeignKeyViolation) {
			return result, nil // 任务已不存在：与原契约一致（幂等成功）
		}
		return result, err
	}
	plan, err := snapshot.LockPlan()
	if err != nil {
		return result, err
	}

	// ── 阶段 2：全序悲观锁（(TableOrder, PK 升序)，含跨根物理副作用行）──────
	if err := lockorder.LockEntitiesInOrder(ctx, tx, lockCtx, plan); err != nil {
		return result, err
	}

	// 测试注入点：快照已成型、成员复核尚未开始。
	if TaskCascadeTestHookAfterSnapshot != nil {
		TaskCascadeTestHookAfterSnapshot(snapshot.TaskID)
	}

	// ── 阶段 3：成员漂移复核（第二条连接的一致性快照；**只发现，不补锁**）────
	// 复核读刻意不发出一条 FOR UPDATE：给计划外的新成员先加锁再比较，等于在旧最大
	// 锁序之后逆序补锁，可能"报漂移前先死锁"（R5）。发现差异一律整体回滚重放。
	var (
		locked  *TaskCascadeClosure
		current []lockorder.EntityKey
	)
	if err := d.relocateTaskClosureOnFreshSnapshot(ctx, uint64(id), opts, func(view DBTX) error {
		closure, err := ComputeArbitratedTaskClosure(ctx, view, uint64(id), opts)
		if err != nil {
			return err
		}
		keys, err := closure.MembershipKeys()
		if err != nil {
			return err
		}
		locked, current = closure, keys
		return nil
	}); err != nil {
		if errors.Is(err, ErrForeignKeyViolation) {
			return result, nil
		}
		return result, err
	}
	if err := lockorder.CompareMembership(plan, current); err != nil {
		// 新成员 / 更低 PK / 成员缺失 → 由 TxRunner 整体回滚 + 指数退避整体重放。
		return result, fmt.Errorf("task cascade membership check: %w", err)
	}

	// ── 流量主机归属：在锁内解析，交由 prepare 暂存可逆的外部删除 ───────────
	if prepare != nil {
		hosts, err := hostsForTaskDeletion(&txQuerier{tx}, int64(locked.TaskID), int64(locked.ExplorationID))
		if err != nil {
			return result, err
		}
		staged, err := prepare(TaskDeletePreparation{
			ExplorationID: int64(locked.ExplorationID),
			TrafficHosts:  hosts,
		})
		if err != nil {
			return result, err
		}
		undo = staged
	}

	// ── 阶段 4：物理逆拓扑清理（写集严格收敛于上面锁定的 PK 集合）──────────
	audit, err := ExecuteTaskCascade(ctx, tx, locked)
	if err != nil {
		return result, err
	}
	result.AssetsDeleted = audit.EntitiesDeleted["assets"]
	result.AssetsDetached = int64(len(locked.AssetIDsToDetach))
	result.FindingsDeleted = audit.EntitiesDeleted["findings"]
	result.LLMRecordsDeleted = audit.EntitiesDeleted["llm_records"]
	attemptCompleted = true
	return result, nil
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

// boolToInt 把布尔写入参数编成 TINYINT(1) 绑定值（TiDB 无 BOOLEAN 字面量参数）。
func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// int64Args 把 int64 列表编成 SQL 参数。
func int64Args(ids []int64) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// inPlaceholders 生成 n 个逗号分隔的 `?`。
func inPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// txQuerier 把 *sql.Tx 适配到只查询接口（hostsForTaskDeletion 等历史签名）。
type txQuerier struct{ tx *sql.Tx }

func (q *txQuerier) Query(query string, args ...any) (*sql.Rows, error) {
	return q.tx.Query(query, args...)
}

func (q *txQuerier) QueryRow(query string, args ...any) *sql.Row {
	return q.tx.QueryRow(query, args...)
}
