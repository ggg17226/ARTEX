package db

// =====================================================================
// 任务分类（P08-02 Task 2.3，TiDB 方言）
//
// PG 用 `ON CONFLICT (nkey) DO NOTHING RETURNING *` 与 SQLSTATE 23505 表达"重名"。
// 新库的等价物是 **canonical identity 列 + hash 唯一索引**：
//   * `nkey_identity` MEDIUMBLOB：schema.NKeyIdentity(nkey) 的 canonical 字节；
//   * `nkey_hash` BINARY(32) 上有 `uq_task_categories_nkey_hash`（唯一）；
//   * 重名 → MySQL/TiDB **1062 ER_DUP_ENTRY**，映射为 ErrTaskCategoryNameConflict；
//   * 自增主键经 `LastInsertId()` 取得，整行随后回读（无 RETURNING）。
//
// 写者协议：`tasks.category_id` 是"给任务挂子实体（分类归属）"，因此
// SetTaskCategory / SetTasksCategory 都在业务门禁内先按 PK 升序锁住目标任务行并
// 核验存活，正在被级联删除的任务不会被并发写成半套分类归属。
// =====================================================================

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

const MaxTaskCategoryNameRunes = 80

// ---------------------------------------------------------------------------
// 测试屏障（默认关闭，生产零行为）
// ---------------------------------------------------------------------------
//
// categoryWriterHookFunc 在"引用分类的写者已经确认分类存活之后、真正写入
// tasks.category_id 之前"被调用。
//
// 为什么需要它：TiDB 基线不建外键，本文件的正确性完全押在"读分类 → 挂引用"这段
// 窗口内没人能把分类删掉。这个窗口在真机上只有几十微秒，任何 sleep 式并发测试都
// 只能碰运气复现，改错了也照样绿。测试在这里插一道显式屏障，才能**确定**把
// "写者已经读到分类"与"对手事务删除分类"钉成审查里那条交错。
//
// 契约：
//   - 默认永远为 nil（生产路径只是多一次 atomic.Pointer.Load）；
//   - 只允许 db 包内的测试经 SetTaskCategoryWriterTestHook 赋值；
//   - 回调在被测事务内部执行，回调自己**不得**再写同一个库；
//   - 用 atomic 存放，安装/卸载与并发读都不构成数据竞争。
type categoryWriterHookFunc func(stage string, categoryID int64)

var categoryWriterHook atomic.Pointer[categoryWriterHookFunc]

// SetTaskCategoryWriterTestHook 安装写者屏障；传 nil 卸载。
func SetTaskCategoryWriterTestHook(h categoryWriterHookFunc) {
	if h == nil {
		categoryWriterHook.Store(nil)
		return
	}
	categoryWriterHook.Store(&h)
}

// notifyTaskCategoryWriterHook 是无钩子时零成本的调用点包装。
func notifyTaskCategoryWriterHook(stage string, categoryID int64) {
	if hook := categoryWriterHook.Load(); hook != nil {
		(*hook)(stage, categoryID)
	}
}

// categoryDeleterHookFunc 在"删除分类者**已经进入共同 Business Gate**、还没开始定位
// 成员"时被调用（stage="in_gate"）。
//
// 为什么需要它（审查 M-R01）：本地那一半门禁是进程内互斥，不产生任何数据库锁，TiDB
// 事务视图本来就看不见"卡在门禁上"。只凭"窗口内没提交"下结论，与"线程还没被调度/卡在
// 别处"不可区分。这个探针给出的是可握手的因果事实：
//   - 对手停在门禁**之前** ⇒ 探针必然没有命中（这正是门禁生效）；
//   - 引用写者一放行 ⇒ 探针必须立刻命中，随后删除完成。
//
// 探针打在门禁**之后**，所以它一旦被命中就说明删除者已经越过门禁 —— 若此时引用写者还
// 停在屏障里，那本身就是"绕开共同门禁"的缺陷，测试可以直接判红。
//
// 契约与 categoryWriterHookFunc 完全一致：默认 nil、仅 db 包内测试可装、回调不得写库。
type categoryDeleterHookFunc func(stage string, categoryID int64)

var categoryDeleterHook atomic.Pointer[categoryDeleterHookFunc]

// SetTaskCategoryDeleterTestHook 安装删除者门禁探针；传 nil 卸载。
func SetTaskCategoryDeleterTestHook(h categoryDeleterHookFunc) {
	if h == nil {
		categoryDeleterHook.Store(nil)
		return
	}
	categoryDeleterHook.Store(&h)
}

// notifyTaskCategoryDeleterHook 是无钩子时零成本的调用点包装。
func notifyTaskCategoryDeleterHook(stage string, categoryID int64) {
	if hook := categoryDeleterHook.Load(); hook != nil {
		(*hook)(stage, categoryID)
	}
}

// ---------------------------------------------------------------------------
// 门禁阶段常量（复审 RR-M01）
// ---------------------------------------------------------------------------
//
// categoryGateQueuedStage 打在**产品入口之内、门禁申请之前**的最后一行：
// DeleteTaskCategory / SetTaskCategory 的下一步就是 runTaskWriteTx →
// lockorder.TxRunner.Run → GateManager.AcquireLocalBusiness（runAttempt 的第一件事），
// 因此这条信号的含义严格强于"goroutine 调用了产品函数"：
//   - 它已经进到产品实现内部；
//   - 它已经站在门禁的争用点上（此后任何阻塞都只可能发生在门禁申请 / 门禁之内）。
//
// 有了它，"已排队"与"根本没被调度 / 卡在别处"第一次变成可区分的两件事：
//   - 拿不到这条信号 ⇒ 观察器直接判红，绝不进入"窗口内没动静"那段判定；
//   - 拿到了、但 categoryInGateStage 一直没命中、也没返回 ⇒ 才是"扣在门禁上"。
//
// 反过来，旧实现把 started 信号发在**调用产品之前**，那条反例始终成立：
// 协程发完信号就在产品调用之前停调度，观察器 5 秒无进展照样报 held。
// 现在信号只能由产品实现自己在争用点上发出，这条反例在结构上不再可能。
const (
	categoryGateQueuedStage = "gate_queued"
	// categoryInGateStage 打在门禁**之后**（本地 + DB 两道都已到手）。
	categoryInGateStage = "in_gate"
	// categoryAfterLocateStage 打在"成员定位完成、物理行锁尚未开始"之间。
	categoryAfterLocateStage = "after_locate"
)

// MaxTaskCategoryBatchSize bounds one batch move so a single request cannot lock
// an unbounded number of task rows.
const MaxTaskCategoryBatchSize = 100

var (
	ErrTaskCategoryInvalid      = errors.New("invalid task category")
	ErrTaskCategoryNameConflict = errors.New("task category name already exists")
	ErrTaskCategoryNotFound     = errors.New("task category not found")
	ErrTaskCategoryTaskNotFound = errors.New("task not found")
)

// TaskCategory is a globally reusable task grouping label.
type TaskCategory struct {
	ID        int64     `json:"id,string"`
	Name      string    `json:"name"`
	NKey      string    `json:"-"`
	TaskCount int       `json:"task_count"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// taskCategoryCols 里的 FILTER (WHERE …) 换成 SUM(CASE WHEN …)（TiDB 无 FILTER 子句）；
// GROUP BY 带上全部非聚合列，兼容 ONLY_FULL_GROUP_BY。
const taskCategoryCols = `category.id, category.name, category.nkey,
       COALESCE(SUM(CASE WHEN task.deleted_at IS NULL THEN 1 ELSE 0 END), 0),
       category.created_at, category.updated_at`

const taskCategoryGroupBy = `GROUP BY category.id, category.name, category.nkey, category.created_at, category.updated_at`

func scanTaskCategory(row interface{ Scan(...any) error }) (TaskCategory, error) {
	var category TaskCategory
	err := row.Scan(&category.ID, &category.Name, &category.NKey, &category.TaskCount, &category.CreatedAt, &category.UpdatedAt)
	return category, err
}

func normalizeTaskCategoryName(name string) (string, string, error) {
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		return "", "", fmt.Errorf("%w: name is required", ErrTaskCategoryInvalid)
	}
	if utf8.RuneCountInString(name) > MaxTaskCategoryNameRunes {
		return "", "", fmt.Errorf("%w: name exceeds %d characters", ErrTaskCategoryInvalid, MaxTaskCategoryNameRunes)
	}
	return name, strings.ToLower(name), nil
}

// taskCategoryUniqueViolation 判定 1062（uq_task_categories_nkey_hash 冲突）。
// 判定必须是**结构化错误编号**，不能靠错误文本里是否出现 "1062"。
func taskCategoryUniqueViolation(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}

// readTaskCategory 按 id 回读一行分类（含实时 task_count）。
func readTaskCategory(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, id int64) (TaskCategory, error) {
	return scanTaskCategory(q.QueryRow(`
SELECT `+taskCategoryCols+`
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
WHERE category.id=?
`+taskCategoryGroupBy, id))
}

func (d *DB) CreateTaskCategory(name string) (*TaskCategory, error) {
	name, nkey, err := normalizeTaskCategoryName(name)
	if err != nil {
		return nil, err
	}
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	res, err := d.Exec(`
INSERT INTO task_categories(name, nkey, nkey_identity, nkey_hash, created_at, updated_at)
VALUES(?, ?, ?, ?, NOW(3), NOW(3))`, name, nkey, identity, hash)
	if taskCategoryUniqueViolation(err) {
		// uq_task_categories_nkey_hash 命中：与旧的 ON CONFLICT (nkey) DO NOTHING +
		// 空结果集完全同义。
		return nil, ErrTaskCategoryNameConflict
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	category, err := readTaskCategory(d, id)
	if err != nil {
		return nil, err
	}
	return &category, nil
}

func (d *DB) ListTaskCategories() ([]*TaskCategory, error) {
	rows, err := d.Query(`
SELECT ` + taskCategoryCols + `
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
` + taskCategoryGroupBy + `
ORDER BY category.name, category.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	categories := []*TaskCategory{}
	for rows.Next() {
		category, err := scanTaskCategory(rows)
		if err != nil {
			return nil, err
		}
		categories = append(categories, &category)
	}
	return categories, rows.Err()
}

func (d *DB) GetTaskCategory(id int64) (*TaskCategory, error) {
	category, err := readTaskCategory(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &category, nil
}

func (d *DB) RenameTaskCategory(id int64, name string) (*TaskCategory, error) {
	name, nkey, err := normalizeTaskCategoryName(name)
	if err != nil {
		return nil, err
	}
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	res, err := d.Exec(`
UPDATE task_categories
   SET name=?, nkey=?, nkey_identity=?, nkey_hash=?, updated_at=NOW(3)
 WHERE id=?`, name, nkey, identity, hash, id)
	if taskCategoryUniqueViolation(err) {
		return nil, ErrTaskCategoryNameConflict
	}
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// UPDATE 命中 0 行既可能是"行不存在"也可能是"值完全没变"，必须回读确认。
		if _, err := readTaskCategory(d, id); errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskCategoryNotFound
		} else if err != nil {
			return nil, err
		}
	}
	category, err := readTaskCategory(d, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskCategoryNotFound
	}
	if err != nil {
		return nil, err
	}
	return &category, nil
}

// DeleteTaskCategory moves affected tasks to the uncategorized bucket.
//
// PG 时代这一步由 tasks.category_id 上的 REFERENCES … ON DELETE SET NULL 顺带完成；
// TiDB 基线不建外键，级联必须在这里显式做掉，否则任务上会留下指向已删分类的悬空 id
// （分类列表里查不到它，任务却仍显示被归类）。删分类与清引用放同一个事务：只删一半
// 就是那种要等下一次列表刷新才暴露的脏数据。
//
// 全写者协议（Spec §3.2.2 / 审查 I-01 + I-R02，改这里前先读）：
//
//	① 共同 Business Gate（runTaskWriteTx）—— 与 CreateTask / SetTaskCategory /
//	   SetTasksCategory / 归档恢复以及任务级联删除共用同一道全局门禁；
//	② **fresh 无锁定位**成员任务（门禁之后、在外层事务之外的独立快照上读）——
//	   外层事务的快照可能早在拿到门禁**之前**就建立了，拿它当"新鲜"用等于用旧视图
//	   规划锁集合；无锁定位本身绝不加行锁，因此发现计划外成员也不会先去锁它；
//	③ 按**任务物理 PK 升序** FOR UPDATE（`WHERE id IN (…) ORDER BY id FOR UPDATE`
//	   走主键，加锁顺序确定；不用 `WHERE category_id=… FOR UPDATE`：TiDB 走 category_id
//	   二级索引扫描时按扫描顺序加锁，PK 升序不成立）；
//	④ 任务锁在手，才锁**分类物理行**（分类是任务持有的引用，出现序永远在任务之后；
//	   TableOrder tasks=1 < task_categories=51）；
//	⑤ 持锁做**新鲜成员核验**：再取一次 fresh 无锁定位，出现没被预锁到的成员就抛
//	   ErrMembershipDrift 整体回滚重放，严禁原地补锁；
//	⑥ 物理清理：只对**明确持锁**的 id 集合 `UPDATE … WHERE id IN (…)` 解除关联，
//	   然后删分类。**禁止** `UPDATE tasks SET category_id=NULL WHERE category_id=?`
//	   这种范围更新 —— 它是当前写，会按扫描顺序去锁计划外的低 PK 任务，等 RowsAffected
//	   发现时锁早已追加，"事后回滚"救不回全序（审查 I-R02 的核心）。
//
// 持分类行锁之后成员集合不可能再增长：全体引用写者（CreateTaskWithOptions /
// SetTaskCategory / SetTasksCategory / 归档恢复）都要先锁 task_categories 这一行才敢
// 挂引用，被挡在这里直到本次删除提交；它们随后拿到锁时读到的分类已不存在。
//
// 旧实现是裸 Begin、先 DELETE 分类再范围 UPDATE tasks、完全不过门禁：引用写者在
// "读到分类"与"写入 category_id"之间提交，就会留下没有外键兜底的悬空引用。
func (d *DB) DeleteTaskCategory(id int64) (bool, error) {
	if id <= 0 {
		return false, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
	}
	var deleted bool
	// 门禁排队信号（复审 RR-M01）：这是**产品入口之内**、门禁申请之前的最后一行 ——
	// runTaskWriteTx 的第一步就是 GateManager.AcquireLocalBusiness。测试据此才能把
	// "确实站在门禁争用点上排队"与"协程根本没跑/卡在别处"分开；没有这条信号，
	// 任何"窗口内没动静"都不许解读成"被门禁扣住"。
	notifyTaskCategoryDeleterHook(categoryGateQueuedStage, id)
	err := d.runTaskWriteTx(func(ctx context.Context, tx *sql.Tx) error {
		// 门禁探针：执行到这里 = 本地 + DB 两道门禁都已到手（见 SetTaskCategoryDeleterTestHook）。
		notifyTaskCategoryDeleterHook(categoryInGateStage, id)
		// ② fresh 无锁定位：本轮要清理的成员任务（不碰任何行锁）。
		candidates, err := locateTaskCategoryMembersFresh(ctx, d.DB, id)
		if err != nil {
			return err
		}
		// 定位/加锁之间的诊断探针（仅测试安装）：在这一刻把新任务挂到本分类上，就是在
		// "锁计划已经定了"与"成员集合还在长"之间制造审查 I-R02 的那次交错。
		notifyTaskCategoryDeleterHook(categoryAfterLocateStage, id)
		// ③ 按 PK 升序物理行锁 + 回读每行当前分类（锁定读即当前读）。
		locked, err := lockTaskRowsAscending(ctx, tx, candidates)
		if err != nil {
			return err
		}
		// ④ 任务已锁，现在才允许碰分类物理行。
		alive, err := lockTaskCategoryRow(ctx, tx, id)
		if err != nil {
			return err
		}
		if !alive {
			// 分类已被并发删除的赢家删掉：与"分类本就不存在"同义，不碰任何任务。
			return nil
		}
		// ⑤ 新鲜成员核验（无锁 fresh 读）：定位之后有外部任务挂到本分类上就是漂移，
		// 必须在**任何**解除关联 DML 之前上抛，由 TxRunner 整体回滚 + 退避重放。
		if err := verifyTaskCategoryMembership(ctx, d.DB, id, locked); err != nil {
			return err
		}
		// ⑥ 持锁当前读里仍然归属本分类的成员，就是本次解除关联的**全部**目标。
		members := make([]int64, 0, len(locked))
		for taskID, current := range locked {
			if current != nil && *current == id {
				members = append(members, taskID)
			}
		}
		sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
		cleared, err := clearTaskCategoryReferences(ctx, tx, members)
		if err != nil {
			return err
		}
		if cleared != int64(len(members)) {
			// 这些行都在本事务的 X 锁之下，计数对不上只可能是实现出了问题（漏锁/写错
			// 条件），不是并发语义；整体回滚，绝不留"分类已删、引用还在"的半态。
			return fmt.Errorf("%w: category %d cleared %d of %d pre-locked member rows",
				lockorder.ErrMembershipDrift, id, cleared, len(members))
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM task_categories WHERE id=?`, id); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// clearTaskCategoryReferences 对**已持锁**的任务 id 集合解除分类归属。
//
// 只按 id 更新，绝不按 category_id 范围更新：范围更新会按扫描顺序追加计划外行锁
// （审查 I-R02），把"任务 PK 升序"这条全序变成事后追认。
func clearTaskCategoryReferences(ctx context.Context, tx *sql.Tx, members []int64) (int64, error) {
	var cleared int64
	for start := 0; start < len(members); start += taskCategoryMemberLockChunk {
		end := min(start+taskCategoryMemberLockChunk, len(members))
		chunk := members[start:end]
		res, err := tx.ExecContext(ctx,
			`UPDATE tasks SET category_id=NULL, updated_at=NOW(3) WHERE id IN (`+inPlaceholders(len(chunk))+`)`,
			int64Args(chunk)...)
		if err != nil {
			return cleared, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return cleared, err
		}
		cleared += affected
	}
	return cleared, nil
}

// taskCategoryMemberLocator 构造"在独立新鲜事务里无锁定位分类成员"的定位器。
//
// 复用 lockorder.ExecuteFreshLocator，而不是自己拿 db 再发一条查询：本函数在 payload
// 内部执行，此刻外层事务已经占着一条连接，现成定位器负责三件本路径自己容易漏的事：
//   - 连接准入限流（DefaultLocatorAdmission）：不限流时并发 Run 会在共享池上 hold-and-wait；
//   - 独立事务开在**门禁到手之后**，并在使用完把连接干净归还（TiDB 拒绝 READ ONLY
//     声明时按其 noop 语义安全降级，快照新鲜度不受影响）；
//   - 候选集数量上限与 PK 升序去重，锁计划天然满足全序前置条件。
func taskCategoryMemberLocator(categoryID int64) lockorder.LocatorFunc {
	return func(ctx context.Context, tx *sql.Tx) (*lockorder.LocatorSnapshot, error) {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM tasks WHERE category_id=? ORDER BY id ASC`, categoryID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		snapshot := &lockorder.LocatorSnapshot{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			snapshot.CandidateKeys = append(snapshot.CandidateKeys,
				lockorder.EntityKey{Table: lockorder.TableOrderTasks, PK: uint64(id)})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return snapshot, nil
	}
}

// locateTaskCategoryMembersFresh 在**当前快照**上无锁定位分类的成员任务（PK 升序去重）。
//
// 为什么必须走 fresh locator 而不是外层 tx：TxRunner 先 Begin 再拿 DB Gate（tx_runner.go
// runAttempt），外层事务的快照可能早在门禁到手**之前**就已建立，用它定位成员等于拿
// 旧视图规划锁集合。定位器在另一条连接的新事务里读，看到的才是"此刻已提交"的成员集合。
//
// 为什么必须是**无锁**读：发现计划外成员的正确反应是"整体回滚重放"，而不是让定位
// 语句自己顺手把那些行锁掉（Spec 明令禁止"先逆序补锁、后报漂移"）。
func locateTaskCategoryMembersFresh(ctx context.Context, db *sql.DB, categoryID int64) ([]int64, error) {
	snapshot, err := lockorder.ExecuteFreshLocator(ctx, db, taskCategoryMemberLocator(categoryID))
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(snapshot.CandidateKeys))
	for _, key := range snapshot.CandidateKeys {
		if key.Table != lockorder.TableOrderTasks {
			continue
		}
		ids = append(ids, int64(key.PK))
	}
	return ids, nil
}

// verifyTaskCategoryMembership 在持锁之后做**新鲜成员核验**。
//
// 再取一次 fresh 无锁定位：出现不属于本轮锁计划的成员，说明定位与加锁之间有人把任务
// 挂到了本分类上。此刻一句 DML 都还没执行，直接抛 ErrMembershipDrift 让 TxRunner
// 整体回滚 + 退避重放（下一轮定位自然带上它）。
//
// 保守性：fresh 读之后对方可能又把手里的成员摘掉，这一次会误报漂移。误红的代价是一次
// 重放，漏红的代价是计划外行锁 —— 显然选前者。
func verifyTaskCategoryMembership(
	ctx context.Context,
	db *sql.DB,
	categoryID int64,
	locked map[int64]*int64,
) error {
	current, err := locateTaskCategoryMembersFresh(ctx, db, categoryID)
	if err != nil {
		return err
	}
	for _, taskID := range current {
		if _, ok := locked[taskID]; !ok {
			return fmt.Errorf("%w: category %d picked up member task %d after the lock plan was built; "+
				"appending locks in place is prohibited, full rollback and re-locate required",
				lockorder.ErrMembershipDrift, categoryID, taskID)
		}
	}
	return nil
}

// taskCategoryMemberLockChunk 限定单条加锁语句里的 PK 个数上界：分类下的任务数
// 没有业务上限，一条 IN 塞几万个占位符会把加锁语句本身变成瓶颈。
const taskCategoryMemberLockChunk = 200

// lockTaskRowsAscending 按**物理 PK 升序**对任务行 FOR UPDATE，并回读每行当前分类。
//
// 升序是硬要求：两个批量写者逆序锁同一批任务行会交叉等待死锁。
// FOR UPDATE 是 TiDB 下的当前读，返回的 category_id 因此可以用来做持锁成员复核；
// 后续那句普通 SELECT 才可能是旧快照，两者不能混用（审查 M-01 的区分）。
//
// 返回 map 的 value：nil 表示该行已不存在（物理被删），否则指向它当前的分类 id。
func lockTaskRowsAscending(ctx context.Context, tx *sql.Tx, ids []int64) (map[int64]*int64, error) {
	locked := make(map[int64]*int64, len(ids))
	for start := 0; start < len(ids); start += taskCategoryMemberLockChunk {
		end := min(start+taskCategoryMemberLockChunk, len(ids))
		chunk := ids[start:end]
		rows, err := tx.QueryContext(ctx,
			`SELECT id, category_id FROM tasks WHERE id IN (`+inPlaceholders(len(chunk))+`) ORDER BY id FOR UPDATE`,
			int64Args(chunk)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var (
				taskID  int64
				current sql.NullInt64
			)
			if err := rows.Scan(&taskID, &current); err != nil {
				rows.Close()
				return nil, err
			}
			if current.Valid {
				value := current.Int64
				locked[taskID] = &value
				continue
			}
			locked[taskID] = nil
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return locked, nil
}

// lockTaskCategoryRow 以当前读锁住分类物理行，返回它是否仍然存在。
//
// 这是"引用分类的写者"与"删除分类"共同的串行点：写者拿到分类行锁之前，
// 删除不可能提交；删除拿到分类行锁之前，写者也不可能把引用挂上去。
func lockTaskCategoryRow(ctx context.Context, tx *sql.Tx, categoryID int64) (bool, error) {
	var locked int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM task_categories WHERE id=? FOR UPDATE`, categoryID).Scan(&locked); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// requireTaskCategoryAlive 是引用写者挂分类前的**持锁**存活核验：普通 SELECT 在
// TiDB 下读本事务快照，看到"分类还在"完全可能是几分钟前的事实，必须锁住分类行
// 再判定（持锁期间删除无法提交，判定结果也就不会过期）。
func requireTaskCategoryAlive(ctx context.Context, tx *sql.Tx, categoryID int64) error {
	alive, err := lockTaskCategoryRow(ctx, tx, categoryID)
	if err != nil {
		return err
	}
	if !alive {
		return ErrTaskCategoryNotFound
	}
	return nil
}

// SetTaskCategory updates one live task. A nil category means uncategorized.
func (d *DB) SetTaskCategory(taskID int64, categoryID *int64) (*TaskCategory, error) {
	if categoryID == nil {
		var cleared bool
		err := d.runTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
			res, err := tx.ExecContext(ctx, `UPDATE tasks SET category_id=NULL, updated_at=NOW(3) WHERE id=?`, taskID)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			cleared = n > 0
			return err
		})
		if err != nil {
			return nil, mapTaskCategoryWriterGone(err)
		}
		if !cleared {
			return nil, ErrTaskCategoryTaskNotFound
		}
		return nil, nil
	}
	if *categoryID <= 0 {
		return nil, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
	}
	var out *TaskCategory
	// 门禁排队信号（复审 RR-M01，与删除侧对称）：下一行就是 runTaskChildWriter →
	// TxRunner.Run → AcquireLocalBusiness，此刻引用写者已经站在与 DeleteTaskCategory
	// 同一道门禁的争用点上。等待侧的"我确实在排队"由此成为可观测事实，
	// 而不是"发个信号再去调用产品"。
	notifyTaskCategoryWriterHook(categoryGateQueuedStage, *categoryID)
	err := d.runTaskChildWriter(taskID, func(ctx context.Context, tx *sql.Tx) error {
		// 分类必须存活。审查 I-01：这里以前是普通 SELECT，读的是本事务快照，
		// 并发删除能在"读到分类"与"UPDATE category_id"之间提交，挂出悬空引用。
		// 现在按共同锁序（任务已锁 → 再锁分类物理行）持锁核验：拿到分类行锁的期间
		// 删除无法提交，判定结果也就不会过期。
		if err := requireTaskCategoryAlive(ctx, tx, *categoryID); err != nil {
			return err
		}
		notifyTaskCategoryWriterHook("set_task_category", *categoryID)
		if _, err := tx.ExecContext(ctx,
			`UPDATE tasks SET category_id=?, updated_at=NOW(3) WHERE id=?`, *categoryID, taskID); err != nil {
			return err
		}
		category, err := readTaskCategoryTx(tx, *categoryID)
		if err != nil {
			return err
		}
		out = &category
		return nil
	})
	if err != nil {
		return nil, mapTaskCategoryWriterGone(err)
	}
	return out, nil
}

// readTaskCategoryTx 在事务内回读分类（readTaskCategory 的事务版）。
func readTaskCategoryTx(tx *sql.Tx, id int64) (TaskCategory, error) {
	return scanTaskCategory(tx.QueryRow(`
SELECT `+taskCategoryCols+`
FROM task_categories category
LEFT JOIN tasks task ON task.category_id=category.id
WHERE category.id=?
`+taskCategoryGroupBy, id))
}

// SetTasksCategory moves several tasks into one category (nil = uncategorized)
// inside a single transaction, so a half-applied batch is never observable.
// It returns the ids that were actually updated — ids missing from that slice
// were deleted between selection and submit — plus the refreshed category row
// whose task_count already reflects this move.
func (d *DB) SetTasksCategory(taskIDs []int64, categoryID *int64) ([]int64, *TaskCategory, error) {
	if len(taskIDs) == 0 {
		return nil, nil, fmt.Errorf("%w: task ids are required", ErrTaskCategoryInvalid)
	}
	if len(taskIDs) > MaxTaskCategoryBatchSize {
		return nil, nil, fmt.Errorf("%w: at most %d tasks per request", ErrTaskCategoryInvalid, MaxTaskCategoryBatchSize)
	}
	for _, id := range taskIDs {
		if id <= 0 {
			return nil, nil, fmt.Errorf("%w: task id must be positive", ErrTaskCategoryInvalid)
		}
	}
	if categoryID != nil && *categoryID <= 0 {
		return nil, nil, fmt.Errorf("%w: category id must be positive", ErrTaskCategoryInvalid)
	}

	var updated []int64
	var category *TaskCategory
	err := d.runTaskChildWriters(taskIDs, func(ctx context.Context, tx *sql.Tx, alive map[int64]bool) error {
		// 批量移动同样必须**持锁**确认分类存活（审查 I-01）：任务已按 PK 升序锁住，
		// 现在才锁分类物理行；普通 SELECT 会被并发删除插队成悬空引用。
		if categoryID != nil {
			if err := requireTaskCategoryAlive(ctx, tx, *categoryID); err != nil {
				return err
			}
			notifyTaskCategoryWriterHook("set_tasks_category", *categoryID)
		}
		// 已确认存活的行才写；已消失（正被级联删除）的 id 静默跳过，与原 RETURNING
		// 语义一致 —— 返回集合就是"真正被更新的那些"。
		targets := make([]int64, 0, len(taskIDs))
		for _, id := range taskIDs {
			if alive[id] {
				targets = append(targets, id)
			}
		}
		if len(targets) == 0 {
			return nil
		}
		rows, err := tx.QueryContext(ctx, `UPDATE tasks SET category_id=?, updated_at=NOW(3) WHERE id IN (`+
			inPlaceholders(len(targets))+`)`,
			append([]any{categoryID}, int64Args(targets)...)...)
		if err != nil {
			return err
		}
		// UPDATE 不返回行，改成"更新后逐行回读"以复现 RETURNING id 的语义。
		rows.Close()
		updated = append(updated, targets...)
		if categoryID != nil {
			fetched, err := readTaskCategoryTx(tx, *categoryID)
			if err != nil {
				return err
			}
			category = &fetched
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrTaskCategoryNotFound) {
			return nil, nil, err
		}
		return nil, nil, err
	}
	return updated, category, nil
}

// mapTaskCategoryWriterGone 保留原有对外错误语义（任务不存在/已删除）。
func mapTaskCategoryWriterGone(err error) error {
	if errors.Is(err, ErrTaskNotAlive) {
		return ErrTaskCategoryTaskNotFound
	}
	return err
}
