package db

// =====================================================================
// W5 主审查 I-01 / I-02 —— 分类删除与渠道删除的引用竞态（真实 TiDB）
//
// I-01：`DeleteTaskCategory` 走裸 Begin、先删分类再范围 UPDATE tasks，既不在共同
//
//	Business Gate 内，也不按任务物理 PK 升序预锁；CreateTask / SetTaskCategory /
//	SetTasksCategory 对分类又只是普通快照读。于是"写者读到分类 → 删除提交 → 写者
//	挂引用"能提交出悬空 category_id。
//
// I-02：`DeleteNotificationChannel` 与 `FanOutPendingEvents` 没有共同串行点：
//
//	fan-out 读出启用渠道后暂停，删除事务提交，fan-out 继续 INSERT 投递行，
//	留下指向已删渠道的孤儿投递（列表 JOIN 把它藏起来，队列与统计照算）。
//
// 四条硬要求，本文件逐条落实：
//  1. **确定交错**：暂停点是 db 包内的显式屏障（SetTaskCategoryWriterTestHook /
//     SetFanOutDeliveryTestHook，与既有 TaskCascadeTestHookAfterSnapshot 同一手法）；
//     "对手事务真的被挡住"用可观测事实证明，不接受宽泛 sleep：
//     — DB 侧能看见的等待（渠道行锁、门禁行锁）一律走 waitForBackendBlock，
//       必须在 cluster_tidb_trx 里看到 STATE='LockWaiting' 才算证据，视图不可用就 SKIP；
//     — 本地门禁是进程内互斥，DB 侧天然不可见，改用**通道握手**：删除者在"进入门禁之后"
//       装了探针（SetTaskCategoryDeleterTestHook），引用写者仍持门禁时探针必须一次不命中
//       （命中=绕开门禁，直接判红），写者放行后探针必须立刻命中（不命中=它刚才根本
//       没在门禁上排队）。
//  2. **断言产品不变量**，不绑定实现细节：分类删除后不得有任务引用它；渠道删除/停用
//     后不得有投递行引用它。红在数据上。
//  3. **只碰自己的夹具**（审查 I-R03 / 复审 RR-I01）：分类、任务、探索、渠道、事件、
//     投递都按本次运行的唯一标识建与删。任务按**物理 id** 登记进 tracker（含并发写者
//     成功创建的那一行），回收按 任务下游关联表 → 探索下游图 → tasks → explorations
//     逆拓扑执行，收尾逐表回读断言归零；库里既有数据一条不动。通知根（渠道 / 事件）
//     与归档七表夹具同样按"创建成功即登记 cleanup、删除错误一律判红、收尾按自有主键
//     回读归零"执行——回收失败还报绿，就是把残留转嫁给下一轮测试和真实 worker。
//  4. **绝不碰别人的通知数据**（审查 I-R04 / 复审 RR-I02）：**隔离做在 SQL 里**，
//     不拿"跑前 COUNT 一次""随机哨兵""事后断言"当保证 ——
//     ① 领取走同包受控入口 `fanOutOwnEventForChannels`（核心与生产 FanOutPendingEvents
//        完全共用，只是把归属钉死）：领取语句追加 `AND id = <自有事件>` ⇒ 这条
//        SELECT 在 SQL 层面就返回不了别人的事件；渠道集合裁剪到自有渠道；读到非自有
//        事件或命中非自有渠道都在**第一条 DML 之前**上抛 + 回滚。于是复审 §5.2 那条
//        轨迹（"测试 fan-out 恢复后把别人的 F 领走并标记 fanned_out"）不再可达，
//        共享库里有没有别人的 pending、有没有外部 dispatcher 都不改变这一点。
//     ② 夹具事件仍用**专用匹配规则**（finding_status_changed + 本次运行唯一的
//        vuln_class 哨兵）+ 测试渠道 on_status_change + 同一哨兵：它把"自有事件被
//        全局 dispatcher 投进别人渠道"的业务风险也一并压低，并在建夹具时逐渠道回读；
//        它**不是**隔离权（隔离靠 ①），所以既有渠道意外宽匹配时只告警、不再冒领。
//     ③ 跑完仍断言别人的事件一条没被分派、自己的事件没有一条投到别人的渠道 —— 这一层
//        现在只是**绊线**（证明 ① 的实现没被后人改坏），不再是唯一的防线。
//     Skip 与 cleanup 的挂载时序也在这里修好：goroutine 尚未启动时发生的跳过，
//     绝不去等一个永远不会关闭的"收尾"通道。
//  5. **归档队列不抢别人的活**（复审 RR-I03）：全局 ClaimTaskArchiveJob 会先把队首
//     UPDATE 成活跃态并 commit，再让调用方看见"领到的不是自己"。本文件的夹具一律改用
//     claimOwnArchiveJob —— 队首不是自己的 archive id 就**一条 UPDATE 都不发**，
//     直接回滚跳过。
//  6. **因果屏障不接受固定窗口**（审查 M-R01 / 复审 RR-M01）：等待侧"我确实排在目标
//     门禁上"由**产品自己在争用点发出的正向信号**证明 —— DeleteTaskCategory 在进入
//     runTaskWriteTx（它的第一件事就是 AcquireLocalBusiness）之前发 `gate_queued`，
//     SetTaskCategory 同序；观察器拿不到这条信号就直接判红，绝不用"5 秒没动静"充数。
//     DB 侧的 LockWaiting 还要核对等待会话所属表与本夹具 blocker 持有的行对得上
//     （物理表 id 拿不到 ⇒ 报错跳过，严禁静默放行），并另有一条"删除者持门禁时引用
//     写者进不来"的反向握手，两道合起来才叫同一道门禁。
// =====================================================================

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/notify"
)

// raceBudget 是所有"等某个确定状态出现"的等待上限：它是超时保护，不是时序假设。
const raceBudget = 20 * time.Second

// raceGateWindow 是"引用写者仍停在屏障里"这段时间内观察对手删除的判定窗口。
//
// 窗口本身**不是**证据，它只是一个失败保护上限（复审 M-R01：拿"5 秒没消息"当结论，
// 与"那个协程压根还没被调度"不可区分）。判定链条是三条正向事实拼起来的：
//  1. **门禁排队信号**（raceObserveDeleter 的 queued 通道）：这条信号**由产品实现自己在
//     门禁申请之前的最后一行发出**（task_categories.go 的 categoryGateQueuedStage），
//     不是测试在调用产品之前自己 close 一个通道。于是"已进到产品之内、且已站在门禁
//     争用点上"成为事实，复审 RR-M01 的反例（"发完信号就在产品调用之前停调度，观察器
//     5 秒无进展照样报 held"）在结构上不再可能。拿不到这条 ⇒ 直接判红；
//  2. 窗口内不得出现"当场提交"（绕开门禁/锁）或"门禁之后的探针命中"（绕开门禁）；
//     TiDB 侧若看到能**关联到本夹具 blocker** 的 LockWaiting，则升级为共同物理串行点的
//     直接证据（关联判定见 raceJudgeLockWait，缺证据不许算数）；
//  3. **放行后的握手**：门禁之后的探针必须命中、随后必须立刻跑完 —— 把"扣住"与
//     "根本没跑"分开。
//
// 本地门禁这一半是进程内互斥，不产生任何数据库锁，TiDB 事务视图本来就看不见它。
// "两道门禁真的共用"这句话由两个方向共同钉住：本文件的交错用例（写者持门禁 ⇒ 删除者
// 进不来）加 TestReferenceWriterBlockedWhileDeleterHoldsBusinessGate（**删除者持门禁
// 这一方可正向观测** ⇒ 写者进不来）。DB 侧可见的那一半由
// TestDeleteTaskCategoryIsInsideSharedBusinessGate 用门禁行 locks('business_relations')
// 的 LockWaiting（并按 raceLockWaitWitness 核对等待方与本夹具 blocker 的对应关系）直接证明。
const raceGateWindow = 5 * time.Second

// raceHookCall 是一次屏障命中记录。
type raceHookCall struct {
	stage string
}

// raceSessionConnections 是每条观察会话允许的物理连接上限：**2**。
//
// 为什么不能是 1：被测的 DeleteTaskCategory 在门禁内要用**独立 fresh 快照**定位成员
// （locateTaskCategoryMembersFresh 走 db 而非外层 tx，见 task_categories.go），这就是
// Spec 允许、并被 TxRunner 准入许可专门覆盖的"外层事务连接 + fresh locator 第二条连接"
// 模式。池上限压到 1 会让 fresh 定位永远排在尚未提交的外层事务后面 —— 自己等自己，
// 表现为 20s 超时而与并发正确性无关。
//
// 为什么 2 条仍然锁得住 pid：本文件所有被观察会话的时序都是 Begin（此刻池里只有
// Open 时建好的那一条连接，外层事务必然复用它）→ 才可能在 payload 里另取第二条。
// 于是"读到的 CONNECTION_ID"与"真正跑 DML 的会话"仍是同一条，锁等待证据不会假阴性。
const raceSessionConnections = 2

// raceOpenSession 打开一条**观察会话**（池上限见 raceSessionConnections），并返回它
// 外层事务所用连接的 CONNECTION_ID。
func raceOpenSession(t *testing.T, dsn string) (*DB, int) {
	t.Helper()
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("打开独立会话失败: %v", err)
	}
	sqlDB := d.DB
	sqlDB.SetMaxOpenConns(raceSessionConnections)
	sqlDB.SetMaxIdleConns(raceSessionConnections)
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("关闭独立会话失败: %v", err)
		}
	})
	var pid int
	if err := sqlDB.QueryRow(`SELECT CONNECTION_ID()`).Scan(&pid); err != nil {
		t.Fatalf("读取 CONNECTION_ID 失败: %v", err)
	}
	return d, pid
}

// raceGuard 记录"被测 goroutine 到底有没有真的启动过"（复审 RR-I02）。
//
// 旧写法是**先**注册"等 goroutine 收尾"的 cleanup，**再**去调用一个可能 t.Skip 的
// 隔离守卫。守卫跳过时 goroutine 根本还没启动、finished 永远不会关闭，cleanup 便白等
// raceBudget 再把一次诚实的 SKIP 打成 "fan-out 未收尾" 的失败。反过来先启动再注册
// cleanup 会留下"启动与注册之间 Fatal"的对称漏洞，因此这里把两件事绑成一个对象：
// cleanup 只登记一次，且只有 start() 真跑过才需要等收尾。
type raceGuard struct {
	label    string
	started  atomic.Bool
	finished chan struct{}
}

// newRaceGuard 建一个守卫并把 cleanup **立刻**挂上（在任何可能跳过/失败的步骤之前）。
func newRaceGuard(t *testing.T, label string) *raceGuard {
	g := &raceGuard{label: label, finished: make(chan struct{})}
	t.Cleanup(func() {
		if !g.started.Load() {
			// 用例在 goroutine 启动之前就 Skip/Fatal 了：没有任何东西需要收尾。
			return
		}
		select {
		case <-g.finished:
		case <-time.After(raceBudget):
			t.Errorf("%s未收尾", g.label)
		}
	})
	return g
}

// start 启动被测 goroutine；finished 无论 run 成功、失败还是 panic 都会关闭。
func (g *raceGuard) start(run func()) {
	g.started.Store(true)
	go func() {
		defer close(g.finished)
		run()
	}()
}

// ---------------------------------------------------------------------------
// 产品探针总线（复审 RR-M01）：把"产品实现在某个确定阶段真的跑过"变成可等待的信号
// ---------------------------------------------------------------------------
//
// 旧写法在这里是一个由**测试自己**在调用产品之前 close 的 started 通道。那条信号只
// 证明"goroutine 执行到产品调用之前那一行"，与"进到产品里、站在门禁争用点上"完全是
// 两回事，因此它撑不起"窗口内没动静 ⇒ 扣在共同门禁上"这个推论。
// 现在唯一的排队证据来自产品实现自己在门禁申请之前发出的 gate_queued（见
// task_categories.go 的 categoryGateQueuedStage），总线只是把它按 stage 分门别类接住。

// raceStageSignal 是**幂等**的一次性信号：产品探针第一次命中即 close，之后重复命中
// 无副作用。与"cap=1 的 buffered channel"不同，close 之后随时都能读到"已经发生过"，
// 因此不会出现"探针在两次轮询之间命中、观察器错过"的窗口。
type raceStageSignal struct {
	once sync.Once
	ch   chan struct{}
}

func newRaceStageSignal() *raceStageSignal {
	return &raceStageSignal{ch: make(chan struct{})}
}

func (s *raceStageSignal) fire() { s.once.Do(func() { close(s.ch) }) }

// C 供 select / range 等待。
func (s *raceStageSignal) C() <-chan struct{} { return s.ch }

// fired 非阻塞地回答"这个 stage 迄今有没有命中过"。
func (s *raceStageSignal) fired() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// raceProbeBus 按 stage 收集产品探针信号。stage 名字直接取产品常量
// （categoryGateQueuedStage / categoryInGateStage / …），拼错就是拿不到信号，
// 不会像字符串前缀匹配那样"看起来命中了"。
type raceProbeBus struct {
	mu     sync.Mutex
	stages map[string]*raceStageSignal
}

func newRaceProbeBus() *raceProbeBus {
	return &raceProbeBus{stages: map[string]*raceStageSignal{}}
}

// signal 取某个 stage 的信号（按需创建；永不为 nil）。
func (b *raceProbeBus) signal(stage string) *raceStageSignal {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.stages[stage]
	if !ok {
		s = newRaceStageSignal()
		b.stages[stage] = s
	}
	return s
}

// fired 非阻塞回答"产品是否已经跑到过这个 stage"。
func (b *raceProbeBus) fired(stage string) bool { return b.signal(stage).fired() }

// raceDeleterProbes 给删除者装上门禁探针，并把两个关键阶段分开接住：
//   - queued：产品入口之内、门禁申请之前（= 确实排在目标门禁上）；
//   - inGate：本地 + DB 两道门禁都已到手（在写者仍持门禁时命中 = 绕开门禁）。
//
// 返回的 beforeStage 是可选的**额外屏障**：需要在某个阶段把删除者停住时才传，
// 传 nil 表示探针只记账、不阻塞（交错用例的"被扣住一侧"正是这种）。
func raceDeleterProbes(t *testing.T, catID int64, beforeStage func(stage string)) *raceProbeBus {
	t.Helper()
	bus := newRaceProbeBus()
	SetTaskCategoryDeleterTestHook(func(stage string, categoryID int64) {
		if categoryID != catID {
			return
		}
		bus.signal(stage).fire()
		if beforeStage != nil {
			beforeStage(stage)
		}
	})
	t.Cleanup(func() { SetTaskCategoryDeleterTestHook(nil) })
	return bus
}

// raceWaitHook 等引用写者抵达屏障。写者提前返回（无论成败）立即判红：
// 那意味着根本没走到"已读分类 / 已读渠道"这一刻，交错没有建立。
func raceWaitHook(t *testing.T, label string, fired <-chan raceHookCall, writerDone <-chan error, wantStage string) {
	t.Helper()
	select {
	case call := <-fired:
		if call.stage != wantStage {
			t.Fatalf("%s：屏障命中的阶段是 %q，期望 %q", label, call.stage, wantStage)
		}
	case err := <-writerDone:
		t.Fatalf("%s：引用写者在抵达屏障前就返回了（err=%v），交错无法建立", label, err)
	case <-time.After(raceBudget):
		t.Fatalf("%s：%s 内没有等到引用写者抵达屏障", label, raceBudget)
	}
}

// raceJoin 带上限地取一次异步结果。
func raceJoin(t *testing.T, label string, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(raceBudget):
		t.Fatalf("%s 在 %s 内没有结束", label, raceBudget)
		return nil
	}
}

// raceDeleteOutcome 描述"引用写者仍停在屏障里时，对手删除做了什么"。
type raceDeleteOutcome string

const (
	// raceDeleteCommitted 当场提交：引用写者手里的门禁/锁根本没挡住它（I-01 的缺陷形态）。
	raceDeleteCommitted raceDeleteOutcome = "committed"
	// raceDeleteBlocked 被 TiDB 事务视图证明正在等 DB 行锁：共同物理串行点生效。
	raceDeleteBlocked raceDeleteOutcome = "blocked"
	// raceDeleteHeld 窗口内没提交、也没抵达门禁之后的探针：被写者手里的门禁扣住（本地
	// 门禁不产生 DB 锁，视图里不可见）。放行写者后它必须先抵达探针、再立刻跑完。
	raceDeleteHeld raceDeleteOutcome = "held"
	// raceDeleteGateBreached 引用写者还停在屏障里（仍持门禁），删除者却已经进到门禁
	// **之后**的探针：它绕开了共同 Business Gate。
	raceDeleteGateBreached raceDeleteOutcome = "gate-breached"
)

// raceWaitDeleterQueued 是观察的**开场**：只接受"产品自己在门禁申请之前发出的排队信号"。
//
// 单独成函数是为了能被**离线**表驱动反例覆盖（TestRaceWaitDeleterQueuedRejectsNoSignal）：
// 复审 RR-M01 的那条反例——"协程发完 started 就在产品调用之前停调度，观察器 5 秒无进展
// 照样报 held"——必须在这里就被判成错误，而不是等真库上撞运气。
// 返回 (false, nil) 只会出现在"删除者还没排队就已经提交完成"这一种结局（= 绕开门禁）。
func raceWaitDeleterQueued(queued <-chan struct{}, deleteDone <-chan error, budget time.Duration) (bool, error) {
	select {
	case <-queued:
		return true, nil
	case err := <-deleteDone:
		if err != nil {
			return false, fmt.Errorf("并发删除提前失败: %w", err)
		}
		// 连"站到门禁争用点上"都还没来得及报告就已经提交完成：与绕开门禁同义。
		return false, nil
	case <-time.After(budget):
		return false, fmt.Errorf("并发删除协程在 %s 内没有抵达目标门禁的争用点（产品未发出 %q 信号），"+
			"本次判定不成立（RR-M01 的假绿形态）", budget, categoryGateQueuedStage)
	}
}

// raceObserveDeleter 判定"引用写者仍持门禁/锁时，删除者去了哪里"。
//
// 第一步是**门禁排队信号**（复审 RR-M01 的核心，见 raceWaitDeleterQueued）：queued 由
// 产品实现自己在 `runTaskWriteTx`（第一件事 = AcquireLocalBusiness）之前那一行发出，
// 因此它同时证明"已经进到产品之内"与"已经站在目标门禁的争用点上"。测试不再自己 close
// 任何"我准备调用了"的通道 —— 那条旧信号既兼容"协程根本没被调度"，也兼容"停在产品调用
// 之前的任意一行"，拿它当结论就是复审点名的假绿。拿不到 queued ⇒ 直接判红。
//
// 之后观察三条可观测事实：
//  1. deleteDone 有值    → 它当场提交（缺陷形态，判红）；
//  2. entered 有值        → 它已经进到门禁之后的探针，而引用写者还持门禁（判红）；
//  3. TiDB 侧出现**能关联到本夹具 blocker**的 LockWaiting（raceJudgeLockWait 判定，
//     含 blocker 事务在场 + 被争用表 id 匹配）→ blocked，最强物理证据。
//
// 已排队 + 三条都没有 = 它停在**本地**门禁上（同进程互斥，不产生 DB 锁，事务视图里
// 天然不可见）—— 这就是 held，再由"放行后必须先命中探针、再立刻完成"补成因果闭环。
//
// wantTableID=0 表示本次拿不到被争用表的物理 id：此时**不再把任意 LockWaiting 认成
// blocked**（没有关联证据就不许宣称"等的是本夹具的锁"），只会继续观察到 held。
func raceObserveDeleter(
	t *testing.T,
	observer *DB,
	deleterPID, blockerPID int,
	deleteDone <-chan error,
	entered <-chan struct{},
	queued <-chan struct{},
	wantTableID int64,
) raceDeleteOutcome {
	t.Helper()
	enteredGate, err := raceWaitDeleterQueued(queued, deleteDone, raceBudget)
	if err != nil {
		t.Fatalf("%v", err)
		return raceDeleteHeld
	}
	if !enteredGate {
		// 没排队就已经返回：与绕开门禁同义。
		return raceDeleteCommitted
	}
	t.Logf("删除者已站在共同 Business Gate 的争用点上（产品探针 %q）", categoryGateQueuedStage)
	startedAt := time.Now()
	deadline := startedAt.Add(raceGateWindow)
	viewAvailable := false
	for {
		select {
		case err := <-deleteDone:
			if err != nil {
				t.Fatalf("并发删除提前失败: %v", err)
			}
			return raceDeleteCommitted
		case <-entered:
			// 引用写者还停在屏障里、门禁仍在它手上，删除者却已经进到门禁之后。
			return raceDeleteGateBreached
		default:
		}
		ev, available := raceProbeLockWait(observer, deleterPID, blockerPID)
		viewAvailable = viewAvailable || available
		if ok, reason := raceJudgeLockWait(ev, wantTableID); ok {
			t.Logf("删除者被证明在等本夹具的锁：%s", reason)
			return raceDeleteBlocked
		} else if ev.waiterState == "LockWaiting" {
			// 看到了锁等待，但关联证据不足：不许据此宣称"等的是本夹具那把锁"。
			t.Logf("检测到删除者 LockWaiting 但不作为夹具锁证据（%s）", reason)
		}
		if time.Now().After(deadline) {
			if !viewAvailable {
				t.Logf("提示：TiDB 事务视图不可用，本次只能凭门禁排队信号 + 门禁探针判定（pid=%d，已排队 %s）",
					deleterPID, time.Since(startedAt).Truncate(time.Millisecond))
			} else {
				t.Logf("删除者已在争用点排队 %s：未提交、未越过门禁探针、也没有可关联的 DB 锁等待 ⇒ 扣在本地共同门禁上（pid=%d）",
					time.Since(startedAt).Truncate(time.Millisecond), deleterPID)
			}
			return raceDeleteHeld
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// raceWaitDeleterEntered 等删除者抵达"门禁之后"的探针：引用写者已经放行，它必须出现。
// 迟迟不出现 = 它刚才并没有等在共同门禁上（而是没跑/卡在别处），判定不成立。
func raceWaitDeleterEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	releasedAt := time.Now()
	select {
	case <-entered:
		t.Logf("放行后删除者抵达门禁之后探针的耗时：%s", time.Since(releasedAt).Truncate(time.Millisecond))
	case <-time.After(raceBudget):
		t.Fatalf("放行引用写者后，删除者始终没有抵达门禁之后的探针：%s 内它并没有在共同门禁上排队", raceBudget)
	}
}

// ---------------------------------------------------------------------------
// 通知用例的自隔离（审查 I-R04 / 复审 RR-I02）
// ---------------------------------------------------------------------------

// raceLogForeignNotificationBacklog 在开跑前记录整库待分派集合，供报告判断"共享库上
// 是否有外部消费者在场"。它**不再是**门禁（复审 RR-I02 明确：跑前 COUNT 不是隔离权）。
//
// 隔离已经下推到 SQL：本文件的分派一律走 `fanOutOwnEventForChannels`，领取语句带
// `AND id = <自有事件>`，别人的 pending 无论多少、无论 id 多小，都不可能进入本次领取
// 结果，也就不可能被标记 fanned_out 或投递。于是"队里有别人的事件"从"必须跳过"
// 降级成"值得留痕"：它只影响一件事 —— 外部 dispatcher 可能抢先把**本夹具**那条事件
// 分派掉，使交错无法建立，由 raceRequireOwnEventPending 诚实跳过。
func raceLogForeignNotificationBacklog(t *testing.T, d *DB) {
	t.Helper()
	var pending, total int
	if err := d.QueryRow(`SELECT count(*), COALESCE(SUM(CASE WHEN NOT fanned_out THEN 1 ELSE 0 END), 0)
FROM notification_events`).Scan(&total, &pending); err != nil {
		t.Fatalf("统计全库事件待分派状态失败: %v", err)
	}
	t.Logf("环境留痕：全库事件 %d 条、待分派 %d 条（受控领取按 id 绑定自有事件，不碰这些行）", total, pending)
}

// raceNotifyFixture 是本用例**专用**的"渠道 + 事件"对（复审 RR-I02）。
type raceNotifyFixture struct {
	channel  *NotificationChannel
	eventID  int64
	sentinel string
	// snapshot 必须与 fan-out 事务里重建出来的那份完全一致：它读事件行的 snapshot
	// JSON、再拿行内 kind 覆盖 Kind 字段。
	snapshot notify.Snapshot
}

// newRaceNotifyFixture 建出一对"只有它们彼此能命中"的渠道与事件。
//
// 专用化做在**匹配规则**上（不改生产匹配逻辑，也不去动库里任何既有渠道）：
//  1. 事件类型取 finding_status_changed。notify.Match 对状态变更事件的契约是
//     "渠道没显式打开 on_status_change 就一律不推"，而库里既有渠道默认全关；
//  2. 事件的 vuln_class 是本次运行唯一的哨兵串；测试渠道再用自己的 filter
//     （on_status_change=true + vulnclass_include=[哨兵]）把它单独认下来。
//
// 两条合起来 ⇒ 既有渠道不可能命中本夹具事件，本夹具渠道也只认这一条事件。
// 建完立刻逐渠道回读验证（见 raceAssertEventMatchesOnlyFixtureChannel），
// 而不是先投进别人家再事后补删。
func newRaceNotifyFixture(t *testing.T, d *DB) *raceNotifyFixture {
	t.Helper()
	sentinel := raceFixturePrefix(t, "notify")
	filter := fmt.Sprintf(`{"on_status_change":true,"vulnclass_include":[%q]}`, sentinel)
	channel := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, filter)
	snapshot := notify.Snapshot{Severity: "high", VulnClass: sentinel}
	eventID := addTestEvent(t, d, notify.EventFindingStatusChanged, 0, snapshot)
	// 与 fan-out 侧重建的结果对齐：AddNotificationEvent 会把 Kind/FindingID 盖进
	// snapshot，fan-out 又会用行内 kind 再覆盖一次。
	snapshot.Kind = notify.EventFindingStatusChanged
	snapshot.FindingID = 0
	fixture := &raceNotifyFixture{channel: channel, eventID: eventID, sentinel: sentinel, snapshot: snapshot}
	fixture.assertOnlyOwnChannelMatches(t, d)
	return fixture
}

// assertOnlyOwnChannelMatches 把"这条事件只会投给我自己的渠道"从注释变成**运行时证据**：
// 用生产同一个匹配函数把全部启用渠道逐条比对一遍。两种结局的处置不同（复审 RR-I02）：
//   - 夹具渠道**不在**命中集合 ⇒ 交错根本无法建立（受控 fan-out 只会得到 0 条投递），
//     判红；
//   - 还有别人的渠道命中 ⇒ 说明库里存在宽匹配渠道。生产全局 fan-out 会把这条事件投进
//     别人家，所以夹具的哨兵匹配仍然重要；但本用例自己的分派已经被
//     `fanOutOwnEventForChannels` 的渠道白名单在写入前钉死，不会再依赖这里当隔离，
//     因此留一条显式告警而不是把测试打死（把它判红会让"共享库上安全复跑"永远做不到）。
func (f *raceNotifyFixture) assertOnlyOwnChannelMatches(t *testing.T, d *DB) {
	t.Helper()
	rows, err := d.Query(`SELECT id, filter FROM notification_channels WHERE enabled`)
	if err != nil {
		t.Fatalf("枚举启用渠道失败: %v", err)
	}
	var matched, total []int64
	for rows.Next() {
		var (
			id        int64
			rawFilter []byte
		)
		if err := rows.Scan(&id, &rawFilter); err != nil {
			rows.Close()
			t.Fatalf("读取启用渠道失败: %v", err)
		}
		total = append(total, id)
		if notify.Match(notify.ParseFilter(rawFilter), f.snapshot) {
			matched = append(matched, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("枚举启用渠道失败: %v", err)
	}
	own := false
	var foreign []int64
	for _, id := range matched {
		if id == f.channel.ID {
			own = true
			continue
		}
		foreign = append(foreign, id)
	}
	if !own {
		t.Fatalf("夹具渠道 %d 没有命中自己的夹具事件（命中集合=%v）：匹配规则已失效，交错无法建立",
			f.channel.ID, matched)
	}
	if len(foreign) > 0 {
		t.Logf("提示：共享库 %d 个启用渠道里另有 %v 也会命中本夹具事件。生产全局 fan-out 存在把它们投递出去的风险，"+
			"但本用例走的是受控领取（渠道白名单 + 事件 id 绑定），不会替这些渠道产生投递（RR-I02）",
			len(total), foreign)
	}
}

// raceRequireOwnEventPending 确认本夹具事件此刻仍是**待分派**状态。
//
// 它不是隔离机制（隔离在受控领取的 SQL 里），只回答"交错还能不能建立"：如果外部
// dispatcher 在本用例领取之前就把这条事件分派掉了，受控领取只会拿到 0 行，
// 用例观察不到任何有意义的事务交错。此时诚实 SKIP，并明确这是共享库上的外部消费者，
// 而不是本用例改写了别人的数据 —— 它一个字节都没动过。
func raceRequireOwnEventPending(t *testing.T, d *DB, eventID int64) {
	t.Helper()
	var own int
	if err := d.QueryRow(`SELECT count(*) FROM notification_events WHERE NOT fanned_out AND id = ?`, eventID).
		Scan(&own); err != nil {
		t.Fatalf("统计夹具事件的待分派状态失败: %v", err)
	}
	if own != 1 {
		t.Skipf("夹具事件 %d 在受控领取之前就不是待分派事件（pending=%d）：共享库上有外部消费者抢先分派，"+
			"本次交错无法建立。受控领取按 id 绑定，绝不可能因此去领别人的事件", eventID, own)
	}
}

// raceOwnedFanOutOutcome 把受控领取的返回计数分类成三态（纯函数，离线可测）。
//
// 受控入口（db/notification.go 的 fanOutOwnership）的结构保证：领取语句带
// `AND id = eventID` ⇒ 结果只可能是 0（自有事件已被外部消费者领走）或 1（自有事件）。
// 别的值只可能是 SQL 里的归属绑定被改坏 —— 那正是 RR-I02 的侵害前提。
func raceOwnedFanOutOutcome(events int) string {
	switch events {
	case 1:
		return "owned"
	case 0:
		return "stolen"
	default:
		return "violation"
	}
}

// raceAssertOwnedFanOutEvents 把 raceOwnedFanOutOutcome 变成用例结局。
//
// 渠道侧的越界由生产代码在第一条 INSERT 之前直接 error 上抛（这里无需再判），
// 渠道白名单生效与否由用例在屏障处直接读候选渠道集合断言。
func raceAssertOwnedFanOutEvents(t *testing.T, events int, eventID int64) {
	t.Helper()
	switch raceOwnedFanOutOutcome(events) {
	case "owned":
	case "stolen":
		t.Skipf("受控领取没有领到夹具事件 %d：外部消费者在它之前把它分派掉了，交错无法建立（本调用按 id 绑定，"+
			"不会去领任何别人的事件）", eventID)
	default:
		t.Fatalf("受控领取返回 %d 条事件：该入口按 `AND id = %d` 领取，只可能返回 0 或 1，"+
			"出现别的值说明领取语句的归属绑定失效（RR-I02 的侵害前提）", events, eventID)
	}
}

// 旧的 raceFanOutOwnEventLimit 已退役：它靠"全库计数 + 挑一个刚好不越界的 limit"求隔离，
// 而复审 RR-I02 指出那条路走不通（两次计数之间随时可能插进一条更早的 pending）。
// 现在由 fanOutOwnEventForChannels 在 SQL 里绑定 `AND id = ?` + 渠道白名单，
// limit 恒为 1，无需再算窗口。

// raceForeignEventSnapshot 是"夹具之外那些事件"的状态指纹。
type raceForeignEventSnapshot struct {
	total   int
	pending int
}

// raceSnapshotForeignEvents 记录夹具之外全部事件的事件数与待分派数。
func raceSnapshotForeignEvents(t *testing.T, d *DB, ownEventID int64) raceForeignEventSnapshot {
	t.Helper()
	var snap raceForeignEventSnapshot
	if err := d.QueryRow(`SELECT count(*), COALESCE(SUM(CASE WHEN NOT fanned_out THEN 1 ELSE 0 END), 0)
FROM notification_events WHERE id <> ?`, ownEventID).Scan(&snap.total, &snap.pending); err != nil {
		t.Fatalf("快照夹具之外的事件状态失败: %v", err)
	}
	return snap
}

// raceAssertForeignEventsUntouched 断言本轮 fan-out **一条都没碰**别人的事件：
// 事件行数不变、待分派数不变（少一条就说明有人替别人把事件分派掉了）。
func raceAssertForeignEventsUntouched(t *testing.T, d *DB, ownEventID int64, before raceForeignEventSnapshot) {
	t.Helper()
	after := raceSnapshotForeignEvents(t, d, ownEventID)
	if after.total != before.total {
		t.Errorf("夹具之外的事件行数从 %d 变成 %d：本用例动了别人的事件行", before.total, after.total)
	}
	if after.pending != before.pending {
		t.Errorf("夹具之外的事件待分派数从 %d 变成 %d：本用例分派了不属于自己夹具的事件（I-R04）",
			before.pending, after.pending)
	}
}

// raceAssertNoForeignDeliveries 从**两个方向**断言本轮 fan-out 没有越界：
//  1. 夹具渠道上不得出现"别人的事件"的投递（fan-out 领了不属于我的事件）；
//  2. 夹具事件名下不得出现"别人的渠道"的投递（我的事件被投进了共享库既有渠道）。
func raceAssertNoForeignDeliveries(t *testing.T, d *DB, channelID, ownEventID int64) {
	t.Helper()
	var foreignEvents int
	if err := d.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE channel_id = ? AND event_id <> ?`,
		channelID, ownEventID).Scan(&foreignEvents); err != nil {
		t.Fatalf("回读夹具渠道上的他人事件投递失败: %v", err)
	}
	if foreignEvents != 0 {
		t.Errorf("夹具渠道 %d 上出现 %d 条不属于本夹具事件的投递：fan-out 越界了", channelID, foreignEvents)
	}
	var foreignChannels int
	if err := d.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE event_id = ? AND channel_id <> ?`,
		ownEventID, channelID).Scan(&foreignChannels); err != nil {
		t.Fatalf("回读本夹具事件在他人渠道上的投递失败: %v", err)
	}
	if foreignChannels != 0 {
		t.Errorf("夹具事件 %d 被投到了 %d 个别人的渠道：夹具的匹配规则没有把事件独占下来（RR-I02）",
			ownEventID, foreignChannels)
	}
}

// ---------------------------------------------------------------------------
// 锁等待证据（复审 RR-M01）：把"某个会话在等锁"升级为"它在等**本夹具 blocker**
// 手里的那把锁"
// ---------------------------------------------------------------------------

// raceLockWaitEvidence 是一次 TiDB 事务视图探测的结果。
//
// waiterSeen 与"状态"分开记：视图里**根本没有 waiter 这一行**（会话没打开事务、
// 或观察连接落在别的 TiDB 实例上）与"有行但不是 LockWaiting"是两种不同的证据缺失，
// 报告里必须能区分，不能都写成"没等到锁等待"。
type raceLockWaitEvidence struct {
	waiterSeen      bool
	waiterState     string
	waiterWaiting   float64
	relatedTableIDs string
	blockerOpenTx   bool
	blockerState    string
}

// raceTableID 取本库某张表的 TiDB 物理表 id，用于把"等待方涉及的表"与"被争用的行"对上。
//
// **拿不到就是错误，绝不返回 0 蒙混**（复审 RR-M01 C）：旧写法在查询失败时返回 0，
// 而调用方的判定是 `if wantTableID > 0 && related != "" { … }`，0 恰好把整条关联校验
// 跳过 —— 于是"拿不到证据"静默退化成"任意 LockWaiting 都算"。现在由调用方明确处置：
// 要么据此诚实 SKIP（关联证据根本不可得），要么判红，没有第三条"继续给绿"的路。
func raceTableID(d *DB, table string) (int64, error) {
	var tableID int64
	if err := d.QueryRow(`SELECT TIDB_TABLE_ID FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&tableID); err != nil {
		return 0, fmt.Errorf("读取 %s 的 TIDB_TABLE_ID 失败: %w", table, err)
	}
	if tableID <= 0 {
		return 0, fmt.Errorf("%s 的 TIDB_TABLE_ID 不是正数（实际 %d）", table, tableID)
	}
	return tableID, nil
}

// raceTrxRow 是 information_schema.{cluster_,}tidb_trx 的一行（只取判定要用的四列）。
// 单独成型是为了让"证据 → 结论"这一步能被**离线表驱动**用例覆盖（复审 RR-M01：
// blocker 缺失、表 id 缺失、错表、错 blocker 这些反例必须在没有真库时也跑得到）。
type raceTrxRow struct {
	sessionID int64
	state     string
	waiting   float64
	related   string
}

// raceParseTrxRows 把视图行折叠成一次观测的证据结构。
//
// tidb_trx 只列出**持有打开事务**的会话：因此"blocker 出现在结果里"本身就等于
// "挡住它的那个事务此刻确实还在"。
func raceParseTrxRows(rows []raceTrxRow, waiterPID, blockerPID int) raceLockWaitEvidence {
	var ev raceLockWaitEvidence
	for _, row := range rows {
		switch row.sessionID {
		case int64(waiterPID):
			ev.waiterState, ev.waiterWaiting, ev.relatedTableIDs = row.state, row.waiting, row.related
			ev.waiterSeen = true
		case int64(blockerPID):
			ev.blockerOpenTx, ev.blockerState = true, row.state
		}
	}
	return ev
}

// raceProbeLockWait 非阻塞地探测一次事务视图。
//
// 它**只负责取数**，不负责下结论；结论一律出自 raceJudgeLockWait（纯函数，可离线测）。
// available=false 表示两个视图都查不动（表/列不存在或权限不足）。
func raceProbeLockWait(observer *DB, waiterPID, blockerPID int) (raceLockWaitEvidence, bool) {
	const cols = `SELECT SESSION_ID, STATE, COALESCE(WAITING_TIME, 0), COALESCE(RELATED_TABLE_IDS, '')`
	rows, err := observer.Query(cols+` FROM information_schema.cluster_tidb_trx WHERE SESSION_ID IN (?, ?)`,
		waiterPID, blockerPID)
	if err != nil {
		rows, err = observer.Query(cols+` FROM information_schema.tidb_trx WHERE SESSION_ID IN (?, ?)`,
			waiterPID, blockerPID)
		if err != nil {
			return raceLockWaitEvidence{}, false
		}
	}
	defer rows.Close()
	var parsed []raceTrxRow
	for rows.Next() {
		var row raceTrxRow
		if err := rows.Scan(&row.sessionID, &row.state, &row.waiting, &row.related); err != nil {
			return raceLockWaitEvidence{}, false
		}
		parsed = append(parsed, row)
	}
	if err := rows.Err(); err != nil {
		return raceLockWaitEvidence{}, false
	}
	return raceParseTrxRows(parsed, waiterPID, blockerPID), true
}

// raceJudgeLockWait 判定"waiter 正在等 **本夹具 blocker 手里那张表** 的锁"这一命题。
//
// 四条证据**全部必需**，任何一条不成立都返回 ok=false 并写清缺哪一条（复审 RR-M01 B/C：
// 旧实现把 `blockerOpenTx` 只写进失败日志，表 id 缺失时整段关联判定被跳过，等于
// "任意一次 LockWaiting 都能让测试过关"）：
//  1. waiter 在视图里可见，且 STATE='LockWaiting'；
//  2. blocker 会话此刻**自己也有一个打开的事务**（同视图可查）—— 否则这条等待与本夹具
//     的 blocker 没有任何关系；
//  3. 被争用表的物理 id 已知（wantTableID>0）。未知 ⇒ 直接不成立，绝不"降级匹配"；
//  4. waiter 的 RELATED_TABLE_IDS 非空且**包含**该表 id。空串意味着 TiDB 没给出关联，
//     同样不许当成"通过"。
//
// 边界说明（诚实标注证据强度）：RELATED_TABLE_IDS 只到**表级**，不含 index/主键与行
// 关键字，因此这里的结论是"waiter 正在等本夹具那张表上的锁，且 blocker 的事务确实还
// 在"，不是"waiter 精确在等 blocker 持有的那一行"。同表内并发到行的歧义在本夹具里
// 不存在（被争用的那一行由本用例独自造出并独自持有），但这句话说的是夹具性质，
// 不是这条视图列的能力，报告与注释都不许把它写成后者。
func raceJudgeLockWait(ev raceLockWaitEvidence, wantTableID int64) (bool, string) {
	switch {
	case !ev.waiterSeen:
		return false, "事务视图里没有 waiter 这一行（会话未进入打开事务，或视图未覆盖该实例）"
	case ev.waiterState != "LockWaiting":
		return false, fmt.Sprintf("waiter 状态是 %q，不是 LockWaiting", ev.waiterState)
	case !ev.blockerOpenTx:
		return false, "事务视图里没有 blocker 的打开事务：这条等待无法归因到本夹具的持有者"
	case wantTableID <= 0:
		return false, fmt.Sprintf("被争用表的物理 id 未知（wantTableID=%d），拒绝按任意 LockWaiting 下结论", wantTableID)
	case ev.relatedTableIDs == "":
		return false, "RELATED_TABLE_IDS 为空：TiDB 没有给出关联表，无法证明等的就是本夹具那张表"
	case !containsTableID(ev.relatedTableIDs, wantTableID):
		return false, fmt.Sprintf("RELATED_TABLE_IDS=%q 不含被争用表 %d", ev.relatedTableIDs, wantTableID)
	}
	return true, fmt.Sprintf("waiter 处于 LockWaiting 已 %.3fs，关联表 %s 含被争用表 %d，blocker 事务仍在场（state=%s）",
		ev.waiterWaiting, ev.relatedTableIDs, wantTableID, ev.blockerState)
}

// containsTableID 判断 RELATED_TABLE_IDS（形如 "[112, 108]"）里是否含某个表 id。
func containsTableID(related string, tableID int64) bool {
	for _, field := range strings.FieldsFunc(related, func(r rune) bool {
		return r < '0' || r > '9'
	}) {
		if parsed, err := strconv.ParseInt(field, 10, 64); err == nil && parsed == tableID {
			return true
		}
	}
	return false
}

// raceWaitBackendBlockOnFixture 证明 waiter **确实被本夹具 blocker 挡在锁上**。
//
// 与 waitForBackendBlock 同一条强证据契约（拿不到 LockWaiting 绝不放行），并把
// "等的是谁的锁"完全交给 raceJudgeLockWait（四条证据缺一不可：LockWaiting +
// blocker 事务在场 + 被争用表 id 已知 + RELATED_TABLE_IDS 命中该表）。
//
// 三种结局，互不混淆：
//   - nil：强证据成立；
//   - errNoBackendTrxView：两个事务视图都查不动 ⇒ 调用方诚实 SKIP（拿不到证据就不给结论）；
//   - 其他 error：视图可用但证据不足 ⇒ 调用方判红，并带上"缺哪一条"的原因。
//
// wantTableID 只允许由 raceMustTableID 提供（拿不到就直接失败/跳过），这里不再接受 0。
func raceWaitBackendBlockOnFixture(
	t *testing.T,
	observer *DB,
	waiterPID, blockerPID int,
	wantTableID int64,
	done <-chan error,
	label string,
) error {
	t.Helper()
	if wantTableID <= 0 {
		// 走到这里说明调用方跳过了"先拿表 id"这一步：这是测试自身的缺陷，直接失败。
		t.Fatalf("%s：调用方没有提供被争用表的物理 id（wantTableID=%d）；"+
			"缺这条关联证据就不许宣称 waiter 等的是本夹具的锁（RR-M01 C）", label, wantTableID)
	}
	deadline := time.Now().Add(raceBudget)
	viewAvailable := false
	var lastReason = "尚未完成第一次探测"
	for {
		if err := lockObservationProbe(done); err != nil {
			return fmt.Errorf("%s没有停在预期的锁上: %w", label, err)
		}
		ev, available := raceProbeLockWait(observer, waiterPID, blockerPID)
		viewAvailable = viewAvailable || available
		ok, reason := raceJudgeLockWait(ev, wantTableID)
		lastReason = reason
		if ok {
			t.Logf("%s：waiter pid=%d / blocker pid=%d —— %s", label, waiterPID, blockerPID, reason)
			return nil
		}
		if time.Now().After(deadline) {
			if !viewAvailable {
				return errNoBackendTrxView
			}
			return fmt.Errorf("%s：waiter pid=%d 在 %s 内没有拿到「等的是本夹具 blocker 的锁」的完整证据"+
				"（waiter=%q blocker在场=%v 涉及表=%q；%s）",
				label, waiterPID, raceBudget, ev.waiterState, ev.blockerOpenTx, ev.relatedTableIDs, lastReason)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// raceMustTableID 取被争用表的物理 id；拿不到就按调用点的约定诚实跳过本用例。
//
// 这里必须是 Skip 而不是"返回 0 继续跑"：0 会让 raceJudgeLockWait 恒假，最后要么把
// 一个本来正确的实现判红，要么（更糟）被后来的改动当成"跳过关联"的许可。
func raceMustTableID(t *testing.T, d *DB, table, usage string) int64 {
	t.Helper()
	tableID, err := raceTableID(d, table)
	if err != nil {
		t.Skipf("拿不到 %s 的 TiDB 物理表 id（%v）：%s 无法把锁等待关联到本夹具被争用的行，"+
			"本用例不降级成「任意 LockWaiting 都算」（RR-M01 C）", table, err, usage)
	}
	return tableID
}

// errNoBackendTrxView 表示 TiDB 事务视图不可用：调用方必须诚实 SKIP，不得降级成计时。
var errNoBackendTrxView = errors.New("tidb 事务视图不可用：拿不到锁等待的强证据")

// raceFanOutCounts 是一次 fan-out 的返回计数。
type raceFanOutCounts struct {
	events     int
	deliveries int
}

// raceFixturePrefix 生成本次运行**唯一**的夹具描述前缀（审查 I-R03）。
//
// 固定描述文本（旧实现里 Create 子例写死的 "race-writer-create"）是漏清的根源：
// cleanup 只按 tracker 自己的前缀枚举，撞不上这条固定文本，每成功一轮就在库里留下
// task + exploration + origin 节点三行查不到主人的孤儿；反过来"按固定文本全库删"又
// 会把别人的同名历史行一起删掉。唯一前缀 + 按物理 id 回收同时避开这两个坑。
func raceFixturePrefix(t *testing.T, label string) string {
	t.Helper()
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("生成夹具前缀随机数失败: %v", err)
	}
	return fmt.Sprintf("race-%s-%d-%s", label, time.Now().UnixNano(), hex.EncodeToString(buf[:]))
}

// raceTaskTracker 回收**本用例自己创建**的任务及其级联实体（审查 I-R03）。
//
// 两条硬要求：
//  1. 按**物理 id** 追踪，不靠描述文本：写者在屏障之后成功创建的那个 task 也必须进
//     回收集合 —— 只按 description 前缀枚举会整个漏掉它，末尾的"回读为 0"于是变成
//     假绿（审查指出的正是这个）；
//  2. **逆拓扑**清理：TiDB 基线不建外键，删 tasks 不会顺带清 exploration 与 origin
//     节点。顺序严格是 任务下游关联表 → 探索下游图 → tasks → explorations，
//     收尾必须逐表回读断言归零。
type raceTaskTracker struct {
	prefix string
	mu     sync.Mutex
	tasks  map[int64]int64 // task 物理 id → exploration 物理 id
}

// newRaceTaskTracker 建一个带唯一前缀的 tracker；label 只影响前缀可读性。
func newRaceTaskTracker(t *testing.T, label string) *raceTaskTracker {
	t.Helper()
	return &raceTaskTracker{prefix: raceFixturePrefix(t, label), tasks: map[int64]int64{}}
}

// track 记录一个由本用例创建成功的 task（并发 goroutine 里创建的同样适用）。
func (tr *raceTaskTracker) track(task *Task) {
	if task == nil {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.tasks[task.ID] = task.ExplorationID
}

// raceExecIn 按 id 集合分批执行一条以 `… IN (` 结尾的语句。
func raceExecIn(t *testing.T, d *DB, statement string, ids []int64) {
	t.Helper()
	const chunkSize = 200
	for start := 0; start < len(ids); start += chunkSize {
		end := min(start+chunkSize, len(ids))
		chunk := ids[start:end]
		query := statement + racePlaceholders(len(chunk)) + ")"
		if _, err := d.Exec(query, int64Args(chunk)...); err != nil {
			t.Errorf("回收夹具失败（%s…）: %v", statement, err)
		}
	}
}

// cleanup 逆拓扑回收本 tracker 名下的全部实体，并回读断言归零。
func (tr *raceTaskTracker) cleanup(t *testing.T, d *DB) {
	t.Helper()

	tr.mu.Lock()
	tasks := make(map[int64]int64, len(tr.tasks))
	for taskID, expID := range tr.tasks {
		tasks[taskID] = expID
	}
	tr.mu.Unlock()

	// 兜底枚举：每个成功创建的 task 都应当被 track 过。这里命中说明追踪本身有缺口
	// （审查 I-R03 的原始形态），必须判错，而不是静默当成普通残留清掉。
	rows, err := d.Query(`SELECT id, exploration_id FROM tasks WHERE description LIKE ?`, tr.prefix+"-%")
	if err != nil {
		t.Errorf("枚举夹具任务失败: %v", err)
		return
	}
	var untracked []int64
	for rows.Next() {
		var taskID, expID int64
		if err := rows.Scan(&taskID, &expID); err != nil {
			rows.Close()
			t.Errorf("读取夹具任务失败: %v", err)
			return
		}
		if _, ok := tasks[taskID]; !ok {
			untracked = append(untracked, taskID)
			tasks[taskID] = expID
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Errorf("枚举夹具任务失败: %v", err)
		return
	}
	if len(untracked) > 0 {
		t.Errorf("以下夹具任务没有被 tracker 记录（追踪缺口=%v）：I-R03 的漏清形态", untracked)
	}

	taskIDs := make([]int64, 0, len(tasks))
	expSeen := map[int64]bool{}
	expIDs := []int64{}
	for taskID, expID := range tasks {
		taskIDs = append(taskIDs, taskID)
		if expID > 0 && !expSeen[expID] {
			expSeen[expID] = true
			expIDs = append(expIDs, expID)
		}
	}
	sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })
	sort.Slice(expIDs, func(i, j int) bool { return expIDs[i] < expIDs[j] })

	// ① 任务下游：按 task 物理 id 精确删，绝不按描述文本模糊删。
	if len(taskIDs) > 0 {
		raceExecIn(t, d, `DELETE FROM task_llm_profiles WHERE task_id IN (`, taskIDs)
		raceExecIn(t, d, `DELETE FROM task_asset_links WHERE task_id IN (`, taskIDs)
		raceExecIn(t, d, `DELETE FROM task_scope WHERE task_id IN (`, taskIDs)
		raceExecIn(t, d, `DELETE FROM task_intercept_rules WHERE task_id IN (`, taskIDs)
		raceExecIn(t, d, `DELETE FROM findings WHERE task_id IN (`, taskIDs)
		// source 方向也要清：夹具可能把某个自己的 task 当别人的 source。
		raceExecIn(t, d, `DELETE FROM task_relations WHERE task_id IN (`, taskIDs)
		raceExecIn(t, d, `DELETE FROM task_relations WHERE source_task_id IN (`, taskIDs)
	}
	// ② 探索下游图（origin 节点就在这张表里）。anchors 只挂 node_id，必须先查出节点 id。
	if len(expIDs) > 0 {
		nodeRows, err := d.Query(`SELECT id FROM exploration_nodes WHERE exploration_id IN (`+
			racePlaceholders(len(expIDs))+`)`, int64Args(expIDs)...)
		if err != nil {
			t.Errorf("枚举夹具节点失败: %v", err)
			return
		}
		nodeIDs := []int64{}
		for nodeRows.Next() {
			var nodeID int64
			if err := nodeRows.Scan(&nodeID); err != nil {
				nodeRows.Close()
				t.Errorf("读取夹具节点失败: %v", err)
				return
			}
			nodeIDs = append(nodeIDs, nodeID)
		}
		nodeRows.Close()
		if err := nodeRows.Err(); err != nil {
			t.Errorf("枚举夹具节点失败: %v", err)
			return
		}
		raceExecIn(t, d, `DELETE FROM exploration_edges WHERE exploration_id IN (`, expIDs)
		raceExecIn(t, d, `DELETE FROM task_constraints WHERE exploration_id IN (`, expIDs)
		raceExecIn(t, d, `DELETE FROM activity WHERE exploration_id IN (`, expIDs)
		if len(nodeIDs) > 0 {
			raceExecIn(t, d, `DELETE FROM exploration_anchors WHERE node_id IN (`, nodeIDs)
			raceExecIn(t, d, `DELETE FROM findings WHERE node_id IN (`, nodeIDs)
			raceExecIn(t, d, `DELETE FROM exploration_nodes WHERE id IN (`, nodeIDs)
		}
	}
	// ③ 最后才是 tasks 与 explorations 本身。
	if len(taskIDs) > 0 {
		raceExecIn(t, d, `DELETE FROM tasks WHERE id IN (`, taskIDs)
	}
	if len(expIDs) > 0 {
		raceExecIn(t, d, `DELETE FROM explorations WHERE id IN (`, expIDs)
	}

	// ④ 回读归零：task / exploration / origin 节点三张表都要验，只查 tasks 是不够的。
	var leftTasks, leftExps, leftNodes int
	if err := d.QueryRow(`SELECT count(*) FROM tasks WHERE description LIKE ?`, tr.prefix+"-%").Scan(&leftTasks); err != nil {
		t.Errorf("回读夹具任务失败: %v", err)
		return
	}
	if leftTasks != 0 {
		t.Errorf("夹具任务未归零，残留 %d 行（前缀 %s）", leftTasks, tr.prefix)
	}
	if len(taskIDs) > 0 {
		if err := d.QueryRow(`SELECT count(*) FROM tasks WHERE id IN (`+racePlaceholders(len(taskIDs))+`)`,
			int64Args(taskIDs)...).Scan(&leftTasks); err != nil {
			t.Errorf("回读夹具任务失败: %v", err)
			return
		}
		if leftTasks != 0 {
			t.Errorf("夹具任务未按 id 归零，残留 %d 行", leftTasks)
		}
	}
	if len(expIDs) > 0 {
		if err := d.QueryRow(`SELECT count(*) FROM explorations WHERE id IN (`+racePlaceholders(len(expIDs))+`)`,
			int64Args(expIDs)...).Scan(&leftExps); err != nil {
			t.Errorf("回读夹具探索失败: %v", err)
			return
		}
		if leftExps != 0 {
			t.Errorf("夹具探索未归零，残留 %d 行", leftExps)
		}
		if err := d.QueryRow(`SELECT count(*) FROM exploration_nodes WHERE exploration_id IN (`+
			racePlaceholders(len(expIDs))+`)`, int64Args(expIDs)...).Scan(&leftNodes); err != nil {
			t.Errorf("回读夹具节点失败: %v", err)
			return
		}
		if leftNodes != 0 {
			t.Errorf("夹具 exploration 节点（含 origin）未归零，残留 %d 行", leftNodes)
		}
	}
}

// racePlaceholders 生成 n 个逗号分隔的 ? 占位符。
func racePlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// raceCreateTask 用夹具前缀建一个任务（categoryID 为 nil 表示不分类），并登记进 tracker。
func raceCreateTask(t *testing.T, d *DB, tracker *raceTaskTracker, label string, categoryID *int64) int64 {
	t.Helper()
	task, err := d.CreateTaskWithOptions(tracker.prefix+"-"+label, "goal", TaskCreateOptions{CategoryID: categoryID})
	if err != nil {
		t.Fatalf("建夹具任务 %s 失败: %v", label, err)
	}
	tracker.track(task)
	return task.ID
}

// TestCategoryDeleteRaceAgainstReferenceWriters 复现审查 I-01 的两种交错：
//
//	引用写者 A 在"确认分类存活"与"写入 tasks.category_id"之间被显式屏障停住
//	  → 对手会话调用真实产品入口 DeleteTaskCategory
//	  → 红：删除当场提交（它绕开了写者的门禁与锁）
//	  → 绿：删除被证明等锁，等 A 收尾后才提交
//
// 两种收场都断言同一条产品不变量：**分类已经不存在，就不该再有任务引用它**。
func TestCategoryDeleteRaceAgainstReferenceWriters(t *testing.T) {
	dsn := testDSN(t)

	cases := []struct {
		name    string
		stage   string
		prepare func(t *testing.T, fixture *DB, tracker *raceTaskTracker) []int64
		launch  func(w *DB, catID int64, targets []int64, tracker *raceTaskTracker) error
		// gateQueuedProbe = 这个产品入口目前在门禁申请之前带 gate_queued 探针
		// （当前只有 SetTaskCategory 有；CreateTaskWithOptions / SetTasksCategory
		//  只有门禁**之内**的屏障，而屏障本身就已经证明"进到产品且已过门禁"，
		//  排队侧的正向证据由删除者一侧的 gate_queued 提供）。
		gateQueuedProbe bool
	}{
		{
			// 审查的第一种交错：A 创建任务读到分类 C 后暂停，B 删 C 并提交，
			// A 再 INSERT category_id=C。
			name:  "CreateTaskWithOptions",
			stage: "create_task",
			launch: func(w *DB, catID int64, _ []int64, tracker *raceTaskTracker) error {
				// 描述必须带本用例**唯一**的 tracker 前缀，且成功创建的 task 必须登记进
				// tracker（审查 I-R03）：旧实现写死 "race-writer-create" 又不记 id，
				// 这一行连同它的 exploration 与 origin 节点每轮都留在库里，而"按前缀回读
				// 为 0"的收尾断言查的根本不是它，是彻头彻尾的假绿。
				task, err := w.CreateTaskWithOptions(tracker.prefix+"-writer-create", "goal",
					TaskCreateOptions{CategoryID: &catID})
				if err != nil {
					return err
				}
				tracker.track(task)
				return nil
			},
		},
		{
			// 审查的第二种交错：T 原本未分类，A 锁住 T 并读到 C 后暂停；
			// B 删 C，范围 UPDATE 不包含仍未分类的 T，提交；A 再把 T 挂到已删的 C 上。
			name:  "SetTaskCategory",
			stage: "set_task_category",
			// 门禁排队探针（复审 RR-M01）：SetTaskCategory 在进入 runTaskChildWriter
			// 之前发 gate_queued，因此这里能同时拿到"进到产品之内"与"站在门禁争用点上"
			// 两条事实。
			gateQueuedProbe: true,
			prepare: func(t *testing.T, fixture *DB, tracker *raceTaskTracker) []int64 {
				return []int64{raceCreateTask(t, fixture, tracker, "single-target", nil)}
			},
			launch: func(w *DB, catID int64, targets []int64, _ *raceTaskTracker) error {
				_, err := w.SetTaskCategory(targets[0], &catID)
				return err
			},
		},
		{
			name:  "SetTasksCategory",
			stage: "set_tasks_category",
			prepare: func(t *testing.T, fixture *DB, tracker *raceTaskTracker) []int64 {
				return []int64{
					raceCreateTask(t, fixture, tracker, "batch-target-a", nil),
					raceCreateTask(t, fixture, tracker, "batch-target-b", nil),
				}
			},
			launch: func(w *DB, catID int64, targets []int64, _ *raceTaskTracker) error {
				_, _, err := w.SetTasksCategory(targets, &catID)
				return err
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fixture, err := Open(dsn)
			if err != nil {
				t.Fatalf("打开夹具会话失败: %v", err)
			}
			t.Cleanup(func() {
				if err := fixture.Close(); err != nil {
					t.Errorf("关闭夹具会话失败: %v", err)
				}
			})

			tracker := newRaceTaskTracker(t, "i01")
			// 注册顺序 = LIFO：任务回收先跑，屏障卸载 + 写者 join 后跑，
			// 保证回收时写者已经收尾，不会漏掉它刚插入的那一行。
			t.Cleanup(func() { tracker.cleanup(t, fixture) })

			category, err := fixture.CreateTaskCategory(fmt.Sprintf("w5-i01-%s-%d", tc.name, time.Now().UnixNano()))
			if err != nil {
				t.Fatalf("建夹具分类失败: %v", err)
			}
			catID := category.ID
			t.Cleanup(func() {
				if _, err := fixture.Exec(`DELETE FROM task_categories WHERE id = ?`, catID); err != nil {
					t.Errorf("回收夹具分类失败: %v", err)
				}
			})

			// 夹具任务在装屏障之前建好（走正常写者路径，此刻还没有屏障）。
			targets := []int64(nil)
			if tc.prepare != nil {
				targets = tc.prepare(t, fixture, tracker)
			}

			writer, writerPID := raceOpenSession(t, dsn)
			deleter, deleterPID := raceOpenSession(t, dsn)
			observer, _ := raceOpenSession(t, dsn)

			fired := make(chan raceHookCall, 4)
			release := make(chan struct{})
			writerDone := make(chan error, 1)
			var releaseOnce sync.Once
			releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }

			// 守卫**先**登记（cleanup 是 LIFO，所以它最后跑）：它只在写者真的启动过
			// 之后才等收尾；随后登记的这条卸载屏障 + 放行必须先跑，否则守卫会去等一个
			// 还停在屏障里的写者（复审 RR-I02 修的就是这类时序错配）。
			writerGuard := newRaceGuard(t, "引用写者")
			writerProbes := newRaceProbeBus()

			// 写者探针：`gate_queued` 只记账（它代表"已进到产品之内、正站在门禁申请之前"），
			// 其余 stage 才是本用例真正要的屏障（在门禁**之内**、写 category_id 之前）。
			// 屏障命中本身就是"写者已持门禁"的事实：那道屏障在产品实现里位于门禁之后。
			SetTaskCategoryWriterTestHook(func(stage string, _ int64) {
				if stage == categoryGateQueuedStage {
					writerProbes.signal(stage).fire()
					return
				}
				select {
				case fired <- raceHookCall{stage: stage}:
				default:
				}
				<-release
			})
			t.Cleanup(func() {
				SetTaskCategoryWriterTestHook(nil)
				releaseWriter()
			})

			// 删除者探针（复审 RR-M01）：两个 stage 各管一半 ——
			//   gate_queued ⇒ "确实排在目标门禁之前"（排队证据，由产品自己发出）；
			//   in_gate     ⇒ "已经进到门禁之后"（引用写者还停在屏障里时命中一次就是绕开门禁）。
			deleteProbes := raceDeleterProbes(t, catID, nil)

			writerGuard.start(func() {
				writerDone <- tc.launch(writer, catID, targets, tracker)
			})

			raceWaitHook(t, tc.name, fired, writerDone, tc.stage)
			// 写者抵达屏障 ⇒ 它必然已经先经过自己的门禁排队点（同一个产品实现里的先后顺序）。
			// 这条断言钉的是"探针被人挪到门禁之前/之后"的回归；写者持门禁这个事实由屏障本身给出。
			if !writerProbes.fired(categoryGateQueuedStage) && tc.gateQueuedProbe {
				t.Fatalf("引用写者抵达了门禁之内的屏障（%s），却没有发出 %q 排队信号：探针顺序与实现不符",
					tc.stage, categoryGateQueuedStage)
			}

			// 删除者：唯一的"我确实排在门禁上"证据来自产品自己在门禁申请之前发出的
			// gate_queued（复审 RR-M01）。测试不再在调用产品之前自己 close 通道。
			deleteDone := make(chan error, 1)
			deleteGuard := newRaceGuard(t, "并发删除分类")
			deleteGuard.start(func() {
				deleted, err := deleter.DeleteTaskCategory(catID)
				switch {
				case err != nil:
					deleteDone <- err
				case !deleted:
					deleteDone <- fmt.Errorf("DeleteTaskCategory 报未删除，但分类 %d 此刻存在", catID)
				default:
					deleteDone <- nil
				}
			})

			// DB 侧的关联证据是**加分项**：删除者如果被 DB 门禁行挡住，等的就是 locks 表。
			// 拿不到表 id 时传 0，raceObserveDeleter 于是**不会**把任意 LockWaiting 认成
			// 夹具锁证据（宁可只凭本地门禁握手给 held，也不静默放行）。
			locksTableID, err := raceTableID(observer, "locks")
			if err != nil {
				t.Logf("提示：%v ⇒ 本次不把 DB 锁等待算作夹具锁证据，只走本地门禁握手", err)
			}
			outcome := raceObserveDeleter(t, observer, deleterPID, writerPID, deleteDone,
				deleteProbes.signal(categoryInGateStage).C(),
				deleteProbes.signal(categoryGateQueuedStage).C(),
				locksTableID)
			releaseWriter()
			if outcome == raceDeleteGateBreached {
				t.Fatalf("引用写者还停在屏障里（共同门禁仍在它手上），DeleteTaskCategory 却已经进到门禁之后的探针：" +
					"删除绕开了 Business Gate，两侧各修各的 = 悬空引用窗口仍在")
			}

			if err := raceJoin(t, "引用写者", writerDone); err != nil {
				t.Fatalf("引用写者失败: %v", err)
			}
			// 被门禁/锁扣住的那一侧，必须在放行写者后立刻跑完：
			// 这一步把"扣住"与"根本没跑"区分开 —— 扣住是等对手，放行就必须结束。
			if outcome != raceDeleteCommitted {
				// 握手第一跳：先证明它真的抵达了门禁之后的探针（而不是碰巧晚了 5 秒）；
				raceWaitDeleterEntered(t, deleteProbes.signal(categoryInGateStage).C())
				// 握手第二跳：探针之后必须秒完。
				if err := raceJoin(t, "删除分类", deleteDone); err != nil {
					t.Fatalf("写者收尾后删除分类失败: %v", err)
				}
			}

			// —— 产品不变量：分类已经不存在，就不该再有任务引用它 ——
			var dangling int
			if err := fixture.QueryRow(`SELECT count(*) FROM tasks WHERE category_id = ?`, catID).Scan(&dangling); err != nil {
				t.Fatalf("回读悬空引用失败: %v", err)
			}
			var categoryLeft int
			if err := fixture.QueryRow(`SELECT count(*) FROM task_categories WHERE id = ?`, catID).Scan(&categoryLeft); err != nil {
				t.Fatalf("回读分类失败: %v", err)
			}
			if dangling != 0 {
				t.Fatalf("分类 %d 已删除（分类残留 %d 行），仍有 %d 个任务引用它：悬空 category_id（删除交错观测=%s）",
					catID, categoryLeft, dangling, outcome)
			}
			if categoryLeft != 0 {
				t.Fatalf("分类 %d 未被删除，交错未完整执行（观测=%s）", catID, outcome)
			}
		})
	}
}

// TestFanOutRaceAgainstChannelWriters 复现审查 I-02 的交错：
//
//	fan-out 读出启用渠道后被显式屏障停住
//	  → 真实产品入口删除 / 停用渠道，并且必须当场提交
//	  → 放行 fan-out 继续 INSERT 投递
//
// 断言：投递行不得引用已删除或已停用的渠道；本轮事件仍要被标记 fanned_out。
//
// 插入投递行的唯一入口就是 fan-out 核心（全仓 `INSERT INTO
// notification_deliveries` 只有这一处；digest 批次与手工重发都是对既有行推进
// 状态，不新建行），因此这一条覆盖就是"所有插 delivery 的入口"。测试侧调用的是
// 同核心的受控入口 `fanOutOwnEventForChannels`：它与生产 FanOutPendingEvents 共用
// 同一段处理，只额外把领取绑定到 `AND id = 自有事件` 并把渠道裁剪到自有白名单，
// 于是"在共享库上跑"与"绝不碰别人的通知数据"第一次同时成立（复审 RR-I02）。
func TestFanOutRaceAgainstChannelWriters(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	cases := []struct {
		name string
		// disrupt 在 fan-out 停在屏障期间执行真实的渠道写者入口。
		disrupt func(d *DB, channelID int64) error
	}{
		{
			name:    "delete channel",
			disrupt: func(d *DB, channelID int64) error { return d.DeleteNotificationChannel(ctx, channelID) },
		},
		{
			name: "disable channel",
			disrupt: func(d *DB, channelID int64) error {
				return d.SetNotificationChannelEnabled(ctx, channelID, false)
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fixture, err := Open(dsn)
			if err != nil {
				t.Fatalf("打开夹具会话失败: %v", err)
			}
			t.Cleanup(func() {
				if err := fixture.Close(); err != nil {
					t.Errorf("关闭夹具会话失败: %v", err)
				}
			})

			// ① 环境留痕（复审 RR-I02）：全库待分派集合**不再是**门禁。隔离做在 SQL 里
			//    （受控领取按 id 绑定自有事件 + 渠道白名单），共享库里有别人的 pending
			//    也不会被本次分派碰到；这里只留一行日志，供报告判断外部消费者是否在场。
			raceLogForeignNotificationBacklog(t, fixture)

			// ② 夹具渠道与事件彼此**专用**（复审 RR-I02）：匹配规则专用化把"自有事件被
			//    全局 dispatcher 投给别人"的业务风险压低；隔离本身由 ③ 的结构保证。
			notifyFixture := newRaceNotifyFixture(t, fixture)
			channel, eventID := notifyFixture.channel, notifyFixture.eventID

			// 最后注册 = 最先执行：投递行必须赶在事件/渠道之前回收。
			t.Cleanup(func() {
				mustDeleteOwned(t, fixture, fmt.Sprintf("夹具投递（渠道 %d / 事件 %d）", channel.ID, eventID),
					`DELETE FROM notification_deliveries WHERE channel_id = ? OR event_id = ?`, channel.ID, eventID)
				mustAssertOwnedGone(t, fixture,
					fmt.Sprintf("夹具投递（渠道 %d / 事件 %d）", channel.ID, eventID),
					`SELECT count(*) FROM notification_deliveries WHERE channel_id = ? OR event_id = ?`,
					channel.ID, eventID)
			})

			fan, _ := raceOpenSession(t, dsn)
			disrupter, disruptPID := raceOpenSession(t, dsn)

			fired := make(chan raceHookCall, 4)
			seenChannels := make(chan []int64, 4)
			release := make(chan struct{})
			fanDone := make(chan error, 1)
			fanCounts := make(chan raceFanOutCounts, 1)
			var releaseOnce sync.Once
			releaseFan := func() { releaseOnce.Do(func() { close(release) }) }

			// 守卫先登记（LIFO ⇒ 最后跑），卸载屏障 + 放行随后登记：
			// 下面任何 t.Skip / t.Fatal 都可能发生在 fan goroutine 启动**之前**，
			// 那时 fanFinished 永远不会关闭，等它就是复审 RR-I02 打的那次假失败。
			fanGuard := newRaceGuard(t, "fan-out")

			// 屏障在**裁剪之后**（notification.go 的 channels_read），因此这里读到的渠道
			// 集合就是"本轮真正可能要写投递的那一批"：拿它直接证明渠道白名单在写入之前
			// 已经生效，而不是靠事后断言补救。
			SetFanOutDeliveryTestHook(func(stage string, channelIDs []int64) {
				select {
				case fired <- raceHookCall{stage: stage}:
				default:
				}
				select {
				case seenChannels <- channelIDs:
				default:
				}
				<-release
			})
			t.Cleanup(func() {
				SetFanOutDeliveryTestHook(nil)
				releaseFan()
			})

			// ③ 分派走**受控入口**（复审 RR-I02 的再次验收要求）：领取语句在 SQL 层就带
			//    `AND id = eventID`，渠道集合裁剪到自有渠道，越界命中在第一条 DML 之前
			//    上抛 + 回滚。别人的事件、别人的渠道在写入之前就不可能被碰到。
			raceRequireOwnEventPending(t, fixture, eventID)
			foreignBefore := raceSnapshotForeignEvents(t, fixture, eventID)

			fanGuard.start(func() {
				events, deliveries, err := fan.fanOutOwnEventForChannels(ctx, eventID, []int64{channel.ID})
				select {
				case fanCounts <- raceFanOutCounts{events: events, deliveries: deliveries}:
				default:
				}
				fanDone <- err
			})

			raceWaitHook(t, tc.name, fired, fanDone, "channels_read")

			// 屏障里看到的渠道集合必须恰好是夹具渠道：白名单生效的直接证据。
			select {
			case ids := <-seenChannels:
				if len(ids) != 1 || ids[0] != channel.ID {
					t.Fatalf("受控 fan-out 在屏障处的候选渠道是 %v，期望恰好 [%d]："+
						"渠道白名单没有在写入前生效（RR-I02）", ids, channel.ID)
				}
			case <-time.After(raceBudget):
				t.Fatalf("没有读到屏障处的渠道集合")
			}

			// 渠道写者必须在 fan-out 仍停在屏障时就能提交。这本身也是契约：
			// fan-out 不得在读完渠道后**提前**占住渠道行，把渠道管理操作永久挡在门外。
			disruptDone := make(chan error, 1)
			go func() {
				disruptDone <- tc.disrupt(disrupter, channel.ID)
			}()
			if err := raceJoin(t, "渠道写者", disruptDone); err != nil {
				t.Fatalf("渠道写者在 fan-out 暂停期间无法结束（pid=%d）: %v", disruptPID, err)
			}

			releaseFan()
			if err := raceJoin(t, "fan-out", fanDone); err != nil {
				t.Fatalf("fan-out 失败: %v", err)
			}

			// —— 自隔离（审查 I-R04 / 复审 RR-I02）：受控入口只可能领到夹具那一条 ——
			select {
			case counts := <-fanCounts:
				raceAssertOwnedFanOutEvents(t, counts.events, eventID)
			case <-time.After(raceBudget):
				t.Errorf("没有读到 fan-out 的处理计数")
			}
			raceAssertForeignEventsUntouched(t, fixture, eventID, foreignBefore)
			raceAssertNoForeignDeliveries(t, fixture, channel.ID, eventID)

			// —— 产品不变量：投递行不得引用已删除 / 已停用的渠道 ——
			var orphans int
			if err := fixture.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE channel_id = ?`,
				channel.ID).Scan(&orphans); err != nil {
				t.Fatalf("回读孤儿投递失败: %v", err)
			}
			if orphans != 0 {
				t.Fatalf("渠道 %d 已删除/停用，仍有 %d 条投递引用它（含 event_id=%d 的孤儿）",
					channel.ID, orphans, eventID)
			}
			// fan-out 仍要把本轮事件标记为已分派，否则同一事件每个 tick 被重扫。
			var fannedOut bool
			if err := fixture.QueryRow(`SELECT fanned_out FROM notification_events WHERE id = ?`, eventID).Scan(&fannedOut); err != nil {
				t.Fatalf("回读事件分派标记失败: %v", err)
			}
			if !fannedOut {
				t.Fatalf("事件 %d 未被标记 fanned_out，会被每个 tick 重复扫描", eventID)
			}
		})
	}
}

// TestDeleteTaskCategoryIsInsideSharedBusinessGate 是 I-01 的**机制**证明
// （"删分类必须与其他任务写者共用同一道 Business Gate"这半条，光看数据看不出来）。
//
// 手法：另开一条裸事务，按 lockorder 定义的全局门禁行 locks('business_relations')
// 取 FOR UPDATE 并持有。这把锁 TiDB 看得见，于是删除路径要么被证明挡在它上面
// （= 确实走了共同门禁），要么当场提交（= 绕开门禁，正是被审查掉的裸 Begin）。
//
// 与交错用例的分工：交错用例证明"数据不会悬空"，本用例证明"靠什么不悬空"。
func TestDeleteTaskCategoryIsInsideSharedBusinessGate(t *testing.T) {
	dsn := testDSN(t)

	fixture, err := Open(dsn)
	if err != nil {
		t.Fatalf("打开夹具会话失败: %v", err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("关闭夹具会话失败: %v", err)
		}
	})

	category, err := fixture.CreateTaskCategory(fmt.Sprintf("w5-i01-gate-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("建夹具分类失败: %v", err)
	}
	catID := category.ID
	t.Cleanup(func() {
		if _, err := fixture.Exec(`DELETE FROM task_categories WHERE id = ?`, catID); err != nil {
			t.Errorf("回收夹具分类失败: %v", err)
		}
	})

	blocker, blockerPID := raceOpenSession(t, dsn)
	tx, err := blocker.Begin()
	if err != nil {
		t.Fatalf("门禁持有权事务开启失败: %v", err)
	}
	// 先登记回滚：任何 t.Fatal 的提前退出都必须把全局门禁行还给后来者。
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec(`INSERT IGNORE INTO locks (name) VALUES ('business_relations')`); err != nil {
		t.Fatalf("预置全局门禁行失败: %v", err)
	}
	var lockedName string
	if err := tx.QueryRow(`SELECT name FROM locks WHERE name = 'business_relations' FOR UPDATE`).Scan(&lockedName); err != nil {
		t.Fatalf("持有全局门禁行失败: %v", err)
	}
	if lockedName != "business_relations" {
		t.Fatalf("全局门禁行不符: %q", lockedName)
	}

	deleter, deleterPID := raceOpenSession(t, dsn)
	observer, _ := raceOpenSession(t, dsn)

	// 排队证据（复审 RR-M01）：产品自己在门禁申请之前发出 gate_queued。拿不到它 = 删除者
	// 根本没站到门禁上，后面的 LockWaiting 判定一律不成立。
	deleteProbes := raceDeleterProbes(t, catID, nil)
	deleteGuard := newRaceGuard(t, "删除分类（门禁机制用例）")
	deleteDone := make(chan error, 1)
	deleteGuard.start(func() {
		deleted, err := deleter.DeleteTaskCategory(catID)
		if err != nil {
			deleteDone <- err
			return
		}
		if !deleted {
			deleteDone <- fmt.Errorf("DeleteTaskCategory 报未删除（分类 %d 存在）", catID)
			return
		}
		deleteDone <- nil
	})
	select {
	case <-deleteProbes.signal(categoryGateQueuedStage).C():
	case err := <-deleteDone:
		_ = tx.Rollback()
		if err != nil {
			t.Fatalf("删除者未经等待就返回: %v", err)
		}
		t.Fatalf("删除者未经等待就提交：它绕开了共同 Business Gate")
	case <-time.After(raceBudget):
		_ = tx.Rollback()
		t.Fatalf("删除者在 %s 内没有抵达门禁的争用点（产品未发出 %q），判定不成立", raceBudget, categoryGateQueuedStage)
	}

	// **强证明**（审查 M-R01 / 复审 RR-M01）：blocker 持全局门禁行期间，必须能在 TiDB
	// 事务视图里看到删除者进入 LockWaiting，并且这条等待要能**对得上本夹具的 blocker**：
	// 等待方涉及的物理表必须包含门禁行所在的 locks 表、blocker 会话自己也必须还开着那个
	// 事务（raceJudgeLockWait 四条证据缺一不可）。旧写法只要"任意会话出现过 LockWaiting"
	// 就算数，分不清它等的是谁的锁。表 id 拿不到 ⇒ 诚实 SKIP（绝不带 0 继续跑）。
	locksTableID := raceMustTableID(t, observer, "locks", "共同门禁行")
	if waitErr := raceWaitBackendBlockOnFixture(t, observer, deleterPID, blockerPID,
		locksTableID, deleteDone, "删除者等待共同门禁行"); waitErr != nil {
		_ = tx.Rollback()
		if errors.Is(waitErr, errNoBackendTrxView) {
			t.Skip("TiDB 事务视图不可用：拿不到锁等待的强证据，本用例不降级为固定窗口判定")
		}
		t.Fatalf("删除者未被证明挡在共同门禁行 locks('business_relations') 上（pid=%d）: %v", deleterPID, waitErr)
	}
	// 门禁之内的那个探针此刻必须**还没有**命中：它一旦命中就说明删除者已经进到门禁之后，
	// 与"它正挡在门禁行上"直接矛盾（复审 RR-M01 要求把这两件事写成同一条判据）。
	if deleteProbes.fired(categoryInGateStage) {
		_ = tx.Rollback()
		t.Fatalf("blocker 仍持全局门禁行，DeleteTaskCategory 却已经进到门禁之后（%q）：它绕开了共同 Business Gate",
			categoryInGateStage)
	}
	if releaseErr := tx.Rollback(); releaseErr != nil {
		t.Fatalf("释放全局门禁行失败: %v", releaseErr)
	}
	// 放行后必须秒完：证明刚才等的就是这把锁，而不是卡死在别处。
	if releaseErr := raceJoin(t, "删除分类", deleteDone); releaseErr != nil {
		t.Fatalf("释放全局门禁行后删除分类失败: %v", releaseErr)
	}
	var left int
	if err := fixture.QueryRow(`SELECT count(*) FROM task_categories WHERE id = ?`, catID).Scan(&left); err != nil {
		t.Fatalf("回读分类失败: %v", err)
	}
	if left != 0 {
		t.Fatalf("分类 %d 仍未被删除", catID)
	}
}

// TestFanOutLocksChannelRowsBeforeInsertingDeliveries 是 I-02 的**机制**证明：
// fan-out 必须把"本轮要写投递行的渠道"锁在自己事务里，否则渠道删除/停用与
// 投递插入之间没有共同串行点（两边各修各的都会漏）。
//
// 手法：另开裸事务持住本用例渠道行的 FOR UPDATE，再跑 fan-out：
//   - 绿：fan-out 被 TiDB 证明挡在该渠道行锁上，放行后正常把投递插进去；
//   - 红：fan-out 当场提交（它根本没锁渠道），投递照插 —— 与并发删除之间就是孤儿。
func TestFanOutLocksChannelRowsBeforeInsertingDeliveries(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	fixture, err := Open(dsn)
	if err != nil {
		t.Fatalf("打开夹具会话失败: %v", err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("关闭夹具会话失败: %v", err)
		}
	})

	// 环境留痕 + 专用事件/渠道 + **受控领取**（复审 RR-I02）：本用例的分派在 SQL 层就
	// 只可能领取夹具自己那一条事件、只可能匹配夹具自己那一个渠道。于是"它进入了
	// LockWaiting"才能被**唯一**解释成"在等本夹具 blocker 手里那一行渠道"，而不是别人的
	// 事件行或别人的渠道行 —— 这是把 M-R01 的"任意 LockWaiting 都算"收紧成"等的是我的锁"
	// 的**语义前提**（结构前提则由 raceJudgeLockWait 的四条证据负责）。
	raceLogForeignNotificationBacklog(t, fixture)
	notifyFixture := newRaceNotifyFixture(t, fixture)
	channel, eventID := notifyFixture.channel, notifyFixture.eventID

	// 投递按 event_id / channel_id 一并回收：本夹具事件在自己的渠道上产生的投递行
	// 属于本次运行造出来的行，必须全部收走，一条不许留在共享库里。
	t.Cleanup(func() {
		mustDeleteOwned(t, fixture, fmt.Sprintf("夹具投递（渠道 %d / 事件 %d）", channel.ID, eventID),
			`DELETE FROM notification_deliveries WHERE channel_id = ? OR event_id = ?`, channel.ID, eventID)
		mustAssertOwnedGone(t, fixture,
			fmt.Sprintf("夹具投递（渠道 %d / 事件 %d）", channel.ID, eventID),
			`SELECT count(*) FROM notification_deliveries WHERE channel_id = ? OR event_id = ?`,
			channel.ID, eventID)
	})

	blocker, blockerPID := raceOpenSession(t, dsn)
	tx, err := blocker.Begin()
	if err != nil {
		t.Fatalf("渠道持有权事务开启失败: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	var lockedChannel int64
	if err := tx.QueryRow(`SELECT id FROM notification_channels WHERE id = ? FOR UPDATE`, channel.ID).Scan(&lockedChannel); err != nil {
		t.Fatalf("持有渠道行失败: %v", err)
	}
	if lockedChannel != channel.ID {
		t.Fatalf("持有的渠道行不符: %d", lockedChannel)
	}

	// 领取只覆盖自己那一条事件（受控入口按 id 绑定）；同时记下别人的事件状态当绊线。
	raceRequireOwnEventPending(t, fixture, eventID)
	foreignBefore := raceSnapshotForeignEvents(t, fixture, eventID)

	fan, fanPID := raceOpenSession(t, dsn)
	observer, _ := raceOpenSession(t, dsn)
	fanGuard := newRaceGuard(t, "fan-out（渠道行锁机制用例）")
	fanDone := make(chan error, 1)
	fanEvents := make(chan int, 1)
	fanGuard.start(func() {
		events, _, err := fan.fanOutOwnEventForChannels(ctx, eventID, []int64{channel.ID})
		select {
		case fanEvents <- events:
		default:
		}
		fanDone <- err
	})

	// **强证明**（审查 M-R01 / 复审 RR-M01）：必须看到 fan-out 进入 TiDB 的 LockWaiting，
	// 且四条证据齐了才算数（LockWaiting + blocker 事务在场 + 被争用表 id 已知 +
	// RELATED_TABLE_IDS 命中该表）；视图查不动才 SKIP，不降级成"3 秒内没结束所以算被挡住"。
	channelTableID := raceMustTableID(t, observer, "notification_channels", "夹具渠道行锁")
	if waitErr := raceWaitBackendBlockOnFixture(t, observer, fanPID, blockerPID,
		channelTableID, fanDone, "fan-out 等待夹具渠道行"); waitErr != nil {
		_ = tx.Rollback()
		if errors.Is(waitErr, errNoBackendTrxView) {
			t.Skip("TiDB 事务视图不可用：拿不到锁等待的强证据，本用例不降级为固定窗口判定")
		}
		t.Fatalf("fan-out 未被证明挡在渠道行 %d 的 FOR UPDATE 上（pid=%d）: %v", channel.ID, fanPID, waitErr)
	}
	if releaseErr := tx.Rollback(); releaseErr != nil {
		t.Fatalf("释放渠道行失败: %v", releaseErr)
	}
	if releaseErr := raceJoin(t, "fan-out", fanDone); releaseErr != nil {
		t.Fatalf("释放渠道行后 fan-out 失败: %v", releaseErr)
	}
	// 受控领取只可能领到夹具自己那一条事件（0 只可能是被外部消费者抢先分派 ⇒ 诚实跳过）。
	select {
	case events := <-fanEvents:
		raceAssertOwnedFanOutEvents(t, events, eventID)
	case <-time.After(raceBudget):
		t.Fatalf("没有读到受控 fan-out 的领取计数")
	}
	// 放行后必须真的把投递插进去：证明刚才那次是"在锁上等"，不是空跑一轮。
	var inserted int
	if err := fixture.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE event_id = ? AND channel_id = ?`,
		eventID, channel.ID).Scan(&inserted); err != nil {
		t.Fatalf("回读投递行失败: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("放行后 fan-out 应插入 1 条投递，实际 %d", inserted)
	}
	// —— 自隔离（审查 I-R04）：别人家的事件一条都没被动过 ——
	raceAssertForeignEventsUntouched(t, fixture, eventID, foreignBefore)
	raceAssertNoForeignDeliveries(t, fixture, channel.ID, eventID)
}

// TestDeleteTaskCategoryRollsBackOnMembershipDrift 钉住审查 I-R02 的第 ③ 条：
// 锁计划已经定好之后又有人把任务挂到本分类上，删除必须**整体回滚 + 退避重放**，
// 由下一轮定位自然带上它。
//
// 为什么这个用例是必要的：交错用例里的两个引用写者都在同一道 Business Gate 内，
// 门禁本身就把手脚挡住了，看不出"新鲜成员核验"有没有真的生效。这里故意请出一个
// **门禁之外**的旁路写者（裸 UPDATE），它专门在"定位完成、加锁未开始"那一瞬提交，
// 让锁计划真的落后于成员集合。
//
// 两种错误结局都要红：
//   - 实现顺手把计划外那行也锁掉（原地补锁）→ 锁序不再是 PK 升序，Spec 明令禁止；
//     本用例通过"最终没有悬空引用 + 未分类归位"来暴露"既不补锁也不核验"的实现；
//   - 实现压根不核验，直接按旧计划清理 → 迟到者留下指向已删分类的悬空 category_id。
func TestDeleteTaskCategoryRollsBackOnMembershipDrift(t *testing.T) {
	dsn := testDSN(t)

	fixture, err := Open(dsn)
	if err != nil {
		t.Fatalf("打开夹具会话失败: %v", err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("关闭夹具会话失败: %v", err)
		}
	})

	tracker := newRaceTaskTracker(t, "i02-drift")
	t.Cleanup(func() { tracker.cleanup(t, fixture) })

	category, err := fixture.CreateTaskCategory(fmt.Sprintf("w5-i02-drift-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("建夹具分类失败: %v", err)
	}
	catID := category.ID
	// 兜底回收：漂移注入的迟到者可能仍引用本分类，必须赶在 tracker 删任务之前处理。
	t.Cleanup(func() {
		if _, err := fixture.Exec(`UPDATE tasks SET category_id = NULL WHERE category_id = ?`, catID); err != nil {
			t.Errorf("解除夹具分类引用失败: %v", err)
		}
		if _, err := fixture.Exec(`DELETE FROM task_categories WHERE id = ?`, catID); err != nil {
			t.Errorf("回收夹具分类失败: %v", err)
		}
	})

	member := raceCreateTask(t, fixture, tracker, "member", &catID)
	latecomer := raceCreateTask(t, fixture, tracker, "latecomer", nil)

	// 旁路写者：完全绕开 Business Gate 的裸 UPDATE，代表"门禁之外仍可能挂引用"的现实风险。
	bypass, _ := raceOpenSession(t, dsn)

	// 只在第一轮注入一次：漂移被 TxRunner 重放后，第二轮必须干净跑完。
	var injectOnce sync.Once
	injected := make(chan error, 1)
	SetTaskCategoryDeleterTestHook(func(stage string, categoryID int64) {
		if stage != categoryAfterLocateStage || categoryID != catID {
			return
		}
		injectOnce.Do(func() {
			_, err := bypass.Exec(`UPDATE tasks SET category_id = ? WHERE id = ?`, catID, latecomer)
			injected <- err
		})
	})
	t.Cleanup(func() { SetTaskCategoryDeleterTestHook(nil) })

	deleted, err := fixture.DeleteTaskCategory(catID)
	if err != nil {
		t.Fatalf("删除分类失败（漂移应由门禁内退避重放自愈，不该把调用方挡在门外）: %v", err)
	}
	if err := <-injected; err != nil {
		t.Fatalf("注入旁路引用失败: %v", err)
	}
	if !deleted {
		t.Fatalf("DeleteTaskCategory 报未删除，但分类 %d 存在", catID)
	}

	// —— 产品不变量：分类已经不存在，任何任务都不该再引用它（含漂移进来的那行）——
	var dangling []int64
	rows, err := fixture.Query(`SELECT id FROM tasks WHERE category_id = ?`, catID)
	if err != nil {
		t.Fatalf("回读悬空引用失败: %v", err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatalf("读取悬空引用失败: %v", err)
		}
		dangling = append(dangling, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("回读悬空引用失败: %v", err)
	}
	if len(dangling) > 0 {
		t.Fatalf("分类 %d 已删除，任务 %v 仍引用它：锁计划之外的成员既没被核验到，也没触发整体回滚（I-R02）",
			catID, dangling)
	}
	for _, taskID := range []int64{member, latecomer} {
		var current sql.NullInt64
		if err := fixture.QueryRow(`SELECT category_id FROM tasks WHERE id = ?`, taskID).Scan(&current); err != nil {
			t.Fatalf("回读任务 %d 失败: %v", taskID, err)
		}
		if current.Valid {
			t.Errorf("任务 %d 未回到未分类，仍指向分类 %d", taskID, current.Int64)
		}
	}
}

// TestRestoreTaskArchiveRaceAgainstCategoryDelete 是审查 I-R01 的**交错**证明：
// 归档恢复把冷包里的 category_id 写回 tasks 时，必须与 DeleteTaskCategory 共用同一道
// Business Gate。否则"restore 核验分类存活 → 删除提交 → restore 写回"就能提交出一个
// 没有外键可挡的悬空 category_id（旧实现是裸 Begin + rowExists 普通快照读）。
//
// 交错点：restore 在**已持分类行锁、尚未写回引用**处被显式屏障停住
// （SetTaskCategoryWriterTestHook 的 "restore_category_locked" 阶段），此刻放一个走真实
// 产品入口的删分类者进来：
//   - 绿：删分类者进不到门禁之后（探针一次不命中）、也没提交；放行 restore 之后它才抵达
//     探针并跑完，最终分类已删、没有任何任务还引用它；
//   - 红：删分类者在 restore 仍持门禁时就进到门禁之后（= restore 没和它共用 Business
//     Gate），或者当场提交（= 两边各修各的）。这两种结局下 restore 随后写回的都是悬空 id。
//
// 判定完全走通道握手，不拿 TiDB 事务视图当必要条件：本地门禁是进程内互斥，本来就不产生
// DB 锁；TiDB 侧可见性只作为加强证据（raceObserveDeleter 里能抓到就升级为 blocked）。
func TestRestoreTaskArchiveRaceAgainstCategoryDelete(t *testing.T) {
	dsn := testDSN(t)

	fixture := newArchiveSevenTableFixture(t)
	d := fixture.d

	// —— 复审 RR-I03 的前置闸门 ——
	// 归档领取 API 是**全局 FIFO**：ClaimTaskArchiveJob 先按 requested_at,id 取队首并
	// 把它 UPDATE 成 archiving/restoring/deleting 再 commit，调用方**之后**才能看见
	// "领到的不是自己"。共享库里只要有一条别人的排队记录排在前面，那次调用就已经把
	// 别人任务的状态、进度和 error 字段改掉并提交了，cleanup 也不拥有它。
	// 因此这里两道防线：①开跑前队列里只要还有别人的排队记录就诚实跳过；
	// ②本文件一律改用 claimOwnArchiveJob —— 队首不是自己的 archive id 就一条 UPDATE
	// 都不发，直接回滚跳过，绝不"先领全局记录再用 id 断言补救"。
	requireNoQueuedArchiveJobs(t, d)

	category, err := d.CreateTaskCategory(fmt.Sprintf("w5-i01-restore-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("建夹具分类失败: %v", err)
	}
	catID := category.ID
	t.Cleanup(func() {
		// 逆拓扑 + 一律判红 + 收尾回读归零（复审 RR-I01）：这个分类可能仍被归档夹具
		// 任务引用（也可能被漂移/失败路径留在原地），必须先解引用再删根行。
		mustDeleteOwned(t, d, fmt.Sprintf("夹具分类 %d 的任务引用", catID),
			`UPDATE tasks SET category_id = NULL WHERE category_id = ?`, catID)
		mustDeleteOwned(t, d, fmt.Sprintf("夹具分类 %d", catID),
			`DELETE FROM task_categories WHERE id = ?`, catID)
		mustAssertOwnedGone(t, d, fmt.Sprintf("引用夹具分类 %d 的任务", catID),
			`SELECT count(*) FROM tasks WHERE category_id = ?`, catID)
		mustAssertOwnedGone(t, d, fmt.Sprintf("夹具分类 %d", catID),
			`SELECT count(*) FROM task_categories WHERE id = ?`, catID)
	})
	if _, err := d.SetTaskCategory(fixture.task.ID, &catID); err != nil {
		t.Fatalf("给归档夹具任务挂分类失败: %v", err)
	}

	snapshot, archiveID := archiveAndCompactFixture(t, fixture)
	if _, err := d.QueueTaskArchiveRestore(archiveID); err != nil {
		t.Fatalf("排队恢复任务失败: %v", err)
	}
	claimOwnArchiveJob(t, t.Context(), d, archiveID, RestoreQueued, Restoring)

	restorer, restorerPID := raceOpenSession(t, dsn)
	deleter, deleterPID := raceOpenSession(t, dsn)
	observer, _ := raceOpenSession(t, dsn)

	fired := make(chan raceHookCall, 4)
	release := make(chan struct{})
	restoreDone := make(chan error, 1)
	var releaseOnce sync.Once
	releaseRestore := func() { releaseOnce.Do(func() { close(release) }) }

	// 守卫先登记（LIFO ⇒ 最后跑）：restore goroutine 尚未启动就发生的 Skip/Fatal，
	// 不该去等一个永不关闭的收尾通道（复审 RR-I02）。
	restoreGuard := newRaceGuard(t, "归档恢复")

	SetTaskCategoryWriterTestHook(func(stage string, categoryID int64) {
		if stage != "restore_category_locked" || categoryID != catID {
			return
		}
		select {
		case fired <- raceHookCall{stage: stage}:
		default:
		}
		<-release
	})
	t.Cleanup(func() {
		SetTaskCategoryWriterTestHook(nil)
		releaseRestore()
	})

	restoreGuard.start(func() {
		_, err := restorer.RestoreTaskArchive(archiveID, snapshot, 600)
		restoreDone <- err
	})

	// restore 就是本用例里的"引用写者"：它必须真的走到"已核验分类、尚未写回"这一刻。
	raceWaitHook(t, "归档恢复", fired, restoreDone, "restore_category_locked")

	// 删分类者的探针（复审 RR-M01）：
	//   gate_queued ⇒ 它确实排在共同门禁之前（正向排队证据）；
	//   in_gate     ⇒ restore 还停在屏障里时一次都不许命中（命中即绕开门禁）。
	deleteProbes := raceDeleterProbes(t, catID, nil)

	deleteDone := make(chan error, 1)
	deleteGuard := newRaceGuard(t, "删除分类（归档恢复交错）")
	deleteGuard.start(func() {
		deleted, err := deleter.DeleteTaskCategory(catID)
		switch {
		case err != nil:
			deleteDone <- err
		case !deleted:
			deleteDone <- fmt.Errorf("DeleteTaskCategory 报未删除，但分类 %d 此刻存在", catID)
		default:
			deleteDone <- nil
		}
	})

	// DB 侧关联证据是加分项（拿不到 locks 表 id 就只走本地门禁握手，绝不静默把任意
	// LockWaiting 认成夹具锁）。
	locksTableID, tableErr := raceTableID(observer, "locks")
	if tableErr != nil {
		t.Logf("提示：%v ⇒ 本次不把 DB 锁等待算作夹具锁证据，只走本地门禁握手", tableErr)
	}
	outcome := raceObserveDeleter(t, observer, deleterPID, restorerPID, deleteDone,
		deleteProbes.signal(categoryInGateStage).C(),
		deleteProbes.signal(categoryGateQueuedStage).C(),
		locksTableID)
	releaseRestore()
	if outcome == raceDeleteGateBreached {
		t.Fatalf("归档恢复还停在屏障里（共同门禁仍在它手上），DeleteTaskCategory 却已经进到门禁之后：" +
			"归档恢复没有和删分类者共用 Business Gate，两侧各修各的 = 悬空引用窗口仍在")
	}
	if err := raceJoin(t, "归档恢复", restoreDone); err != nil {
		t.Fatalf("归档恢复失败: %v", err)
	}
	raceWaitDeleterEntered(t, deleteProbes.signal(categoryInGateStage).C())
	if err := raceJoin(t, "删除分类", deleteDone); err != nil {
		t.Fatalf("放行归档恢复后删除分类失败: %v", err)
	}

	// —— 产品不变量：分类已经不存在，就不该再有任务引用它 ——
	var dangling int
	if err := d.QueryRow(`SELECT count(*) FROM tasks WHERE category_id = ?`, catID).Scan(&dangling); err != nil {
		t.Fatalf("回读悬空引用失败: %v", err)
	}
	if dangling != 0 {
		t.Fatalf("分类 %d 已删除，仍有 %d 个任务引用它：归档恢复写回了悬空 category_id（交错观测=%s）",
			catID, dangling, outcome)
	}
	var categoryLeft int
	if err := d.QueryRow(`SELECT count(*) FROM task_categories WHERE id = ?`, catID).Scan(&categoryLeft); err != nil {
		t.Fatalf("回读分类失败: %v", err)
	}
	if categoryLeft != 0 {
		t.Fatalf("分类 %d 未被删除，交错未完整执行（观测=%s）", catID, outcome)
	}
}

// TestReferenceWriterBlockedWhileDeleterHoldsBusinessGate 是复审 RR-M01 要求的
// **反方向门禁握手**：交错用例里"被扣住的一方"是删除者，而本地那一半门禁是进程内
// 互斥、TiDB 事务视图本来就看不见，"窗口内没动静"始终差一条正向事实。这里把
// **正握着门禁的一方**换成可以正向观测的那一侧：
//
//	删除者进到共同 Business Gate（in_gate 探针命中 = 它此刻**确实**持有本地 + DB
//	两道门禁，这是可观测事实，不是"5 秒没消息所以算它拿着"）
//	→ 测试把它停在自己手里
//	→ 引用写者（真实产品入口 SetTaskCategory）由**产品自己**发出 gate_queued，
//	  证明它已经进到产品之内、正站在同一道门禁的申请之前（不是"测试发了个信号就去调用"）；
//	  在删除者持门禁期间它一步都进不到门禁之后的屏障里，也一步都跑不完
//	→ 删除者一放行，写者必须立刻结束
//
// 等待侧从此有了自己的正向排队证据（复审 RR-M01）：旧写法只等到"测试自己发的 started
// 信号 + 5 秒无进展"，那条反例（信号发完就停在产品调用之前）永远成立。
//
// 与交错用例各证一半才是"同一道门禁"：互斥是对称的，任何一侧被绕过都会命中对应的
// 那个用例。放行后写者的业务结局允许两种（成功挂上引用，或被分类已删挡回来），
// 但**任何时刻**都不许留下"分类已删仍有任务引用"的悬空数据。
func TestReferenceWriterBlockedWhileDeleterHoldsBusinessGate(t *testing.T) {
	dsn := testDSN(t)

	fixture, err := Open(dsn)
	if err != nil {
		t.Fatalf("打开夹具会话失败: %v", err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("关闭夹具会话失败: %v", err)
		}
	})

	tracker := newRaceTaskTracker(t, "gate-holder")
	t.Cleanup(func() { tracker.cleanup(t, fixture) })

	category, err := fixture.CreateTaskCategory(fmt.Sprintf("w5-m01-holder-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("建夹具分类失败: %v", err)
	}
	catID := category.ID
	t.Cleanup(func() {
		mustDeleteOwned(t, fixture, fmt.Sprintf("夹具分类 %d 的任务引用", catID),
			`UPDATE tasks SET category_id = NULL WHERE category_id = ?`, catID)
		mustDeleteOwned(t, fixture, fmt.Sprintf("夹具分类 %d", catID),
			`DELETE FROM task_categories WHERE id = ?`, catID)
		mustAssertOwnedGone(t, fixture, fmt.Sprintf("引用夹具分类 %d 的任务", catID),
			`SELECT count(*) FROM tasks WHERE category_id = ?`, catID)
		mustAssertOwnedGone(t, fixture, fmt.Sprintf("夹具分类 %d", catID),
			`SELECT count(*) FROM task_categories WHERE id = ?`, catID)
	})

	taskID := raceCreateTask(t, fixture, tracker, "writer-target", nil)

	writer, _ := raceOpenSession(t, dsn)
	deleter, _ := raceOpenSession(t, dsn)

	// —— 门禁持有者：删除者进到门禁之后立刻被测试停住 ——
	inGate := make(chan struct{})
	var inGateOnce sync.Once
	releaseDeleter := make(chan struct{})
	var releaseOnce sync.Once
	releaseDeleterNow := func() { releaseOnce.Do(func() { close(releaseDeleter) }) }

	// 守卫先登记（LIFO ⇒ 最后跑），下面这条卸载屏障 + 放行随后登记、先跑。
	deleteGuard := newRaceGuard(t, "删除分类（门禁持有者）")
	t.Cleanup(func() {
		SetTaskCategoryDeleterTestHook(nil)
		releaseDeleterNow()
	})

	SetTaskCategoryDeleterTestHook(func(stage string, categoryID int64) {
		if stage != categoryInGateStage || categoryID != catID {
			return
		}
		inGateOnce.Do(func() { close(inGate) })
		<-releaseDeleter
	})

	deleteDone := make(chan error, 1)
	deleteGuard.start(func() {
		deleted, err := deleter.DeleteTaskCategory(catID)
		switch {
		case err != nil:
			deleteDone <- err
		case !deleted:
			deleteDone <- fmt.Errorf("DeleteTaskCategory 报未删除，但分类 %d 此刻存在", catID)
		default:
			deleteDone <- nil
		}
	})

	// 正向事实 ①：删除者**确实**已经拿到共同门禁（拿不到 ⇒ 后面的观察毫无意义）。
	select {
	case <-inGate:
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("删除者在持有共同门禁之前就失败了: %v", err)
		}
		t.Fatalf("删除者没有停在门禁之内，交错无法建立")
	case <-time.After(raceBudget):
		t.Fatalf("删除者在 %s 内没有进到共同门禁之内，本用例的握手无法建立", raceBudget)
	}

	// —— 门禁等待者：真实产品入口 SetTaskCategory，屏障只记录抵达、绝不阻塞 ——
	//
	// 两个 stage 各管一半（复审 RR-M01 的等待侧）：
	//   gate_queued ⇒ 写者已经进到产品之内、正站在与删除者同一道门禁的申请之前。
	//                 **这条信号由产品实现发出**，不再由测试在调用产品之前自己 close，
	//                 于是"发完信号就在产品调用之前停调度"那条反例不再可能；
	//   set_task_category ⇒ 写者已经进到门禁**之内**（该屏障在门禁之后）。删除者还持有
	//                 门禁时它一旦命中，就是两侧没有共用门禁的缺陷形态。
	writerArrived := make(chan raceHookCall, 4)
	writerProbes := newRaceProbeBus()
	writerGuard := newRaceGuard(t, "引用写者（门禁等待者）")
	t.Cleanup(func() { SetTaskCategoryWriterTestHook(nil) })
	SetTaskCategoryWriterTestHook(func(stage string, _ int64) {
		if stage == categoryGateQueuedStage {
			writerProbes.signal(stage).fire()
			return
		}
		select {
		case writerArrived <- raceHookCall{stage: stage}:
		default:
		}
	})

	writerDone := make(chan error, 1)
	writerGuard.start(func() {
		_, err := writer.SetTaskCategory(taskID, &catID)
		writerDone <- err
	})

	// 正向事实 ②：写者确实已经站在同一道门禁的申请之前（拿不到 ⇒ 后面的观察毫无意义）。
	select {
	case <-writerProbes.signal(categoryGateQueuedStage).C():
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("引用写者未经等待就带着错误结束了: %v", err)
		}
		t.Fatalf("引用写者未经等待就提交完成：它绕开了共同 Business Gate")
	case <-time.After(raceBudget):
		t.Fatalf("引用写者在 %s 内没有抵达门禁的争用点（产品未发出 %q），本用例的握手无法建立",
			raceBudget, categoryGateQueuedStage)
	}

	// 删除者持门禁期间：写者既不许抵达自己的屏障，也不许跑完。
	select {
	case call := <-writerArrived:
		t.Fatalf("删除者还持有共同 Business Gate，引用写者却已经抵达自己的屏障（stage=%s）："+
			"两边没有共用同一道门禁", call.stage)
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("删除者持有共同门禁期间引用写者带着错误结束了: %v", err)
		}
		t.Fatalf("删除者持有共同 Business Gate 期间，SetTaskCategory 竟然已经提交完成：" +
			"引用写者绕开了共同门禁（M-R01 的缺陷形态）")
	case <-time.After(raceGateWindow):
	}

	// 放行持有者：等待者必须**立刻**结束（它刚才就是排在这道门禁上）。
	releasedAt := time.Now()
	releaseDeleterNow()
	if err := raceJoin(t, "引用写者", writerDone); err != nil {
		t.Logf("放行门禁后引用写者的业务结局（分类可能已被删除，属合法拒绝）: %v", err)
	}
	t.Logf("放行共同门禁后引用写者结束的耗时：%s", time.Since(releasedAt).Truncate(time.Millisecond))
	if err := raceJoin(t, "删除分类（门禁持有者）", deleteDone); err != nil {
		t.Fatalf("放行后删除分类失败: %v", err)
	}

	// —— 产品不变量：分类已不存在，任何任务都不该再引用它 ——
	var dangling int
	if err := fixture.QueryRow(`SELECT count(*) FROM tasks WHERE category_id = ?`, catID).Scan(&dangling); err != nil {
		t.Fatalf("回读悬空引用失败: %v", err)
	}
	if dangling != 0 {
		t.Fatalf("分类 %d 已删除，仍有 %d 个任务引用它：门禁只挡住了一半（对称性破裂）", catID, dangling)
	}
	var categoryLeft int
	if err := fixture.QueryRow(`SELECT count(*) FROM task_categories WHERE id = ?`, catID).Scan(&categoryLeft); err != nil {
		t.Fatalf("回读分类失败: %v", err)
	}
	if categoryLeft != 0 {
		t.Fatalf("分类 %d 未被删除，交错未完整执行", catID)
	}
}

// ---------------------------------------------------------------------------
// 离线（无需 DSN）判定用例
//
// 真库用例只在有 ARTEX_TIDB_DSN 时跑，"证据不足不许给绿"这条纪律本身必须在**没有库**
// 的时候也被验证：复审 RR-M01 明确要求补 blocker 缺失、表 id 缺失、错表、错 blocker
// 这几条反例。它们全部是纯函数判定，离线必跑、恒快、不碰任何数据。
// ---------------------------------------------------------------------------

// TestRaceJudgeLockWaitRequiresEveryPieceOfEvidence 表驱动钉住锁等待关联判定。
func TestRaceJudgeLockWaitRequiresEveryPieceOfEvidence(t *testing.T) {
	const (
		targetTable int64 = 108
		otherTable  int64 = 112
	)
	full := raceLockWaitEvidence{
		waiterSeen: true, waiterState: "LockWaiting", waiterWaiting: 1.5,
		relatedTableIDs: "[112, 108]", blockerOpenTx: true, blockerState: "Running",
	}
	cases := []struct {
		name        string
		evidence    raceLockWaitEvidence
		wantTableID int64
		wantOK      bool
		wantReason  string
	}{
		{
			name: "四条证据齐备", evidence: full, wantTableID: targetTable, wantOK: true,
		},
		{
			name:       "blocker 不在场（等待无法归因到本夹具）",
			evidence:   raceLockWaitEvidence{waiterSeen: true, waiterState: "LockWaiting", relatedTableIDs: "[108]"},
			wantReason: "事务视图里没有 blocker 的打开事务",
		},
		{
			name:        "表 id 缺失（旧实现在这里静默放行任意 LockWaiting）",
			evidence:    full,
			wantTableID: 0,
			wantReason:  "被争用表的物理 id 未知",
		},
		{
			name:        "RELATED_TABLE_IDS 为空串",
			evidence:    raceLockWaitEvidence{waiterSeen: true, waiterState: "LockWaiting", blockerOpenTx: true},
			wantTableID: targetTable,
			wantReason:  "RELATED_TABLE_IDS 为空",
		},
		{
			name:        "等的是别的表",
			evidence:    raceLockWaitEvidence{waiterSeen: true, waiterState: "LockWaiting", blockerOpenTx: true, relatedTableIDs: "[112]"},
			wantTableID: targetTable,
			wantReason:  "不含被争用表",
		},
		{
			name:        "waiter 压根不在视图里",
			evidence:    raceLockWaitEvidence{blockerOpenTx: true, blockerState: "Running"},
			wantTableID: targetTable,
			wantReason:  "事务视图里没有 waiter",
		},
		{
			name:        "waiter 是 Running 而不是 LockWaiting",
			evidence:    raceLockWaitEvidence{waiterSeen: true, waiterState: "Running", blockerOpenTx: true, relatedTableIDs: "[108]"},
			wantTableID: targetTable,
			wantReason:  "不是 LockWaiting",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := raceJudgeLockWait(tc.evidence, tc.wantTableID)
			if ok != tc.wantOK {
				t.Fatalf("判定 = %v（reason=%q），期望 %v", ok, reason, tc.wantOK)
			}
			if !tc.wantOK && !strings.Contains(reason, tc.wantReason) {
				t.Fatalf("缺失原因 = %q，期望包含 %q", reason, tc.wantReason)
			}
		})
	}
}

// TestRaceParseTrxRowsSeparatesWaiterFromBlocker 确认折叠过程不会把两个会话混成一个：
// 错 blocker（视图里只有第三方会话）必须留不下 blockerOpenTx。
func TestRaceParseTrxRowsSeparatesWaiterFromBlocker(t *testing.T) {
	rows := []raceTrxRow{
		{sessionID: 1001, state: "LockWaiting", waiting: 0.4, related: "[108]"},
		{sessionID: 2002, state: "Running", waiting: 0, related: "[108]"},
	}
	ev := raceParseTrxRows(rows, 1001, 3003)
	if !ev.waiterSeen || ev.waiterState != "LockWaiting" {
		t.Fatalf("waiter 证据丢失：%+v", ev)
	}
	if ev.blockerOpenTx {
		t.Fatalf("视图里根本没有 blocker 会话（3003），却判定它在场：%+v", ev)
	}
	if ok, _ := raceJudgeLockWait(ev, 108); ok {
		t.Fatalf("blocker 是第三方会话（2002）时不许判定成立")
	}
	if paired := raceParseTrxRows(rows, 1001, 2002); !paired.blockerOpenTx {
		t.Fatalf("blocker 会话 2002 在视图里，必须判定它在场")
	}
	if ok, reason := raceJudgeLockWait(raceParseTrxRows(rows, 1001, 2002), 108); !ok {
		t.Fatalf("正确配对的四条证据被判不成立：%s", reason)
	}
}

// TestRaceWaitDeleterQueuedRejectsNoSignal 是复审 RR-M01 那条反例的离线版本：
// "有 done、有预算，但产品从未发出排队信号" 必须是**错误**，绝不能被读成 held。
func TestRaceWaitDeleterQueuedRejectsNoSignal(t *testing.T) {
	queued := make(chan struct{})
	deleteDone := make(chan error, 1)

	if entered, err := raceWaitDeleterQueued(queued, deleteDone, 50*time.Millisecond); err == nil {
		t.Fatalf("没有任何排队信号时竟然给了结论（entered=%v）：这就是 5 秒假绿的形态", entered)
	} else if !strings.Contains(err.Error(), categoryGateQueuedStage) {
		t.Fatalf("错误原因没有指向排队探针：%v", err)
	}

	close(queued)
	if entered, err := raceWaitDeleterQueued(queued, deleteDone, 50*time.Millisecond); err != nil || !entered {
		t.Fatalf("排队信号已发出，判定应为已进入：%v", err)
	}

	// 未排队就提交完成：返回 (false, nil)，由调用方判成"绕开门禁"。
	submitted := make(chan error, 1)
	submitted <- nil
	if entered, err := raceWaitDeleterQueued(make(chan struct{}), submitted, 50*time.Millisecond); err != nil || entered {
		t.Fatalf("当场提交应判为未排队且无错误，实际 entered=%v err=%v", entered, err)
	}

	// 未排队就带错误返回：算错误，不算 held。
	failed := make(chan error, 1)
	failed <- errors.New("boom")
	if entered, err := raceWaitDeleterQueued(make(chan struct{}), failed, 50*time.Millisecond); err == nil || entered {
		t.Fatalf("提前失败必须报错，实际 entered=%v err=%v", entered, err)
	}
}

// TestRaceProbeBusIsStageScoped 确认探针总线不会把两个 stage 混成一个信号：
// gate_queued（排队）命中绝不能让 in_gate（越门禁）也变成"已发生"。
func TestRaceProbeBusIsStageScoped(t *testing.T) {
	bus := newRaceProbeBus()
	if bus.fired(categoryGateQueuedStage) || bus.fired(categoryInGateStage) {
		t.Fatalf("新总线上不该有任何已命中的 stage")
	}
	bus.signal(categoryGateQueuedStage).fire()
	bus.signal(categoryGateQueuedStage).fire() // 幂等：重复命中不得 panic
	if !bus.fired(categoryGateQueuedStage) {
		t.Fatalf("gate_queued 命中后必须可读到")
	}
	if bus.fired(categoryInGateStage) {
		t.Fatalf("in_gate 没有命中过，却读到了：stage 之间串了")
	}
	select {
	case <-bus.signal(categoryGateQueuedStage).C():
	default:
		t.Fatalf("close 语义失效：已命中的信号必须能被任意时刻的 select 读到")
	}
}

// TestRaceOwnedFanOutOutcomeRejectsForeignCount 钉住受控领取的计数契约：
// 只有 0/1 是合法值，任何别的值都必须落到 violation（它意味着 SQL 里的归属绑定被改坏）。
func TestRaceOwnedFanOutOutcomeRejectsForeignCount(t *testing.T) {
	if got := raceOwnedFanOutOutcome(2); got != "violation" {
		t.Fatalf("越界计数 2 的分类 = %q，期望 violation", got)
	}
	if got := raceOwnedFanOutOutcome(-1); got != "violation" {
		t.Fatalf("负数计数 %d 的分类 = %q，期望 violation", -1, got)
	}
	if got := raceOwnedFanOutOutcome(1); got != "owned" {
		t.Fatalf("领到自有事件的分类 = %q，期望 owned", got)
	}
	if got := raceOwnedFanOutOutcome(0); got != "stolen" {
		t.Fatalf("被外部消费者抢先的分类 = %q，期望 stolen", got)
	}
}
