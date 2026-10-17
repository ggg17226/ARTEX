package lockorder_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

// ---------------------------------------------------------------------------
// R2 I8 测试真实性强化说明（本文件的全部设计决策都围绕"不许假绿"）
//
//	① 观测器独立于被测分类器：conflictObserver 只认 *mysql.MySQLError 的结构化编号，
//	   且**在每一次 payload 尝试内**记录错误，而不是只看 Run 的最终返回值 ——
//	   TxRunner.Run 默认 5 次尝试 + 指数退避会把瞬态 1213 吞成 err==nil。
//	② 双重"零死锁"证据：payload 逐次尝试的 1213 计数 == 0，
//	   且 runner 退避休眠被调用的次数（= 重试次数）== 0，
//	   后者是兜底：任何阶段（含 payload 之外的 DB 门禁加锁）产生的可重试冲突都会触发退避。
//	③ 校准用例（TestDeadlockImmunity_TiDBRawOrderDeadlocks）：绕开全序排序器、
//	   按互为反序的业务序直接 FOR UPDATE，**必须**观测到 1213。
//	   它证明观测器不是瞎的，主用例的"零 1213"才有意义。
//	④ 真实执行链路：fresh locator（独立只读事务）→ LockAndVerifyWithReLocate
//	   （真实 FOR UPDATE 加锁 → 持锁 current read 重定位成员集 → 逐实体当前态核验），
//	   不再出现"固定 Live:true 的假 verifier"。
//	⑤ 行锁真实性探针：worker 持锁期间，测试主线程用**第三个显式事务**
//	   FOR UPDATE NOWAIT 探测，必须拿到 3572（TiDB：锁无法立即取得）。
//	   注意 TiDB 在 autocommit 下对 SELECT ... FOR UPDATE 不做持续加锁，
//	   因此探针必须在显式事务内，否则会得到"加锁成功"的假象。
//	⑥ 1205 场景显式把会话级 innodb_lock_wait_timeout 压到 1s，
//	   严禁把 TiDB 默认 50s 当成测试预算。
//	⑦ 并发用 barrier 同步起跑；测试行按前缀隔离并在 Cleanup 中删除 + 复核零残留。
// ---------------------------------------------------------------------------

const (
	mysqlNumDuplicateEntry     uint16 = 1062 // ER_DUP_ENTRY
	mysqlNumLockWaitTimeout    uint16 = 1205 // ER_LOCK_WAIT_TIMEOUT
	mysqlNumDeadlock           uint16 = 1213 // ER_LOCK_DEADLOCK
	mysqlNumUnsupportedFeature uint16 = 1235 // ER_UNSUPPORTED_ACTION：TiDB 未启用的 noop 函数
	mysqlNumLockNowaitAborted  uint16 = 3572 // TiDB: NOWAIT 无法立即取得锁
)

// mysqlErrNum 测试侧独立的结构化错误编号提取。
// 刻意不复用被测包内部的 sqlErrorNumber：观测器必须独立于被测分类器，
// 否则"分类器漏判 1213"会让观测器同时失明，形成假绿（R2 I6/I8 同源要求）。
func mysqlErrNum(err error) (uint16, bool) {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number, true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// 冲突观测器
// ---------------------------------------------------------------------------

// conflictObserver 按 MySQL/TiDB 结构化错误编号统计冲突。
// 只统计 *mysql.MySQLError：错误文本里出现 "1213"/"Deadlock" 一律不计入。
type conflictObserver struct {
	mu       sync.Mutex
	byNumber map[uint16]int
	other    []string
}

func newConflictObserver() *conflictObserver {
	return &conflictObserver{byNumber: make(map[uint16]int)}
}

func (o *conflictObserver) observe(err error) {
	if err == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if n, ok := mysqlErrNum(err); ok {
		o.byNumber[n]++
		return
	}
	o.other = append(o.other, err.Error())
}

func (o *conflictObserver) count(number uint16) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.byNumber[number]
}

// report 返回"编号=次数"的紧凑摘要，供失败信息与验收日志使用。
func (o *conflictObserver) report() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.byNumber) == 0 && len(o.other) == 0 {
		return "无任何结构化冲突"
	}
	nums := make([]uint16, 0, len(o.byNumber))
	for n := range o.byNumber {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	parts := make([]string, 0, len(nums)+1)
	for _, n := range nums {
		parts = append(parts, fmt.Sprintf("%d=%d", n, o.byNumber[n]))
	}
	if len(o.other) > 0 {
		parts = append(parts, fmt.Sprintf("无编号错误 %d 条（首条：%s）", len(o.other), o.other[0]))
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------------------
// 真实 TiDB 环境辅助
// ---------------------------------------------------------------------------

// openTestTiDB 打开真实 TiDB 连接池；缺 DSN 或 Ping 不通一律 Honest Skip
// （明确标记"未验证"，绝不伪装成通过）。
//
// extraParams 形如 "innodb_lock_wait_timeout=1"：go-sql-driver 会在**每条新建连接**上
// 下发对应 SET 语句，因此只影响本测试自己的会话，不改动数据库全局设置。
func openTestTiDB(t *testing.T, extraParams ...string) *sql.DB {
	t.Helper()

	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("Honest Skip：环境变量 ARTEX_TIDB_DSN 未配置，本用例「未验证」，不代表通过")
	}
	for _, p := range extraParams {
		dsn += "&" + p
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open tidb dsn: %v", err)
	}
	// 与 db/tidb 生产池上限对齐（本包不得反向依赖 db/tidb，故直接写常量）
	db.SetMaxOpenConns(32)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("Honest Skip：TiDB 无法 Ping 通 (%v)，本用例「未验证」，不代表通过", err)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}

// registerTiDBCleanup 注册"按前缀删除测试行 + 复核零残留"。
//
// 注册顺序刻意相反于执行顺序（testing 的 Cleanup 是 LIFO）：
// 先注册复核 → 最后执行；后注册删除 → 先执行。
// 也即：删除 → 复核 → （更早注册的）db.Close。
func registerTiDBCleanup(t *testing.T, db *sql.DB, prefix string) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var left int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM locks WHERE name LIKE ?", prefix+"%").Scan(&left); err != nil {
			t.Errorf("cleanup 复核失败（前缀 %s）: %v", prefix, err)
			return
		}
		if left != 0 {
			t.Errorf("cleanup 不健全：locks 表仍残留 %d 行（前缀 %s）", left, prefix)
		}
	})

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, err := db.ExecContext(ctx, "DELETE FROM locks WHERE name LIKE ?", prefix+"%")
		if err != nil {
			t.Errorf("cleanup 删除测试行失败（前缀 %s）: %v", prefix, err)
			return
		}
		if n, _ := res.RowsAffected(); n > 0 {
			t.Logf("cleanup：已删除 %d 行测试数据（前缀 %s）", n, prefix)
		}
	})
}

// mustInsertLockRows 在真实 locks 表插入测试行，返回与入参同序的物理 row_id。
func mustInsertLockRows(t *testing.T, ctx context.Context, db *sql.DB, names []string) []uint64 {
	t.Helper()

	for _, n := range names {
		if _, err := db.ExecContext(ctx, "INSERT INTO locks (name) VALUES (?)", n); err != nil {
			t.Fatalf("准备测试行 %s: %v", n, err)
		}
	}

	ids := make([]uint64, len(names))
	seen := make(map[uint64]string, len(names))
	for i, n := range names {
		if err := db.QueryRowContext(ctx, "SELECT row_id FROM locks WHERE name = ?", n).Scan(&ids[i]); err != nil {
			t.Fatalf("查询测试行 %s 的物理主键: %v", n, err)
		}
		if prev, dup := seen[ids[i]]; dup {
			t.Fatalf("测试行物理主键冲突：%s 与 %s 同为 row_id=%d", prev, n, ids[i])
		}
		seen[ids[i]] = n
	}
	return ids
}

// locateLockRowsByNames 以 name 逻辑成员谓词在 locks 表定位实体行。
//
//   - forUpdate=false：fresh locator 的纯读快照（独立只读事务，MVCC 快照读，绝不加锁）；
//   - forUpdate=true ：持锁事务内的 current read 重定位。REPEATABLE READ 的普通 SELECT
//     读的是本事务旧快照，发现不了新成员，因此重定位必须显式锁定读（R2 C2 契约）。
//
// Rows 显式 Close + Err 检查（R2 I8：不得只断言返回值）。
func locateLockRowsByNames(
	ctx context.Context,
	q lockorder.ReLocateRower,
	meta lockorder.TableMeta,
	names []string,
	forUpdate bool,
) (*lockorder.LocatorSnapshot, error) {
	if len(names) == 0 {
		return &lockorder.LocatorSnapshot{}, nil
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT %s, name FROM %s WHERE name IN (", meta.PKColumn, meta.Name)
	args := make([]any, 0, len(names))
	for i, n := range names {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
		args = append(args, n)
	}
	sb.WriteString(")")
	if forUpdate {
		sb.WriteString(" FOR UPDATE")
	}

	rows, err := q.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("locate locks rows by name: %w", err)
	}
	defer rows.Close()

	snapshot := &lockorder.LocatorSnapshot{StableIdentities: make(map[lockorder.EntityKey]string)}
	for rows.Next() {
		var pk uint64
		var name string
		if err := rows.Scan(&pk, &name); err != nil {
			return nil, fmt.Errorf("scan located locks row: %w", err)
		}
		key := lockorder.EntityKey{Table: meta.Order, PK: pk}
		snapshot.CandidateKeys = append(snapshot.CandidateKeys, key)
		snapshot.StableIdentities[key] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate located locks rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close located locks rows: %w", err)
	}
	return snapshot, nil
}

// ---------------------------------------------------------------------------
// fresh locator：被测实现优先，真实环境不兼容时显式退回等价实现
// ---------------------------------------------------------------------------

// freshLocatorFunc 每次调用都从连接池另开一条**独立新鲜事务**定位候选集。
type freshLocatorFunc func(ctx context.Context) (*lockorder.LocatorSnapshot, error)

// holdSignal 探针握手信号：探针 worker 持锁成功后通知测试主线程来探测行锁，
// 主线程探测完毕再放行该 worker 提交。once 保证 held 只被关闭一次
// （探针 worker 万一发生重试，不会重复 close 造成 panic）。
type holdSignal struct {
	once   sync.Once
	held   chan struct{}
	resume chan struct{}
}

func (s *holdSignal) signalHeld() { s.once.Do(func() { close(s.held) }) }

// newPackageFreshLocator 走被测实现 lockorder.ExecuteFreshLocator
// （P04b Architecture 3：独立只读事务 + 显式提交 + 自动排序去重）。
func newPackageFreshLocator(db *sql.DB, meta lockorder.TableMeta, names []string, calls *int64) freshLocatorFunc {
	return func(ctx context.Context) (*lockorder.LocatorSnapshot, error) {
		atomic.AddInt64(calls, 1)
		snapshot, err := lockorder.ExecuteFreshLocator(ctx, db, func(lctx context.Context, locTx *sql.Tx) (*lockorder.LocatorSnapshot, error) {
			return locateLockRowsByNames(lctx, locTx, meta, names, false)
		})
		if err != nil {
			return nil, fmt.Errorf("fresh locator: %w", err)
		}
		return snapshot, nil
	}
}

// newFallbackFreshLocator 与 ExecuteFreshLocator 等价的测试侧实现：
// 同池另开独立事务 → 纯读定位 → 显式 Commit 归还连接 → 排序去重。
//
// 唯一差异是不带 START TRANSACTION READ ONLY 声明。TiDB 自述 READ ONLY 是 noop 实现
// （见 probeFreshLocatorCapability），因此快照新鲜度、连接独立性与"定位不加锁"
// 三条性质与本用例的用途完全等价。
func newFallbackFreshLocator(db *sql.DB, meta lockorder.TableMeta, names []string, calls *int64) freshLocatorFunc {
	return func(ctx context.Context) (*lockorder.LocatorSnapshot, error) {
		atomic.AddInt64(calls, 1)

		locTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("fallback fresh locator begin tx: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = locTx.Rollback()
			}
		}()

		snapshot, err := locateLockRowsByNames(ctx, locTx, meta, names, false)
		if err != nil {
			return nil, err
		}
		if err := locTx.Commit(); err != nil {
			return nil, fmt.Errorf("fallback fresh locator commit: %w", err)
		}
		committed = true

		snapshot.CandidateKeys = lockorder.SortEntityKeys(snapshot.CandidateKeys)
		if snapshot.StableIdentities == nil {
			snapshot.StableIdentities = make(map[lockorder.EntityKey]string)
		}
		if snapshot.CreatedAt.IsZero() {
			snapshot.CreatedAt = time.Now().UTC()
		}
		return snapshot, nil
	}
}

// newFreshLocatorFor 按能力探测结果选择定位器实现（被测实现优先）。
func newFreshLocatorFor(
	db *sql.DB,
	meta lockorder.TableMeta,
	names []string,
	calls *int64,
	usePackageImpl bool,
) freshLocatorFunc {
	if usePackageImpl {
		return newPackageFreshLocator(db, meta, names, calls)
	}
	return newFallbackFreshLocator(db, meta, names, calls)
}

// probeFreshLocatorCapability 用生产 DSN（不启用任何 noop 函数）实测被测 fresh locator
// 在目标 TiDB 上是否可用；返回 true 表示可以直接使用被测实现。
//
// 实测事实（TiDB v8.5.8 Community，tidb_enable_noop_functions=OFF）：
//
//	BeginTx(&sql.TxOptions{ReadOnly: true}) → Error 1235 (42000):
//	function READ ONLY has only noop implementation in tidb now,
//	use tidb_enable_noop_functions to enable these functions
//
// 即 lockorder.ExecuteFreshLocator 在该真实环境下**必然失败**，属生产侧缺陷
// （本用例不改数据库设置、也不改被测代码，只把它如实暴露出来）。
// 命中该已知缺陷时退回 newFallbackFreshLocator，使零死锁验证仍能真实执行；
// 其他错误一律直接失败，绝不静默降级。
func probeFreshLocatorCapability(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	meta lockorder.TableMeta,
	names []string,
) bool {
	t.Helper()

	probe := newPackageFreshLocator(db, meta, names, new(int64))
	_, err := probe(ctx)
	if err == nil {
		t.Logf("fresh locator：被测 lockorder.ExecuteFreshLocator 在目标 TiDB 上可用（READ ONLY 事务被接受）")
		return true
	}

	if n, ok := mysqlErrNum(err); ok && n == mysqlNumUnsupportedFeature {
		t.Logf("⚠️ 真实环境缺陷（生产侧待修，非本用例可修范围）：lockorder.ExecuteFreshLocator "+
			"在目标 TiDB 上不可用 → %v\n"+
			"    原因：TiDB 把 START TRANSACTION READ ONLY 归为 noop 函数，tidb_enable_noop_functions=OFF "+
			"时直接拒绝，P04b 的只读定位事务在真实 TiDB 上从未可用。\n"+
			"    处置：本用例退回等价定位器（同池独立事务 + 纯读 + 显式提交 + 排序去重；TiDB 自述 "+
			"READ ONLY 为 noop，快照新鲜度与不加锁性质等价），零死锁/零重试验证照常真实执行；"+
			"该缺陷已写入 w6d-report.md 等待生产侧修复。", err)
		return false
	}

	t.Fatalf("fresh locator 能力探测失败（非已知的 1235 缺陷，不得降级绕过）: %v", err)
	return false
}

// ---------------------------------------------------------------------------
// 单元测试：全序排序器 + LockContext 阶段校验（纯内存，不连库）
// ---------------------------------------------------------------------------

// TestDeadlockImmunity_UnitReverseOrder 验证反序业务请求经 SortEntityKeys 后
// 收敛到同一加锁全序，且 LockContext 拒绝任何逆序/重复登记。
//
// 范围声明（R2 I8 第 1 条）：本用例是**纯内存的锁序性质测试**，不含 goroutine、
// 不含真实行锁，因此它本身**不构成**"并发死锁免疫"的证据；真实并发免疫性由
// TestDeadlockImmunity_TiDBIntegration 在真实 TiDB 上验证，
// 并由 TestDeadlockImmunity_TiDBRawOrderDeadlocks 校准"反序确实会死锁"。
func TestDeadlockImmunity_UnitReverseOrder(t *testing.T) {
	reqA := []lockorder.EntityKey{
		{Table: lockorder.TableOrderTasks, PK: 10},
		{Table: lockorder.TableOrderCompanies, PK: 5},
	}
	reqB := []lockorder.EntityKey{
		{Table: lockorder.TableOrderCompanies, PK: 5},
		{Table: lockorder.TableOrderTasks, PK: 10},
	}

	sortedA := lockorder.SortEntityKeys(reqA)
	sortedB := lockorder.SortEntityKeys(reqB)

	if len(sortedA) != 2 || len(sortedB) != 2 {
		t.Fatalf("unexpected sorted lengths")
	}
	for i := 0; i < 2; i++ {
		if sortedA[i] != sortedB[i] {
			t.Fatalf("expected identical total order at slot %d: A=%+v, B=%+v", i, sortedA[i], sortedB[i])
		}
	}
	// 第一位均为 Tasks (TableOrder 1)，第二位均为 Companies (TableOrder 5)
	if sortedA[0].Table != lockorder.TableOrderTasks || sortedA[1].Table != lockorder.TableOrderCompanies {
		t.Errorf("expected Tasks then Companies, got %+v", sortedA)
	}

	// LockContext 必须拒绝逆序与重复登记：这是"已按全序加锁"的运行时防线
	lockCtx := lockorder.NewLockContext()
	if err := lockCtx.RecordEntityLock(sortedA[0]); err != nil {
		t.Fatalf("record first lock: %v", err)
	}
	if err := lockCtx.RecordEntityLock(sortedA[0]); err == nil {
		t.Error("expected duplicate entity lock to be rejected")
	}
	if err := lockCtx.RecordEntityLock(sortedA[1]); err != nil {
		t.Fatalf("record second lock: %v", err)
	}
	if err := lockCtx.RecordEntityLock(sortedA[0]); err == nil {
		t.Error("expected backwards entity lock to be rejected")
	}
	if !lockorder.IsSorted(sortedA) {
		t.Error("expected sorted candidate set to satisfy IsSorted")
	}
}

// ---------------------------------------------------------------------------
// 真实 TiDB：校准用例（证明 1213 观测器不是瞎的）
// ---------------------------------------------------------------------------

// TestDeadlockImmunity_TiDBRawOrderDeadlocks 绕开全序排序器，让两个事务按**互为反序**的
// 业务序直接 FOR UPDATE 同一组真实行，必须观测到 1213 死锁。
//
// 本用例的存在理由：主用例断言"零 1213"，而"零"既可能是真的没死锁，也可能是观测器
// 根本没抓到。本用例用握手同步制造确定的循环等待，证明同一套观测器能抓到 1213，
// 从而让主用例的"零 1213"成为有效证据（R2 I8：绿色不能当真实性通过）。
//
// 会话级 innodb_lock_wait_timeout 压到 3s：万一死锁探测器未介入，最多等 3s 得到 1205，
// 绝不出现 TiDB 默认 50s 的干等。
func TestDeadlockImmunity_TiDBRawOrderDeadlocks(t *testing.T) {
	db := openTestTiDB(t, "innodb_lock_wait_timeout=3")

	prefix := fmt.Sprintf("w6dcal_%d_", time.Now().UnixNano()%100000000)
	registerTiDBCleanup(t, db, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	names := []string{prefix + "row_0", prefix + "row_1"}
	ids := mustInsertLockRows(t, ctx, db, names)
	lo, hi := ids[0], ids[1]
	if hi < lo {
		lo, hi = hi, lo
	}
	// worker0 业务序 (lo, hi)，worker1 业务序 (hi, lo)：不加全序排序器即形成循环等待
	order := [2][2]uint64{{lo, hi}, {hi, lo}}

	obs := newConflictObserver()
	const rounds = 3
	start := time.Now()

	for round := 0; round < rounds; round++ {
		var holding sync.WaitGroup
		holding.Add(2)
		fire := make(chan struct{})
		errs := make([]error, 2)

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				announced := false
				markHeld := func() {
					if !announced {
						announced = true
						holding.Done()
					}
				}
				defer markHeld()

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errs[i] = fmt.Errorf("begin: %w", err)
					return
				}
				defer func() { _ = tx.Rollback() }()

				var x uint64
				if err := tx.QueryRowContext(ctx,
					"SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE", order[i][0]).Scan(&x); err != nil {
					errs[i] = fmt.Errorf("first lock row %d: %w", order[i][0], err)
					return
				}
				markHeld()
				<-fire // 双方都持有各自的行之后同时发起第二个加锁请求
				if err := tx.QueryRowContext(ctx,
					"SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE", order[i][1]).Scan(&x); err != nil {
					errs[i] = err
				}
			}(i)
		}

		holding.Wait()
		close(fire)
		wg.Wait()

		for i := range errs {
			obs.observe(errs[i])
		}
	}

	deadlocks := obs.count(mysqlNumDeadlock)
	lockTimeouts := obs.count(mysqlNumLockWaitTimeout)
	t.Logf("反序加锁校准：%d 轮，耗时 %v，冲突统计 [%s]",
		rounds, time.Since(start).Round(time.Millisecond), obs.report())

	if deadlocks == 0 {
		t.Fatalf("校准失败：反序交叉加锁 %d 轮未观测到任何 1213 死锁（1205 次数 %d，统计 [%s]）；"+
			"观测器失效时主用例的「零 1213」不成立，必须先修复观测器",
			rounds, lockTimeouts, obs.report())
	}
	if lockTimeouts != 0 {
		t.Logf("提示：出现 %d 次 1205（死锁探测器未及时介入，属 TiDB 侧调度），不影响校准结论", lockTimeouts)
	}
	if !lockorder.IsRetryableConflict(&mysql.MySQLError{Number: mysqlNumDeadlock, Message: "Deadlock found"}) {
		t.Error("被测分类器未把 1213 归入可重试冲突")
	}
}

// ---------------------------------------------------------------------------
// 真实 TiDB：主用例（全序 + fresh locator + 真实复核，零 1213 零重试）
// ---------------------------------------------------------------------------

// TestDeadlockImmunity_TiDBIntegration 在真实外部 TiDB 上执行高并发交叉乱序加锁，
// 断言全序加锁**零死锁、零重试**。
//
// 与旧实现（R2 I8 指出的假绿）的逐条差异：
//  1. 候选集来自 lockorder.ExecuteFreshLocator 的**独立只读事务**，不是预读的 row_id 常量；
//  2. 加锁走 lockorder.LockAndVerifyWithReLocate：真实 FOR UPDATE → 持锁 current read
//     重定位成员集 → 逐实体当前态核验，verifier 真实查库并回填稳定身份，不再固定 Live:true；
//  3. 走本地 + DB namespace 门禁（每 worker 独立 namespace，避免门禁把并发串行化）；
//  4. 每一次 payload 尝试的错误都进观测器，并断言 runner 退避重试次数为 0
//     —— 只有"首次尝试即成功"才可能零重试，重试被吞掉的路径被封死；
//  5. barrier 同步起跑，持锁窗口内用第三个显式事务 NOWAIT 探针证明行锁真实存在。
func TestDeadlockImmunity_TiDBIntegration(t *testing.T) {
	const (
		entityRowCount    = 4
		concurrentWorkers = 8
		rowsPerWorker     = 3
		probeWorker       = 0
	)

	db := openTestTiDB(t)
	probeDB := openTestTiDB(t) // 探针专用池：确保探测语句不会复用 worker 的连接

	prefix := fmt.Sprintf("w6dit_%d_", time.Now().UnixNano()%100000000)
	registerTiDBCleanup(t, db, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	metaLocks, err := lockorder.LookupTableName("locks")
	if err != nil {
		t.Fatalf("lookup locks meta: %v", err)
	}

	// 1. 真实测试行：entityRowCount 行参与并发加锁；另插 1 行"任何 worker 都不会申请"
	//    的对照行，用来证明 NOWAIT 探针本身不会误报（未被持有的行必须能立即取得锁）。
	names := make([]string, entityRowCount)
	for i := range names {
		names[i] = fmt.Sprintf("%srow_%d", prefix, i)
	}
	controlName := prefix + "row_free"
	allNames := append(append(make([]string, 0, entityRowCount+1), names...), controlName)
	allIDs := mustInsertLockRows(t, ctx, db, allNames)
	ids := allIDs[:entityRowCount]
	freeID := allIDs[entityRowCount]

	// 2. fresh locator 能力探测：优先使用被测实现，仅在命中已知的 TiDB READ ONLY
	//    noop 缺陷（1235）时才退回等价实现，并在日志中显式暴露该缺陷。
	usePackageLocator := probeFreshLocatorCapability(t, ctx, db, metaLocks, names)

	// 业务序刻意取"物理 PK 降序"：每个 worker 的候选请求顺序都与全序完全相反，
	// 且窗口在相邻 worker 间重叠，最大化交叉等待。
	descIdx := make([]int, entityRowCount)
	for i := range descIdx {
		descIdx[i] = i
	}
	sort.Slice(descIdx, func(a, b int) bool { return ids[descIdx[a]] > ids[descIdx[b]] })

	businessNames := func(workerID int) []string {
		out := make([]string, 0, rowsPerWorker)
		for k := 0; k < rowsPerWorker; k++ {
			out = append(out, names[descIdx[(workerID+k)%entityRowCount]])
		}
		return out
	}

	// 3. runner：注入 sleeper 以精确统计退避次数（= 重试次数）
	runner := lockorder.NewTxRunner(db)
	var retryAttempts int64
	runner.SetSleeper(func(time.Duration) { atomic.AddInt64(&retryAttempts, 1) })

	var (
		locatorCalls    int64
		relocateCalls   int64
		verifierCalls   int64
		payloadAttempts int64
	)
	obs := newConflictObserver()

	probeSignal := &holdSignal{held: make(chan struct{}), resume: make(chan struct{})}
	start := make(chan struct{})

	var wg sync.WaitGroup
	errCh := make(chan error, concurrentWorkers)

	for w := 0; w < concurrentWorkers; w++ {
		workerID := w
		bizNames := businessNames(workerID)
		locate := newFreshLocatorFor(db, metaLocks, bizNames, &locatorCalls, usePackageLocator)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // barrier：所有 worker 同时起跑

			ns := fmt.Sprintf("%sns_%d", prefix, workerID)
			err := runner.Run(ctx, lockorder.TxRunnerOptions{
				RequiresBusinessGate: false,
				RequiredNamespaces:   []string{ns},
			}, func(txCtx context.Context, tx *sql.Tx, lockCtx *lockorder.LockContext) error {
				atomic.AddInt64(&payloadAttempts, 1)
				attemptErr := workerAttempt(txCtx, tx, lockCtx, locate, metaLocks, bizNames,
					workerID == probeWorker, 5+3*(workerID%4), probeSignal,
					&relocateCalls, &verifierCalls)
				// 关键：逐次尝试都进观测器，Run 的最终返回值不能反映被重试吞掉的 1213
				obs.observe(attemptErr)
				return attemptErr
			})

			if err != nil {
				obs.observe(err)
				errCh <- fmt.Errorf("worker %d: %w", workerID, err)
			}
		}()
	}

	close(start)

	// 4. 持锁探针：等探针 worker 报告"已持有实体锁"，用第三个显式事务验证行锁真实存在
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-probeSignal.held:
		probeIDs := make([]uint64, 0, rowsPerWorker)
		heldSet := make(map[string]struct{}, rowsPerWorker)
		for _, n := range businessNames(probeWorker) {
			heldSet[n] = struct{}{}
		}
		for i, n := range names {
			if _, ok := heldSet[n]; ok {
				probeIDs = append(probeIDs, ids[i])
			}
		}
		if len(probeIDs) != rowsPerWorker {
			t.Errorf("探针待验证的持锁行数 %d，期望 %d", len(probeIDs), rowsPerWorker)
		}
		probeRealRowLocksHeldByTx(t, probeDB, ctx, probeIDs, freeID)
		close(probeSignal.resume)
	case <-done:
		t.Fatal("探针 worker 未进入持锁阶段即结束，无法验证行锁真实性")
	case <-time.After(90 * time.Second):
		t.Fatal("等待探针 worker 持锁超时")
	}

	<-done
	close(errCh)

	for err := range errCh {
		t.Errorf("worker 在并发交叉乱序加锁下失败: %v", err)
	}

	// 5. 零死锁 / 零重试断言
	if n := obs.count(mysqlNumDeadlock); n != 0 {
		t.Errorf("观测到 %d 次 1213 死锁：全序加锁的并发免疫性不成立（统计 [%s]）", n, obs.report())
	}
	if n := obs.count(mysqlNumLockWaitTimeout); n != 0 {
		t.Errorf("观测到 %d 次 1205 锁等待超时（统计 [%s]）", n, obs.report())
	}
	if n := obs.count(mysqlNumDuplicateEntry); n != 0 {
		t.Errorf("观测到 %d 次 1062 唯一键冲突（统计 [%s]）", n, obs.report())
	}
	if n := atomic.LoadInt64(&retryAttempts); n != 0 {
		t.Errorf("runner 退避重试 %d 次：全序加锁要求每个 worker 首次尝试即成功，"+
			"任何重试都可能把瞬态 1213 掩盖成最终 err==nil", n)
	}
	if got, want := atomic.LoadInt64(&payloadAttempts), int64(concurrentWorkers); got != want {
		t.Errorf("payload 执行 %d 次，期望 %d 次（每 worker 恰好 1 次尝试）", got, want)
	}
	if got, want := atomic.LoadInt64(&locatorCalls), int64(concurrentWorkers); got != want {
		t.Errorf("fresh locator 执行 %d 次，期望 %d 次（每 worker 每次尝试都要真实定位）", got, want)
	}
	if got, want := atomic.LoadInt64(&relocateCalls), int64(concurrentWorkers); got != want {
		t.Errorf("持锁重定位执行 %d 次，期望 %d 次", got, want)
	}
	if got, want := atomic.LoadInt64(&verifierCalls), int64(concurrentWorkers*rowsPerWorker); got != want {
		t.Errorf("当前态核验 SQL 执行 %d 次，期望 %d 次", got, want)
	}

	locatorImpl := "lockorder.ExecuteFreshLocator（被测实现）"
	if !usePackageLocator {
		locatorImpl = "测试侧等价实现（被测实现因 TiDB READ ONLY noop 缺陷 1235 不可用）"
	}
	t.Logf("全序加锁验收：%d worker × %d 行（业务序为物理 PK 降序），1213=%d，1205=%d，重试=%d，"+
		"fresh locator=%d，持锁重定位=%d，当前态核验 SQL=%d，定位器实现=%s，冲突统计 [%s]",
		concurrentWorkers, rowsPerWorker, obs.count(mysqlNumDeadlock), obs.count(mysqlNumLockWaitTimeout),
		atomic.LoadInt64(&retryAttempts), atomic.LoadInt64(&locatorCalls),
		atomic.LoadInt64(&relocateCalls), atomic.LoadInt64(&verifierCalls), locatorImpl, obs.report())
}

// workerAttempt 单次事务尝试：fresh locator → 全序加锁 → 持锁重定位 → 当前态核验。
//
// 全部 SQL 都在真实 TiDB 上执行；任何一步失败都原样返回，由 TxRunner 分类决定是否重试。
func workerAttempt(
	txCtx context.Context,
	tx *sql.Tx,
	lockCtx *lockorder.LockContext,
	locate freshLocatorFunc,
	meta lockorder.TableMeta,
	bizNames []string,
	isProbeWorker bool,
	holdMillis int,
	probe *holdSignal,
	relocateCalls, verifierCalls *int64,
) error {
	// ① fresh locator：独立新鲜事务、非加锁快照（P04b Architecture 3）
	snapshot, err := locate(txCtx)
	if err != nil {
		return err
	}
	if len(snapshot.CandidateKeys) != len(bizNames) {
		return fmt.Errorf("fresh locator 返回 %d 个候选，期望 %d 个", len(snapshot.CandidateKeys), len(bizNames))
	}
	if !lockorder.IsSorted(snapshot.CandidateKeys) {
		return errors.New("fresh locator 返回的候选集未满足全序前置条件")
	}

	// ② 当前态核验：真实 current read（本行 X 锁已在手，FOR UPDATE 不额外等待）
	verifier := func(vctx context.Context, q lockorder.QueryRower, k lockorder.EntityKey) (*lockorder.EntityState, error) {
		var name string
		if err := q.QueryRowContext(vctx,
			"SELECT name FROM locks WHERE row_id = ? FOR UPDATE", k.PK).Scan(&name); err != nil {
			return nil, err
		}
		atomic.AddInt64(verifierCalls, 1)
		return &lockorder.EntityState{Key: k, StableIdentity: name, Live: true}, nil
	}

	// ③ 持锁重定位：在同一持锁事务内以 current read 重新枚举成员集（R2 C2）
	relocate := func(rctx context.Context, q lockorder.ReLocateRower) ([]lockorder.EntityKey, error) {
		atomic.AddInt64(relocateCalls, 1)
		current, err := locateLockRowsByNames(rctx, q, meta, bizNames, true)
		if err != nil {
			return nil, err
		}
		return current.CandidateKeys, nil
	}

	states, err := lockorder.LockAndVerifyWithReLocate(txCtx, tx, lockCtx, snapshot, verifier, relocate)
	if err != nil {
		return err
	}
	if len(states) != len(bizNames) {
		return fmt.Errorf("当前态核验返回 %d 条状态，期望 %d 条", len(states), len(bizNames))
	}
	for _, st := range states {
		if st == nil {
			return errors.New("实体当前态为 nil")
		}
		if !st.Live || st.StableIdentity == "" {
			return fmt.Errorf("实体 %s 的当前态不真实：%+v", st.Key, st)
		}
	}

	if isProbeWorker && probe != nil {
		// 持锁期间让测试主线程用第三个事务探测行锁；超时兜底避免死等
		probe.signalHeld()
		select {
		case <-probe.resume:
		case <-time.After(60 * time.Second):
			return errors.New("探针 worker 等待主线程探测超时")
		}
		return nil
	}

	// 非探针 worker：持锁一小段时间，放大交叉等待窗口（顺序一致 → 只会排队，不会成环）
	time.Sleep(time.Duration(holdMillis) * time.Millisecond)
	return nil
}

// probeRealRowLocksHeldByTx 用第三个显式事务证明 worker 的行锁真实存在于 TiDB。
//
// 必须在显式事务内探测：TiDB 在 autocommit 下对 SELECT ... FOR UPDATE 不做持续加锁，
// 直接裸探测会得到"加锁成功"的假象（本机 TiDB v8.5.8 实测）。
// 期望：被持有的行 NOWAIT 立即失败（3572）；未被持有的行 NOWAIT 立即成功。
func probeRealRowLocksHeldByTx(t *testing.T, db *sql.DB, ctx context.Context, heldIDs []uint64, freeID uint64) {
	t.Helper()

	if len(heldIDs) == 0 {
		t.Error("探针未获得任何待探测行")
		return
	}

	for _, id := range heldIDs {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("探针开启事务失败: %v", err)
			continue
		}
		var x uint64
		err = tx.QueryRowContext(ctx,
			"SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE NOWAIT", id).Scan(&x)
		_ = tx.Rollback()

		if err == nil {
			t.Errorf("行 row_id=%d 已被 worker 持有，但第三个事务 FOR UPDATE NOWAIT 仍然成功：行锁未真实生效", id)
			continue
		}
		if n, ok := mysqlErrNum(err); !ok || n != mysqlNumLockNowaitAborted {
			t.Errorf("行 row_id=%d 的 NOWAIT 探测错误 = %v（期望结构化编号 %d）", id, err, mysqlNumLockNowaitAborted)
		}
	}

	if freeID != 0 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("探针开启事务失败: %v", err)
		} else {
			var x uint64
			err = tx.QueryRowContext(ctx,
				"SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE NOWAIT", freeID).Scan(&x)
			_ = tx.Rollback()
			if err != nil {
				t.Errorf("未被持有的行 row_id=%d 的 NOWAIT 探测失败（探针本身不应阻塞）: %v", freeID, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 真实 TiDB：1205 锁等待超时（超时必须极短，严禁等 TiDB 默认 50s）
// ---------------------------------------------------------------------------

// TestDeadlockImmunity_TiDBLockWaitTimeoutBounded 验证 1205 场景在**有界**时间内被观测、
// 归入可重试冲突、并由 runner 退避重试后成功。
//
// 会话级 innodb_lock_wait_timeout 显式压到 1s（仅作用于本测试新建的连接，
// 不改动数据库全局设置）；用例整体预算 20s，远小于 TiDB 默认 50s 行锁等待。
func TestDeadlockImmunity_TiDBLockWaitTimeoutBounded(t *testing.T) {
	const sessionLockWaitSeconds = 1

	db := openTestTiDB(t, fmt.Sprintf("innodb_lock_wait_timeout=%d", sessionLockWaitSeconds))
	holderDB := openTestTiDB(t)

	prefix := fmt.Sprintf("w6d1205_%d_", time.Now().UnixNano()%100000000)
	registerTiDBCleanup(t, db, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 先确认会话超时真的被压低，否则本用例等价于干等 50s，直接失败
	var sessTimeout int
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_lock_wait_timeout").Scan(&sessTimeout); err != nil {
		t.Fatalf("读取会话 innodb_lock_wait_timeout: %v", err)
	}
	if sessTimeout > 2 {
		t.Fatalf("会话 innodb_lock_wait_timeout = %ds，未被压到 1~2s，禁止按 TiDB 默认 50s 预算跑本用例", sessTimeout)
	}

	metaLocks, err := lockorder.LookupTableName("locks")
	if err != nil {
		t.Fatalf("lookup locks meta: %v", err)
	}
	names := []string{prefix + "row_0"}
	ids := mustInsertLockRows(t, ctx, db, names)
	targetID := ids[0]

	// 1. holder：独立连接池上的显式事务，真实持有目标行的 X 锁
	holderConn, err := holderDB.Conn(ctx)
	if err != nil {
		t.Fatalf("holder conn: %v", err)
	}
	defer holderConn.Close()

	holderTx, err := holderConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	var held uint64
	if err := holderTx.QueryRowContext(ctx,
		"SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE", targetID).Scan(&held); err != nil {
		_ = holderTx.Rollback()
		t.Fatalf("holder 加行锁失败: %v", err)
	}

	contended := make(chan struct{})
	var signalOnce sync.Once
	signalContended := func() { signalOnce.Do(func() { close(contended) }) }

	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		select {
		case <-contended:
		case <-time.After(15 * time.Second):
			t.Error("holder 等待 1205 信号超时，强制释放行锁")
		}
		_ = holderTx.Rollback()
	}()

	// 2. waiter：走完整 runner + fresh locator + 全序加锁链路
	runner := lockorder.NewTxRunner(db)
	var retryAttempts int64
	runner.SetSleeper(func(time.Duration) { atomic.AddInt64(&retryAttempts, 1) })

	obs := newConflictObserver()
	var attempts int64
	var locatorCalls int64
	usePackageLocator := probeFreshLocatorCapability(t, ctx, db, metaLocks, names)
	locate := newFreshLocatorFor(db, metaLocks, names, &locatorCalls, usePackageLocator)

	runStart := time.Now()
	runErr := runner.Run(ctx, lockorder.TxRunnerOptions{
		RequiresBusinessGate: false,
		RequiredNamespaces:   []string{prefix + "ns"},
	}, func(txCtx context.Context, tx *sql.Tx, lockCtx *lockorder.LockContext) error {
		atomic.AddInt64(&attempts, 1)
		err := workerAttempt(txCtx, tx, lockCtx, locate, metaLocks, names, false, 0, nil,
			new(int64), new(int64))
		if err != nil {
			obs.observe(err)
			if n, ok := mysqlErrNum(err); ok && n == mysqlNumLockWaitTimeout {
				signalContended()
			}
		}
		return err
	})
	elapsed := time.Since(runStart)

	<-holderDone

	t.Logf("1205 场景：会话超时=%ds，总耗时=%v，尝试=%d 次，退避重试=%d 次，冲突统计 [%s]，最终错误=%v",
		sessTimeout, elapsed.Round(time.Millisecond), atomic.LoadInt64(&attempts),
		atomic.LoadInt64(&retryAttempts), obs.report(), runErr)

	// 3. 断言：1205 被观测、被归类为可重试、重试后成功、且整体耗时远小于默认 50s
	if n := obs.count(mysqlNumLockWaitTimeout); n == 0 {
		t.Errorf("未观测到任何 1205 锁等待超时（统计 [%s]）：行锁阻塞未生效，用例失去意义", obs.report())
	}
	if n := obs.count(mysqlNumDeadlock); n != 0 {
		t.Errorf("1205 场景意外观测到 %d 次 1213 死锁", n)
	}
	if n := atomic.LoadInt64(&retryAttempts); n == 0 {
		t.Error("runner 未对 1205 执行退避重试：IsRetryableConflict 未把 1205 归入可重试冲突")
	}
	if runErr != nil {
		t.Errorf("行锁释放后 runner 仍失败: %v", runErr)
	}
	if elapsed > 20*time.Second {
		t.Errorf("1205 场景耗时 %v，超过 20s 预算：超时配置未生效，疑似退回 TiDB 默认 50s", elapsed)
	}
}
