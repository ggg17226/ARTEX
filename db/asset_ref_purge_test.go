package db

// =====================================================================
// 资产引用完整性回归（W5 · TiDB 基线不建外键）
//
// 这里守的是三条会**永久卡死任务删除**的缺陷：
//  1. AssetStore.DeleteByIDs 只删 assets 本体，把 task_asset_links /
//     exploration_anchors / finding_assets 留在库里成为悬空引用（PG 靠 ON DELETE
//     CASCADE 兜底，TiDB 没有外键）；
//  2. 悬空引用让 DeleteTask 的成员漂移复核永远判定 drift —— 对应任务再也删不掉；
//  3. ExplorationStore.AddNode / Anchor 不校验 asset_ids，一条幻觉 id 就能造出悬空锚点
//     （与已修复的 RecordFindingTx / validateAnchorAssetsTx 属同一缺陷类）。
//
// 两层用例：
//   - 离线语句级守卫（expMock，不连库也跑）：证明"空输入零 SQL、绝不拼 IN ()"、
//     "从属行按物理主键删"、"锁/删顺序符合 lockorder"、"整条路径只有一个 commit"、
//     "资产校验失败时一行都不写"；
//   - 真实库集成（未设置 ARTEX_TIDB_DSN 时诚实 t.Skip）：真删真查，终态必须归零，
//     且资产删除后的两个任务都能被 DeleteTask 正常删除。
//
// 自隔离：所有 fixture 用随机前缀命名，t.Cleanup 逆拓扑清理并回读归零，
// 任何清理失败一律 t.Errorf 判红（共享库里的残留会把下一轮用例踩红）。
// =====================================================================

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 1) 离线守卫：空输入 / 非正数不得产生任何 SQL
// ---------------------------------------------------------------------------

// TestAssetDeleteByIDsIgnoresEmptyAndNonPositiveIDs 证明 DeleteByIDs 在"没有任何合法
// id"时**一条 SQL 都不发**：naive 实现会把空列表拼成 `IN ()`（TiDB 直接语法错误），
// 或把 0 / 负数当真实 id 去锁行、去删引用表。
func TestAssetDeleteByIDsIgnoresEmptyAndNonPositiveIDs(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	as := d.Assets()

	for _, ids := range [][]int64{nil, {}, {0}, {0, -7}, {-1, -2}} {
		eng.resetCalls()
		deleted, err := as.DeleteByIDs(ids)
		if err != nil {
			t.Errorf("DeleteByIDs(%v) 应静默返回 0，实得错误 %v", ids, err)
		}
		if deleted != 0 {
			t.Errorf("DeleteByIDs(%v) 删除数=%d, want 0", ids, deleted)
		}
		if calls := eng.snapshotCalls(); len(calls) != 0 {
			t.Errorf("DeleteByIDs(%v) 必须一条 SQL 都不发，实发 %v", ids, queriesOf(calls))
		}
	}
}

// ---------------------------------------------------------------------------
// 2) 离线守卫：同事务、按物理主键、按 lockorder 顺序清理引用
// ---------------------------------------------------------------------------

// TestAssetDeleteByIDsPurgesReferencesInOneTransaction 是 DeleteByIDs 的语句形状契约：
//
//	锁 assets → 锁/删 exploration_anchors → 锁/删 finding_assets → 锁/删 task_asset_links
//	→ 删 assets 本体，全程一次 commit。
//
// 三条硬约束各挡一类事故：
//   - **单次 commit**：拆成两次提交就会出现"资产已删、link 还在"的悬空窗口（正是本次缺陷）；
//   - **按物理主键删**：`DELETE … WHERE asset_id IN (…)` 是开放范围 DML，会按扫描顺序
//     拿计划外的锁，与任务级联执行器交错时形成反向锁序；
//   - **lockorder 升序**：assets(8) → exploration_anchors(20) → finding_assets(22) →
//     task_asset_links(50)，与 DeleteTask 级联同向，避免两条路径互等。
func TestAssetDeleteByIDsPurgesReferencesInOneTransaction(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	// 非 INSERT 语句的 ROW_COUNT 钉成 2：deleteAssetRowsTx 按批累加，替身要能表达"删了 2 行"。
	eng.forceAffectedRows(2)
	eng.on("SELECT id FROM assets WHERE id IN", []string{"id"},
		[]driver.Value{int64(11)}, []driver.Value{int64(22)})
	eng.on("SELECT row_id FROM exploration_anchors", []string{"row_id"}, []driver.Value{uint64(505)})
	eng.on("SELECT id FROM finding_assets", []string{"id"})
	eng.on("SELECT row_id FROM task_asset_links", []string{"row_id"}, []driver.Value{uint64(707)})

	// 乱序 + 重复 + 非正数：入参必须先规整成「去重升序正数」[11,22]。
	deleted, err := d.Assets().DeleteByIDs([]int64{22, 11, 11, 0, -3})
	if err != nil {
		t.Fatalf("DeleteByIDs: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("删除资产数=%d, want 2", deleted)
	}

	calls := eng.snapshotCalls()
	if len(calls) == 0 {
		t.Fatal("DeleteByIDs 没有发出任何语句")
	}
	for _, c := range calls {
		expAssertOrdinal(t, c)
		upper := strings.ToUpper(c.Query)
		if strings.Contains(upper, "IN ()") {
			t.Errorf("拼出了非法的空 IN: %s", c.Query)
		}
		if strings.HasPrefix(upper, "DELETE") && strings.Contains(c.Query, "asset_id") {
			t.Errorf("引用清理必须按物理主键删，不能发按 asset_id 的裸范围删除: %s", c.Query)
		}
	}

	firstMatch := func(substr string) int {
		for i, c := range calls {
			if strings.Contains(c.Query, substr) {
				return i
			}
		}
		return -1
	}
	// 顺序表：每一项必须严格晚于前一项。
	steps := []string{
		"SELECT id FROM assets WHERE id IN",
		"SELECT row_id FROM exploration_anchors",
		"DELETE FROM exploration_anchors",
		"SELECT id FROM finding_assets",
		"SELECT row_id FROM task_asset_links",
		"DELETE FROM task_asset_links",
		"DELETE FROM assets",
	}
	prev := -1
	for _, step := range steps {
		at := firstMatch(step)
		switch {
		case at < 0:
			t.Fatalf("缺少语句 %q；实际序列: %v", step, queriesOf(calls))
		case at <= prev:
			t.Fatalf("语句 %q 出现在位置 %d，必须晚于前一步 %d；实际序列: %v", step, at, prev, queriesOf(calls))
		}
		prev = at
	}

	// 删除语句必须按物理主键定位，并携带锁读回来的那批主键。
	expectPKDelete(t, calls, "DELETE FROM exploration_anchors", uint64(505))
	expectPKDelete(t, calls, "DELETE FROM task_asset_links", uint64(707))

	if counts := eng.txCounts(); counts.Commits != 1 || counts.Rollbacks != 0 {
		t.Fatalf("引用清理必须与本体删除共享同一次提交: commits=%d rollbacks=%d（引用清理与本体删除必须同事务）",
			counts.Commits, counts.Rollbacks)
	}
}

// expectPKDelete 断言存在一条且仅一条按主键的 DELETE，且参数恰好是给定主键。
func expectPKDelete(t *testing.T, calls []expMockCall, prefix string, want ...driver.Value) {
	t.Helper()
	var hits []expMockCall
	for _, c := range calls {
		if strings.Contains(c.Query, prefix) {
			hits = append(hits, c)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("%q 期望 1 条 DELETE，实得 %d", prefix, len(hits))
	}
	got := hits[0].Args
	if len(got) != len(want) {
		t.Fatalf("%q args=%v, want %v", prefix, got, want)
	}
	for i := range want {
		if fmt.Sprint(got[i]) != fmt.Sprint(want[i]) {
			t.Fatalf("%q arg %d = %v, want %v", prefix, i, got[i], want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// 3) 离线守卫：AddNode / Anchor 的资产存在性校验
// ---------------------------------------------------------------------------

// TestExplorationAnchorValidationRejectsMissingAssets 证明探索图谱的两条锚点写路径
// 都不会再把幻觉 asset id 落库（TiDB 无外键，只能由应用层把关）：
//   - AddNode：任一 anchor 缺失即整体失败，**连 exploration_nodes 都不写**；
//   - Anchor：缺失即 ErrForeignKeyViolation，不写锚点行；
//   - 资产存在时两条路径行为不变（幂等 INSERT + 正常提交）。
func TestExplorationAnchorValidationRejectsMissingAssets(t *testing.T) {
	// AddNode：[11 存在, 22 不存在]
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(9100)
	eng.on("SELECT id FROM assets WHERE id IN", []string{"id"}, []driver.Value{int64(11)})
	if _, err := d.Exploration(501).AddNode("intent", map[string]any{"summary": "x"}, 1, "open", "planner", []int64{11, 22}); !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("缺失资产必须以 ErrForeignKeyViolation 拒绝，实得 %v", err)
	}
	for _, c := range eng.snapshotCalls() {
		if strings.Contains(strings.ToUpper(c.Query), "INSERT") {
			t.Errorf("资产校验失败后不得写任何一行: %s args=%v", c.Query, c.Args)
		}
	}
	if counts := eng.txCounts(); counts.Commits != 0 {
		t.Fatalf("校验失败绝不能提交（commits=%d）", counts.Commits)
	}

	// AddNode：两个 anchor 都存在 → 正常写 2 条锚点
	engOK := newExpMockEngine()
	dOK := openExpMockDB(t, engOK)
	engOK.setNextInsertID(9200)
	engOK.on("SELECT id FROM assets WHERE id IN", []string{"id"},
		[]driver.Value{int64(11)}, []driver.Value{int64(22)})
	if _, err := dOK.Exploration(501).AddNode("intent", map[string]any{"summary": "x"}, 1, "open", "planner", []int64{11, 22}); err != nil {
		t.Fatalf("合法锚定不应失败: %v", err)
	}
	if n := len(engOK.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO exploration_anchors")
	})); n != 2 {
		t.Fatalf("合法锚定应写 2 条锚点，实得 %d", n)
	}

	// Anchor：资产不存在（替身未预置规则 = 空结果集）
	engSingle := newExpMockEngine()
	dSingle := openExpMockDB(t, engSingle)
	if err := dSingle.Exploration(501).Anchor(900, 424242); !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("Anchor 缺失资产必须报错，实得 %v", err)
	}
	for _, c := range engSingle.snapshotCalls() {
		if strings.Contains(c.Query, "INSERT INTO exploration_anchors") {
			t.Errorf("缺失资产的锚点不得落库: %s args=%v", c.Query, c.Args)
		}
	}
	if counts := engSingle.txCounts(); counts.Commits != 0 {
		t.Fatalf("Anchor 校验失败绝不能提交（commits=%d）", counts.Commits)
	}

	// Anchor：资产存在 → 幂等 INSERT 一次并提交
	engAnchorOK := newExpMockEngine()
	dAnchorOK := openExpMockDB(t, engAnchorOK)
	engAnchorOK.on("SELECT id FROM assets WHERE id IN", []string{"id"}, []driver.Value{int64(424242)})
	if err := dAnchorOK.Exploration(501).Anchor(900, 424242); err != nil {
		t.Fatalf("合法 Anchor 不应失败: %v", err)
	}
	if n := len(engAnchorOK.statements(func(c expMockCall) bool {
		return strings.Contains(c.Query, "INSERT INTO exploration_anchors")
	})); n != 1 {
		t.Fatalf("合法 Anchor 应写 1 条锚点，实得 %d", n)
	}
	if counts := engAnchorOK.txCounts(); counts.Commits != 1 {
		t.Fatalf("合法 Anchor 必须提交（commits=%d）", counts.Commits)
	}
}

// ---------------------------------------------------------------------------
// 4) 真实库集成：删资产后零残留 + 任务仍可删除
// ---------------------------------------------------------------------------

// TestDeleteByIDsLeavesNoDanglingReferences 是本次缺陷的端到端回归：
//
// 一张资产同时挂在两个任务上（assets.task_ids + task_asset_links 双写），并被一个
// 探索节点锚定。DeleteByIDs 之后：
//   - assets / task_asset_links / exploration_anchors / finding_assets 里该资产的行
//     必须**全部归零**（旧实现会留下指向已删资产的 link 与 anchor）；
//   - 两个受影响任务随后都必须能被 DeleteTask 正常删除 —— 悬空引用正是让 DeleteTask
//     的成员漂移复核永远判定 drift、任务永久删不掉的原因。
func TestDeleteByIDsLeavesNoDanglingReferences(t *testing.T) {
	d, as, _ := testSetup(t)
	tag := fmt.Sprintf("w5ref%d", time.Now().UnixNano()%1_000_000_000)
	domain := tag + ".test"

	// ---- fixture：两个存活任务（刻意**不**预清 link，正是要验证任务自己能删掉）----
	taskA := mustAssetRefTask(t, d, "W5 assetref A "+tag)
	taskB := mustAssetRefTask(t, d, "W5 assetref B "+tag)

	// 同一张资产先挂 A 再挂 B：assets.task_ids 与 task_asset_links 必须同步双写。
	assetID, err := as.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: taskA.ID})
	if err != nil {
		t.Fatalf("UpsertRootDomain(A): %v", err)
	}
	if _, err := as.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: taskB.ID}); err != nil {
		t.Fatalf("UpsertRootDomain(B): %v", err)
	}

	// 探索锚点：A 的 exploration 里一个节点锚定这张资产。
	nodeID, err := d.Exploration(taskA.ExpID).AddNode(
		"fact", map[string]any{"summary": "assetref " + tag}, 0, "confirmed", "worker", []int64{assetID})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// ---- 前置断言：引用确实建立了（否则后面的"归零"毫无意义）----
	if got := countAssetRef(t, d, `SELECT COUNT(*) FROM task_asset_links WHERE asset_id = ?`, assetID); got < 2 {
		t.Fatalf("前置条件失败：task_asset_links 应有 A/B 两条 link，实得 %d", got)
	}
	if got := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_anchors WHERE asset_id = ?`, assetID); got < 1 {
		t.Fatalf("前置条件失败：exploration_anchors 应至少 1 条，实得 %d", got)
	}

	// ---- 被测动作 ----
	deleted, err := as.DeleteByIDs([]int64{assetID})
	if err != nil {
		t.Fatalf("DeleteByIDs: %v", err)
	}
	if deleted != 1 {
		t.Errorf("DeleteByIDs 删除数=%d, want 1", deleted)
	}

	// ---- 终态断言：零残留 ----
	for _, probe := range []struct {
		label string
		query string
	}{
		{"assets", `SELECT COUNT(*) FROM assets WHERE id = ?`},
		{"task_asset_links", `SELECT COUNT(*) FROM task_asset_links WHERE asset_id = ?`},
		{"exploration_anchors", `SELECT COUNT(*) FROM exploration_anchors WHERE asset_id = ?`},
		{"finding_assets", `SELECT COUNT(*) FROM finding_assets WHERE asset_id = ?`},
	} {
		if got := countAssetRef(t, d, probe.query, assetID); got != 0 {
			t.Errorf("删除资产后 %s 仍残留 %d 行（悬空引用会让对应任务的 DeleteTask 永久漂移）", probe.label, got)
		}
	}
	// 锚点行本身也应随资产清理消失（节点保留，只是断开 asset 引用）。
	if got := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_anchors WHERE node_id = ?`, nodeID); got != 0 {
		t.Errorf("节点 %d 的锚点未清理，实得 %d 行", nodeID, got)
	}
	// 幂等：再删一次必须是 0 且不报错。
	if again, err := as.DeleteByIDs([]int64{assetID}); err != nil || again != 0 {
		t.Errorf("重复删除应幂等返回 0，实得 (%d, %v)", again, err)
	}

	// ---- 关键回归：两个任务都必须删得掉（悬空 link/anchor 会让这里 drift 到重试耗尽）----
	for _, task := range []assetRefTask{taskA, taskB} {
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("删除任务 %d 失败（资产引用未清理干净会导致成员漂移）: %v", task.ID, err)
		}
		*task.deleted = true
		if left := countAssetRef(t, d, `SELECT COUNT(*) FROM tasks WHERE id = ?`, task.ID); left != 0 {
			t.Errorf("任务 %d 删除后仍残留 %d 行", task.ID, left)
		}
	}
}

// assetRefTask 是一条 fixture 任务及其自清理所需的状态。
type assetRefTask struct {
	ID    int64
	ExpID int64
	// deleted 由用例在产品删除成功后置 true；t.Cleanup 见到 true 就不再兜底。
	deleted *bool
}

// mustAssetRefTask 造一个真实存活的父任务，并登记**产品级**清理（DeleteTask）。
//
// 与 assets_test.go 的 assetTestTaskSet 有意不同：这里绝不在清理时预先 DELETE
// task_asset_links / assets —— 那样等于把被测缺陷手工抹掉再断言。兜底清理只在
// 产品删除没跑成（用例提前判红）时补刀，保证共享库零残留。
func mustAssetRefTask(t *testing.T, d *DB, name string) assetRefTask {
	t.Helper()
	task, err := d.CreateTask(name, "assetref fixture", nil, 0, 0)
	if err != nil {
		t.Fatalf("创建任务 %q 失败: %v", name, err)
	}
	deleted := new(bool)
	t.Cleanup(func() {
		if *deleted {
			return
		}
		// 兜底逆拓扑：锚点 → activity → link → 资产 → 产品级级联删除。
		if _, err := d.Exec(`DELETE anchor FROM exploration_anchors anchor
 JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id = ?`, task.ExplorationID); err != nil {
			t.Errorf("兜底清理任务 %d 的锚点失败: %v", task.ID, err)
		}
		if _, err := d.Exec(`DELETE FROM activity WHERE exploration_id = ?`, task.ExplorationID); err != nil {
			t.Errorf("兜底清理任务 %d 的 activity 失败: %v", task.ID, err)
		}
		if _, err := d.Exec(`DELETE FROM task_asset_links WHERE task_id = ?`, task.ID); err != nil {
			t.Errorf("兜底清理任务 %d 的 link 失败: %v", task.ID, err)
		}
		// 资产本体的从属引用必须先断开（TiDB 无外键），否则这条兜底反而制造悬空引用。
		deleteAssetsByWhere(t, d, `WHERE JSON_CONTAINS(COALESCE(task_ids,'[]'), CAST(? AS JSON))`, task.ID)
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("兜底删除任务 %d 失败: %v", task.ID, err)
		}
		var left int
		if err := d.QueryRow(`SELECT COUNT(*) FROM tasks WHERE id = ?`, task.ID).Scan(&left); err != nil {
			t.Errorf("回读任务 %d 失败: %v", task.ID, err)
		} else if left != 0 {
			t.Errorf("清理后任务 %d 仍残留 %d 行", task.ID, left)
		}
	})
	return assetRefTask{ID: task.ID, ExpID: task.ExplorationID, deleted: deleted}
}

// countAssetRef 执行一条 COUNT 探针；探针失败本身就是判红（不许静默当成 0）。
func countAssetRef(t *testing.T, d *DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := d.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("探针 %q args=%v 失败: %v", query, args, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// 5) 真实库集成：AddNode / Anchor 拒绝幻觉 asset id
// ---------------------------------------------------------------------------

// TestExplorationAnchorValidationOnRealTiDB 在真库上确认幻觉 asset id 写不进
// exploration_anchors，并且失败不会留下节点行。
func TestExplorationAnchorValidationOnRealTiDB(t *testing.T) {
	d, _, _ := testSetup(t)
	tag := fmt.Sprintf("w5refv%d", time.Now().UnixNano()%1_000_000_000)
	fixture := mustAssetRefTask(t, d, "W5 assetref V "+tag)
	expID := fixture.ExpID

	nodesBefore := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id = ?`, expID)
	anchorsBefore := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_anchors anchor
 JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id = ?`, expID)

	// 一个不存在的 asset id（TiDB 自增 PK 远小于此，且不可能被本用例创建）。
	ghost := int64(9_200_000_000 + time.Now().UnixNano()%1000)
	if _, err := d.Exploration(expID).AddNode("fact", map[string]any{"summary": tag}, 0, "confirmed", "worker", []int64{ghost}); !errors.Is(err, ErrForeignKeyViolation) {
		t.Errorf("AddNode 幻觉资产应返回 ErrForeignKeyViolation，实得 %v", err)
	}
	if err := d.Exploration(expID).Anchor(1, ghost); !errors.Is(err, ErrForeignKeyViolation) {
		t.Errorf("Anchor 幻觉资产应返回 ErrForeignKeyViolation，实得 %v", err)
	}

	if nodesAfter := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id = ?`, expID); nodesAfter != nodesBefore {
		t.Errorf("校验失败不得写节点行：before=%d after=%d", nodesBefore, nodesAfter)
	}
	if anchorsAfter := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_anchors anchor
 JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id = ?`, expID); anchorsAfter != anchorsBefore {
		t.Errorf("校验失败不得写锚点行：before=%d after=%d", anchorsBefore, anchorsAfter)
	}
	if got := countAssetRef(t, d, `SELECT COUNT(*) FROM exploration_anchors WHERE asset_id = ?`, ghost); got != 0 {
		t.Errorf("幻觉 asset id 仍可写入 exploration_anchors（%d 行）", got)
	}
}
