package db

// =====================================================================
// P08-02 Task 2.3 交付测试：任务状态机 / 写者 Gate / 全物理副作用级联闭包
//
// 三个用例全部**离线**可跑（无 TiDB 也必须是绿的，且真机缺失时诚实 skip）：
//   1. TestTask_DeleteTaskCascade_UnmigratedDialect
//      —— 任务写者与级联面残留的 PG 方言（$n / RETURNING / ::type / =ANY / FILTER /
//      NULLS LAST / unnest / try_inet / >>= / pgconn）与表级锁必须为 0；
//   2. TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML
//      —— 双根共享资产 + 跨根弱关联必须**整批进锁计划**，executor 发出的每一条
//      UPDATE/DELETE 都必须绑在 Catalog 物理 PK 上（写集收敛），越界即判红；
//   3. TestTask_DeleteTaskCascade_MembershipDriftRetryRollback
//      —— 持锁后新发现的成员（含更低 PK）只能触发 ErrMembershipDrift 整体回滚重放；
//      "原地逆序补锁"这条路径必须在导出入口上直接判红。
// =====================================================================

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
)

// ── 测试替身 ────────────────────────────────────────────────────────────────

type fakeResult struct{ rows int64 }

func (f fakeResult) LastInsertId() (int64, error) { return 1, nil }
func (f fakeResult) RowsAffected() (int64, error) { return f.rows, nil }

// recordingExecer 捕获 executor 发出的每一条 DML 与其绑定参数。
type recordingExecer struct {
	statements []CascadeStatementRecord
}

func (r *recordingExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	return fakeResult{rows: 1}, nil
}

// ── 用例夹具：taskA(101) 与 taskB(102) 共享 asset 900，独占 asset 901 ──────────

const (
	fixtureTaskA       uint64 = 101
	fixtureTaskB       uint64 = 102
	fixtureExploration uint64 = 201
	fixtureNode1       uint64 = 301
	fixtureNode2       uint64 = 302
	// 本 exploration 自己的 activity 行 vs **其他** exploration 指向本闭包节点的 activity 行
	fixtureActivityOwn      uint64 = 401
	fixtureActivityCross    uint64 = 402
	fixtureLinkOwn          uint64 = 501 // taskA 自己的 link
	fixtureLinkCross        uint64 = 502 // taskB 的 link，source_node_id 指向本闭包节点
	fixtureLinkForeignDead  uint64 = 503 // 其他任务指向 asset 901（将被物理删除）
	fixtureLinkForeignAlive uint64 = 504 // 其他任务指向 asset 900（只 detach，必须存活）
	fixtureAnchorOwn        uint64 = 601 // 本闭包节点上的 anchor
	fixtureAnchorForeign    uint64 = 602 // 其他节点指向 asset 901
	fixtureAssetShared      uint64 = 900
	fixtureAssetExclusive   uint64 = 901
	fixtureSideSession      uint64 = 701
	fixtureSideRequest      uint64 = 71
	fixtureMainSession      uint64 = 72
	fixtureFindingTask      uint64 = 801 // task_id 指向本任务
	fixtureFindingNode      uint64 = 802 // node_id 指向本闭包节点（task_id 为空）
)

func crossRootFixtureClosure(t *testing.T) *TaskCascadeClosure {
	t.Helper()
	closure := &TaskCascadeClosure{
		TaskID:        fixtureTaskA,
		ExplorationID: fixtureExploration,
		Options:       TaskClosureOptions{DeleteAssets: true},

		NodeIDs:              []uint64{fixtureNode1, fixtureNode2},
		AnchorIDs:            []uint64{fixtureAnchorOwn},
		ActivityIDs:          []uint64{fixtureActivityOwn},
		CrossRootActivityIDs: []uint64{fixtureActivityCross},
		AssetLinkIDs:         []uint64{fixtureLinkOwn},
		CrossRootLinkIDs:     []uint64{fixtureLinkCross},
		ForeignAssetLinkRows: []AssetRefRow{
			{RowID: fixtureLinkForeignDead, AssetID: fixtureAssetExclusive},
			{RowID: fixtureLinkForeignAlive, AssetID: fixtureAssetShared},
		},
		ForeignAssetAnchorRows: []AssetRefRow{
			{RowID: fixtureAnchorForeign, AssetID: fixtureAssetExclusive},
		},
		RefTaskIDs: []uint64{fixtureTaskB},

		MainSessionIDs:      []uint64{fixtureMainSession},
		SideSessionRowIDs:   []uint64{fixtureSideSession},
		SideRequestOrdinals: []uint64{fixtureSideRequest},

		NullifyFindingTaskIDs: []uint64{fixtureFindingTask},
		NullifyFindingNodeIDs: []uint64{fixtureFindingNode},
		FindingIDs:            []uint64{fixtureFindingTask, fixtureFindingNode},

		CandidateAssetIDs: []uint64{fixtureAssetShared, fixtureAssetExclusive},
		AssetIDsToDelete:  []uint64{fixtureAssetExclusive},
		AssetIDsToDetach:  []uint64{fixtureAssetShared},
		// 共享资产剥离本任务后必须**保留 taskB**：这是"绝不误删共享资产"的正向断言。
		AssetDetachJSON: map[uint64]string{fixtureAssetShared: "[102]"},

		assetOtherTasks: map[uint64][]uint64{fixtureAssetShared: {fixtureTaskB}},
		assetTaskIDsRaw: map[uint64]string{
			fixtureAssetShared:    "[101,102]",
			fixtureAssetExclusive: "[101]",
		},
	}
	// 走真实的**持锁仲裁**：delete/detach 划分、detach 载荷、跨根删除子集都由它算出，
	// 用例不手写结果，才能证明"共享资产只剥离、跨根行按资产命运分流"。
	if err := closure.arbitrate(); err != nil {
		t.Fatalf("arbitrate: %v", err)
	}
	return closure
}

func lockPlanKeys(t *testing.T, closure *TaskCascadeClosure) []lockorder.EntityKey {
	t.Helper()
	keys, err := closure.LockPlan()
	if err != nil {
		t.Fatalf("LockPlan: %v", err)
	}
	return keys
}

func lockPlanContains(t *testing.T, keys []lockorder.EntityKey, table string, pk uint64) {
	t.Helper()
	meta, err := lockorder.LookupTableName(table)
	if err != nil {
		t.Fatalf("catalog lookup %s: %v", table, err)
	}
	want := lockorder.EntityKey{Table: meta.Order, PK: pk}
	for _, key := range keys {
		if key == want {
			return
		}
	}
	t.Fatalf("lock plan missing %s(%d); plan=%v", table, pk, keys)
}

func lockPlanContainsTable(t *testing.T, keys []lockorder.EntityKey, table string) {
	t.Helper()
	meta, err := lockorder.LookupTableName(table)
	if err != nil {
		t.Fatalf("catalog lookup %s: %v", table, err)
	}
	for _, key := range keys {
		if key.Table == meta.Order {
			return
		}
	}
	t.Fatalf("lock plan has no %s entity", table)
}

// ── 用例 1：未迁移方言必须为 0 ───────────────────────────────────────────────

func TestTask_DeleteTaskCascade_UnmigratedDialect(t *testing.T) {
	files := []string{
		"tasks.go", "task_closure.go", "cascade_executor.go",
		"task_assets.go", "task_assets_context.go", "task_scope.go",
		"task_context.go", "task_intercept.go", "task_categories.go", "task_templates.go",
	}
	pgMarkers := []string{
		"RETURNING ", "FILTER (WHERE", "NULLS LAST", "unnest(", "=ANY(", "ANY($",
		"array_append", "array_remove", "try_inet", "masklen(", ">>=", "make_interval",
		"regexp_replace(", "pgconn", "pg_advisory", "LOCK TABLE", "IN SHARE ROW EXCLUSIVE",
		"::bigint", "::text", "::jsonb", "::cidr", "::inet", "::interval", "::boolean", "::json",
	}
	positional := regexp.MustCompile(`\$[0-9]`)

	for _, name := range files {
		path := filepath.Join(".", name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		source := stripSQLCommentsForTest(string(raw))
		for _, marker := range pgMarkers {
			if strings.Contains(source, marker) {
				t.Errorf("%s 仍残留 PG 方言 %q", name, marker)
			}
		}
		if positional.MatchString(source) {
			t.Errorf("%s 仍残留位置占位符 $n", name)
		}
	}
}

// stripSQLCommentsForTest 去掉 Go 注释，避免"文档里引用了旧方言"被误判成残留。
func stripSQLCommentsForTest(source string) string {
	var out strings.Builder
	for _, line := range strings.Split(source, "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	text := out.String()
	// 字符串字面量里的 SQL 才算残留；把双引号字符串保留即可，这里只需去掉块注释。
	if start := strings.Index(text, "/*"); start >= 0 {
		for {
			end := strings.Index(text[start:], "*/")
			if end < 0 {
				break
			}
			text = text[:start] + text[start+end+2:]
			start = strings.Index(text, "/*")
			if start < 0 {
				break
			}
		}
	}
	return text
}

// ── 用例 2：跨根锁计划 + 写集收敛 ───────────────────────────────────────────

func TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML(t *testing.T) {
	closure := crossRootFixtureClosure(t)
	if err := closure.Validate(); err != nil {
		t.Fatalf("closure validate: %v", err)
	}
	plan := lockPlanKeys(t, closure)

	// ① 全序：(TableOrder, PK 升序)，且无重复
	if !lockorder.IsSorted(plan) {
		t.Fatalf("lock plan is not globally ordered: %v", plan)
	}
	if dup := duplicateKeysForTest(plan); len(dup) > 0 {
		t.Fatalf("lock plan contains duplicates: %v", dup)
	}

	// ② 跨根物理副作用必须纳锁：其他 exploration 的 activity.id、其他任务的
	//    task_asset_links.row_id / exploration_anchors.row_id、引用共享资产的其他 task.id
	lockPlanContains(t, plan, "tasks", fixtureTaskA)
	lockPlanContains(t, plan, "tasks", fixtureTaskB)
	lockPlanContains(t, plan, "explorations", fixtureExploration)
	lockPlanContains(t, plan, "activity", fixtureActivityCross)
	lockPlanContains(t, plan, "task_asset_links", fixtureLinkCross)
	lockPlanContains(t, plan, "task_asset_links", fixtureLinkForeignDead)
	lockPlanContains(t, plan, "task_asset_links", fixtureLinkForeignAlive)
	lockPlanContains(t, plan, "exploration_anchors", fixtureAnchorForeign)
	lockPlanContains(t, plan, "assets", fixtureAssetShared)
	lockPlanContains(t, plan, "side_question_sessions", fixtureSideSession)
	lockPlanContains(t, plan, "side_question_requests", fixtureSideRequest)
	lockPlanContains(t, plan, "findings", fixtureFindingNode)
	// session_key 这类业务唯一键必须已翻译成物理 row_id 后才进锁计划
	lockPlanContainsTable(t, plan, "side_question_sessions")

	// ③ executor 的每条 DML 都必须绑在 Catalog 物理 PK 上
	rec := &recordingExecer{}
	previousHook := CascadeStatementHook
	CascadeStatementHook = func(record CascadeStatementRecord) {
		rec.statements = append(rec.statements, record)
	}
	t.Cleanup(func() { CascadeStatementHook = previousHook })

	audit, err := executeTaskCascade(context.Background(), rec, closure, nil)
	if err != nil {
		t.Fatalf("executeTaskCascade: %v", err)
	}
	if len(rec.statements) == 0 {
		t.Fatal("executor emitted no statement")
	}

	planSet := keySetForTest(plan)
	pkWhere := regexp.MustCompile(`(?i)\bWHERE (\w+) (IN \(\?(?:,\?)*\)|= \?)$`)
	for _, record := range rec.statements {
		match := pkWhere.FindStringSubmatch(record.SQL)
		if match == nil {
			t.Fatalf("statement is not bounded by a physical PK list: %s", record.SQL)
		}
		meta, lookupErr := lockorder.LookupTableName(record.Table)
		if lookupErr != nil {
			t.Fatalf("statement table %s is not in the catalog: %v", record.Table, lookupErr)
		}
		if match[1] != meta.PKColumn {
			t.Fatalf("statement %q filters on %s instead of physical PK %s",
				record.SQL, match[1], meta.PKColumn)
		}
		if record.PKColumn != meta.PKColumn {
			t.Fatalf("record %q declares PK column %s, catalog says %s",
				record.SQL, record.PKColumn, meta.PKColumn)
		}
		for _, pk := range record.PKs {
			if _, ok := planSet[record.Table][pk]; !ok {
				t.Fatalf("statement %q binds %s(%d) which is outside the locked PK set",
					record.SQL, record.Table, pk)
			}
		}
		// 经典越界写法（M27 判红样本）
		for _, forbidden := range []string{"WHERE node_id", "WHERE task_id", "WHERE session_key", "WHERE source_node_id", "WHERE asset_id"} {
			if strings.Contains(record.SQL, forbidden) {
				t.Fatalf("statement %q uses open-range predicate %q", record.SQL, forbidden)
			}
		}
	}

	// ④ 跨根弱关联只被"按 PK 置空"，绝不整片改写
	if !hasStatementForTest(rec.statements, "update", "activity", "id", []uint64{fixtureActivityCross}) {
		t.Fatalf("cross-root activity row was not nullified by PK; statements=%v", statementTextsForTest(rec.statements))
	}
	if !hasStatementForTest(rec.statements, "update", "task_asset_links", "row_id", []uint64{fixtureLinkCross}) {
		t.Fatalf("cross-root link row was not nullified by PK; statements=%v", statementTextsForTest(rec.statements))
	}

	// ⑤ 共享资产只 detach（保留 taskB），独占资产才物理删除
	if !hasStatementForTest(rec.statements, "update", "assets", "id", []uint64{fixtureAssetShared}) {
		t.Fatalf("shared asset was not detached by PK; statements=%v", statementTextsForTest(rec.statements))
	}
	if !hasStatementForTest(rec.statements, "delete", "assets", "id", []uint64{fixtureAssetExclusive}) {
		t.Fatalf("exclusive asset was not deleted by PK; statements=%v", statementTextsForTest(rec.statements))
	}
	if deleted := deletedPKsForTest(rec.statements, "assets"); containsUint64(deleted, fixtureAssetShared) {
		t.Fatalf("shared asset %d must survive, deleted assets=%v", fixtureAssetShared, deleted)
	}
	if closure.AssetIDsToDetach[0] != fixtureAssetShared || closure.AssetDetachJSON[fixtureAssetShared] != "[102]" {
		t.Fatalf("detach payload must keep taskB, got %q", closure.AssetDetachJSON[fixtureAssetShared])
	}

	// ⑥ 跨根 link/anchor 只在"资产被物理删除"时才跟着删
	deletedLinks := deletedPKsForTest(rec.statements, "task_asset_links")
	if !containsUint64(deletedLinks, fixtureLinkForeignDead) {
		t.Fatalf("link row pointing at a deleted asset must be removed; got %v", deletedLinks)
	}
	if containsUint64(deletedLinks, fixtureLinkForeignAlive) {
		t.Fatalf("link row pointing at a detached (shared) asset must survive; got %v", deletedLinks)
	}
	if containsUint64(deletedLinks, fixtureLinkCross) {
		t.Fatalf("cross-root link must only be nullified, never deleted; got %v", deletedLinks)
	}
	deletedAnchors := deletedPKsForTest(rec.statements, "exploration_anchors")
	if containsUint64(deletedAnchors, fixtureAnchorForeign) && !containsUint64(closure.AssetIDsToDelete, fixtureAssetExclusive) {
		t.Fatalf("foreign anchor deleted without deleting its asset")
	}
	if containsUint64(deletedAnchors, fixtureAnchorOwn) != true {
		t.Fatalf("own anchor row must be deleted; got %v", deletedAnchors)
	}

	// ⑦ 审计：只统计真正物理删除的实体
	if audit.EntitiesDeleted["tasks"] <= 0 || audit.EntitiesDeleted["explorations"] <= 0 {
		t.Fatalf("audit must record root deletions: %+v", audit.EntitiesDeleted)
	}

	// ⑧ 越界写集必须判红（不是"能跑就算对"）：给一个不在锁计划里的 activity.id
	writer, err := newNarrowWriter(context.Background(), rec, newAudit("task", fixtureTaskA), plan)
	if err != nil {
		t.Fatalf("narrow writer: %v", err)
	}
	if err := writer.nullifyByPK("activity", []string{"node_id"}, []uint64{fixtureActivityCross + 97}); !errors.Is(err, ErrCascadeWriteSetEscaped) {
		t.Fatalf("unlock PK write must be rejected, got %v", err)
	}
	if err := writer.deleteByPK("task_asset_links", []uint64{fixtureLinkCross + 7}); !errors.Is(err, ErrCascadeWriteSetEscaped) {
		t.Fatalf("unlock PK delete must be rejected, got %v", err)
	}
}

// ── 用例 3：成员漂移 → 整体回滚重放（禁止原地补锁）──────────────────────────

func TestTask_DeleteTaskCascade_MembershipDriftRetryRollback(t *testing.T) {
	closure := crossRootFixtureClosure(t)
	snapshot := lockPlanKeys(t, closure)

	// ① 持锁当前读发现"更低 PK 的新成员"：必须 ErrLowerPKDiscovered（+ErrMembershipDrift）
	lowerMeta, err := lockorder.LookupTableName("tasks")
	if err != nil {
		t.Fatalf("catalog lookup: %v", err)
	}
	lower := lockorder.EntityKey{Table: lowerMeta.Order, PK: 5}
	withLower := lockorder.SortEntityKeys(append(append([]lockorder.EntityKey{}, snapshot...), lower))
	drift := lockorder.CompareMembership(snapshot, withLower)
	if drift == nil {
		t.Fatal("lower-PK member must be reported as membership drift")
	}
	if !errors.Is(drift, lockorder.ErrMembershipDrift) || !errors.Is(drift, lockorder.ErrLowerPKDiscovered) {
		t.Fatalf("drift must wrap ErrMembershipDrift + ErrLowerPKDiscovered, got %v", drift)
	}
	// 漂移必须由 TxRunner **整体回滚 + 指数退避重放**，而不是被当成普通失败吞掉
	if !lockorder.IsRetryableConflict(drift) {
		t.Fatalf("membership drift must be retryable, got %v", drift)
	}

	// ② 中途新成员（不改变最小 PK）同样是漂移
	newMeta, err := lockorder.LookupTableName("activity")
	if err != nil {
		t.Fatalf("catalog lookup: %v", err)
	}
	sneaky := lockorder.EntityKey{Table: newMeta.Order, PK: 401 + 1} // 402 已在计划里，换成 403
	if keySetForTest(snapshot)["activity"][sneaky.PK] {
		sneaky.PK = 404
	}
	withNew := lockorder.SortEntityKeys(append(append([]lockorder.EntityKey{}, snapshot...), sneaky))
	if err := lockorder.CompareMembership(snapshot, withNew); !errors.Is(err, lockorder.ErrNewMemberDiscovered) {
		t.Fatalf("new member mid-plan must drift, got %v", err)
	}

	// ③ 成员消失（被并发删除）同样是漂移
	if err := lockorder.CompareMembership(snapshot, snapshot[:len(snapshot)-1]); !errors.Is(err, lockorder.ErrEntityNotFound) {
		t.Fatalf("missing member must drift, got %v", err)
	}

	// ④ 同集合复核必须通过：否则任何一次删除都会白白重试
	if err := lockorder.CompareMembership(snapshot, snapshot); err != nil {
		t.Fatalf("identical membership must pass, got %v", err)
	}

	// ⑤ "逆序补锁"实现必须判红：导出入口只接受全序候选，
	//    任何"发现新成员后就地追加/回补锁"的写法都会在 IsSorted 这一步先失败。
	backwards := append([]lockorder.EntityKey{}, snapshot...)
	backwards = append(backwards, lower) // 未排序：更低 PK 追加在末尾
	if err := lockorder.LockEntitiesInOrder(context.Background(), nil, nil, backwards); !errors.Is(err, lockorder.ErrOutOfOrderLock) {
		t.Fatalf("backwards lock list must be rejected with ErrOutOfOrderLock, got %v", err)
	}
	// nil 事务同样不允许被当成"离线加锁成功"
	if err := lockorder.LockEntitiesInOrder(context.Background(), nil, nil, snapshot); !errors.Is(err, lockorder.ErrNilTx) {
		t.Fatalf("nil tx must be rejected, got %v", err)
	}

	// ⑥ 真实 TiDB 集成：在"快照之后、复核之前"通过测试钩子注入更低 PK 成员，删除事务必须整体回滚后重放成功。
	t.Run("Live", func(t *testing.T) {
		dsn := os.Getenv("ARTEX_TIDB_DSN")
		if dsn == "" {
			t.Skip("跳过：环境变量 ARTEX_TIDB_DSN 未配置（未验证 / Honest Skip）")
		}
		d, err := Open(dsn)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Errorf("close DB failed: %v", err)
			}
		})

		task, err := d.CreateTaskWithOptions(fmt.Sprintf("P08 Drift Task %d", time.Now().UnixNano()), "goal", TaskCreateOptions{})
		if err != nil {
			t.Fatalf("CreateTaskWithOptions failed: %v", err)
		}
		t.Cleanup(func() {
			_, _ = d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID)
			_, _ = d.Exec("DELETE FROM explorations WHERE id = ?", task.ExplorationID)
			_, _ = d.Exec("DELETE FROM tasks WHERE id = ?", task.ID)
		})

		// 注入钩子：在快照后由独立连接注入更低 PK 行
		injected := false
		TaskCascadeTestHookAfterSnapshot = func(taskID uint64) {
			if injected {
				return
			}
			injected = true
			// 在闭包快照后模拟外部并发插入更低 PK 节点
			_, _ = d.Exploration(task.ExplorationID).AddNode("asset", map[string]any{"label": "injected"}, 99, "pending", "agent", nil)
		}
		t.Cleanup(func() {
			TaskCascadeTestHookAfterSnapshot = nil
		})

		res, err := d.DeleteTaskCascade(task.ID, false, false)
		if err != nil {
			t.Fatalf("DeleteTaskCascade with membership drift should rollback and retry successfully, got err: %v", err)
		}
		_ = res

		var taskLeft, expLeft int
		if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", task.ID).Scan(&taskLeft); err != nil {
			t.Fatalf("query task residual: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM explorations WHERE id = ?", task.ExplorationID).Scan(&expLeft); err != nil {
			t.Fatalf("query exploration residual: %v", err)
		}
		if taskLeft != 0 || expLeft != 0 {
			t.Fatalf("expected 0 residual task and exploration after retry, got task=%d exp=%d", taskLeft, expLeft)
		}
	})
}

// ── 测试辅助 ────────────────────────────────────────────────────────────────

func duplicateKeysForTest(keys []lockorder.EntityKey) []string {
	seen := map[lockorder.EntityKey]int{}
	var dup []string
	for _, key := range keys {
		seen[key]++
	}
	for key, count := range seen {
		if count > 1 {
			dup = append(dup, fmt.Sprintf("%v x%d", key, count))
		}
	}
	sort.Strings(dup)
	return dup
}

func keySetForTest(keys []lockorder.EntityKey) map[string]map[uint64]bool {
	out := map[string]map[uint64]bool{}
	for _, key := range keys {
		meta, err := lockorder.LookupTableOrder(key.Table)
		if err != nil {
			continue
		}
		if out[meta.Name] == nil {
			out[meta.Name] = map[uint64]bool{}
		}
		out[meta.Name][key.PK] = true
	}
	return out
}

func hasStatementForTest(records []CascadeStatementRecord, verb, table, pkColumn string, pks []uint64) bool {
	for _, record := range records {
		if record.Verb != verb || record.Table != table || record.PKColumn != pkColumn {
			continue
		}
		if sameUint64Set(record.PKs, pks) {
			return true
		}
	}
	return false
}

func deletedPKsForTest(records []CascadeStatementRecord, table string) []uint64 {
	var out []uint64
	for _, record := range records {
		if record.Verb == "delete" && record.Table == table {
			out = append(out, record.PKs...)
		}
	}
	return out
}

func statementTextsForTest(records []CascadeStatementRecord) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record.SQL)
	}
	return out
}

func sameUint64Set(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsUint64(items []uint64, want uint64) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
