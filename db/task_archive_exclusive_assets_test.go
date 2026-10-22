package db

// =====================================================================
// 归档压缩侧「独占资产删除」的语句形状契约（deleteExclusiveArchiveAssets）
//
// 这里守的是与 AssetStore.DeleteByIDs 同型的那类缺陷：TiDB 基线不建外键，
// 冷包压缩时对独占资产的删除如果只发一条裸 `DELETE FROM assets`，就会把
// task_asset_links / exploration_anchors / finding_assets 里指向这些资产的行
// 原样留在热库成为悬空引用；task_closure 又把这些引用指向的资产算进任务删除
// 闭包、cascade_executor 按物理 PK FOR UPDATE 去锁 —— 悬空 id 锁不到行 ⇒
// 成员漂移 ⇒ 对应任务永久删不掉。
//
// 三条硬约束各挡一类事故：
//   - **先复核再删**：独占判据（company_id IS NULL + 无其他未删除任务认领 +
//     无其他未删除任务的活跃锚点）必须在删除事务里用当前读重算，复核不通过的
//     id 一个都不许删（删除范围不许扩大）；
//   - **同事务清引用**：复用 deleteAssetsByIDsWithTx，锁/删顺序与 lockorder
//     升序一致（assets → exploration_anchors → finding_assets → task_asset_links），
//     从属行按物理主键删，且与本体删除共享同一次提交；
//   - **中途失败不得先动本体**：引用清理报错时必须还没发出 `DELETE FROM assets`。
//
// 全部离线（expMock，不连库）：真实库上的归档压缩用例由 task_archives 侧覆盖。
// =====================================================================

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
)

// TestArchiveExclusiveAssetDeleteReusesReferencePurge 证明压缩侧删独占资产走的是
// 产品级「清引用 + 删本体」同事务路径，并且独占复核是一条当前读（FOR UPDATE）。
func TestArchiveExclusiveAssetDeleteReusesReferencePurge(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.forceAffectedRows(1)
	// 独占复核的可辨识特征只有 company_id IS NULL；故意乱序返回，
	// 下游必须规整成「去重升序正数」再锁 / 删。
	eng.on("company_id IS NULL", []string{"id"}, []driver.Value{int64(22)}, []driver.Value{int64(11)})
	// deleteAssetsByIDsWithTx 自己的资产行锁定读。
	eng.on("SELECT id FROM assets WHERE id IN", []string{"id"}, []driver.Value{int64(11)}, []driver.Value{int64(22)})
	// 三张从属表：anchors / finding_assets / links（finding_assets 返回空集）。
	eng.on("SELECT row_id FROM exploration_anchors", []string{"row_id"}, []driver.Value{uint64(505)})
	eng.on("SELECT id FROM finding_assets", []string{"id"})
	eng.on("SELECT row_id FROM task_asset_links", []string{"row_id"}, []driver.Value{uint64(707)})

	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := deleteExclusiveArchiveAssets(tx, 7, []int64{11, 22}); err != nil {
		t.Fatalf("deleteExclusiveArchiveAssets: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	calls := eng.snapshotCalls()
	if len(calls) == 0 {
		t.Fatal("没有发出任何语句")
	}
	for _, c := range calls {
		expAssertOrdinal(t, c)
		upper := strings.ToUpper(c.Query)
		if strings.HasPrefix(upper, "DELETE") && strings.Contains(c.Query, "asset_id") {
			t.Errorf("引用清理必须按物理主键删，不能发按 asset_id 的裸范围删除: %s", c.Query)
		}
	}

	// firstMatch 按子串定位语句；exclude 用来把"同一段前缀但语义不同"的语句排掉
	// （独占复核与 deleteAssetsByIDsWithTx 的资产锁定读都以 `SELECT id FROM assets
	//  WHERE id IN` 开头）。
	firstMatch := func(substr string, exclude ...string) int {
		for i, c := range calls {
			if !strings.Contains(c.Query, substr) {
				continue
			}
			skip := false
			for _, ex := range exclude {
				if ex != "" && strings.Contains(c.Query, ex) {
					skip = true
					break
				}
			}
			if !skip {
				return i
			}
		}
		return -1
	}
	// 第 0 条必须是独占复核，且是**当前读**（FOR UPDATE）：普通 SELECT 在事务里走快照读，
	// 与原「DELETE … WHERE NOT EXISTS」的当前读判据不等价。
	recheck := calls[0].Query
	if !strings.Contains(recheck, "company_id IS NULL") || !strings.Contains(strings.ToUpper(recheck), "FOR UPDATE") {
		t.Fatalf("首条语句必须是带独占判据的锁定读，实得: %s", recheck)
	}
	if got := fmt.Sprint(calls[0].Args); got != "[11 22 7 7]" {
		t.Fatalf("独占复核参数 = %s, want [11 22 7 7]（先 IN 列表，再两个 task.id<>?）", got)
	}
	// 顺序表：复核 → 锁资产 → 清 anchors → 清 finding_assets → 清 links → 删本体。
	type step struct {
		substr  string
		exclude string
	}
	steps := []step{
		{"company_id IS NULL", ""},
		{"SELECT id FROM assets WHERE id IN", "company_id IS NULL"},
		{"SELECT row_id FROM exploration_anchors", ""},
		{"DELETE FROM exploration_anchors", ""},
		{"SELECT id FROM finding_assets", ""},
		{"SELECT row_id FROM task_asset_links", ""},
		{"DELETE FROM task_asset_links", ""},
		{"DELETE FROM assets", ""},
	}
	prev := -1
	for _, st := range steps {
		at := firstMatch(st.substr, st.exclude)
		step := st.substr
		switch {
		case at < 0:
			t.Fatalf("缺少语句 %q；实际序列: %v", step, queriesOf(calls))
		case at <= prev:
			t.Fatalf("语句 %q 出现在位置 %d，必须晚于前一步 %d；实际序列: %v", step, at, prev, queriesOf(calls))
		}
		prev = at
	}
	expectPKDelete(t, calls, "DELETE FROM exploration_anchors", uint64(505))
	expectPKDelete(t, calls, "DELETE FROM task_asset_links", uint64(707))
	expectPKDelete(t, calls, "DELETE FROM assets", int64(11), int64(22))

	if counts := eng.txCounts(); counts.Commits != 1 || counts.Rollbacks != 0 {
		t.Fatalf("引用清理必须与本体删除共享同一次提交: commits=%d rollbacks=%d", counts.Commits, counts.Rollbacks)
	}
}

// TestArchiveExclusiveAssetDeleteSkipsNonExclusiveAssets 证明删除范围没有被扩大：
// 独占复核一条都不通过时，除了那条复核 SELECT 之外**不得再发任何语句**，
// 尤其不得出现 DELETE。
func TestArchiveExclusiveAssetDeleteSkipsNonExclusiveAssets(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("company_id IS NULL", []string{"id"}) // 全部资产已被别的未删除任务认领

	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := deleteExclusiveArchiveAssets(tx, 7, []int64{11, 22}); err != nil {
		t.Fatalf("deleteExclusiveArchiveAssets: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	calls := eng.snapshotCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].Query, "company_id IS NULL") {
		t.Fatalf("复核不通过时只允许一条独占复核 SELECT，实得: %v", queriesOf(calls))
	}
	for _, c := range calls {
		if strings.HasPrefix(strings.ToUpper(c.Query), "DELETE") {
			t.Fatalf("复核不通过仍发出删除: %s", c.Query)
		}
	}
}

// TestArchiveExclusiveAssetDeleteNoIDsNoSQL 证明空输入零 SQL：不能拼出 `IN ()`。
func TestArchiveExclusiveAssetDeleteNoIDsNoSQL(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := deleteExclusiveArchiveAssets(tx, 7, nil); err != nil {
		t.Fatalf("deleteExclusiveArchiveAssets(nil): %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if calls := eng.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("空 id 列表必须一条 SQL 都不发，实发 %v", queriesOf(calls))
	}
}

// TestArchiveExclusiveAssetDeleteAbortsBeforeBodyDelete 证明引用清理中途失败时
// 资产本体一行都没删（同事务 + 失败整体回滚的前提是顺序上先清完引用）。
func TestArchiveExclusiveAssetDeleteAbortsBeforeBodyDelete(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.forceAffectedRows(1)
	eng.on("company_id IS NULL", []string{"id"}, []driver.Value{int64(11)})
	eng.on("SELECT id FROM assets WHERE id IN", []string{"id"}, []driver.Value{int64(11)})
	eng.on("SELECT row_id FROM exploration_anchors", []string{"row_id"}, []driver.Value{uint64(505)})
	eng.failOnExec("DELETE FROM exploration_anchors")

	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := deleteExclusiveArchiveAssets(tx, 7, []int64{11}); err == nil {
		t.Fatal("引用清理失败必须向上抛错，让外层整笔回滚")
	}
	for _, c := range eng.statements(func(x expMockCall) bool {
		return strings.HasPrefix(strings.ToUpper(x.Query), "DELETE")
	}) {
		if strings.Contains(c.Query, "DELETE FROM assets") {
			t.Fatalf("引用清理失败后仍删除了资产本体: %s", c.Query)
		}
	}
}
