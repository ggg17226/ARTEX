package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDeleteFinding verifies删除漏洞 removes both the findings row and its
// originating exploration node (kind='finding').
func TestDeleteFinding(t *testing.T) {
	d := openTestDB(t)

	tk, err := d.CreateTask("删除漏洞测试", "目标", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(tk.ID)

	// seed a finding node in the task's exploration graph, then a findings row on it
	es := d.Exploration(tk.ExplorationID)
	nodeID, err := es.AddNode(KindFinding, map[string]any{"summary": "x", "severity": "high"}, 5, "confirmed", "worker", nil)
	if err != nil {
		t.Fatal(err)
	}
	fid, err := d.AddFinding(tk.ID, nodeID, "XSS", "反射型 XSS", "high", "summary", "poc", "worker", nil)
	if err != nil {
		t.Fatal(err)
	}

	n, err := d.DeleteFinding(fid)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("DeleteFinding rows: want 1, got %d", n)
	}
	if f, _ := d.GetFinding(fid); f != nil {
		t.Fatalf("finding row should be gone, got %+v", f)
	}
	var cnt int
	d.QueryRow(`SELECT count(*) FROM exploration_nodes WHERE id=?`, nodeID).Scan(&cnt)
	if cnt != 0 {
		t.Fatalf("originating finding node should be deleted, still %d", cnt)
	}

	// deleting a non-existent finding is a no-op (0 rows), not an error
	if n, err := d.DeleteFinding(fid); err != nil || n != 0 {
		t.Fatalf("re-delete: want (0,nil), got (%d,%v)", n, err)
	}
}

// TestFindingsPageAndStats exercises ListFindingsPage (filter/sort/paging) and
// FindingStats against the live dev PG. It tags its rows with a unique vulnclass
// so assertions are isolated from any pre-existing data, and cleans up after.
func TestFindingsPageAndStats(t *testing.T) {
	d := openTestDB(t)

	const vc = "__test_vc_pagination__"
	// clean any leftovers from a prior aborted run, and clean up on exit
	cleanup := func() { _, _ = d.Exec(`DELETE FROM findings WHERE vulnclass=?`, vc) }
	cleanup()
	defer cleanup()

	// Seed 6 findings under the marker vulnclass: 1 critical, 3 high, 2 low; 2 pending.
	// The critical row carries a name to verify round-trip.
	seed := []struct {
		sev, status, name string
	}{
		{"critical", "pending", "严重漏洞标题"},
		{"high", "resolved", ""},
		{"high", "resolved", ""},
		{"high", "pending", ""},
		{"low", "resolved", ""},
		{"low", "resolved", ""},
	}
	var ids []int64
	for i, s := range seed {
		id, err := d.AddFinding(0, 0, vc, s.name, s.sev, "summary", "poc", "tester", nil)
		if err != nil {
			t.Fatalf("AddFinding[%d]: %v", i, err)
		}
		if _, err := d.SetFindingStatus(id, s.status); err != nil {
			t.Fatalf("SetFindingStatus[%d]: %v", i, err)
		}
		ids = append(ids, id)
	}

	// Filter by our vulnclass → exactly the 6 seeded rows, paged 2 per page.
	p1, total, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Sort: "severity"}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 6 {
		t.Fatalf("total: want 6, got %d", total)
	}
	if len(p1) != 2 {
		t.Fatalf("page1 size: want 2, got %d", len(p1))
	}
	// severity sort → critical first (with its name round-tripped), then high.
	if p1[0].Severity != "critical" {
		t.Fatalf("severity sort: want critical first, got %q", p1[0].Severity)
	}
	if p1[0].Name != "严重漏洞标题" {
		t.Fatalf("name round-trip: want 严重漏洞标题, got %q", p1[0].Name)
	}
	if p1[1].Severity != "high" {
		t.Fatalf("severity sort: want high second, got %q", p1[1].Severity)
	}

	// Combined filter: vulnclass + status=pending → 2 rows.
	pend, total, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Status: FindingPending}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(pend) != 2 {
		t.Fatalf("pending filter: want 2/2, got %d/%d", total, len(pend))
	}

	// Combined filter: vulnclass + severity=high → 3 rows.
	_, total, err = d.ListFindingsPage(FindingFilter{VulnClass: vc, Severity: "high"}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("high filter: want 3, got %d", total)
	}

	// Stats: whole-table, so assert our contribution is reflected (>=) and the
	// marker vulnclass is present.
	st, err := d.FindingStats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Critical < 1 {
		t.Fatalf("stats critical undercount: %+v", st)
	}
	if st.Total < 6 || st.High < 3 || st.Low < 2 || st.Pending < 2 {
		t.Fatalf("stats undercount: %+v", st)
	}
	if !slices.Contains(st.VulnClasses, vc) {
		t.Fatalf("stats vulnclasses missing %q", vc)
	}

	// GetFinding: single-row fetch round-trips id/name/severity.
	one, err := d.GetFinding(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if one == nil || one.ID != ids[0] || one.Severity != "critical" || one.Name != "严重漏洞标题" {
		t.Fatalf("GetFinding mismatch: %+v", one)
	}
	if one.Report != "" {
		t.Fatalf("new finding report should be empty, got %q", one.Report)
	}
	// report column round-trips through GetFinding.
	if _, err := d.Exec(`UPDATE findings SET report=? WHERE id=?`, "# 报告\n正文", ids[0]); err != nil {
		t.Fatal(err)
	}
	if one, _ = d.GetFinding(ids[0]); one.Report != "# 报告\n正文" {
		t.Fatalf("report not read back: %q", one.Report)
	}
	for _, test := range []struct {
		name  string
		query string
		want  int
	}{
		{name: "name", query: "严重漏洞标题", want: 1},
		{name: "summary", query: "summary", want: 6},
		{name: "evidence", query: "poc", want: 6},
		{name: "report", query: "正文", want: 1},
		{name: "case insensitive vulnclass", query: strings.ToUpper(vc), want: 6},
	} {
		t.Run("query_"+test.name, func(t *testing.T) {
			matches, searchTotal, searchErr := d.ListFindingsPage(
				FindingFilter{VulnClass: vc, Query: test.query}, 1, 20,
			)
			if searchErr != nil || searchTotal != test.want || len(matches) != test.want {
				t.Fatalf("query %q: len=%d total=%d err=%v, want %d", test.query, len(matches), searchTotal, searchErr, test.want)
			}
		})
	}
	if _, err := d.Exec(`UPDATE findings SET name=? WHERE id=?`, "literal %_ marker", ids[1]); err != nil {
		t.Fatal(err)
	}
	matches, searchTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Query: "%_"}, 1, 20)
	if err != nil || searchTotal != 1 || len(matches) != 1 || matches[0].ID != ids[1] {
		t.Fatalf("query wildcards must be literal: matches=%+v total=%d err=%v", matches, searchTotal, err)
	}
	if miss, err := d.GetFinding(-1); err != nil || miss != nil {
		t.Fatalf("GetFinding(-1): want nil,nil got %+v,%v", miss, err)
	}

	// SetFindingSeverity: standalone row updates; 0 rows for unknown id.
	if n, err := d.SetFindingSeverity(ids[0], "high"); err != nil || n != 1 {
		t.Fatalf("SetFindingSeverity: want 1,nil got %d,%v", n, err)
	}
	one, _ = d.GetFinding(ids[0])
	if one.Severity != "high" {
		t.Fatalf("severity not updated: %q", one.Severity)
	}
	if n, err := d.SetFindingSeverity(-1, "low"); err != nil || n != 0 {
		t.Fatalf("SetFindingSeverity(-1): want 0,nil got %d,%v", n, err)
	}
}

func TestListFindingsPageUsesStableIDTieBreaker(t *testing.T) {
	d := openTestDB(t)

	vc := fmt.Sprintf("__test_finding_stable_page_%d__", time.Now().UnixNano())
	defer d.Exec(`DELETE FROM findings WHERE vulnclass=?`, vc)
	ids := make([]int64, 0, 7)
	for i := 0; i < 7; i++ {
		id, addErr := d.AddFinding(0, 0, vc, "", SeverityHigh, fmt.Sprintf("finding %d", i), "", "test", nil)
		if addErr != nil {
			t.Fatalf("AddFinding[%d]: %v", i, addErr)
		}
		ids = append(ids, id)
	}
	sharedCreatedAt := time.Date(2026, time.August, 21, 8, 30, 0, 0, time.UTC)
	if _, err := d.Exec(`UPDATE findings SET created_at=? WHERE vulnclass=?`, sharedCreatedAt, vc); err != nil {
		t.Fatal(err)
	}

	want := make([]int64, len(ids))
	for i := range ids {
		want[i] = ids[len(ids)-1-i]
	}
	for _, sort := range []string{"time", "severity"} {
		t.Run(sort, func(t *testing.T) {
			var got []int64
			for page := 1; page <= 3; page++ {
				items, total, pageErr := d.ListFindingsPage(FindingFilter{VulnClass: vc, Sort: sort}, page, 3)
				if pageErr != nil {
					t.Fatal(pageErr)
				}
				if total != len(ids) {
					t.Fatalf("page %d total=%d, want %d", page, total, len(ids))
				}
				for _, item := range items {
					got = append(got, item.ID)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("same-timestamp pagination was unstable: got=%v want=%v", got, want)
			}
		})
	}
}

func TestFindingGroupsAndUnassignedPaging(t *testing.T) {
	d := openTestDB(t)

	vc := fmt.Sprintf("__test_finding_groups_%d__", time.Now().UnixNano())
	cleanupFindings := func() { _, _ = d.Exec(`DELETE FROM findings WHERE vulnclass=?`, vc) }
	defer cleanupFindings()

	taskA, err := d.CreateTask("group A", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(taskA.ID)
	taskB, err := d.CreateTask("group B", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(taskB.ID)

	seed := []struct {
		taskID   int64
		severity string
		status   string
	}{
		{taskA.ID, SeverityCritical, FindingPending},
		{taskA.ID, SeverityHigh, FindingResolved},
		{taskB.ID, SeverityLow, FindingPending},
		{0, SeverityMedium, FindingPending},
	}
	for i, item := range seed {
		id, addErr := d.AddFinding(item.taskID, 0, vc, "", item.severity, fmt.Sprintf("summary %d", i), "", "test", nil)
		if addErr != nil {
			t.Fatalf("AddFinding[%d]: %v", i, addErr)
		}
		if _, setErr := d.SetFindingStatus(id, item.status); setErr != nil {
			t.Fatalf("SetFindingStatus[%d]: %v", i, setErr)
		}
	}
	matchedGroups, matchedGroupTotal, matchedFindingTotal, err := d.ListFindingGroups(
		FindingFilter{VulnClass: vc, Query: "summary 0"}, 1, 10,
	)
	if err != nil || matchedGroupTotal != 1 || matchedFindingTotal != 1 || len(matchedGroups) != 1 ||
		matchedGroups[0].TaskID == nil || *matchedGroups[0].TaskID != taskA.ID {
		t.Fatalf("query-filtered groups: %+v groups=%d findings=%d err=%v",
			matchedGroups, matchedGroupTotal, matchedFindingTotal, err)
	}
	// Retained findings from deleted tasks join the same bucket as findings that
	// were created without any task.
	if err := d.DeleteTask(taskB.ID); err != nil {
		t.Fatal(err)
	}

	page, groupTotal, findingTotal, err := d.ListFindingGroups(FindingFilter{VulnClass: vc, Sort: "severity"}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || groupTotal != 2 || findingTotal != 4 {
		t.Fatalf("page totals: items=%d groups=%d findings=%d", len(page), groupTotal, findingTotal)
	}
	groups, _, _, err := d.ListFindingGroups(FindingFilter{VulnClass: vc}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	var live, unassigned *FindingGroup
	for i := range groups {
		if groups[i].TaskID == nil {
			unassigned = &groups[i]
		} else if *groups[i].TaskID == taskA.ID {
			live = &groups[i]
		}
	}
	if live == nil || live.Count != 2 || live.Critical != 1 || live.High != 1 || live.TaskDescription != "group A" {
		t.Fatalf("live group mismatch: %+v", live)
	}
	if unassigned == nil || unassigned.Count != 2 || unassigned.Medium != 1 || unassigned.Low != 1 {
		t.Fatalf("unassigned group mismatch: %+v", unassigned)
	}
	groupForTask := func(items []FindingGroup, taskID int64) *FindingGroup {
		for i := range items {
			if items[i].TaskID != nil && *items[i].TaskID == taskID {
				return &items[i]
			}
		}
		return nil
	}
	if err := d.SetPaused(taskA.ID, true); err != nil {
		t.Fatal(err)
	}
	groups, _, _, err = d.ListFindingGroups(FindingFilter{VulnClass: vc}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if group := groupForTask(groups, taskA.ID); group == nil || group.TaskStatus != "paused" {
		t.Fatalf("paused task group status mismatch: %+v", groups)
	}
	if err := d.SetPaused(taskA.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(taskA.ID, "resume"); err != nil {
		t.Fatal(err)
	}
	groups, _, _, err = d.ListFindingGroups(FindingFilter{VulnClass: vc}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if group := groupForTask(groups, taskA.ID); group == nil || group.TaskStatus != "queued" {
		t.Fatalf("queued task group status mismatch: %+v", groups)
	}
	if err := d.SetStatus(taskA.ID, "done"); err != nil {
		t.Fatal(err)
	}
	groups, _, _, err = d.ListFindingGroups(FindingFilter{VulnClass: vc}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if group := groupForTask(groups, taskA.ID); group == nil || group.TaskStatus != "done" {
		t.Fatalf("terminal task status must win over queued flag: %+v", groups)
	}

	orphans, total, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, TaskID: FindingUnassignedTask}, 1, 10)
	if err != nil || total != 2 || len(orphans) != 2 {
		t.Fatalf("unassigned page: len=%d total=%d err=%v", len(orphans), total, err)
	}
	maxPage := int(^uint(0) >> 1)
	farFindings, farTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc}, maxPage, 200)
	if err != nil || farTotal != len(seed) || len(farFindings) != 0 {
		t.Fatalf("far finding page: len=%d total=%d err=%v", len(farFindings), farTotal, err)
	}
	farGroups, farGroupTotal, farFindingTotal, err := d.ListFindingGroups(FindingFilter{VulnClass: vc}, maxPage, 100)
	if err != nil || farGroupTotal != 2 || farFindingTotal != len(seed) || len(farGroups) != 0 {
		t.Fatalf("far group page: len=%d groups=%d findings=%d err=%v",
			len(farGroups), farGroupTotal, farFindingTotal, err)
	}
	filtered, filteredGroups, filteredFindings, err := d.ListFindingGroups(
		FindingFilter{VulnClass: vc, Status: FindingResolved}, 1, 10,
	)
	if err != nil || filteredGroups != 1 || filteredFindings != 1 || len(filtered) != 1 || filtered[0].High != 1 {
		t.Fatalf("filtered groups: %+v groups=%d findings=%d err=%v", filtered, filteredGroups, filteredFindings, err)
	}
}

func TestAddFindingFollowUpIntent(t *testing.T) {
	d := openTestDB(t)

	task, err := d.CreateTask("finding follow-up", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(task.ID)
	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{
		Domain: fmt.Sprintf("follow-up-%d.example.test", time.Now().UnixNano()),
		TaskID: task.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAssetsByWhere(t, d, "WHERE id=?", assetID)

	store := d.Exploration(task.ExplorationID)
	findingNodeID, err := store.AddNode(KindFinding, map[string]any{"summary": "source"}, 5, "confirmed", "worker", []int64{assetID})
	if err != nil {
		t.Fatal(err)
	}
	findingID, err := d.AddFinding(task.ID, findingNodeID, "test", "source", SeverityHigh, "source", "", "worker", []int64{assetID})
	if err != nil {
		t.Fatal(err)
	}

	auditInput := Activity{Worker: "system", Kind: "text", Summary: "人工提交漏洞深入利用意图", Detail: "验证可利用性并形成证据链"}
	intentID, audit, err := store.AddFindingFollowUpIntent(findingID, findingNodeID, "验证可利用性并形成证据链", auditInput)
	if err != nil {
		t.Fatal(err)
	}
	secondID, _, err := store.AddFindingFollowUpIntent(findingID, findingNodeID, "从另一条路径深入", Activity{
		Worker: "system", Kind: "text", Summary: "人工提交漏洞深入利用意图", Detail: "从另一条路径深入",
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondID == intentID {
		t.Fatal("repeated follow-up submissions must create distinct intents")
	}
	expectedAuditSummary := fmt.Sprintf("%s #%d", auditInput.Summary, intentID)
	if audit.ID <= 0 || audit.NodeID == nil || *audit.NodeID != intentID || audit.CreatedAt.IsZero() || audit.Summary != expectedAuditSummary {
		t.Fatalf("persisted audit mismatch: %+v", audit)
	}
	node, err := store.GetNode(intentID)
	if err != nil || node == nil {
		t.Fatalf("GetNode: node=%+v err=%v", node, err)
	}
	if node.Kind != KindIntent || node.Priority != 10 || node.State != "open" || node.Origin != "human" {
		t.Fatalf("follow-up intent metadata: %+v", node)
	}
	var anchorCount, edgeCount int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_anchors WHERE node_id=? AND asset_id=?`, intentID, assetID).Scan(&anchorCount); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_edges
		WHERE exploration_id=? AND src_id=? AND rel=? AND dst_id=?`,
		task.ExplorationID, findingNodeID, RelDerivedFrom, intentID).Scan(&edgeCount); err != nil {
		t.Fatal(err)
	}
	if anchorCount != 1 || edgeCount != 1 {
		t.Fatalf("follow-up lineage: anchors=%d edges=%d", anchorCount, edgeCount)
	}
	var persistedAuditCount int
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity
		WHERE id=? AND exploration_id=? AND node_id=? AND worker='system' AND summary=?`,
		audit.ID, task.ExplorationID, intentID, expectedAuditSummary).Scan(&persistedAuditCount); err != nil {
		t.Fatal(err)
	}
	if persistedAuditCount != 1 {
		t.Fatalf("atomic audit count=%d, want 1", persistedAuditCount)
	}

	var nodesBefore, activitiesBefore int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, task.ExplorationID).Scan(&nodesBefore); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity WHERE exploration_id=?`, task.ExplorationID).Scan(&activitiesBefore); err != nil {
		t.Fatal(err)
	}
	// 这一步要的是"审计写入必然失败"，从而验证图变更整笔回滚。
	// PG 时代靠 metadata 列是 JSONB：塞一个非法 JSON（`{`）就被数据库拒绝。
	// TiDB 的 activity.metadata 是 mediumtext（TiDB 的 TEXT 系列不吃 JSON 校验），
	// 非法 JSON 不再构成失败。改用一个在 TiDB 上同样必然被拒绝的写入：
	// activity.input_tokens 是 4 字节 INT，严格模式（STRICT_TRANS_TABLES）下
	// 超界的值直接 1264，事务必须整笔回滚。
	outOfRangeTokens := 1 << 40
	if _, _, err := store.AddFindingFollowUpIntent(findingID, findingNodeID, "must roll back", Activity{
		Worker: "system", Kind: "text", Summary: "invalid audit",
		Metadata: json.RawMessage(`{`), InputTokens: &outOfRangeTokens,
	}); err == nil {
		t.Fatal("invalid activity metadata should fail the transaction")
	}
	var nodesAfter, activitiesAfter int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, task.ExplorationID).Scan(&nodesAfter); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity WHERE exploration_id=?`, task.ExplorationID).Scan(&activitiesAfter); err != nil {
		t.Fatal(err)
	}
	if nodesAfter != nodesBefore || activitiesAfter != activitiesBefore {
		t.Fatalf("activity failure did not roll back graph: nodes %d->%d activities %d->%d",
			nodesBefore, nodesAfter, activitiesBefore, activitiesAfter)
	}

	if _, err := d.Exec(`DELETE FROM exploration_nodes WHERE id=?`, findingNodeID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddFindingFollowUpIntent(findingID, findingNodeID, "should fail", auditInput); !errors.Is(err, ErrFindingOriginUnavailable) {
		t.Fatalf("missing source node: want ErrFindingOriginUnavailable, got %v", err)
	}
}

// =============================================================================
// P08-03 Task 3.2（M22 / M29）— 漏洞管理与流量关联的 TiDB 方言改造
//
// 两层验证：
//  1. **离线方言门禁**（无需数据库）：证明这 5 个业务文件不再含任何 PostgreSQL
//     专有写法；聚合统计必须是 `COUNT(CASE WHEN … THEN 1 END)`（空表返回数值 0
//     而不是 SQL NULL）；动态过滤必须是参数化的 `?` / `IN (?, ?, …)`；复测结论
//     写入不得再拿 MySQL 的 RowsAffected 当"记录是否存在"的判据（M29）。
//  2. **真机集成用例**：`ARTEX_TIDB_DSN` 在场时端到端跑通聚合、漏洞 CRUD、
//     复测记录更新与删除解除关联；未配置时诚实 skip，绝不算通过。
// =============================================================================

// task32Files 是 Task 3.2 改造清单里的 5 个业务文件（相对 db 包目录）。
var task32Files = []string{
	"findings.go",
	"finding_assets.go",
	"finding_retests.go",
	"finding_traffic.go",
	"finding_traffic_archive.go",
}

// task32DialectMarkers 是 pgDialectMarkers 之外、本任务专属的 PG 残留特征。
// sample 必须是该特征的真实 PG 片段，供自检用例证明扫描器不是空转。
//
// 注意 json 特征的边界：TiDB 自己的 JSON_SET / JSON_CONTAINS / JSON_LENGTH 是合法
// 写法，绝不能顺手把整族 `json*_` 都判成方言，因此 PG 侧只认带 b 的 jsonb_* 以及
// json_build_object / json_to_array / json_populate_recordset / unnest。
var task32DialectMarkers = []struct {
	why     string
	pattern *regexp.Regexp
	sample  string
}{
	{"PostgreSQL aggregate FILTER clause", regexp.MustCompile(`(?i)\bFILTER\s*\(`), `SELECT COUNT(*) FILTER (WHERE f.severity = 'high') FROM findings f`},
	{"PostgreSQL case-insensitive LIKE", regexp.MustCompile(`(?i)\bILIKE\b`), `WHERE f.name ILIKE 'x'`},
	{"PostgreSQL NULLS LAST ordering", regexp.MustCompile(`(?i)\bNULLS\s+(LAST|FIRST)\b`), `ORDER BY t.id DESC NULLS LAST`},
	{"PostgreSQL array ANY predicate", regexp.MustCompile(`(?i)=\s*ANY\s*\(`), `WHERE a.domain = ANY($1::text[])`},
	{"PostgreSQL json set-returning function", regexp.MustCompile(`(?i)\bjsonb_(build_object|array_elements|agg|array_length|set)\b|\bjson_(build_object|to_array)\b|json_populate_recordset|\bunnest\s*\(`), `SELECT * FROM json_populate_recordset(NULL::t, ?::json)`},
	{"PostgreSQL array constructor", regexp.MustCompile(`(?i)\bARRAY\s*\[`), `to_jsonb(ARRAY[a.id])`},
	{"PostgreSQL DISTINCT ON projection", regexp.MustCompile(`(?i)\bDISTINCT\s+ON\s*\(`), `SELECT DISTINCT ON (f.id) f.id FROM findings f`},
}

// task32Source 读出包内某个文件的源码（已抹掉注释，只看真正会发给数据库的文本）。
//
// 唯一豁免：Go 侧的 `time.Now(…)` 会被通用门禁的 `now()` 特征命中，但它是宿主语言
// 的取时函数，不是会发给数据库的 SQL `now()`，因此统一改名后再扫。
func task32Source(t *testing.T, name string) string {
	t.Helper()
	return strings.ReplaceAll(stripGoComments(t, name), "time.Now(", "timeNowEvaluated(")
}

// task32FuncBody 从源码里按声明前缀抠出一个完整函数体（花括号配平扫描）。
func task32FuncBody(t *testing.T, file, decl string) string {
	t.Helper()
	src := task32Source(t, file)
	start := strings.Index(src, decl)
	if start < 0 {
		t.Fatalf("%s: declaration %q not found", file, decl)
	}
	open := strings.Index(src[start:], "{")
	if open < 0 {
		t.Fatalf("%s: no body for %q", file, decl)
	}
	open += start
	depth := 0
	for i := open; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[start : i+1]
			}
		}
	}
	t.Fatalf("%s: unterminated body for %q", file, decl)
	return ""
}

// TestFindings_Task32FilesAreTiDBDialect 是 Step 1 的核心判红用例：5 个文件必须
// 同时通过阶段 1 的通用方言门禁与本任务专属门禁。
func TestFindings_Task32FilesAreTiDBDialect(t *testing.T) {
	for _, name := range task32Files {
		src := task32Source(t, name)
		for _, marker := range pgDialectMarkers {
			if loc := marker.pattern.FindString(src); loc != "" {
				t.Errorf("%s: PostgreSQL dialect %q (%s)", name, loc, marker.why)
			}
		}
		for _, marker := range task32DialectMarkers {
			if loc := marker.pattern.FindString(src); loc != "" {
				t.Errorf("%s: PostgreSQL dialect %q (%s)", name, loc, marker.why)
			}
		}
	}
}

// TestFindings_Task32DialectMarkersDetectOwnSamples 自检专属扫描器不是空转：
// 每个特征都必须命中自己的 PG 样例，否则上面的绿只是"什么都没查"。
func TestFindings_Task32DialectMarkersDetectOwnSamples(t *testing.T) {
	for _, marker := range task32DialectMarkers {
		if !marker.pattern.MatchString(marker.sample) {
			t.Errorf("marker %q does not detect its own PG sample %q", marker.why, marker.sample)
		}
	}
}

// TestFindings_StatsAggregateUsesCaseWhenCounts 钉死 M22 的聚合形状：
// 严重度/状态分桶必须是 COUNT(CASE WHEN … THEN 1 END)，绝不残留 FILTER (WHERE …)。
func TestFindings_StatsAggregateUsesCaseWhenCounts(t *testing.T) {
	src := task32Source(t, "findings.go")
	if loc := regexp.MustCompile(`(?i)\bFILTER\s*\(`).FindString(src); loc != "" {
		t.Fatalf("findings.go still uses %q: aggregate must be COUNT(CASE WHEN … THEN 1 END)", loc)
	}
	caseCount := regexp.MustCompile(`(?is)COUNT\(\s*CASE\s+WHEN\s+.+?\s+THEN\s+1\s+END\s*\)`)
	got := len(caseCount.FindAllString(src, -1))
	// FindingStats 的 5 个分桶（pending/critical/high/medium/low）+
	// ListFindingGroups 的 4 个严重度分桶，至少要 9 处。
	if got < 9 {
		t.Fatalf("COUNT(CASE WHEN … THEN 1 END) count = %d, want >= 9 (FindingStats + ListFindingGroups)", got)
	}
	for _, bucket := range []string{"pending", "critical", "high", "medium", "low"} {
		cond := regexp.MustCompile(`(?is)COUNT\(\s*CASE\s+WHEN\s+[^)]*'` + bucket + `'[^)]*THEN\s+1\s+END\s*\)`)
		if !cond.MatchString(src) {
			t.Errorf("findings.go: severity/status bucket %q is not a COUNT(CASE WHEN … THEN 1 END)", bucket)
		}
	}
}

// TestFindings_DynamicFiltersAreParameterized 钉死动态过滤的形状：
// 一律 `?` 占位符 + 参数化 `IN (?, ?, …)`，绝不允许再按 $n 下标自举。
func TestFindings_DynamicFiltersAreParameterized(t *testing.T) {
	for _, file := range []string{"findings.go", "finding_assets.go", "finding_traffic.go"} {
		src := task32Source(t, file)
		if strings.Contains(src, `fmt.Sprintf("$`) {
			t.Errorf("%s: still builds positional $n placeholders", file)
		}
		if !strings.Contains(src, "questionPlaceholders(") {
			t.Errorf("%s: dynamic IN/JSON filter must reuse questionPlaceholders(…) with ? placeholders", file)
		}
	}
	if src := task32Source(t, "finding_assets.go"); !strings.Contains(src, "IN (") {
		t.Errorf("finding_assets.go: batched id/host lookups must use parameterized IN (?, ?, …)")
	}
}

// TestFindings_RetestResultDoesNotTrustRowsAffected 钉死 M29：复测结论写入必须
// 先锁行回查再判定，不能拿"值未变更 → RowsAffected=0"误判为复测不存在。
func TestFindings_RetestResultDoesNotTrustRowsAffected(t *testing.T) {
	body := task32FuncBody(t, "finding_retests.go", "func (d *DB) RecordFindingRetestResult(")
	if !strings.Contains(body, "FOR UPDATE") {
		t.Errorf("RecordFindingRetestResult must lock the running retest row (SELECT … FOR UPDATE) before deciding")
	}
	if strings.Contains(body, "RowsAffected") {
		t.Errorf("RecordFindingRetestResult must not use RowsAffected to decide existence (unchanged values return 0)")
	}
	if !strings.Contains(body, "ErrRetestNotRunning") {
		t.Errorf("RecordFindingRetestResult must still refuse a second verdict with ErrRetestNotRunning")
	}
}

// TestFindings_DeleteFindingUnbindsTrafficAndRetests 钉死删除语义：漏洞删除必须
// 在同一事务里显式解除 evidence bindings 与 retest 关联（新库没有 FK 级联）。
//
// 「解除关联」只有一种合法实现：finding_retests 子行**整行删除**。把活跃复测改成
// stopped 只终结了状态，completed/failed/stopped 的历史连同指向已删漏洞的
// finding_id 仍然留在库里 —— 那是悬挂引用，不是解除关联。
func TestFindings_DeleteFindingUnbindsTrafficAndRetests(t *testing.T) {
	body := task32FuncBody(t, "findings.go", "func (d *DB) DeleteFinding(")
	for _, want := range []string{"finding_traffic_bindings", "finding_retests", "exploration_nodes"} {
		if !strings.Contains(body, want) {
			t.Errorf("DeleteFinding must explicitly clean up %s inside the same transaction", want)
		}
	}
	if strings.Contains(body, "RETURNING") {
		t.Errorf("DeleteFinding must not use RETURNING (TiDB has none)")
	}
	if !strings.Contains(body, `DELETE FROM finding_retests WHERE finding_id = ?`) {
		t.Errorf("DeleteFinding must delete the finding's retest rows outright (sealing status alone leaves dangling finding_id)")
	}
	if strings.Contains(body, `UPDATE finding_retests`) {
		t.Errorf("DeleteFinding must not merely seal retest status; the whole child row has to go")
	}
	if !strings.Contains(body, "WithEvidenceTx") {
		t.Errorf("DeleteFinding must run inside the shared evidence writer transaction")
	}
}

// TestFindings_DeleteFindingsByTaskIsAtomic 钉死批量删除的事务与锁协议：成员定位、
// 父任务锁、两张子表的清理与父表删除必须全部落在同一个 WithEvidenceTx 里；三条各自
// 提交的 d.Exec 会让中途失败变成「绑定已删、漏洞还在」的半成品，并在语句之间给并发
// 写者留下塞入孤儿子行的窗口。
func TestFindings_DeleteFindingsByTaskIsAtomic(t *testing.T) {
	body := task32FuncBody(t, "findings.go", "func (d *DB) DeleteFindingsByTask(")
	if !strings.Contains(body, "WithEvidenceTx") {
		t.Errorf("DeleteFindingsByTask must run in one WithEvidenceTx (shared writer gate + atomic rollback)")
	}
	if strings.Contains(body, "d.Exec(") {
		t.Errorf("DeleteFindingsByTask must not issue auto-committed d.Exec statements; every statement belongs to the tx")
	}
	if !strings.Contains(body, "LockTaskEvidenceTx") {
		t.Errorf("DeleteFindingsByTask must take the parent task lock (tasks precede findings in the global lock order)")
	}
	for _, want := range []string{
		`DELETE FROM finding_traffic_bindings WHERE finding_id IN (`,
		`DELETE FROM finding_retests WHERE finding_id IN (`,
		`DELETE FROM findings WHERE task_id = ?`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("DeleteFindingsByTask is missing %q", want)
		}
	}
	if strings.Contains(body, `UPDATE finding_retests`) {
		t.Errorf("DeleteFindingsByTask must delete retest rows outright, not seal their status")
	}
	// 持锁后的成员集合必须是当前读，否则锁隙期变化的成员会被漏删或误删。
	if !strings.Contains(body, "taskFindingIDsTx(tx, taskID, true)") {
		t.Errorf("DeleteFindingsByTask must re-read its members under FOR UPDATE after locking the task")
	}
}

// task32ParenGroup 吃一段含第一枚 '(' 的文本，返回该对括号内的内容与右括号之后的剩余。
func task32ParenGroup(text string) (inner, rest string) {
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return text, ""
	}
	depth := 0
	for i := open; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return text[open+1 : i], text[i+1:]
			}
		}
	}
	return text, ""
}

// task32InsertColumns 从一段代码里抠出 `INSERT INTO <table>(…)` 的列清单与 VALUES
// 的顶层表达式，两者必须一一对应（列多参数少 = 严格模式下 1364 的现场）。
func task32InsertColumns(t *testing.T, code, table string) (columns, values []string) {
	t.Helper()
	marker := "INSERT INTO " + table
	start := strings.Index(code, marker)
	if start < 0 {
		t.Fatalf("no `%s` statement found", marker)
	}
	rest := code[start+len(marker):]
	open := strings.Index(rest, "(")
	if open < 0 {
		t.Fatalf("%s: no column list after `%s`", table, marker)
	}
	columnText, afterColumns := task32ParenGroup(rest[open:])
	for _, column := range strings.Split(columnText, ",") {
		if column = strings.TrimSpace(column); column != "" {
			columns = append(columns, column)
		}
	}
	valuesStart := strings.Index(strings.ToUpper(afterColumns), "VALUES")
	if valuesStart < 0 {
		t.Fatalf("%s: no VALUES clause found", table)
	}
	valuesText, _ := task32ParenGroup(afterColumns[valuesStart+len("VALUES"):])
	depth := 0
	var current strings.Builder
	for _, r := range valuesText {
		if r == '(' {
			depth++
		} else if r == ')' {
			depth--
		} else if r == ',' && depth == 0 {
			values = append(values, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	if last := strings.TrimSpace(current.String()); last != "" {
		values = append(values, last)
	}
	return columns, values
}

// TestFindings_StrictInsertsPopulateEveryNotNullTextColumn 钉死 I1：基线里
// findings.status/report 与 finding_retests.status/verdict/notes/summary/evidence/
// error 都是 MEDIUMTEXT NOT NULL，而 TiDB 8.5 严格模式禁止 TEXT/JSON 列带 DEFAULT
// （schema.TextN/JSONNN 的 def 只是声明期元数据，renderColumnFor 一律不输出），
// 所以 INSERT 少写任何一列都不是「数据库补默认值」，而是 1364 直接失败。
func TestFindings_StrictInsertsPopulateEveryNotNullTextColumn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		file     string
		decl     string
		table    string
		required []string
	}{
		{
			name: "AddFinding", file: "findings.go", decl: "func (d *DB) AddFinding(",
			table: "findings", required: []string{"task_id", "node_id", "vulnclass", "name", "severity", "summary", "evidence", "worker", "asset_ids", "status", "report"},
		},
		{
			name: "RecordFindingTx", file: "finding_traffic.go", decl: "func RecordFindingTx(",
			table: "findings", required: []string{"task_id", "node_id", "vulnclass", "name", "severity", "summary", "evidence", "worker", "asset_ids", "status", "report"},
		},
		{
			// CreateFindingRetest 现在只是"撞去重唯一索引 → 新事务重放"的重试外壳，
			// 真正的 SQL 在 createFindingRetestOnce 里，所以这条按新函数体抠。
			name: "CreateFindingRetest", file: "finding_retests.go", decl: "func (d *DB) createFindingRetestOnce(",
			table: "finding_retests", required: []string{"finding_id", "conversation_id", "status", "notes", "verdict", "summary", "evidence", "error", "snapshot"},
		},
		{
			// fixture 自身就在本文件里：按 decl 抠函数体会先命中下面这张表里的
			// 那串声明文本，所以这条走"整份源码"路线（本文件里 INSERT INTO
			// finding_retests 只此一处）。
			name: "task32SeedRetest fixture", file: "findings_test.go", decl: "",
			table: "finding_retests", required: []string{"finding_id", "conversation_id", "status", "notes", "verdict", "summary", "evidence", "error", "snapshot"},
		},
	} {
		code := task32Source(t, tc.file)
		if tc.decl != "" {
			code = task32FuncBody(t, tc.file, tc.decl)
		}
		columns, values := task32InsertColumns(t, code, tc.table)
		declared := map[string]bool{}
		for _, column := range columns {
			declared[strings.ToLower(column)] = true
		}
		for _, want := range tc.required {
			if !declared[strings.ToLower(want)] {
				t.Errorf("%s: INSERT INTO %s omits NOT NULL column %q (strict TiDB has no TEXT default, so this is 1364)", tc.name, tc.table, want)
			}
		}
		if len(values) != len(columns) {
			t.Errorf("%s: INSERT INTO %s lists %d columns but %d VALUES expressions", tc.name, tc.table, len(columns), len(values))
		}
		for i, value := range values {
			if !strings.Contains(value, "?") {
				t.Errorf("%s: VALUES expression %d (%s) is not a bound parameter", tc.name, i, value)
			}
		}
	}
}

// TestFindings_FollowUpIntentLocksInGlobalOrder 钉死 I4 + R2：AddFindingFollowUpIntent
// 必须「普通读定位 → 要求 Business Gate 的 TxRunner → 事务内按全局锁序加锁并重验」。
// 全局锁序是 tasks(1) → exploration_nodes(7) → findings(9)；旧实现先锁 findings 再锁
// tasks，与删除路径（先 task 后 finding）逆序，两个事务交错时必死锁。
//
// R2（上轮 I4 续项）：门禁从"必须有 Begin"改成"必须没有裸 Begin"。裸 Begin/Commit 的
// 事务没有业务门禁（真正的 gate 是 locks.business_relations 行锁，不是任务行锁或
// evidence 锚点锁）、没有 1213/1205/1062 的整体回滚 + 退避重放、也没有提交结局未知时
// 的 fail closed。局部三表顺序正确不能代替全写者协议。
func TestFindings_FollowUpIntentLocksInGlobalOrder(t *testing.T) {
	body := task32FuncBody(t, "findings.go", "func (s *ExplorationStore) AddFindingFollowUpIntent(")
	for _, bare := range []string{"s.db.Begin()", "tx.Commit()", "defer tx.Rollback()"} {
		if strings.Contains(body, bare) {
			t.Errorf("AddFindingFollowUpIntent must not manage its own transaction (%s); run it under taskTxRunner().Run with RequiresBusinessGate", bare)
		}
	}
	// 业务门禁开关必须是 true，且 Runner 就是这条路径唯一的事务入口。
	if !strings.Contains(body, "taskTxRunner().Run(") {
		t.Errorf("AddFindingFollowUpIntent must run through s.db.taskTxRunner().Run (spec §3.2.2 writer protocol)")
	}
	if !strings.Contains(body, "lockorder.TxRunnerOptions{RequiresBusinessGate: true}") {
		t.Errorf("AddFindingFollowUpIntent must require the DB business gate (locks.business_relations), not just a tx")
	}
	gate := strings.Index(body, "RequiresBusinessGate: true")
	if gate < 0 {
		t.Fatalf("AddFindingFollowUpIntent: business gate option not found")
	}
	// 定位读必须发生在进入 Runner（门禁 + 事务）之前，且不得携带任何行锁。
	locating := body[:gate]
	if !strings.Contains(locating, "s.db.QueryRow(`SELECT node_id, task_id FROM findings WHERE id = ?`") {
		t.Errorf("AddFindingFollowUpIntent must locate the finding with a plain (unlocked) read before the gated transaction starts")
	}
	if strings.Contains(locating, "FOR UPDATE") {
		t.Errorf("AddFindingFollowUpIntent must not take any row lock before the global lock order starts")
	}
	// 三把排他锁的先后顺序就是全局锁序本身，且全部在门禁之后取得。
	step := gate
	for _, want := range []string{
		"FROM tasks WHERE id = ? FOR UPDATE",
		"FROM exploration_nodes WHERE id = ? FOR UPDATE",
		"FROM findings WHERE id = ? FOR UPDATE",
	} {
		at := strings.Index(body[step:], want)
		if at < 0 {
			t.Fatalf("AddFindingFollowUpIntent must lock in global order; missing %q after the previous lock", want)
		}
		step += at + len(want)
	}
	// 父任务的删除标记必须在持锁后核验，否则正在归档/清理的任务仍会被派生意图。
	if !strings.Contains(body, "deleted_at") || !strings.Contains(body, "taskDeletedAt.Valid") {
		t.Errorf("AddFindingFollowUpIntent must verify the task deleted_at marker after locking it")
	}
	// Runner 会整体重放 payload：payload 里就地累加 audit 的话，重试一次就多发一条
	// 带 " #<id>" 后缀的审计文本。所以必须从入参副本出发，不能改写形参。
	if strings.Contains(body, "audit.NodeID = ") || strings.Contains(body, "audit.Summary = ") {
		t.Errorf("AddFindingFollowUpIntent must not mutate the audit argument inside a replayable payload; copy it per attempt")
	}
}

// TestFindings_CRUDDefaultsAreAssertedBeforeAnyStatusMutation 是 R1 的离线门禁：
// 默认值（status='pending' / report=”）只在 INSERT 那一刻成立，先按 seed 改状态再断言
// 初态，实现全对也会稳定误报两条。真机缺席时这条用例负责把"先断言、后改状态"的顺序
// 钉死在源码形状上；setter 之后还必须按 seed 目标状态回读一次。
func TestFindings_CRUDDefaultsAreAssertedBeforeAnyStatusMutation(t *testing.T) {
	// decl 拼串而成：task32FuncBody 取的是**首个**匹配，若这里写成完整的
	// "func TestFindings_CRUDRoundTrip(" 字面量，本测试自己的调用参数就会先被匹配上，
	// 抠出来的是本测试这一段，断言全部落空（真机 R1 又躲过门禁）。
	body := task32FuncBody(t, "findings_test.go", "func "+"TestFindings_CRUDRoundTrip(")
	defaults := strings.Index(body, "SELECT status, report FROM findings WHERE id = ?")
	setter := strings.Index(body, "d.SetFindingStatus(")
	if defaults < 0 {
		t.Fatalf("CRUD test lost the strict defaults read-back (SELECT status, report FROM findings WHERE id = ?)")
	}
	if setter < 0 {
		t.Fatalf("CRUD test lost its SetFindingStatus coverage")
	}
	if defaults > setter {
		t.Errorf("the default value read-back must precede the first SetFindingStatus call (index %d > %d): asserting 'pending' after the seed changed the status is a guaranteed false failure", defaults, setter)
	}
	// setter 之后必须按 seed 断言目标状态，而不是继续核默认值。
	lastSetter := strings.LastIndex(body, "d.SetFindingStatus(")
	seeded := strings.Index(body[lastSetter:], "SELECT status FROM findings WHERE id = ?")
	if seeded < 0 {
		t.Errorf("the CRUD test must read the row back after SetFindingStatus and assert the seeded status")
	}
}

// TestFindings_FilterQueryIsCaseInsensitiveUnderBinaryCollation 钉死 I5：基线表是
// utf8mb4_bin，裸 LIKE 大小写敏感，直接拿它替换 ILIKE 会让大小写不同的关键词搜不到。
// 形状门禁：列侧 5 个 LOWER(…)，参数侧在 Go 里已折叠成小写，且元字符转义原样保留。
func TestFindings_FilterQueryIsCaseInsensitiveUnderBinaryCollation(t *testing.T) {
	where, args := FindingFilter{Query: "SQL Injection"}.where()
	if got, want := strings.Count(where, "LOWER("), 5; got != want {
		t.Errorf("case-folded predicates = %d, want %d (one per searched column): %s", got, want, where)
	}
	if len(args) != 5 {
		t.Fatalf("query filter: want 5 bound patterns, got %d", len(args))
	}
	for i, arg := range args {
		pattern, ok := arg.(string)
		if !ok {
			t.Fatalf("arg %d is %T, want string", i, arg)
		}
		if pattern != "%sql injection%" {
			t.Errorf("arg %d = %q, want the Go-side lower-cased %%sql injection%%", i, pattern)
		}
	}
	// 折叠不能把 LIKE 元字符一起放出去：转义先发生，大小写折叠只作用于字母。
	_, rawArgs := FindingFilter{Query: `MiXeD%\_`}.where()
	if len(rawArgs) != 5 {
		t.Fatalf("escaped query filter: want 5 args, got %d", len(rawArgs))
	}
	// 期望值按段拼接，免得反斜杠的层数在 raw string 里数错。
	want := "%" + "mixed" + `\%` + `\\` + `\_` + "%"
	if pattern, _ := rawArgs[0].(string); pattern != want {
		t.Errorf("LIKE wildcards were not escaped before folding, got %q, want %q", pattern, want)
	}
}

// TestFindings_FindingStats_UnmigratedDialect 是 Step 1 的聚合统计用例（真机）。
func TestFindings_FindingStats_UnmigratedDialect(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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

	stats, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats failed: %v", err)
	}
	if stats == nil || stats.Total < 0 {
		t.Fatalf("expected valid non-negative FindingStats, got %+v", stats)
	}
	if stats.Critical+stats.High+stats.Medium+stats.Low > stats.Total {
		t.Fatalf("severity buckets exceed total: %+v", stats)
	}
	if stats.Pending > stats.Total {
		t.Fatalf("pending bucket exceeds total: %+v", stats)
	}
	if stats.VulnClasses == nil || stats.Tasks == nil {
		t.Fatalf("FindingStats slices must be non-nil empty slices, got %+v", stats)
	}
}

// TestFindings_CRUDRoundTrip 覆盖漏洞 CRUD + 聚合差值 + 分页筛选（真机）。
func TestFindings_CRUDRoundTrip(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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

	vc := fmt.Sprintf("P08T32 %d", time.Now().UnixNano())
	var ids []int64
	// M1：这条 cleanup **先注册所以最后运行**，负责终态零残留断言；每条 fixture 的
	// 按主键清理则在创建成功后立刻登记（见 task32TrackFinding），因此即使中途 Fatal
	// 或 fixture 被 SetFindingVulnClass 改了类别（vc+"-moved"）也不会留下残渣。
	t.Cleanup(func() { task32AssertNoFindingResidue(t, d, vc, vc+"-moved") })

	before, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats (before) failed: %v", err)
	}

	seed := []struct{ severity, status, summary string }{
		{SeverityCritical, FindingPending, "P08T32 critical case"},
		{SeverityHigh, FindingInProgress, "P08T32 high case"},
		{SeverityLow, FindingResolved, "P08T32 low case"},
	}
	for _, s := range seed {
		id, err := d.AddFinding(0, 0, vc, "名称-"+s.severity, s.severity, s.summary, "poc", "worker", []int64{900001, 900002})
		if err != nil || id <= 0 {
			t.Fatalf("AddFinding(%s): id=%d err=%v", s.severity, id, err)
		}
		ids = append(ids, id)
		task32TrackFinding(t, d, id)
		// I1：TiDB 严格模式下这些 NOT NULL 文本列没有默认值可补，新增漏洞必须已经
		// 带上应用层默认值（'pending' / 空报告），而不是留到读取时才"看起来对"。
		//
		// R1：这条回读**必须紧跟 AddFinding、排在一切 SetFindingStatus 之前**。默认值
		// 只在 INSERT 那一刻成立，先按 seed 把 high 改成 in_progress、low 改成
		// resolved 再去断言 pending，实现完全正确也会稳定误报两条 —— 那不是产品缺陷，
		// 而是测试自己把初态改掉了。
		var status, report string
		if err := d.QueryRow(`SELECT status, report FROM findings WHERE id = ?`, id).Scan(&status, &report); err != nil {
			t.Fatalf("read back strict defaults (%d): %v", id, err)
		}
		if status != FindingPending || report != "" {
			t.Errorf("AddFinding defaults (%d): status=%q report=%q, want %q/%q", id, status, report, FindingPending, "")
		}
		if s.status != FindingPending {
			if n, err := d.SetFindingStatus(id, s.status); err != nil || n != 1 {
				t.Fatalf("SetFindingStatus(%d,%s): n=%d err=%v", id, s.status, n, err)
			}
		}
		// M29 同一语义：把同一个状态值再写一次，行仍然存在，绝不能因为
		// "值未变更"被报成 0 行（上层据此会误判漏洞不存在）。
		if n, err := d.SetFindingStatus(id, s.status); err != nil || n != 1 {
			t.Fatalf("SetFindingStatus idempotent rewrite (%d,%s): n=%d err=%v", id, s.status, n, err)
		}
		// R1：setter 之后按 seed 断言**目标状态**（这里核的已经不是默认值，而是状态
		// 推进真的落到了这一行；RowsAffected=1 与库里的值可能来自不同的读）。
		var settled string
		if err := d.QueryRow(`SELECT status FROM findings WHERE id = ?`, id).Scan(&settled); err != nil {
			t.Fatalf("read back seeded status (%d): %v", id, err)
		}
		if settled != s.status {
			t.Errorf("status after SetFindingStatus (%d) = %q, want %q", id, settled, s.status)
		}
	}

	after, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats (after) failed: %v", err)
	}
	if got, want := after.Total-before.Total, len(seed); got != want {
		t.Errorf("stats total delta = %d, want %d", got, want)
	}
	// 六个桶逐个核差值（M1）：少一个桶就少一条能证明 CASE 聚合真机行为的断言。
	for _, bucket := range []struct {
		name        string
		delta, want int
	}{
		{"critical", after.Critical - before.Critical, 1},
		{"high", after.High - before.High, 1},
		{"medium", after.Medium - before.Medium, 0},
		{"low", after.Low - before.Low, 1},
		{"pending", after.Pending - before.Pending, 1},
	} {
		if bucket.delta != bucket.want {
			t.Errorf("stats %s delta = %d, want %d", bucket.name, bucket.delta, bucket.want)
		}
	}
	if !slices.Contains(after.VulnClasses, vc) {
		t.Errorf("vulnclass %q missing from FindingStats.VulnClasses", vc)
	}

	// 分页 + 类别筛选
	page, total, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Sort: "severity"}, 1, 10)
	if err != nil || total != len(seed) || len(page) != len(seed) {
		t.Fatalf("ListFindingsPage: len=%d total=%d err=%v", len(page), total, err)
	}
	if page[0].Severity != SeverityCritical {
		t.Errorf("severity sort: first row = %q, want %q", page[0].Severity, SeverityCritical)
	}

	// I5：基线是 utf8mb4_bin，大小写必须靠 LOWER + Go 侧折叠才无关；正文是小写的
	// "P08T32 high case"，三种大小写的关键词必须都命中同一条。
	for _, keyword := range []string{"high case", "HIGH CASE", "HiGh CaSe"} {
		matched, matchedTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Query: keyword}, 1, 10)
		if err != nil || matchedTotal != 1 || len(matched) != 1 {
			t.Fatalf("case-insensitive query %q: len=%d total=%d err=%v, want 1/1", keyword, len(matched), matchedTotal, err)
		}
	}

	// 状态筛选
	paged, pagedTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Status: FindingResolved}, 1, 10)
	if err != nil || pagedTotal != 1 || len(paged) != 1 {
		t.Fatalf("status filter: len=%d total=%d err=%v", len(paged), pagedTotal, err)
	}

	// 分组视图的分桶计数同样必须是 CASE 形状（空桶返回 0，不是 NULL）
	groups, groupTotal, findingTotal, err := d.ListFindingGroups(FindingFilter{VulnClass: vc}, 1, 10)
	if err != nil {
		t.Fatalf("ListFindingGroups failed: %v", err)
	}
	if groupTotal != 1 || findingTotal != len(seed) || len(groups) != 1 {
		t.Fatalf("ListFindingGroups totals: groups=%d findings=%d len=%d", groupTotal, findingTotal, len(groups))
	}
	if groups[0].Critical != 1 || groups[0].High != 1 || groups[0].Low != 1 || groups[0].Medium != 0 {
		t.Errorf("group severity buckets: %+v want critical=1 high=1 low=1 medium=0", groups[0])
	}
	if groups[0].TaskID != nil {
		t.Errorf("group task id: want nil for unassigned findings, got %v", *groups[0].TaskID)
	}

	// 单字段 setter 与 report 回读
	if n, err := d.SetFindingName(ids[0], "改名后的漏洞"); err != nil || n != 1 {
		t.Fatalf("SetFindingName: n=%d err=%v", n, err)
	}
	if n, err := d.SetFindingVulnClass(ids[0], vc+"-moved"); err != nil || n != 1 {
		t.Fatalf("SetFindingVulnClass: n=%d err=%v", n, err)
	}
	if n, err := d.SetFindingSeverity(ids[0], SeverityMedium); err != nil || n != 1 {
		t.Fatalf("SetFindingSeverity: n=%d err=%v", n, err)
	}

	// I5：大小写折叠只作用于字母，LIKE 元字符必须仍按字面匹配 —— 名字里带 "%_" 的
	// 这条是唯一能被查询 "%_" 命中的行（ids[0] 已被改成 vc+"-moved"，不在本筛选内）。
	if _, err := d.Exec(`UPDATE findings SET name = ? WHERE id = ?`, "字面 %_ 标记", ids[2]); err != nil {
		t.Fatalf("seed literal-wildcard name: %v", err)
	}
	literal, literalTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Query: "%_"}, 1, 10)
	if err != nil || literalTotal != 1 || len(literal) != 1 || literal[0].ID != ids[2] {
		t.Fatalf("query wildcards must stay literal: len=%d total=%d err=%v", len(literal), literalTotal, err)
	}

	// 导出去向：按 ids 精确导出必须走参数化 IN (?, ?, …)
	Exported, err := d.ListFindingsForExport(FindingFilter{}, ids)
	if err != nil || len(Exported) != len(ids) {
		t.Fatalf("ListFindingsForExport by ids: len=%d err=%v", len(Exported), err)
	}

	// 逐条清理，避免污染 vulnclass 之外的统计
	for _, id := range ids {
		if n, err := d.DeleteFinding(id); err != nil || n != 1 {
			t.Fatalf("DeleteFinding(%d): n=%d err=%v", id, n, err)
		}
		if f, err := d.GetFinding(id); err != nil || f != nil {
			t.Fatalf("GetFinding after delete (%d): finding=%+v err=%v", id, f, err)
		}
	}
	if n, err := d.DeleteFinding(ids[0]); err != nil || n != 0 {
		t.Fatalf("re-delete: want (0,nil), got (%d,%v)", n, err)
	}
}

// TestFindings_DeleteFindingUnbindsEvidenceAndRetests 覆盖删除时的显式解除（I2）：
// finding_traffic_bindings 与 finding_retests 都必须整行删除 —— 两张子表的 finding_id
// 都是 NOT NULL，没有"置空"这条退路；只把活跃复测封成 stopped 会让 completed/failed
// 的历史连同指向已删漏洞的 finding_id 永久悬挂在库里。承载复测的 conversation 独立
// 保留（历史会话不随漏洞删除）。
func TestFindings_DeleteFindingUnbindsEvidenceAndRetests(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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

	findingID, err := d.AddFinding(0, 0, fmt.Sprintf("P08T32 del %d", time.Now().UnixNano()), "待删除", SeverityHigh, "摘要", "证据", "worker", nil)
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	// M1：按主键登记的 cleanup 在创建成功后**立即**注册（证据与复测的 seed helper 也
	// 各自登记自己那一份），因此 seed 半途失败同样不会漏清理。
	task32TrackFinding(t, d, findingID)

	snapshotID := task32BindEvidence(t, d, findingID)
	// active 与 terminal 两类复测历史都要被解除：前者证明没有留下"复测中"，后者才
	// 证明删除真的解除了关联，而不是只改了活跃那几条的状态。
	runningConv := task32SeedRetest(t, d, findingID, "running", "", "复测摘要", "复测证据")
	completedConv := task32SeedRetest(t, d, findingID, "completed", "fixed", "已完成", "已完成证据")

	if n, err := d.DeleteFinding(findingID); err != nil || n != 1 {
		t.Fatalf("DeleteFinding: n=%d err=%v", n, err)
	}

	if bindings := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID); bindings != 0 {
		t.Errorf("evidence bindings after delete = %d, want 0", bindings)
	}
	if dangling := task32Count(t, d, `SELECT COUNT(*) FROM finding_retests WHERE finding_id = ?`, findingID); dangling != 0 {
		t.Errorf("retest rows still owned by the deleted finding = %d, want 0 (dangling finding_id)", dangling)
	}
	if bound := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE snapshot_id = ?`, snapshotID); bound != 0 {
		t.Errorf("snapshot still bound after delete = %d, want 0", bound)
	}
	for _, conversationID := range []int64{runningConv, completedConv} {
		if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_retests WHERE conversation_id = ?`, conversationID); left != 0 {
			t.Errorf("retest rows of conversation %d after delete = %d, want 0", conversationID, left)
		}
		if kept := task32Count(t, d, `SELECT COUNT(*) FROM conversations WHERE id = ?`, conversationID); kept != 1 {
			t.Errorf("retest conversation %d must survive finding deletion, got %d", conversationID, kept)
		}
	}
	active, err := d.ListActiveFindingRetests(context.Background())
	if err != nil {
		t.Fatalf("ListActiveFindingRetests failed: %v", err)
	}
	for _, item := range active {
		if item.ConversationID == runningConv || item.FindingID == findingID {
			t.Errorf("deleted finding still owns an active retest: %+v", item)
		}
	}
}

// TestFindings_DeleteFindingsByTaskRemovesEveryChildRow 覆盖任务级批量删除的解除
// 完整性：该任务名下每条漏洞的证据绑定与复测子行（active + terminal）都必须归零。
func TestFindings_DeleteFindingsByTaskRemovesEveryChildRow(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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
	task, err := d.CreateTask("P08T32 批量删除", "goal", nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	defer func() {
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("delete task: %v", err)
		}
	}()

	vc := fmt.Sprintf("P08T32 batch %d", time.Now().UnixNano())
	var ids []int64
	for i := range 3 {
		findingID, err := d.AddFinding(task.ID, 0, vc, fmt.Sprintf("批量删除 %d", i), SeverityHigh, "摘要", "证据", "worker", nil)
		if err != nil || findingID <= 0 {
			t.Fatalf("AddFinding[%d]: id=%d err=%v", i, findingID, err)
		}
		ids = append(ids, findingID)
		task32TrackFinding(t, d, findingID)
		task32BindEvidence(t, d, findingID)
		// 一半挂 active 复测、一半挂 terminal 复测，两类都必须随漏洞消失。
		task32SeedRetest(t, d, findingID, []string{"running", "completed", "stopped"}[i], "", "摘要", "证据")
	}

	deleted, err := d.DeleteFindingsByTask(task.ID)
	if err != nil {
		t.Fatalf("DeleteFindingsByTask: %v", err)
	}
	if deleted != int64(len(ids)) {
		t.Errorf("DeleteFindingsByTask rows = %d, want %d", deleted, len(ids))
	}
	for _, findingID := range ids {
		if left := task32Count(t, d, `SELECT COUNT(*) FROM findings WHERE id = ?`, findingID); left != 0 {
			t.Errorf("finding %d survived the task cascade", findingID)
		}
		if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID); left != 0 {
			t.Errorf("bindings owned by %d after task delete = %d, want 0", findingID, left)
		}
		if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_retests WHERE finding_id = ?`, findingID); left != 0 {
			t.Errorf("retests owned by %d after task delete = %d, want 0", findingID, left)
		}
	}
	// 幂等：再删一次是 0 行，不是错误。
	if deleted, err := d.DeleteFindingsByTask(task.ID); err != nil || deleted != 0 {
		t.Errorf("re-delete: want (0,nil), got (%d,%v)", deleted, err)
	}
}

// TestFindings_DeleteFindingsByTaskRollsBackOnLockFailure 钉死 I3 的原子性：父任务
// 锁（归档状态门禁）失败时整笔事务必须回滚，绝不能出现"绑定已删、漏洞还在"的半成品。
func TestFindings_DeleteFindingsByTaskRollsBackOnLockFailure(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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
	task, err := d.CreateTask("P08T32 回滚", "goal", nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	// 先注册删任务、后注册"撤销删除标记"：t.Cleanup 是 LIFO，恢复 deleted_at 一定先于
	// DeleteTask 执行，否则会在一个已打删除标记的任务上去删任务。
	t.Cleanup(func() {
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("delete task: %v", err)
		}
	})

	findingID, err := d.AddFinding(task.ID, 0, fmt.Sprintf("P08T32 rb %d", time.Now().UnixNano()), "回滚样本", SeverityHigh, "摘要", "证据", "worker", nil)
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	task32TrackFinding(t, d, findingID)
	task32BindEvidence(t, d, findingID)
	task32SeedRetest(t, d, findingID, "running", "", "摘要", "证据")

	// 让父任务锁先失败（deleted_at 非空 → LockTaskEvidenceTx → ErrTaskArchiveState）。
	if _, err := d.Exec(`UPDATE tasks SET deleted_at = CURRENT_TIMESTAMP(3) WHERE id = ?`, task.ID); err != nil {
		t.Fatalf("mark task deleted: %v", err)
	}
	t.Cleanup(func() {
		task32MustExec(t, d, `UPDATE tasks SET deleted_at = NULL WHERE id = ?`, task.ID)
	})

	if _, err := d.DeleteFindingsByTask(task.ID); !errors.Is(err, ErrTaskArchiveState) {
		t.Fatalf("DeleteFindingsByTask on a deleting task: want ErrTaskArchiveState, got %v", err)
	}
	// 失败之后每一条子行与父行都必须原封不动：三条独立自动提交语句做不到这点。
	if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID); left != 1 {
		t.Errorf("bindings after a rolled-back task delete = %d, want 1 (nothing may be removed)", left)
	}
	if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_retests WHERE finding_id = ?`, findingID); left != 1 {
		t.Errorf("retests after a rolled-back task delete = %d, want 1 (nothing may be removed)", left)
	}
	if left := task32Count(t, d, `SELECT COUNT(*) FROM findings WHERE id = ?`, findingID); left != 1 {
		t.Errorf("finding after a rolled-back task delete = %d, want 1", left)
	}
}

// TestFindings_DeleteFindingsByTaskSerializesEvidenceWriters 钉死 I3 的并发窗口：
// 批量删除与"检查漏洞存在 → 追加绑定"共用同一把证据写者门禁，因此删除一旦提交，
// 任何后来者都不可能在已删漏洞上再挂出一条绑定。
func TestFindings_DeleteFindingsByTaskSerializesEvidenceWriters(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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
	task, err := d.CreateTask("P08T32 并发绑定", "goal", nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	defer func() {
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("delete task: %v", err)
		}
	}()
	findingID, err := d.AddFinding(task.ID, 0, fmt.Sprintf("P08T32 race %d", time.Now().UnixNano()), "并发样本", SeverityHigh, "摘要", "证据", "worker", nil)
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	task32TrackFinding(t, d, findingID)
	// 并发写者会不断造出新的内容寻址快照；测试结束按来源标记整批清掉，不给基线库
	// 留下无主快照（M1 的零残留同样覆盖证据快照）。
	t.Cleanup(func() {
		task32MustExec(t, d, `DELETE FROM finding_traffic_bindings WHERE snapshot_id IN (SELECT id FROM traffic_evidence_snapshots WHERE source_traffic_id LIKE ?)`, "p08-t32-race-%")
		task32MustExec(t, d, `DELETE FROM traffic_evidence_snapshots WHERE source_traffic_id LIKE ?`, "p08-t32-race-%")
	})

	var (
		wg        sync.WaitGroup
		stop      = make(chan struct{})
		failures  = make(chan error, 8)
		attempts  atomic.Int64
		committed atomic.Int64
	)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := d.WithEvidenceTx(context.Background(), func(tx *sql.Tx) error {
					// 与删除共用证据门禁，因此这段"存在性检查 + 追加绑定"整体串行。
					//
					// 存在性检查必须是**当前读**（FOR UPDATE）：PG 的 READ COMMITTED 每条
					// 语句取新快照，拿到门禁锁之后的第一条 SELECT 必然看得见刚提交的删除；
					// TiDB 默认 REPEATABLE READ，普通读沿用本事务进入时的旧快照，会拿着
					// "漏洞还在"的旧结论在已删漏洞上挂出绑定。生产侧同一协议
					// （LockFindingEvidenceTx）本来就是先锁 findings 行再判定，这里对齐。
					var live int
					if err := tx.QueryRow(`SELECT 1 FROM findings WHERE id = ? FOR UPDATE`, findingID).Scan(&live); err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							return nil // 漏洞已删：不挂绑定，也不报错
						}
						return err
					}
					if live == 0 {
						return nil
					}
					if err := AddFindingTrafficTx(tx, findingID, []PreparedTrafficEvidence{{
						Ref:      TrafficRef{TrafficID: fmt.Sprintf("race-%d", attempts.Add(1)), Role: "supporting"},
						Snapshot: task32EvidenceSnapshot(fmt.Sprintf("p08-t32-race-%d-%d", findingID, attempts.Load())),
					}}); err != nil {
						return err
					}
					committed.Add(1)
					return nil
				})
				if err != nil {
					select {
					case failures <- err:
					default:
					}
					return
				}
			}
		}()
	}
	if _, err := d.DeleteFindingsByTask(task.ID); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("DeleteFindingsByTask: %v", err)
	}
	close(stop)
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Errorf("concurrent evidence writer failed: %v", failure)
	}
	if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID); left != 0 {
		t.Errorf("bindings created against the deleted finding = %d, want 0", left)
	}
}

// TestFindings_RetestResultIdenticalRetry 覆盖 M29 的真机症状：同一结论重复提交
// 必须成功（MySQL 的 RowsAffected 是 0），换一个新结论必须被拒。
func TestFindings_RetestResultIdenticalRetry(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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

	findingID, err := d.AddFinding(0, 0, fmt.Sprintf("P08T32 retest %d", time.Now().UnixNano()), "复测目标", SeverityHigh, "摘要", "证据", "worker", nil)
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	task32TrackFinding(t, d, findingID)

	convID := task32SeedRetest(t, d, findingID, "running", "reproduced", "同样的摘要", "同样的证据")

	if err := d.RecordFindingRetestResult(context.Background(), convID, "reproduced", "同样的摘要", "同样的证据"); err != nil {
		t.Fatalf("identical retry must succeed, got %v", err)
	}
	if err := d.RecordFindingRetestResult(context.Background(), convID, "fixed", "换个结论", "换个证据"); !errors.Is(err, ErrRetestNotRunning) {
		t.Fatalf("second verdict: want ErrRetestNotRunning, got %v", err)
	}
	if err := d.RecordFindingRetestResult(context.Background(), convID, "reproduced", "同样的摘要", "同样的证据"); err != nil {
		t.Fatalf("identical retry after refusal must still succeed, got %v", err)
	}
	var verdict, summary string
	if err := d.QueryRow(`SELECT verdict, summary FROM finding_retests WHERE conversation_id = ?`, convID).Scan(&verdict, &summary); err != nil {
		t.Fatalf("read back verdict failed: %v", err)
	}
	if verdict != "reproduced" || summary != "同样的摘要" {
		t.Errorf("persisted verdict changed: verdict=%q summary=%q", verdict, summary)
	}
}

// TestFindings_CreateFindingRetestWritesStrictDefaults 是真机侧的 I1 验收：发起复测
// 时每条 NOT NULL 文本列都必须带上应用层默认值，否则严格 TiDB 直接 1364，复测根本
// 发不起来（Light 的 fixture 补齐掩盖不了产品 INSERT 缺列）。
func TestFindings_CreateFindingRetestWritesStrictDefaults(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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
	findingID, err := d.AddFinding(0, 0, fmt.Sprintf("P08T32 strict %d", time.Now().UnixNano()), "严格默认值", SeverityHigh, "摘要", "证据", "worker", nil)
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	task32TrackFinding(t, d, findingID)

	retest, conversation, created, err := d.CreateFindingRetest(context.Background(), findingID, "补充说明")
	if err != nil {
		t.Fatalf("CreateFindingRetest: %v", err)
	}
	if !created || retest == nil || conversation == nil {
		t.Fatalf("CreateFindingRetest must dispatch a fresh run, got created=%v retest=%+v", created, retest)
	}
	// R4：CreateFindingRetest 成功提交时必然连带写一条 conversation 与一条首轮活动，
	// 只登记 finding 的 cleanup 会让这条测试在"全绿"的同时每次留下一整套会话残渣。
	// 拿到主键后**立即**登记，因此其后任何一步 Fatal 也不会漏清理。
	task32TrackRetestConversation(t, d, conversation.ID)
	var status, notes, verdict, summary, evidence, failure string
	if err := d.QueryRow(`SELECT status, notes, verdict, summary, evidence, error FROM finding_retests WHERE id = ?`, retest.ID).
		Scan(&status, &notes, &verdict, &summary, &evidence, &failure); err != nil {
		t.Fatalf("read back retest defaults: %v", err)
	}
	if status != "pending" {
		t.Errorf("new retest status = %q, want pending", status)
	}
	if notes != "补充说明" {
		t.Errorf("new retest notes = %q, want the trimmed input", notes)
	}
	if verdict != "" || summary != "" || evidence != "" || failure != "" {
		t.Errorf("new retest conclusion columns must start empty, got verdict=%q summary=%q evidence=%q error=%q", verdict, summary, evidence, failure)
	}
}

// TestFindings_RetestSnapshotKeepsIDsAndTextTypes 是真机侧的 I6 验收：快照里的资产
// id 不能被 float64 舍入，普通 TEXT 列（evidence/summary/notes）即使正文是 JSON 也
// 必须仍是字符串。
func TestFindings_RetestSnapshotKeepsIDsAndTextTypes(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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
	const hugeID = 9007199254740993 // 2^53 + 1：float64 会把它舍成 …992
	findingID, err := d.AddFinding(0, 0, fmt.Sprintf("P08T32 snap %d", time.Now().UnixNano()), "快照保真", SeverityHigh,
		`{"note":"summary 正文长得像 JSON"}`, `{"payload":[{"huge":9007199254740993}]}`, "worker", []int64{hugeID})
	if err != nil || findingID <= 0 {
		t.Fatalf("AddFinding: id=%d err=%v", findingID, err)
	}
	task32TrackFinding(t, d, findingID)

	retest, conversation, created, err := d.CreateFindingRetest(context.Background(), findingID, "")
	if err != nil || !created || retest == nil {
		t.Fatalf("CreateFindingRetest: created=%v err=%v", created, err)
	}
	// R4：返回的 conversation 不能被丢掉就走 —— 它连同首轮 activity 一样是这次测试
	// 造出来的行，必须按主键登记清理并核查终态。
	if conversation == nil {
		t.Fatalf("CreateFindingRetest must return the conversation it created")
	}
	task32TrackRetestConversation(t, d, conversation.ID)
	var raw []byte
	if err := d.QueryRow(`SELECT snapshot FROM finding_retests WHERE id = ?`, retest.ID).Scan(&raw); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "9007199254740993") || strings.Contains(text, "9007199254740992") {
		t.Errorf("64-bit ids were rounded inside the snapshot: %s", text)
	}
	// 回读也必须 UseNumber：拿默认 interface{} 解码去断言，测试自己就先丢了精度。
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var doc struct {
		Finding map[string]any `json:"finding"`
	}
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("snapshot is not valid json: %v (%s)", err, text)
	}
	// 普通 TEXT 列（evidence/summary）正文恰好是 JSON 也必须保持字符串。
	if evidence, ok := doc.Finding["evidence"].(string); !ok || !strings.Contains(evidence, "payload") {
		t.Errorf("ordinary TEXT evidence must stay a string in the snapshot, got %T %v", doc.Finding["evidence"], doc.Finding["evidence"])
	}
	if _, ok := doc.Finding["summary"].(string); !ok {
		t.Errorf("ordinary TEXT summary must stay a string, got %T", doc.Finding["summary"])
	}
	// JSON 载荷列（asset_ids）解回数组，且元素仍是那个 64 位整数本身。
	ids, ok := doc.Finding["asset_ids"].([]any)
	if !ok || len(ids) != 1 {
		t.Fatalf("json asset_ids must decode into an array, got %T %v", doc.Finding["asset_ids"], doc.Finding["asset_ids"])
	}
	if number, isNumber := ids[0].(json.Number); !isNumber || number.String() != "9007199254740993" {
		t.Errorf("asset id in the snapshot = %v (%T), want 9007199254740993", ids[0], ids[0])
	}
}

// task32MustExec 执行一条 fixture 写语句并检查错误：cleanup 里的 `_, _ =` 会把
// 清理失败静默吞掉，规格要求的"零残留"就只剩一句口头承诺。
func task32MustExec(t *testing.T, d *DB, query string, args ...any) {
	t.Helper()
	if _, err := d.Exec(query, args...); err != nil {
		t.Errorf("fixture exec %q failed: %v", query, err)
	}
}

// task32Count 读一个计数，读失败直接判红（不能把"读不到"当成 0）。
func task32Count(t *testing.T, d *DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := d.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count via %q failed: %v", query, err)
	}
	return n
}

// task32TrackFinding 为一条 fixture 漏洞主键登记 cleanup：创建成功后**立即**调用，
// 按子表 → 父表的顺序删除，逐条检查错误，且父表那条必须最多只动一行（按主键删）。
//
// R4：终态核查按**记录下来的 finding 主键**直接数子表，绝不 JOIN findings ——
// JOIN 只能看见父行还在的子行，父行被删光之后真正悬挂的孤儿根本进不了 JOIN，
// 那种断言拿到 0 什么也证明不了。断言**先注册**（cleanup 逆序，所以最后运行），
// 因此它核的是按主键清理真正跑完之后的库状态。
func task32TrackFinding(t *testing.T, d *DB, findingID int64) {
	t.Helper()
	t.Cleanup(func() {
		for _, check := range []struct{ table, query string }{
			{"finding_traffic_bindings", `SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`},
			{"finding_retests", `SELECT COUNT(*) FROM finding_retests WHERE finding_id = ?`},
			{"findings", `SELECT COUNT(*) FROM findings WHERE id = ?`},
		} {
			if left := task32Count(t, d, check.query, findingID); left != 0 {
				t.Errorf("residual %s rows for fixture finding %d = %d, want 0 (primary-key check, no JOIN on the parent)", check.table, findingID, left)
			}
		}
	})
	t.Cleanup(func() {
		for _, query := range []string{
			`DELETE FROM finding_traffic_bindings WHERE finding_id = ?`,
			`DELETE FROM finding_retests WHERE finding_id = ?`,
		} {
			task32MustExec(t, d, query, findingID)
		}
		res, err := d.Exec(`DELETE FROM findings WHERE id = ?`, findingID)
		if err != nil {
			t.Errorf("cleanup finding %d failed: %v", findingID, err)
			return
		}
		if rows, err := res.RowsAffected(); err != nil {
			t.Errorf("cleanup finding %d rows: %v", findingID, err)
		} else if rows > 1 {
			t.Errorf("cleanup finding %d removed %d rows, want at most 1 (primary key)", findingID, rows)
		}
	})
}

// task32TrackRetestConversation 为一条复测会话登记完整清理链（会话活动 → 复测子行 →
// 会话本身），并在按主键清理之后按**同一批主键**核查零残留（R4）。
//
// 为什么必须单独登记：CreateFindingRetest 成功提交时**必然**连带写一条 conversations
// 与一条首轮 conversation_activities（见 finding_retests.go 的写入序列），而 finding 的
// cleanup 只覆盖 bindings/retests/findings 三张表。只登记 finding 的复测测试即使全绿，
// 也会每次在共享库里留下一整套会话/活动 —— 那是"M1 fixture 残留"，不是产品行为。
func task32TrackRetestConversation(t *testing.T, d *DB, conversationID int64) {
	t.Helper()
	if conversationID <= 0 {
		t.Fatalf("track retest conversation: conversation id %d is not a primary key", conversationID)
	}
	// 先注册 → 最后运行：核的是下面那条按主键清理真正跑完之后的库状态。
	t.Cleanup(func() {
		for _, check := range []struct{ table, query string }{
			{"conversation_activities", `SELECT COUNT(*) FROM conversation_activities WHERE conversation_id = ?`},
			{"finding_retests", `SELECT COUNT(*) FROM finding_retests WHERE conversation_id = ?`},
			{"conversations", `SELECT COUNT(*) FROM conversations WHERE id = ?`},
		} {
			if left := task32Count(t, d, check.query, conversationID); left != 0 {
				t.Errorf("residual %s rows for fixture conversation %d = %d, want 0", check.table, conversationID, left)
			}
		}
	})
	t.Cleanup(func() {
		for _, query := range []string{
			`DELETE FROM conversation_activities WHERE conversation_id = ?`,
			`DELETE FROM finding_retests WHERE conversation_id = ?`,
		} {
			task32MustExec(t, d, query, conversationID)
		}
		res, err := d.Exec(`DELETE FROM conversations WHERE id = ?`, conversationID)
		if err != nil {
			t.Errorf("cleanup conversation %d failed: %v", conversationID, err)
			return
		}
		if rows, err := res.RowsAffected(); err != nil {
			t.Errorf("cleanup conversation %d rows: %v", conversationID, err)
		} else if rows > 1 {
			t.Errorf("cleanup conversation %d removed %d rows, want at most 1 (primary key)", conversationID, rows)
		}
	})
}

// task32AssertNoFindingResidue 断言这几个 vulnclass 的 fixture 连子行一起清零。
// 它注册在按主键 cleanup 之前（所以最后运行），是 M1 要求的终态残留核查。
//
// 局限（R4）：这里按 vulnclass 过滤必须 JOIN findings，父行一旦先被删掉，孤儿子行
// 就统计不到。所以它只是"类别视图"，真正不依赖父行的孤儿核查由 task32TrackFinding /
// task32TrackRetestConversation 按**记录下来的主键**完成，两者互补，缺一不可。
func task32AssertNoFindingResidue(t *testing.T, d *DB, vulnClasses ...string) {
	t.Helper()
	if len(vulnClasses) == 0 {
		return
	}
	args := make([]any, len(vulnClasses))
	for i, vulnclass := range vulnClasses {
		args[i] = vulnclass
	}
	in := questionPlaceholders(len(args))
	if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings b JOIN findings f ON f.id = b.finding_id WHERE f.vulnclass IN (`+in+`)`, args...); left != 0 {
		t.Errorf("residual evidence bindings = %d, want 0", left)
	}
	if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_retests r JOIN findings f ON f.id = r.finding_id WHERE f.vulnclass IN (`+in+`)`, args...); left != 0 {
		t.Errorf("residual retest rows = %d, want 0", left)
	}
	if left := task32Count(t, d, `SELECT COUNT(*) FROM findings WHERE vulnclass IN (`+in+`)`, args...); left != 0 {
		t.Errorf("residual findings = %d, want 0", left)
	}
}

// task32EvidenceSnapshot 造一条内容寻址的流量证据快照（正文与 id 一致）。
func task32EvidenceSnapshot(sourceTrafficID string) TrafficEvidenceSnapshot {
	snapshot := TrafficEvidenceSnapshot{
		SourceTrafficID: sourceTrafficID,
		CapturedAt:      time.Now().Unix(),
		URL:             "https://p08-t32.example.test/api/vuln",
		Method:          "GET",
		Status:          200,
		ReqHash:         strings.Repeat("a", 64),
		RespHash:        strings.Repeat("b", 64),
		ReqLen:          16,
		RespLen:         2048,
	}
	snapshot.ID = TrafficSnapshotID(snapshot)
	return snapshot
}

// task32BindEvidence 给漏洞绑一条流量证据快照，返回 snapshot id。绑定成功后立即登记
// 按主键（snapshot id）的 cleanup，因此 seed 之后任何一步 Fatal 都不会漏清理。
//
// R4：清理之后还要按**记录下来的 snapshot 主键**核查终态 —— 它同样不 JOIN 父表，
// 绑定行成为孤儿时照样能被逮住。
func task32BindEvidence(t *testing.T, d *DB, findingID int64) string {
	t.Helper()
	snapshot := task32EvidenceSnapshot(fmt.Sprintf("p08-t32-%d", time.Now().UnixNano()))
	if err := d.WithEvidenceTx(context.Background(), func(tx *sql.Tx) error {
		if err := InsertEvidenceSnapshotTx(tx, snapshot); err != nil {
			return err
		}
		return AddFindingTrafficTx(tx, findingID, []PreparedTrafficEvidence{{
			Ref:      TrafficRef{TrafficID: snapshot.ID, Role: "proof", Note: "P08 Task 3.2"},
			Snapshot: snapshot,
		}})
	}); err != nil {
		t.Fatalf("bind evidence: %v", err)
	}
	// 先注册 → 最后运行：核的是紧随其后的按主键清理真正跑完之后的库状态。
	t.Cleanup(func() {
		if left := task32Count(t, d, `SELECT COUNT(*) FROM finding_traffic_bindings WHERE snapshot_id = ?`, snapshot.ID); left != 0 {
			t.Errorf("residual evidence bindings for snapshot %s = %d, want 0 (primary-key check)", snapshot.ID, left)
		}
		if left := task32Count(t, d, `SELECT COUNT(*) FROM traffic_evidence_snapshots WHERE id = ?`, snapshot.ID); left != 0 {
			t.Errorf("residual traffic evidence snapshot %s = %d, want 0", snapshot.ID, left)
		}
	})
	t.Cleanup(func() {
		task32MustExec(t, d, `DELETE FROM finding_traffic_bindings WHERE snapshot_id = ?`, snapshot.ID)
		task32MustExec(t, d, `DELETE FROM traffic_evidence_snapshots WHERE id = ?`, snapshot.ID)
	})
	return snapshot.ID
}

// task32SeedRetest 直接落一条复测记录（含其 conversation），返回 conversation id。
//
// 与产品代码同一严格口径：finding_retests 的每条 MEDIUMTEXT NOT NULL 列都必须显式
// 给值（TiDB 严格模式下 TEXT 没有 DEFAULT 可补），notes/error 也不能省。
// conversation 与复测子行在各自创建成功后立即按主键登记 cleanup。
func task32SeedRetest(t *testing.T, d *DB, findingID int64, status, verdict, summary, evidence string) int64 {
	t.Helper()
	res, err := d.Exec(`INSERT INTO conversations(agent_key, title) VALUES(?, ?)`, FindingRetestAgentKey, "P08 Task 3.2 复测")
	if err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	convID, err := res.LastInsertId()
	if err != nil || convID <= 0 {
		t.Fatalf("seed conversation LastInsertId: id=%d err=%v", convID, err)
	}
	t.Cleanup(func() {
		task32MustExec(t, d, `DELETE FROM finding_retests WHERE conversation_id = ?`, convID)
		if res, err := d.Exec(`DELETE FROM conversations WHERE id = ?`, convID); err != nil {
			t.Errorf("cleanup conversation %d failed: %v", convID, err)
		} else if rows, err := res.RowsAffected(); err == nil && rows > 1 {
			t.Errorf("cleanup conversation %d removed %d rows, want at most 1", convID, rows)
		}
	})
	if _, err := d.Exec(`INSERT INTO finding_retests(finding_id, conversation_id, status, notes, verdict, summary, evidence, error, snapshot)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, findingID, convID, status, "", verdict, summary, evidence, "", "{}"); err != nil {
		t.Fatalf("seed retest: %v", err)
	}
	return convID
}

// task32IsDeadlock 认出数据库返回的死锁中止（TiDB/MySQL 的 1213）。
func task32IsDeadlock(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "deadlock") || strings.Contains(text, "1213")
}

// TestFindings_FollowUpIntentVersusDeleteHasNoDeadlock 是真机侧的 I4 验收：follow-up
// 派生（曾经按 findings → tasks 逆序加锁）与任务级删除（tasks → findings）交错跑。
// 锁序正确时最坏只是一方排队；逆序则会形成等待环，由数据库强行中止其中一方。
func TestFindings_FollowUpIntentVersusDeleteHasNoDeadlock(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
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

	for round := range 3 {
		task, err := d.CreateTask(fmt.Sprintf("P08T32 锁序 %d", round), "goal", nil, 0, 0)
		if err != nil {
			t.Fatalf("CreateTask[%d]: %v", round, err)
		}
		t.Cleanup(func() {
			if err := d.DeleteTask(task.ID); err != nil {
				t.Errorf("delete task %d: %v", task.ID, err)
			}
		})
		store := d.Exploration(task.ExplorationID)
		nodeID, err := store.AddNode(KindFinding, map[string]any{"summary": "lock order"}, 5, "confirmed", "worker", nil)
		if err != nil {
			t.Fatalf("AddNode[%d]: %v", round, err)
		}
		findingID, err := d.AddFinding(task.ID, nodeID, fmt.Sprintf("P08T32 lock %d", round), "锁序样本", SeverityHigh, "摘要", "证据", "worker", nil)
		if err != nil || findingID <= 0 {
			t.Fatalf("AddFinding[%d]: id=%d err=%v", round, findingID, err)
		}
		task32TrackFinding(t, d, findingID)

		var wg sync.WaitGroup
		results := make(chan error, 8)
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 3 {
					if _, _, err := store.AddFindingFollowUpIntent(findingID, nodeID, "深入利用", Activity{
						Worker: "system", Kind: "text", Summary: "锁序交错",
					}); err != nil && !errors.Is(err, ErrFindingOriginUnavailable) {
						results <- err
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.DeleteFindingsByTask(task.ID); err != nil {
				results <- err
			}
		}()
		wg.Wait()
		close(results)
		for failure := range results {
			if task32IsDeadlock(failure) {
				t.Errorf("round %d: follow-up and task delete deadlocked (reverse lock order): %v", round, failure)
				continue
			}
			t.Errorf("round %d: interleaved follow-up/delete failed: %v", round, failure)
		}
	}
}

// TestFindings_FindingStats_EmptyBaselineIsAllZero 在**独占的干净库**上钉死空表契约：
// 六个统计字段必须逐一是数值 0（COUNT(CASE WHEN … THEN 1 END) 的形状保证），分组视图
// 同理。共享业务库绝不允许被清空来制造空表，所以这里要求单独指一个专用库。
func TestFindings_FindingStats_EmptyBaselineIsAllZero(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("ARTEX_TIDB_EMPTY_DSN"))
	if dsn == "" {
		t.Skip("ARTEX_TIDB_EMPTY_DSN not set, skipping dedicated empty-database test")
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
	// 先证明这确实是独占空库；不是就判红，绝不去清空别人的数据。
	if rows := task32Count(t, d, `SELECT COUNT(*) FROM findings`); rows != 0 {
		t.Fatalf("ARTEX_TIDB_EMPTY_DSN must point at a dedicated baseline database whose findings table is empty, found %d rows", rows)
	}

	stats, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats failed: %v", err)
	}
	if stats.Total != 0 || stats.Pending != 0 || stats.Critical != 0 || stats.High != 0 || stats.Medium != 0 || stats.Low != 0 {
		t.Errorf("empty-table stats = %+v, want every one of total/pending/critical/high/medium/low to be 0", stats)
	}
	if stats.VulnClasses == nil || stats.Tasks == nil || len(stats.VulnClasses) != 0 || len(stats.Tasks) != 0 {
		t.Errorf("empty-table slices = %+v, want non-nil empty slices", stats)
	}
	groups, groupTotal, findingTotal, err := d.ListFindingGroups(FindingFilter{}, 1, 10)
	if err != nil {
		t.Fatalf("ListFindingGroups failed: %v", err)
	}
	if groupTotal != 0 || findingTotal != 0 || len(groups) != 0 {
		t.Errorf("empty-table groups = %+v groups=%d findings=%d, want none", groups, groupTotal, findingTotal)
	}
}

// TestFindings_QuestionPlaceholdersShape 钉死动态占位符的形状：长度随输入变化，
// 值一律留给绑定参数，空输入绝不能留下悬空的逗号。
func TestFindings_QuestionPlaceholdersShape(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want string
	}{
		{0, ""},
		{-1, ""},
		{1, "?"},
		{3, "?, ?, ?"},
	} {
		if got := questionPlaceholders(tc.n); got != tc.want {
			t.Errorf("questionPlaceholders(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

// TestFindings_JsonLiteralIsJSONDocument 钉死 `CAST(? AS JSON)` 的入参形状：
// 必须是合法 JSON 文档（字符串自带引号），否则 JSON_SET 会把值写坏。
func TestFindings_JsonLiteralIsJSONDocument(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`high`, `"high"`},
		{`说"引号"`, `"说\"引号\""`},
		{``, `""`},
	} {
		if got := jsonLiteral(tc.in); got != tc.want {
			t.Errorf("jsonLiteral(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// TestFindings_FilterPlaceholderCountMatchesArgs 是纯逻辑门禁：FindingFilter 生成的
// WHERE 里 `?` 的个数必须与参数个数逐一相等（PG 的 $n 自举一旦被改错，这里立刻红，
// 不必等真机报 "sql: expected N arguments"）。
func TestFindings_FilterPlaceholderCountMatchesArgs(t *testing.T) {
	filters := []struct {
		name   string
		filter FindingFilter
	}{
		{"empty", FindingFilter{}},
		{"severity", FindingFilter{Severity: SeverityHigh}},
		{"status+class", FindingFilter{Status: FindingPending, VulnClass: "XSS"}},
		{"unassigned task", FindingFilter{TaskID: FindingUnassignedTask}},
		{"numeric task", FindingFilter{TaskID: "42"}},
		{"query", FindingFilter{Query: `  100%_\ 关键词  `}},
		{"asset none", FindingFilter{assetNone: true}},
		{"asset miss", FindingFilter{assetMiss: true}},
		{"asset ids", FindingFilter{assetIDs: []int64{7, 8, 9}}},
		{"everything", FindingFilter{Severity: SeverityCritical, Status: FindingFixed, VulnClass: "SQLi", TaskID: "9", Query: "x", assetIDs: []int64{1, 2}}},
	}
	for _, tc := range filters {
		where, args := tc.filter.where()
		if got, want := strings.Count(where, "?"), len(args); got != want {
			t.Errorf("%s: %d `?` in WHERE but %d args: %s", tc.name, got, want, where)
		}
		if strings.Contains(where, "$") {
			t.Errorf("%s: WHERE still carries a positional marker: %s", tc.name, where)
		}
		if strings.Contains(where, "ILIKE") || strings.Contains(where, "jsonb") {
			t.Errorf("%s: WHERE still carries PG dialect: %s", tc.name, where)
		}
	}

	// 关键词必须被转义后整段绑定（LIKE 元字符不能作用于用户输入）。
	where, args := FindingFilter{Query: `50%\_x\`}.where()
	if strings.Contains(where, "50%") {
		t.Errorf("query text must never be interpolated into SQL: %s", where)
	}
	if len(args) != 5 {
		t.Fatalf("query filter: want 5 bound patterns, got %d", len(args))
	}
	pattern, ok := args[0].(string)
	if !ok || pattern != `%50\%\\\_x\\%` {
		t.Errorf("LIKE wildcards were not escaped, got %q", pattern)
	}
	for i, arg := range args {
		if arg != pattern {
			t.Errorf("arg %d = %v, want the same escaped pattern as arg 0", i, arg)
		}
	}
}

// TestFindings_ArchiveTimestampParsesArchivedShapes 覆盖归档时间字段的还原：
// 归档由 encoding/json 写出（RFC3339），老归档可能是 SQL 字面量；解不出来时必须
// 交给列默认值，而不是塞一个非法值进去。
func TestFindings_ArchiveTimestampParsesArchivedShapes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		ok    bool
	}{
		{"rfc3339 nano", "2026-10-18T02:10:00.123456789Z", true},
		{"rfc3339", "2026-10-18T02:10:00Z", true},
		{"sql literal", "2026-10-18 02:10:00.123", true},
		{"sql literal second", "2026-10-18 02:10:00", true},
		{"empty", "", false},
		{"blank", "   ", false},
		{"number", json.Number("1766023800"), false},
		{"nil", nil, false},
		{"garbage", "not-a-time", false},
	} {
		at, ok := archiveTimestamp(tc.value)
		if ok != tc.ok {
			t.Errorf("%s: ok = %v, want %v", tc.name, ok, tc.ok)
		}
		if tc.ok && at.IsZero() {
			t.Errorf("%s: parsed value is zero time", tc.name)
		}
	}
}

// TestFindings_SnapshotValueKeepsJSONDocuments 覆盖复测快照的值归一（I6）：只有
// schema 声明为 JSON 载荷（TJSON）的列才解回结构，普通文本列一律保持字符串；解析
// JSON 载荷必须保住 64 位整数精度。类型判据来自声明，绝不来自文本外观。
func TestFindings_SnapshotValueKeepsJSONDocuments(t *testing.T) {
	// JSON 载荷列：对象/数组解回结构（否则快照里多一层转义字符串）。
	if got := snapshotValue([]byte(`[1,2]`), true); fmt.Sprint(got) != "[1 2]" {
		t.Errorf("json array column should decode into a slice, got %#v", got)
	}
	if got := snapshotValue([]byte(`{"k":"v"}`), true); fmt.Sprint(got) != "map[k:v]" {
		t.Errorf("json object column should decode into a map, got %#v", got)
	}
	// 嵌套结构 + > 2^53 的资产 id：默认 interface{} 解码会把数字过一遍 float64，
	// 9007199254740993 会被舍成 9007199254740992；UseNumber 必须逐字保住。
	nested := `{"asset_ids":[9007199254740993,{"inner":[9007199254740993,1.5]}],"n":0}`
	got := snapshotValue([]byte(nested), true)
	if _, ok := got.(map[string]any); !ok {
		t.Fatalf("nested json should stay a map, got %#v", got)
	}
	if raw, err := json.Marshal(got); err != nil || string(raw) != nested {
		t.Errorf("64-bit ids must survive decode/encode: got %s err=%v, want %s", raw, err, nested)
	}
	// 普通 TEXT 列：正文恰好是一整篇 JSON 也保持字符串文本（旧实现按外观改类型）。
	for _, text := range []string{`{"k":"v"}`, `[1,2]`, `plain text`, `123`} {
		if got := snapshotValue([]byte(text), false); got != text {
			t.Errorf("ordinary text column changed type: got %#v, want the string %q", got, text)
		}
	}
	// JSON 载荷列里的脏数据（解不出来）原样带出，既不吞值也不改型。
	if got := snapshotValue([]byte(`{broken`), true); got != "{broken" {
		t.Errorf("unparsable json payload should stay verbatim, got %#v", got)
	}
	// RR5：合法**前缀** + 尾部脏数据。单次 Decode 只读第一个合法 JSON 值就宣告成功，
	// 尾部被静默跳过，下一轮 Marshal 时脏尾巴凭空消失 —— 那不是"原样带出"，而是
	// 悄悄改写脏数据（TJSON 物理上就是 MEDIUMTEXT，库端从不校验整段 JSON）。
	// 判据：首个值之后必须立刻见到 io.EOF，否则整列按原文返回。
	for _, dirty := range []string{
		`[{"id":9007199254740993}] trailing-corruption`,
		`{"asset_ids":[9007199254740993]}garbage`,
		`[] {}`,
		`{"a":1} {"b":2}`,
	} {
		if got := snapshotValue([]byte(dirty), true); got != dirty {
			t.Errorf("json payload with trailing garbage must stay verbatim, got %#v, want %q", got, dirty)
		}
	}
	// 合法的尾部空白不是脏数据：必须照常解回结构，不能因 RR5 的严格校验退化成套字符串。
	for _, clean := range []string{`[1,2] `, " [1,2]\t", "\n[1,2]\r\n"} {
		if got := snapshotValue([]byte(clean), true); fmt.Sprint(got) != "[1 2]" {
			t.Errorf("trailing whitespace is not corruption: got %#v for %q", got, clean)
		}
	}
	if got := snapshotValue(nil, true); got != nil {
		t.Errorf("nil should stay nil, got %#v", got)
	}
	if got := snapshotValue(42, true); got != 42 {
		t.Errorf("non-bytes values should pass through, got %#v", got)
	}
}

// TestFindings_JSONPayloadColumnsComeFromCatalog 钉死 I6 的类型判据：JSON 载荷列
// 必须来自基线 schema 目录（TJSON），不是文本猜测，也不是 driver 的列类型 ——
// TJSON 在 DDL 层渲染成 MEDIUMTEXT，driver 眼里它和 evidence 没有区别。
func TestFindings_JSONPayloadColumnsComeFromCatalog(t *testing.T) {
	for _, tc := range []struct {
		table, column string
		want          bool
	}{
		{"findings", "asset_ids", true},
		{"findings", "report", false},
		{"findings", "evidence", false},
		{"finding_retests", "snapshot", true},
		{"finding_retests", "summary", false},
		{"finding_retests", "evidence", false},
		{"finding_retests", "notes", false},
		{"no_such_table", "asset_ids", false},
		{"findings", "no_such_column", false},
	} {
		if got := jsonPayloadColumn(tc.table, tc.column); got != tc.want {
			t.Errorf("jsonPayloadColumn(%q, %q) = %v, want %v", tc.table, tc.column, got, tc.want)
		}
	}
}
