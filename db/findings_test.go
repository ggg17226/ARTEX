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
	"testing"
	"time"
)

// TestDeleteFinding verifies删除漏洞 removes both the findings row and its
// originating exploration node (kind='finding').
func TestDeleteFinding(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("postgres unavailable (%v) — skipping", err)
	}
	defer d.Close()

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
	d.QueryRow(`SELECT count(*) FROM exploration_nodes WHERE id=$1`, nodeID).Scan(&cnt)
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("postgres unavailable (%v) — skipping", err)
	}
	defer d.Close()

	const vc = "__test_vc_pagination__"
	// clean any leftovers from a prior aborted run, and clean up on exit
	cleanup := func() { _, _ = d.Exec(`DELETE FROM findings WHERE vulnclass=$1`, vc) }
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
	if _, err := d.Exec(`UPDATE findings SET report=$1 WHERE id=$2`, "# 报告\n正文", ids[0]); err != nil {
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
	if _, err := d.Exec(`UPDATE findings SET name=$1 WHERE id=$2`, "literal %_ marker", ids[1]); err != nil {
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("postgres unavailable (%v) — skipping", err)
	}
	defer d.Close()

	vc := fmt.Sprintf("__test_finding_stable_page_%d__", time.Now().UnixNano())
	defer d.Exec(`DELETE FROM findings WHERE vulnclass=$1`, vc)
	ids := make([]int64, 0, 7)
	for i := 0; i < 7; i++ {
		id, addErr := d.AddFinding(0, 0, vc, "", SeverityHigh, fmt.Sprintf("finding %d", i), "", "test", nil)
		if addErr != nil {
			t.Fatalf("AddFinding[%d]: %v", i, addErr)
		}
		ids = append(ids, id)
	}
	sharedCreatedAt := time.Date(2026, time.August, 21, 8, 30, 0, 0, time.UTC)
	if _, err := d.Exec(`UPDATE findings SET created_at=$1 WHERE vulnclass=$2`, sharedCreatedAt, vc); err != nil {
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("postgres unavailable (%v) — skipping", err)
	}
	defer d.Close()

	vc := fmt.Sprintf("__test_finding_groups_%d__", time.Now().UnixNano())
	cleanupFindings := func() { _, _ = d.Exec(`DELETE FROM findings WHERE vulnclass=$1`, vc) }
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("postgres unavailable (%v) — skipping", err)
	}
	defer d.Close()

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
	defer d.Exec(`DELETE FROM assets WHERE id=$1`, assetID)

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
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_anchors WHERE node_id=$1 AND asset_id=$2`, intentID, assetID).Scan(&anchorCount); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_edges
		WHERE exploration_id=$1 AND src_id=$2 AND rel=$3 AND dst_id=$4`,
		task.ExplorationID, findingNodeID, RelDerivedFrom, intentID).Scan(&edgeCount); err != nil {
		t.Fatal(err)
	}
	if anchorCount != 1 || edgeCount != 1 {
		t.Fatalf("follow-up lineage: anchors=%d edges=%d", anchorCount, edgeCount)
	}
	var persistedAuditCount int
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity
		WHERE id=$1 AND exploration_id=$2 AND node_id=$3 AND worker='system' AND summary=$4`,
		audit.ID, task.ExplorationID, intentID, expectedAuditSummary).Scan(&persistedAuditCount); err != nil {
		t.Fatal(err)
	}
	if persistedAuditCount != 1 {
		t.Fatalf("atomic audit count=%d, want 1", persistedAuditCount)
	}

	var nodesBefore, activitiesBefore int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=$1`, task.ExplorationID).Scan(&nodesBefore); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity WHERE exploration_id=$1`, task.ExplorationID).Scan(&activitiesBefore); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddFindingFollowUpIntent(findingID, findingNodeID, "must roll back", Activity{
		Worker: "system", Kind: "text", Summary: "invalid audit", Metadata: json.RawMessage(`{`),
	}); err == nil {
		t.Fatal("invalid activity metadata should fail the transaction")
	}
	var nodesAfter, activitiesAfter int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=$1`, task.ExplorationID).Scan(&nodesAfter); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM activity WHERE exploration_id=$1`, task.ExplorationID).Scan(&activitiesAfter); err != nil {
		t.Fatal(err)
	}
	if nodesAfter != nodesBefore || activitiesAfter != activitiesBefore {
		t.Fatalf("activity failure did not roll back graph: nodes %d->%d activities %d->%d",
			nodesBefore, nodesAfter, activitiesBefore, activitiesAfter)
	}

	if _, err := d.Exec(`DELETE FROM exploration_nodes WHERE id=$1`, findingNodeID); err != nil {
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
	if !strings.Contains(body, "status IN ('pending','running')") {
		t.Errorf("DeleteFinding must seal the finding's unfinished retests (status IN ('pending','running'))")
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
	cleanup := func() { _, _ = d.Exec(`DELETE FROM findings WHERE vulnclass = ?`, vc) }
	cleanup()
	t.Cleanup(cleanup)

	before, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats (before) failed: %v", err)
	}

	seed := []struct{ severity, status, summary string }{
		{SeverityCritical, FindingPending, "P08T32 critical case"},
		{SeverityHigh, FindingInProgress, "P08T32 high case"},
		{SeverityLow, FindingResolved, "P08T32 low case"},
	}
	ids := make([]int64, 0, len(seed))
	for _, s := range seed {
		id, err := d.AddFinding(0, 0, vc, "名称-"+s.severity, s.severity, s.summary, "poc", "worker", []int64{900001, 900002})
		if err != nil || id <= 0 {
			t.Fatalf("AddFinding(%s): id=%d err=%v", s.severity, id, err)
		}
		ids = append(ids, id)
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
	}

	after, err := d.FindingStats()
	if err != nil {
		t.Fatalf("FindingStats (after) failed: %v", err)
	}
	if got, want := after.Total-before.Total, len(seed); got != want {
		t.Errorf("stats total delta = %d, want %d", got, want)
	}
	if got, want := after.Critical-before.Critical, 1; got != want {
		t.Errorf("stats critical delta = %d, want %d", got, want)
	}
	if got, want := after.High-before.High, 1; got != want {
		t.Errorf("stats high delta = %d, want %d", got, want)
	}
	if got, want := after.Low-before.Low, 1; got != want {
		t.Errorf("stats low delta = %d, want %d", got, want)
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

	// 关键词模糊筛选（LIKE 语义在 TiDB 默认排序规则下即大小写不敏感）
	matched, matchedTotal, err := d.ListFindingsPage(FindingFilter{VulnClass: vc, Query: "high case"}, 1, 10)
	if err != nil || matchedTotal != 1 || len(matched) != 1 {
		t.Fatalf("query filter: len=%d total=%d err=%v", len(matched), matchedTotal, err)
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

// TestFindings_DeleteFindingUnbindsEvidenceAndRetests 覆盖删除时的显式解除：
// finding_traffic_bindings 必须整行删除（finding_id 是 NOT NULL 列，没有"置空"
// 这条退路），仍未收尾的复测必须被封成终态（否则 active_finding_id 生成列会让
// 前端永久显示"复测中"），而承载复测的 conversation 仍保留（历史不随漏洞删除）。
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

	snapshotID := task32BindEvidence(t, d, findingID)
	convID := task32SeedRetest(t, d, findingID, "running", "", "复测摘要", "复测证据")
	t.Cleanup(func() {
		_, _ = d.Exec(`DELETE FROM finding_traffic_bindings WHERE snapshot_id = ?`, snapshotID)
		_, _ = d.Exec(`DELETE FROM traffic_evidence_snapshots WHERE id = ?`, snapshotID)
		_, _ = d.Exec(`DELETE FROM findings WHERE id = ?`, findingID)
	})

	if n, err := d.DeleteFinding(findingID); err != nil || n != 1 {
		t.Fatalf("DeleteFinding: n=%d err=%v", n, err)
	}

	var bindings int
	if err := d.QueryRow(`SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID).Scan(&bindings); err != nil {
		t.Fatalf("count bindings failed: %v", err)
	}
	if bindings != 0 {
		t.Errorf("evidence bindings after delete = %d, want 0", bindings)
	}

	var sealed, unfinished int
	if err := d.QueryRow(`SELECT COUNT(*) FROM finding_retests WHERE conversation_id = ?`, convID).Scan(&sealed); err != nil {
		t.Fatalf("count sealed retests failed: %v", err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM finding_retests WHERE conversation_id = ? AND status IN ('pending','running')`, convID).Scan(&unfinished); err != nil {
		t.Fatalf("count unfinished retests failed: %v", err)
	}
	if sealed != 1 || unfinished != 0 {
		t.Errorf("retest linkage after delete: rows=%d unfinished=%d, want 1/0", sealed, unfinished)
	}
	active, err := d.ListActiveFindingRetests(context.Background())
	if err != nil {
		t.Fatalf("ListActiveFindingRetests failed: %v", err)
	}
	for _, item := range active {
		if item.ConversationID == convID || item.FindingID == findingID {
			t.Errorf("deleted finding still owns an active retest: %+v", item)
		}
	}

	var conversations int
	if err := d.QueryRow(`SELECT COUNT(*) FROM conversations WHERE id = ?`, convID).Scan(&conversations); err != nil {
		t.Fatalf("count conversations failed: %v", err)
	}
	if conversations != 1 {
		t.Errorf("retest conversation must survive finding deletion, got %d", conversations)
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
	t.Cleanup(func() {
		_, _ = d.Exec(`DELETE FROM findings WHERE id = ?`, findingID)
	})

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

// task32BindEvidence 给漏洞绑一条流量证据快照，返回 snapshot id。
func task32BindEvidence(t *testing.T, d *DB, findingID int64) string {
	t.Helper()
	snapshot := TrafficEvidenceSnapshot{
		SourceTrafficID: fmt.Sprintf("p08-t32-%d", time.Now().UnixNano()),
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
	return snapshot.ID
}

// task32SeedRetest 直接落一条复测记录（含其 conversation），返回 conversation id。
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
		_, _ = d.Exec(`DELETE FROM finding_retests WHERE conversation_id = ?`, convID)
		_, _ = d.Exec(`DELETE FROM conversations WHERE id = ?`, convID)
	})
	if _, err := d.Exec(`INSERT INTO finding_retests(finding_id, conversation_id, status, verdict, summary, evidence, snapshot)
VALUES(?, ?, ?, ?, ?, ?, CAST(? AS JSON))`, findingID, convID, status, verdict, summary, evidence, "{}"); err != nil {
		t.Fatalf("seed retest: %v", err)
	}
	return convID
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

// TestFindings_SnapshotValueKeepsJSONDocuments 覆盖复测快照的值归一：JSON 列要解回
// 结构（否则快照里是转义字符串），普通文本与 NULL 原样保留。
func TestFindings_SnapshotValueKeepsJSONDocuments(t *testing.T) {
	if got := snapshotValue([]byte(`[1,2]`)); !strings.Contains(fmt.Sprint(got), "1 2") && fmt.Sprint(got) != "[1 2]" {
		t.Errorf("json array column should decode into a slice, got %#v", got)
	}
	if got := snapshotValue([]byte(`{"k":"v"}`)); fmt.Sprint(got) != "map[k:v]" {
		t.Errorf("json object column should decode into a map, got %#v", got)
	}
	if got := snapshotValue([]byte(`plain text`)); got != "plain text" {
		t.Errorf("non-json bytes should stay a string, got %#v", got)
	}
	if got := snapshotValue(nil); got != nil {
		t.Errorf("nil should stay nil, got %#v", got)
	}
	if got := snapshotValue(42); got != 42 {
		t.Errorf("non-bytes values should pass through, got %#v", got)
	}
}
