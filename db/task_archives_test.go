package db

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

// =====================================================================
// 离线用例：不连库也必须守住 Manifest 双白名单（M06）
// =====================================================================

type archiveJSONTestRows struct {
	values [][]byte
	next   int
}

func (r *archiveJSONTestRows) Next() bool {
	if r.next >= len(r.values) {
		return false
	}
	r.next++
	return true
}

func (r *archiveJSONTestRows) Scan(dest ...any) error {
	if len(dest) != 1 || r.next == 0 || r.next > len(r.values) {
		return fmt.Errorf("invalid archive test row scan")
	}
	target, ok := dest[0].(*[]byte)
	if !ok {
		return fmt.Errorf("archive test row destination is %T", dest[0])
	}
	*target = append((*target)[:0], r.values[r.next-1]...)
	return nil
}

func (r *archiveJSONTestRows) Err() error { return nil }

func TestQueryArchiveRowsStreamsJSON(t *testing.T) {
	payload := strings.Repeat("large request/response payload ", 64*1024)
	values := make([][]byte, 2)
	var err error
	values[0], err = json.Marshal(map[string]any{"id": int64(1), "body": payload})
	if err != nil {
		t.Fatal(err)
	}
	values[1], err = json.Marshal(map[string]any{"id": int64(2), "body": "quoted: \"value\"\nline"})
	if err != nil {
		t.Fatal(err)
	}
	raw, count, err := encodeArchiveRows(&archiveJSONTestRows{values: values})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("row count=%d, want 2", count)
	}
	var rows []struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("decoded row count=%d, want 2", len(rows))
	}
	if rows[0].ID != 1 || rows[0].Body != payload || rows[1].ID != 2 || rows[1].Body != "quoted: \"value\"\nline" {
		t.Fatal("streamed rows were reordered or truncated")
	}
}

func TestWriteArchiveRowsProducesJSONSequence(t *testing.T) {
	values := [][]byte{
		json.RawMessage(`{"id":1,"body":"first"}`),
		json.RawMessage(`{"id":2,"body":"second\\nline"}`),
	}
	var output bytes.Buffer
	count, err := writeArchiveRows(&archiveJSONTestRows{values: values}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("streamed row count=%d, want 2", count)
	}
	decoder := json.NewDecoder(&output)
	for wantID := int64(1); wantID <= 2; wantID++ {
		var row struct {
			ID int64 `json:"id"`
		}
		if err := decoder.Decode(&row); err != nil {
			t.Fatal(err)
		}
		if row.ID != wantID {
			t.Fatalf("streamed row id=%d, want %d", row.ID, wantID)
		}
	}
}

func TestTaskArchiveFormatCompatibility(t *testing.T) {
	for _, version := range []int{TaskArchiveLegacyFormatVersion, TaskArchiveFormatVersion} {
		if !IsTaskArchiveFormatSupported(version) {
			t.Fatalf("archive format %d should be supported", version)
		}
	}
	for _, version := range []int{0, TaskArchiveFormatVersion + 1} {
		if IsTaskArchiveFormatSupported(version) {
			t.Fatalf("archive format %d should be rejected", version)
		}
	}
	invalidSnapshots := []*TaskArchiveSnapshot{
		{FormatVersion: TaskArchiveLegacyFormatVersion, StreamedTables: map[string]string{"llm_records": TaskArchiveLLMRecordsPath}},
		{FormatVersion: TaskArchiveFormatVersion, StreamedTables: map[string]string{"unknown": "database/unknown.ndjson"}},
	}
	for _, snapshot := range invalidSnapshots {
		if _, err := (&DB{}).RestoreTaskArchive(1, snapshot, 0); !errors.Is(err, ErrTaskArchiveFormatMismatch) {
			t.Fatalf("invalid streamed table metadata returned %v", err)
		}
	}
}

// archivePlanWhitelists 是 plan Task 4.2 M06 规定的 7 张表严格列白名单：
// 物理主键 row_id 与 STORED 生成列一律不得出现在冷包里。
var archivePlanWhitelists = map[string][]string{
	"task_relations":             {"task_id", "source_task_id", "created_at"},
	"task_asset_links":           {"task_id", "asset_id", "source", "source_summary", "source_node_id", "created_at", "updated_at"},
	"task_llm_profiles":          {"task_id", "profile_id", "position", "status", "last_error", "exhausted_at", "created_at", "updated_at"},
	"exploration_edges":          {"exploration_id", "src_id", "dst_id", "rel", "created_at"},
	"exploration_anchors":        {"node_id", "asset_id"},
	"side_question_sessions":     {"session_key", "conversation_id", "task_id", "exploration_id", "intent_id", "run_id", "version", "snapshot", "generation", "memory"},
	"traffic_evidence_snapshots": {"id", "source_traffic_id", "captured_at", "url", "method", "status", "content_type", "req_head", "resp_head", "req_hash", "resp_hash", "req_len", "resp_len", "unreferenced_at", "created_at"},
}

// archiveEngineWrittenTables 是必须经 archiveImportColumns 写入白名单落回的表集合。
// 新增走通用引擎的表时，这里与 archiveImportAllowList 必须一起更新（否则本用例判红）。
var archiveEngineWrittenTables = []string{
	"assets", "exploration_nodes", "exploration_edges", "exploration_anchors", "task_constraints",
	"activity", "task_asset_links", "findings", "llm_records", "llm_usage", "skill_usage",
	"tool_usage", "side_question_sessions", "side_question_requests", "task_scope",
	"task_llm_profiles", "intercept_pending",
}

// TestTaskArchive_ManifestWhitelistMatchesPlan 逐表比对 Catalog 推导出的导出白名单
// 与 plan 规定的严格白名单，并且确认 row_id / 生成列 / canonical 派生列一处不剩。
// 本用例不连库：没有 TiDB 也必须真实执行并 PASS。
func TestTaskArchive_ManifestWhitelistMatchesPlan(t *testing.T) {
	for table, want := range archivePlanWhitelists {
		got, err := ArchiveTableWhitelist(table)
		if err != nil {
			t.Fatalf("%s whitelist: %v", table, err)
		}
		if !sameColumnSet(got, want) {
			t.Fatalf("%s whitelist mismatch:\n got=%v\nwant=%v", table, got, want)
		}
		for _, column := range got {
			if column == "row_id" {
				t.Fatalf("%s whitelist still exports the hidden physical key", table)
			}
			definition, err := archiveColumnOf(table, column)
			if err != nil {
				t.Fatalf("%s.%s: %v", table, column, err)
			}
			if definition.Generated != "" {
				t.Fatalf("%s.%s is a generated column and must never be exported", table, column)
			}
		}
	}

	// side_question_requests：生成列 running_session_key 必须在导出与导入两侧都缺席。
	requests, err := ArchiveTableWhitelist("side_question_requests")
	if err != nil {
		t.Fatal(err)
	}
	if containsColumn(requests, "running_session_key") {
		t.Fatalf("side_question_requests whitelist contains the generated column: %v", requests)
	}
	// identity/identity_hash 是 canonical 派生列，导出侧剔除、导入侧重算（偏差记录在报告里）。
	if containsColumn(requests, "identity") || containsColumn(requests, "identity_hash") {
		t.Fatalf("side_question_requests whitelist still carries derived columns: %v", requests)
	}
	for _, column := range []string{"id", "session_key", "client_id", "question", "status", "model"} {
		if !containsColumn(requests, column) {
			t.Fatalf("side_question_requests whitelist lost the logical column %s: %v", column, requests)
		}
	}

	// 导入侧额外摘掉自增代理键 ordinal，物理顺序号必须由 TiDB 重新分配。
	importRequests, err := archiveImportColumns("side_question_requests")
	if err != nil {
		t.Fatal(err)
	}
	if containsColumn(importRequests, "ordinal") {
		t.Fatalf("side_question_requests import whitelist reuses the surrogate ordinal: %v", importRequests)
	}
	if containsColumn(importRequests, "running_session_key") {
		t.Fatalf("side_question_requests import whitelist writes the generated column: %v", importRequests)
	}

	// hash 表的 canonical 派生列一律不进白名单（导入时按当前语义列重算）。
	importScope, err := archiveImportColumns("task_scope")
	if err != nil {
		t.Fatal(err)
	}
	if containsColumn(importScope, "identity") || containsColumn(importScope, "identity_hash") {
		t.Fatalf("task_scope import whitelist writes derived columns: %v", importScope)
	}
	scope, err := ArchiveTableWhitelist("task_scope")
	if err != nil {
		t.Fatal(err)
	}
	if containsColumn(scope, "identity") || containsColumn(scope, "identity_hash") {
		t.Fatalf("task_scope export whitelist still carries derived columns: %v", scope)
	}
	assets, err := ArchiveTableWhitelist("assets")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range assets {
		if strings.HasPrefix(column, "identity_") || strings.HasPrefix(column, "hash_") {
			t.Fatalf("assets whitelist still exports canonical branch column %s", column)
		}
	}
	importAssets, err := archiveImportColumns("assets")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range importAssets {
		if strings.HasPrefix(column, "identity_") || strings.HasPrefix(column, "hash_") {
			t.Fatalf("assets import whitelist writes canonical branch column %s", column)
		}
	}

	// 核心图实体的 id 是被引用方，必须原样写回；叶子账目行的 id 一律交给 TiDB 重新分配。
	for _, table := range []string{"exploration_nodes", "findings"} {
		columns, err := archiveImportColumns(table)
		if err != nil {
			t.Fatalf("%s import whitelist: %v", table, err)
		}
		if !containsColumn(columns, "id") {
			t.Fatalf("%s import whitelist dropped the referenced primary key id: %v", table, columns)
		}
	}
	for table := range archiveImportSurrogates {
		columns, err := archiveImportColumns(table)
		if err != nil {
			t.Fatalf("%s import whitelist: %v", table, err)
		}
		if containsColumn(columns, archiveImportSurrogates[table]) {
			t.Fatalf("%s import whitelist still writes the surrogate %s: %v",
				table, archiveImportSurrogates[table], columns)
		}
	}

	// 所有走通用引擎的表都必须有可写入白名单，且其中没有隐藏主键与生成列。
	for _, table := range archiveEngineWrittenTables {
		columns, err := archiveImportColumns(table)
		if err != nil {
			t.Fatalf("%s import whitelist: %v", table, err)
		}
		if len(columns) == 0 {
			t.Fatalf("%s import whitelist is empty", table)
		}
		for _, column := range columns {
			definition, err := archiveColumnOf(table, column)
			if err != nil {
				t.Fatalf("%s.%s: %v", table, column, err)
			}
			if definition.Generated != "" {
				t.Fatalf("%s import whitelist writes generated column %s", table, column)
			}
			item, ok := archiveCatalogTable(table)
			if !ok {
				t.Fatalf("%s is absent from the real catalog", table)
			}
			if item.PKColumn == "row_id" && column == "row_id" {
				t.Fatalf("%s import whitelist reuses the hidden physical key", table)
			}
		}
	}
}

// TestTaskArchive_QueriesUseTiDBGrammar 静态守住"归档导出/恢复不再含 PG 方言"。
// 只扫描源码里的字符串字面量（SQL 都在字面量里），注释里对旧方言的说明不算违规。
func TestTaskArchive_QueriesUseTiDBGrammar(t *testing.T) {
	for _, file := range []string{"task_archives.go", "task_archives_restore.go"} {
		literals := goStringLiterals(t, file)
		for _, marker := range []string{
			"row_to_json", "json_agg", "json_populate_record", "make_interval", "array_remove",
			"array_append", "jsonb_array_elements", "ANY($", "::", "RETURNING", "ILIKE",
			"SKIP LOCKED", "LOCK TABLE", " now(", "$1", "$2", "ON CONFLICT", "INSERT IGNORE",
		} {
			for _, literal := range literals {
				if strings.Contains(literal, marker) {
					t.Errorf("%s still contains PostgreSQL marker %q in: %s", file, marker, trimForReport(literal))
				}
			}
		}
		if strings.Contains(strings.Join(literals, "\n"), "?") == false {
			t.Errorf("%s has no TiDB placeholder at all", file)
		}
	}
}

// goStringLiterals 解析源文件并返回全部字符串字面量的取值（反引号内容原样保留）。
func goStringLiterals(t *testing.T, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		if strings.HasPrefix(literal.Value, "`") {
			out = append(out, strings.TrimPrefix(strings.TrimSuffix(literal.Value, "`"), "`"))
			return true
		}
		decoded, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatalf("unquote %s: %v", literal.Value, err)
		}
		out = append(out, decoded)
		return true
	})
	if len(out) == 0 {
		t.Fatalf("%s has no string literals", path)
	}
	return out
}

func trimForReport(literal string) string {
	oneLine := strings.Join(strings.Fields(literal), " ")
	if len(oneLine) > 160 {
		return oneLine[:160] + "…"
	}
	return oneLine
}

// =====================================================================
// 真机用例：7 张表 Manifest 严格白名单 + 全量恢复保真 + 冲突 fail-closed
// =====================================================================

// TestTaskArchive_SevenTableManifestWhitelistStrict 在 7 张表里各写入真实数据，导出
// 后逐行断言列集合与 plan 白名单完全相等（漏列、多列、混进 row_id 都要判红）。
func TestTaskArchive_SevenTableManifestWhitelistStrict(t *testing.T) {
	fixture := newArchiveSevenTableFixture(t)
	snapshot, _ := archiveAndCompactFixture(t, fixture)
	for table, want := range archivePlanWhitelists {
		if snapshot.DataCounts[table] < 1 {
			t.Fatalf("%s fixture produced %d rows, manifest must be non-empty", table, snapshot.DataCounts[table])
		}
		rows, err := archiveDecodeRawRows(snapshot.Tables[table])
		if err != nil {
			t.Fatalf("%s manifest: %v", table, err)
		}
		if int64(len(rows)) != snapshot.DataCounts[table] {
			t.Fatalf("%s manifest rows=%d, manifest count=%d", table, len(rows), snapshot.DataCounts[table])
		}
		for index, row := range rows {
			got := make([]string, 0, len(row))
			for column := range row {
				got = append(got, column)
			}
			if !sameColumnSet(got, want) {
				t.Fatalf("%s row %d column set mismatch:\n got=%v\nwant=%v", table, index, sortedCopy(got), want)
			}
			if _, found := row["row_id"]; found {
				t.Fatalf("%s row %d still carries the hidden physical key row_id", table, index)
			}
		}
	}
	if raw := string(snapshot.Tables["side_question_requests"]); strings.Contains(raw, "running_session_key") {
		t.Fatal("side_question_requests manifest carries the generated running_session_key")
	}
	if raw := string(snapshot.Tables["side_question_requests"]); strings.Contains(raw, `"identity"`) || strings.Contains(raw, `"identity_hash"`) {
		t.Fatal("side_question_requests manifest carries derived canonical columns")
	}
	if raw := string(snapshot.Tables["assets"]); strings.Contains(raw, "identity_root_domain") || strings.Contains(raw, "hash_root_domain") {
		t.Fatal("assets manifest carries canonical branch columns")
	}
}

// TestTaskArchive_SevenTableRestoreFidelity 走完整归档→恢复链路，逐表比对行数与
// 关键语义列，并确认物理键由 TiDB 重新分配、canonical identity 由当前口径重算。
func TestTaskArchive_SevenTableRestoreFidelity(t *testing.T) {
	fixture := newArchiveSevenTableFixture(t)
	// 归档之前先把夹具自己的**旧物理键**留档：判定"物理键被重新分配"必须拿导出前
	// 的真实键做对照，而不是在压缩之后读一个恒为 0 的全库 MAX。
	legacyKeys := archiveFixturePhysicalKeys(t, fixture)
	snapshot, archiveID := archiveAndCompactFixture(t, fixture)

	// 压缩后的残留用独立 ID 集合断言：夹具范围内的七表连同边/锚/证据绑定必须一处不剩。
	// 锚点判据走 asset_id、绑定判据走 finding_id，都不依赖已被删除的父行子查询，
	// 因此 I01 那类"父行删了、关系还在"的孤儿一定会在这里现形。
	if leftovers := archiveFixturePhysicalKeys(t, fixture); len(leftovers) > 0 {
		for table, ids := range leftovers {
			if len(ids) > 0 {
				t.Fatalf("%s still holds %d rows after hot compaction: %v", table, len(ids), ids)
			}
		}
	}

	restored := restoreArchiveFixture(t, fixture, archiveID, snapshot)
	if len(restored) != 0 {
		t.Fatalf("restore returned warnings for a fully owned package: %v", restored)
	}
	restoredKeys := archiveFixturePhysicalKeys(t, fixture)
	for table, count := range archiveFixtureSevenTableCounts(t, fixture) {
		want := snapshot.DataCounts[table]
		if count != want {
			t.Fatalf("%s restored %d rows, manifest recorded %d", table, count, want)
		}
	}
	// 物理键必须是 TiDB 新分配的号段：恢复后不得出现任何归档前的旧键。
	// traffic_evidence_snapshots 是 VARCHAR 逻辑主键（内容哈希），必须原样保留，不参与本断言。
	for _, table := range []string{"task_relations", "task_asset_links", "task_llm_profiles",
		"exploration_edges", "exploration_anchors", "side_question_sessions",
		"side_question_requests", "finding_traffic_bindings"} {
		before := map[int64]bool{}
		for _, id := range legacyKeys[table] {
			before[id] = true
		}
		for _, id := range restoredKeys[table] {
			if before[id] {
				t.Fatalf("%s reused archived physical key %d: surrogate must be reassigned by TiDB", table, id)
			}
		}
	}
	if restored := archiveFixturePhysicalKeys(t, fixture)["side_question_requests"]; len(restored) != int(snapshot.DataCounts["side_question_requests"]) {
		t.Fatalf("restored side_question_requests rows=%d, manifest recorded %d",
			len(restored), snapshot.DataCounts["side_question_requests"])
	}
	if restored := archiveFixturePhysicalKeys(t, fixture)["finding_traffic_bindings"]; len(restored) != int(snapshot.DataCounts["finding_traffic_bindings"]) {
		t.Fatalf("restored finding_traffic_bindings rows=%d, manifest recorded %d",
			len(restored), snapshot.DataCounts["finding_traffic_bindings"])
	}

	f := fixture
	if id := archiveScalar(t, f.d, `SELECT COALESCE(MIN(source_task_id),0) FROM task_relations WHERE task_id=`+fmt.Sprint(f.task.ID)); id != f.source.ID {
		t.Fatalf("restored task_relations source_task_id=%d, want %d", id, f.source.ID)
	}
	var linkSource, linkSummary string
	var linkNode int64
	if err := f.d.QueryRow(`SELECT source,source_summary,COALESCE(source_node_id,0) FROM task_asset_links
WHERE task_id=? AND asset_id=?`, f.task.ID, f.assetID).Scan(&linkSource, &linkSummary, &linkNode); err != nil {
		t.Fatalf("restored task_asset_links missing: %v", err)
	}
	if linkSource != archiveFixtureLinkSource || linkSummary != archiveFixtureLinkSummary || linkNode != f.nodeIDs[0] {
		t.Fatalf("task_asset_links fidelity lost: source=%q summary=%q node=%d (want %q/%q/%d)",
			linkSource, linkSummary, linkNode, archiveFixtureLinkSource, archiveFixtureLinkSummary, f.nodeIDs[0])
	}
	var chainStatus, chainError string
	var exhaustedAt time.Time
	if err := f.d.QueryRow(`SELECT status,COALESCE(last_error,''),exhausted_at FROM task_llm_profiles
WHERE task_id=? AND profile_id=?`, f.task.ID, f.profileID).Scan(&chainStatus, &chainError, &exhaustedAt); err != nil {
		t.Fatalf("restored task_llm_profiles missing: %v", err)
	}
	if chainStatus != "quota_exhausted" || chainError != "balance exhausted" {
		t.Fatalf("task_llm_profiles state lost: status=%q error=%q", chainStatus, chainError)
	}
	if !exhaustedAt.Equal(archiveFixtureExhaustedAt.Truncate(time.Millisecond)) {
		t.Fatalf("task_llm_profiles exhausted_at=%s, want %s", exhaustedAt, archiveFixtureExhaustedAt)
	}
	var edges int
	if err := f.d.QueryRow(`SELECT COUNT(*) FROM exploration_edges WHERE exploration_id=? AND src_id=? AND dst_id=? AND rel=?`,
		f.task.ExplorationID, f.nodeIDs[0], f.nodeIDs[1], archiveFixtureEdgeRel).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if edges != 1 {
		t.Fatalf("restored exploration_edges lost the archived relation: %d", edges)
	}
	var anchors int
	if err := f.d.QueryRow(`SELECT COUNT(*) FROM exploration_anchors anchor
JOIN exploration_nodes node ON node.id=anchor.node_id
WHERE node.exploration_id=? AND anchor.node_id=? AND anchor.asset_id=?`,
		f.task.ExplorationID, f.nodeIDs[0], f.assetID).Scan(&anchors); err != nil {
		t.Fatal(err)
	}
	if anchors != 1 {
		t.Fatalf("restored exploration_anchors lost the archived anchor: %d", anchors)
	}
	var sessionSnapshot, sessionMemory string
	var sessionVersion, sessionGeneration int64
	if err := f.d.QueryRow(`SELECT snapshot,memory,version,generation FROM side_question_sessions WHERE session_key=?`,
		f.sessionKey).Scan(&sessionSnapshot, &sessionMemory, &sessionVersion, &sessionGeneration); err != nil {
		t.Fatalf("restored side_question_sessions missing: %v", err)
	}
	if !sameJSONObject(sessionSnapshot, archiveFixtureSessionSnapshot) ||
		!sameJSONObject(sessionMemory, archiveFixtureSessionMemory) ||
		sessionVersion != archiveFixtureSessionVersion || sessionGeneration != archiveFixtureSessionGeneration {
		t.Fatalf("side_question_sessions fidelity lost: snapshot=%q memory=%q version=%d generation=%d",
			sessionSnapshot, sessionMemory, sessionVersion, sessionGeneration)
	}
	// canonical identity 必须按当前口径重算：两条请求各自用自己的 client_id 独立算一遍
	// 再比对字节。任何一行退回冷包里的旧 identity，都会掉出这个集合。
	for _, client := range []string{archiveFixtureRequestClient, archiveFixtureRequestClient2} {
		payload := schema.SideQuestionRequestsIdentity(f.sessionKey, archiveFixtureRequestGeneration, client)
		digest := schema.CanonicalHash(payload)
		var requests int
		if err := f.d.QueryRow(`SELECT COUNT(*) FROM side_question_requests
WHERE session_key=? AND client_id=? AND identity=? AND identity_hash=?`,
			f.sessionKey, client, payload, digest[:]).Scan(&requests); err != nil {
			t.Fatalf("restored side_question_requests identity for %s: %v", client, err)
		}
		if requests != 1 {
			t.Fatalf("restored side_question_requests matching recomputed identity for %s = %d, want 1", client, requests)
		}
	}
	var urls, reqHashes string
	var capturedAt, status int64
	if err := f.d.QueryRow(`SELECT url,req_hash,captured_at,status FROM traffic_evidence_snapshots WHERE id=?`,
		f.snapshotID).Scan(&urls, &reqHashes, &capturedAt, &status); err != nil {
		t.Fatalf("restored traffic_evidence_snapshots missing: %v", err)
	}
	if urls != f.snapshotURL || reqHashes != f.snapshotReqHash || capturedAt != f.snapshotCaptured || status != 200 {
		t.Fatalf("traffic snapshot fidelity lost: url=%q hash=%q captured=%d status=%d", urls, reqHashes, capturedAt, status)
	}
}

// TestTaskArchive_CoreConflictFailsClosedWithFullRollback 在恢复前抢占冷包要写回的
// uq_task_asset_links(task_id,asset_id) 身份：恢复必须报错，且 7 张表一行都不留下
// （已经插入的图节点也得随事务一起回滚）。
func TestTaskArchive_CoreConflictFailsClosedWithFullRollback(t *testing.T) {
	fixture := newArchiveSevenTableFixture(t)
	snapshot, archiveID := archiveAndCompactFixture(t, fixture)
	d := fixture.d

	// 该资产被来源任务共同持有，因此归档压缩不会删除它，恢复时必然复用同一 asset id，
	// 预先插入的关联行就能与冷包里的关联行正面撞上唯一键。
	if _, err := d.Exec(`INSERT INTO task_asset_links(task_id,asset_id,source,source_summary) VALUES(?,?,?,?)`,
		fixture.task.ID, fixture.assetID, "conflict", "occupies the archived identity"); err != nil {
		t.Fatalf("pre-occupy task_asset_links: %v", err)
	}
	before := archiveFixtureSevenTableCounts(t, fixture)

	if _, err := d.QueueTaskArchiveRestore(archiveID); err != nil {
		t.Fatalf("queue restore: %v", err)
	}
	// 只领取自己那条恢复记录：队首是别人的就跳过，绝不先改写别人的排队任务（RR-I03）。
	if claimed := claimOwnArchiveJob(t, t.Context(), d, archiveID, RestoreQueued, Restoring); claimed.State != Restoring {
		t.Fatalf("restore claim state = %q, want %q", claimed.State, Restoring)
	}
	if _, err := d.RestoreTaskArchive(archiveID, snapshot, 600); err == nil {
		t.Fatal("conflicting core graph restore must be refused")
	} else {
		// 只要求"报个错"是不够的：必须确认拒绝确实来自被预占的 task_asset_links，
		// 而不是压缩残留让流程在更早的位置提前失败。
		if !strings.Contains(err.Error(), "task_asset_links") {
			t.Fatalf("restore failed for the wrong reason, want a task_asset_links conflict: %v", err)
		}
		t.Logf("fail-closed as expected: %v", err)
	}

	after := archiveFixtureSevenTableCounts(t, fixture)
	for table, count := range after {
		if count != before[table] {
			t.Fatalf("%s gained %d rows after a refused restore (want %d, got %d): transaction was not rolled back",
				table, count-before[table], before[table], count)
		}
	}
	if nodes := archiveCount(t, d, `SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, fixture.task.ExplorationID); nodes != 0 {
		t.Fatalf("refused restore left %d graph nodes behind", nodes)
	}
	if findings := archiveCount(t, d, `SELECT COUNT(*) FROM findings WHERE task_id=?`, fixture.task.ID); findings != 0 {
		t.Fatalf("refused restore left %d findings behind", findings)
	}
	var state string
	if err := d.QueryRow(`SELECT state FROM task_archives WHERE id=?`, archiveID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != Restoring {
		t.Fatalf("refused restore archive state=%q, want %q (its progress write must roll back too)", state, Restoring)
	}

	// 合法 stub 例外（I05）：把根 task 改回"活跃"状态（清掉归档标记）。恢复必须拒绝，
	// 绝不能因为冷包子实体恰好不冲突，就把冷包内容覆盖到一个活跃任务上。
	if _, err := d.Exec(`UPDATE tasks SET archived_at=NULL WHERE id=?`, fixture.task.ID); err != nil {
		t.Fatalf("make the root task live again: %v", err)
	}
	if _, err := d.RestoreTaskArchive(archiveID, snapshot, 600); err == nil {
		t.Fatal("restoring onto an active (non-stub) task must be refused")
	} else if !strings.Contains(err.Error(), "archived stub") {
		t.Fatalf("active root task was refused for the wrong reason: %v", err)
	}
	if _, err := d.Exec(`UPDATE tasks SET archived_at=NOW(3) WHERE id=?`, fixture.task.ID); err != nil {
		t.Fatalf("restore the archived marker: %v", err)
	}
	active := archiveFixtureSevenTableCounts(t, fixture)
	for table, count := range active {
		if count != before[table] {
			t.Fatalf("%s changed to %d rows after the refused active-root restore (want %d): not fully rolled back",
				table, count, before[table])
		}
	}
}

// =====================================================================
// 既有用例迁移到 TiDB 方言
// =====================================================================

func TestTaskArchive_EndToEndRoundTrip(t *testing.T) {
	d := openTestDB(t)
	if err := d.EnsureLLMRecordsTable(); err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureLLMUsageTable(); err != nil {
		t.Fatal(err)
	}
	task, err := d.CreateTaskWithOptions("archive database roundtrip", "restore exact graph", TaskCreateOptions{Name: "cold task"})
	if err != nil {
		t.Fatal(err)
	}
	profileID, err := d.SaveProfile(&LLMProfile{Name: fmt.Sprintf("archive-chain-%d", task.ID), Format: "openai", Model: "archive-model"})
	if err != nil {
		t.Fatal(err)
	}
	exhaustedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	chainCreatedAt := exhaustedAt.Add(-time.Hour)
	if _, err := d.Exec(`UPDATE tasks SET llm_profile_id=?,active_llm_profile_id=? WHERE id=?`, profileID, profileID, task.ID); err != nil {
		t.Fatal(err)
	}
	// 8 列 / 8 个占位符 / 8 个实参严格对齐，并且绑定真实的 profileID：
	// profile_id 写死 0 时 restoreTaskLLMProfiles 会因 llm_profiles 查不到该配置而整行跳过。
	if _, err := d.Exec(`INSERT INTO task_llm_profiles(task_id,profile_id,position,status,last_error,exhausted_at,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?)`, task.ID, profileID, 0, "quota_exhausted", "balance exhausted", exhaustedAt, chainCreatedAt, exhaustedAt); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = d.Exec(`DELETE FROM llm_usage WHERE task_id=? OR exploration_id=?`, fmt.Sprint(task.ID), task.ExplorationID)
		_, _ = d.Exec(`DELETE FROM skill_usage WHERE task_id=? OR exploration_id=?`, task.ID, task.ExplorationID)
		_, _ = d.Exec(`DELETE FROM tool_usage WHERE task_id=? OR exploration_id=?`, task.ID, task.ExplorationID)
		_, _ = d.Exec(`DELETE FROM task_archives WHERE task_id=?`, task.ID)
		_ = d.DeleteTask(task.ID)
		_ = d.DeleteProfile(profileID)
	}()
	if err := d.SetPaused(task.ID, true); err != nil {
		t.Fatal(err)
	}
	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: fmt.Sprintf("archive-%d.example", task.ID), TaskID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	store := d.Exploration(task.ExplorationID)
	nodeID, err := store.AddNode(KindFact, map[string]any{"summary": "archived fact", "asset_ids": []int64{assetID}}, 1, "confirmed", "worker", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Anchor(nodeID, assetID); err != nil {
		t.Fatal(err)
	}
	profileName := fmt.Sprintf("archive-profile-%d", task.ID)
	skillName := fmt.Sprintf("archive-skill-%d", task.ID)
	toolName := fmt.Sprintf("archive-tool-%d", task.ID)
	vulnclass := fmt.Sprintf("archive-vuln-%d", task.ID)
	if err := d.InsertLLMUsage(&LLMUsage{TaskID: fmt.Sprint(task.ID), ExplorationID: task.ExplorationID, Worker: "worker", Model: "test", ProfileName: profileName, InputTokens: 11, OutputTokens: 7}); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertSkillUsage(&SkillUsage{Skill: skillName, AgentKey: "worker", TaskID: task.ID, ExplorationID: task.ExplorationID, Found: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertToolUsage(&ToolUsage{ToolKey: toolName, AgentKey: "worker", TaskID: task.ID, ExplorationID: task.ExplorationID}); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertLLMRecord(&LLMRecord{
		TaskID: fmt.Sprint(task.ID), Model: "archive-model", SessionID: "archive-session",
		Status: "ok", RawRequest: strings.Repeat("request", 1024), RawResponse: strings.Repeat("response", 1024),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddFinding(task.ID, nodeID, vulnclass, "archive finding", SeverityCritical, "summary", "evidence", "worker", []int64{assetID}); err != nil {
		t.Fatal(err)
	}
	archive, err := d.QueueTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 只领取自己那条归档记录（RR-I03）：全局 FIFO 领取会先改写别人的队首再报 id 不符。
	if claimed := claimOwnArchiveJob(t, t.Context(), d, archive.ID, ArchiveQueued, Archiving); claimed.State != Archiving {
		t.Fatalf("archive claim state = %q, want %q", claimed.State, Archiving)
	}
	var llmRecords bytes.Buffer
	snapshot, err := d.SnapshotTaskArchiveWithLLMRecords(task.ID, &llmRecords)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StreamedTables["llm_records"] != TaskArchiveLLMRecordsPath || snapshot.DataCounts["llm_records"] != 1 {
		t.Fatalf("unexpected streamed LLM metadata: paths=%v counts=%v", snapshot.StreamedTables, snapshot.DataCounts)
	}
	if rawRowCount(snapshot.Tables["llm_records"]) != 0 {
		t.Fatal("streamed LLM records were also retained in manifest memory")
	}
	if snapshot.DataCounts["assets"] != 1 || snapshot.DataCounts["exploration_nodes"] < 2 {
		t.Fatalf("unexpected snapshot counts: %#v", snapshot.DataCounts)
	}
	if err := d.CompleteTaskArchive(archive.ID, snapshot, "/tmp/test-task.tar.zst", "abc", 100, 50); err != nil {
		t.Fatal(err)
	}
	if live, err := d.GetTask(task.ID); err != nil || live != nil {
		t.Fatalf("archived task must be hidden, got %+v, %v", live, err)
	}
	var nodes, assets, usage int
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, task.ExplorationID).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE id=?`, assetID).Scan(&assets); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM llm_usage WHERE task_id=?`, fmt.Sprint(task.ID)).Scan(&usage); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 || assets != 0 || usage != 0 {
		t.Fatalf("hot compaction left nodes=%d assets=%d usage=%d", nodes, assets, usage)
	}
	ready, err := d.GetTaskArchive(archive.ID)
	if err != nil || ready == nil || ready.State != ArchiveReady {
		t.Fatalf("ready archive = %+v, %v", ready, err)
	}
	var stats map[string]any
	if err := json.Unmarshal(ready.AggregateStats, &stats); err != nil {
		t.Fatal(err)
	}
	if _, ok := stats["tokens"]; !ok {
		t.Fatalf("archive token summary missing: %#v", stats)
	}
	assertArchiveGlobalStats(t, d, profileName, skillName, toolName, vulnclass)
	// 配置被删除后恢复：全局对象不重建，只降级并给出告警。
	if _, err := d.Exec(`DELETE FROM task_llm_profiles WHERE task_id=?`, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteProfile(profileID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.QueueTaskArchiveRestore(archive.ID); err != nil {
		t.Fatal(err)
	}
	// 同样只领取自己那条恢复记录（RR-I03）。
	if claimed := claimOwnArchiveJob(t, t.Context(), d, archive.ID, RestoreQueued, Restoring); claimed.State != Restoring {
		t.Fatalf("restore claim state = %q, want %q", claimed.State, Restoring)
	}
	warnings, err := d.RestoreTaskArchiveWithLLMRecords(
		archive.ID, snapshot, int64((10 * time.Minute).Seconds()), bytes.NewReader(llmRecords.Bytes()),
	)
	if err != nil {
		t.Fatal(err)
	}
	warningFound := false
	for _, warning := range warnings {
		if strings.Contains(warning, fmt.Sprintf("LLM 配置 %d 已删除", profileID)) {
			warningFound = true
		}
	}
	if !warningFound {
		t.Fatalf("missing deleted profile warning: %v", warnings)
	}
	live, err := d.GetTask(task.ID)
	if err != nil || live == nil {
		t.Fatalf("restored task = %+v, %v", live, err)
	}
	if live.Name != "cold task" || !live.Paused {
		t.Fatalf("restored task metadata mismatch: %+v", live)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, task.ExplorationID).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE id=? AND JSON_CONTAINS(COALESCE(task_ids,'[]'), CAST(? AS JSON))`, assetID, task.ID).Scan(&assets); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT COUNT(*) FROM llm_usage WHERE task_id=?`, fmt.Sprint(task.ID)).Scan(&usage); err != nil {
		t.Fatal(err)
	}
	if nodes < 2 || assets != 1 || usage != 1 {
		t.Fatalf("restore incomplete nodes=%d assets=%d usage=%d", nodes, assets, usage)
	}
	var restoredRawRequest, restoredRawResponse string
	if err := d.QueryRow(`SELECT COALESCE(raw_request,''),COALESCE(raw_response,'') FROM llm_records WHERE task_id=?`, fmt.Sprint(task.ID)).
		Scan(&restoredRawRequest, &restoredRawResponse); err != nil {
		t.Fatal(err)
	}
	if restoredRawRequest != strings.Repeat("request", 1024) || restoredRawResponse != strings.Repeat("response", 1024) {
		t.Fatal("restored streamed LLM record body was truncated")
	}
	var restoredProfileID *int64
	if err := d.QueryRow(`SELECT llm_profile_id FROM tasks WHERE id=?`, task.ID).Scan(&restoredProfileID); err != nil {
		t.Fatal(err)
	}
	if restoredProfileID != nil {
		t.Fatalf("deleted LLM profile restored as %v", *restoredProfileID)
	}
	assertArchiveGlobalStats(t, d, profileName, skillName, toolName, vulnclass)
	if err := d.CompleteTaskArchiveRestore(archive.ID); err != nil {
		t.Fatal(err)
	}
	if item, err := d.GetTaskArchive(archive.ID); err != nil || item != nil {
		t.Fatalf("archive metadata must be consumed: %+v, %v", item, err)
	}
}

func TestTaskArchive_BlockersIgnoreQueuedDependents(t *testing.T) {
	d := openTestDB(t)
	source, err := d.CreateTaskWithOptions("archive blocker source", "source", TaskCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependent, err := d.CreateTaskWithOptions("archive blocker dependent", "dependent", TaskCreateOptions{
		SourceTaskIDs: []int64{source.ID},
	})
	if err != nil {
		_ = d.DeleteTask(source.ID)
		t.Fatal(err)
	}
	defer func() {
		_, _ = d.Exec(`DELETE FROM task_archives WHERE task_id IN (?,?)`, source.ID, dependent.ID)
		_ = d.DeleteTask(dependent.ID)
		_ = d.DeleteTask(source.ID)
	}()

	blockers, err := d.TaskArchiveBlockers()
	if err != nil {
		t.Fatal(err)
	}
	if blockers[source.ID] != dependent.ID {
		t.Fatalf("source blocker=%d, want dependent %d", blockers[source.ID], dependent.ID)
	}
	if err := d.SetPaused(dependent.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := d.QueueTaskArchive(dependent.ID); err != nil {
		t.Fatal(err)
	}
	blockers, err = d.TaskArchiveBlockers()
	if err != nil {
		t.Fatal(err)
	}
	if blocker := blockers[source.ID]; blocker != 0 {
		t.Fatalf("queued dependent must not block source, got %d", blocker)
	}
}

func TestTaskArchive_RecoverInterruptedRequiresManualRetry(t *testing.T) {
	d := openTestDB(t)
	task, err := d.CreateTaskWithOptions("interrupted archive", "must not restart automatically", TaskCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = d.Exec(`DELETE FROM task_archives WHERE task_id=?`, task.ID)
		_ = d.DeleteTask(task.ID)
	}()
	if err := d.SetPaused(task.ID, true); err != nil {
		t.Fatal(err)
	}
	queued, err := d.QueueTaskArchive(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 占位符按出现顺序绑定：state=? 在前、id=? 在后，实参必须同序
	// （原先写反成 (queued.ID, Archiving)，TiDB 会把 'archiving' 当整数塞给 id 而报 1292）。
	if _, err := d.Exec(`UPDATE task_archives SET state=?,phase='snapshot_database',progress=10 WHERE id=?`, Archiving, queued.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.RecoverTaskArchiveJobs(); err != nil {
		t.Fatal(err)
	}
	recovered, err := d.GetTaskArchive(queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.State != ArchiveFailed || recovered.Phase != "interrupted" || recovered.Error == "" {
		t.Fatalf("recovered archive=%+v, want explicit-retry failure", recovered)
	}
}

func assertArchiveGlobalStats(t *testing.T, d *DB, profileName, skillName, toolName, vulnclass string) {
	t.Helper()
	profiles, err := d.UsageByProfile()
	if err != nil {
		t.Fatal(err)
	}
	matchedProfile := false
	for _, profile := range profiles {
		if profile.ProfileName != profileName {
			continue
		}
		matchedProfile = true
		if profile.Calls != 1 || profile.Tasks != 1 || profile.InputTokens != 11 || profile.OutputTokens != 7 {
			t.Fatalf("archive profile aggregate double-counted or missing: %+v", profile)
		}
	}
	if !matchedProfile {
		t.Fatalf("archive profile aggregate %q missing", profileName)
	}
	skills, err := d.SkillStats()
	if err != nil {
		t.Fatal(err)
	}
	matchedSkill := false
	for _, skill := range skills {
		if skill.Skill == skillName {
			matchedSkill = skill.Calls == 1 && skill.Tasks == 1
		}
	}
	if !matchedSkill {
		t.Fatalf("archive skill aggregate %q missing or double-counted: %+v", skillName, skills)
	}
	tools, err := d.ToolUsageCounts()
	if err != nil {
		t.Fatal(err)
	}
	if tools[toolName] != 1 {
		t.Fatalf("archive tool aggregate %q=%d, want 1", toolName, tools[toolName])
	}
	findings, err := d.FindingStats()
	if err != nil {
		t.Fatal(err)
	}
	foundClass := false
	for _, item := range findings.VulnClasses {
		if item == vulnclass {
			foundClass = true
		}
	}
	if !foundClass {
		t.Fatalf("archive finding class %q missing", vulnclass)
	}
}

// =====================================================================
// 7 张表夹具
// =====================================================================

const (
	archiveFixtureLinkSource        = "manual"
	archiveFixtureLinkSummary       = "archive fixture link"
	archiveFixtureEdgeRel           = "leads_to"
	archiveFixtureSessionSnapshot   = `{"phase":"done"}`
	archiveFixtureSessionMemory     = `{"turns":2}`
	archiveFixtureSessionVersion    = int64(3)
	archiveFixtureSessionGeneration = int64(1)
	archiveFixtureRequestGeneration = uint64(1)
	archiveFixtureRequestClient     = "archive-client"
	// 第二条旁路请求必须有**自己的** client_id：identity 的 canonical 口径是
	// (session_key, generation, client_id)，复用第一条的 client_id 就会复用同一条
	// identity/identity_hash，直接撞上 uq_side_requests_identity。
	archiveFixtureRequestClient2 = "archive-client-2"
)

var archiveFixtureExhaustedAt = time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

// archiveSevenTableFixture 是一个真实任务，7 张受管表里各有一行可辨识的数据。
type archiveSevenTableFixture struct {
	d          *DB
	task       *Task
	source     *Task
	assetID    int64
	profileID  int64
	nodeIDs    []int64
	findingID  int64
	sessionKey string

	snapshotID       string
	snapshotURL      string
	snapshotReqHash  string
	snapshotCaptured int64
}

func newArchiveSevenTableFixture(t *testing.T) *archiveSevenTableFixture {
	t.Helper()
	d := openTestDB(t)
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB: %v", err)
		}
	})
	if err := d.EnsureLLMRecordsTable(); err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("p08-04-%d", time.Now().UnixNano()%1e9)
	fixture := &archiveSevenTableFixture{d: d}

	// —— 复审 RR-I01 ——
	// 旧实现直到**全部**实体创建完成之后才注册业务 cleanup：source 建好、target 失败时，
	// 已经落库的 source（连它的 exploration 与 origin 节点）就永久留在共享库里；
	// 收尾里所有删除错误又只 t.Logf，残留永远不可能让用例变红。
	// 现在改成：**每成功创建一个实体就立刻登记**逆拓扑回收（重复登记由 once 兜底，
	// 真正的回收只跑一次，且只按此刻已经拿到的物理 id 删），删除错误一律 t.Errorf，
	// 收尾必须按自有主键把七表全部回读归零。
	recycler := newArchiveFixtureRecycler(t, fixture)
	recycle := recycler.register

	source, err := d.CreateTaskWithOptions(tag+" source", "archive fixture source", TaskCreateOptions{})
	if err != nil {
		t.Fatalf("create source task: %v", err)
	}
	fixture.source = source
	recycle()
	task, err := d.CreateTaskWithOptions(tag+" target", "archive fixture target", TaskCreateOptions{
		SourceTaskIDs: []int64{source.ID},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	fixture.task = task
	recycle()

	// 资产由 target 与 source 共同持有：归档压缩不会删除它，因此恢复时一定复用同一个
	// asset id，冲突用例才能稳定地撞上 uq_task_asset_links。
	domain := fmt.Sprintf("%s.example", tag)
	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: task.ID})
	if err != nil {
		t.Fatalf("upsert asset for task: %v", err)
	}
	fixture.assetID = assetID
	recycle()
	if _, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: source.ID}); err != nil {
		t.Fatalf("co-own asset with source task: %v", err)
	}
	recycle()

	store := d.Exploration(task.ExplorationID)
	fact, err := store.AddNode(KindFact, map[string]any{"summary": "archived fact", "asset_ids": []int64{assetID}}, 1, "confirmed", "worker", nil)
	if err != nil {
		t.Fatalf("add fact node: %v", err)
	}
	fixture.nodeIDs = []int64{fact}
	recycle()
	intent, err := store.AddNode(KindIntent, map[string]any{"summary": "archived intent"}, 2, "open", "worker", nil)
	if err != nil {
		t.Fatalf("add intent node: %v", err)
	}
	fixture.nodeIDs = []int64{fact, intent}
	recycle()
	if err := store.Link(fact, archiveFixtureEdgeRel, intent); err != nil {
		t.Fatalf("link nodes: %v", err)
	}
	if err := store.Anchor(fact, assetID); err != nil {
		t.Fatalf("anchor asset: %v", err)
	}
	recycle()

	// UpsertRootDomain 已经在资产的唯一写出口里原子双写了这条 (task_id,asset_id) link
	// （db/assets.go writeAsset → ensureTaskAssetLinkTx）。夹具绝不能再 INSERT 一份，
	// 那会正面撞上 uq_task_asset_links；七表保真度要断言的是来源三列，因此改成在原地
	// 盖戳，并核实行确实由双写产生（影响 1 行）。
	linkRows, err := d.Exec(`UPDATE task_asset_links SET source=?,source_summary=?,source_node_id=?
WHERE task_id=? AND asset_id=?`, archiveFixtureLinkSource, archiveFixtureLinkSummary, fact, task.ID, assetID)
	if err != nil {
		t.Fatalf("stamp task_asset_links provenance: %v", err)
	}
	if affected, err := linkRows.RowsAffected(); err != nil {
		t.Fatalf("task_asset_links rows affected: %v", err)
	} else if affected != 1 {
		t.Fatalf("task_asset_links dual write left %d rows for (task %d, asset %d), want exactly 1",
			affected, task.ID, assetID)
	}
	recycle()

	profileID, err := d.SaveProfile(&LLMProfile{Name: tag + "-profile", Format: "openai", Model: "archive-model"})
	if err != nil {
		t.Fatalf("create llm profile: %v", err)
	}
	fixture.profileID = profileID
	recycle()
	// 列清单、占位符与实参严格 8/8/8，并且必须绑定真实的 profileID——写死 0 会让
	// restoreTaskLLMProfiles 因为 llm_profiles 里查不到该配置而整行跳过。
	if _, err := d.Exec(`INSERT INTO task_llm_profiles(task_id,profile_id,position,status,last_error,exhausted_at,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?)`, task.ID, profileID, int64(1), "quota_exhausted", "balance exhausted",
		archiveFixtureExhaustedAt, archiveFixtureExhaustedAt.Add(-time.Hour), archiveFixtureExhaustedAt); err != nil {
		t.Fatalf("insert task_llm_profiles: %v", err)
	}
	recycle()

	findingID, err := d.AddFinding(task.ID, fact, tag+"-vuln", "archive finding", SeverityCritical,
		"summary", "evidence", "worker", []int64{assetID})
	if err != nil {
		t.Fatalf("add finding: %v", err)
	}
	fixture.findingID = findingID
	recycle()

	fixture.snapshotCaptured = time.Now().UTC().Unix()
	fixture.snapshotURL = "https://" + tag + ".example.test/vuln"
	fixture.snapshotReqHash = strings.Repeat("a", 64)
	snapshot := TrafficEvidenceSnapshot{
		SourceTrafficID: tag + "-traffic", CapturedAt: fixture.snapshotCaptured,
		URL: fixture.snapshotURL, Method: "GET", Status: 200, ContentType: "text/html",
		ReqHead: "GET /vuln HTTP/1.1", RespHead: "HTTP/1.1 200 OK",
		ReqHash: fixture.snapshotReqHash, RespHash: strings.Repeat("b", 64), ReqLen: 24, RespLen: 2048,
	}
	snapshot.ID = TrafficSnapshotID(snapshot)
	fixture.snapshotID = snapshot.ID
	recycle()
	if err := d.WithEvidenceTx(context.Background(), func(tx *sql.Tx) error {
		return AddFindingTrafficTx(tx, findingID, []PreparedTrafficEvidence{{
			Ref:      TrafficRef{TrafficID: tag + "-traffic", Role: "proof", Note: "archive fixture"},
			Snapshot: snapshot,
		}})
	}); err != nil {
		t.Fatalf("bind traffic evidence: %v", err)
	}
	recycle()

	fixture.sessionKey = tag + "@archive"
	if _, err := d.Exec(`INSERT INTO side_question_sessions
(session_key,task_id,exploration_id,run_id,version,snapshot,generation,memory) VALUES(?,?,?,?,?,?,?,?)`,
		fixture.sessionKey, task.ID, task.ExplorationID, 0, archiveFixtureSessionVersion,
		archiveFixtureSessionSnapshot, archiveFixtureSessionGeneration, archiveFixtureSessionMemory); err != nil {
		t.Fatalf("insert side_question_sessions: %v", err)
	}
	recycle()
	// 两条旁路请求：14 列 / 14 个 VALUES 项 / 14 个占位符 / 14 个实参，逐一对齐。
	// 第二条使用自己的 client_id，并按它自己的 canonical identity 重算 identity 与
	// identity_hash，否则两行会撞 uq_side_requests_identity 唯一索引。
	if _, err := d.Exec(`INSERT INTO side_question_requests
(id,session_key,generation,client_id,identity,identity_hash,question,answer,status,error,model,sequence,`+"`usage`"+`,context_info)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "req-"+tag, fixture.sessionKey, int64(archiveFixtureRequestGeneration),
		archiveFixtureRequestClient,
		requestIdentityPayload(fixture.sessionKey, archiveFixtureRequestClient),
		requestIdentityHash(fixture.sessionKey, archiveFixtureRequestClient),
		"why?", "because", "done", "", `{"model":"archive-model"}`, int64(0), `{"calls":1}`, `{"nodes":0}`); err != nil {
		t.Fatalf("insert side_question_requests: %v", err)
	}
	recycle()
	if _, err := d.Exec(`INSERT INTO side_question_requests
(id,session_key,generation,client_id,identity,identity_hash,question,answer,status,error,model,sequence,`+"`usage`"+`,context_info)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "req2-"+tag, fixture.sessionKey, int64(archiveFixtureRequestGeneration),
		archiveFixtureRequestClient2,
		requestIdentityPayload(fixture.sessionKey, archiveFixtureRequestClient2),
		requestIdentityHash(fixture.sessionKey, archiveFixtureRequestClient2),
		"again?", "again", "done", "", `{"model":"archive-model"}`, int64(1), `{"calls":2}`, `{"nodes":1}`); err != nil {
		t.Fatalf("insert second side_question_requests: %v", err)
	}
	// 最后一次登记：准备阶段全部成功时也照旧登记（once 兜底，回收只跑一次）。
	recycle()
	return fixture
}

// archiveFixtureRecycler 把"每个实体创建成功后登记一次逆拓扑回收"变成机械动作
// （复审 RR-I01）。
//
// t.Cleanup 按登记顺序 LIFO 执行、登记几次就跑几次，因此真正的回收用 sync.Once 兜底：
// 无论在哪一步 t.Fatalf 提前退出，**此刻已经落库**的那几个实体都会被收走，
// 而不是像旧实现那样"没走到最后一行就没有任何业务 cleanup"。
type archiveFixtureRecycler struct {
	t       *testing.T
	fixture *archiveSevenTableFixture
	once    sync.Once
}

func newArchiveFixtureRecycler(t *testing.T, fixture *archiveSevenTableFixture) *archiveFixtureRecycler {
	t.Helper()
	recycler := &archiveFixtureRecycler{t: t, fixture: fixture}
	// 立刻登记一次：连第一个实体都还没建出来时的提前失败也有收尾。
	recycler.register()
	return recycler
}

// register 在**每一个**实体创建成功之后调用。
func (r *archiveFixtureRecycler) register() {
	r.t.Cleanup(func() {
		r.once.Do(func() { archiveFixtureCleanup(r.t, r.fixture) })
	})
}

// requestIdentityPayload 用与线上写路径同一个 canonical 口径算出 identity 载荷。
// client_id 是 identity 的一部分，每条请求都必须按自己的 client_id 计算。
func requestIdentityPayload(sessionKey, clientID string) []byte {
	return schema.SideQuestionRequestsIdentity(sessionKey, archiveFixtureRequestGeneration, clientID)
}

func requestIdentityHash(sessionKey, clientID string) []byte {
	sum := schema.CanonicalHash(requestIdentityPayload(sessionKey, clientID))
	return sum[:]
}

// archiveFixtureValidIDs 只留下**正数**且去重后的自有 id（复审 RR-I01）。
//
// 准备阶段是在每一步中途都可能失败的：task 还没建出来时 taskID 就是 0，asset 还没
// 拿到时 assetID 也是 0，snapshotID 还是空串。旧实现把整条语句用**一个** ready 标志
// 兜住，只要有一个父 id 有效就整条 SQL 照发，于是谓词里混进 `IN (0, 7)`、
// `OR snapshot_id=”`、`OR asset_id=0` 这类分支 —— 共享库里凡是 task_id=0 /
// asset_id=0 / snapshot_id=” 的旧行或无外键孤儿都会被当成"本夹具的东西"删掉。
// 一个父 id 有效，从来不等于另一个未取得的 id 可以进删除范围。
//
// 现在每条谓词都只按**自己**已经拿到的有效标识构造；一个有效 id 都没有，整条语句
// 连发都不发。
func archiveFixtureValidIDs(values ...int64) []int64 {
	out := make([]int64, 0, len(values))
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func archiveFixtureIDArgs(values []int64) []any {
	args := make([]any, 0, len(values))
	for _, value := range values {
		args = append(args, value)
	}
	return args
}

// archiveFixtureCleanup 逆拓扑回收本夹具**已经拿到有效物理 id** 的全部实体。
//
// 复审 RR-I01 的三条硬要求：
//  1. **逐谓词**过滤有效自有标识：每个 DELETE 只用正数 id / 非空字符串，复合条件一律
//     拆成独立语句，杜绝 `IN (0, …)`、`OR snapshot_id=”`、`OR asset_id=0`；
//  2. 删除错误一律 t.Errorf 标红；
//  3. **每一张实际被清理的表都自动带一条同谓词的终态归零回读**（add() 成对登记，
//     漏配在结构上不可能发生），另加父根（tasks/explorations/llm_profiles）回读。
func archiveFixtureCleanup(t *testing.T, fixture *archiveSevenTableFixture) {
	t.Helper()
	d := fixture.d
	var taskID, sourceID, expID, sourceExpID int64
	if fixture.task != nil {
		taskID, expID = fixture.task.ID, fixture.task.ExplorationID
	}
	if fixture.source != nil {
		sourceID, sourceExpID = fixture.source.ID, fixture.source.ExplorationID
	}
	// 每条谓词都从这里取有效集合：为空 ⇒ 该支路一条 SQL 都不发。
	tasks := archiveFixtureValidIDs(taskID, sourceID)
	exps := archiveFixtureValidIDs(expID, sourceExpID)
	nodes := archiveFixtureValidIDs(fixture.nodeIDs...)
	assets := archiveFixtureValidIDs(fixture.assetID)
	archiveFixtureCleanupWith(t, d, fixture, tasks, exps, nodes, assets)
}

// archiveFixtureCleanupWith 是 archiveFixtureCleanup 的执行体：入参已经是**逐字段
// 过滤过**的有效自有标识集合，本函数只负责按逆拓扑拼 SQL、执行、逐表回读。
//
// 与表名一样按参数顺序登记，DELETE 顺序 = 回收顺序 = 七表夹具准备顺序的逆序。
func archiveFixtureCleanupWith(t *testing.T, d *DB, fixture *archiveSevenTableFixture, tasks, exps, nodes, assets []int64) {
	t.Helper()

	// add 成对登记一条 DELETE 与它**完全同谓词**的终态归零回读：
	// "清了哪张表、按什么条件清"与"回读哪张表、按什么条件断言归零"永远是同一条谓词，
	// 不会再出现旧实现那种"删了 llm_records / exploration_anchors，终态列表里却没有
	// 这两张表"的漏网（复审 RR-I01 B）。
	type step struct {
		label string
		query string
		args  []any
	}
	var deletes, probes []step
	add := func(table, where string, args ...any) {
		label := table + " WHERE " + where
		deletes = append(deletes, step{label: label, query: `DELETE FROM ` + table + ` WHERE ` + where, args: args})
		probes = append(probes, step{label: label, query: `SELECT count(*) FROM ` + table + ` WHERE ` + where, args: args})
	}
	// 按有效 id 列表展开一个 `col IN (?,?,…)`；一个有效 id 都没有就整条不发。
	addIn := func(table, column string, ids []int64) {
		if len(ids) == 0 {
			return
		}
		add(table, column+` IN (`+inPlaceholders(len(ids))+`)`, archiveFixtureIDArgs(ids)...)
	}
	addTextIn := func(table, column string, ids []int64) {
		if len(ids) == 0 {
			return
		}
		args := make([]any, 0, len(ids))
		for _, id := range ids {
			args = append(args, fmt.Sprint(id))
		}
		add(table, `COALESCE(`+column+`,'') IN (`+inPlaceholders(len(ids))+`)`, args...)
	}
	// 父行子查询支路：压缩/恢复失败留下的孤儿关系没有父行可回指，只能顺着**自有**
	// 的父 id 找。同样只在父 id 有效时才拼这一条。
	addNodeSubquery := func(table, nodeColumn, column string, ids []int64) {
		if len(ids) == 0 {
			return
		}
		args := make([]any, 0, len(ids))
		for _, id := range ids {
			args = append(args, id)
		}
		add(table, nodeColumn+" IN (SELECT id FROM exploration_nodes WHERE exploration_id IN ("+
			inPlaceholders(len(ids))+"))", args...)
	}
	// 额外的终态回读（对象由产品 API 删除，不是这里的 DELETE 语句）。
	addProbe := func(label, query string, args ...any) {
		probes = append(probes, step{label: label, query: query, args: args})
	}

	// —— ① 投递关系：先按自有的 findingID 精确清，再按 snapshot_id 清孤儿 ——
	if fixture.findingID > 0 {
		add("finding_traffic_bindings", `finding_id=?`, fixture.findingID)
	}
	if len(tasks) > 0 {
		// 自有 findingID 之外，压缩可能为同一任务的其他 finding 建过绑定：顺着**自有
		// 任务 id** 的子查询收口，父行子查询里的 IN 列表只含正数 id。
		args := archiveFixtureIDArgs(tasks)
		add("finding_traffic_bindings", `finding_id IN (SELECT id FROM findings WHERE task_id IN (`+
			inPlaceholders(len(tasks))+`))`, args...)
	}
	if fixture.snapshotID != "" {
		add("finding_traffic_bindings", `snapshot_id=?`, fixture.snapshotID)
		add("traffic_evidence_snapshots", `id=?`, fixture.snapshotID)
	}
	if fixture.sessionKey != "" {
		add("side_question_requests", `session_key=?`, fixture.sessionKey)
	}
	addIn("side_question_sessions", "task_id", tasks)
	addTextIn("llm_records", "task_id", tasks)
	// —— ② 用量表：task 维度与 exploration 维度**各自独立**成句（旧实现把它们 OR 在
	// 一条 SQL 里，于是一边有效就连带把另一边的 0 也发了出去）——
	addTextIn("llm_usage", "task_id", tasks)
	addIn("llm_usage", "exploration_id", exps)
	addIn("skill_usage", "task_id", tasks)
	addIn("skill_usage", "exploration_id", exps)
	addIn("tool_usage", "task_id", tasks)
	addIn("tool_usage", "exploration_id", exps)
	addIn("task_archives", "task_id", tasks)
	addIn("findings", "task_id", tasks)
	addIn("task_asset_links", "task_id", tasks)
	addIn("task_llm_profiles", "task_id", tasks)
	addIn("task_scope", "task_id", tasks)
	addIn("task_relations", "task_id", tasks)
	addIn("task_relations", "source_task_id", tasks)
	addIn("activity", "exploration_id", exps)
	addIn("task_constraints", "exploration_id", exps)
	// —— ③ 锚点是无外键的关系表：按自有 nodeID / 自有 exploration 的节点 / 自有 asset
	// 三条**互不 OR** 的谓词分别回收，每条都各自带回读 ——
	addIn("exploration_anchors", "node_id", nodes)
	addNodeSubquery("exploration_anchors", "node_id", "exploration_id", exps)
	addIn("exploration_anchors", "asset_id", assets)
	addIn("exploration_edges", "exploration_id", exps)
	addIn("exploration_nodes", "exploration_id", exps)
	addIn("assets", "id", assets)

	for _, statement := range deletes {
		if _, err := d.Exec(statement.query, statement.args...); err != nil {
			t.Errorf("archive fixture cleanup %s: %v", statement.label, err)
		}
	}

	// 任务根行走产品自己的级联入口（DeleteTask 比裸 DELETE 多清一批下游表）。
	// 集合来自 archiveFixtureValidIDs：只有**正数自有** id 才会进来，准备阶段还没建出来
	// 的那一侧根本不会出现在这里（复审 RR-I01 A 的零值污染）。
	for _, id := range tasks {
		if err := d.DeleteTask(id); err != nil {
			t.Errorf("delete task %d: %v", id, err)
		}
	}
	if fixture.profileID > 0 {
		if err := d.DeleteProfile(fixture.profileID); err != nil {
			t.Errorf("delete profile %d: %v", fixture.profileID, err)
		}
	}

	// —— 终态回读：本夹具拿到的每一个物理 id 都必须彻底归零（复审 RR-I01）——
	// deletes 里每一条都已经自动配了同谓词回读，这里只补父根。
	addInProbe := func(label, column string, ids []int64) {
		if len(ids) == 0 {
			return
		}
		addProbe(label, `SELECT count(*) FROM tasks WHERE `+column+` IN (`+inPlaceholders(len(ids))+`)`,
			archiveFixtureIDArgs(ids)...)
	}
	addInProbe("tasks(id)", "id", tasks)
	// 探索根表按**自己的主键**回读。这里绝不能沿用"下游表按 exploration_id"的写法：
	// explorations 表没有 exploration_id 这一列，写错只会在真库上炸成 Unknown column，
	// 而这条回读恰恰是"探索根到底还在不在"的唯一证据。
	if len(exps) > 0 {
		addProbe("explorations(id)", `SELECT count(*) FROM explorations WHERE id IN (`+
			inPlaceholders(len(exps))+`)`, archiveFixtureIDArgs(exps)...)
	}
	if fixture.findingID > 0 {
		addProbe("finding_traffic_bindings(finding_id)",
			`SELECT count(*) FROM finding_traffic_bindings WHERE finding_id=?`, fixture.findingID)
	}
	if fixture.snapshotID != "" {
		addProbe("traffic_evidence_snapshots(id)",
			`SELECT count(*) FROM traffic_evidence_snapshots WHERE id=?`, fixture.snapshotID)
	}
	if fixture.sessionKey != "" {
		addProbe("side_question_requests(session_key)",
			`SELECT count(*) FROM side_question_requests WHERE session_key=?`, fixture.sessionKey)
	}
	if fixture.profileID > 0 {
		addProbe("llm_profiles(id)", `SELECT count(*) FROM llm_profiles WHERE id=?`, fixture.profileID)
	}

	for _, probe := range probes {
		mustAssertOwnedGone(t, d, "归档夹具 "+probe.label, probe.query, probe.args...)
	}
}

// requireNoQueuedArchiveJobs 在测试**开始**时核验归档队列里没有排队记录
// （复审 RR-I03）。
//
// 归档 worker 是全局单队列 FIFO（archive_queued / restore_queued / delete_queued 混排），
// 测试夹具一旦在脏队列上跑"领取"，无论怎么检查都已经晚了。这里只接受两种结局：
// 队列干净 ⇒ 继续；否则诚实 SKIP 并写清楚原因，绝不盲目领全局队首。
func requireNoQueuedArchiveJobs(t *testing.T, d *DB) {
	t.Helper()
	var queued int
	if err := d.QueryRow(`SELECT count(*) FROM task_archives
WHERE state IN ('archive_queued','restore_queued','delete_queued')`).Scan(&queued); err != nil {
		t.Fatalf("统计归档排队记录失败: %v", err)
	}
	if queued > 0 {
		t.Skipf("共享库归档队列里已有 %d 条别人的排队记录：全局领取会先改写它们的 state/phase/progress，"+
			"本夹具 cleanup 并不拥有那些记录，撤不回。请等外部 worker 清空队列，或在独占干净库上运行（RR-I03）", queued)
	}
}

// claimOwnArchiveJob 只领取**本夹具自己**那条排队记录（复审 RR-I03）。
//
// 为什么不能再直接用全局 ClaimTaskArchiveJob：它按 requested_at,id 取全表队首，
// **先**把队首 UPDATE 成 archiving/restoring/deleting 并 commit，调用方**之后**才可能
// 看见 claimed.ID != 自己的 id。共享库里只要有一条别人的记录排在前面，那次调用就已经
// 改掉别人任务的状态、进度与 error，"检查"永远追不上"写入"。
//
// 本函数把判定与推进放进同一个事务，保持与生产完全相同的状态推进 SQL：
//  1. 与生产一模一样的队首 `SELECT … FOR UPDATE`；
//  2. 队首不是本夹具的 id、或状态不是预期的排队态 ⇒ **不发任何 UPDATE**，直接回滚并
//     t.Skipf（对别人一个字节都不改，也不做"回滚别人原状态"这种伪补偿）；
//  3. 队首正是本夹具记录 ⇒ 在同一事务里执行生产同一条推进语句并提交。
//
// "读到队首是自己"与"改写队首"之间因此不存在任何窗口。
func claimOwnArchiveJob(t *testing.T, ctx context.Context, d *DB, wantID int64, wantQueued, wantActive string) *TaskArchive {
	t.Helper()
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("archive claim begin tx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后是 no-op
	var id int64
	var queuedState string
	err = tx.QueryRowContext(ctx, `SELECT id,state FROM task_archives
WHERE state IN ('archive_queued','restore_queued','delete_queued')
ORDER BY requested_at,id LIMIT 1 FOR UPDATE`).Scan(&id, &queuedState)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		t.Skipf("归档队列此刻为空，而本夹具刚排队的 id=%d 也不在里面：有外部 worker 正在消费同一个队列，"+
			"本用例不与它竞争，诚实跳过（RR-I03）", wantID)
		return nil
	}
	if err != nil {
		t.Fatalf("archive claim peek head: %v", err)
	}
	if id != wantID || queuedState != wantQueued {
		_ = tx.Rollback()
		t.Skipf("归档队首是 id=%d(state=%s)，不是本夹具的 id=%d(state=%s)：全局 FIFO 领取会先把这条**别人的**记录"+
			"UPDATE 成活跃态并 commit，之后才让调用方看见 id 不符。本函数一条 UPDATE 都不发，直接跳过（RR-I03）",
			id, queuedState, wantID, wantQueued)
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE task_archives
SET state=?,phase='starting',progress=1,error='' WHERE id=?`, wantActive, id); err != nil {
		t.Fatalf("archive claim advance: %v", err)
	}
	item, err := scanTaskArchive(tx.QueryRowContext(ctx, `SELECT `+taskArchiveCols+` FROM task_archives WHERE id=?`, id))
	if err != nil {
		t.Fatalf("archive claim rescan: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("archive claim commit: %v", err)
	}
	return item
}

// archiveAndCompactFixture 暂停任务、排队归档、领取任务并导出冷包，最后完成热库压缩。
// 返回冷包快照与归档 id（任务此时已是归档 stub）。
func archiveAndCompactFixture(t *testing.T, fixture *archiveSevenTableFixture) (*TaskArchiveSnapshot, int64) {
	t.Helper()
	d := fixture.d
	if err := d.SetPaused(fixture.task.ID, true); err != nil {
		t.Fatalf("pause task: %v", err)
	}
	archive, err := d.QueueTaskArchive(fixture.task.ID)
	if err != nil {
		t.Fatalf("queue archive: %v", err)
	}
	// 只领取自己那条记录：队首不是它就跳过，绝不改写别人的排队任务（复审 RR-I03）。
	if claimed := claimOwnArchiveJob(t, t.Context(), d, archive.ID, ArchiveQueued, Archiving); claimed.State != Archiving {
		t.Fatalf("archive claim state = %q, want %q", claimed.State, Archiving)
	}
	snapshot, err := d.SnapshotTaskArchive(fixture.task.ID)
	if err != nil {
		t.Fatalf("snapshot archive: %v", err)
	}
	if err := d.CompleteTaskArchive(archive.ID, snapshot, "/tmp/artex-archive-fixture.tar.zst", "fixture-sha", 1024, 512); err != nil {
		t.Fatalf("complete archive: %v", err)
	}
	return snapshot, archive.ID
}

// restoreArchiveFixture 领取恢复任务并把冷包写回热库，返回告警列表。
func restoreArchiveFixture(t *testing.T, fixture *archiveSevenTableFixture, archiveID int64, snapshot *TaskArchiveSnapshot) []string {
	t.Helper()
	if _, err := fixture.d.QueueTaskArchiveRestore(archiveID); err != nil {
		t.Fatalf("queue restore: %v", err)
	}
	// 同样只领取自己那条记录（复审 RR-I03）。
	if claimed := claimOwnArchiveJob(t, t.Context(), fixture.d, archiveID, RestoreQueued, Restoring); claimed.State != Restoring {
		t.Fatalf("restore claim state = %q, want %q", claimed.State, Restoring)
	}
	warnings, err := fixture.d.RestoreTaskArchive(archiveID, snapshot, 600)
	if err != nil {
		t.Fatalf("restore archive: %v", err)
	}
	return warnings
}

// archiveFixtureSevenTableCounts 统计夹具在 7 张受管表里的行数。
func archiveFixtureSevenTableCounts(t *testing.T, fixture *archiveSevenTableFixture) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(archivePlanWhitelists))
	for table := range archivePlanWhitelists {
		out[table] = archiveCount(t, fixture.d,
			`SELECT COUNT(*) FROM `+table+` WHERE `+archiveFixtureSevenTableWhere(table, fixture))
	}
	return out
}

// archiveFixturePhysicalKeys 采集夹具在受管表上留下的物理主键集合（升序）。
//
// 每张表的定位条件都必须**不依赖可能已被压缩删除的父行**：锚点按 asset_id、证据
// 绑定按 finding_id、旁路请求按 session_key。只有这样才能区分"父行删干净了"和
// "父行删了、关系行还留在热库"这两种完全不同的状态。
func archiveFixturePhysicalKeys(t *testing.T, fixture *archiveSevenTableFixture) map[string][]int64 {
	t.Helper()
	task := fmt.Sprint(fixture.task.ID)
	exp := fmt.Sprint(fixture.task.ExplorationID)
	probes := []struct {
		table  string
		column string
		where  string
		args   []any
	}{
		{"task_relations", "row_id", "task_id=" + task, nil},
		{"task_asset_links", "row_id", "task_id=" + task, nil},
		{"task_llm_profiles", "row_id", "task_id=" + task, nil},
		{"exploration_edges", "row_id", "exploration_id=" + exp, nil},
		{"exploration_anchors", "row_id", "asset_id=?", []any{fixture.assetID}},
		{"side_question_sessions", "row_id", "task_id=" + task, nil},
		{"side_question_requests", "ordinal", "session_key=?", []any{fixture.sessionKey}},
		{"finding_traffic_bindings", "id", "finding_id=?", []any{fixture.findingID}},
	}
	out := make(map[string][]int64, len(probes))
	for _, probe := range probes {
		rows, err := fixture.d.Query(`SELECT `+probe.column+` FROM `+probe.table+` WHERE `+probe.where+` ORDER BY `+probe.column, probe.args...)
		if err != nil {
			t.Fatalf("read %s physical keys: %v", probe.table, err)
		}
		ids := []int64{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Fatalf("scan %s physical keys: %v", probe.table, err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("iterate %s physical keys: %v", probe.table, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close %s physical keys: %v", probe.table, err)
		}
		out[probe.table] = ids
	}
	return out
}

// archiveFixtureSevenTableWhere 把每张表限定到夹具自己的数据上。
func archiveFixtureSevenTableWhere(table string, fixture *archiveSevenTableFixture) string {
	task := fmt.Sprint(fixture.task.ID)
	exp := fmt.Sprint(fixture.task.ExplorationID)
	switch table {
	case "task_relations":
		return "task_id=" + task
	case "task_asset_links":
		return "task_id=" + task
	case "task_llm_profiles":
		return "task_id=" + task
	case "exploration_edges":
		return "exploration_id=" + exp
	case "exploration_anchors":
		// 判据不能走 nodes 子查询：压缩把节点删干净后，孤儿锚恰好查不到，
		// 计数会假装"已清空"。夹具资产由来源任务共享、不会被压缩删除，按它定位
		// 就能看到任何残留下来的锚。
		return "asset_id=" + fmt.Sprint(fixture.assetID)
	case "side_question_sessions":
		return "task_id=" + task
	case "traffic_evidence_snapshots":
		return "id='" + fixture.snapshotID + "'"
	}
	panic("unsupported archive fixture table " + table)
}

// =====================================================================
// 通用小工具
// =====================================================================

func sameColumnSet(got []string, want []string) bool {
	if len(got) == 0 || len(want) == 0 {
		return false
	}
	left, right := sortedCopy(got), sortedCopy(want)
	if len(left) != len(right) {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func containsColumn(columns []string, want string) bool {
	for _, column := range columns {
		if column == want {
			return true
		}
	}
	return false
}

func sameJSONObject(got, want string) bool {
	var left, right any
	if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &left); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(want)), &right); err != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func archiveCount(t *testing.T, d *DB, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := d.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return count
}

func archiveScalar(t *testing.T, d *DB, query string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := d.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("scalar %q: %v", query, err)
	}
	return value
}

// readRepoFile 读取包目录里的源文件用于静态方言检查（go test 的工作目录就是包目录）。
func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// =====================================================================
// 离线门禁：夹具 SQL 的列 / 占位符 / 实参必须逐一对齐（I02）
// =====================================================================

// TestTaskArchive_FixtureSQLBindingsAligned 用 AST 扫本文件里每一条以字符串字面量
// 发出的 Exec/ExecContext，核对三条确定性不变式：
//  1. INSERT 的列数必须等于 VALUES 项数；
//  2. 语句里 `?` 的数量必须等于绑定的实参数量；
//  3. 外键语义列（`*_id`）绝不允许被写成常量 0 —— profile_id=0 一类的写死会让恢复侧
//     按"引用的配置已删除"整行跳过，夹具却照样建得出来。
//
// 这三类错位既不违反编译也不被 go vet 捕获，只在真实 TiDB 上以"夹具根本插不进去"
// 或"恢复静默少一行"的形式暴露，集成用例没 DSN 时又整组 SKIP。把它们变成离线可执行
// 的门禁，缺陷就不会再退回。
func TestTaskArchive_FixtureSQLBindingsAligned(t *testing.T) {
	const source = "task_archives_test.go"
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, source, readRepoFile(t, source), 0)
	if err != nil {
		t.Fatalf("parse %s: %v", source, err)
	}
	checked := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name != "Exec" && sel.Sel.Name != "ExecContext" {
			return true
		}
		args := call.Args
		if sel.Sel.Name == "ExecContext" {
			if len(args) < 2 {
				return true
			}
			args = args[1:]
		}
		// Exec(query, args...) 一类的动态绑定数量不在本门禁的能力范围内。
		if len(args) == 0 || call.Ellipsis != token.NoPos {
			return true
		}
		statement, ok := literalSQLText(args[0])
		if !ok {
			return true
		}
		checked++
		line := fileSet.Position(call.Lparen).Line
		if bound, want := len(args)-1, strings.Count(statement, "?"); bound != want {
			t.Errorf("line %d binds %d arguments but the statement carries %d placeholders:\n%s",
				line, bound, want, trimForReport(statement))
		}
		columns, values, ok := insertColumnsAndValues(statement)
		if !ok {
			return true
		}
		if len(columns) != len(values) {
			t.Errorf("line %d lists %d INSERT columns but supplies %d VALUES items:\n%s",
				line, len(columns), len(values), trimForReport(statement))
		}
		for index, item := range values {
			if index >= len(columns) {
				break
			}
			if !strings.HasSuffix(columns[index], "_id") {
				continue
			}
			if trimmed := strings.TrimSpace(item); trimmed == "0" || trimmed == "'0'" {
				t.Errorf("line %d hard-wires INSERT column %s to %s instead of binding the real reference",
					line, columns[index], trimmed)
			}
		}
		return true
	})
	if checked == 0 {
		t.Fatal("fixture binding gate inspected no literal SQL statement; the scanner is broken")
	}
}

// literalSQLText 取出一条语句的常量文本：反引号/双引号字面量，或它们的 `+` 拼接。
func literalSQLText(expr ast.Expr) (string, bool) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(typed.Value)
		if err != nil {
			return "", false
		}
		return value, true
	case *ast.BinaryExpr:
		if typed.Op != token.ADD {
			return "", false
		}
		left, ok := literalSQLText(typed.X)
		if !ok {
			return "", false
		}
		right, ok := literalSQLText(typed.Y)
		if !ok {
			return "", false
		}
		return left + right, true
	default:
		return "", false
	}
}

// insertColumnsAndValues 解出一条 INSERT 的列清单与 VALUES 项（按顶层逗号切分）。
// 只处理"表名 (列) … VALUES (项)"这一种夹具写法，其他形态直接判定为不适用。
func insertColumnsAndValues(statement string) ([]string, []string, bool) {
	lower := strings.ToLower(statement)
	start := strings.Index(lower, "insert into")
	if start < 0 {
		return nil, nil, false
	}
	rest := statement[start:]
	restLower := lower[start:]
	open := strings.Index(rest, "(")
	if open < 0 {
		return nil, nil, false
	}
	columns, closed := splitParenthesized(rest, open)
	if closed < 0 {
		return nil, nil, false
	}
	rest = rest[closed+1:]
	restLower = restLower[closed+1:]
	valuesAt := strings.Index(restLower, "values")
	if valuesAt < 0 {
		return nil, nil, false
	}
	rest = rest[valuesAt+len("values"):]
	open = strings.Index(rest, "(")
	if open < 0 {
		return nil, nil, false
	}
	values, closed := splitParenthesized(rest, open)
	if closed < 0 {
		return nil, nil, false
	}
	// 多行 VALUES(`(…), (…)`）不在夹具写法内，出现即视为不适用而不是误报。
	if strings.Contains(strings.TrimSpace(rest[closed+1:]), "(") {
		return nil, nil, false
	}
	return columns, values, true
}

// splitParenthesized 从 open 处的 `(` 起读到配对 `)`，返回按顶层逗号切开的片段。
func splitParenthesized(text string, open int) ([]string, int) {
	depth := 0
	quote := byte(0)
	parts := []string{}
	start := open + 1
	for index := open; index < len(text); index++ {
		char := text[index]
		switch {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"' || char == '`':
			quote = char
		case char == '(':
			depth++
		case char == ')':
			depth--
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(text[start:index]))
				return parts, index
			}
		case char == ',' && depth == 1:
			parts = append(parts, strings.TrimSpace(text[start:index]))
			start = index + 1
		}
	}
	return nil, -1
}

// =====================================================================
// 替身驱动的离线控制流门禁（I03 / I04）
//
// 复用 P08-02 留下的 database/sql driver 替身：它记录每一条真正经过 database/sql
// 的语句。这里证明的是"实现发出了什么 SQL、走了哪条分支"，不冒充 TiDB 的真机锁行为。
// =====================================================================

// TestTaskArchive_ScopeHashHitLocksCandidateAndArbitrates 让 task_scope 的 hash 探测
// 命中一条候选行：恢复必须 (1) 先按物理 PK 取 FOR UPDATE 行锁，(2) 在同一事务里调用
// VerifyHashArbitration 核验完整 identity，(3) 当返回不同 identity 时确切报 ErrIdentityHashCollision 并回滚（闭环 RR-I02）。
func TestTaskArchive_ScopeHashHitLocksCandidateAndArbitrates(t *testing.T) {
	state := newStubState(
		// 非锁定定位：按 hash 命中候选物理 PK 9001。
		stubRule{match: stubMatch("FROM `task_scope` WHERE `identity_hash`=?"), rows: stubRowsAlways(9001)},
		// 仲裁读取两列 (id, identity)：返回不同的 identity 字节，真实触发 ErrIdentityHashCollision
		stubRule{
			match: stubMatch("SELECT `id`, `identity` FROM `task_scope`"),
			rows: func(int, int64) [][]driver.Value {
				return [][]driver.Value{{int64(9001), []byte("different-identity-payload")}}
			},
		},
	)
	d := openStubDB(t, state)
	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("begin stub tx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	raw := json.RawMessage(`[{"task_id":101,"kind":"domain","domain":"corp.example"}]`)
	_, err = restoreTaskScopes(context.Background(), tx, raw)
	if err == nil {
		t.Fatal("a hash hit came back without an error: the arbitration outcome was swallowed")
	}
	if !errors.Is(err, schema.ErrIdentityHashCollision) {
		t.Fatalf("expected ErrIdentityHashCollision, got: %v", err)
	}

	var lockAt, arbitrationAt = -1, -1
	for index, call := range state.statements(func(call stubCall) bool {
		return strings.Contains(call.Query, "task_scope")
	}) {
		switch {
		case strings.Contains(call.Query, "SELECT `id` FROM `task_scope` WHERE `id`=? FOR UPDATE"):
			lockAt = index
		case strings.Contains(call.Query, "SELECT `id`, `identity` FROM `task_scope`"):
			arbitrationAt = index
		case strings.Contains(call.Query, "INSERT INTO `task_scope`"):
			t.Fatalf("a hash hit must never insert blindly: %s", call.Query)
		case strings.Contains(call.Query, "COUNT(*)"):
			t.Fatalf("hash lookup must resolve the candidate physical PK, not just count rows: %s", call.Query)
		}
	}
	if lockAt < 0 {
		t.Fatal("hash hit never took a FOR UPDATE lock on the candidate physical PK")
	}
	if arbitrationAt < 0 {
		t.Fatal("hash hit never ran VerifyHashArbitration over the full identity")
	}
	if lockAt > arbitrationAt {
		t.Fatalf("arbitration (statement %d) must run after the PK lock (statement %d)", arbitrationAt, lockAt)
	}
}

// TestTaskArchive_InterceptOccupiedPrimaryKeyIsReassigned 让 intercept_pending 的旧
// 物理键探测返回"已被占用"：恢复必须摘掉旧 id、由 TiDB 重新分配，绝不能把冷包里的
// 旧主键插回别人的行上。
func TestTaskArchive_InterceptOccupiedPrimaryKeyIsReassigned(t *testing.T) {
	state := newStubState(
		// 主键占用探测：返回 1 行，表示 id=17 已经被无关任务占走。
		stubRule{match: stubMatch("COUNT(*) FROM `intercept_pending`"), rows: stubRowsAlways(1)},
	)
	d := openStubDB(t, state)
	tx, err := d.Begin()
	if err != nil {
		t.Fatalf("begin stub tx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	raw := json.RawMessage(`[{"id":17,"status":"approved","decision_source":"rule","reason":"fixture"}]`)
	if err := restoreInterceptRowsCtx(context.Background(), tx, raw); err != nil {
		t.Fatalf("restore intercept rows: %v", err)
	}
	probes := state.statements(func(call stubCall) bool {
		return strings.Contains(call.Query, "COUNT(*) FROM `intercept_pending`")
	})
	if len(probes) != 1 {
		t.Fatalf("occupancy probes issued = %d, want exactly 1", len(probes))
	}
	inserts := state.statements(func(call stubCall) bool {
		return strings.Contains(call.Query, "INSERT INTO `intercept_pending`")
	})
	if len(inserts) != 1 {
		t.Fatalf("intercept_pending inserts = %d, want exactly 1", len(inserts))
	}
	columns, _, ok := insertColumnsAndValues(inserts[0].Query)
	if !ok {
		t.Fatalf("cannot parse the emitted INSERT: %s", inserts[0].Query)
	}
	if containsColumn(columns, "id") {
		t.Fatalf("an occupied archived primary key was replayed instead of reassigned: %s", inserts[0].Query)
	}
}
