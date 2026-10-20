package db

import (
	"bytes"
	"context"
	"database/sql"
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
	snapshot, archiveID := archiveAndCompactFixture(t, fixture)

	// 压缩后 7 张表必须彻底清空，并留下自增水位用于断言"物理键被重新分配"。
	watermarks := map[string]int64{}
	for table, query := range map[string]string{
		"task_relations":             `SELECT COALESCE(MAX(row_id),0) FROM task_relations`,
		"task_asset_links":           `SELECT COALESCE(MAX(row_id),0) FROM task_asset_links`,
		"task_llm_profiles":          `SELECT COALESCE(MAX(row_id),0) FROM task_llm_profiles`,
		"exploration_edges":          `SELECT COALESCE(MAX(row_id),0) FROM exploration_edges`,
		"exploration_anchors":        `SELECT COALESCE(MAX(row_id),0) FROM exploration_anchors`,
		"side_question_sessions":     `SELECT COALESCE(MAX(row_id),0) FROM side_question_sessions`,
		"side_question_requests":     `SELECT COALESCE(MAX(ordinal),0) FROM side_question_requests`,
		"traffic_evidence_snapshots": `SELECT COALESCE(MAX(row_id),0) FROM traffic_evidence_snapshots`,
	} {
		if rows := archiveCount(t, fixture.d, query); rows != 0 {
			t.Fatalf("%s still holds %d rows after hot compaction", table, rows)
		}
		watermarks[table] = archiveScalar(t, fixture.d, query)
	}

	restored := restoreArchiveFixture(t, fixture, archiveID, snapshot)
	if restored != nil {
		t.Fatalf("restore returned warnings for a fully owned package: %v", restored)
	}
	for table, count := range archiveFixtureSevenTableCounts(t, fixture) {
		want := snapshot.DataCounts[table]
		if count != want {
			t.Fatalf("%s restored %d rows, manifest recorded %d", table, count, want)
		}
		if table == "traffic_evidence_snapshots" {
			continue
		}
		key := "row_id"
		if table == "side_question_requests" {
			key = "ordinal"
		}
		lowest := archiveScalar(t, fixture.d, fmt.Sprintf(`SELECT COALESCE(MIN(%s),0) FROM %s WHERE %s`,
			key, table, archiveFixtureSevenTableWhere(table, fixture)))
		if lowest <= watermarks[table] {
			t.Fatalf("%s reused physical key %d (watermark %d): surrogate must be reassigned by TiDB",
				table, lowest, watermarks[table])
		}
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
	// canonical identity 必须按当前口径重算：用同样的输入独立算一遍再比对字节。
	payload := schema.SideQuestionRequestsIdentity(f.sessionKey, archiveFixtureRequestGeneration, archiveFixtureRequestClient)
	digest := schema.CanonicalHash(payload)
	var requests int
	if err := f.d.QueryRow(`SELECT COUNT(*) FROM side_question_requests
WHERE session_key=? AND client_id=? AND identity=? AND identity_hash=?`,
		f.sessionKey, archiveFixtureRequestClient, payload, digest[:]).Scan(&requests); err != nil {
		t.Fatalf("restored side_question_requests identity: %v", err)
	}
	if requests != int(snapshot.DataCounts["side_question_requests"]) {
		t.Fatalf("restored side_question_requests with recomputed identity=%d, want %d", requests, snapshot.DataCounts["side_question_requests"])
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
	claimed, err := d.ClaimTaskArchiveJob(t.Context())
	if err != nil || claimed == nil || claimed.ID != archiveID || claimed.State != Restoring {
		t.Fatalf("restore claim = %+v, %v", claimed, err)
	}
	if _, err := d.RestoreTaskArchive(archiveID, snapshot, 600); err == nil {
		t.Fatal("conflicting core graph restore must be refused")
	} else {
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
}

// =====================================================================
// 既有用例迁移到 TiDB 方言
// =====================================================================

func TestTaskArchive_EndToEndRoundTrip(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB: %v", err)
	}
	defer d.Close()
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
	if _, err := d.Exec(`INSERT INTO task_llm_profiles(task_id,profile_id,position,status,last_error,exhausted_at,created_at,updated_at)
VALUES(?,0,0,'quota_exhausted','balance exhausted',?,?,?)`, task.ID, exhaustedAt, chainCreatedAt, exhaustedAt); err != nil {
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
	claimed, err := d.ClaimTaskArchiveJob(t.Context())
	if err != nil || claimed == nil || claimed.ID != archive.ID || claimed.State != Archiving {
		t.Fatalf("claim = %+v, %v", claimed, err)
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
	claimed, err = d.ClaimTaskArchiveJob(t.Context())
	if err != nil || claimed == nil || claimed.State != Restoring {
		t.Fatalf("restore claim = %+v, %v", claimed, err)
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB: %v", err)
	}
	defer d.Close()
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB: %v", err)
	}
	defer d.Close()
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
	if _, err := d.Exec(`UPDATE task_archives SET state=?,phase='snapshot_database',progress=10 WHERE id=?`, queued.ID, Archiving); err != nil {
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
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB: %v", err)
	}
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

	source, err := d.CreateTaskWithOptions(tag+" source", "archive fixture source", TaskCreateOptions{})
	if err != nil {
		t.Fatalf("create source task: %v", err)
	}
	fixture.source = source
	task, err := d.CreateTaskWithOptions(tag+" target", "archive fixture target", TaskCreateOptions{
		SourceTaskIDs: []int64{source.ID},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	fixture.task = task

	// 资产由 target 与 source 共同持有：归档压缩不会删除它，因此恢复时一定复用同一个
	// asset id，冲突用例才能稳定地撞上 uq_task_asset_links。
	domain := fmt.Sprintf("%s.example", tag)
	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: task.ID})
	if err != nil {
		t.Fatalf("upsert asset for task: %v", err)
	}
	if _, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: source.ID}); err != nil {
		t.Fatalf("co-own asset with source task: %v", err)
	}
	fixture.assetID = assetID

	store := d.Exploration(task.ExplorationID)
	fact, err := store.AddNode(KindFact, map[string]any{"summary": "archived fact", "asset_ids": []int64{assetID}}, 1, "confirmed", "worker", nil)
	if err != nil {
		t.Fatalf("add fact node: %v", err)
	}
	intent, err := store.AddNode(KindIntent, map[string]any{"summary": "archived intent"}, 2, "open", "worker", nil)
	if err != nil {
		t.Fatalf("add intent node: %v", err)
	}
	if err := store.Link(fact, archiveFixtureEdgeRel, intent); err != nil {
		t.Fatalf("link nodes: %v", err)
	}
	if err := store.Anchor(fact, assetID); err != nil {
		t.Fatalf("anchor asset: %v", err)
	}
	fixture.nodeIDs = []int64{fact, intent}

	if _, err := d.Exec(`INSERT INTO task_asset_links(task_id,asset_id,source,source_summary,source_node_id)
VALUES(?,?,?,?,?)`, task.ID, assetID, archiveFixtureLinkSource, archiveFixtureLinkSummary, fact); err != nil {
		t.Fatalf("insert task_asset_links: %v", err)
	}

	profileID, err := d.SaveProfile(&LLMProfile{Name: tag + "-profile", Format: "openai", Model: "archive-model"})
	if err != nil {
		t.Fatalf("create llm profile: %v", err)
	}
	fixture.profileID = profileID
	if _, err := d.Exec(`INSERT INTO task_llm_profiles(task_id,profile_id,position,status,last_error,exhausted_at,created_at,updated_at)
VALUES(?,0,0,'quota_exhausted','balance exhausted',?,?,?)`, task.ID, profileID,
		archiveFixtureExhaustedAt, archiveFixtureExhaustedAt.Add(-time.Hour), archiveFixtureExhaustedAt); err != nil {
		t.Fatalf("insert task_llm_profiles: %v", err)
	}

	findingID, err := d.AddFinding(task.ID, fact, tag+"-vuln", "archive finding", SeverityCritical,
		"summary", "evidence", "worker", []int64{assetID})
	if err != nil {
		t.Fatalf("add finding: %v", err)
	}
	fixture.findingID = findingID

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
	if err := d.WithEvidenceTx(context.Background(), func(tx *sql.Tx) error {
		return AddFindingTrafficTx(tx, findingID, []PreparedTrafficEvidence{{
			Ref:      TrafficRef{TrafficID: tag + "-traffic", Role: "proof", Note: "archive fixture"},
			Snapshot: snapshot,
		}})
	}); err != nil {
		t.Fatalf("bind traffic evidence: %v", err)
	}

	fixture.sessionKey = tag + "@archive"
	if _, err := d.Exec(`INSERT INTO side_question_sessions
(session_key,task_id,exploration_id,run_id,version,snapshot,generation,memory) VALUES(?,?,?,?,?,?,?,?)`,
		fixture.sessionKey, task.ID, task.ExplorationID, 0, archiveFixtureSessionVersion,
		archiveFixtureSessionSnapshot, archiveFixtureSessionGeneration, archiveFixtureSessionMemory); err != nil {
		t.Fatalf("insert side_question_sessions: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO side_question_requests
(id,session_key,generation,client_id,identity,identity_hash,question,answer,status,model,sequence,usage,context_info)
VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?,?)`, "req-"+tag, fixture.sessionKey, int64(archiveFixtureRequestGeneration),
		archiveFixtureRequestClient, requestIdentityPayload(fixture.sessionKey), requestIdentityHash(fixture.sessionKey),
		"why?", "because", "done", `{"model":"archive-model"}`, `{"calls":1}`, `{"nodes":0}`); err != nil {
		t.Fatalf("insert side_question_requests: %v", err)
	}
	if _, err := d.Exec(`INSERT INTO side_question_requests
(id,session_key,generation,client_id,identity,identity_hash,question,answer,status,model,sequence,usage,context_info)
VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?,?)`, "req2-"+tag, fixture.sessionKey, int64(archiveFixtureRequestGeneration),
		archiveFixtureRequestClient+"-2", requestIdentityPayload(fixture.sessionKey), requestIdentityHash(fixture.sessionKey),
		"again?", "again", "done", `{"model":"archive-model"}`, `{"calls":2}`, `{"nodes":1}`); err != nil {
		t.Fatalf("insert second side_question_requests: %v", err)
	}

	t.Cleanup(func() { archiveFixtureCleanup(t, fixture) })
	return fixture
}

// requestIdentityPayload 用与线上写路径同一个 canonical 口径算出 identity 载荷。
func requestIdentityPayload(sessionKey string) []byte {
	return schema.SideQuestionRequestsIdentity(sessionKey, archiveFixtureRequestGeneration, archiveFixtureRequestClient)
}

func requestIdentityHash(sessionKey string) []byte {
	sum := schema.CanonicalHash(requestIdentityPayload(sessionKey))
	return sum[:]
}

func archiveFixtureCleanup(t *testing.T, fixture *archiveSevenTableFixture) {
	t.Helper()
	d := fixture.d
	taskID, sourceID, expID := fixture.task.ID, fixture.source.ID, fixture.task.ExplorationID
	statements := []struct {
		query string
		args  []any
	}{
		{`DELETE FROM finding_traffic_bindings WHERE finding_id IN (SELECT id FROM findings WHERE task_id IN (?,?))`, []any{taskID, sourceID}},
		{`DELETE FROM traffic_evidence_snapshots WHERE id=?`, []any{fixture.snapshotID}},
		{`DELETE FROM side_question_requests WHERE session_key=?`, []any{fixture.sessionKey}},
		{`DELETE FROM side_question_sessions WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM llm_records WHERE COALESCE(task_id,'') IN (?,?)`, []any{fmt.Sprint(taskID), fmt.Sprint(sourceID)}},
		{`DELETE FROM llm_usage WHERE COALESCE(task_id,'') IN (?,?) OR exploration_id=?`, []any{fmt.Sprint(taskID), fmt.Sprint(sourceID), expID}},
		{`DELETE FROM skill_usage WHERE task_id IN (?,?) OR exploration_id=?`, []any{taskID, sourceID, expID}},
		{`DELETE FROM tool_usage WHERE task_id IN (?,?) OR exploration_id=?`, []any{taskID, sourceID, expID}},
		{`DELETE FROM task_archives WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM findings WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM task_asset_links WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM task_llm_profiles WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM task_scope WHERE task_id IN (?,?)`, []any{taskID, sourceID}},
		{`DELETE FROM task_relations WHERE task_id IN (?,?) OR source_task_id IN (?,?)`, []any{taskID, sourceID, taskID, sourceID}},
		{`DELETE FROM activity WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM task_constraints WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM exploration_anchors WHERE node_id IN (SELECT id FROM exploration_nodes WHERE exploration_id=?)`, []any{expID}},
		{`DELETE FROM exploration_edges WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM exploration_nodes WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM assets WHERE id=?`, []any{fixture.assetID}},
	}
	for _, statement := range statements {
		if _, err := d.Exec(statement.query, statement.args...); err != nil {
			t.Logf("archive fixture cleanup %q: %v", statement.query, err)
		}
	}
	if err := d.DeleteTask(taskID); err != nil {
		t.Logf("delete task: %v", err)
	}
	if err := d.DeleteTask(sourceID); err != nil {
		t.Logf("delete source task: %v", err)
	}
	if err := d.DeleteProfile(fixture.profileID); err != nil {
		t.Logf("delete profile: %v", err)
	}
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
	claimed, err := d.ClaimTaskArchiveJob(t.Context())
	if err != nil || claimed == nil || claimed.ID != archive.ID || claimed.State != Archiving {
		t.Fatalf("archive claim = %+v, %v", claimed, err)
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
	claimed, err := fixture.d.ClaimTaskArchiveJob(t.Context())
	if err != nil || claimed == nil || claimed.ID != archiveID || claimed.State != Restoring {
		t.Fatalf("restore claim = %+v, %v", claimed, err)
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
		return "node_id IN (SELECT id FROM exploration_nodes WHERE exploration_id=" + exp + ")"
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
