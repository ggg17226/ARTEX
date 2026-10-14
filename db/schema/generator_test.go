package schema

import (
	"fmt"
	"strings"
	"testing"
)

func tableByNameAll(t *testing.T, name string) *Table {
	t.Helper()
	for _, tb := range AllTables() {
		if tb.Name == name {
			return tb
		}
	}
	t.Fatalf("table %s not in AllTables()", name)
	return nil
}

func genOrFatalAll(t *testing.T, name string) string {
	t.Helper()
	tb := tableByNameAll(t, name)
	s, err := GenerateDDL(tb)
	if err != nil {
		t.Fatalf("GenerateDDL(%s): %v", name, err)
	}
	return s
}

func TestGenerateDDLCompanies(t *testing.T) {
	got := genOrFatalAll(t, "companies")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `companies` (",
		"  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,",
		"  `name` MEDIUMTEXT NOT NULL DEFAULT '',",
		"  `nkey` MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',",
		"  `nkey_identity` MEDIUMBLOB NOT NULL,",
		"  `nkey_hash` BINARY(32) NOT NULL,",
		"  `logo` MEDIUMTEXT,",
		"  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),",
		"  `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),",
		"  PRIMARY KEY (`id`),",
		"  UNIQUE KEY `uq_companies_nkey_hash` (`nkey_hash`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE=1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("GenerateDDL(companies) missing %q\n%s", want, got)
		}
	}
	for _, ban := range []string{"FOREIGN KEY", "REFERENCES", "CHECK (", "AUTO_RANDOM", "CREATE DATABASE"} {
		if strings.Contains(got, ban) {
			t.Errorf("GenerateDDL(companies) must not contain %q", ban)
		}
	}
	if strings.Contains(got, ";") {
		t.Errorf("GenerateDDL(companies) must not contain semicolons:\n%s", got)
	}
}

func TestGenerateDDLProfileHealthNotAutoInc(t *testing.T) {
	got := genOrFatalAll(t, "llm_profile_health")
	if !strings.Contains(got, "`profile_id` BIGINT UNSIGNED NOT NULL,") {
		t.Errorf("llm_profile_health: profile_id must be plain BIGINT UNSIGNED NOT NULL\n%s", got)
	}
	if strings.Contains(got, "AUTO_INCREMENT") {
		t.Errorf("llm_profile_health must not contain AUTO_INCREMENT\n%s", got)
	}
	if strings.Contains(got, "AUTO_ID_CACHE") {
		t.Errorf("llm_profile_health must not contain AUTO_ID_CACHE\n%s", got)
	}
	if !strings.Contains(got, "PRIMARY KEY (`profile_id`)") {
		t.Errorf("llm_profile_health: missing PRIMARY KEY (`profile_id`)\n%s", got)
	}
	wantTail := ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	if !strings.HasSuffix(got, wantTail) {
		t.Errorf("llm_profile_health: table tail = %q, want suffix %q", got, wantTail)
	}
}

func TestGenerateDDLRowIDTable(t *testing.T) {
	got := genOrFatalAll(t, "locks")
	if !strings.Contains(got, "`row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,") {
		t.Errorf("locks: row_id must be AUTO_INCREMENT without column-level cache\n%s", got)
	}
	if !strings.Contains(got, "PRIMARY KEY (`row_id`)") {
		t.Errorf("locks: missing PRIMARY KEY (`row_id`)\n%s", got)
	}
	wantTail := ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE=1"
	if !strings.HasSuffix(got, wantTail) {
		t.Errorf("locks: table tail = %q, want suffix %q", got, wantTail)
	}
}

func TestGenerateDDLGeneratedColumns(t *testing.T) {
	got := genOrFatalAll(t, "llm_profiles")
	if !strings.Contains(got, "`is_default_key` TINYINT GENERATED ALWAYS AS (IF(is_default,1,NULL)) STORED,") {
		t.Errorf("llm_profiles: missing generated column is_default_key\n%s", got)
	}
	got = genOrFatalAll(t, "finding_retests")
	if !strings.Contains(got, "`active_finding_id` BIGINT UNSIGNED GENERATED ALWAYS AS (IF(status IN ('pending','running'), finding_id, NULL)) STORED,") {
		t.Errorf("finding_retests: missing generated column active_finding_id\n%s", got)
	}
	got = genOrFatalAll(t, "side_question_requests")
	if !strings.Contains(got, "`running_session_key` VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin GENERATED ALWAYS AS (IF(status='running', session_key, NULL)) STORED,") {
		t.Errorf("side_question_requests: missing ascii_bin generated column running_session_key\n%s", got)
	}
	if !strings.Contains(got, "KEY `idx_side_requests_history` (`session_key`, `ordinal` DESC)") {
		t.Errorf("side_question_requests: missing DESC index part in idx_side_requests_history\n%s", got)
	}
}

func TestGenerateDDLExactContracts(t *testing.T) {
	// 1. traffic_exchange_bodies: BLOB columns without charset, nullable, no extra NOT NULL
	gotBodies := genOrFatalAll(t, "traffic_exchange_bodies")
	if !strings.Contains(gotBodies, "`req_body` MEDIUMBLOB,") {
		t.Errorf("traffic_exchange_bodies missing req_body MEDIUMBLOB nullable:\n%s", gotBodies)
	}
	if !strings.Contains(gotBodies, "`resp_body` MEDIUMBLOB,") {
		t.Errorf("traffic_exchange_bodies missing resp_body MEDIUMBLOB nullable:\n%s", gotBodies)
	}
	if strings.Contains(gotBodies, "`req_body` MEDIUMBLOB CHARACTER SET") {
		t.Errorf("traffic_exchange_bodies req_body must not contain CHARACTER SET:\n%s", gotBodies)
	}

	// 2. assets: index prefix PP("domain", 191) and PD("last_seen") DESC index
	gotAssets := genOrFatalAll(t, "assets")
	if !strings.Contains(gotAssets, "KEY `idx_assets_domain` (`domain`(191))") {
		t.Errorf("assets missing idx_assets_domain (`domain`(191)):\n%s", gotAssets)
	}
	if !strings.Contains(gotAssets, "KEY `idx_assets_last_seen` (`last_seen` DESC)") {
		t.Errorf("assets missing idx_assets_last_seen (`last_seen` DESC):\n%s", gotAssets)
	}

	// 3. storage_job_pins: size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0
	gotPins := genOrFatalAll(t, "storage_job_pins")
	if !strings.Contains(gotPins, "`size_bytes` BIGINT UNSIGNED NOT NULL DEFAULT 0,") {
		t.Errorf("storage_job_pins missing size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0:\n%s", gotPins)
	}

	// 4. llm_profiles: name_hash BINARY(32), name_identity MEDIUMBLOB
	gotLLM := genOrFatalAll(t, "llm_profiles")
	if !strings.Contains(gotLLM, "`name_identity` MEDIUMBLOB NOT NULL,") {
		t.Errorf("llm_profiles missing name_identity MEDIUMBLOB NOT NULL:\n%s", gotLLM)
	}
	if !strings.Contains(gotLLM, "`name_hash` BINARY(32) NOT NULL,") {
		t.Errorf("llm_profiles missing name_hash BINARY(32) NOT NULL:\n%s", gotLLM)
	}
}

// splitTableParts splits DDL by the table closing bracket `\n) ENGINE=`.
// It returns (columnsAndIndexesSection, tableOptionsSection, error).
func splitTableParts(ddl string) (string, string, error) {
	sep := "\n) ENGINE="
	idx := strings.Index(ddl, sep)
	if idx < 0 {
		return "", "", fmt.Errorf("separator %q not found in DDL:\n%s", sep, ddl)
	}
	colPart := ddl[:idx]
	optPart := ddl[idx+len("\n)"):] // starts with " ENGINE="
	return colPart, optPart, nil
}

func TestGenerateDDLAll65CachePlacement(t *testing.T) {
	all := AllTables()
	if len(all) != 65 {
		t.Fatalf("AllTables count = %d, want 65", len(all))
	}

	totalPKAutoInc := 0
	totalTableOptionCache := 0

	for _, tbl := range all {
		ddl, err := GenerateDDL(tbl)
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", tbl.Name, err)
		}

		colPart, optPart, err := splitTableParts(ddl)
		if err != nil {
			t.Fatalf("table %s: %v", tbl.Name, err)
		}

		// 1. Column & index area must NEVER contain AUTO_ID_CACHE
		if strings.Contains(colPart, "AUTO_ID_CACHE") {
			t.Errorf("table %s has AUTO_ID_CACHE in columns/indexes section:\n%s", tbl.Name, colPart)
		}

		// 2. PK column line checks
		pkPattern := fmt.Sprintf("`%s` BIGINT UNSIGNED NOT NULL", tbl.PKColumn)
		if !strings.Contains(colPart, pkPattern) {
			t.Errorf("table %s colPart missing PK pattern %q", tbl.Name, pkPattern)
		}

		colAutoCount := strings.Count(colPart, "AUTO_INCREMENT")
		optCacheCount := strings.Count(optPart, "AUTO_ID_CACHE=1")

		if tbl.AutoIncrement {
			// Auto-increment table: exactly 1 AUTO_INCREMENT in columns section, on the PK column
			if colAutoCount != 1 {
				t.Errorf("table %s: colPart AUTO_INCREMENT count = %d, want 1", tbl.Name, colAutoCount)
			}
			pkAutoPattern := fmt.Sprintf("`%s` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT", tbl.PKColumn)
			if !strings.Contains(colPart, pkAutoPattern) {
				t.Errorf("table %s: PK column %q missing AUTO_INCREMENT", tbl.Name, tbl.PKColumn)
			}
			// Exactly 1 AUTO_ID_CACHE=1 in table options
			if optCacheCount != 1 {
				t.Errorf("table %s: optPart AUTO_ID_CACHE=1 count = %d, want 1", tbl.Name, optCacheCount)
			}
			wantTail := ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE=1"
			if !strings.HasSuffix(ddl, wantTail) {
				t.Errorf("table %s: DDL tail mismatch, want suffix %q\n%s", tbl.Name, wantTail, ddl)
			}
			totalPKAutoInc++
			totalTableOptionCache++
		} else {
			// Non-auto-increment table (llm_profile_health): 0 AUTO_INCREMENT, 0 AUTO_ID_CACHE
			if colAutoCount != 0 {
				t.Errorf("table %s (non-auto): colPart AUTO_INCREMENT count = %d, want 0", tbl.Name, colAutoCount)
			}
			if optCacheCount != 0 {
				t.Errorf("table %s (non-auto): optPart AUTO_ID_CACHE=1 count = %d, want 0", tbl.Name, optCacheCount)
			}
			wantTail := ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
			if !strings.HasSuffix(ddl, wantTail) {
				t.Errorf("table %s (non-auto): DDL tail mismatch, want suffix %q\n%s", tbl.Name, wantTail, ddl)
			}
		}
	}

	if totalPKAutoInc != 64 {
		t.Errorf("totalPKAutoInc = %d, want 64", totalPKAutoInc)
	}
	if totalTableOptionCache != 64 {
		t.Errorf("totalTableOptionCache = %d, want 64", totalTableOptionCache)
	}
}

func TestGenerateAllDDL(t *testing.T) {
	script, err := GenerateAllDDL()
	if err != nil {
		t.Fatalf("GenerateAllDDL: %v", err)
	}
	if n := strings.Count(script, "CREATE TABLE IF NOT EXISTS"); n != 65 {
		t.Errorf("script CREATE TABLE count = %d, want 65", n)
	}
	if n := strings.Count(script, "AUTO_INCREMENT"); n != 64 {
		t.Errorf("script AUTO_INCREMENT count = %d, want 64", n)
	}
	if n := strings.Count(script, "AUTO_ID_CACHE=1"); n != 64 {
		t.Errorf("script AUTO_ID_CACHE=1 count = %d, want 64", n)
	}
	// Column-level cache must not exist anywhere
	if strings.Contains(script, "AUTO_INCREMENT AUTO_ID_CACHE") {
		t.Errorf("script must not contain column-level 'AUTO_INCREMENT AUTO_ID_CACHE'")
	}
	if strings.Contains(script, "CREATE DATABASE") {
		t.Error("script must not contain CREATE DATABASE")
	}
	if !strings.HasSuffix(script, ";\n") {
		t.Error("script must end with ;\\n")
	}
}

func TestSplitStatements(t *testing.T) {
	stmts, err := GenerateStatements()
	if err != nil {
		t.Fatalf("GenerateStatements: %v", err)
	}
	if len(stmts) != 65 {
		t.Fatalf("GenerateStatements() = %d, want 65", len(stmts))
	}
	script, err := GenerateAllDDL()
	if err != nil {
		t.Fatalf("GenerateAllDDL: %v", err)
	}
	parts := SplitStatements(script)
	if len(parts) != 65 {
		t.Fatalf("SplitStatements() = %d, want 65", len(parts))
	}
	for i := range stmts {
		if parts[i] != stmts[i] {
			t.Fatalf("statement %d differs between GenerateStatements and SplitStatements:\ngot:  %s\nwant: %s", i, parts[i], stmts[i])
		}
	}
}

func TestGenerateDDLValidationRejection(t *testing.T) {
	// Table with invalid structure (no PK) must fail Validate() and not return SQL
	invalidTbl := &Table{
		Name:     "invalid_no_pk",
		PKColumn: "id",
		Columns: []Column{
			{Name: "data", Type: TText},
		},
	}
	sql, err := GenerateDDL(invalidTbl)
	if err == nil {
		t.Fatalf("GenerateDDL(invalid_no_pk) expected error, got nil, sql=%s", sql)
	}
}

// ---------------------------------------------------------------------------
// Task 6 Acceptance Matrix
// ---------------------------------------------------------------------------

// wantOrdered65 defines the exact ordered 65 table names:
// 49 baseline tables (schema.sql appearance order, 1-49) followed by
// 16 extension/traffic/infrastructure tables (50-65).
var wantOrdered65 = []string{
	// 49 Baseline tables
	"companies", "assets", "company_scope", "explorations", "exploration_nodes",
	"exploration_edges", "exploration_anchors", "task_constraints", "activity", "main_sessions",
	"settings", "llm_profiles", "llm_profile_health", "task_categories", "tasks",
	"task_archives", "task_templates", "task_relations", "task_asset_links", "task_llm_profiles",
	"task_scope", "agents", "agent_prompts", "agent_prompt_vars", "mcp_servers",
	"mcp_tools_cache", "agent_visibility", "agent_skill_visibility", "skill_usage", "tool_usage",
	"tools", "conversations", "conversation_activities", "agent_triggers", "scheduler_state",
	"intercept_rules", "intercept_pending", "findings", "finding_retests", "traffic_evidence_snapshots",
	"finding_traffic_bindings", "server_logs", "side_question_sessions", "side_question_requests", "asset_intercept_rules",
	"task_intercept_rules", "notification_channels", "notification_events", "notification_deliveries",
	// 16 Extension / traffic / infrastructure tables
	"llm_records", "llm_usage", "finding_assets", "traffic_exchanges", "traffic_exchange_bodies",
	"traffic_blob_refs", "traffic_index_chunks", "traffic_stores", "locks", "schema_meta",
	"business_deployment", "maintenance", "storage_jobs", "storage_id_map", "storage_job_pins",
	"op_outcomes",
}

// 1. TestCatalog65Tables
func TestCatalog65Tables(t *testing.T) {
	all := AllTables()
	if len(all) != 65 {
		t.Fatalf("AllTables() length = %d, want 65", len(all))
	}
	if len(wantOrdered65) != 65 {
		t.Fatalf("wantOrdered65 length = %d, want 65", len(wantOrdered65))
	}

	seen := make(map[string]bool)
	for i, tb := range all {
		if tb == nil {
			t.Fatalf("table at index %d is nil", i)
		}
		if seen[tb.Name] {
			t.Fatalf("duplicate table %q encountered at index %d", tb.Name, i)
		}
		seen[tb.Name] = true

		if i < len(wantOrdered65) && tb.Name != wantOrdered65[i] {
			t.Errorf("table at index %d = %q, want %q", i, tb.Name, wantOrdered65[i])
		}

		if err := tb.Validate(); err != nil {
			t.Errorf("table %s: Validate() failed: %v", tb.Name, err)
		}
	}

	// Verify all expected names were seen
	for _, wantName := range wantOrdered65 {
		if !seen[wantName] {
			t.Errorf("missing expected table %q from AllTables()", wantName)
		}
	}

	// Explicit 49/16 boundary check: notification_deliveries(48) -> llm_records(49) in 0-indexed slice
	if all[48].Name != "notification_deliveries" {
		t.Errorf("table at boundary index 48 = %q, want notification_deliveries", all[48].Name)
	}
	if all[49].Name != "llm_records" {
		t.Errorf("table at boundary index 49 = %q, want llm_records", all[49].Name)
	}
}

// 2. TestCatalogPKClassification
func TestCatalogPKClassification(t *testing.T) {
	pkCounts := map[string]int{}
	autoIncCount := 0
	var nonAutoIncTables []string

	for _, tb := range AllTables() {
		pkCounts[tb.PKColumn]++
		if tb.AutoIncrement {
			autoIncCount++
		} else {
			nonAutoIncTables = append(nonAutoIncTables, tb.Name)
		}

		// Every table must have exactly one PK column which is BIGINT UNSIGNED NOT NULL
		foundPK := false
		for _, c := range tb.Columns {
			if c.Name == tb.PKColumn {
				foundPK = true
				if c.Type != TBigUnsigned {
					t.Errorf("table %s: PK column %s type = %v, want TBigUnsigned", tb.Name, c.Name, c.Type)
				}
				if !c.NotNull {
					t.Errorf("table %s: PK column %s must be NOT NULL", tb.Name, c.Name)
				}
			}
		}
		if !foundPK {
			t.Errorf("table %s: PK column %s not found in columns", tb.Name, tb.PKColumn)
		}
	}

	if pkCounts["id"] != 37 || pkCounts["ordinal"] != 1 || pkCounts["profile_id"] != 1 || pkCounts["row_id"] != 26 {
		t.Errorf("PK classification = %+v, want id=37, ordinal=1, profile_id=1, row_id=26", pkCounts)
	}
	if autoIncCount != 64 {
		t.Errorf("AutoIncrement tables count = %d, want 64", autoIncCount)
	}
	if len(nonAutoIncTables) != 1 || nonAutoIncTables[0] != "llm_profile_health" {
		t.Errorf("non-auto-increment tables = %v, want exactly [llm_profile_health]", nonAutoIncTables)
	}
}

// 3. TestCatalogRowIDHidden
func TestCatalogRowIDHidden(t *testing.T) {
	rowIDTableCount := 0
	for _, tb := range AllTables() {
		if tb.PKColumn != "row_id" {
			continue
		}
		rowIDTableCount++
		for _, ix := range tb.Indexes {
			for _, p := range ix.Parts {
				if p.Column == "row_id" {
					t.Errorf("table %s index %s (unique=%v) must not contain hidden row_id", tb.Name, ix.Name, ix.Unique)
				}
			}
		}
	}
	if rowIDTableCount != 26 {
		t.Errorf("hidden row_id table count = %d, want 26", rowIDTableCount)
	}
}

// 4. TestDDLNoFKNoCheckNoAutoRandom
func TestDDLNoFKNoCheckNoAutoRandom(t *testing.T) {
	stmts, err := GenerateStatements()
	if err != nil {
		t.Fatalf("GenerateStatements: %v", err)
	}
	if len(stmts) != 65 {
		t.Fatalf("GenerateStatements length = %d, want 65", len(stmts))
	}

	bannedList := []string{"FOREIGN KEY", "REFERENCES", "CHECK (", "AUTO_RANDOM", "CREATE DATABASE"}
	for i, stmt := range stmts {
		for _, ban := range bannedList {
			if strings.Contains(stmt, ban) {
				t.Errorf("statement %d (%s...): contains banned keyword %q", i, stmt[:min(40, len(stmt))], ban)
			}
		}
	}
}

// 5. TestDDLIndexBudget
func TestDDLIndexBudget(t *testing.T) {
	var maxObservedBudget int
	var maxIndexName string
	var maxTableName string

	for _, tb := range AllTables() {
		colMap := make(map[string]Column, len(tb.Columns))
		for _, c := range tb.Columns {
			colMap[c.Name] = c
		}

		// 1. Physical PK column budget (0 < bytes <= 3072)
		pkCol, ok := colMap[tb.PKColumn]
		if !ok {
			t.Errorf("table %s: PK column %q missing from columns map", tb.Name, tb.PKColumn)
			continue
		}
		pkBytes := pkCol.KeyBytes(0)
		if pkBytes <= 0 || pkBytes > 3072 {
			t.Errorf("table %s: PK key bytes = %d, want (0, 3072]", tb.Name, pkBytes)
		}

		// 2. All secondary indexes budget (0 <= bytes <= 3072, parts resolvable)
		for _, ix := range tb.Indexes {
			b := IndexBytes(tb, ix)
			if b < 0 {
				t.Errorf("table %s index %s: unresolvable column or unbounded part (IndexBytes=%d)", tb.Name, ix.Name, b)
				continue
			}
			if b > 3072 {
				t.Errorf("table %s index %s: byte width %d exceeds budget 3072", tb.Name, ix.Name, b)
			}
			if b > maxObservedBudget {
				maxObservedBudget = b
				maxIndexName = ix.Name
				maxTableName = tb.Name
			}
		}
	}

	// Known maximum index budget across catalog: traffic_exchanges.idx_traffic_exchanges_tmpl = 1564
	// (namespace CHAR(36) ascii = 36 + host(191)*4 = 764 + url_template(191)*4 = 764 => 1564)
	if maxObservedBudget != 1564 {
		t.Errorf("maxObservedBudget = %d (table=%s, index=%s), want 1564", maxObservedBudget, maxTableName, maxIndexName)
	}
	if maxTableName != "traffic_exchanges" || maxIndexName != "idx_traffic_exchanges_tmpl" {
		t.Errorf("expected max budget index to be traffic_exchanges.idx_traffic_exchanges_tmpl, got %s.%s", maxTableName, maxIndexName)
	}
}

// 6. TestDDLAsciiBinIdentity
func TestDDLAsciiBinIdentity(t *testing.T) {
	// All bounded ASCII identity / reference / snapshot columns in the catalog:
	asciiColumns := []struct {
		table    string
		col      string
		nullable bool
	}{
		// Baseline tables
		{"exploration_edges", "rel", false},
		{"traffic_evidence_snapshots", "id", false},
		{"traffic_evidence_snapshots", "req_hash", false},
		{"traffic_evidence_snapshots", "resp_hash", false},
		{"finding_traffic_bindings", "snapshot_id", false},
		{"side_question_sessions", "session_key", false},
		{"side_question_requests", "id", false},
		{"side_question_requests", "session_key", false},
		{"side_question_requests", "running_session_key", true}, // nullable generated column

		// Extension / traffic tables
		{"traffic_exchanges", "namespace", false},
		{"traffic_exchanges", "id", false},
		{"traffic_exchange_bodies", "namespace", false},
		{"traffic_exchange_bodies", "exchange_id", false},
		{"traffic_exchange_bodies", "req_blob", true},  // nullable
		{"traffic_exchange_bodies", "resp_blob", true}, // nullable
		{"traffic_blob_refs", "namespace", false},
		{"traffic_blob_refs", "hash", false},
		{"traffic_blob_refs", "exchange_id", false},
		{"traffic_index_chunks", "namespace", false},
		{"traffic_index_chunks", "exchange_id", false},
		{"traffic_stores", "namespace", false},
		{"traffic_stores", "role", false},
		{"traffic_stores", "relative_dir", false},

		// Infrastructure tables
		{"locks", "name", false},
		{"schema_meta", "key", false},
		{"business_deployment", "deployment_id", false},
		{"maintenance", "singleton_key", false},
	}

	byName := make(map[string]*Table)
	for _, tb := range AllTables() {
		byName[tb.Name] = tb
	}

	for _, tc := range asciiColumns {
		tb := byName[tc.table]
		if tb == nil {
			t.Fatalf("table %s not found in AllTables()", tc.table)
		}
		var col *Column
		for i := range tb.Columns {
			if tb.Columns[i].Name == tc.col {
				col = &tb.Columns[i]
				break
			}
		}
		if col == nil {
			t.Fatalf("column %s.%s not found", tc.table, tc.col)
		}

		// 1. Catalog property check
		if col.Charset != "ascii" || col.Collation != "ascii_bin" {
			t.Errorf("%s.%s: want Charset=ascii Collation=ascii_bin, got Charset=%q Collation=%q",
				tc.table, tc.col, col.Charset, col.Collation)
		}
		if col.NotNull == tc.nullable {
			t.Errorf("%s.%s: NotNull = %v, want %v", tc.table, tc.col, col.NotNull, !tc.nullable)
		}

		// 2. Generated DDL string check
		ddl, err := GenerateDDL(tb)
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", tc.table, err)
		}
		expectedPattern := fmt.Sprintf("`%s` %s", col.Name, col.SQLType())
		if !strings.Contains(ddl, expectedPattern) {
			t.Errorf("%s: DDL missing column pattern %q\n%s", tc.table, expectedPattern, ddl)
		}
		if !strings.Contains(expectedPattern, "CHARACTER SET ascii COLLATE ascii_bin") {
			t.Errorf("%s.%s: SQLType() missing ascii_bin: %q", tc.table, tc.col, expectedPattern)
		}
	}

	// 3. Verify BINARY and BLOB columns in catalog do NOT have Charset or Collation
	for _, tb := range AllTables() {
		for _, c := range tb.Columns {
			if c.Type == TBinary || c.Type == TVarbinary || c.Type == TMediumBlob {
				if c.Charset != "" || c.Collation != "" {
					t.Errorf("binary column %s.%s must not have charset/collation, got Charset=%q Collation=%q",
						tb.Name, c.Name, c.Charset, c.Collation)
				}
			}
		}
	}

	// 4. Verify the 33 revised identity text columns (32 Unicode + 1 archive SHA256) retain exact contracts
	// and are rendered with explicit CHARACTER SET and COLLATE in DDL
	for _, exp := range target33Manifest {
		tb := byName[exp.Table]
		if tb == nil {
			t.Fatalf("manifest table %s not found", exp.Table)
		}
		ddl, err := GenerateDDL(tb)
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", exp.Table, err)
		}
		if !strings.Contains(ddl, exp.SQLContains) {
			t.Errorf("table %s DDL missing expected manifest substring %q\n%s", exp.Table, exp.SQLContains, ddl)
		}
	}
}

// 7. TestDDLGeneratedColumns
func TestDDLGeneratedColumns(t *testing.T) {
	// Case 1: llm_profiles.is_default_key
	ddlLLM := genOrFatalAll(t, "llm_profiles")
	wantColLLM := "`is_default_key` TINYINT GENERATED ALWAYS AS (IF(is_default,1,NULL)) STORED,"
	wantIdxLLM := "UNIQUE KEY `uq_llm_one_default` (`is_default_key`)"
	if !strings.Contains(ddlLLM, wantColLLM) {
		t.Errorf("llm_profiles: missing generated column\nwant: %s\ngot:\n%s", wantColLLM, ddlLLM)
	}
	if !strings.Contains(ddlLLM, wantIdxLLM) {
		t.Errorf("llm_profiles: missing unique index\nwant: %s\ngot:\n%s", wantIdxLLM, ddlLLM)
	}
	// llm_profiles.name must NOT be part of any UNIQUE index or have prefix index
	if strings.Contains(ddlLLM, "UNIQUE KEY `uq_llm_profiles_name`") || strings.Contains(ddlLLM, "UNIQUE KEY `uq_llm_name`") {
		t.Errorf("llm_profiles must not have unique index on name:\n%s", ddlLLM)
	}

	// Case 2: finding_retests.active_finding_id
	ddlRetests := genOrFatalAll(t, "finding_retests")
	wantColRetests := "`active_finding_id` BIGINT UNSIGNED GENERATED ALWAYS AS (IF(status IN ('pending','running'), finding_id, NULL)) STORED,"
	wantIdxRetests := "UNIQUE KEY `uq_finding_retests_active` (`active_finding_id`)"
	if !strings.Contains(ddlRetests, wantColRetests) {
		t.Errorf("finding_retests: missing generated column\nwant: %s\ngot:\n%s", wantColRetests, ddlRetests)
	}
	if !strings.Contains(ddlRetests, wantIdxRetests) {
		t.Errorf("finding_retests: missing unique index\nwant: %s\ngot:\n%s", wantIdxRetests, ddlRetests)
	}

	// Case 3: side_question_requests.running_session_key
	ddlSide := genOrFatalAll(t, "side_question_requests")
	wantColSide := "`running_session_key` VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin GENERATED ALWAYS AS (IF(status='running', session_key, NULL)) STORED,"
	wantIdxSide := "UNIQUE KEY `uq_side_requests_running` (`running_session_key`)"
	if !strings.Contains(ddlSide, wantColSide) {
		t.Errorf("side_question_requests: missing generated column\nwant: %s\ngot:\n%s", wantColSide, ddlSide)
	}
	if !strings.Contains(ddlSide, wantIdxSide) {
		t.Errorf("side_question_requests: missing unique index\nwant: %s\ngot:\n%s", wantIdxSide, ddlSide)
	}
}

// Helper checking single key hash pair for a table
func checkSingleKeyHashPair(t *testing.T, tb *Table, identCol, hashCol, uqIndexName string) {
	t.Helper()
	var foundIdent, foundHash bool
	for _, c := range tb.Columns {
		if c.Name == identCol {
			foundIdent = true
			if c.Type != TMediumBlob || !c.NotNull || c.Generated != "" || c.Charset != "" || c.Collation != "" {
				t.Errorf("table %s column %s: want MEDIUMBLOB NOT NULL non-generated no-charset, got type=%v notNull=%v gen=%q charset=%q",
					tb.Name, identCol, c.Type, c.NotNull, c.Generated, c.Charset)
			}
		}
		if c.Name == hashCol {
			foundHash = true
			if c.Type != TBinary || c.Length != 32 || !c.NotNull || c.Generated != "" || c.Charset != "" || c.Collation != "" {
				t.Errorf("table %s column %s: want BINARY(32) NOT NULL non-generated no-charset, got type=%v len=%d notNull=%v gen=%q",
					tb.Name, hashCol, c.Type, c.Length, c.NotNull, c.Generated)
			}
		}
	}
	if !foundIdent {
		t.Errorf("table %s: missing identity column %q", tb.Name, identCol)
	}
	if !foundHash {
		t.Errorf("table %s: missing hash column %q", tb.Name, hashCol)
	}

	var foundUQ bool
	for _, ix := range tb.Indexes {
		if ix.Name == uqIndexName {
			foundUQ = true
			if !ix.Unique {
				t.Errorf("table %s index %s: must be Unique", tb.Name, uqIndexName)
			}
			if len(ix.Parts) != 1 || ix.Parts[0].Column != hashCol || ix.Parts[0].Prefix != 0 || ix.Parts[0].Desc {
				t.Errorf("table %s index %s: parts = %+v, want exactly 1 part on %s (prefix=0, desc=false)",
					tb.Name, uqIndexName, ix.Parts, hashCol)
			}
		}
	}
	if !foundUQ {
		t.Errorf("table %s: missing UNIQUE index %q", tb.Name, uqIndexName)
	}

	// Verify generated DDL contains column definitions and UNIQUE key
	ddl, err := GenerateDDL(tb)
	if err != nil {
		t.Fatalf("GenerateDDL(%s): %v", tb.Name, err)
	}
	wantIdentDDL := fmt.Sprintf("`%s` MEDIUMBLOB NOT NULL,", identCol)
	wantHashDDL := fmt.Sprintf("`%s` BINARY(32) NOT NULL,", hashCol)
	wantUQDDL := fmt.Sprintf("UNIQUE KEY `%s` (`%s`)", uqIndexName, hashCol)
	if !strings.Contains(ddl, wantIdentDDL) {
		t.Errorf("table %s DDL missing %q\n%s", tb.Name, wantIdentDDL, ddl)
	}
	if !strings.Contains(ddl, wantHashDDL) {
		t.Errorf("table %s DDL missing %q\n%s", tb.Name, wantHashDDL, ddl)
	}
	if !strings.Contains(ddl, wantUQDDL) {
		t.Errorf("table %s DDL missing %q\n%s", tb.Name, wantUQDDL, ddl)
	}
}

// 8. TestDDLHashDDLLayout
func TestDDLHashDDLLayout(t *testing.T) {
	byName := make(map[string]*Table)
	for _, tb := range AllTables() {
		byName[tb.Name] = tb
	}

	// 15 Ordinary single-key hash DDL tables:
	// 13 mapped in HashIdentityColumns + task_scope + side_question_requests
	singleKeyTables := []struct {
		table    string
		identCol string
		hashCol  string
		uqIndex  string
	}{
		{"companies", "nkey_identity", "nkey_hash", "uq_companies_nkey_hash"},
		{"task_categories", "nkey_identity", "nkey_hash", "uq_task_categories_nkey_hash"},
		{"task_templates", "nkey_identity", "nkey_hash", "uq_task_templates_nkey_hash"},
		{"agents", "key_identity", "key_hash", "uq_agents_key_hash"},
		{"mcp_servers", "name_identity", "name_hash", "uq_mcp_servers_name_hash"},
		{"settings", "key_identity", "key_hash", "uq_settings_key_hash"},
		{"tools", "key_identity", "key_hash", "uq_tools_key_hash"},
		{"scheduler_state", "key_identity", "key_hash", "uq_scheduler_state_key_hash"},
		{"agent_prompt_vars", "identity", "identity_hash", "uq_agent_prompt_vars_identity"},
		{"mcp_tools_cache", "identity", "identity_hash", "uq_mcp_tools_cache_identity"},
		{"agent_visibility", "identity", "identity_hash", "uq_agent_visibility_identity"},
		{"agent_skill_visibility", "identity", "identity_hash", "uq_agent_skill_visibility_identity"},
		{"llm_profiles", "name_identity", "name_hash", "uq_llm_profiles_name_hash"},
		{"task_scope", "identity", "identity_hash", "uq_task_scope_identity"},
		{"side_question_requests", "identity", "identity_hash", "uq_side_requests_identity"},
	}

	if len(singleKeyTables) != 15 {
		t.Fatalf("singleKeyTables count = %d, want 15", len(singleKeyTables))
	}

	for _, item := range singleKeyTables {
		tb := byName[item.table]
		if tb == nil {
			t.Fatalf("table %s not found in AllTables()", item.table)
		}
		checkSingleKeyHashPair(t, tb, item.identCol, item.hashCol, item.uqIndex)
	}

	// 2 Branch tables: assets (8 branch pairs) and company_scope (3 branch pairs)
	// assets: 8 pairs
	assetsBranchPairs := []struct {
		identCol string
		hashCol  string
		uqIndex  string
	}{
		{"identity_root_domain", "hash_root_domain", "uq_assets_hash_root_domain"},
		{"identity_ip", "hash_ip", "uq_assets_hash_ip"},
		{"identity_subdomain", "hash_subdomain", "uq_assets_hash_subdomain"},
		{"identity_app_bundle", "hash_app_bundle", "uq_assets_hash_app_bundle"},
		{"identity_app_name", "hash_app_name", "uq_assets_hash_app_name"},
		{"identity_service_http", "hash_service_http", "uq_assets_hash_service_http"},
		{"identity_service_other", "hash_service_other", "uq_assets_hash_service_other"},
		{"identity_endpoint", "hash_endpoint", "uq_assets_hash_endpoint"},
	}
	tbAssets := byName["assets"]
	if tbAssets == nil {
		t.Fatal("assets table missing")
	}
	ddlAssets, err := GenerateDDL(tbAssets)
	if err != nil {
		t.Fatalf("GenerateDDL(assets): %v", err)
	}
	for _, pair := range assetsBranchPairs {
		cIdent := findColumn(tbAssets, pair.identCol)
		if cIdent == nil || cIdent.Type != TMediumBlob || cIdent.NotNull {
			t.Errorf("assets.%s: want nullable MEDIUMBLOB, got %+v", pair.identCol, cIdent)
		}
		cHash := findColumn(tbAssets, pair.hashCol)
		if cHash == nil || cHash.Type != TBinary || cHash.Length != 32 || cHash.NotNull {
			t.Errorf("assets.%s: want nullable BINARY(32), got %+v", pair.hashCol, cHash)
		}
		wantUQ := fmt.Sprintf("UNIQUE KEY `%s` (`%s`)", pair.uqIndex, pair.hashCol)
		if !strings.Contains(ddlAssets, wantUQ) {
			t.Errorf("assets DDL missing %q", wantUQ)
		}
		// Full rendered column-line boundary check: the identity line must be
		// exactly "  `<col>` MEDIUMBLOB,\n" and the hash line exactly
		// "  `<col>` BINARY(32),\n" (nullable, no NOT NULL/DEFAULT/charset drift).
		// Expected lines are built from the independent pair literals above,
		// never from Column.SQLType() or renderColumnFor.
		wantIdentLine := fmt.Sprintf("  `%s` MEDIUMBLOB,\n", pair.identCol)
		if !strings.Contains(ddlAssets, wantIdentLine) {
			t.Errorf("assets DDL missing identity column line %q\n%s", wantIdentLine, ddlAssets)
		}
		wantHashLine := fmt.Sprintf("  `%s` BINARY(32),\n", pair.hashCol)
		if !strings.Contains(ddlAssets, wantHashLine) {
			t.Errorf("assets DDL missing hash column line %q\n%s", wantHashLine, ddlAssets)
		}
	}

	// company_scope: 3 pairs
	companyScopeBranchPairs := []struct {
		identCol string
		hashCol  string
		uqIndex  string
	}{
		{"identity_scope_domain", "hash_scope_domain", "uq_cscope_hash_domain"},
		{"identity_scope_net", "hash_scope_net", "uq_cscope_hash_net"},
		{"identity_scope_value", "hash_scope_value", "uq_cscope_hash_value"},
	}
	tbCScope := byName["company_scope"]
	if tbCScope == nil {
		t.Fatal("company_scope table missing")
	}
	ddlCScope, err := GenerateDDL(tbCScope)
	if err != nil {
		t.Fatalf("GenerateDDL(company_scope): %v", err)
	}
	for _, pair := range companyScopeBranchPairs {
		cIdent := findColumn(tbCScope, pair.identCol)
		if cIdent == nil || cIdent.Type != TMediumBlob || cIdent.NotNull {
			t.Errorf("company_scope.%s: want nullable MEDIUMBLOB, got %+v", pair.identCol, cIdent)
		}
		cHash := findColumn(tbCScope, pair.hashCol)
		if cHash == nil || cHash.Type != TBinary || cHash.Length != 32 || cHash.NotNull {
			t.Errorf("company_scope.%s: want nullable BINARY(32), got %+v", pair.hashCol, cHash)
		}
		wantUQ := fmt.Sprintf("UNIQUE KEY `%s` (`%s`)", pair.uqIndex, pair.hashCol)
		if !strings.Contains(ddlCScope, wantUQ) {
			t.Errorf("company_scope DDL missing %q", wantUQ)
		}
		// Full rendered column-line boundary check, same contract as assets:
		// identity "  `<col>` MEDIUMBLOB,\n", hash "  `<col>` BINARY(32),\n".
		wantIdentLine := fmt.Sprintf("  `%s` MEDIUMBLOB,\n", pair.identCol)
		if !strings.Contains(ddlCScope, wantIdentLine) {
			t.Errorf("company_scope DDL missing identity column line %q\n%s", wantIdentLine, ddlCScope)
		}
		wantHashLine := fmt.Sprintf("  `%s` BINARY(32),\n", pair.hashCol)
		if !strings.Contains(ddlCScope, wantHashLine) {
			t.Errorf("company_scope DDL missing hash column line %q\n%s", wantHashLine, ddlCScope)
		}
	}
}

// 9. TestDDLUniqueStringPartsAsciiBin
func TestDDLUniqueStringPartsAsciiBin(t *testing.T) {
	for _, tb := range AllTables() {
		colMap := make(map[string]Column, len(tb.Columns))
		for _, c := range tb.Columns {
			colMap[c.Name] = c
		}

		for _, ix := range tb.Indexes {
			if !ix.Unique {
				continue
			}
			for _, p := range ix.Parts {
				c, ok := colMap[p.Column]
				if !ok {
					t.Fatalf("table %s unique index %s references nonexistent column %s", tb.Name, ix.Name, p.Column)
				}
				// If part column is a string family type (VARCHAR/CHAR/TEXT/JSON)
				if c.IsString() {
					if c.Charset != "ascii" || c.Collation != "ascii_bin" {
						t.Errorf("table %s unique index %s string part %s must be ascii_bin, got Charset=%q Collation=%q",
							tb.Name, ix.Name, c.Name, c.Charset, c.Collation)
					}
					if p.Prefix != 0 {
						t.Errorf("table %s unique index %s string part %s must not have prefix, got Prefix=%d",
							tb.Name, ix.Name, c.Name, p.Prefix)
					}
				}
				// Binary types (BINARY, VARBINARY) are not strings and need no charset/collation
			}
		}
	}
}

// expectedInfraIndexLine builds the expected rendered index line from
// independent name/parts literals (test-only helper; deliberately does NOT
// call the production renderIndex). kind is "UNIQUE KEY" or "KEY".
func expectedInfraIndexLine(kind, name string, parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = "`" + p + "`"
	}
	return "  " + kind + " `" + name + "` (" + strings.Join(quoted, ", ") + ")"
}

// containsFullLine reports whether line appears in ddl as a complete line:
// terminated by ",\n" (followed by another definition) or "\n" (last line
// before the closing parenthesis), so no trailing prefix/DESC can hide.
func containsFullLine(ddl, line string) bool {
	return strings.Contains(ddl, line+",\n") || strings.Contains(ddl, line+"\n")
}

// 10. TestDDLInfraTables
func TestDDLInfraTables(t *testing.T) {
	byName := make(map[string]*Table)
	for _, tb := range AllTables() {
		byName[tb.Name] = tb
	}

	// 10 Exact UNIQUE indexes across the 8 infrastructure tables
	type expectedUQ struct {
		table string
		name  string
		parts []string
	}
	expected10UQ := []expectedUQ{
		{"locks", "uq_locks_name", []string{"name"}},
		{"schema_meta", "uq_schema_meta_key", []string{"key"}},
		{"business_deployment", "uq_business_deployment_id", []string{"deployment_id"}},
		{"business_deployment", "uq_business_deployment_uuid", []string{"deployment_uuid"}},
		{"business_deployment", "uq_business_deployment_root", []string{"root_uuid"}},
		{"maintenance", "uq_maintenance_singleton", []string{"singleton_key"}},
		{"storage_jobs", "uq_storage_jobs_op", []string{"op_id"}},
		{"storage_id_map", "uq_storage_id_map", []string{"op_id", "entity_kind", "old_id"}},
		{"storage_job_pins", "uq_storage_job_pins", []string{"op_id", "file_seq"}},
		{"op_outcomes", "uq_op_outcomes_identity", []string{"op_id", "tx_step", "segment_seq", "attempt_no"}},
	}

	infraTableNames := []string{
		"locks", "schema_meta", "business_deployment", "maintenance",
		"storage_jobs", "storage_id_map", "storage_job_pins", "op_outcomes",
	}

	// Verify exact count of UNIQUE indexes across 8 tables is exactly 10
	totalInfraUQCount := 0
	actualUQMap := make(map[string]expectedUQ)
	for _, tblName := range infraTableNames {
		tb := byName[tblName]
		if tb == nil {
			t.Fatalf("infra table %s missing", tblName)
		}
		for _, ix := range tb.Indexes {
			if ix.Unique {
				totalInfraUQCount++
				parts := make([]string, len(ix.Parts))
				for i, p := range ix.Parts {
					parts[i] = p.Column
					if p.Prefix != 0 {
						t.Errorf("infra table %s unique index %s part %s has prefix %d, want 0", tblName, ix.Name, p.Column, p.Prefix)
					}
					if p.Desc {
						t.Errorf("infra table %s unique index %s part %s has desc true, want false", tblName, ix.Name, p.Column)
					}
				}
				actualUQMap[ix.Name] = expectedUQ{table: tblName, name: ix.Name, parts: parts}
			}
		}
	}
	if totalInfraUQCount != 10 {
		t.Errorf("total unique indexes across 8 infra tables = %d, want exactly 10", totalInfraUQCount)
	}

	for _, exp := range expected10UQ {
		act, ok := actualUQMap[exp.name]
		if !ok {
			t.Errorf("missing expected unique index %s in table %s", exp.name, exp.table)
			continue
		}
		if act.table != exp.table {
			t.Errorf("index %s belongs to table %s, want %s", exp.name, act.table, exp.table)
		}
		if len(act.parts) != len(exp.parts) {
			t.Errorf("index %s parts length = %d, want %d", exp.name, len(act.parts), len(exp.parts))
			continue
		}
		for i := range exp.parts {
			if act.parts[i] != exp.parts[i] {
				t.Errorf("index %s part %d = %s, want %s", exp.name, i, act.parts[i], exp.parts[i])
			}
		}
	}

	// Rendered DDL evidence for all 10 UNIQUE indexes: each expected full line
	// is built from the independent exp.parts literals above.
	infraDDL := make(map[string]string, len(infraTableNames))
	for _, tblName := range infraTableNames {
		ddl, err := GenerateDDL(byName[tblName])
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", tblName, err)
		}
		infraDDL[tblName] = ddl
	}
	for _, exp := range expected10UQ {
		wantLine := expectedInfraIndexLine("UNIQUE KEY", exp.name, exp.parts)
		if !containsFullLine(infraDDL[exp.table], wantLine) {
			t.Errorf("%s DDL missing unique index line %q\n%s", exp.table, wantLine, infraDDL[exp.table])
		}
	}

	// 4 Non-unique KEYs across 8 infra tables
	type expectedKEY struct {
		table string
		name  string
		parts []string
	}
	expected4KEY := []expectedKEY{
		{"storage_jobs", "idx_storage_jobs_state", []string{"state", "updated_at", "op_id"}},
		{"storage_id_map", "idx_storage_id_map_reverse", []string{"op_id", "entity_kind", "new_id"}},
		{"storage_job_pins", "idx_storage_job_pins_lookup", []string{"store_id", "sha256", "op_id"}},
		{"op_outcomes", "idx_op_outcomes_lookup", []string{"op_id", "tx_step", "segment_seq"}},
	}
	totalInfraNonUQCount := 0
	for _, tblName := range infraTableNames {
		tb := byName[tblName]
		for _, ix := range tb.Indexes {
			if !ix.Unique {
				totalInfraNonUQCount++
			}
		}
	}
	if totalInfraNonUQCount != 4 {
		t.Errorf("total non-unique indexes across 8 infra tables = %d, want exactly 4", totalInfraNonUQCount)
	}

	for _, exp := range expected4KEY {
		tb := byName[exp.table]
		var found *Index
		for i := range tb.Indexes {
			if tb.Indexes[i].Name == exp.name {
				found = &tb.Indexes[i]
				break
			}
		}
		if found == nil {
			t.Errorf("table %s missing index %s", exp.table, exp.name)
			continue
		}
		if found.Unique {
			t.Errorf("table %s index %s must be non-unique KEY", exp.table, exp.name)
		}
		if len(found.Parts) != len(exp.parts) {
			t.Errorf("index %s parts len = %d, want %d", exp.name, len(found.Parts), len(exp.parts))
			continue
		}
		for i := range exp.parts {
			if found.Parts[i].Column != exp.parts[i] {
				t.Errorf("index %s part %d = %s, want %s", exp.name, i, found.Parts[i].Column, exp.parts[i])
			}
			if found.Parts[i].Prefix != 0 || found.Parts[i].Desc {
				t.Errorf("index %s part %s: prefix=%d, desc=%v, want 0/false", exp.name, found.Parts[i].Column, found.Parts[i].Prefix, found.Parts[i].Desc)
			}
		}
	}

	// Rendered DDL evidence for all 4 non-unique KEYs: each expected full line
	// is built from the independent exp.parts literals above.
	for _, exp := range expected4KEY {
		wantLine := expectedInfraIndexLine("KEY", exp.name, exp.parts)
		if !containsFullLine(infraDDL[exp.table], wantLine) {
			t.Errorf("%s DDL missing index line %q\n%s", exp.table, wantLine, infraDDL[exp.table])
		}
	}

	// Explicit rule: storage_id_map.new_id must NOT appear in any UNIQUE index
	tbStorageIDMap := byName["storage_id_map"]
	for _, ix := range tbStorageIDMap.Indexes {
		if ix.Unique {
			for _, p := range ix.Parts {
				if p.Column == "new_id" {
					t.Errorf("storage_id_map.new_id must NOT appear in UNIQUE index %s", ix.Name)
				}
			}
		}
	}

	// Verify specific BIGINT UNSIGNED columns in infra tables from specs §7.5 / §7.6
	requiredCols := []struct {
		table      string
		col        string
		wantType   ColumnType
		notNull    bool
		ddlPattern string
	}{
		{"maintenance", "revision", TBigUnsigned, true, "`revision` BIGINT UNSIGNED NOT NULL DEFAULT 0,"},
		{"storage_jobs", "segment_seq", TBigUnsigned, true, "`segment_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0,"},
		{"storage_job_pins", "file_seq", TBigUnsigned, true, "`file_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0,"},
		{"storage_job_pins", "size_bytes", TBigUnsigned, true, "`size_bytes` BIGINT UNSIGNED NOT NULL DEFAULT 0,"},
		{"op_outcomes", "segment_seq", TBigUnsigned, true, "`segment_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0,"},
	}
	for _, rc := range requiredCols {
		tb := byName[rc.table]
		col := findColumn(tb, rc.col)
		if col == nil {
			t.Errorf("%s.%s missing", rc.table, rc.col)
			continue
		}
		if col.Type != rc.wantType || col.NotNull != rc.notNull {
			t.Errorf("%s.%s type=%v notNull=%v, want type=%v notNull=%v", rc.table, rc.col, col.Type, col.NotNull, rc.wantType, rc.notNull)
		}
		ddl, err := GenerateDDL(tb)
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", rc.table, err)
		}
		if !strings.Contains(ddl, rc.ddlPattern) {
			t.Errorf("%s DDL missing pattern %q\n%s", rc.table, rc.ddlPattern, ddl)
		}
	}
}

// 11. TestDDLAutoIncScript
func TestDDLAutoIncScript(t *testing.T) {
	script, err := GenerateAllDDL()
	if err != nil {
		t.Fatalf("GenerateAllDDL: %v", err)
	}

	stmts := SplitStatements(script)
	if len(stmts) != 65 {
		t.Fatalf("SplitStatements count = %d, want 65", len(stmts))
	}

	autoIncTableCount := 0
	cacheTableCount := 0

	for i, stmt := range stmts {
		colPart, optPart, err := splitTableParts(stmt)
		if err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}

		// Column section must NEVER contain AUTO_ID_CACHE
		if strings.Contains(colPart, "AUTO_ID_CACHE") {
			t.Errorf("statement %d contains AUTO_ID_CACHE in column section:\n%s", i, colPart)
		}

		isHealthTable := strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS `llm_profile_health`")
		if isHealthTable {
			if strings.Contains(stmt, "AUTO_INCREMENT") {
				t.Errorf("llm_profile_health statement must not contain AUTO_INCREMENT:\n%s", stmt)
			}
			if strings.Contains(stmt, "AUTO_ID_CACHE") {
				t.Errorf("llm_profile_health statement must not contain AUTO_ID_CACHE:\n%s", stmt)
			}
		} else {
			// Auto-increment table
			if !strings.Contains(colPart, "AUTO_INCREMENT") {
				t.Errorf("statement %d missing AUTO_INCREMENT in columns:\n%s", i, colPart)
			}
			if !strings.Contains(optPart, "AUTO_ID_CACHE=1") {
				t.Errorf("statement %d missing AUTO_ID_CACHE=1 in options:\n%s", i, optPart)
			}
			autoIncTableCount++
			cacheTableCount++
		}
	}

	if autoIncTableCount != 64 {
		t.Errorf("auto-increment table statements count = %d, want 64", autoIncTableCount)
	}
	if cacheTableCount != 64 {
		t.Errorf("cache table statements count = %d, want 64", cacheTableCount)
	}
}

// 12. TestDDLTrafficUniqueLayout
func TestDDLTrafficUniqueLayout(t *testing.T) {
	byName := make(map[string]*Table)
	for _, tb := range AllTables() {
		byName[tb.Name] = tb
	}

	type trafficExpectation struct {
		table   string
		uqName  string
		parts   []string
		wantDDL string
	}
	expectedTraffic := []trafficExpectation{
		{
			table:   "traffic_exchanges",
			uqName:  "uq_traffic_exchanges",
			parts:   []string{"namespace", "id"},
			wantDDL: "UNIQUE KEY `uq_traffic_exchanges` (`namespace`, `id`)",
		},
		{
			table:   "traffic_exchange_bodies",
			uqName:  "uq_traffic_exchange_bodies",
			parts:   []string{"namespace", "exchange_id"},
			wantDDL: "UNIQUE KEY `uq_traffic_exchange_bodies` (`namespace`, `exchange_id`)",
		},
		{
			table:   "traffic_blob_refs",
			uqName:  "uq_traffic_blob_refs",
			parts:   []string{"namespace", "hash", "exchange_id"},
			wantDDL: "UNIQUE KEY `uq_traffic_blob_refs` (`namespace`, `hash`, `exchange_id`)",
		},
		{
			table:   "traffic_index_chunks",
			uqName:  "uq_traffic_index_chunks",
			parts:   []string{"namespace", "exchange_id", "seq"},
			wantDDL: "UNIQUE KEY `uq_traffic_index_chunks` (`namespace`, `exchange_id`, `seq`)",
		},
		{
			table:   "traffic_stores",
			uqName:  "uq_traffic_stores_namespace",
			parts:   []string{"namespace"},
			wantDDL: "UNIQUE KEY `uq_traffic_stores_namespace` (`namespace`)",
		},
		{
			table:   "traffic_stores",
			uqName:  "uq_traffic_stores_store",
			parts:   []string{"store_id"},
			wantDDL: "UNIQUE KEY `uq_traffic_stores_store` (`store_id`)",
		},
		{
			table:   "traffic_stores",
			uqName:  "uq_traffic_stores_role",
			parts:   []string{"role"},
			wantDDL: "UNIQUE KEY `uq_traffic_stores_role` (`role`)",
		},
	}

	for _, exp := range expectedTraffic {
		tb := byName[exp.table]
		if tb == nil {
			t.Fatalf("table %s missing", exp.table)
		}
		var found *Index
		for i := range tb.Indexes {
			if tb.Indexes[i].Name == exp.uqName {
				found = &tb.Indexes[i]
				break
			}
		}
		if found == nil {
			t.Fatalf("table %s missing index %s", exp.table, exp.uqName)
		}
		if !found.Unique {
			t.Errorf("table %s index %s must be Unique", exp.table, exp.uqName)
		}
		if len(found.Parts) != len(exp.parts) {
			t.Fatalf("table %s index %s parts len = %d, want %d", exp.table, exp.uqName, len(found.Parts), len(exp.parts))
		}
		for i := range exp.parts {
			if found.Parts[i].Column != exp.parts[i] {
				t.Errorf("table %s index %s part %d = %s, want %s", exp.table, exp.uqName, i, found.Parts[i].Column, exp.parts[i])
			}
			if found.Parts[i].Prefix != 0 {
				t.Errorf("table %s index %s part %s must not have prefix, got %d", exp.table, exp.uqName, found.Parts[i].Column, found.Parts[i].Prefix)
			}
			if found.Parts[i].Desc {
				t.Errorf("table %s index %s part %s must not be DESC", exp.table, exp.uqName, found.Parts[i].Column)
			}
		}

		ddl, err := GenerateDDL(tb)
		if err != nil {
			t.Fatalf("GenerateDDL(%s): %v", exp.table, err)
		}
		if !strings.Contains(ddl, exp.wantDDL) {
			t.Errorf("table %s DDL missing %q\n%s", exp.table, exp.wantDDL, ddl)
		}
	}

	// Additional contracts for traffic tables:
	// 1. traffic_exchange_bodies req_blob and resp_blob are CHAR(64) ascii_bin nullable (spec §6.1, I2)
	tbBodies := byName["traffic_exchange_bodies"]
	reqBlob := findColumn(tbBodies, "req_blob")
	respBlob := findColumn(tbBodies, "resp_blob")
	if reqBlob == nil || reqBlob.Type != TChar || reqBlob.Length != 64 || reqBlob.NotNull || reqBlob.Charset != "ascii" || reqBlob.Collation != "ascii_bin" {
		t.Errorf("traffic_exchange_bodies.req_blob contract violation: %+v", reqBlob)
	}
	if respBlob == nil || respBlob.Type != TChar || respBlob.Length != 64 || respBlob.NotNull || respBlob.Charset != "ascii" || respBlob.Collation != "ascii_bin" {
		t.Errorf("traffic_exchange_bodies.resp_blob contract violation: %+v", respBlob)
	}

	// 2. traffic_index_chunks text column is MEDIUMTEXT utf8mb4_bin NOT NULL (spec §6.1, I4)
	tbChunks := byName["traffic_index_chunks"]
	textCol := findColumn(tbChunks, "text")
	if textCol == nil || textCol.Type != TText || !textCol.NotNull || textCol.Charset != "utf8mb4" || textCol.Collation != "utf8mb4_bin" {
		t.Errorf("traffic_index_chunks.text contract violation: %+v", textCol)
	}
	ddlChunks, err := GenerateDDL(tbChunks)
	if err != nil {
		t.Fatalf("GenerateDDL(traffic_index_chunks): %v", err)
	}
	if !strings.Contains(ddlChunks, "`text` MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,") {
		t.Errorf("traffic_index_chunks DDL missing utf8mb4_bin text definition:\n%s", ddlChunks)
	}
}

// 13. TestGenerateDDLBacktickEscaping
func TestGenerateDDLBacktickEscaping(t *testing.T) {
	// Synthetic table with embedded backticks in table name, column name, PK, and index names
	tbl := &Table{
		Name:          "tab`le",
		PKColumn:      "p`k",
		AutoIncrement: true,
		Columns: []Column{
			{Name: "p`k", Type: TBigUnsigned, NotNull: true},
			{Name: "col`umn", Type: TVarchar, Length: 32, Charset: "ascii", Collation: "ascii_bin", NotNull: true},
		},
		Indexes: []Index{
			{
				Name:   "uq`idx",
				Unique: true,
				Parts:  []IndexPart{{Column: "col`umn"}},
			},
		},
	}

	ddl, err := GenerateDDL(tbl)
	if err != nil {
		t.Fatalf("GenerateDDL with backtick identifier failed: %v", err)
	}

	wantFragments := []string{
		"CREATE TABLE IF NOT EXISTS `tab``le` (",
		"  `p``k` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,",
		"  `col``umn` VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,",
		"  PRIMARY KEY (`p``k`),",
		"  UNIQUE KEY `uq``idx` (`col``umn`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_ID_CACHE=1",
	}

	for _, frag := range wantFragments {
		if !strings.Contains(ddl, frag) {
			t.Errorf("backtick escaping DDL missing %q\nFull DDL:\n%s", frag, ddl)
		}
	}
}
