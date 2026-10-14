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
