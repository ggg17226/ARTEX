package schema

import "testing"

var wantBaselinePKs = map[string]string{
	"companies": "id", "assets": "id", "company_scope": "id", "explorations": "id",
	"exploration_nodes": "id", "exploration_edges": "row_id", "exploration_anchors": "row_id",
	"task_constraints": "id", "activity": "id", "main_sessions": "row_id",
	"settings": "row_id", "llm_profiles": "id", "llm_profile_health": "profile_id",
	"task_categories": "id", "tasks": "id", "task_archives": "id", "task_templates": "id",
	"task_relations": "row_id", "task_asset_links": "row_id", "task_llm_profiles": "row_id",
	"task_scope": "id", "agents": "id", "agent_prompts": "id", "agent_prompt_vars": "id",
	"mcp_servers": "id", "mcp_tools_cache": "id", "agent_visibility": "row_id",
	"agent_skill_visibility": "row_id", "skill_usage": "id", "tool_usage": "id",
	"tools": "row_id", "conversations": "id", "conversation_activities": "id",
	"agent_triggers": "id", "scheduler_state": "row_id", "intercept_rules": "id",
	"intercept_pending": "id", "findings": "id", "finding_retests": "id",
	"traffic_evidence_snapshots": "row_id", "finding_traffic_bindings": "id",
	"server_logs": "id", "side_question_sessions": "row_id", "side_question_requests": "ordinal",
	"asset_intercept_rules": "id", "task_intercept_rules": "id", "notification_channels": "id",
	"notification_events": "id", "notification_deliveries": "id",
}

func TestBaselineTablesShape(t *testing.T) {
	tbls := BaselineTables()
	if len(tbls) != 49 {
		t.Fatalf("BaselineTables() = %d tables, want 49", len(tbls))
	}
	byKind := map[string]int{}
	autoInc := 0
	for _, tb := range tbls {
		if want, ok := wantBaselinePKs[tb.Name]; !ok {
			t.Errorf("unexpected baseline table %s", tb.Name)
		} else if tb.PKColumn != want {
			t.Errorf("table %s: PKColumn = %q, want %q", tb.Name, tb.PKColumn, want)
		}
		var pk *Column
		for i := range tb.Columns {
			if tb.Columns[i].Name == tb.PKColumn {
				pk = &tb.Columns[i]
			}
		}
		if pk == nil {
			t.Errorf("table %s: PK column %s missing", tb.Name, tb.PKColumn)
			continue
		}
		if pk.Type != TBigUnsigned || !pk.NotNull {
			t.Errorf("table %s: PK column must be BIGINT UNSIGNED NOT NULL", tb.Name)
		}
		if tb.AutoIncrement {
			autoInc++
		}
		if !tb.AutoIncrement && tb.PKColumn != "profile_id" {
			t.Errorf("table %s: expected AUTO_INCREMENT PK, got none", tb.Name)
		}
		byKind[tb.PKColumn]++
		if err := tb.Validate(); err != nil {
			t.Errorf("table %s: Validate: %v", tb.Name, err)
		}
	}
	if byKind["id"] != 34 || byKind["ordinal"] != 1 || byKind["profile_id"] != 1 || byKind["row_id"] != 13 {
		t.Errorf("PK kinds = %+v, want id=34 ordinal=1 profile_id=1 row_id=13", byKind)
	}
	if autoInc != 48 {
		t.Errorf("auto-increment baseline tables = %d, want 48", autoInc)
	}
}

func TestBaselineTablesHashDDL(t *testing.T) {
	cases := map[string]string{
		"companies": "nkey_hash", "task_categories": "nkey_hash", "task_templates": "nkey_hash",
		"agents": "key_hash", "mcp_servers": "name_hash",
		"settings": "key_hash", "tools": "key_hash", "scheduler_state": "key_hash",
		"agent_prompt_vars": "identity_hash", "mcp_tools_cache": "identity_hash",
		"agent_visibility": "identity_hash", "agent_skill_visibility": "identity_hash",
	}
	for name, hashCol := range cases {
		tbl := lookupBaseline(t, name)
		if !hasBlobCol(tbl) {
			t.Errorf("table %s: missing MEDIUMBLOB identity column", name)
		}
		if !hasBinCol(tbl, hashCol) {
			t.Errorf("table %s: missing BINARY(32) NOT NULL %q column", name, hashCol)
		}
		if !hasUniqueOn(tbl, hashCol) {
			t.Errorf("table %s: missing UNIQUE KEY on %q", name, hashCol)
		}
	}
	for name, want := range map[string]int{"assets": 8, "company_scope": 3} {
		tbl := lookupBaseline(t, name)
		blobCols, hashCols, uqCols := 0, 0, 0
		for _, c := range tbl.Columns {
			if c.Type == TMediumBlob {
				blobCols++
			}
			if c.Type == TBinary && c.Length == 32 {
				hashCols++
			}
		}
		for _, ix := range tbl.Indexes {
			if ix.Unique && len(ix.Parts) == 1 && ix.Parts[0].Column != tbl.PKColumn && isBinary32(tbl, ix.Parts[0].Column) {
				uqCols++
			}
		}
		if blobCols != want {
			t.Errorf("%s: MEDIUMBLOB identity columns = %d, want %d", name, blobCols, want)
		}
		if hashCols != want {
			t.Errorf("%s: BINARY(32) hash columns = %d, want %d", name, hashCols, want)
		}
		if uqCols != want {
			t.Errorf("%s: UNIQUE hash indexes = %d, want %d", name, uqCols, want)
		}
	}
}

func TestBaselineSideQuestionRequests(t *testing.T) {
	tbl := lookupBaseline(t, "side_question_requests")
	if tbl.PKColumn != "ordinal" || !tbl.AutoIncrement {
		t.Fatalf("side_question_requests: want ordinal AUTO_INCREMENT PK, got %q/%v", tbl.PKColumn, tbl.AutoIncrement)
	}
	var hasID, hasRunning bool
	for _, c := range tbl.Columns {
		if c.Name == "id" && c.Type == TVarchar && c.Length == 64 && c.Charset == "ascii" && c.Collation == "ascii_bin" {
			hasID = true
		}
		if c.Name == "running_session_key" && c.Generated != "" && c.Charset == "ascii" && c.Collation == "ascii_bin" {
			hasRunning = true
		}
	}
	if !hasID {
		t.Error("side_question_requests.id must be VARCHAR(64) ascii_bin")
	}
	if !hasRunning {
		t.Error("side_question_requests.running_session_key generated column (ascii_bin) missing")
	}
}

func lookupBaseline(t *testing.T, name string) *Table {
	t.Helper()
	for _, tb := range BaselineTables() {
		if tb.Name == name {
			return tb
		}
	}
	t.Fatalf("baseline table %s not found", name)
	return nil
}

func hasBlobCol(tb *Table) bool {
	for _, c := range tb.Columns {
		if c.Type == TMediumBlob {
			return true
		}
	}
	return false
}

func hasBinCol(tb *Table, name string) bool {
	for _, c := range tb.Columns {
		if c.Name == name && c.Type == TBinary && c.Length == 32 && c.NotNull {
			return true
		}
	}
	return false
}

func isBinary32(tb *Table, name string) bool {
	for _, c := range tb.Columns {
		if c.Name == name && c.Type == TBinary && c.Length == 32 {
			return true
		}
	}
	return false
}

func hasUniqueOn(tb *Table, col string) bool {
	for _, ix := range tb.Indexes {
		if ix.Unique && len(ix.Parts) == 1 && ix.Parts[0].Column == col {
			return true
		}
	}
	return false
}
