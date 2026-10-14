package schema

import (
	"reflect"
	"testing"
)

func findColumn(tbl *Table, name string) *Column {
	for i := range tbl.Columns {
		if tbl.Columns[i].Name == name {
			return &tbl.Columns[i]
		}
	}
	return nil
}

// TestLLMProfileIdentityLayout verifies that llm_profiles declared in BaselineTables
// has canonical identity and hash columns aligned with specifications:
// - Columns name, name_identity, name_hash exist in that exact order
// - name_identity is MEDIUMBLOB NOT NULL with no default, no generated expr, no character set/collation
// - name_hash is BINARY(32) NOT NULL with a single-column non-prefixed UNIQUE index
// - no legacy name prefix UNIQUE index exists
// - is_default_key generated column and uq_llm_one_default UNIQUE index remain intact
// - name has no arbitrary VARCHAR truncation (it is TextNN / MEDIUMTEXT NOT NULL)
// - PK remains id AUTO_INCREMENT BIGINT UNSIGNED NOT NULL
func TestLLMProfileIdentityLayout(t *testing.T) {
	var tbl *Table
	for _, tb := range BaselineTables() {
		if tb.Name == "llm_profiles" {
			tbl = tb
			break
		}
	}
	if tbl == nil {
		t.Fatalf("llm_profiles table not found in BaselineTables")
	}

	if tbl.PKColumn != "id" || !tbl.AutoIncrement {
		t.Fatalf("llm_profiles PKColumn = %q, AutoIncrement = %v; want 'id' / true", tbl.PKColumn, tbl.AutoIncrement)
	}

	pkCol := findColumn(tbl, tbl.PKColumn)
	if pkCol == nil || pkCol.Type != TBigUnsigned || !pkCol.NotNull {
		t.Fatalf("llm_profiles PK column must be BIGINT UNSIGNED NOT NULL, got %+v", pkCol)
	}

	nameIdx, identIdx, hashIdx := -1, -1, -1
	for i, c := range tbl.Columns {
		switch c.Name {
		case "name":
			nameIdx = i
		case "name_identity":
			identIdx = i
		case "name_hash":
			hashIdx = i
		}
	}

	if nameIdx == -1 {
		t.Fatalf("column 'name' missing from llm_profiles")
	}
	if identIdx == -1 {
		t.Fatalf("column 'name_identity' missing from llm_profiles")
	}
	if hashIdx == -1 {
		t.Fatalf("column 'name_hash' missing from llm_profiles")
	}

	if !(nameIdx < identIdx && identIdx < hashIdx) {
		t.Errorf("columns order mismatch: want name (idx %d) < name_identity (idx %d) < name_hash (idx %d)",
			nameIdx, identIdx, hashIdx)
	}

	identCol := findColumn(tbl, "name_identity")
	if identCol == nil {
		t.Fatalf("name_identity column not found")
	}
	if identCol.Type != TMediumBlob {
		t.Errorf("name_identity Type = %v, want TMediumBlob (%v)", identCol.Type, TMediumBlob)
	}
	if !identCol.NotNull {
		t.Errorf("name_identity NotNull = false, want true")
	}
	if identCol.Default != "" {
		t.Errorf("name_identity Default = %q, want empty", identCol.Default)
	}
	if identCol.Generated != "" {
		t.Errorf("name_identity Generated = %q, want empty", identCol.Generated)
	}
	if identCol.Charset != "" || identCol.Collation != "" {
		t.Errorf("name_identity Charset=%q Collation=%q, want empty (binary blob)", identCol.Charset, identCol.Collation)
	}

	hashCol := findColumn(tbl, "name_hash")
	if hashCol == nil {
		t.Fatalf("name_hash column not found")
	}
	if hashCol.Type != TBinary || hashCol.Length != 32 {
		t.Errorf("name_hash Type=%v Length=%d, want TBinary(32)", hashCol.Type, hashCol.Length)
	}
	if !hashCol.NotNull {
		t.Errorf("name_hash NotNull = false, want true")
	}

	// name is TextNN (MEDIUMTEXT NOT NULL), not arbitrary VARCHAR
	nameCol := findColumn(tbl, "name")
	if nameCol == nil || nameCol.Type != TText || !nameCol.NotNull {
		t.Errorf("name col = %+v, want TText / true", nameCol)
	}

	// Generated column is_default_key and uq_llm_one_default
	genCol := findColumn(tbl, "is_default_key")
	if genCol == nil || genCol.Generated == "" {
		t.Errorf("is_default_key generated column missing or empty")
	}
	var hasOneDefaultUQ bool
	for _, ix := range tbl.Indexes {
		if ix.Name == "uq_llm_one_default" && ix.Unique {
			hasOneDefaultUQ = true
			if len(ix.Parts) != 1 || ix.Parts[0].Column != "is_default_key" {
				t.Errorf("uq_llm_one_default parts = %+v, want [is_default_key]", ix.Parts)
			}
		}
	}
	if !hasOneDefaultUQ {
		t.Errorf("uq_llm_one_default index missing or not unique")
	}

	// Exactly one non-prefixed UNIQUE index on name_hash; no prefix UNIQUE on name
	var hasHashUQ bool
	for _, ix := range tbl.Indexes {
		if ix.Unique {
			if len(ix.Parts) == 1 && ix.Parts[0].Column == "name_hash" {
				if ix.Parts[0].Prefix != 0 {
					t.Errorf("unique index on name_hash has prefix length %d, want full column (0)", ix.Parts[0].Prefix)
				}
				hasHashUQ = true
			}
			for _, p := range ix.Parts {
				if p.Column == "name" {
					t.Errorf("unexpected UNIQUE index part on 'name': %+v", ix)
				}
			}
		}
	}
	if !hasHashUQ {
		t.Errorf("single-column non-prefixed UNIQUE index on name_hash missing")
	}

	if err := tbl.Validate(); err != nil {
		t.Errorf("llm_profiles Table.Validate() failed: %v", err)
	}
}

// TestHashIdentityColumnMappings asserts the 13 canonical identity/hash mappings
// (12 baseline tables + llm_profiles), and verifies table/column existence and constraints:
// - exact key set and exact [2]string values
// - each table exists in AllTables()
// - identity column exists, is TMediumBlob NOT NULL
// - hash column exists, is TBinary(32) NOT NULL
// - hash column has at least one single-column non-prefixed UNIQUE index
func TestHashIdentityColumnMappings(t *testing.T) {
	wantMappings := map[string][2]string{
		"companies":              {"nkey_identity", "nkey_hash"},
		"task_categories":        {"nkey_identity", "nkey_hash"},
		"task_templates":         {"nkey_identity", "nkey_hash"},
		"agents":                 {"key_identity", "key_hash"},
		"mcp_servers":            {"name_identity", "name_hash"},
		"settings":               {"key_identity", "key_hash"},
		"tools":                  {"key_identity", "key_hash"},
		"scheduler_state":        {"key_identity", "key_hash"},
		"agent_prompt_vars":      {"identity", "identity_hash"},
		"mcp_tools_cache":        {"identity", "identity_hash"},
		"agent_visibility":       {"identity", "identity_hash"},
		"agent_skill_visibility": {"identity", "identity_hash"},
		"llm_profiles":           {"name_identity", "name_hash"},
	}

	if !reflect.DeepEqual(HashIdentityColumns, wantMappings) {
		t.Errorf("HashIdentityColumns mismatch:\ngot:  %+v\nwant: %+v", HashIdentityColumns, wantMappings)
	}

	allTbls := AllTables()
	tblMap := make(map[string]*Table, len(allTbls))
	for _, tbl := range allTbls {
		tblMap[tbl.Name] = tbl
	}

	for tblName, cols := range wantMappings {
		tbl, ok := tblMap[tblName]
		if !ok {
			t.Errorf("mapped table %q not found in AllTables()", tblName)
			continue
		}

		identName, hashName := cols[0], cols[1]
		identCol := findColumn(tbl, identName)
		if identCol == nil {
			t.Errorf("table %s: identity column %q missing", tblName, identName)
		} else {
			if identCol.Type != TMediumBlob {
				t.Errorf("table %s: identity column %q type = %v, want TMediumBlob", tblName, identName, identCol.Type)
			}
			if !identCol.NotNull {
				t.Errorf("table %s: identity column %q NotNull = false, want true", tblName, identName)
			}
		}

		hashCol := findColumn(tbl, hashName)
		if hashCol == nil {
			t.Errorf("table %s: hash column %q missing", tblName, hashName)
		} else {
			if hashCol.Type != TBinary || hashCol.Length != 32 {
				t.Errorf("table %s: hash column %q type = %v (len %d), want TBinary(32)", tblName, hashName, hashCol.Type, hashCol.Length)
			}
			if !hashCol.NotNull {
				t.Errorf("table %s: hash column %q NotNull = false, want true", tblName, hashName)
			}
		}

		var hasSingleColFullUQ bool
		for _, ix := range tbl.Indexes {
			if ix.Unique && len(ix.Parts) == 1 && ix.Parts[0].Column == hashName && ix.Parts[0].Prefix == 0 {
				hasSingleColFullUQ = true
				break
			}
		}
		if !hasSingleColFullUQ {
			t.Errorf("table %s: missing single-column full UNIQUE index on %q", tblName, hashName)
		}
	}
}
