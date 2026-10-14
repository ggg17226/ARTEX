package schema

import "testing"

var wantExtensionPKs = map[string]string{
	"llm_records": "id", "llm_usage": "id", "finding_assets": "id",
	"traffic_exchanges": "row_id", "traffic_exchange_bodies": "row_id",
	"traffic_blob_refs": "row_id", "traffic_index_chunks": "row_id",
	"traffic_stores": "row_id",
	"locks":          "row_id", "schema_meta": "row_id", "business_deployment": "row_id",
	"maintenance": "row_id", "storage_jobs": "row_id", "storage_id_map": "row_id",
	"storage_job_pins": "row_id", "op_outcomes": "row_id",
}

func TestExtensionTablesShape(t *testing.T) {
	tbls := ExtensionTables()
	if len(tbls) != 16 {
		t.Fatalf("ExtensionTables() = %d tables, want 16", len(tbls))
	}
	idCount, rowIDCount, autoInc := 0, 0, 0
	seen := map[string]bool{}
	for _, tb := range tbls {
		if seen[tb.Name] {
			t.Fatalf("duplicate table %s", tb.Name)
		}
		seen[tb.Name] = true
		want, ok := wantExtensionPKs[tb.Name]
		if !ok {
			t.Errorf("unexpected extension table %s", tb.Name)
		} else if tb.PKColumn != want {
			t.Errorf("table %s: PKColumn = %q, want %q", tb.Name, tb.PKColumn, want)
		}
		if tb.AutoIncrement {
			autoInc++
		}
		switch tb.PKColumn {
		case "id":
			idCount++
		case "row_id":
			rowIDCount++
		}
		if err := tb.Validate(); err != nil {
			t.Errorf("table %s: Validate: %v", tb.Name, err)
		}
	}
	if idCount != 3 || rowIDCount != 13 || autoInc != 16 {
		t.Errorf("extension PK kinds: id=%d row_id=%d autoInc=%d, want 3/13/16", idCount, rowIDCount, autoInc)
	}
}

func TestAllTables65(t *testing.T) {
	all := AllTables()
	if len(all) != 65 {
		t.Fatalf("AllTables() = %d, want 65", len(all))
	}
	pk := map[string]int{}
	autoInc := 0
	seen := map[string]bool{}
	for _, tb := range all {
		if seen[tb.Name] {
			t.Fatalf("duplicate table %s", tb.Name)
		}
		seen[tb.Name] = true
		pk[tb.PKColumn]++
		if tb.AutoIncrement {
			autoInc++
		}
		if err := tb.Validate(); err != nil {
			t.Errorf("table %s: Validate: %v", tb.Name, err)
		}
	}
	if pk["id"] != 37 || pk["ordinal"] != 1 || pk["profile_id"] != 1 || pk["row_id"] != 26 {
		t.Errorf("PK kinds = %+v, want id=37 ordinal=1 profile_id=1 row_id=26", pk)
	}
	if autoInc != 64 {
		t.Errorf("auto-increment tables = %d, want 64", autoInc)
	}
}

func TestTrafficTablesUniqueIdentity(t *testing.T) {
	want := map[string][]string{
		"traffic_exchanges":       {"namespace", "id"},
		"traffic_exchange_bodies": {"namespace", "exchange_id"},
		"traffic_blob_refs":       {"namespace", "hash", "exchange_id"},
		"traffic_index_chunks":    {"namespace", "exchange_id", "seq"},
	}
	byName := map[string]*Table{}
	for _, tb := range AllTables() {
		byName[tb.Name] = tb
	}
	for name, cols := range want {
		tbl := byName[name]
		if tbl == nil {
			t.Errorf("table %s missing", name)
			continue
		}
		found := false
		for _, ix := range tbl.Indexes {
			if !ix.Unique || len(ix.Parts) != len(cols) {
				continue
			}
			match := true
			for i, p := range ix.Parts {
				if p.Column != cols[i] {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("table %s: missing UNIQUE KEY (%s)", name, joinCols(cols))
		}
	}
	// 5 流量表的 namespace/id 类身份列必须显式 ascii_bin
	for _, c := range []struct{ table, col string }{
		{"traffic_exchanges", "namespace"}, {"traffic_exchanges", "id"},
		{"traffic_exchange_bodies", "namespace"}, {"traffic_exchange_bodies", "exchange_id"},
		{"traffic_blob_refs", "namespace"}, {"traffic_blob_refs", "hash"},
		{"traffic_index_chunks", "namespace"},
		{"traffic_stores", "namespace"}, {"traffic_stores", "role"},
		{"locks", "name"}, {"schema_meta", "key"},
	} {
		tbl := byName[c.table]
		if tbl == nil {
			continue
		}
		var col *Column
		for i := range tbl.Columns {
			if tbl.Columns[i].Name == c.col {
				col = &tbl.Columns[i]
			}
		}
		if col == nil {
			t.Errorf("%s.%s missing", c.table, c.col)
			continue
		}
		if col.Charset != "ascii" || col.Collation != "ascii_bin" {
			t.Errorf("%s.%s must be CHARACTER SET ascii COLLATE ascii_bin", c.table, c.col)
		}
	}
}

func joinCols(cols []string) string {
	s := ""
	for i, c := range cols {
		if i > 0 {
			s += ", "
		}
		s += c
	}
	return s
}
