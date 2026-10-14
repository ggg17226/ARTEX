package schema

import (
	"strings"
	"testing"
)

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

func TestExtensionColumnContracts(t *testing.T) {
	byName := map[string]*Table{}
	for _, tb := range ExtensionTables() {
		byName[tb.Name] = tb
	}

	findCol := func(tb *Table, colName string) *Column {
		if tb == nil {
			return nil
		}
		for i := range tb.Columns {
			if tb.Columns[i].Name == colName {
				return &tb.Columns[i]
			}
		}
		return nil
	}

	t.Run("I2_traffic_exchange_bodies_blob_refs", func(t *testing.T) {
		tb := byName["traffic_exchange_bodies"]
		if tb == nil {
			t.Fatalf("traffic_exchange_bodies missing")
		}
		// req_body / resp_body 原始正文保持 MEDIUMBLOB NULL
		for _, name := range []string{"req_body", "resp_body"} {
			c := findCol(tb, name)
			if c == nil {
				t.Fatalf("column %s missing", name)
			}
			if c.Type != TMediumBlob {
				t.Errorf("%s type = %v, want TMediumBlob", name, c.Type)
			}
			if c.NotNull {
				t.Errorf("%s NotNull = %v, want false", name, c.NotNull)
			}
		}
		// req_blob / resp_blob 必须为 CHAR(64) ascii_bin, 可空 NotNull=false, 无默认值
		for _, name := range []string{"req_blob", "resp_blob"} {
			c := findCol(tb, name)
			if c == nil {
				t.Fatalf("column %s missing", name)
			}
			if c.Type != TChar || c.Length != 64 {
				t.Errorf("%s Type=%v Length=%d, want TChar(64)", name, c.Type, c.Length)
			}
			if c.Charset != "ascii" || c.Collation != "ascii_bin" {
				t.Errorf("%s Charset=%q Collation=%q, want ascii/ascii_bin", name, c.Charset, c.Collation)
			}
			if c.NotNull {
				t.Errorf("%s NotNull = true, want false (nullable hash)", name)
			}
			if c.Default != "" {
				t.Errorf("%s Default = %q, want empty", name, c.Default)
			}
		}
	})

	// F2 回归: search_bloom 与 checkpoint 无 DEFAULT
	t.Run("F2_no_default_on_search_bloom_and_checkpoint", func(t *testing.T) {
		tbTraffic := byName["traffic_exchanges"]
		if tbTraffic == nil {
			t.Fatalf("traffic_exchanges missing")
		}
		cBloom := findCol(tbTraffic, "search_bloom")
		if cBloom == nil {
			t.Fatalf("search_bloom missing")
		}
		if cBloom.Default != "" {
			t.Errorf("traffic_exchanges.search_bloom must NOT have default, got %q", cBloom.Default)
		}
		ddlTraffic, err := GenerateDDL(tbTraffic)
		if err != nil {
			t.Fatalf("GenerateDDL(traffic_exchanges): %v", err)
		}
		if strings.Contains(ddlTraffic, "`search_bloom` VARBINARY(8192) NOT NULL DEFAULT") {
			t.Errorf("GenerateDDL(traffic_exchanges) should not contain DEFAULT for search_bloom: %s", ddlTraffic)
		}

		tbStorage := byName["storage_jobs"]
		if tbStorage == nil {
			t.Fatalf("storage_jobs missing")
		}
		cCheckpoint := findCol(tbStorage, "checkpoint")
		if cCheckpoint == nil {
			t.Fatalf("checkpoint missing")
		}
		if cCheckpoint.Default != "" {
			t.Errorf("storage_jobs.checkpoint must NOT have default, got %q", cCheckpoint.Default)
		}
		ddlStorage, err := GenerateDDL(tbStorage)
		if err != nil {
			t.Fatalf("GenerateDDL(storage_jobs): %v", err)
		}
		if strings.Contains(ddlStorage, "`checkpoint` VARBINARY(1024) NOT NULL DEFAULT") {
			t.Errorf("GenerateDDL(storage_jobs) should not contain DEFAULT for checkpoint: %s", ddlStorage)
		}
	})

	// F3 回归: idx_traffic_bodies_blobs 冗余索引已删除，但保持 uq_traffic_exchange_bodies 及 req/resp 索引
	t.Run("F3_redundant_index_deleted_and_identity_preserved", func(t *testing.T) {
		tb := byName["traffic_exchange_bodies"]
		if tb == nil {
			t.Fatalf("traffic_exchange_bodies missing")
		}
		for _, ix := range tb.Indexes {
			if ix.Name == "idx_traffic_bodies_blobs" {
				t.Fatalf("redundant index idx_traffic_bodies_blobs must be deleted")
			}
		}
		// 断言 uq_traffic_exchange_bodies, idx_traffic_blobs_req, idx_traffic_blobs_resp 均完好
		expectedIndexes := map[string]bool{
			"uq_traffic_exchange_bodies": false,
			"idx_traffic_blobs_req":      false,
			"idx_traffic_blobs_resp":     false,
		}
		for _, ix := range tb.Indexes {
			if _, ok := expectedIndexes[ix.Name]; ok {
				expectedIndexes[ix.Name] = true
			}
		}
		for name, found := range expectedIndexes {
			if !found {
				t.Errorf("expected index %s was not found", name)
			}
		}
	})

	t.Run("I3_storage_job_pins_size_bytes_unsigned", func(t *testing.T) {
		tb := byName["storage_job_pins"]
		if tb == nil {
			t.Fatalf("storage_job_pins missing")
		}
		c := findCol(tb, "size_bytes")
		if c == nil {
			t.Fatalf("size_bytes missing")
		}
		if c.Type != TBigUnsigned {
			t.Errorf("size_bytes Type = %v, want TBigUnsigned", c.Type)
		}
		if !c.NotNull {
			t.Errorf("size_bytes NotNull = false, want true")
		}
		if c.Default != "0" {
			t.Errorf("size_bytes Default = %q, want '0'", c.Default)
		}
		if sql := c.SQLType(); sql != "BIGINT UNSIGNED" {
			t.Errorf("size_bytes SQLType = %q, want 'BIGINT UNSIGNED'", sql)
		}
	})

	t.Run("I4_traffic_index_chunks_text_utf8mb4", func(t *testing.T) {
		tb := byName["traffic_index_chunks"]
		if tb == nil {
			t.Fatalf("traffic_index_chunks missing")
		}
		c := findCol(tb, "text")
		if c == nil {
			t.Fatalf("text missing")
		}
		if c.Type != TText {
			t.Errorf("text Type = %v, want TText (MEDIUMTEXT)", c.Type)
		}
		if !c.NotNull {
			t.Errorf("text NotNull = false, want true")
		}
		if c.Default != "" {
			t.Errorf("text Default = %q, want empty", c.Default)
		}
		if c.Charset != "utf8mb4" || c.Collation != "utf8mb4_bin" {
			t.Errorf("text Charset=%q Collation=%q, want utf8mb4/utf8mb4_bin", c.Charset, c.Collation)
		}
		wantSQL := "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
		if sql := c.SQLType(); sql != wantSQL {
			t.Errorf("text SQLType = %q, want %q", sql, wantSQL)
		}
	})

	t.Run("I5_llm_cache_column_names", func(t *testing.T) {
		// llm_records: cache_read / cache_write 为 INTEGER NULL, 绝无 _tokens 别名
		records := byName["llm_records"]
		if records == nil {
			t.Fatalf("llm_records missing")
		}
		for _, bad := range []string{"cache_read_tokens", "cache_write_tokens"} {
			if findCol(records, bad) != nil {
				t.Errorf("llm_records should not contain %q", bad)
			}
		}
		for _, name := range []string{"cache_read", "cache_write"} {
			c := findCol(records, name)
			if c == nil {
				t.Fatalf("llm_records missing column %s", name)
			}
			if c.Type != TInt {
				t.Errorf("llm_records.%s Type = %v, want TInt", name, c.Type)
			}
			if c.NotNull {
				t.Errorf("llm_records.%s NotNull = true, want false", name)
			}
			if c.Default != "" {
				t.Errorf("llm_records.%s Default = %q, want empty", name, c.Default)
			}
		}

		// llm_usage: cache_read / cache_write 为 INTEGER NOT NULL DEFAULT 0, 绝无 _tokens 别名
		usage := byName["llm_usage"]
		if usage == nil {
			t.Fatalf("llm_usage missing")
		}
		for _, bad := range []string{"cache_read_tokens", "cache_write_tokens"} {
			if findCol(usage, bad) != nil {
				t.Errorf("llm_usage should not contain %q", bad)
			}
		}
		for _, name := range []string{"cache_read", "cache_write"} {
			c := findCol(usage, name)
			if c == nil {
				t.Fatalf("llm_usage missing column %s", name)
			}
			if c.Type != TInt {
				t.Errorf("llm_usage.%s Type = %v, want TInt", name, c.Type)
			}
			if !c.NotNull {
				t.Errorf("llm_usage.%s NotNull = false, want true", name)
			}
			if c.Default != "0" {
				t.Errorf("llm_usage.%s Default = %q, want '0'", name, c.Default)
			}
		}
	})
}
