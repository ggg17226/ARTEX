package schema

import (
	"strings"
	"testing"
)

func TestColumnSQLType(t *testing.T) {
	cases := []struct {
		name string
		col  Column
		want string
	}{
		{"bigint unsigned", Column{Name: "id", Type: TBigUnsigned}, "BIGINT UNSIGNED"},
		{"bigint signed", Column{Name: "ts", Type: TBig}, "BIGINT"},
		{"int", Column{Name: "n", Type: TInt}, "INT"},
		{"int unsigned", Column{Name: "n", Type: TIntUnsigned}, "INT UNSIGNED"},
		{"tinyint", Column{Name: "m", Type: TTiny}, "TINYINT"},
		{"tinyint1", Column{Name: "b", Type: TBool}, "TINYINT(1)"},
		{"double", Column{Name: "r", Type: TDouble}, "DOUBLE"},
		{"datetime3", Column{Name: "t", Type: TDate3}, "DATETIME(3)"},
		{"mediumtext", Column{Name: "x", Type: TText}, "MEDIUMTEXT"},
		{"json", Column{Name: "j", Type: TJSON}, "MEDIUMTEXT"},
		{"varchar ascii", VarcharA("session_key", 128), "VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin"},
		{"char64 ascii", CharA("hash", 64), "CHAR(64) CHARACTER SET ascii COLLATE ascii_bin"},
		{"binary32", BinN("identity_hash", 32), "BINARY(32)"},
		{"varbinary8192", VarBinN("search_bloom", 8192, ""), "VARBINARY(8192)"},
		{"mediumblob", Column{Name: "body", Type: TMediumBlob}, "MEDIUMBLOB"},
		{"smallint", Column{Name: "s", Type: TSmall}, "SMALLINT"},
	}
	for _, c := range cases {
		if got := c.col.SQLType(); got != c.want {
			t.Errorf("SQLType(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestIndexPartBytes(t *testing.T) {
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, P("id"))); got != 8 {
		t.Errorf("bigint part = %d, want 8", got)
	}
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, P("k"))); got != 64 {
		t.Errorf("ascii varchar(64) part = %d, want 64", got)
	}
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, P("note"), P("id"))); got != 65536+8 {
		t.Errorf("full mediumtext part = %d, want 65536", got)
	}
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, PP("note", 191), P("id"))); got != 764+8 {
		t.Errorf("prefix 191 utf8mb4 part = %d, want 764", got)
	}
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, P("h"))); got != 32 {
		t.Errorf("binary(32) part = %d, want 32", got)
	}
	if got := IndexBytes(T("t", "id", true, []Column{CPK("id"), CRef("a"), VarcharAN("k", 64, ""), Text("note"), BinN("h", 32)}, nil), IX("x", false, P("missing"))); got != -1 {
		t.Errorf("unknown column part = %d, want -1", got)
	}
}

func TestTableValidate(t *testing.T) {
	ok := T("t", "id", true,
		[]Column{CPK("id"), CRef("a"), TextN("note", "x")},
		[]Index{IX("uq_t_a", true, P("a"))},
	)
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid table rejected: %v", err)
	}

	cases := []struct {
		name    string
		tbl     *Table
		wantErr string
	}{
		{"pk missing", T("t", "id", true, []Column{CRefN("a")}, nil), "primary key column not found"},
		{"pk type", T("t", "id", true, []Column{{Name: "id", Type: TBig, NotNull: true}}, nil), "BIGINT UNSIGNED"},
		{"two autoinc", T("t", "id", true, []Column{CPK("id"), func() Column { c := CPK("row_id"); c.AutoInc = true; return c }()}, nil), "two AUTO_INCREMENT"},
		{"dup column", T("t", "id", true, []Column{CPK("id"), CRefN("a"), CRefN("a")}, nil), "duplicate column"},
		{"dup index", T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("u", true, P("a")), IX("u", true, P("a"))}), "duplicate index name"},
		{"text full index", T("t", "id", true, []Column{CPK("id"), Text("note")}, []Index{IX("u", true, P("note"))}), "unbounded"},
		{"index on pk", T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("u", true, P("id"))}), "primary key column"},
	}
	for _, c := range cases {
		err := c.tbl.Validate()
		if err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: error %q, want containing %q", c.name, err, c.wantErr)
		}
	}
}
