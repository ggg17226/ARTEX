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

	// TMediumBlob tests
	blobCol := Blob("b")
	if got := blobCol.KeyBytes(0); got != -1 {
		t.Errorf("mediumblob prefix 0 = %d, want -1", got)
	}
	if got := blobCol.KeyBytes(191); got != 191 {
		t.Errorf("mediumblob prefix 191 = %d, want 191", got)
	}
	if got := blobCol.KeyBytes(3073); got != 3073 {
		t.Errorf("mediumblob prefix 3073 = %d, want 3073", got)
	}
}

func TestTableValidate(t *testing.T) {
	ok := T("t", "id", true,
		[]Column{CPK("id"), CRef("a"), TextN("note", "x"), Blob("body")},
		[]Index{IX("uq_t_a", true, P("a")), IX("ix_t_blob", false, PP("body", 191))},
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
		{"index on pk", T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("u", true, P("id"))}), "duplicate the primary key column"},
		{"blob without prefix", T("t", "id", true, []Column{CPK("id"), Blob("payload")}, []Index{IX("ix_blob", false, P("payload"))}), "requires prefix length"},
		{"blob prefix 3073 exceeds limit", T("t", "id", true, []Column{CPK("id"), Blob("payload")}, []Index{IX("ix_blob", false, PP("payload", 3073))}), "exceeds TiDB limit 3072"},
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

	// M2: 收窄规则边界回归测试
	// 负例：单列纯 PK 索引（无论唯一性、无论 ASC/DESC）均拒绝
	negCases := []struct {
		name    string
		tbl     *Table
		wantErr string
	}{
		{
			name:    "single non-unique pk asc",
			tbl:     T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("ix_id", false, P("id"))}),
			wantErr: "duplicate the primary key column",
		},
		{
			name:    "single non-unique pk desc",
			tbl:     T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("ix_id_desc", false, PD("id"))}),
			wantErr: "duplicate the primary key column",
		},
		{
			name:    "single unique pk desc",
			tbl:     T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("uq_id_desc", true, PD("id"))}),
			wantErr: "duplicate the primary key column",
		},
	}
	for _, c := range negCases {
		err := c.tbl.Validate()
		if err == nil {
			t.Errorf("neg %s: expected error, got nil", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("neg %s: error %q, want containing %q", c.name, err, c.wantErr)
		}
	}

	// 正例：复合索引包含 PK 列（PK 首位、尾位、唯一复合、非唯一复合、PK 尾位 DESC）均被接受
	posCases := []struct {
		name string
		tbl  *Table
	}{
		{
			name: "composite pk head unique",
			tbl:  T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("uq_id_a", true, P("id"), P("a"))}),
		},
		{
			name: "composite pk head non-unique",
			tbl:  T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("ix_id_a", false, P("id"), P("a"))}),
		},
		{
			name: "composite pk tail unique",
			tbl:  T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("uq_a_id", true, P("a"), P("id"))}),
		},
		{
			name: "composite pk tail non-unique",
			tbl:  T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("ix_a_id", false, P("a"), P("id"))}),
		},
		{
			name: "composite pk tail desc",
			tbl:  T("t", "id", true, []Column{CPK("id"), CRefN("a")}, []Index{IX("ix_a_id_desc", false, P("a"), PD("id"))}),
		},
	}
	for _, c := range posCases {
		if err := c.tbl.Validate(); err != nil {
			t.Errorf("pos %s: unexpected error: %v", c.name, err)
		}
	}

	// 断言复合索引含 PK 时原有检查仍执行：unbounded TEXT part 仍报错、3072 字节预算仍生效
	compositeCheckCases := []struct {
		name    string
		tbl     *Table
		wantErr string
	}{
		{
			name:    "composite with pk and unbounded text",
			tbl:     T("t", "id", true, []Column{CPK("id"), Text("note")}, []Index{IX("ix_pk_text", false, P("id"), P("note"))}),
			wantErr: "unbounded",
		},
		{
			name:    "composite with pk and blob exceeding 3072 limit",
			tbl:     T("t", "id", true, []Column{CPK("id"), Blob("payload")}, []Index{IX("ix_pk_blob", false, P("id"), PP("payload", 3072))}),
			wantErr: "exceeds TiDB limit 3072",
		},
	}
	for _, c := range compositeCheckCases {
		err := c.tbl.Validate()
		if err == nil {
			t.Errorf("composite-check %s: expected error, got nil", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("composite-check %s: error %q, want containing %q", c.name, err, c.wantErr)
		}
	}
}
