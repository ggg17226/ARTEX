package schema

import (
	"fmt"
	"strings"
)

// quoteIdentifier encloses an identifier in backticks, escaping any embedded backticks.
func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// GenerateDDL compiles one declarative table to a TiDB-native
// CREATE TABLE IF NOT EXISTS statement (no trailing semicolon).
//
// 口径与约束：
// - 标识符全部反引号安全引用；
// - 先执行 t.Validate()，结构不合法直接返回错误；
// - 列区保留 NOT NULL / DEFAULT / ON UPDATE，但 TText/TJSON 不输出 DEFAULT（见 renderColumnFor，TiDB 8.5 严格模式 Error 1101）；
// - 生成列渲染 GENERATED ALWAYS AS (expr) STORED，不附加 NOT NULL/DEFAULT/ON UPDATE/AUTO_INCREMENT；
// - 自增表 (t.AutoIncrement=true) 的 PK 列追加 AUTO_INCREMENT；
// - AUTO_ID_CACHE 绝不出现在列/索引区，仅在闭括号后的表选项区根据 t.AutoIncrement 追加 AUTO_ID_CACHE=1；
// - 表选项统一为 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin（自增表后追加 AUTO_ID_CACHE=1）；
// - 无 FOREIGN KEY / REFERENCES / CHECK / AUTO_RANDOM / CREATE DATABASE。
func GenerateDDL(t *Table) (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}
	lines := make([]string, 0, len(t.Columns)+1+len(t.Indexes))
	for i := range t.Columns {
		lines = append(lines, "  "+renderColumnFor(t, t.Columns[i]))
	}
	lines = append(lines, "  PRIMARY KEY ("+quoteIdentifier(t.PKColumn)+")")
	for _, ix := range t.Indexes {
		lines = append(lines, "  "+renderIndex(ix))
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS " + quoteIdentifier(t.Name) + " (\n")
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	if t.AutoIncrement {
		b.WriteString(" AUTO_ID_CACHE=1")
	}
	return b.String(), nil
}

// renderColumnFor renders one column definition line.
// 生成列渲染为 GENERATED ALWAYS AS (expr) STORED；普通列依次附加 NOT NULL / DEFAULT / ON UPDATE。
// 当且仅当 c.Name == t.PKColumn 且 t.AutoIncrement 为 true 时，追加 AUTO_INCREMENT。
// CACHE 绝对不在此处输出。
//
// DEFAULT 例外（真实 TiDB v8.5.8 实测）：TText/TJSON（均渲染为 MEDIUMTEXT）与
// TMediumBlob（MEDIUMBLOB）一律不输出 DEFAULT。该实例默认 sql_mode 含
// STRICT_TRANS_TABLES，声明 MEDIUMTEXT NOT NULL DEFAULT 空串 直接失败
// （Error 1101 (42000): BLOB/TEXT/JSON column can't have a default value），
// 且 TiDB 8.5 不支持 DEFAULT 括号表达式默认（语法错误 1064）。
// 这些列的 Column.Default 只是声明期元数据（应用层默认值语义），写入方必须显式
// 提供值（fail closed）。其余类型的 DEFAULT 渲染保持原样。
func renderColumnFor(t *Table, c Column) string {
	var b strings.Builder
	b.WriteString(quoteIdentifier(c.Name) + " " + c.SQLType())
	if c.Generated != "" {
		b.WriteString(" GENERATED ALWAYS AS (" + c.Generated + ") STORED")
		return b.String()
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	if c.Default != "" && !rejectsColumnDefault(c.Type) {
		b.WriteString(" DEFAULT " + c.Default)
	}
	if c.OnUpdate != "" {
		b.WriteString(" ON UPDATE " + c.OnUpdate)
	}
	if c.Name == t.PKColumn && t.AutoIncrement {
		b.WriteString(" AUTO_INCREMENT")
	}
	return b.String()
}

// rejectsColumnDefault reports whether TiDB 8.5 rejects a column-level DEFAULT
// for this column type (TEXT/BLOB 家族：MEDIUMTEXT / MEDIUMBLOB)。
// TMediumBlob 目录中从不携带 Default（Blob/BlobN 助手不设置），列入此处只为
// 守住同一条 TiDB 红线，防止未来回归；对当前 65 表 DDL 输出零影响。
func rejectsColumnDefault(ct ColumnType) bool {
	switch ct {
	case TText, TJSON, TMediumBlob:
		return true
	}
	return false
}

// renderIndex renders one named index line (UNIQUE KEY / KEY，支持 prefix 与 DESC).
func renderIndex(ix Index) string {
	parts := make([]string, 0, len(ix.Parts))
	for _, p := range ix.Parts {
		s := quoteIdentifier(p.Column)
		if p.Prefix > 0 {
			s += fmt.Sprintf("(%d)", p.Prefix)
		}
		if p.Desc {
			s += " DESC"
		}
		parts = append(parts, s)
	}
	kind := "KEY"
	if ix.Unique {
		kind = "UNIQUE KEY"
	}
	return kind + " " + quoteIdentifier(ix.Name) + " (" + strings.Join(parts, ", ") + ")"
}

// GenerateStatements compiles all 65 tables (AllTables 固定顺序) to statements
// without semicolons.
func GenerateStatements() ([]string, error) {
	all := AllTables()
	stmts := make([]string, 0, len(all))
	for _, tb := range all {
		s, err := GenerateDDL(tb)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// GenerateAllDDL joins all 65 statements into one idempotent DDL script
// (";\n" separated, trailing ";\n"). 不含 CREATE DATABASE。
func GenerateAllDDL() (string, error) {
	stmts, err := GenerateStatements()
	if err != nil {
		return "", err
	}
	return strings.Join(stmts, ";\n") + ";\n", nil
}

// SplitStatements splits a DDL script (as produced by GenerateAllDDL) back
// into individual statements without trailing semicolons.
func SplitStatements(script string) []string {
	body := strings.TrimSuffix(script, ";\n")
	if body == "" {
		return nil
	}
	var out []string
	for _, s := range strings.Split(body, ";\n") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
