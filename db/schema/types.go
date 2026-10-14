package schema

import (
	"fmt"
)

// SQLType renders the column type segment (charset/collation included when set).
func (c Column) SQLType() string {
	var s string
	switch c.Type {
	case TBool:
		s = "TINYINT(1)"
	case TTiny:
		s = "TINYINT"
	case TSmall:
		s = "SMALLINT"
	case TInt:
		s = "INT"
	case TIntUnsigned:
		s = "INT UNSIGNED"
	case TBig:
		s = "BIGINT"
	case TBigUnsigned:
		s = "BIGINT UNSIGNED"
	case TDouble:
		s = "DOUBLE"
	case TDate3:
		s = "DATETIME(3)"
	case TText, TJSON:
		s = "MEDIUMTEXT"
	case TMediumBlob:
		s = "MEDIUMBLOB"
	case TVarchar:
		s = fmt.Sprintf("VARCHAR(%d)", c.Length)
	case TChar:
		s = fmt.Sprintf("CHAR(%d)", c.Length)
	case TBinary:
		s = fmt.Sprintf("BINARY(%d)", c.Length)
	case TVarbinary:
		s = fmt.Sprintf("VARBINARY(%d)", c.Length)
	default:
		panic(fmt.Sprintf("unknown ColumnType %d (column %s)", int(c.Type), c.Name))
	}
	if c.Charset != "" {
		s += " CHARACTER SET " + c.Charset
		if c.Collation != "" {
			s += " COLLATE " + c.Collation
		}
	}
	return s
}

// IsString reports whether the column is a string family type.
func (c Column) IsString() bool {
	switch c.Type {
	case TText, TJSON, TVarchar, TChar:
		return true
	}
	return false
}

// KeyBytes is the byte width of this column in one index part, toward the
// 3072-byte budget (spec §3.3). prefix 0 = 整列；字符串类按字符数 × 字符集字节
// (utf8mb4=4, ascii=1, 缺省 4)；定宽类型按类型固定字节。返回值 <0 = 该列不可用
// （如 MEDIUMBLOB 无 prefix）。
func (c Column) KeyBytes(prefix int) int {
	switch c.Type {
	case TBig, TBigUnsigned:
		return 8
	case TInt, TIntUnsigned:
		return 4
	case TTiny, TBool:
		return 1
	case TSmall:
		return 2
	case TDouble:
		return 8
	case TDate3:
		return 7
	case TBinary, TVarbinary:
		n := c.Length
		if prefix > 0 && prefix < n {
			n = prefix
		}
		return n
	case TMediumBlob:
		if prefix <= 0 {
			return -1
		}
		return prefix
	case TVarchar, TChar:
		w := 4
		if c.Charset == "ascii" {
			w = 1
		}
		n := c.Length
		if prefix > 0 && prefix < n {
			n = prefix
		}
		return n * w
	case TText, TJSON:
		w := 4
		if c.Charset == "ascii" {
			w = 1
		}
		if prefix > 0 {
			return prefix * w
		}
		return 16384 * w // MEDIUMTEXT 整列 = 16384 字符（utf8mb4 → 65536 字节，必超预算）
	default:
		return -1
	}
}

// IndexBytes is the total bytes of one index (不含编码少量开销；预算口径 spec §3.3)。
// 返回 -1 = 有 part 不可解析（列缺失）。
func IndexBytes(t *Table, ix Index) int {
	sum := 0
	for _, p := range ix.Parts {
		var col *Column
		for i := range t.Columns {
			if t.Columns[i].Name == p.Column {
				col = &t.Columns[i]
				break
			}
		}
		if col == nil {
			return -1
		}
		b := col.KeyBytes(p.Prefix)
		if b < 0 {
			return -1
		}
		sum += b
	}
	return sum
}

// Validate performs the structural self-check of one declarative table:
// PK 列存在且为 BIGINT UNSIGNED；至多 1 个 AUTO_INCREMENT 且必须是 PK 列
// (一表两列 AUTO_INCREMENT 实测报 1075，§11.1)；列名/索引名不重复；索引 part 可解析；
// 仅禁止单列且完全由物理 PK 构成的二级索引；复合索引允许包含 PK 列（TiDB 8.5 聚簇二级索引显式含 PK 列时不重复附加 handle，非唯一索引零冗余）。
func (t *Table) Validate() error {
	var foundPK bool
	for i := range t.Columns {
		c := &t.Columns[i]
		if c.Name == t.PKColumn {
			if c.Type != TBigUnsigned {
				return fmt.Errorf("table %s: primary key column %s must be BIGINT UNSIGNED", t.Name, c.Name)
			}
			foundPK = true
		}
	}
	if !foundPK {
		return fmt.Errorf("table %s: primary key column not found", t.Name)
	}
	auto := 0
	if t.AutoIncrement {
		auto++ // PK 列自增（生成器据此附加 AUTO_INCREMENT AUTO_ID_CACHE=1）
	}
	var nonPKAuto []string
	for i := range t.Columns {
		c := &t.Columns[i]
		if c.AutoInc && c.Name != t.PKColumn {
			auto++
			nonPKAuto = append(nonPKAuto, c.Name)
		}
	}
	if auto > 1 {
		return fmt.Errorf("table %s: two AUTO_INCREMENT columns", t.Name)
	}
	for _, n := range nonPKAuto {
		return fmt.Errorf("table %s: AUTO_INCREMENT column %s is not the primary key", t.Name, n)
	}
	seen := make(map[string]bool, len(t.Columns))
	for i := range t.Columns {
		n := t.Columns[i].Name
		if seen[n] {
			return fmt.Errorf("table %s: duplicate column %s", t.Name, n)
		}
		seen[n] = true
	}
	seenIdx := make(map[string]bool, len(t.Indexes))
	for _, ix := range t.Indexes {
		if seenIdx[ix.Name] {
			return fmt.Errorf("table %s: duplicate index name %s", t.Name, ix.Name)
		}
		seenIdx[ix.Name] = true
		// TiDB 8.5 默认聚簇表的非唯一二级索引显式包含 PK 列时物理不再重复附加 handle（非唯一索引零冗余，DESC 尾缀表达混合方向排序语义），故允许复合索引包含 PK 列；仅禁止单列索引与 PK 完全重复。
		if len(ix.Parts) == 1 && ix.Parts[0].Column == t.PKColumn {
			return fmt.Errorf("table %s: index %s must not duplicate the primary key column", t.Name, ix.Name)
		}
		for _, p := range ix.Parts {
			for i := range t.Columns {
				c := &t.Columns[i]
				if c.Name == p.Column {
					if c.Type == TMediumBlob && p.Prefix <= 0 {
						return fmt.Errorf("table %s index %s: BLOB column %s requires prefix length", t.Name, ix.Name, p.Column)
					}
					if (c.Type == TText || c.Type == TJSON) && p.Prefix == 0 {
						return fmt.Errorf("table %s: index %s part %s is TEXT without prefix (unbounded)", t.Name, ix.Name, p.Column)
					}
				}
			}
		}
		totalBytes := ix.IndexBytes(t)
		if totalBytes < 0 {
			return fmt.Errorf("table %s: index %s has an unbounded or unresolvable part", t.Name, ix.Name)
		}
		if totalBytes > 3072 {
			return fmt.Errorf("table %s index %s: index bytes %d exceeds TiDB limit 3072", t.Name, ix.Name, totalBytes)
		}
	}
	return nil
}
