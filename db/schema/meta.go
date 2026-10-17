// Package schema is the declarative catalog + TiDB DDL generator for all 65 tables
// (spec: docs/superpowers/specs/2026-10-10-native-tidb-design.md §2.4/§3/§6.1/§7).
//
// 包约束（Global Constraints）：
//   - 实现文件仅依赖 Go 标准库（验收测试另用 database/sql + mysql driver，P01 已引入）；
//   - 不 import 既有 db 包，不修改 db 包任何文件；
//   - 目录顺序 = DDL 执行顺序 = 验收顺序（固定，不排序）。
package schema

import "strconv"

// ColumnType is a TiDB column type (mapping source: spec §2.2 类型与方言矩阵).
type ColumnType int

const (
	TBool        ColumnType = iota // TINYINT(1)（原 BOOLEAN）
	TTiny                          // TINYINT（mode/state/kind 枚举，无显示宽度）
	TSmall                         // SMALLINT
	TInt                           // INT（计数/秒/版本，保留符号）
	TIntUnsigned                   // INT UNSIGNED
	TBig                           // BIGINT（符号：计数/ts/timeout）
	TBigUnsigned                   // BIGINT UNSIGNED（实体引用/PK/ordinal/profile_id/row_id）
	TDouble                        // DOUBLE（原 DOUBLE PRECISION）
	TDate3                         // DATETIME(3)（原 TIMESTAMPTZ，UTC）
	TText                          // MEDIUMTEXT（原 TEXT/CIDR/自由长键）
	TJSON                          // MEDIUMTEXT JSON 载荷（原 JSONB / BIGINT[] / TEXT[] / JSONB[]）
	TVarchar                       // VARCHAR(n)（短有界键）
	TChar                          // CHAR(n)（UUID/hash/自然有界键）
	TBinary                        // BINARY(n)
	TVarbinary                     // VARBINARY(n)
	TMediumBlob                    // MEDIUMBLOB（原始正文/规范身份载荷）
)

// Column is one declarative column.
type Column struct {
	Name      string
	Type      ColumnType
	Length    int // TVarchar/TChar/TBinary/TVarbinary 用
	NotNull   bool
	Default   string // SQL 字面量：0 / 'open' / '[]' / CURRENT_TIMESTAMP(3)；TText/TJSON/TMediumBlob 的取值仅为声明期元数据，不渲染进 DDL（见 TextN/JSONN 与 renderColumnFor）
	OnUpdate  string // "CURRENT_TIMESTAMP(3)" 或 ""
	Charset   string // "ascii" 或 ""（继承表默认 utf8mb4）
	Collation string // "ascii_bin" 或 ""
	AutoInc   bool   // 仅 PK 列（AUTO_INCREMENT AUTO_ID_CACHE=1 由生成器附加）
	Generated string // STORED 生成列表达式，"" = 普通列
}

// IndexPart is one index part (TEXT 类可声明字符 prefix，§3.3).
type IndexPart struct {
	Column string
	Prefix int // 0 = 整列
	Desc   bool
}

// Index is a named index (Unique=true → UNIQUE KEY). 物理 PRIMARY KEY 不在此列，
// 由生成器从 Table.PKColumn 附加。
type Index struct {
	Name   string
	Unique bool
	Parts  []IndexPart
}

// IndexBytes calculates the total byte budget of this index on the given table.
func (ix Index) IndexBytes(t *Table) int {
	return IndexBytes(t, ix)
}

// Table is one declarative table (单列 BIGINT UNSIGNED 物理主键).
type Table struct {
	Name          string
	Comment       string
	PKColumn      string // "id" | "ordinal" | "profile_id" | "row_id"
	AutoIncrement bool   // 64 表 true；llm_profile_health false
	Columns       []Column
	Indexes       []Index
}

// ---- 列助手 ----

// C is a generic column.
func C(name string, dt ColumnType, length int) Column {
	return Column{Name: name, Type: dt, Length: length}
}

// CPK is a single-column BIGINT UNSIGNED physical PK column.
func CPK(name string) Column {
	return Column{Name: name, Type: TBigUnsigned, NotNull: true}
}

// CRef is a nullable entity reference (BIGINT UNSIGNED NULL；清空必须显式 NULL，0 不冒充空引用).
func CRef(name string) Column { return C(name, TBigUnsigned, 0) }

// CRefN is a NOT NULL entity reference.
func CRefN(name string) Column {
	c := C(name, TBigUnsigned, 0)
	c.NotNull = true
	return c
}

// BigNN is a signed BIGINT NOT NULL (no default; Go 调用方必须显式给值).
func BigNN(name string) Column {
	c := C(name, TBig, 0)
	c.NotNull = true
	return c
}

// Big is a signed BIGINT NULL.
func Big(name string) Column { return C(name, TBig, 0) }

// BigN is a signed BIGINT NOT NULL with default literal.
func BigN(name, def string) Column {
	c := C(name, TBig, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// IntNN is an INT NOT NULL (no default; Go 调用方必须显式给值).
func IntNN(name string) Column {
	c := C(name, TInt, 0)
	c.NotNull = true
	return c
}

// Int is an INT NULL.
func Int(name string) Column { return C(name, TInt, 0) }

// IntN is an INT NOT NULL with default literal.
func IntN(name, def string) Column {
	c := C(name, TInt, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// IntUN is an INT UNSIGNED NOT NULL with default literal.
func IntUN(name, def string) Column {
	c := C(name, TIntUnsigned, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// BigintUN is a BIGINT UNSIGNED NOT NULL with default value (R2-I1：规格 §7.5/§7.6
// 要求 maintenance.revision / storage_jobs.segment_seq / storage_job_pins.file_seq /
// op_outcomes.segment_seq 均为 BIGINT UNSIGNED）。
func BigintUN(name string, def int) Column {
	c := C(name, TBigUnsigned, 0)
	c.NotNull = true
	c.Default = strconv.Itoa(def)
	return c
}

// Bool is a TINYINT(1) NULL.
func Bool(name string) Column { return C(name, TBool, 0) }

// BoolN is a TINYINT(1) NOT NULL with 0/1 default.
func BoolN(name string, def1 bool) Column {
	def := "0"
	if def1 {
		def = "1"
	}
	c := C(name, TBool, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// Tiny is a TINYINT NOT NULL with default literal (mode/state/kind 枚举).
func Tiny(name, def string) Column {
	c := C(name, TTiny, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// TinyNN is a TINYINT NOT NULL (no default; mode/state/kind 枚举，Go 必须显式给值).
func TinyNN(name string) Column {
	c := C(name, TTiny, 0)
	c.NotNull = true
	return c
}

// DoubleN is a DOUBLE NOT NULL with default literal.
func DoubleN(name, def string) Column {
	c := C(name, TDouble, 0)
	c.NotNull = true
	c.Default = def
	return c
}

// Date is a DATETIME(3) NULL.
func Date(name string) Column { return C(name, TDate3, 0) }

// DateN is a DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3).
func DateN(name string) Column {
	c := C(name, TDate3, 0)
	c.NotNull = true
	c.Default = "CURRENT_TIMESTAMP(3)"
	return c
}

// DateNOU is a DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3).
func DateNOU(name string) Column {
	c := DateN(name)
	c.OnUpdate = "CURRENT_TIMESTAMP(3)"
	return c
}

// Text is a MEDIUMTEXT NULL (原 TEXT/CIDR；原始正文用 Blob).
func Text(name string) Column { return C(name, TText, 0) }

// TextNN is a MEDIUMTEXT NOT NULL (no default; Go 调用方必须显式给值).
func TextNN(name string) Column {
	c := C(name, TText, 0)
	c.NotNull = true
	return c
}

// TextN is a MEDIUMTEXT NOT NULL 列；def 自动加引号后存入 Column.Default，仅作为
// 声明期元数据（应用层默认值语义），不进入 DDL。
//
// 原因（真实 TiDB v8.5.8 实测）：该实例默认 sql_mode 含 STRICT_TRANS_TABLES，
// TiDB 8.5 严格模式禁止 TEXT/BLOB/JSON 列声明 DEFAULT（Error 1101: BLOB/TEXT/JSON
// column 不可有 default value），且不支持 DEFAULT 括号表达式默认（语法错误 1064）。
// 因此 DDL 渲染层（renderColumnFor）对 TText/TJSON 一律不输出 DEFAULT，该列渲染为
// MEDIUMTEXT NOT NULL；写入方必须显式提供值（fail closed：缺值由 TiDB 严格模式报错，
// 不会静默补空串）。
func TextN(name, def string) Column {
	c := C(name, TText, 0)
	c.NotNull = true
	c.Default = "'" + def + "'"
	return c
}

// JSON is a MEDIUMTEXT JSON payload NULL.
func JSON(name string) Column { return C(name, TJSON, 0) }

// JSONNN is a MEDIUMTEXT JSON payload NOT NULL (no default; 如 finding_retests.snapshot).
func JSONNN(name string) Column {
	c := C(name, TJSON, 0)
	c.NotNull = true
	return c
}

// JSONN is a MEDIUMTEXT JSON 载荷 NOT NULL 列；def（空对象/空数组字面量）自动加引号后
// 存入 Column.Default，仅作为声明期元数据（应用层默认值语义），不进入 DDL。
//
// 原因与 TextN 相同：TiDB 8.5 严格模式禁止 TEXT/BLOB/JSON 列声明 DEFAULT
// （Error 1101），DEFAULT 括号表达式默认亦不支持（语法错误 1064）。DDL 渲染层对
// TJSON 不输出 DEFAULT，该列渲染为 MEDIUMTEXT NOT NULL；写入方必须显式提供 JSON 值
// （fail closed：空载荷须由应用层显式写入空对象/空数组）。
func JSONN(name, def string) Column {
	c := C(name, TJSON, 0)
	c.NotNull = true
	c.Default = "'" + def + "'"
	return c
}

// Blob is a MEDIUMBLOB NULL.
func Blob(name string) Column { return C(name, TMediumBlob, 0) }

// BlobN is a MEDIUMBLOB NOT NULL.
func BlobN(name string) Column {
	c := C(name, TMediumBlob, 0)
	c.NotNull = true
	return c
}

// VarcharA is a VARCHAR(n) CHARACTER SET ascii COLLATE ascii_bin NULL.
func VarcharA(name string, n int) Column {
	return Column{Name: name, Type: TVarchar, Length: n, Charset: "ascii", Collation: "ascii_bin"}
}

// VarcharAN is a VARCHAR(n) ascii_bin NOT NULL with string default.
func VarcharAN(name string, n int, def string) Column {
	c := VarcharA(name, n)
	c.NotNull = true
	c.Default = "'" + def + "'"
	return c
}

// CharA is a CHAR(n) CHARACTER SET ascii COLLATE ascii_bin NOT NULL (UUID/hash/自然有界键).
func CharA(name string, n int) Column {
	return Column{Name: name, Type: TChar, Length: n, Charset: "ascii", Collation: "ascii_bin", NotNull: true}
}

// Bin is a BINARY(n) NULL.
func Bin(name string, n int) Column { return C(name, TBinary, n) }

// BinN is a BINARY(n) NOT NULL.
func BinN(name string, n int) Column {
	c := C(name, TBinary, n)
	c.NotNull = true
	return c
}

// VarBin is a VARBINARY(n) NULL.
func VarBin(name string, n int) Column { return C(name, TVarbinary, n) }

// VarBinN is a VARBINARY(n) NOT NULL with default literal.
func VarBinN(name string, n int, def string) Column {
	c := C(name, TVarbinary, n)
	c.NotNull = true
	c.Default = def
	return c
}

// Gen is a STORED generated column (表达式必须确定；字符集/排序规则可预设).
func Gen(name string, dt ColumnType, length int, expr string) Column {
	return Column{Name: name, Type: dt, Length: length, Generated: expr}
}

// ---- 索引助手 ----

// IX is a named index.
func IX(name string, unique bool, parts ...IndexPart) Index {
	return Index{Name: name, Unique: unique, Parts: parts}
}

// P is an index part (整列).
func P(name string) IndexPart { return IndexPart{Column: name} }

// PP is an index part with character prefix (TEXT 类，§3.3).
func PP(name string, n int) IndexPart { return IndexPart{Column: name, Prefix: n} }

// PD is a descending index part (仅普通索引).
func PD(name string) IndexPart { return IndexPart{Column: name, Desc: true} }

// ---- 表助手 ----

// T is a table constructor (目录与测试共用).
func T(name, pk string, autoInc bool, cols []Column, idx []Index) *Table {
	return &Table{Name: name, PKColumn: pk, AutoIncrement: autoInc, Columns: cols, Indexes: idx}
}
