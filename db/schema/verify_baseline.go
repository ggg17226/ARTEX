package schema

// 纯只读元数据基线检查器（P08-01 Task 1.2 / spec §3.1.2）。
//
// 设计红线：
//  1. 纯只读。本文件只允许对 information_schema 的 tables / columns / statistics
//     三个字典对象执行单条 SELECT；严禁任何 DML（INSERT/UPDATE/DELETE）、DDL、
//     写入探针（verify_probes.go 那类 INSERT+DELETE 往返）以及多语句拼接。
//  2. 不可忽略。任何缺失/漂移都必须返回 error，且包装 ErrBaselineMismatch，
//     由启动时序直接中止并提示 `artex db-migrate --prepare`。
//  3. 已知差异白名单。真实 TiDB 把目录中 21 个含 DESC 部件的索引按 ASC 等价方式
//     存储（COLLATION 报 'A'/NULL），本检查器容忍该方向差异并计数；除此之外方向
//     漂移（目录 ASC 却存成 DESC）仍判红。
//  4. 生成列表达式严格比较（Review R1）。表达式比较是**有序**词法 token 的"等长 + 逐位
//     相等"，只容忍空白、大小写、标识符反引号、字符集引导符、字符串引号风格五类书写差异；
//     运算符、括号、逗号、关键字、参数顺序与重复次数全部参与判定，绝不排序/去重/集合比较。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// 基线计数（spec §3.1.2 固定口径，也是 AllTables() 目录的自洽门）。
// 目录若新增/删除表列或唯一键，必须同步更新这四个常量并同步 spec。
const (
	// BaselineTableCount 是 AllTables() 必须提供的基线表数量。
	BaselineTableCount = 65
	// BaselineColumnCount 是 65 张表的列总数。
	BaselineColumnCount = 729
	// BaselineUniqueIndexCount 是基线唯一键（UNIQUE 索引）数量。
	BaselineUniqueIndexCount = 62
	// BaselineDescIndexCount 是含 DESC 部件的索引数量，即已知 DESC→ASC 差异处数。
	BaselineDescIndexCount = 21
)

// ErrBaselineMismatch 标识"库结构与声明式基线不一致"。
// 连接失败/字典读取失败等技术性错误不包装该哨兵，便于 CLI 区分"需要 --prepare"
// 与"根本连不上"。
var ErrBaselineMismatch = errors.New("schema baseline mismatch")

// baselinePrepareHint 是失败时给运维的固定指引（spec §3.1.2 第 3 点）。
const baselinePrepareHint = "run `artex db-migrate --prepare` to create or repair the baseline schema"

// baselineMaxReportedProblems 限制单条错误里逐条列出的问题数（其余仅报总数），
// 避免整库缺失时刷屏。
const baselineMaxReportedProblems = 40

// 三条字典查询：单条、纯 SELECT、无分号、无写入。
const (
	qBaselineTables = `SELECT TABLE_NAME, TABLE_TYPE
FROM information_schema.tables
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_TYPE = 'BASE TABLE'`

	qBaselineColumns = `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, IFNULL(GENERATION_EXPRESSION, '')
FROM information_schema.columns
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, ORDINAL_POSITION`

	qBaselineIndexes = `SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE, IFNULL(COLLATION, '')
FROM information_schema.statistics
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`
)

// baselineReport 是一次核查的计数汇总（供日志与 CLI 输出）。
type baselineReport struct {
	// Tables 是在库中确认存在的基线表数量。
	Tables int
	// TotalColumns 是基线表在库中实际存在的列总数。
	TotalColumns int
	// UniqueIndexes 是核查过的基线 UNIQUE 索引数量。
	UniqueIndexes int
	// ToleratedDescIndexes 是因"真实 TiDB 按 ASC 等价存储"而被容忍的索引数量。
	ToleratedDescIndexes int
}

// VerifyBaseline 以纯只读方式核查当前库是否等于声明式 65 表基线
// （spec §3.1.2：65 表 / 729 列 / 列类型 / UNSIGNED / NOT NULL / 生成列表达式 /
// 62 个 UNIQUE 索引，并容忍 21 处 DESC 索引差异）。
//
// 契约：
//   - db == nil、上下文已取消、目录计数漂移、库结构漂移均返回非 nil error；
//   - 失败错误包装 ErrBaselineMismatch（结构漂移），并附带 --prepare 指引；
//   - 全程只执行三条 information_schema SELECT，绝不执行任何写入或探针语句。
func VerifyBaseline(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return errors.New("VerifyBaseline: nil context")
	}
	if db == nil {
		return errors.New("VerifyBaseline: db is nil (open the TiDB connection before verifying the baseline)")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("VerifyBaseline: %w", err)
	}
	tables := AllTables()
	if err := checkBaselineCatalogCounts(tables); err != nil {
		return err
	}
	if _, err := runBaselineVerify(ctx, DBQuerier{DB: db}, tables); err != nil {
		return fmt.Errorf("VerifyBaseline: %w", err)
	}
	return nil
}

// checkBaselineCatalogCounts 断言目录自身仍等于 spec §3.1.2 固定的 65/729/62/21。
// 目录漂移属于编程错误，必须显式失败而不是让线上核查出现假绿。
func checkBaselineCatalogCounts(tables []*Table) error {
	var columns, uniqueIndexes, descIndexes int
	for _, tb := range tables {
		if tb == nil {
			continue
		}
		columns += len(tb.Columns)
		for _, ix := range tb.Indexes {
			if ix.Unique {
				uniqueIndexes++
			}
			// 已知差异按"索引"计数：一个索引哪怕含多个 DESC 部件也只算一处。
			for _, p := range ix.Parts {
				if p.Desc {
					descIndexes++
					break
				}
			}
		}
	}
	if len(tables) != BaselineTableCount {
		return fmt.Errorf("baseline catalog drifted: %d tables, want %d (update spec §3.1.2 counts deliberately)", len(tables), BaselineTableCount)
	}
	if columns != BaselineColumnCount {
		return fmt.Errorf("baseline catalog drifted: %d columns, want %d (update spec §3.1.2 counts deliberately)", columns, BaselineColumnCount)
	}
	if uniqueIndexes != BaselineUniqueIndexCount {
		return fmt.Errorf("baseline catalog drifted: %d unique indexes, want %d (update spec §3.1.2 counts deliberately)", uniqueIndexes, BaselineUniqueIndexCount)
	}
	if descIndexes != BaselineDescIndexCount {
		return fmt.Errorf("baseline catalog drifted: %d indexes with DESC parts, want %d (known TiDB ASC-storage differences; update spec §3.1.2 deliberately)", descIndexes, BaselineDescIndexCount)
	}
	return nil
}

// runBaselineVerify 是 VerifyBaseline 的可测试内核：只依赖 Querier 抽象，
// 便于用 fake 字典结果集离线覆盖（不连接任何数据库）。
func runBaselineVerify(ctx context.Context, q Querier, tables []*Table) (baselineReport, error) {
	var report baselineReport
	if q == nil {
		return report, errors.New("runBaselineVerify: querier is nil")
	}
	if ctx == nil {
		return report, errors.New("runBaselineVerify: nil context")
	}
	if len(tables) == 0 {
		return report, errors.New("runBaselineVerify: empty catalog")
	}
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("runBaselineVerify: %w", err)
	}

	tableRows, err := baselineFetch(ctx, q, "tables", qBaselineTables)
	if err != nil {
		return report, fmt.Errorf("read information_schema.tables: %w", err)
	}
	columnRows, err := baselineFetch(ctx, q, "columns", qBaselineColumns)
	if err != nil {
		return report, fmt.Errorf("read information_schema.columns: %w", err)
	}
	indexRows, err := baselineFetch(ctx, q, "statistics", qBaselineIndexes)
	if err != nil {
		return report, fmt.Errorf("read information_schema.statistics: %w", err)
	}

	c := &baselineChecker{problems: make([]string, 0, 16)}

	// ---- 1. 表存在性 ----
	if len(tableRows) == 0 {
		c.failf("current schema holds no table at all (DATABASE() is empty or not selected): %s", baselinePrepareHint)
		return report, c.err(report)
	}
	present := make(map[string]bool, len(tableRows))
	for _, row := range tableRows {
		if len(row) < 2 {
			continue
		}
		present[baselineIdent(row[0])] = true
	}
	missingTables := make(map[string]bool, 8)
	for _, tb := range tables {
		if present[baselineIdent(tb.Name)] {
			report.Tables++
			continue
		}
		missingTables[tb.Name] = true
		c.failf("table %s is missing: %s", tb.Name, baselinePrepareHint)
	}

	// ---- 2. 列：逐表列名 / DATA_TYPE / UNSIGNED / NOT NULL / 生成列表达式 ----
	dbCols := make(map[string][]baselineDBColumn, len(tables))
	for _, row := range columnRows {
		if len(row) < 6 {
			continue
		}
		key := baselineIdent(row[0])
		dbCols[key] = append(dbCols[key], baselineDBColumn{
			name:       row[1],
			dataType:   row[2],
			columnType: row[3],
			nullable:   row[4],
			genExpr:    row[5],
		})
	}

	var dbTotalColumns int
	for _, tb := range tables {
		if missingTables[tb.Name] {
			continue
		}
		cols := dbCols[baselineIdent(tb.Name)]
		dbTotalColumns += len(cols)
		if len(cols) == 0 {
			c.failf("table %s reports 0 columns in information_schema.columns, want %d", tb.Name, len(tb.Columns))
			continue
		}
		byName := make(map[string]baselineDBColumn, len(cols))
		for _, col := range cols {
			key := baselineIdent(col.name)
			if _, dup := byName[key]; dup {
				c.failf("table %s: column %s reported twice by information_schema.columns", tb.Name, col.name)
				continue
			}
			byName[key] = col
		}
		wanted := make(map[string]bool, len(tb.Columns))
		for i := range tb.Columns {
			col := tb.Columns[i]
			wanted[baselineIdent(col.Name)] = true
			got, ok := byName[baselineIdent(col.Name)]
			if !ok {
				c.failf("table %s: column %s is missing", tb.Name, col.Name)
				continue
			}
			c.checkColumn(tb.Name, col, got)
		}
		for _, col := range cols {
			if !wanted[baselineIdent(col.name)] {
				c.failf("table %s: unexpected column %s (%s) is not part of the baseline catalog", tb.Name, col.name, col.columnType)
			}
		}
	}
	report.TotalColumns = dbTotalColumns
	if dbTotalColumns != BaselineColumnCount {
		c.failf("baseline column total is %d, want %d", dbTotalColumns, BaselineColumnCount)
	}

	// ---- 3. 索引：62 个 UNIQUE 键存在性 + 列顺序 + 唯一性；DESC→ASC 容忍 ----
	dbIndexes := make(map[string]*baselineDBIndex, len(indexRows))
	for _, row := range indexRows {
		if len(row) < 6 {
			continue
		}
		key := baselineIdent(row[0]) + "." + baselineIdent(row[1])
		ix, ok := dbIndexes[key]
		if !ok {
			ix = &baselineDBIndex{table: row[0], name: row[1]}
			dbIndexes[key] = ix
		}
		nonUnique, err := baselineParseBoolish(row[4])
		if err != nil {
			return report, fmt.Errorf("parse NON_UNIQUE for %s.%s: %w", row[0], row[1], err)
		}
		if !ix.hasParts {
			ix.unique = !nonUnique
			ix.hasParts = true
		} else if ix.unique == nonUnique {
			c.failf("table %s: index %s reports inconsistent NON_UNIQUE values in information_schema.statistics", row[0], row[1])
		}
		ix.parts = append(ix.parts, baselineDBIndexPart{
			column:    row[3],
			collation: row[5],
			seq:       baselineParseSeq(row[2]),
		})
	}
	for _, ix := range dbIndexes {
		sort.SliceStable(ix.parts, func(i, j int) bool { return ix.parts[i].seq < ix.parts[j].seq })
	}

	for _, tb := range tables {
		if missingTables[tb.Name] {
			continue
		}
		for _, ix := range tb.Indexes {
			if ix.Unique {
				report.UniqueIndexes++
			}
			got, ok := dbIndexes[baselineIdent(tb.Name)+"."+baselineIdent(ix.Name)]
			if !ok {
				c.failf("table %s: index %s (%s) is missing: %s", tb.Name, ix.Name, baselineIndexKind(ix), baselinePrepareHint)
				continue
			}
			if ix.Unique && !got.unique {
				c.failf("table %s: index %s is not UNIQUE in the database, the baseline requires UNIQUE: %s", tb.Name, ix.Name, baselinePrepareHint)
			}
			if !ix.Unique && got.unique {
				c.failf("table %s: index %s is UNIQUE in the database, the baseline declares a non-unique index", tb.Name, ix.Name)
			}
			if len(got.parts) != len(ix.Parts) {
				c.failf("table %s: index %s has %d column(s) %s, want %d %s", tb.Name, ix.Name, len(got.parts),
					baselinePartNames(got.parts), len(ix.Parts), baselineCatalogPartNames(ix.Parts))
				continue
			}
			tolerated := false
			for i, want := range ix.Parts {
				have := got.parts[i]
				if baselineIdent(have.column) != baselineIdent(want.Column) {
					c.failf("table %s: index %s column %d is %s, want %s", tb.Name, ix.Name, i+1, have.column, want.Column)
					continue
				}
				haveDesc := strings.EqualFold(strings.TrimSpace(have.collation), "D")
				switch {
				case want.Desc && !haveDesc:
					// 已知差异：真实 TiDB 将 DESC 索引按 ASC 等价存储。
					tolerated = true
				case !want.Desc && haveDesc:
					c.failf("table %s: index %s column %s is stored DESC, the baseline declares ASC", tb.Name, ix.Name, want.Column)
				}
			}
			if tolerated {
				report.ToleratedDescIndexes++
			}
		}
	}

	return report, c.err(report)
}

// ---- 列比对 ----

// checkColumn 逐属性比对单列：DATA_TYPE、UNSIGNED、NOT NULL、生成列表达式。
func (c *baselineChecker) checkColumn(table string, want Column, got baselineDBColumn) {
	wantType, err := baselineExpectedColumnType(want)
	if err != nil {
		c.failf("table %s: column %s: %v", table, want.Name, err)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(got.dataType), wantType.DataType) {
		c.failf("table %s: column %s has DATA_TYPE %s, want %s", table, want.Name, baselineQuoteOrNone(got.dataType), wantType.DataType)
		return
	}
	haveUnsigned := strings.Contains(strings.ToLower(got.columnType), "unsigned")
	if haveUnsigned != wantType.Unsigned {
		wantState := "not unsigned"
		if wantType.Unsigned {
			wantState = "UNSIGNED"
		}
		c.failf("table %s: column %s has COLUMN_TYPE %s, the baseline requires %s", table, want.Name, baselineQuoteOrNone(got.columnType), wantState)
	}
	if wantType.ExactColumnType != "" && strings.ToLower(strings.TrimSpace(got.columnType)) != wantType.ExactColumnType {
		c.failf("table %s: column %s has COLUMN_TYPE %s, want %s", table, want.Name, baselineQuoteOrNone(got.columnType), wantType.ExactColumnType)
	}

	haveNotNull := strings.EqualFold(strings.TrimSpace(got.nullable), "NO")
	if haveNotNull != want.NotNull {
		if want.NotNull {
			c.failf("table %s: column %s is nullable (IS_NULLABLE=%s), the baseline requires NOT NULL", table, want.Name, baselineQuoteOrNone(got.nullable))
		} else {
			c.failf("table %s: column %s is NOT NULL, the baseline requires a nullable column", table, want.Name)
		}
	}

	if want.Generated != "" {
		if strings.TrimSpace(got.genExpr) == "" {
			c.failf("table %s: column %s is a STORED generated column in the baseline but the database reports no GENERATION_EXPRESSION", table, want.Name)
			return
		}
		if !baselineGenerationExprMatches(want.Generated, got.genExpr) {
			c.failf("table %s: column %s generated expression %s does not match the baseline expression %s",
				table, want.Name, baselineQuoteOrNone(got.genExpr), baselineQuoteOrNone(want.Generated))
		}
		return
	}
	if strings.TrimSpace(got.genExpr) != "" {
		c.failf("table %s: column %s is a generated column (%s), the baseline declares a plain column",
			table, want.Name, baselineQuoteOrNone(got.genExpr))
	}
}

// baselineColumnType 是一个列在 information_schema 中的期望口径。
type baselineColumnType struct {
	// DataType 是 information_schema.COLUMNS.DATA_TYPE（TiDB 不含显示宽度）。
	DataType string
	// Unsigned 表示 COLUMN_TYPE 必须（true）或不得（false）含 unsigned。
	Unsigned bool
	// ExactColumnType 非空时 COLUMN_TYPE 必须与之完全一致（大小写不敏感、去空格）。
	// 仅 BOOLEAN 使用：TINYINT(1) 是驱动侧 bool 语义的载体，宽度不能退化。
	ExactColumnType string
}

// baselineExpectedColumnType 把声明式 ColumnType 映射到 information_schema 口径。
func baselineExpectedColumnType(c Column) (baselineColumnType, error) {
	switch c.Type {
	case TBool:
		return baselineColumnType{DataType: "tinyint", ExactColumnType: "tinyint(1)"}, nil
	case TTiny:
		return baselineColumnType{DataType: "tinyint"}, nil
	case TSmall:
		return baselineColumnType{DataType: "smallint"}, nil
	case TInt:
		return baselineColumnType{DataType: "int"}, nil
	case TIntUnsigned:
		return baselineColumnType{DataType: "int", Unsigned: true}, nil
	case TBig:
		return baselineColumnType{DataType: "bigint"}, nil
	case TBigUnsigned:
		return baselineColumnType{DataType: "bigint", Unsigned: true}, nil
	case TDouble:
		return baselineColumnType{DataType: "double"}, nil
	case TDate3:
		return baselineColumnType{DataType: "datetime"}, nil
	case TText, TJSON:
		return baselineColumnType{DataType: "mediumtext"}, nil
	case TMediumBlob:
		return baselineColumnType{DataType: "mediumblob"}, nil
	case TVarchar:
		return baselineColumnType{DataType: "varchar"}, nil
	case TChar:
		return baselineColumnType{DataType: "char"}, nil
	case TBinary:
		return baselineColumnType{DataType: "binary"}, nil
	case TVarbinary:
		return baselineColumnType{DataType: "varbinary"}, nil
	default:
		return baselineColumnType{}, fmt.Errorf("unknown ColumnType %d", int(c.Type))
	}
}

// ---- 生成列表达式比较 ----

// baselineExpressionCharsetIntroducers 是 TiDB/MySQL 在 restore 表达式时可能补上的
// 字符集引导符（例如 _utf8mb4'pending'），比较时忽略。
var baselineExpressionCharsetIntroducers = map[string]bool{
	"_utf8mb4": true,
	"_utf8mb3": true,
	"_utf8":    true,
	"_latin1":  true,
	"_ascii":   true,
	"_binary":  true,
	"_bin":     true,
}

// 生成列表达式的比较口径（Fix Round 1 / Review R1）。
//
// 只容忍以下五类"书写差异"，其余一律判红：
//  1. 空白：空格、换行、缩进（TiDB 会按自身风格重排空白）；
//  2. 大小写：函数名/关键字/标识符统一按小写比较（字符串字面量内容大小写敏感，原样保留）；
//  3. 标识符反引号：`status` 与 status 等价；
//  4. 字符集引导符：_utf8mb4'x' 与 'x' 等价；
//  5. 字符串字面量引号风格：'x' 与 "x"（含 '' 与反斜杠转义）归一为同一字面量内容。
//
// 除上述书写差异外，表达式的词法结构必须逐 token 严格一致：运算符（= <> != < > <= >= <=> + - * /
// % && || …）、标点（( ) , . …）、关键字与函数名（IF / IN / NOT / AND / OR …）、参数个数与顺序、
// 同名 token 的出现次数全部保留。条件取反、分支互换、参数换位、新增算术必须判红。
// 比较是"等长 + 逐位相等"，绝不排序、绝不去重、绝不做集合比较，也不丢弃任何词法 token。

// baselineExpressionOperatorTexts 是必须作为独立 token 保留的运算符与结构标点。
var baselineExpressionOperatorTexts = []string{
	"<=>", "<>", "<=", ">=", "!=", ":=", "&&", "||", "<<", ">>", "->", "=>",
	"(", ")", ",", ";", "=", "<", ">", "+", "-", "*", "/", "%", "!", "&", "|", "^", "~",
	".", "@", "?", ":", "\\", "{", "}", "[", "]",
}

// baselineExpressionOperators 是运算符的 rune 形式，按长度降序排列以保证最长匹配
// （否则 "<=" 会被切成 "<" 与 "="）。
var baselineExpressionOperators = func() [][]rune {
	ops := make([][]rune, 0, len(baselineExpressionOperatorTexts))
	for _, op := range baselineExpressionOperatorTexts {
		ops = append(ops, []rune(op))
	}
	sort.SliceStable(ops, func(i, j int) bool { return len(ops[i]) > len(ops[j]) })
	return ops
}()

// token 前缀把词法类别编进 token 自身，避免 1 / '1' / `1` 这类跨类别误等同。
const (
	baselineTokenIdentifier = "id:"
	baselineTokenString     = "str:"
	baselineTokenNumber     = "num:"
	baselineTokenOperator   = "op:"
)

// baselineExpressionTokens 把表达式切成**有序**词法 token 序列，仅归一上面列出的五类书写差异。
//
// token 形态：
//   - id:<小写的标识符 / 关键字 / 函数名>
//   - str:'<解码并统一单引号后的字符串字面量内容>'
//   - num:<数字字面量原文>
//   - op:<运算符或标点原文>
//
// 无法可靠切分的输入（未闭合的反引号或引号）返回 nil：调用方据此判红，绝不因切分失败而放行。
func baselineExpressionTokens(expr string) []string {
	runes := []rune(expr)
	tokens := make([]string, 0, len(runes))
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case baselineExprIsSpace(r):
			i++
		case r == '`':
			name, next, ok := baselineExprScanQuotedIdentifier(runes, i)
			if !ok {
				return nil
			}
			tokens = append(tokens, baselineTokenIdentifier+name)
			i = next
		case r == '\'' || r == '"':
			literal, next, ok := baselineExprScanStringLiteral(runes, i)
			if !ok {
				return nil
			}
			tokens = append(tokens, baselineTokenString+literal)
			i = next
		case baselineExprIdentStart(r):
			j := i
			for j < len(runes) && baselineExprIdentPart(runes[j]) {
				j++
			}
			word := strings.ToLower(string(runes[i:j]))
			// 字符集引导符只有紧贴字符串字面量时才是引导符（TiDB 回显会补 _utf8mb4 等）。
			if baselineExpressionCharsetIntroducers[word] && j < len(runes) && (runes[j] == '\'' || runes[j] == '"') {
				i = j
				continue
			}
			tokens = append(tokens, baselineTokenIdentifier+word)
			i = j
		case r >= '0' && r <= '9':
			number, next := baselineExprScanNumber(runes, i)
			tokens = append(tokens, baselineTokenNumber+number)
			i = next
		default:
			op := baselineExprOperatorAt(runes[i:])
			if len(op) == 0 {
				// 未知字符独立成 token：两侧同规则，识别不了的文本只会判红，不会放行。
				op = []rune{r}
			}
			tokens = append(tokens, baselineTokenOperator+string(op))
			i += len(op)
		}
	}
	return tokens
}

// baselineExprIsSpace 判定可被完全忽略的空白（含换行与缩进）。
func baselineExprIsSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

func baselineExprIdentStart(r rune) bool {
	return r == '_' || r == '$' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= 0x80
}

func baselineExprIdentPart(r rune) bool {
	return baselineExprIdentStart(r) || r >= '0' && r <= '9'
}

// baselineExprScanQuotedIdentifier 读取一段反引号标识符（MySQL 以双写反引号转义反引号），
// 返回小写内容；未闭合时返回 ok=false。
func baselineExprScanQuotedIdentifier(runes []rune, i int) (string, int, bool) {
	var b strings.Builder
	for j := i + 1; j < len(runes); {
		if runes[j] == '`' {
			if j+1 < len(runes) && runes[j+1] == '`' {
				b.WriteRune('`')
				j += 2
				continue
			}
			return strings.ToLower(b.String()), j + 1, true
		}
		b.WriteRune(runes[j])
		j++
	}
	return "", 0, false
}

// baselineExprScanStringLiteral 读取一段字符串字面量并把引号风格与转义归一为
// '…' 包裹的字面量内容；未闭合时返回 ok=false。
func baselineExprScanStringLiteral(runes []rune, i int) (string, int, bool) {
	quote := runes[i]
	var b strings.Builder
	for j := i + 1; j < len(runes); {
		c := runes[j]
		switch {
		case c == '\\':
			if j+1 >= len(runes) {
				return "", 0, false
			}
			n := runes[j+1]
			switch n {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case 'b':
				b.WriteByte('\b')
			case '0':
				b.WriteByte(0)
			case 'Z':
				b.WriteByte(0x1a)
			case '\'', '"', '\\':
				b.WriteRune(n)
			default:
				// \%= 与 \_ 在 MySQL 里保留反斜杠（LIKE 转义），原样带上。
				b.WriteRune('\\')
				b.WriteRune(n)
			}
			j += 2
		case c == quote:
			if j+1 < len(runes) && runes[j+1] == quote {
				// 双写引号是字面量引号本身。
				b.WriteRune(quote)
				j += 2
				continue
			}
			return "'" + b.String() + "'", j + 1, true
		default:
			b.WriteRune(c)
			j++
		}
	}
	return "", 0, false
}

// baselineExprScanNumber 读取一段数字字面量（含 0x/0b/0o 前缀、小数与科学计数）。
func baselineExprScanNumber(runes []rune, i int) (string, int) {
	if runes[i] == '0' && i+1 < len(runes) && strings.ContainsRune("xXbBoO", runes[i+1]) {
		k := i + 2
		for k < len(runes) && (runes[k] >= '0' && runes[k] <= '9' || runes[k] >= 'a' && runes[k] <= 'f' ||
			runes[k] >= 'A' && runes[k] <= 'F' || runes[k] == '_') {
			k++
		}
		if k > i+2 {
			return string(runes[i:k]), k
		}
	}
	j := i
	for j < len(runes) && runes[j] >= '0' && runes[j] <= '9' {
		j++
	}
	if j+1 < len(runes) && runes[j] == '.' && runes[j+1] >= '0' && runes[j+1] <= '9' {
		j++
		for j < len(runes) && runes[j] >= '0' && runes[j] <= '9' {
			j++
		}
	}
	if j < len(runes) && (runes[j] == 'e' || runes[j] == 'E') {
		k := j + 1
		if k < len(runes) && (runes[k] == '+' || runes[k] == '-') {
			k++
		}
		if k < len(runes) && runes[k] >= '0' && runes[k] <= '9' {
			j = k
			for j < len(runes) && runes[j] >= '0' && runes[j] <= '9' {
				j++
			}
		}
	}
	if j == i {
		// 理论上不会发生（调用方保证首位是数字），保底推进一格避免死循环。
		j = i + 1
	}
	return string(runes[i:j]), j
}

// baselineExprOperatorAt 返回 runes 前缀上最长的已知运算符；没有匹配时返回 nil。
func baselineExprOperatorAt(runes []rune) []rune {
	for _, op := range baselineExpressionOperators {
		if len(runes) < len(op) {
			continue
		}
		matched := true
		for k := range op {
			if runes[k] != op[k] {
				matched = false
				break
			}
		}
		if matched {
			return op
		}
	}
	return nil
}

// baselineGenerationExprMatches 判断库侧 GENERATION_EXPRESSION 是否等价于目录表达式。
//
// 判定是严格顺序敏感的：两侧归一化后的 token 序列必须等长且逐位完全相同。
// 空库侧表达式、切分失败、token 数不同、任一位置不同（含条件取反、分支互换、
// 参数换位、新增/替换运算符、重复次数不同）一律返回 false。
func baselineGenerationExprMatches(catalogExpr, dbExpr string) bool {
	if strings.TrimSpace(dbExpr) == "" {
		return false
	}
	want := baselineExpressionTokens(catalogExpr)
	got := baselineExpressionTokens(dbExpr)
	if len(want) == 0 || len(want) != len(got) {
		return false
	}
	for i := range want {
		if want[i] != got[i] {
			return false
		}
	}
	return true
}

// ---- 结果集读取 ----

// baselineFetch 执行一条只读字典查询并整体读取为字符串矩阵。
// 任何路径（含 Scan/迭代错误）都关闭 Rows，绝不提前 return 泄漏连接。
func baselineFetch(ctx context.Context, q Querier, label, query string) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return baselineDrainRows(label, rows)
}

// baselineDrainRows 读取全部行；列值经 normalizeDriverValue 归一化为字符串，
// 以兼容驱动返回 []byte/int64/uint64 等不同形态。
func baselineDrainRows(label string, rows Rows) (out [][]string, retErr error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("columns of %s result: %w", label, err), rows.Close())
	}
	n := len(cols)
	var loopErr error
	for rows.Next() {
		cells := make([]any, n)
		dest := make([]any, n)
		for i := range cells {
			dest[i] = &cells[i]
		}
		if err := rows.Scan(dest...); err != nil {
			loopErr = fmt.Errorf("scan %s row: %w", label, err)
			break
		}
		rec := make([]string, n)
		for i := range cells {
			rec[i] = normalizeDriverValue(cells[i])
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil && loopErr == nil {
		loopErr = fmt.Errorf("iterate %s rows: %w", label, err)
	}
	closeErr := rows.Close()
	if loopErr != nil {
		return nil, errors.Join(loopErr, closeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close %s rows: %w", label, closeErr)
	}
	return out, nil
}

// ---- 问题聚合 ----

// baselineChecker 收集全部结构问题，聚合为一条可读、不可忽略的错误。
type baselineChecker struct {
	problems []string
}

func (c *baselineChecker) failf(format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, args...))
}

func (c *baselineChecker) err(report baselineReport) error {
	if len(c.problems) == 0 {
		return nil
	}
	shown := c.problems
	lines := make([]string, 0, len(shown)+2)
	if len(shown) > baselineMaxReportedProblems {
		lines = append(lines, fmt.Sprintf("  ... and %d more problem(s)", len(shown)-baselineMaxReportedProblems))
		shown = shown[:baselineMaxReportedProblems]
	}
	for _, p := range shown {
		lines = append(lines, "  - "+p)
	}
	lines = append(lines, "  hint: "+baselinePrepareHint)
	return fmt.Errorf("%w: %d problem(s) versus the %d-table baseline (verified %d table(s), %d column(s), %d unique index/index(es)):\n%s",
		ErrBaselineMismatch, len(c.problems), BaselineTableCount, report.Tables, report.TotalColumns, report.UniqueIndexes,
		strings.Join(lines, "\n"))
}

// ---- 小型辅助 ----

// baselineDBColumn 是 information_schema.columns 的一行。
type baselineDBColumn struct {
	name       string
	dataType   string
	columnType string
	nullable   string
	genExpr    string
}

// baselineDBIndexPart 是 information_schema.statistics 的一行（一个索引部件）。
type baselineDBIndexPart struct {
	column    string
	collation string
	seq       int64
}

// baselineDBIndex 是按索引名归并后的索引。
type baselineDBIndex struct {
	table  string
	name   string
	unique bool
	// hasParts 标记 unique 是否已由首个部件确立。
	hasParts bool
	parts    []baselineDBIndexPart
}

// baselineIdent 归一化标识符用于比较（库名/表名/列名大小写敏感性随平台变化）。
func baselineIdent(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// baselineQuoteOrNone 把可疑值渲染成可读引号形式。
func baselineQuoteOrNone(s string) string {
	return "'" + strings.TrimSpace(s) + "'"
}

func baselineIndexKind(ix Index) string {
	if ix.Unique {
		return "UNIQUE KEY"
	}
	return "KEY"
}

func baselinePartNames(parts []baselineDBIndexPart) string {
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		names = append(names, p.column)
	}
	return "[" + strings.Join(names, ", ") + "]"
}

func baselineCatalogPartNames(ix []IndexPart) string {
	names := make([]string, 0, len(ix))
	for _, p := range ix {
		names = append(names, p.Column)
	}
	return "[" + strings.Join(names, ", ") + "]"
}

// baselineParseBoolish 解析 NON_UNIQUE：驱动可能回 "0"/"1"/int64/bool 文本。
func baselineParseBoolish(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	default:
		return false, fmt.Errorf("unexpected NON_UNIQUE value %q", raw)
	}
}

// baselineParseSeq 解析 SEQ_IN_INDEX；异常值退化为 0（排序稳定，交由逐列比对判红）。
func baselineParseSeq(raw string) int64 {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return 0
}
