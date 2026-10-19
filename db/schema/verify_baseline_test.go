package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// ============================================================================
// Task 1.2 — schema.VerifyBaseline 离线单元测试
//
// 覆盖三层：
//  1. *sql.DB 入口契约（nil db / 取消上下文 / 连接失败）；
//  2. 目录基线计数自洽（65 表 / 729 列 / 62 UNIQUE 索引 / 21 处 DESC 索引）；
//  3. 纯只读 information_schema 比对核心（以目录自身合成 fake 结果集 + 定点变异）。
// ============================================================================

// ---- fake driver.Connector：只在 *sql.DB 入口契约测试中使用，绝不真正建连 ----

type blRejectConnector struct {
	mu     sync.Mutex
	dialed int
}

func (c *blRejectConnector) Connect(context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dialed++
	return nil, errors.New("blRejectConnector: connection must not be established")
}

func (c *blRejectConnector) Driver() driver.Driver { return blRejectDriver{} }

type blRejectDriver struct{}

func (blRejectDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("blRejectDriver: Open not supported")
}

func (c *blRejectConnector) dialCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dialed
}

// ---- 入口契约测试 ----

func TestVerifyBaseline_NilDBFails(t *testing.T) {
	ctx := context.Background()
	if err := VerifyBaseline(ctx, nil); err == nil {
		t.Fatalf("expected error for nil db, got nil")
	}
}

func TestVerifyBaseline_NilDBDoesNotPanicWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := VerifyBaseline(ctx, nil); err == nil {
		t.Fatalf("expected error for nil db, got nil")
	}
}

func TestVerifyBaseline_CanceledContextFails(t *testing.T) {
	connector := &blRejectConnector{}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := VerifyBaseline(ctx, db)
	if err == nil {
		t.Fatalf("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v does not wrap context.Canceled", err)
	}
	if got := connector.dialCount(); got != 0 {
		t.Fatalf("canceled context dialed %d times, want 0 (must fail before any query)", got)
	}
}

func TestVerifyBaseline_ConnectFailurePropagates(t *testing.T) {
	connector := &blRejectConnector{}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	err := VerifyBaseline(context.Background(), db)
	if err == nil {
		t.Fatalf("expected error when the connection cannot be established, got nil")
	}
	if connector.dialCount() == 0 {
		t.Fatalf("expected at least one dial, got 0")
	}
	if errors.Is(err, ErrBaselineMismatch) {
		t.Fatalf("connection failure must not be reported as %v, got %v", ErrBaselineMismatch, err)
	}
}

// ---- 目录基线计数自洽 ----

func TestVerifyBaseline_CatalogCountsPinned(t *testing.T) {
	if err := checkBaselineCatalogCounts(AllTables()); err != nil {
		t.Fatalf("AllTables() violates the pinned baseline counts: %v", err)
	}
}

func TestVerifyBaseline_CatalogDriftFailsClosed(t *testing.T) {
	// 每个子用例都必须在启动校验期立刻失败，而不是静默放过。
	cases := map[string]func(tables []*Table){
		"missing table":   func(tables []*Table) { tables[0].Columns = tables[0].Columns[:1] },
		"extra column":    func(tables []*Table) { tables[0].Columns = append(tables[0].Columns, Text("drift_col")) },
		"extra unique ix": func(tables []*Table) { tables[0].Indexes = append(tables[0].Indexes, IX("uq_drift", true, P("name"))) },
		"desc ix removed": func(tables []*Table) {
			for _, tb := range tables {
				for i := range tb.Indexes {
					for j := range tb.Indexes[i].Parts {
						if tb.Indexes[i].Parts[j].Desc {
							tb.Indexes[i].Parts[j].Desc = false
							return
						}
					}
				}
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tables := blCloneCatalog(AllTables())
			mutate(tables)
			if err := checkBaselineCatalogCounts(tables); err == nil {
				t.Fatalf("expected drift (%s) to fail the catalog count guard", name)
			}
		})
	}
}

// ============================================================================
// information_schema fake 结果集
// ============================================================================

type blQueryKind int

const (
	blKindTables blQueryKind = iota
	blKindColumns
	blKindIndexes
)

func (k blQueryKind) String() string {
	switch k {
	case blKindTables:
		return "tables"
	case blKindColumns:
		return "columns"
	case blKindIndexes:
		return "statistics"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// blQueryKindOf 依据 FROM 子句里的 information_schema 对象路由查询。
// 未知查询直接报错：保证实现不会偷偷多读别的字典表。
func blQueryKindOf(query string) (blQueryKind, error) {
	q := strings.ToLower(query)
	var found []blQueryKind
	for _, probe := range []struct {
		token string
		kind  blQueryKind
	}{{"information_schema.tables", blKindTables}, {"information_schema.columns", blKindColumns}, {"information_schema.statistics", blKindIndexes}} {
		if strings.Contains(q, probe.token) {
			found = append(found, probe.kind)
		}
	}
	if len(found) != 1 {
		return 0, fmt.Errorf("expected exactly one information_schema dictionary source, got %d for query %q", len(found), query)
	}
	return found[0], nil
}

// blRows 是最小 Rows 实现，支持 *any 目标（生产侧按列位置泛型扫描）。
type blRows struct {
	cols     []string
	data     [][]string
	pos      int
	closed   bool
	scanErr  error
	closeErr error
	errVal   error
}

func newBLRows(cols []string, data [][]string) *blRows {
	return &blRows{cols: cols, data: data}
}

func (r *blRows) Columns() ([]string, error) { return r.cols, nil }

func (r *blRows) Next() bool {
	if r.closed || r.pos >= len(r.data) {
		return false
	}
	r.pos++
	return true
}

func (r *blRows) Scan(dest ...any) error {
	if r.closed {
		return errors.New("blRows: scan on closed rows")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos == 0 || r.pos > len(r.data) {
		return fmt.Errorf("blRows: invalid cursor %d (rows %d)", r.pos, len(r.data))
	}
	row := r.data[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("blRows: scan dest count %d, row has %d", len(dest), len(row))
	}
	for i, v := range row {
		switch d := dest[i].(type) {
		case *any:
			if v == sentinelNull {
				*d = nil
			} else {
				*d = v
			}
		case *string:
			*d = v
		default:
			return fmt.Errorf("blRows: unsupported dest %T", dest[i])
		}
	}
	return nil
}

func (r *blRows) Close() error {
	r.closed = true
	return r.closeErr
}

func (r *blRows) Err() error { return r.errVal }

// sentinelNull 让 fake 结果集能表达 NULL。
const sentinelNull = "\x00__bl_null__"

// blQuerier 记录全部查询/Exec，并断言行集生命周期（不泄漏、不并发打开）。
type blQuerier struct {
	mu      sync.Mutex
	queries []string
	execs   []string
	open    int
	maxOpen int

	route     func(kind blQueryKind, query string) (Rows, error)
	failQuery func(kind blQueryKind, query string) error
	failExec  func(query string) error
}

func (q *blQuerier) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, err := blQueryKindOf(query)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.queries = append(q.queries, query)
	if q.failQuery != nil {
		if err := q.failQuery(kind, query); err != nil {
			q.mu.Unlock()
			return nil, err
		}
	}
	route := q.route
	open, maxOpen := q.open+1, q.maxOpen
	if open > maxOpen {
		maxOpen = open
	}
	q.open, q.maxOpen = open, maxOpen
	q.mu.Unlock()

	if route == nil {
		return nil, fmt.Errorf("blQuerier: no route for %s query", kind)
	}
	rows, err := route(kind, query)
	if err != nil {
		q.mu.Lock()
		q.open--
		q.mu.Unlock()
		return nil, err
	}
	return &blTrackedRows{Rows: rows, q: q}, nil
}

// Exec 被调用即失败：VerifyBaseline 必须纯只读，严禁 DML/写入探针。
func (q *blQuerier) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	q.mu.Lock()
	q.execs = append(q.execs, query)
	q.mu.Unlock()
	if q.failExec != nil {
		return nil, q.failExec(query)
	}
	return nil, fmt.Errorf("blQuerier: VerifyBaseline must not Exec, got %q", query)
}

func (q *blQuerier) snapshot() (queries, execs []string, maxOpen int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.queries...), append([]string(nil), q.execs...), q.maxOpen
}

type blTrackedRows struct {
	Rows
	q    *blQuerier
	once sync.Once
}

func (r *blTrackedRows) Close() error {
	err := r.Rows.Close()
	r.once.Do(func() {
		r.q.mu.Lock()
		r.q.open--
		r.q.mu.Unlock()
	})
	return err
}

// ---- 从目录合成 information_schema 视图（模拟真实 TiDB 口径）----

// blDB 是三张字典表的 fake 内容。
type blDB struct {
	tables  [][]string // TABLE_NAME, TABLE_TYPE
	columns [][]string // TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, GENERATION_EXPRESSION
	indexes [][]string // TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE, COLLATION
}

type blSynthOptions struct {
	// DescCollation 决定目录中 Desc=true 的部件写出的 COLLATION。
	DescCollation string
	// GeneratedExpr 重写生成列表达式；为空时用真实 TiDB 风格归一化文本。
	GeneratedExpr func(column Column) string
	// ExtraTable 追加一张目录外基表（必须被忽略）。
	ExtraTable bool
}

// blSynthesize 依据声明式目录构造与真实 TiDB 观测口径一致的字典表内容。
func blSynthesize(t *testing.T, tables []*Table, opts blSynthOptions) *blDB {
	t.Helper()
	db := &blDB{}
	for _, tb := range tables {
		db.tables = append(db.tables, []string{tb.Name, "BASE TABLE"})
		for _, c := range tb.Columns {
			dataType, columnType := blExpectedType(t, c)
			nullable := "YES"
			if c.NotNull {
				nullable = "NO"
			}
			genExpr := ""
			if c.Generated != "" {
				if opts.GeneratedExpr != nil {
					genExpr = opts.GeneratedExpr(c)
				} else {
					genExpr = blTiDBStyleExpr(c.Generated)
				}
			}
			db.columns = append(db.columns, []string{tb.Name, c.Name, dataType, columnType, nullable, genExpr})
		}
		for _, ix := range tb.Indexes {
			nonUnique := "1"
			if ix.Unique {
				nonUnique = "0"
			}
			for i, p := range ix.Parts {
				collation := "A"
				if p.Desc {
					collation = opts.DescCollation
					if collation == "" {
						collation = "A"
					}
				}
				db.indexes = append(db.indexes, []string{
					tb.Name, ix.Name, fmt.Sprintf("%d", i+1), p.Column, nonUnique, collation,
				})
			}
		}
	}
	if opts.ExtraTable {
		db.tables = append(db.tables, []string{"zz_external_table", "BASE TABLE"})
		db.columns = append(db.columns, []string{"zz_external_table", "whatever", "varchar", "varchar(16)", "YES", ""})
	}
	return db
}

// blExpectedType 是测试侧独立推导的 DATA_TYPE / COLUMN_TYPE 口径。
func blExpectedType(t *testing.T, c Column) (dataType, columnType string) {
	t.Helper()
	switch c.Type {
	case TBool:
		return "tinyint", "tinyint(1)"
	case TTiny:
		return "tinyint", "tinyint"
	case TSmall:
		return "smallint", "smallint"
	case TInt:
		return "int", "int"
	case TIntUnsigned:
		return "int", "int unsigned"
	case TBig:
		return "bigint", "bigint"
	case TBigUnsigned:
		return "bigint", "bigint unsigned"
	case TDouble:
		return "double", "double"
	case TDate3:
		return "datetime", "datetime(3)"
	case TText, TJSON:
		return "mediumtext", "mediumtext"
	case TMediumBlob:
		return "mediumblob", "mediumblob"
	case TVarchar:
		return "varchar", fmt.Sprintf("varchar(%d)", c.Length)
	case TChar:
		return "char", fmt.Sprintf("char(%d)", c.Length)
	case TBinary:
		return "binary", fmt.Sprintf("binary(%d)", c.Length)
	case TVarbinary:
		return "varbinary", fmt.Sprintf("varbinary(%d)", c.Length)
	default:
		t.Fatalf("blExpectedType: unknown ColumnType %d on column %s", int(c.Type), c.Name)
		return "", ""
	}
}

// blTiDBStyleExpr 模拟真实 TiDB 回写 GENERATION_EXPRESSION 的归一化风格：
// 函数名/关键字小写、标识符加反引号、字符串字面量前置 _utf8mb4 字符集引导符、
// 运算符两侧补空格并追加保护括号。仅做书写风格变换，不改变参与计算的列与字面量。
func blTiDBStyleExpr(expr string) string {
	keywords := map[string]bool{
		"if": true, "in": true, "null": true, "is": true, "not": true,
		"and": true, "or": true, "true": true, "false": true, "case": true,
		"when": true, "then": true, "else": true, "end": true,
	}
	var b strings.Builder
	runes := []rune(expr)
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case r == '\'' || r == '"':
			quote := r
			lit := strings.Builder{}
			lit.WriteRune(r)
			j := i + 1
			for j < len(runes) {
				lit.WriteRune(runes[j])
				if runes[j] == quote {
					break
				}
				j++
			}
			b.WriteString("_utf8mb4" + lit.String())
			i = j + 1
		case r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
			word := strings.Builder{}
			j := i
			for j < len(runes) {
				c := runes[j]
				if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
					word.WriteRune(c)
					j++
					continue
				}
				break
			}
			w := strings.ToLower(word.String())
			if keywords[w] || blExprIsAllDigits(w) {
				// 关键字小写、数字字面量原样；真实 TiDB 不会把 1 写成 `1`。
				b.WriteString(w)
			} else {
				b.WriteString("`" + w + "`")
			}
			i = j
		case r == '=' || r == '<' || r == '>' || r == '!':
			b.WriteString(" " + string(r) + " ")
			i++
		default:
			b.WriteRune(r)
			i++
		}
	}
	return b.String()
}

func (d *blDB) querier(t *testing.T) *blQuerier {
	t.Helper()
	return &blQuerier{
		route: func(kind blQueryKind, query string) (Rows, error) {
			switch kind {
			case blKindTables:
				return newBLRows([]string{"TABLE_NAME", "TABLE_TYPE"}, d.tables), nil
			case blKindColumns:
				return newBLRows([]string{
					"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE", "GENERATION_EXPRESSION",
				}, d.columns), nil
			case blKindIndexes:
				return newBLRows([]string{
					"TABLE_NAME", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "NON_UNIQUE", "COLLATION",
				}, d.indexes), nil
			default:
				return nil, fmt.Errorf("unexpected query kind %s", kind)
			}
		},
	}
}

func (d *blDB) dropTable(name string) {
	d.tables = blDeleteWhere(d.tables, func(row []string) bool { return row[0] == name })
	d.columns = blDeleteWhere(d.columns, func(row []string) bool { return row[0] == name })
	d.indexes = blDeleteWhere(d.indexes, func(row []string) bool { return row[0] == name })
}

func (d *blDB) columnRow(table, column string) []string {
	for _, row := range d.columns {
		if row[0] == table && row[1] == column {
			return row
		}
	}
	panic("blDB: column row not found: " + table + "." + column)
}

func (d *blDB) dropColumn(table, column string) {
	d.columns = blDeleteWhere(d.columns, func(row []string) bool { return row[0] == table && row[1] == column })
}

func (d *blDB) addColumn(table, column, dataType, columnType, nullable string) {
	d.columns = append(d.columns, []string{table, column, dataType, columnType, nullable, ""})
}

func (d *blDB) indexRows(table, index string) [][]string {
	var out [][]string
	for _, row := range d.indexes {
		if row[0] == table && row[1] == index {
			out = append(out, row)
		}
	}
	return out
}

func (d *blDB) dropIndex(table, index string) {
	d.indexes = blDeleteWhere(d.indexes, func(row []string) bool { return row[0] == table && row[1] == index })
}

func blDeleteWhere(rows [][]string, pred func([]string) bool) [][]string {
	out := rows[:0]
	for _, row := range rows {
		if !pred(row) {
			out = append(out, row)
		}
	}
	return out
}

// blCloneCatalog 深拷贝目录（避免变异污染 AllTables 的其他用例）。
func blCloneCatalog(tables []*Table) []*Table {
	out := make([]*Table, 0, len(tables))
	for _, tb := range tables {
		cp := *tb
		cp.Columns = append([]Column(nil), tb.Columns...)
		cp.Indexes = append([]Index(nil), tb.Indexes...)
		for i := range cp.Indexes {
			cp.Indexes[i].Parts = append([]IndexPart(nil), tb.Indexes[i].Parts...)
		}
		out = append(out, &cp)
	}
	return out
}

// ---- 核心比对的通用断言助手 ----

func blRun(t *testing.T, db *blDB, tables []*Table) (baselineReport, *blQuerier, error) {
	t.Helper()
	q := db.querier(t)
	report, err := runBaselineVerify(context.Background(), q, tables)
	return report, q, err
}

// blMustPassFullBaseline 用整份 65 表目录跑一遍全量比对。
func blMustPassFullBaseline(t *testing.T, opts blSynthOptions) (*blDB, baselineReport) {
	t.Helper()
	tables := AllTables()
	db := blSynthesize(t, tables, opts)
	report, q, err := blRun(t, db, tables)
	if err != nil {
		t.Fatalf("expected clean baseline to pass, got: %v", err)
	}
	if queries, execs, maxOpen := q.snapshot(); len(queries) != 3 || len(execs) != 0 || maxOpen != 1 {
		t.Fatalf("expected 3 sequential read-only queries, got queries=%d execs=%d maxOpen=%d", len(queries), len(execs), maxOpen)
	}
	return db, report
}

// ---- 全量通过 ----

func TestVerifyBaseline_HappyPathFullBaseline(t *testing.T) {
	_, report := blMustPassFullBaseline(t, blSynthOptions{DescCollation: "A"})
	if report.Tables != BaselineTableCount {
		t.Errorf("report.Tables = %d, want %d", report.Tables, BaselineTableCount)
	}
	if report.TotalColumns != BaselineColumnCount {
		t.Errorf("report.TotalColumns = %d, want %d", report.TotalColumns, BaselineColumnCount)
	}
	if report.UniqueIndexes != BaselineUniqueIndexCount {
		t.Errorf("report.UniqueIndexes = %d, want %d", report.UniqueIndexes, BaselineUniqueIndexCount)
	}
	if report.ToleratedDescIndexes != BaselineDescIndexCount {
		t.Errorf("report.ToleratedDescIndexes = %d, want %d", report.ToleratedDescIndexes, BaselineDescIndexCount)
	}
}

func TestVerifyBaseline_ReadOnlyQueriesOnly(t *testing.T) {
	db := blSynthesize(t, AllTables(), blSynthOptions{DescCollation: "A"})
	q := db.querier(t)
	if _, err := runBaselineVerify(context.Background(), q, AllTables()); err != nil {
		t.Fatalf("runBaselineVerify: %v", err)
	}
	queries, execs, _ := q.snapshot()
	if len(execs) != 0 {
		t.Fatalf("VerifyBaseline executed writes: %v", execs)
	}
	if len(queries) == 0 {
		t.Fatalf("no queries recorded")
	}
	banned := []string{"insert ", "update ", "delete ", "alter ", "drop ", "create ", "truncate ", "replace ", "call ", ";"}
	for _, query := range queries {
		lower := strings.ToLower(strings.TrimSpace(query))
		if !strings.HasPrefix(lower, "select ") {
			t.Errorf("query is not a plain SELECT: %q", query)
		}
		if !strings.Contains(lower, "information_schema.") {
			t.Errorf("query does not read information_schema: %q", query)
		}
		if strings.Contains(lower, ";") {
			t.Errorf("query contains a statement separator: %q", query)
		}
		for _, b := range banned {
			if strings.Contains(lower, b) {
				t.Errorf("query contains banned token %q: %q", b, query)
			}
		}
	}
	if len(queries) != 3 {
		t.Errorf("expected exactly 3 dictionary queries (tables/columns/statistics), got %d", len(queries))
	}
}

func TestVerifyBaseline_IgnoresForeignTable(t *testing.T) {
	// 真实库里可能有目录外的表（其他应用遗留），Baseline 只核查 65 张表自身。
	_, report := blMustPassFullBaseline(t, blSynthOptions{DescCollation: "A", ExtraTable: true})
	if report.Tables != BaselineTableCount {
		t.Fatalf("report.Tables = %d, want %d", report.Tables, BaselineTableCount)
	}
}

func TestVerifyBaseline_ClosesRowsBeforeNextQuery(t *testing.T) {
	db, _ := blMustPassFullBaseline(t, blSynthOptions{DescCollation: "A"})
	q := db.querier(t)
	if _, err := runBaselineVerify(context.Background(), q, AllTables()); err != nil {
		t.Fatalf("runBaselineVerify: %v", err)
	}
	if _, _, maxOpen := q.snapshot(); maxOpen != 1 {
		t.Fatalf("max concurrently open rows = %d, want 1 (rows must be closed before the next query)", maxOpen)
	}
}

// ---- DESC 索引已知差异容忍 ----

func TestVerifyBaseline_ToleratesDescStoredAsASC(t *testing.T) {
	_, report := blMustPassFullBaseline(t, blSynthOptions{DescCollation: "A"})
	if report.ToleratedDescIndexes != BaselineDescIndexCount {
		t.Fatalf("tolerated DESC-as-ASC indexes = %d, want %d", report.ToleratedDescIndexes, BaselineDescIndexCount)
	}
}

func TestVerifyBaseline_ExactDescStorageAlsoPasses(t *testing.T) {
	_, report := blMustPassFullBaseline(t, blSynthOptions{DescCollation: "D"})
	if report.ToleratedDescIndexes != 0 {
		t.Fatalf("tolerated DESC-as-ASC indexes = %d, want 0 when the real DB stores DESC", report.ToleratedDescIndexes)
	}
}

func TestVerifyBaseline_UnexpectedDescendingIndexFails(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "D"})
	// idx_assets_domain 在目录里是 ASC；真实库报成 DESC 属于真实漂移。
	rows := db.indexRows("assets", "idx_assets_domain")
	if len(rows) != 1 {
		t.Fatalf("expected 1 part for idx_assets_domain, got %d", len(rows))
	}
	rows[0][5] = "D"

	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure when an ASC index is stored DESC")
	} else if !errors.Is(err, ErrBaselineMismatch) {
		t.Fatalf("error %v does not wrap %v", err, ErrBaselineMismatch)
	}
}

// ---- 缺失/漂移必须失败 ----

func TestVerifyBaseline_FailsOnMissingTable(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.dropTable("findings")

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure for missing table findings")
	}
	if !errors.Is(err, ErrBaselineMismatch) || !strings.Contains(err.Error(), "findings") {
		t.Fatalf("error %v must name the missing table and wrap %v", err, ErrBaselineMismatch)
	}
	if !strings.Contains(err.Error(), "artex db-migrate --prepare") {
		t.Fatalf("error %v must tell the operator to run artex db-migrate --prepare", err)
	}
}

func TestVerifyBaseline_FailsOnEmptySchema(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.tables = nil
	db.columns = nil
	db.indexes = nil

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure when the schema has no tables at all")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "no table") {
		t.Fatalf("error %v must explain that the current schema holds no table", err)
	}
}

func TestVerifyBaseline_FailsOnMissingColumn(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.dropColumn("companies", "nkey_hash")

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure for missing column companies.nkey_hash")
	}
	if !strings.Contains(err.Error(), "nkey_hash") {
		t.Fatalf("error %v must name the missing column", err)
	}
}

func TestVerifyBaseline_FailsOnUnexpectedColumn(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.addColumn("companies", "extra_col", "varchar", "varchar(64)", "YES")

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure for an unexpected extra column")
	}
	if !strings.Contains(err.Error(), "extra_col") {
		t.Fatalf("error %v must name the unexpected column", err)
	}
}

func TestVerifyBaseline_FailsOnTotalColumnCount(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	// 列总数（729）是独立的整体红线：即便逐表判定被绕过也必须抓住。
	db.columns = db.columns[:len(db.columns)-1]

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure when the total column count drops below %d", BaselineColumnCount)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(BaselineColumnCount)) {
		t.Fatalf("error %v must mention the expected column total %d", err, BaselineColumnCount)
	}
}

func TestVerifyBaseline_FailsOnDataTypeMismatch(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.columnRow("assets", "type")[2] = "varchar"

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure when DATA_TYPE drifts (mediumtext -> varchar)")
	}
	if !strings.Contains(err.Error(), "assets") || !strings.Contains(err.Error(), "type") {
		t.Fatalf("error %v must name assets.type", err)
	}
}

func TestVerifyBaseline_FailsOnTinyint1ForBool(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	row := db.columnRow("llm_profiles", "is_default")
	if row[2] != "tinyint" {
		t.Fatalf("fixture drift: llm_profiles.is_default DATA_TYPE = %s", row[2])
	}
	row[3] = "tinyint" // TINYINT(1) 被降级为 TINYINT

	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure when a BOOLEAN column loses its TINYINT(1) width")
	}
}

func TestVerifyBaseline_FailsOnUnsignedMismatch(t *testing.T) {
	cases := map[string]func(t *testing.T, db *blDB){
		"missing unsigned": func(t *testing.T, db *blDB) {
			db.columnRow("companies", "id")[3] = "bigint"
		},
		"unexpected unsigned": func(t *testing.T, db *blDB) {
			db.columnRow("tasks", "timeout_seconds")[3] = "int unsigned"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tables := AllTables()
			db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
			mutate(t, db)
			if _, _, err := blRun(t, db, tables); err == nil {
				t.Fatalf("expected failure for %s", name)
			}
		})
	}
}

func TestVerifyBaseline_FailsOnNullabilityMismatch(t *testing.T) {
	cases := map[string]func(db *blDB){
		"unexpected nullable": func(db *blDB) { db.columnRow("companies", "name")[4] = "YES" },
		"unexpected not null": func(db *blDB) { db.columnRow("assets", "domain")[4] = "NO" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tables := AllTables()
			db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
			mutate(db)
			if _, _, err := blRun(t, db, tables); err == nil {
				t.Fatalf("expected failure for %s", name)
			}
		})
	}
}

func TestVerifyBaseline_ToleratesColumnOrderAndWidthNoise(t *testing.T) {
	// 真实 TiDB 的 COLUMN_TYPE 带显示宽度/精度噪声；除 TINYINT(1) 外不得据此判红。
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	row := db.columnRow("tasks", "timeout_seconds")
	row[3] = "int(11)"
	if _, _, err := blRun(t, db, tables); err != nil {
		t.Fatalf("display width noise must be tolerated, got: %v", err)
	}
}

// ---- 生成列 ----

func TestVerifyBaseline_GeneratedColumnsPassWithTiDBStyleExpr(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	// 断言 fake 真的写出了 TiDB 风格的差异文本（否则本用例是自证）。
	got := db.columnRow("llm_profiles", "is_default_key")[5]
	if got == "" || strings.EqualFold(got, "IF(is_default,1,NULL)") {
		t.Fatalf("fixture must store a TiDB-normalized expression, got %q", got)
	}
	if _, _, err := blRun(t, db, tables); err != nil {
		t.Fatalf("TiDB-normalized generated expression must pass, got: %v", err)
	}
}

func TestVerifyBaseline_FailsOnGeneratedExprMismatch(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{
		DescCollation: "A",
		GeneratedExpr: func(c Column) string {
			return "if((`status` = _utf8mb4'done'),`finding_id`,NULL)"
		},
	})
	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure for a drifted generated column expression")
	}
}

func TestVerifyBaseline_FailsOnMissingGeneratedColumn(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{
		DescCollation: "A",
		GeneratedExpr: func(c Column) string { return "" },
	})
	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure when a STORED generated column became a plain column")
	}
	if !strings.Contains(err.Error(), "is_default_key") {
		t.Fatalf("error %v must name the drifted generated column", err)
	}
}

func TestVerifyBaseline_FailsOnUnexpectedGeneratedColumn(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.columnRow("companies", "nkey")[5] = "concat(`nkey`,'x')"

	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure when a plain column turned into a generated column")
	}
}

// ---- UNIQUE 索引 ----

func TestVerifyBaseline_UniqueIndexesPass(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	seen := 0
	for _, tb := range tables {
		for _, ix := range tb.Indexes {
			if ix.Unique {
				seen++
			}
		}
	}
	if seen != BaselineUniqueIndexCount {
		t.Fatalf("fixture drift: catalog unique indexes = %d, want %d", seen, BaselineUniqueIndexCount)
	}
	if _, _, err := blRun(t, db, tables); err != nil {
		t.Fatalf("clean unique indexes must pass, got: %v", err)
	}
}

func TestVerifyBaseline_FailsOnMissingUniqueIndex(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.dropIndex("companies", "uq_companies_nkey_hash")

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected failure for missing unique index uq_companies_nkey_hash")
	}
	if !strings.Contains(err.Error(), "uq_companies_nkey_hash") {
		t.Fatalf("error %v must name the missing index", err)
	}
	if !strings.Contains(err.Error(), "artex db-migrate --prepare") {
		t.Fatalf("error %v must tell the operator to run artex db-migrate --prepare", err)
	}
}

func TestVerifyBaseline_FailsOnUniqueIndexNotUnique(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	for _, row := range db.indexRows("assets", "uq_assets_hash_ip") {
		row[4] = "1"
	}
	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure when a UNIQUE index is stored as non-unique")
	}
}

func TestVerifyBaseline_FailsOnIndexColumnDrift(t *testing.T) {
	cases := map[string]func(t *testing.T, db *blDB){
		"wrong column": func(t *testing.T, db *blDB) {
			db.indexRows("companies", "uq_companies_nkey_hash")[0][3] = "nkey_identity"
		},
		"missing part": func(t *testing.T, db *blDB) {
			db.indexes = blDeleteWhere(db.indexes, func(row []string) bool {
				return row[0] == "task_asset_links" && row[1] == "uq_task_asset_links" && row[3] == "asset_id"
			})
		},
		"swapped order": func(t *testing.T, db *blDB) {
			rows := db.indexRows("task_asset_links", "uq_task_asset_links")
			if len(rows) != 2 {
				t.Fatalf("fixture drift: uq_task_asset_links has %d parts", len(rows))
			}
			rows[0][3], rows[1][3] = rows[1][3], rows[0][3]
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tables := AllTables()
			db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
			mutate(t, db)
			if _, _, err := blRun(t, db, tables); err == nil {
				t.Fatalf("expected failure for index drift (%s)", name)
			}
		})
	}
}

func TestVerifyBaseline_FailsOnNonUniqueIndexMissing(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.dropIndex("assets", "idx_assets_company")

	if _, _, err := blRun(t, db, tables); err == nil {
		t.Fatalf("expected failure for a missing secondary index")
	}
}

// ---- 错误传播 ----

func TestVerifyBaseline_QueryErrorPropagates(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	sentinel := errors.New("boom: dictionary read failed")
	q := db.querier(t)
	q.failQuery = func(kind blQueryKind, query string) error {
		if kind == blKindTables {
			return sentinel
		}
		return nil
	}
	_, err := runBaselineVerify(context.Background(), q, tables)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the query failure %v", err, sentinel)
	}
	if errors.Is(err, ErrBaselineMismatch) {
		t.Fatalf("query failure must not be reported as %v, got %v", ErrBaselineMismatch, err)
	}
}

func TestVerifyBaseline_RowIterationErrorPropagates(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	sentinel := errors.New("boom: row iteration broke")
	q := db.querier(t)
	inner := q.route
	q.route = func(kind blQueryKind, query string) (Rows, error) {
		rows, err := inner(kind, query)
		if err != nil {
			return nil, err
		}
		if kind == blKindColumns {
			if fr, ok := rows.(*blRows); ok {
				fr.errVal = sentinel
			}
		}
		return rows, nil
	}
	_, err := runBaselineVerify(context.Background(), q, tables)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the iteration failure %v", err, sentinel)
	}
}

func TestVerifyBaseline_ScanErrorPropagates(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	sentinel := errors.New("boom: scan failed")
	q := db.querier(t)
	inner := q.route
	q.route = func(kind blQueryKind, query string) (Rows, error) {
		rows, err := inner(kind, query)
		if err != nil {
			return nil, err
		}
		if kind == blKindIndexes {
			if fr, ok := rows.(*blRows); ok {
				fr.scanErr = sentinel
			}
		}
		return rows, nil
	}
	_, err := runBaselineVerify(context.Background(), q, tables)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the scan failure %v", err, sentinel)
	}
}

func TestVerifyBaseline_CloseErrorPropagates(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	sentinel := errors.New("boom: close failed")
	q := db.querier(t)
	inner := q.route
	q.route = func(kind blQueryKind, query string) (Rows, error) {
		rows, err := inner(kind, query)
		if err != nil {
			return nil, err
		}
		if kind == blKindTables {
			if fr, ok := rows.(*blRows); ok {
				fr.closeErr = sentinel
			}
		}
		return rows, nil
	}
	_, err := runBaselineVerify(context.Background(), q, tables)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the close failure %v", err, sentinel)
	}
}

func TestVerifyBaseline_NilQuerierFails(t *testing.T) {
	if _, err := runBaselineVerify(context.Background(), nil, AllTables()); err == nil {
		t.Fatalf("expected failure for a nil querier")
	}
}

func TestVerifyBaseline_CanceledContextStopsDictionaryReads(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := db.querier(t)
	if _, err := runBaselineVerify(ctx, q, tables); err == nil {
		t.Fatalf("expected failure for a canceled context")
	}
	if queries, _, _ := q.snapshot(); len(queries) != 0 {
		t.Fatalf("canceled context still issued %d queries: %v", len(queries), queries)
	}
}

// ---- 聚合可读性 ----

func TestVerifyBaseline_AggregatesProblemsWithHint(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	db.dropTable("companies")
	db.dropTable("findings")
	db.dropColumn("tasks", "name")

	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected aggregated failure")
	}
	msg := err.Error()
	for _, want := range []string{"companies", "findings", "name", "artex db-migrate --prepare"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("aggregated error must mention %q; got:\n%s", want, msg)
		}
	}
	if n := strings.Count(msg, "\n"); n < 2 {
		t.Fatalf("aggregated error must be a readable multi-line report, got:\n%s", msg)
	}
}

func TestVerifyBaseline_ErrorListIsCappedButCounted(t *testing.T) {
	tables := AllTables()
	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	dropped := 0
	for i := 0; i < 40 && i < len(tables); i++ {
		db.dropTable(tables[i].Name)
		dropped++
	}
	_, _, err := blRun(t, db, tables)
	if err == nil {
		t.Fatalf("expected aggregated failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, "problem(s)") {
		t.Fatalf("error must report the total number of problems; got:\n%s", msg)
	}
	if !strings.Contains(msg, "more") {
		t.Fatalf("error must say that further problems were suppressed (dropped %d tables); got:\n%s", dropped, msg)
	}
	if lines := len(strings.Split(msg, "\n")); lines > baselineMaxReportedProblems+4 {
		t.Fatalf("error report is not capped: %d lines (cap %d)", lines, baselineMaxReportedProblems)
	}
}

// ---- 目录自洽：真实 TiDB 观测口径的字典内容必须能过 65/729/62/21 门 ----

func TestVerifyBaseline_ReportMatchesIndependentCatalogWalk(t *testing.T) {
	tables := AllTables()
	wantTables := len(tables)
	wantCols := 0
	wantUnique := 0
	wantDescIdx := 0
	for _, tb := range tables {
		wantCols += len(tb.Columns)
		for _, ix := range tb.Indexes {
			if ix.Unique {
				wantUnique++
			}
			for _, p := range ix.Parts {
				if p.Desc {
					wantDescIdx++
					break
				}
			}
		}
	}
	if wantTables != BaselineTableCount || wantCols != BaselineColumnCount || wantUnique != BaselineUniqueIndexCount || wantDescIdx != BaselineDescIndexCount {
		t.Fatalf("catalog walk mismatch: tables=%d cols=%d unique=%d descIndexes=%d", wantTables, wantCols, wantUnique, wantDescIdx)
	}

	db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
	report, _, err := blRun(t, db, tables)
	if err != nil {
		t.Fatalf("clean baseline failed: %v", err)
	}
	if report.Tables != wantTables || report.TotalColumns != wantCols || report.UniqueIndexes != wantUnique || report.ToleratedDescIndexes != wantDescIdx {
		t.Fatalf("report %+v does not match catalog walk {tables:%d cols:%d unique:%d desc:%d}",
			report, wantTables, wantCols, wantUnique, wantDescIdx)
	}
}

// ---- 生成列表达式比较（Fix Round 1 / Review R1）----
//
// 口径：只容忍「空白、大小写、标识符反引号、字符集引导符、字符串引号风格」五类书写差异。
// 归一化后的词法 token 序列必须等长且逐位相同：运算符、括号、逗号、关键字、参数位置、
// 重复次数全部保留；条件取反、分支互换、参数换位、新增算术一律判红。

// blExprKeywords 是书写风格变换时视为关键字/函数名（小写、不加反引号）的词。
var blExprKeywords = map[string]bool{
	"if": true, "in": true, "null": true, "is": true, "not": true,
	"and": true, "or": true, "true": true, "false": true, "case": true,
	"when": true, "then": true, "else": true, "end": true,
}

// blExprIsAllDigits 判定纯数字词（数字字面量不加反引号：真实 TiDB 不会把 1 写成 `1`）。
func blExprIsAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func blExprIsWordChar(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

// blExprFormatNoise 只做书写风格变换：关键字/函数名小写、标识符加反引号、字符串字面量补
// _utf8mb4 引导符并改用双引号、逗号后换行缩进、运算符两侧补空格。
// 它不增删任何运算符/括号/逗号，也不改字面量内容，因此归一化后必须与目录表达式逐 token 相同。
// （目录里的生成列表达式不含引号转义，字面量按"无内嵌引号"处理。）
func blExprFormatNoise(expr string) string {
	var b strings.Builder
	runes := []rune(expr)
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case r == '\'' || r == '"':
			quote := r
			var content strings.Builder
			j := i + 1
			for j < len(runes) && runes[j] != quote {
				content.WriteRune(runes[j])
				j++
			}
			b.WriteString("_utf8mb4\"" + content.String() + "\"")
			i = j + 1
		case blExprIsWordChar(r):
			var word strings.Builder
			j := i
			for j < len(runes) && blExprIsWordChar(runes[j]) {
				word.WriteRune(runes[j])
				j++
			}
			w := strings.ToLower(word.String())
			if blExprKeywords[w] || blExprIsAllDigits(w) {
				b.WriteString(w)
			} else {
				b.WriteString("`" + w + "`")
			}
			i = j
		case r == ',':
			b.WriteString(",\n    ")
			i++
		case r == '<' || r == '>' || r == '=' || r == '!':
			op := string(r)
			if r == '<' && i+3 <= len(runes) && string(runes[i:i+3]) == "<=>" {
				op = "<=>"
			} else if i+2 <= len(runes) {
				switch string(runes[i : i+2]) {
				case "<>", "<=", ">=", "!=":
					op = string(runes[i : i+2])
				}
			}
			b.WriteString(" " + op + " ")
			i += len([]rune(op))
		default:
			b.WriteRune(r)
			i++
		}
	}
	return b.String()
}

// blCatalogGeneratedExprs 返回目录里全部 STORED 生成列表达式（专项用例的独立输入源）。
func blCatalogGeneratedExprs(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, tb := range AllTables() {
		for _, c := range tb.Columns {
			if c.Generated != "" {
				out = append(out, c.Generated)
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("catalog drift: no generated column expression found")
	}
	return out
}

// TestBaselineGenerationExprTokensKeepStructure 锁死词法切分本身：
// 顺序、运算符、括号、逗号、重复 token 都必须原样保留，非法文本 fail-closed。
func TestBaselineGenerationExprTokensKeepStructure(t *testing.T) {
	t.Run("有序 token 序列逐位可见", func(t *testing.T) {
		got := baselineExpressionTokens("IF(status='running', session_key, NULL)")
		want := []string{
			"id:if", "op:(", "id:status", "op:=", "str:'running'",
			"op:,", "id:session_key", "op:,", "id:null", "op:)",
		}
		if len(got) != len(want) {
			t.Fatalf("token count = %d (%v), want %d (%v)", len(got), got, len(want), want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("token %d = %s, want %s (full: %v)", i, got[i], want[i], got)
			}
		}
	})

	t.Run("重复 token 不得折叠", func(t *testing.T) {
		got := baselineExpressionTokens("IF(a=b,b,c)")
		var count int
		for _, tok := range got {
			if tok == "id:b" {
				count++
			}
		}
		if count != 2 {
			t.Fatalf("repeated token collapsed: %v holds id:b %d times, want 2", got, count)
		}
	})

	t.Run("参数顺序敏感", func(t *testing.T) {
		if strings.Join(baselineExpressionTokens("f(a,b)"), " ") == strings.Join(baselineExpressionTokens("f(b,a)"), " ") {
			t.Fatalf("argument order is not preserved by the tokenizer")
		}
	})

	t.Run("多字符运算符不被切碎", func(t *testing.T) {
		for _, tc := range []struct{ expr, op string }{
			{"a<=b", "op:<="}, {"a>=b", "op:>="}, {"a<>b", "op:<>"}, {"a!=b", "op:!="},
			{"a<=>b", "op:<=>"}, {"a+b", "op:+"}, {"a-b", "op:-"}, {"a*b", "op:*"}, {"a/b", "op:/"},
		} {
			toks := baselineExpressionTokens(tc.expr)
			var found bool
			for _, tok := range toks {
				if tok == tc.op {
					found = true
				}
			}
			if !found {
				t.Fatalf("operator %s missing from %q tokens %v", tc.op, tc.expr, toks)
			}
		}
	})

	t.Run("逻辑与集合关键字保留", func(t *testing.T) {
		toks := strings.Join(baselineExpressionTokens("IF(a=1 AND b IN ('x','y') OR NOT c IS NULL, 1, NULL)"), " ")
		for _, want := range []string{"id:and", "id:or", "id:not", "id:in", "id:is", "id:null"} {
			if !strings.Contains(toks, want) {
				t.Fatalf("token %s missing from %s", want, toks)
			}
		}
	})

	t.Run("无法切分的表达式 fail-closed", func(t *testing.T) {
		for _, expr := range []string{
			"IF(status='running, session_key, NULL)",   // 字符串字面量未闭合
			"IF(`status='running', session_key, NULL)", // 反引号标识符未闭合
			`IF(status="running, session_key, NULL)`,   // 双引号字面量未闭合
		} {
			if toks := baselineExpressionTokens(expr); toks != nil {
				t.Fatalf("unterminated lexical input %q must yield nil tokens, got %v", expr, toks)
			}
			if baselineGenerationExprMatches("IF(status='running', session_key, NULL)", expr) {
				t.Fatalf("unterminated lexical input %q must never match", expr)
			}
		}
	})
}

// TestBaselineGenerationExprMatchesFormattingOnly 保证五类允许的书写差异不产生伪红。
func TestBaselineGenerationExprMatchesFormattingOnly(t *testing.T) {
	cases := []struct {
		name string
		cat  string
		db   string
	}{
		{"identical", "IF(status='running', session_key, NULL)", "IF(status='running', session_key, NULL)"},
		{"whitespace newline indent", "IF(status='running', session_key, NULL)",
			"IF(\n\t\tstatus   =   'running'\n\t\t,\n\t\tsession_key,\n\t\tNULL\n)"},
		{"function & keyword case", "IF(status='running', session_key, NULL)", "if(status='running', session_key, null)"},
		{"identifier case", "IF(status='running', session_key, NULL)", "IF(STATUS='running', SESSION_KEY, NULL)"},
		{"backticks", "IF(status='running', session_key, NULL)", "IF(`status`='running', `session_key`, NULL)"},
		{"charset introducer", "IF(status='running', session_key, NULL)", "IF(status=_utf8mb4'running', session_key, NULL)"},
		{"other introducers", "IF(status IN ('pending','running'), finding_id, NULL)",
			"IF(status IN (_utf8'pending',_latin1'running'), finding_id, NULL)"},
		{"double quoted literal", "IF(status='running', session_key, NULL)", `IF(status="running", session_key, NULL)`},
		{"doubled quote escape", "IF(status='a''b', x, NULL)", `IF(status="a'b", x, NULL)`},
		{"all five at once", "IF(status='running', session_key, NULL)",
			"if(\n    `status`  =  _utf8mb4\"running\" ,\n    `session_key`,\n    null\n)"},
		{"in list with noise", "IF(status IN ('pending','running'), finding_id, NULL)",
			"if(`status` in (_utf8mb4'pending', _utf8mb4'running'), `finding_id`, null)"},
		{"numeric literal spacing", "IF(is_default,1,NULL)", "if(`is_default`, 1 , NULL)"},
		{"decimal literal", "IF(a=1.50,b,c)", "if(`a` = 1.50 , `b` , `c` )"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !baselineGenerationExprMatches(tc.cat, tc.db) {
				t.Fatalf("formatting-only difference rejected\n catalog: %q\n     db: %q\n tokens: %v vs %v",
					tc.cat, tc.db, baselineExpressionTokens(tc.cat), baselineExpressionTokens(tc.db))
			}
		})
	}

	// 目录里真实存在的生成列：两种书写风格仿真都必须判等。
	for _, expr := range blCatalogGeneratedExprs(t) {
		for name, rewrite := range map[string]func(string) string{
			"tidb style":   blTiDBStyleExpr,
			"format noise": blExprFormatNoise,
		} {
			noise := rewrite(expr)
			t.Run(name+" "+expr, func(t *testing.T) {
				if noise == expr {
					t.Fatalf("fixture is vacuous: %q was not rewritten", expr)
				}
				if !baselineGenerationExprMatches(expr, noise) {
					t.Fatalf("formatting-only rewrite rejected\n catalog: %q\n     db: %q\n tokens: %v vs %v",
						expr, noise, baselineExpressionTokens(expr), baselineExpressionTokens(noise))
				}
			})
		}
	}
}

// TestBaselineGenerationExprLiteralQuoteEquivalence 单独覆盖引号风格与转义归一。
func TestBaselineGenerationExprLiteralQuoteEquivalence(t *testing.T) {
	// 同一字面量的多种书写（引号风格 × 转义方式）；两组内容不同，不得互相判等。
	groups := [][]string{
		{
			"IF(status='a''b', x, NULL)",
			`IF(status='a\'b', x, NULL)`,
			`IF(status="a'b", x, NULL)`,
			`IF(status="a\'b", x, NULL)`,
		},
		{
			`IF(status='a"b', x, NULL)`,
			`IF(status='a\"b', x, NULL)`,
			`IF(status="a""b", x, NULL)`,
			`IF(status="a\"b", x, NULL)`,
		},
	}
	for _, spellings := range groups {
		for _, a := range spellings {
			for _, b := range spellings {
				if !baselineGenerationExprMatches(a, b) {
					t.Fatalf("quote spellings of the same literal must match:\n %q\n %q\n tokens: %v vs %v",
						a, b, baselineExpressionTokens(a), baselineExpressionTokens(b))
				}
			}
		}
	}
	// 字面量内容差异不得被引号/转义归一化吞掉（内容大小写同样敏感）。
	if baselineGenerationExprMatches("IF(status='a', x, NULL)", `IF(status="A", x, NULL)`) {
		t.Fatalf("string literal content is case sensitive and must not be folded")
	}
	if baselineGenerationExprMatches("IF(status='a''b', x, NULL)", `IF(status="a""b", x, NULL)`) {
		t.Fatalf("literals a'b and a\"b are different values and must not match")
	}
}

// TestBaselineGenerationExprRejectsSemanticDrift 是 R1 的核心回归：实质逻辑漂移必须判红。
func TestBaselineGenerationExprRejectsSemanticDrift(t *testing.T) {
	cases := []struct {
		name  string
		cat   string
		db    string
		class string
	}{
		// 1) 条件取反 / 比较运算符改变
		{"eq to neq", "IF(status='running', session_key, NULL)", "IF(status<>'running', session_key, NULL)", "条件取反"},
		{"eq to bang ne", "IF(status='running', session_key, NULL)", "IF(status != 'running', session_key, NULL)", "条件取反"},
		{"eq to lt", "IF(status='running', session_key, NULL)", "IF(status<'running', session_key, NULL)", "运算符改变"},
		{"eq to gte", "IF(status='running', session_key, NULL)", "IF(status>='running', session_key, NULL)", "运算符改变"},
		{"eq to null safe eq", "IF(status='running', session_key, NULL)", "IF(status<=>'running', session_key, NULL)", "运算符改变"},
		{"in to not in", "IF(status IN ('pending','running'), finding_id, NULL)",
			"IF(status NOT IN ('pending','running'), finding_id, NULL)", "条件取反"},
		{"and to or", "IF(a=1 AND b=2, c, NULL)", "IF(a=1 OR b=2, c, NULL)", "逻辑算子改变"},
		{"not inserted", "IF(status='running', session_key, NULL)", "IF(NOT status='running', session_key, NULL)", "条件取反"},

		// 2) 分支互换与参数换位
		{"branches swapped", "IF(status='running', session_key, NULL)", "IF(status='running', NULL, session_key)", "分支互换"},
		{"branches swapped in list", "IF(status IN ('pending','running'), finding_id, NULL)",
			"IF(status IN ('pending','running'), NULL, finding_id)", "分支互换"},
		{"condition and branch swapped", "IF(status='running', session_key, NULL)", "IF(session_key='running', status, NULL)", "参数换位"},
		{"in list reordered", "IF(status IN ('pending','running'), finding_id, NULL)",
			"IF(status IN ('running','pending'), finding_id, NULL)", "参数换位"},

		// 3) 新增/替换/丢失运算
		{"arithmetic added", "IF(is_default,1,NULL)", "IF(is_default+1,1,NULL)", "新增算术"},
		{"arithmetic replaced", "IF(is_default+1,1,NULL)", "IF(is_default-1,1,NULL)", "算术改变"},
		{"arithmetic dropped", "IF(a+1,1,NULL)", "IF(a,1,NULL)", "算术丢失"},
		{"boolean expanded to comparison", "IF(is_default,1,NULL)", "if((`is_default` = 1),1,NULL)", "新增运算"},
		{"divide to multiply", "IF(a/b,1,NULL)", "IF(a*b,1,NULL)", "算术改变"},

		// 4) 结构、重复项与词法类别
		{"parentheses added", "IF(is_default,1,NULL)", "if((`is_default`),1,NULL)", "结构改变"},
		{"parenthesized in list", "IF(status IN ('pending','running'), finding_id, NULL)",
			"if(`status` in ((_utf8mb4'pending',_utf8mb4'running')),`finding_id`,NULL)", "结构改变"},
		{"repeated literal collapsed", "IF(status IN ('pending','running'), finding_id, NULL)",
			"IF(status IN ('pending','pending'), finding_id, NULL)", "重复项折叠"},
		{"repeated column collapsed", "IF(a=b,b,c)", "IF(a=b,c,c)", "重复项折叠"},
		{"function renamed", "IF(is_default,1,NULL)", "IFNULL(is_default,1)", "函数改变"},
		{"number to string literal", "IF(is_default,1,NULL)", "IF(is_default,'1',NULL)", "字面量类别改变"},
		{"backticked digit to number", "IF(is_default,`1`,NULL)", "IF(is_default,'1',NULL)", "字面量类别改变"},
		{"literal case changed", "IF(status='running', session_key, NULL)", "IF(status='Running', session_key, NULL)", "字面量取值改变"},
		{"literal shortened", "IF(status='running', session_key, NULL)", "IF(status='run', session_key, NULL)", "字面量取值改变"},
		{"column renamed", "IF(is_default,1,NULL)", "if((`is_active` = 1),1,NULL)", "列改变"},
		{"list item dropped", "IF(status IN ('pending','running'), finding_id, NULL)",
			"if((`status` in (_utf8mb4'pending')),`finding_id`,NULL)", "参数丢失"},
		{"argument dropped", "IF(status='running', session_key, NULL)", "IF(status='running', session_key)", "参数丢失"},
		{"argument appended", "IF(status='running', session_key, NULL)", "IF(status='running', session_key, NULL, NULL)", "参数新增"},

		// 5) 空与非法输入
		{"empty db expression", "IF(is_default,1,NULL)", "", "空表达式"},
		{"whitespace only db expression", "IF(is_default,1,NULL)", "   \n ", "空表达式"},
		{"unterminated literal", "IF(status='running', session_key, NULL)", "IF(status='running, session_key, NULL)", "词法不可切分"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if baselineGenerationExprMatches(tc.cat, tc.db) {
				t.Fatalf("%s drift accepted as equal\n catalog: %q\n     db: %q\n tokens: %v == %v",
					tc.class, tc.cat, tc.db, baselineExpressionTokens(tc.cat), baselineExpressionTokens(tc.db))
			}
		})
	}
}

// TestVerifyBaseline_GeneratedColumnsPassWithTiDBExprSpellings 走全量基线路径：
// 3 个生成列同时按书写风格仿真改写（大小写 + 反引号 + 引导符 + 引号风格 + 空白）必须整体通过。
func TestVerifyBaseline_GeneratedColumnsPassWithTiDBExprSpellings(t *testing.T) {
	for _, name := range []string{"tidb style", "format noise"} {
		name := name
		t.Run(name, func(t *testing.T) {
			rewrite := func(expr string) string { return blTiDBStyleExpr(expr) }
			if name == "format noise" {
				rewrite = blExprFormatNoise
			}
			tables := AllTables()
			db := blSynthesize(t, tables, blSynthOptions{
				DescCollation: "A",
				GeneratedExpr: func(c Column) string { return rewrite(c.Generated) },
			})
			if _, _, err := blRun(t, db, tables); err != nil {
				t.Fatalf("generated expressions written as %s must pass, got: %v", name, err)
			}
		})
	}
}

// TestVerifyBaseline_FailsOnGeneratedExprSemanticDrift 走全量基线路径：
// 条件取反 / 分支互换 / 新增算术 / 参数换位等实质漂移必须返回 ErrBaselineMismatch。
func TestVerifyBaseline_FailsOnGeneratedExprSemanticDrift(t *testing.T) {
	cases := []struct {
		name   string
		table  string
		column string
		expr   string
	}{
		{"condition negated neq", "side_question_requests", "running_session_key", "IF(status<>'running', session_key, NULL)"},
		{"condition negated bang", "side_question_requests", "running_session_key", "IF(status != 'running', session_key, NULL)"},
		{"condition widened", "side_question_requests", "running_session_key", "IF(status>'running', session_key, NULL)"},
		{"branches swapped", "side_question_requests", "running_session_key", "IF(status='running', NULL, session_key)"},
		{"arithmetic added", "llm_profiles", "is_default_key", "IF(is_default+1,1,NULL)"},
		{"arithmetic replaced", "llm_profiles", "is_default_key", "IF(is_default-1,1,NULL)"},
		{"boolean expanded with operator", "llm_profiles", "is_default_key", "if((`is_default` = 1),1,NULL)"},
		{"literal value changed", "llm_profiles", "is_default_key", "IF(is_default,2,NULL)"},
		{"in list reordered", "finding_retests", "active_finding_id", "IF(status IN ('running','pending'), finding_id, NULL)"},
		{"in negated", "finding_retests", "active_finding_id", "IF(status NOT IN ('pending','running'), finding_id, NULL)"},
		{"branches swapped retest", "finding_retests", "active_finding_id", "IF(status IN ('pending','running'), NULL, finding_id)"},
		{"repeated literal collapsed", "finding_retests", "active_finding_id", "IF(status IN ('pending','pending'), finding_id, NULL)"},
		{"redundant parens around condition", "finding_retests", "active_finding_id",
			"if((`status` in (_utf8mb4'pending',_utf8mb4'running')),`finding_id`,NULL)"},
		{"plain column expression", "finding_retests", "active_finding_id", "concat(`status`,'x')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tables := AllTables()
			db := blSynthesize(t, tables, blSynthOptions{DescCollation: "A"})
			db.columnRow(tc.table, tc.column)[5] = tc.expr
			_, _, err := blRun(t, db, tables)
			if !errors.Is(err, ErrBaselineMismatch) {
				t.Fatalf("semantic expression drift accepted: %s.%s = %q; err=%v", tc.table, tc.column, tc.expr, err)
			}
			if !strings.Contains(err.Error(), tc.column) {
				t.Fatalf("error must name the drifted column %s: %v", tc.column, err)
			}
		})
	}
}
