package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
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
			if keywords[w] {
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

// ---- 表达式归一化单元测试 ----

func TestBaselineGenerationExprNormalizedCompare(t *testing.T) {
	cases := []struct {
		name  string
		cat   string
		db    string
		match bool
	}{
		{"tidb style is_default", "IF(is_default,1,NULL)", "if((`is_default` = 1),1,NULL)", true},
		{"tidb style active_finding_id", "IF(status IN ('pending','running'), finding_id, NULL)",
			"if((`status` in (_utf8mb4'pending',_utf8mb4'running')),`finding_id`,NULL)", true},
		{"identical", "IF(status='running', session_key, NULL)", "IF(status='running', session_key, NULL)", true},
		{"wrong column", "IF(is_default,1,NULL)", "if((`is_active` = 1),1,NULL)", false},
		{"dropped literal", "IF(status IN ('pending','running'), finding_id, NULL)",
			"if((`status` in (_utf8mb4'pending')),`finding_id`,NULL)", false},
		{"empty db expression", "IF(is_default,1,NULL)", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := baselineGenerationExprMatches(tc.cat, tc.db); got != tc.match {
				t.Fatalf("baselineGenerationExprMatches(%q, %q) = %v, want %v", tc.cat, tc.db, got, tc.match)
			}
		})
	}
	// 归一化自身必须可传递（同一表达式两种书写都收敛到同一 token 组）。
	a := blExprTokens("IF(status IN ('pending','running'), finding_id, NULL)")
	b := blExprTokens("if((`status` in (_utf8mb4'pending',_utf8mb4'running')),`finding_id`,NULL)")
	if strings.Join(a, "\x1f") != strings.Join(b, "\x1f") {
		t.Fatalf("token normalization is not stable:\n%v\n%v", a, b)
	}
}

func blExprTokens(expr string) []string {
	toks := baselineExpressionTokens(expr)
	out := append([]string(nil), toks...)
	sort.Strings(out)
	return out
}
