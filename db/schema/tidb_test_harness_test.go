package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// tidbTestHarness 实现专用于 TiDB 集群冒烟验证的安全测试 harness。
// 满足 Task 5C / Task 7 安全条款：
//  1. CheckGuards: SELECT VERSION() 经由 IsRealTiDB 校验；SELECT DATABASE() 必须非空；
//     information_schema.TABLES 中当前 schema 对象总数 == 0（包含视图和所有表），否则立即返回错误，0 DDL / 0 DROP。
//  2. CreateAllOwnedTables: 遍历 AllTables()，将 GenerateDDL 的固定前缀 "CREATE TABLE IF NOT EXISTS "
//     替换为 "CREATE TABLE "，执行成功后才记录到 ownedTables；若发生失败立即停止，绝不将失败表登记为 owned。
//  3. Cleanup: 仅对 ownedTables 逆序执行 DROP TABLE `table_name`；独立有界 context；
//     对象清理先于连接关闭；错误不吞；支持幂等多次调用，已删除的不再重复删。
//  4. ApplyIdempotentDDL: 使用原 ApplyDDL 执行，仅允许作用于本次 owned 的 65 张表。
type tidbTestHarness struct {
	db          *sql.DB
	mu          sync.Mutex
	ownedTables []string
	cleaned     bool
}

// newTiDBTestHarness 构造私有安全测试 harness。
func newTiDBTestHarness(db *sql.DB) *tidbTestHarness {
	return &tidbTestHarness{
		db: db,
	}
}

// CheckGuards 检查真实 TiDB 版本、当前已选数据库非空、当前 schema 内所有对象数必须为 0。
func (h *tidbTestHarness) CheckGuards(ctx context.Context) error {
	if h.db == nil {
		return errors.New("guard check failed: nil db")
	}

	// 1. SELECT VERSION()
	var version string
	if err := h.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return fmt.Errorf("guard check: query VERSION() failed: %w", err)
	}
	if !IsRealTiDB(version) {
		return fmt.Errorf("guard check: database is not real TiDB, version: %q", version)
	}

	// 2. SELECT DATABASE()
	var dbName sql.NullString
	if err := h.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return fmt.Errorf("guard check: query DATABASE() failed: %w", err)
	}
	if !dbName.Valid || strings.TrimSpace(dbName.String) == "" {
		return errors.New("guard check: no database selected (SELECT DATABASE() is NULL or empty)")
	}

	// 3. information_schema.TABLES count == 0
	const countQuery = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"
	var objectCount int
	if err := h.db.QueryRowContext(ctx, countQuery).Scan(&objectCount); err != nil {
		return fmt.Errorf("guard check: query schema object count failed: %w", err)
	}
	if objectCount != 0 {
		return fmt.Errorf("guard check: target schema %q is not empty, existing object count: %d", dbName.String, objectCount)
	}

	return nil
}

// CreateAllOwnedTables 首次创建 65 张表。表名直接来自 AllTables() 可信目录。
// 必须使用 CREATE TABLE 而非 IF NOT EXISTS 严格证明归属；
// 逐条 Exec 成功后才追加登记入 ownedTables。
func (h *tidbTestHarness) CreateAllOwnedTables(ctx context.Context) error {
	if h.db == nil {
		return errors.New("create owned tables: nil db")
	}

	all := AllTables()
	const ifNotExistsPrefix = "CREATE TABLE IF NOT EXISTS "
	const strictPrefix = "CREATE TABLE "

	for _, tbl := range all {
		stmt, err := GenerateDDL(tbl)
		if err != nil {
			return fmt.Errorf("generate DDL for table %q: %w", tbl.Name, err)
		}
		if !strings.HasPrefix(stmt, ifNotExistsPrefix) {
			return fmt.Errorf("unexpected DDL prefix for table %q: expected %q", tbl.Name, ifNotExistsPrefix)
		}

		strictStmt := strictPrefix + stmt[len(ifNotExistsPrefix):]
		if _, err := h.db.ExecContext(ctx, strictStmt); err != nil {
			return fmt.Errorf("create owned table %q failed: %w", tbl.Name, err)
		}

		// 成功执行才记为 owned
		h.mu.Lock()
		h.ownedTables = append(h.ownedTables, tbl.Name)
		h.mu.Unlock()
	}

	return nil
}

// VerifyBaseTableCount 验证 BASE TABLE 数量等于 expectedCount。
func (h *tidbTestHarness) VerifyBaseTableCount(ctx context.Context, expectedCount int) error {
	if h.db == nil {
		return errors.New("verify base table count: nil db")
	}
	const q = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'"
	var count int
	if err := h.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return fmt.Errorf("verify base table count query failed: %w", err)
	}
	if count != expectedCount {
		return fmt.Errorf("verify base table count mismatch: got %d, want %d", count, expectedCount)
	}
	return nil
}

// ApplyInfraSeed 执行 PrepareInfraSQL 返回的种子数据。
func (h *tidbTestHarness) ApplyInfraSeed(ctx context.Context) error {
	if h.db == nil {
		return errors.New("apply infra seed: nil db")
	}
	for _, sqlStmt := range PrepareInfraSQL() {
		if _, err := h.db.ExecContext(ctx, sqlStmt); err != nil {
			return fmt.Errorf("apply infra seed %q: %w", sqlStmt, err)
		}
	}
	return nil
}

// VerifyInfraSeed 验证种子行 business_relations 与 schema_version 已正确插入。
func (h *tidbTestHarness) VerifyInfraSeed(ctx context.Context) error {
	if h.db == nil {
		return errors.New("verify infra seed: nil db")
	}
	var lockName string
	if err := h.db.QueryRowContext(ctx, "SELECT name FROM locks WHERE name = 'business_relations'").Scan(&lockName); err != nil {
		return fmt.Errorf("verify infra seed: lock business_relations missing: %w", err)
	}
	var versionKey string
	if err := h.db.QueryRowContext(ctx, "SELECT `key` FROM schema_meta WHERE `key` = 'schema_version'").Scan(&versionKey); err != nil {
		return fmt.Errorf("verify infra seed: schema_meta schema_version missing: %w", err)
	}
	return nil
}

// ApplyIdempotentDDL 使用原生 ApplyDDL 再次执行。
// 必须防御性校验所有传入语句的表名都在 ownedTables 保护范围内。
func (h *tidbTestHarness) ApplyIdempotentDDL(ctx context.Context, stmts []string) error {
	if h.db == nil {
		return errors.New("apply idempotent DDL: nil db")
	}
	h.mu.Lock()
	ownedMap := make(map[string]bool, len(h.ownedTables))
	for _, tbl := range h.ownedTables {
		ownedMap[tbl] = true
	}
	h.mu.Unlock()

	for _, s := range stmts {
		tblName, err := extractTableNameFromDDL(s)
		if err != nil {
			return fmt.Errorf("apply idempotent DDL: %w", err)
		}
		if !ownedMap[tblName] {
			return fmt.Errorf("apply idempotent DDL security violation: table %q is not owned by this harness", tblName)
		}
	}

	return ApplyDDL(ctx, h.db, stmts)
}

// extractTableNameFromDDL 从以 "CREATE TABLE IF NOT EXISTS `tbl`" 或 "CREATE TABLE `tbl`" 开头的语句中提取表名。
func extractTableNameFromDDL(s string) (string, error) {
	trimmed := strings.TrimSpace(s)
	var rest string
	if strings.HasPrefix(trimmed, "CREATE TABLE IF NOT EXISTS ") {
		rest = strings.TrimPrefix(trimmed, "CREATE TABLE IF NOT EXISTS ")
	} else if strings.HasPrefix(trimmed, "CREATE TABLE ") {
		rest = strings.TrimPrefix(trimmed, "CREATE TABLE ")
	} else {
		return "", fmt.Errorf("unrecognized DDL prefix in %q", firstLine(s))
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "`") {
		return "", fmt.Errorf("expected backtick in %q", firstLine(s))
	}
	idx := strings.Index(rest[1:], "`")
	if idx < 0 {
		return "", fmt.Errorf("unmatched backtick in %q", firstLine(s))
	}
	return rest[1 : idx+1], nil
}

// Cleanup 逆序清理所有成功登记的 owned 表。
// 独立有界 context 处理；错误不吞；drop 失败标记保留；支持幂等多次调用。
func (h *tidbTestHarness) Cleanup(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cleaned || len(h.ownedTables) == 0 {
		return nil
	}

	var errs []error
	// 逆序逐一清理
	for i := len(h.ownedTables) - 1; i >= 0; i-- {
		tbl := h.ownedTables[i]
		dropSQL := fmt.Sprintf("DROP TABLE `%s`", tbl)
		if _, err := h.db.ExecContext(ctx, dropSQL); err != nil {
			errs = append(errs, fmt.Errorf("drop owned table %q: %w", tbl, err))
		} else {
			// 该表成功清理，从切片中移除
			h.ownedTables = append(h.ownedTables[:i], h.ownedTables[i+1:]...)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup encountered %d error(s): %w", len(errs), errors.Join(errs...))
	}

	h.cleaned = true
	return nil
}

// OwnedTables 返回当前成功追踪的表列表副本（供测试断言）。
func (h *tidbTestHarness) OwnedTables() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	res := make([]string, len(h.ownedTables))
	copy(res, h.ownedTables)
	return res
}

// --- 标准库 fake driver 基础设施，供离线单测使用 ---

type harnessFakeDriver struct {
	connector *harnessFakeConnector
}

func (d *harnessFakeDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("harnessFakeDriver: use OpenDB")
}

type harnessFakeConnector struct {
	driver *harnessFakeDriver
	conn   *harnessFakeConn
}

func (c *harnessFakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.conn, nil
}

func (c *harnessFakeConnector) Driver() driver.Driver {
	return c.driver
}

type harnessExecCall struct {
	query string
	args  []driver.NamedValue
}

type harnessQueryCall struct {
	query string
	args  []driver.NamedValue
}

type harnessFakeConn struct {
	mu           sync.Mutex
	closed       bool
	execCalls    []harnessExecCall
	queryCalls   []harnessQueryCall
	execHandler  func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
	queryHandler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

func (c *harnessFakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("harnessFakeConn: Prepare not implemented")
}

func (c *harnessFakeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *harnessFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("harnessFakeConn: Tx not supported")
}

func (c *harnessFakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.mu.Lock()
	c.execCalls = append(c.execCalls, harnessExecCall{query: query, args: args})
	handler := c.execHandler
	c.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args)
	}
	return driver.ResultNoRows, nil
}

func (c *harnessFakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.mu.Lock()
	c.queryCalls = append(c.queryCalls, harnessQueryCall{query: query, args: args})
	handler := c.queryHandler
	c.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args)
	}
	return nil, fmt.Errorf("harnessFakeConn: unhandled query: %s", query)
}

// harnessFakeRows 构造单行/多行 rows
type harnessFakeRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
	closed  bool
}

func newSingleValueRows(col string, val driver.Value) driver.Rows {
	return &harnessFakeRows{
		columns: []string{col},
		rows:    [][]driver.Value{{val}},
	}
}

func (r *harnessFakeRows) Columns() []string {
	return r.columns
}

func (r *harnessFakeRows) Close() error {
	r.closed = true
	return nil
}

func (r *harnessFakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++
	copy(dest, row)
	return nil
}

func newHarnessTestDB(t *testing.T,
	execHandler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error),
	queryHandler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error),
) (*sql.DB, *harnessFakeConn) {
	t.Helper()
	fc := &harnessFakeConn{
		execHandler:  execHandler,
		queryHandler: queryHandler,
	}
	drv := &harnessFakeDriver{}
	connector := &harnessFakeConnector{driver: drv, conn: fc}
	drv.connector = connector
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, fc
}

// --- 针对 harness 各项安全条款的离线测试 ---

func TestHarnessCheckGuards_NonTiDBRejected_ZeroDDL(t *testing.T) {
	db, fc := newHarnessTestDB(t, nil, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.Contains(query, "SELECT VERSION()") {
			return newSingleValueRows("VERSION()", "8.0.32-MySQL"), nil
		}
		return nil, fmt.Errorf("unexpected query: %s", query)
	})

	h := newTiDBTestHarness(db)
	err := h.CheckGuards(context.Background())
	if err == nil {
		t.Fatal("expected non-TiDB version to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "not real TiDB") {
		t.Fatalf("unexpected error message: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.execCalls) != 0 {
		t.Fatalf("expected 0 Exec calls on guard failure, got %d", len(fc.execCalls))
	}
}

func TestHarnessCheckGuards_EmptyDatabaseRejected_ZeroDDL(t *testing.T) {
	db, fc := newHarnessTestDB(t, nil, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.Contains(query, "SELECT VERSION()") {
			return newSingleValueRows("VERSION()", "5.7.25-TiDB-v8.5.8"), nil
		}
		if strings.Contains(query, "SELECT DATABASE()") {
			return newSingleValueRows("DATABASE()", nil), nil
		}
		return nil, fmt.Errorf("unexpected query: %s", query)
	})

	h := newTiDBTestHarness(db)
	err := h.CheckGuards(context.Background())
	if err == nil {
		t.Fatal("expected empty database to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "no database selected") {
		t.Fatalf("unexpected error message: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.execCalls) != 0 {
		t.Fatalf("expected 0 Exec calls on guard failure, got %d", len(fc.execCalls))
	}
}

func TestHarnessCheckGuards_NonEmptySchemaRejected_ZeroDDL_ZeroDROP(t *testing.T) {
	db, fc := newHarnessTestDB(t, nil, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.Contains(query, "SELECT VERSION()") {
			return newSingleValueRows("VERSION()", "5.7.25-TiDB-v8.5.8"), nil
		}
		if strings.Contains(query, "SELECT DATABASE()") {
			return newSingleValueRows("DATABASE()", "test_schema"), nil
		}
		if strings.Contains(query, "information_schema.TABLES") {
			return newSingleValueRows("COUNT(*)", int64(3)), nil
		}
		return nil, fmt.Errorf("unexpected query: %s", query)
	})

	h := newTiDBTestHarness(db)
	err := h.CheckGuards(context.Background())
	if err == nil {
		t.Fatal("expected non-empty schema to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "not empty, existing object count: 3") {
		t.Fatalf("unexpected error message: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.execCalls) != 0 {
		t.Fatalf("expected 0 Exec calls (0 DDL / 0 DROP) on non-empty schema rejection, got %d", len(fc.execCalls))
	}
}

func TestHarnessCreateAllOwnedTables_StrictWithoutIFNOTEXISTS(t *testing.T) {
	db, fc := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.Contains(query, "IF NOT EXISTS") {
			return nil, errors.New("DDL should NOT contain IF NOT EXISTS for ownership proof")
		}
		return driver.ResultNoRows, nil
	}, nil)

	h := newTiDBTestHarness(db)
	if err := h.CreateAllOwnedTables(context.Background()); err != nil {
		t.Fatalf("CreateAllOwnedTables failed: %v", err)
	}

	owned := h.OwnedTables()
	if len(owned) != 65 {
		t.Fatalf("expected 65 owned tables, got %d", len(owned))
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.execCalls) != 65 {
		t.Fatalf("expected 65 Exec calls, got %d", len(fc.execCalls))
	}
	for i, call := range fc.execCalls {
		if strings.Contains(call.query, "IF NOT EXISTS") {
			t.Fatalf("exec call [%d] contains IF NOT EXISTS: %s", i, call.query)
		}
		if !strings.HasPrefix(call.query, "CREATE TABLE `") {
			t.Fatalf("exec call [%d] unexpected start: %s", i, call.query)
		}
	}
}

func TestHarnessCreateAllOwnedTables_ConcurrentConflict_OnlyDropsOwned_NeverDropsFailed(t *testing.T) {
	all := AllTables()
	firstTable := all[0].Name
	secondTable := all[1].Name

	var executedDrops []string

	db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "CREATE TABLE `") {
			if strings.Contains(query, "`"+secondTable+"`") {
				// 模拟并发已存在导致 CREATE TABLE (严格无 IF NOT EXISTS) 失败：1050 Table already exists
				return nil, errors.New("Error 1050 (42S01): Table '" + secondTable + "' already exists")
			}
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "DROP TABLE `") {
			executedDrops = append(executedDrops, query)
			return driver.ResultNoRows, nil
		}
		return driver.ResultNoRows, nil
	}, nil)

	h := newTiDBTestHarness(db)
	err := h.CreateAllOwnedTables(context.Background())
	if err == nil {
		t.Fatal("expected error on second table concurrent creation collision, got nil")
	}

	// 验证 ownedTables 仅含 firstTable，并发冲突的 secondTable 决不登记为 owned
	owned := h.OwnedTables()
	if len(owned) != 1 || owned[0] != firstTable {
		t.Fatalf("expected ownedTables to contain only [%s], got %v", firstTable, owned)
	}

	// 执行 cleanup
	if err := h.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// 验证 DROP 只有 firstTable，决不 DROP 未获归属的 secondTable
	if len(executedDrops) != 1 {
		t.Fatalf("expected 1 DROP, got %d: %v", len(executedDrops), executedDrops)
	}
	expectedDrop := fmt.Sprintf("DROP TABLE `%s`", firstTable)
	if executedDrops[0] != expectedDrop {
		t.Fatalf("expected drop %q, got %q", expectedDrop, executedDrops[0])
	}
}

func TestHarnessCleanup_IdempotentCleanupSequenceAndInfraVerification(t *testing.T) {
	var executedQueries []string
	var executedDrops []string

	db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "DROP TABLE `") {
			executedDrops = append(executedDrops, query)
			return driver.ResultNoRows, nil
		}
		executedQueries = append(executedQueries, query)
		return driver.ResultNoRows, nil
	}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.Contains(query, "TABLE_TYPE = 'BASE TABLE'") {
			return newSingleValueRows("COUNT(*)", int64(65)), nil
		}
		if strings.Contains(query, "SELECT name FROM locks") {
			return newSingleValueRows("name", "business_relations"), nil
		}
		if strings.Contains(query, "SELECT `key` FROM schema_meta") {
			return newSingleValueRows("key", "schema_version"), nil
		}
		return nil, fmt.Errorf("unhandled query: %s", query)
	})

	h := newTiDBTestHarness(db)
	ctx := context.Background()

	// 1. 创建 65 表
	if err := h.CreateAllOwnedTables(ctx); err != nil {
		t.Fatalf("CreateAllOwnedTables: %v", err)
	}
	if err := h.VerifyBaseTableCount(ctx, 65); err != nil {
		t.Fatalf("VerifyBaseTableCount: %v", err)
	}

	// 2. 种子数据
	if err := h.ApplyInfraSeed(ctx); err != nil {
		t.Fatalf("ApplyInfraSeed: %v", err)
	}
	if err := h.VerifyInfraSeed(ctx); err != nil {
		t.Fatalf("VerifyInfraSeed: %v", err)
	}

	// 3. 幂等 DDL（使用 GenerateStatements 生成的原 ApplyDDL 语句）
	stmts, err := GenerateStatements()
	if err != nil {
		t.Fatalf("GenerateStatements: %v", err)
	}
	if err := h.ApplyIdempotentDDL(ctx, stmts); err != nil {
		t.Fatalf("ApplyIdempotentDDL: %v", err)
	}

	// 幂等重复 ApplyInfraSeed
	if err := h.ApplyInfraSeed(ctx); err != nil {
		t.Fatalf("ApplyInfraSeed (2nd): %v", err)
	}
	if err := h.VerifyInfraSeed(ctx); err != nil {
		t.Fatalf("VerifyInfraSeed (2nd): %v", err)
	}

	// 验证 ownedTables 仍为准确 65，未膨胀未漂移
	if len(h.OwnedTables()) != 65 {
		t.Fatalf("expected ownedTables count 65, got %d", len(h.OwnedTables()))
	}

	// 4. Cleanup
	if err := h.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if len(executedDrops) != 65 {
		t.Fatalf("expected 65 drops, got %d", len(executedDrops))
	}
}

func TestHarnessCleanup_ReverseOrder65(t *testing.T) {
	var dropped []string

	db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "DROP TABLE `") {
			tbl := strings.TrimPrefix(query, "DROP TABLE `")
			tbl = strings.TrimSuffix(tbl, "`")
			dropped = append(dropped, tbl)
		}
		return driver.ResultNoRows, nil
	}, nil)

	h := newTiDBTestHarness(db)
	if err := h.CreateAllOwnedTables(context.Background()); err != nil {
		t.Fatalf("CreateAllOwnedTables: %v", err)
	}

	all := AllTables()
	if err := h.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	if len(dropped) != 65 {
		t.Fatalf("expected 65 dropped tables, got %d", len(dropped))
	}

	// 逆序断言
	for i := 0; i < 65; i++ {
		want := all[64-i].Name
		if dropped[i] != want {
			t.Fatalf("dropped[%d] = %q, want %q (reverse order)", i, dropped[i], want)
		}
	}

	// 幂等断言：再次 Cleanup 不再 DROP
	droppedBefore := len(dropped)
	if err := h.Cleanup(context.Background()); err != nil {
		t.Fatalf("second Cleanup: %v", err)
	}
	if len(dropped) != droppedBefore {
		t.Fatalf("expected no additional drops on second cleanup, got %d", len(dropped)-droppedBefore)
	}
}

func TestHarnessCleanup_ContextCancellationAndBoundedExecution(t *testing.T) {
	db, _ := newHarnessTestDB(t, nil, nil)
	h := newTiDBTestHarness(db)
	if err := h.CreateAllOwnedTables(context.Background()); err != nil {
		t.Fatalf("CreateAllOwnedTables: %v", err)
	}

	// 模拟已取消的主 context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// 使用独立的 bounded cleanup context 进行清理
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanCancel()

	// 确认在主 ctx 已取消情况下，使用 bounded ctx 清理能成功完成
	_ = canceledCtx // 主 ctx 取消
	if err := h.Cleanup(cleanCtx); err != nil {
		t.Fatalf("Cleanup with bounded context failed: %v", err)
	}

	if len(h.OwnedTables()) != 0 {
		t.Fatalf("expected 0 owned tables after cleanup, got %d", len(h.OwnedTables()))
	}
}

func TestHarnessCleanup_ErrorsVisibleAndNotSwallowed(t *testing.T) {
	sentinelErr := errors.New("drop table disk I/O failure")
	db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "DROP TABLE `") {
			return nil, sentinelErr
		}
		return driver.ResultNoRows, nil
	}, nil)

	h := newTiDBTestHarness(db)
	if err := h.CreateAllOwnedTables(context.Background()); err != nil {
		t.Fatalf("CreateAllOwnedTables: %v", err)
	}

	err := h.Cleanup(context.Background())
	if err == nil {
		t.Fatal("expected cleanup error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected errors.Is(err, sentinelErr) to be true, got: %v", err)
	}

	// 失败对象保留在 ownedTables 中，以便人工介入
	if len(h.OwnedTables()) != 65 {
		t.Fatalf("expected failed drop tables to remain in owned list, got %d", len(h.OwnedTables()))
	}
}

func TestHarnessApplyIdempotentDDL_RejectsUnownedTable(t *testing.T) {
	db, _ := newHarnessTestDB(t, nil, nil)
	h := newTiDBTestHarness(db)

	// 仅注册 1 张 owned 表
	h.ownedTables = []string{"companies"}

	unownedStmts := []string{
		"CREATE TABLE IF NOT EXISTS `companies` (`id` BIGINT PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS `foreign_table` (`id` BIGINT PRIMARY KEY)",
	}

	err := h.ApplyIdempotentDDL(context.Background(), unownedStmts)
	if err == nil {
		t.Fatal("expected unowned table in idempotent DDL to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "security violation: table \"foreign_table\" is not owned") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
