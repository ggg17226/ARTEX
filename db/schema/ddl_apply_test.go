package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

type ddlExecCall struct {
	query string
}

type ddlFakeDriver struct {
	connector *ddlFakeConnector
}

func (d *ddlFakeDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("ddlFakeDriver: Open not implemented, use OpenDB")
}

type ddlFakeConnector struct {
	driver *ddlFakeDriver
	conn   *ddlFakeConn
}

func (c *ddlFakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.conn, nil
}

func (c *ddlFakeConnector) Driver() driver.Driver {
	return c.driver
}

type ddlFakeConn struct {
	mu            sync.Mutex
	execCalls     []ddlExecCall
	beginCount    int
	commitCount   int
	rollbackCount int
	execHandler   func(ctx context.Context, query string) (driver.Result, error)
}

func (c *ddlFakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("ddlFakeConn: Prepare not implemented")
}

func (c *ddlFakeConn) Close() error {
	return nil
}

func (c *ddlFakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *ddlFakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beginCount++
	return &ddlFakeTx{conn: c}, nil
}

func (c *ddlFakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.mu.Lock()
	c.execCalls = append(c.execCalls, ddlExecCall{query: query})
	handler := c.execHandler
	c.mu.Unlock()

	if handler != nil {
		return handler(ctx, query)
	}
	return driver.ResultNoRows, nil
}

type ddlFakeTx struct {
	conn *ddlFakeConn
}

func (t *ddlFakeTx) Commit() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	t.conn.commitCount++
	return nil
}

func (t *ddlFakeTx) Rollback() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	t.conn.rollbackCount++
	return nil
}

func newDDLTestDB(t *testing.T, handler func(ctx context.Context, query string) (driver.Result, error)) (*sql.DB, *ddlFakeConn) {
	t.Helper()
	fc := &ddlFakeConn{execHandler: handler}
	drv := &ddlFakeDriver{}
	connector := &ddlFakeConnector{driver: drv, conn: fc}
	drv.connector = connector
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, fc
}

func TestApplyDDL_SuccessSequenceAndNoTx(t *testing.T) {
	stmts := []string{
		"CREATE TABLE `first` (`id` BIGINT PRIMARY KEY);",
		"CREATE TABLE `second` (`id` BIGINT PRIMARY KEY);",
		"CREATE TABLE `测试表` (`名称` VARCHAR(32) NOT NULL) COMMENT = '包含Unicode与特殊符号';",
	}

	db, fc := newDDLTestDB(t, nil)
	ctx := context.Background()

	if err := ApplyDDL(ctx, db, stmts); err != nil {
		t.Fatalf("ApplyDDL returned unexpected error: %v", err)
	}

	fc.mu.Lock()
	calls := make([]ddlExecCall, len(fc.execCalls))
	copy(calls, fc.execCalls)
	begins := fc.beginCount
	commits := fc.commitCount
	rollbacks := fc.rollbackCount
	fc.mu.Unlock()

	if len(calls) != len(stmts) {
		t.Fatalf("expected %d Exec calls, got %d", len(stmts), len(calls))
	}
	for i, want := range stmts {
		if calls[i].query != want {
			t.Errorf("call[%d] query mismatch:\ngot:  %s\nwant: %s", i, calls[i].query, want)
		}
	}

	if begins != 0 || commits != 0 || rollbacks != 0 {
		t.Errorf("expected no transaction lifecycle calls, got begin=%d commit=%d rollback=%d",
			begins, commits, rollbacks)
	}

	// Empty and nil slices should succeed with 0 additional queries
	if err := ApplyDDL(ctx, db, []string{}); err != nil {
		t.Errorf("ApplyDDL with empty slice failed: %v", err)
	}
	if err := ApplyDDL(ctx, db, nil); err != nil {
		t.Errorf("ApplyDDL with nil slice failed: %v", err)
	}

	fc.mu.Lock()
	totalCalls := len(fc.execCalls)
	fc.mu.Unlock()
	if totalCalls != len(stmts) {
		t.Errorf("expected still %d calls after empty/nil ApplyDDL, got %d", len(stmts), totalCalls)
	}
}

func TestApplyDDL_FirstErrorStopsAndWraps(t *testing.T) {
	sentinelErr := errors.New("driver execution failure: table already exists")
	stmts := []string{
		"CREATE TABLE `table_one` (`id` BIGINT);",
		"CREATE TABLE `table_two` (\n  `id` BIGINT PRIMARY KEY,\n  `name` VARCHAR(64)\n);",
		"CREATE TABLE `table_three` (`id` BIGINT);",
	}

	db, fc := newDDLTestDB(t, func(ctx context.Context, query string) (driver.Result, error) {
		if strings.Contains(query, "table_two") {
			return nil, sentinelErr
		}
		return driver.ResultNoRows, nil
	})

	ctx := context.Background()
	err := ApplyDDL(ctx, db, stmts)
	if err == nil {
		t.Fatal("expected ApplyDDL to return error, got nil")
	}

	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected errors.Is(err, sentinelErr) to be true, got err: %v", err)
	}

	// Verify error message formatting includes first line context
	expectedFirstLine := "CREATE TABLE `table_two` ("
	if !strings.Contains(err.Error(), expectedFirstLine) {
		t.Errorf("expected error message to contain first line %q, got: %s", expectedFirstLine, err.Error())
	}

	fc.mu.Lock()
	calls := make([]ddlExecCall, len(fc.execCalls))
	copy(calls, fc.execCalls)
	begins := fc.beginCount
	commits := fc.commitCount
	rollbacks := fc.rollbackCount
	fc.mu.Unlock()

	// Third statement must NOT have been executed
	if len(calls) != 2 {
		t.Fatalf("expected exactly 2 Exec calls before stop, got %d", len(calls))
	}
	if calls[0].query != stmts[0] {
		t.Errorf("call 0 mismatch: got %s, want %s", calls[0].query, stmts[0])
	}
	if calls[1].query != stmts[1] {
		t.Errorf("call 1 mismatch: got %s, want %s", calls[1].query, stmts[1])
	}

	if begins != 0 || commits != 0 || rollbacks != 0 {
		t.Errorf("expected no transaction lifecycle calls, got begin=%d commit=%d rollback=%d",
			begins, commits, rollbacks)
	}
}

func TestApplyDDL_ContextCancellation(t *testing.T) {
	stmts := []string{
		"CREATE TABLE `first` (`id` BIGINT);",
		"CREATE TABLE `second` (`id` BIGINT);",
		"CREATE TABLE `third` (`id` BIGINT);",
	}

	t.Run("canceled before execution", func(t *testing.T) {
		db, fc := newDDLTestDB(t, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := ApplyDDL(ctx, db, stmts)
		if err == nil {
			t.Fatal("expected error with canceled context, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected errors.Is(err, context.Canceled), got %v", err)
		}

		fc.mu.Lock()
		numCalls := len(fc.execCalls)
		fc.mu.Unlock()
		if numCalls != 0 {
			t.Fatalf("expected 0 Exec calls when context already canceled, got %d", numCalls)
		}
	})

	t.Run("canceled during sequence", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		db, fc := newDDLTestDB(t, func(ctx context.Context, query string) (driver.Result, error) {
			if strings.Contains(query, "first") {
				cancel()
				return driver.ResultNoRows, nil
			}
			return driver.ResultNoRows, nil
		})

		err := ApplyDDL(ctx, db, stmts)
		if err == nil {
			t.Fatal("expected error when context canceled during sequence, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected errors.Is(err, context.Canceled), got %v", err)
		}

		fc.mu.Lock()
		numCalls := len(fc.execCalls)
		fc.mu.Unlock()
		// Only "first" was called before cancel; "second" failed on ctx or was canceled, "third" never called
		if numCalls > 2 {
			t.Fatalf("expected at most 2 Exec calls, got %d", numCalls)
		}
	})
}

func TestApplyDDL_NilDB(t *testing.T) {
	ctx := context.Background()
	err := ApplyDDL(ctx, nil, []string{"CREATE TABLE x (id INT);"})
	if err == nil {
		t.Fatal("expected error with nil db, got nil")
	}
}

func TestPrepareInfraSQL(t *testing.T) {
	seeds1 := PrepareInfraSQL()
	if len(seeds1) != 2 {
		t.Fatalf("expected exactly 2 seed statements, got %d", len(seeds1))
	}

	want0 := "INSERT IGNORE INTO `locks` (`name`) VALUES ('business_relations')"
	want1 := "INSERT IGNORE INTO `schema_meta` (`key`, `value`, `updated_at`) VALUES ('schema_version', 0, NOW(3))"

	if seeds1[0] != want0 {
		t.Errorf("seed[0] mismatch:\ngot:  %s\nwant: %s", seeds1[0], want0)
	}
	if seeds1[1] != want1 {
		t.Errorf("seed[1] mismatch:\ngot:  %s\nwant: %s", seeds1[1], want1)
	}

	// Verify idempotence keywords
	for i, s := range seeds1 {
		if !strings.HasPrefix(s, "INSERT IGNORE INTO") {
			t.Errorf("seed[%d] should start with INSERT IGNORE INTO, got %s", i, s)
		}
		if strings.Contains(strings.ToUpper(s), "CREATE DATABASE") {
			t.Errorf("seed[%d] must not contain CREATE DATABASE", i)
		}
	}

	// Verify fresh independent slice allocation
	seeds1[0] = "MUTATED"
	seeds2 := PrepareInfraSQL()
	if seeds2[0] != want0 {
		t.Errorf("PrepareInfraSQL must return a fresh slice; mutating prior return affected subsequent call: got %s", seeds2[0])
	}
}

func TestIsRealTiDB(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
	}{
		{name: "TiDB v8.5.8 banner", version: "8.0.11-TiDB-v8.5.8", want: true},
		{name: "TiDB v6.5.0 banner", version: "5.7.25-TiDB-v6.5.0", want: true},
		{name: "TiDB v8.1.0 banner", version: "8.0.18-TiDB-v8.1.0", want: true},
		{name: "TiDB minimal substring", version: "TiDB", want: true},
		{name: "MySQL 5.7 banner", version: "5.7.44-log", want: false},
		{name: "MySQL 8.0 banner", version: "8.0.35", want: false},
		{name: "MySQL explicit name", version: "MySQL 8.0.28", want: false},
		{name: "SQLite banner", version: "3.39.0", want: false},
		{name: "SQLite explicit name", version: "SQLite 3.39.0", want: false},
		{name: "PostgreSQL banner", version: "PostgreSQL 14.2", want: false},
		{name: "MariaDB banner", version: "MariaDB 10.5", want: false},
		{name: "empty string", version: "", want: false},
		{name: "whitespace only", version: "   ", want: false},
		{name: "lowercase tidb", version: "tidb-server", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsRealTiDB(tc.version)
			if got != tc.want {
				t.Errorf("IsRealTiDB(%q) = %v, want %v", tc.version, got, tc.want)
			}
		})
	}
}
