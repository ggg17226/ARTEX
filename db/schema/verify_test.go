package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// ---- 离线测试 fake 驱动与数据结构 ----

type verifyFakeRows struct {
	mu       sync.Mutex
	cols     []string
	data     [][]any
	pos      int
	closed   bool
	errVal   error // 供 Err() 返回的错误
	closeErr error // 供 Close() 返回的错误
	scanErr  error // 供 Scan() 返回的错误
}

func newVerifyFakeRows(cols []string, data [][]any) *verifyFakeRows {
	return &verifyFakeRows{
		cols: cols,
		data: data,
		pos:  0,
	}
}

func (r *verifyFakeRows) Columns() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cols, nil
}

func (r *verifyFakeRows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.pos < len(r.data) {
		r.pos++
		return true
	}
	return false
}

func (r *verifyFakeRows) Scan(dest ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("scan on closed rows")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos == 0 || r.pos > len(r.data) {
		return fmt.Errorf("scan called at invalid cursor position: %d (data len: %d)", r.pos, len(r.data))
	}
	row := r.data[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan dest count mismatch: got %d, row has %d", len(dest), len(row))
	}
	for i, val := range row {
		if err := assignFakeValue(dest[i], val); err != nil {
			return fmt.Errorf("scan col %d: %w", i, err)
		}
	}
	return nil
}

func (r *verifyFakeRows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.closeErr
}

func (r *verifyFakeRows) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errVal
}

func assignFakeValue(dest any, src any) error {
	switch d := dest.(type) {
	case *string:
		switch s := src.(type) {
		case string:
			*d = s
			return nil
		case []byte:
			*d = string(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *string", src)
		}
	case *int:
		switch s := src.(type) {
		case int:
			*d = s
			return nil
		case int64:
			*d = int(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *int", src)
		}
	case *int64:
		switch s := src.(type) {
		case int:
			*d = int64(s)
			return nil
		case int64:
			*d = s
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
	case *uint64:
		switch s := src.(type) {
		case uint64:
			*d = s
			return nil
		case int:
			*d = uint64(s)
			return nil
		case int64:
			*d = uint64(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *uint64", src)
		}
	default:
		return fmt.Errorf("unsupported dest type %T", dest)
	}
}

type verifyQueryCall struct {
	query string
	args  []any
}

type verifyExecCall struct {
	query string
	args  []any
}

type verifyFakeQuerier struct {
	mu           sync.Mutex
	queries      []verifyQueryCall
	execs        []verifyExecCall
	queryHandler func(ctx context.Context, query string, args ...any) (Rows, error)
	execHandler  func(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (q *verifyFakeQuerier) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	q.mu.Lock()
	q.queries = append(q.queries, verifyQueryCall{query: query, args: args})
	handler := q.queryHandler
	q.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args...)
	}
	return nil, fmt.Errorf("verifyFakeQuerier: unexpected query %q", query)
}

func (q *verifyFakeQuerier) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	q.mu.Lock()
	q.execs = append(q.execs, verifyExecCall{query: query, args: args})
	handler := q.execHandler
	q.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args...)
	}
	return nil, fmt.Errorf("verifyFakeQuerier: unexpected exec %q", query)
}

// ---- CheckPrimaryKeys 单元测试 ----

func TestCheckPrimaryKeys_Success(t *testing.T) {
	ctx := context.Background()

	// 覆盖四种不同主键列名的表：id, ordinal, profile_id, row_id
	tables := []*Table{
		{Name: "companies", PKColumn: "id"},
		{Name: "side_question_requests", PKColumn: "ordinal"},
		{Name: "llm_profiles", PKColumn: "profile_id"},
		{Name: "settings", PKColumn: "row_id"},
	}

	var createdRows []*verifyFakeRows
	var activeOpenRows int

	fq := &verifyFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if query != qPKShape {
				return nil, fmt.Errorf("unexpected query: %s", query)
			}
			if len(args) != 1 {
				return nil, fmt.Errorf("expected 1 arg (table name), got %d", len(args))
			}
			tableName, ok := args[0].(string)
			if !ok {
				return nil, fmt.Errorf("arg[0] not string: %T", args[0])
			}

			// 在查第二张表时，前一张表的 rows 必须已经被 Close，不能 defer 到末尾
			if activeOpenRows > 0 {
				return nil, fmt.Errorf("prior rows not closed before starting query for %s; active rows: %d", tableName, activeOpenRows)
			}

			var pkCol string
			switch tableName {
			case "companies":
				pkCol = "id"
			case "side_question_requests":
				pkCol = "ordinal"
			case "llm_profiles":
				pkCol = "profile_id"
			case "settings":
				pkCol = "row_id"
			default:
				return nil, fmt.Errorf("unknown table in test: %s", tableName)
			}

			activeOpenRows++
			rows := newVerifyFakeRows(
				[]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
				[][]any{
					{pkCol, "bigint", "bigint unsigned"},
				},
			)
			origClose := rows.closeErr
			_ = origClose
			rows.closeErr = nil

			// 包装一下以监控 activeOpenRows
			tracked := &verifyTrackedRows{
				verifyFakeRows: rows,
				onClose: func() {
					activeOpenRows--
				},
			}
			createdRows = append(createdRows, rows)
			return tracked, nil
		},
	}

	err := CheckPrimaryKeys(ctx, fq, tables)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(createdRows) != 4 {
		t.Fatalf("expected 4 queries, got %d", len(createdRows))
	}
	for i, r := range createdRows {
		if !r.closed {
			t.Errorf("rows[%d] was not closed", i)
		}
	}
	if activeOpenRows != 0 {
		t.Errorf("activeOpenRows != 0 at end: %d", activeOpenRows)
	}
}

type verifyTrackedRows struct {
	*verifyFakeRows
	onClose func()
}

func (tr *verifyTrackedRows) Close() error {
	err := tr.verifyFakeRows.Close()
	if tr.onClose != nil {
		tr.onClose()
		tr.onClose = nil
	}
	return err
}

func TestCheckPrimaryKeys_NegativeCases(t *testing.T) {
	ctx := context.Background()
	queryErrSentinel := errors.New("db disconnect sentinel")

	tests := []struct {
		name      string
		table     *Table
		rowsData  [][]any
		queryErr  error
		scanErr   error
		rowsErr   error
		closeErr  error
		wantErr   string
		isErrWrap error
	}{
		{
			name:      "query_fails",
			table:     &Table{Name: "t1", PKColumn: "id"},
			queryErr:  queryErrSentinel,
			wantErr:   "table t1: db disconnect sentinel",
			isErrWrap: queryErrSentinel,
		},
		{
			name:     "no_pk_row_zero_pk",
			table:    &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{},
			wantErr:  "table t1: 0 PK columns [], want exactly 1",
		},
		{
			name:  "two_pk_rows",
			table: &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{
				{"id", "bigint", "bigint unsigned"},
				{"sub_id", "bigint", "bigint unsigned"},
			},
			wantErr: "table t1: 2 PK columns [id sub_id], want exactly 1",
		},
		{
			name:  "wrong_pk_column_name",
			table: &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{
				{"wrong_id", "bigint", "bigint unsigned"},
			},
			wantErr: "table t1: PK column wrong_id, want id",
		},
		{
			name:  "data_type_not_bigint",
			table: &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{
				{"id", "int", "int unsigned"},
			},
			wantErr: "table t1: PK column id DATA_TYPE=int, want bigint",
		},
		{
			name:  "column_type_not_unsigned",
			table: &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{
				{"id", "bigint", "bigint"},
			},
			wantErr: "table t1: PK column id COLUMN_TYPE=bigint, want unsigned",
		},
		{
			name:     "scan_error_fails_and_closes",
			table:    &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{{"id", "bigint", "bigint unsigned"}},
			scanErr:  errors.New("scan failure sentinel"),
			wantErr:  "scan failure sentinel",
		},
		{
			name:     "rows_err_propagated",
			table:    &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{{"id", "bigint", "bigint unsigned"}},
			rowsErr:  errors.New("driver iteration error sentinel"),
			wantErr:  "driver iteration error sentinel",
		},
		{
			name:     "close_error_propagated",
			table:    &Table{Name: "t1", PKColumn: "id"},
			rowsData: [][]any{{"id", "bigint", "bigint unsigned"}},
			closeErr: errors.New("close socket error sentinel"),
			wantErr:  "close socket error sentinel",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedRows *verifyFakeRows
			sentinelErr := tc.isErrWrap
			if sentinelErr == nil && tc.queryErr != nil {
				sentinelErr = tc.queryErr
			}
			if sentinelErr == nil && tc.scanErr != nil {
				sentinelErr = tc.scanErr
			}
			if sentinelErr == nil && tc.rowsErr != nil {
				sentinelErr = tc.rowsErr
			}
			if sentinelErr == nil && tc.closeErr != nil {
				sentinelErr = tc.closeErr
			}

			fq := &verifyFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if tc.queryErr != nil {
						return nil, tc.queryErr
					}
					r := newVerifyFakeRows([]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"}, tc.rowsData)
					r.scanErr = tc.scanErr
					r.errVal = tc.rowsErr
					r.closeErr = tc.closeErr
					capturedRows = r
					return r, nil
				},
			}

			err := CheckPrimaryKeys(ctx, fq, []*Table{tc.table})
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}

			if sentinelErr != nil && !errors.Is(err, sentinelErr) {
				t.Errorf("expected errors.Is(err, sentinelErr) to be true for %v, got %v", sentinelErr, err)
			}

			if capturedRows != nil && !capturedRows.closed {
				t.Errorf("rows was not closed on failure path %s", tc.name)
			}
		})
	}
}

func TestCheckPrimaryKeys_DualErrors_PreservesBoth(t *testing.T) {
	ctx := context.Background()
	scanSentinel := errors.New("scan failed")
	closeSentinel := errors.New("close failed")

	var capturedRows *verifyFakeRows
	fq := &verifyFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			r := newVerifyFakeRows([]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"}, [][]any{
				{"id", "bigint", "bigint unsigned"},
			})
			r.scanErr = scanSentinel
			r.closeErr = closeSentinel
			capturedRows = r
			return r, nil
		},
	}

	err := CheckPrimaryKeys(ctx, fq, []*Table{{Name: "t1", PKColumn: "id"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, scanSentinel) {
		t.Errorf("expected errors.Is(err, scanSentinel), got %v", err)
	}
	if !errors.Is(err, closeSentinel) {
		t.Errorf("expected errors.Is(err, closeSentinel), got %v", err)
	}
	if capturedRows != nil && !capturedRows.closed {
		t.Error("rows was not closed")
	}
}

func TestCheckPrimaryKeys_NilInputs(t *testing.T) {
	ctx := context.Background()
	if err := CheckPrimaryKeys(ctx, nil, []*Table{{Name: "t1"}}); err == nil {
		t.Error("expected error for nil Querier, got nil")
	}
	fq := &verifyFakeQuerier{}
	if err := CheckPrimaryKeys(ctx, fq, nil); err != nil {
		t.Errorf("empty/nil tables should return nil, got %v", err)
	}
}

// ---- CheckNoFKNoCheck 单元测试 ----

func TestCheckNoFKNoCheck_Success(t *testing.T) {
	ctx := context.Background()
	var capturedRows *verifyFakeRows

	fq := &verifyFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if query != qFKCheckCount {
				return nil, fmt.Errorf("unexpected query: %s", query)
			}
			r := newVerifyFakeRows([]string{"COUNT(*)"}, [][]any{{0}})
			capturedRows = r
			return r, nil
		},
	}

	if err := CheckNoFKNoCheck(ctx, fq); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedRows == nil || !capturedRows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestCheckNoFKNoCheck_NegativeCases(t *testing.T) {
	ctx := context.Background()
	queryFkSentinel := errors.New("query fk check failed")

	tests := []struct {
		name      string
		rowsData  [][]any
		queryErr  error
		scanErr   error
		rowsErr   error
		closeErr  error
		wantErr   string
		isErrWrap error
	}{
		{
			name:      "query_error",
			queryErr:  queryFkSentinel,
			wantErr:   "query fk check failed",
			isErrWrap: queryFkSentinel,
		},
		{
			name:     "no_rows_returned",
			rowsData: [][]any{},
			wantErr:  "FK/CHECK count: no row",
		},
		{
			name:     "multiple_count_rows",
			rowsData: [][]any{{0}, {0}},
			wantErr:  "FK/CHECK count: multiple rows",
		},
		{
			name:     "non_zero_constraints",
			rowsData: [][]any{{3}},
			wantErr:  "found 3 FOREIGN KEY/CHECK constraints, want 0",
		},
		{
			name:     "scan_error",
			rowsData: [][]any{{0}},
			scanErr:  errors.New("scan failure count"),
			wantErr:  "scan failure count",
		},
		{
			name:     "rows_err_propagated",
			rowsData: [][]any{{0}},
			rowsErr:  errors.New("rows iteration error"),
			wantErr:  "rows iteration error",
		},
		{
			name:     "close_error_propagated",
			rowsData: [][]any{{0}},
			closeErr: errors.New("close error fk count"),
			wantErr:  "close error fk count",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedRows *verifyFakeRows
			sentinelErr := tc.isErrWrap
			if sentinelErr == nil && tc.queryErr != nil {
				sentinelErr = tc.queryErr
			}
			if sentinelErr == nil && tc.scanErr != nil {
				sentinelErr = tc.scanErr
			}
			if sentinelErr == nil && tc.rowsErr != nil {
				sentinelErr = tc.rowsErr
			}
			if sentinelErr == nil && tc.closeErr != nil {
				sentinelErr = tc.closeErr
			}

			fq := &verifyFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if tc.queryErr != nil {
						return nil, tc.queryErr
					}
					r := newVerifyFakeRows([]string{"COUNT(*)"}, tc.rowsData)
					r.scanErr = tc.scanErr
					r.errVal = tc.rowsErr
					r.closeErr = tc.closeErr
					capturedRows = r
					return r, nil
				},
			}

			err := CheckNoFKNoCheck(ctx, fq)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}

			if sentinelErr != nil && !errors.Is(err, sentinelErr) {
				t.Errorf("expected errors.Is(err, sentinelErr) to be true for %v, got %v", sentinelErr, err)
			}

			if capturedRows != nil && !capturedRows.closed {
				t.Errorf("rows was not closed on failure path %s", tc.name)
			}
		})
	}
}

func TestCheckNoFKNoCheck_NilInputs(t *testing.T) {
	ctx := context.Background()
	if err := CheckNoFKNoCheck(ctx, nil); err == nil {
		t.Error("expected error for nil Querier, got nil")
	}
}

// ---- DBQuerier 适配 database/sql 边界测试 ----

type verifySqlDriverConn struct {
	lastQuery string
	lastArgs  []driver.NamedValue
	lastExec  string
	execArgs  []driver.NamedValue
}

func (c *verifySqlDriverConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *verifySqlDriverConn) Close() error {
	return nil
}

func (c *verifySqlDriverConn) Begin() (driver.Tx, error) {
	return nil, errors.New("tx not implemented")
}

func (c *verifySqlDriverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.lastQuery = query
	c.lastArgs = args
	return &verifySqlDriverRows{
		cols: []string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
		rows: [][]driver.Value{
			{"id", "bigint", "bigint unsigned"},
		},
	}, nil
}

func (c *verifySqlDriverConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.lastExec = query
	c.execArgs = args
	return driver.RowsAffected(1), nil
}

type verifySqlDriverRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *verifySqlDriverRows) Columns() []string {
	return r.cols
}

func (r *verifySqlDriverRows) Close() error {
	return nil
}

func (r *verifySqlDriverRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.pos]
	r.pos++
	for i, v := range row {
		dest[i] = v
	}
	return nil
}

type verifySqlConnector struct {
	conn *verifySqlDriverConn
}

func (sc *verifySqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return sc.conn, nil
}

func (sc *verifySqlConnector) Driver() driver.Driver {
	return &verifySqlDummyDriver{}
}

type verifySqlDummyDriver struct{}

func (d *verifySqlDummyDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("Open not implemented")
}

func TestDBQuerier_DatabaseSqlBoundary(t *testing.T) {
	conn := &verifySqlDriverConn{}
	connector := &verifySqlConnector{conn: conn}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
	})

	dq := DBQuerier{DB: db}

	// 1. 测试 Query 传递 ctx 与 args
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "ctx-val")
	rows, err := dq.Query(ctx, "SELECT * FROM t WHERE id = ? AND name = ?", 123, "test_name")
	if err != nil {
		t.Fatalf("DBQuerier.Query failed: %v", err)
	}
	defer rows.Close()

	if conn.lastQuery != "SELECT * FROM t WHERE id = ? AND name = ?" {
		t.Errorf("Query query mismatch: got %q", conn.lastQuery)
	}
	if len(conn.lastArgs) != 2 || conn.lastArgs[0].Value != int64(123) || conn.lastArgs[1].Value != "test_name" {
		t.Errorf("Query args mismatch: got %+v", conn.lastArgs)
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("rows.Columns: %v", err)
	}
	if !reflect.DeepEqual(cols, []string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"}) {
		t.Errorf("Columns mismatch: got %v", cols)
	}

	if !rows.Next() {
		t.Fatal("expected rows.Next() == true")
	}
	var colName, dataType, colType string
	if err := rows.Scan(&colName, &dataType, &colType); err != nil {
		t.Fatalf("rows.Scan: %v", err)
	}
	if colName != "id" || dataType != "bigint" || colType != "bigint unsigned" {
		t.Errorf("scanned values mismatch: %s, %s, %s", colName, dataType, colType)
	}
	if rows.Next() {
		t.Fatal("expected rows.Next() == false after 1 row")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("rows.Err() mismatch: %v", err)
	}

	// 2. 测试 Exec 传递 ctx 与 args
	res, err := dq.Exec(ctx, "UPDATE t SET name = ? WHERE id = ?", "new_name", 456)
	if err != nil {
		t.Fatalf("DBQuerier.Exec failed: %v", err)
	}
	ra, err := res.RowsAffected()
	if err != nil || ra != 1 {
		t.Errorf("RowsAffected got %d, err %v", ra, err)
	}
	if conn.lastExec != "UPDATE t SET name = ? WHERE id = ?" {
		t.Errorf("Exec query mismatch: got %q", conn.lastExec)
	}
	if len(conn.execArgs) != 2 || conn.execArgs[0].Value != "new_name" || conn.execArgs[1].Value != int64(456) {
		t.Errorf("Exec args mismatch: got %+v", conn.execArgs)
	}
}
