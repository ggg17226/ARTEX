package idalloc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"testing"
)

// ---- driver 级 fake：让 *sql.Tx 可离线驱动 ----

type fakeStmt struct {
	conn    *fakeConn
	sqlText string
	args    []any
}

func (s *fakeStmt) Close() error { return nil }
func (s *fakeStmt) NumInput() int {
	return -1 // 不校验参数个数
}
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.lastExec = s.sqlText
	s.conn.execCount++
	return driver.RowsAffected(1), nil
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.rowsFor(s.sqlText, args)
}

type fakeRows struct {
	cols   []string
	values []driver.Value
	done   bool
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

type fakeConn struct {
	driver.Conn
	lastExec  string
	execCount int
	lastID    uint64
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, sqlText: query}, nil
}

// rowsFor 模拟数据库对 SELECT 的应答：LAST_INSERT_ID() 返回 lastID；
// 其余 SELECT 返回单行（identity 回读用）。
func (c *fakeConn) rowsFor(query string, args []driver.Value) (driver.Rows, error) {
	switch {
	case query == "SELECT LAST_INSERT_ID()":
		return &fakeRows{cols: []string{"LAST_INSERT_ID()"}, values: []driver.Value{c.lastID}, done: false}, nil
	default:
		if len(args) < 1 {
			return nil, fmt.Errorf("identity query needs at least one arg")
		}
		var val driver.Value
		if s, ok := args[0].(string); ok && s == "k1" {
			val = uint64(math.MaxUint64)
		} else {
			val = args[0]
		}
		return &fakeRows{cols: []string{"id"}, values: []driver.Value{val}, done: false}, nil
	}
}

func (c *fakeConn) Begin() (driver.Tx, error) { return c, nil }

func (c *fakeConn) Commit() error { return nil }

func (c *fakeConn) Rollback() error { return nil }

func (c *fakeConn) Close() error { return nil }

var driverSeq uint64

func newFakeDB(t *testing.T) (*sql.DB, *fakeConn) {
	t.Helper()
	c := &fakeConn{}
	d := &fakeDriver{conn: c}
	driverName := fmt.Sprintf("idalloc_fake_%s_%d", t.Name(), atomic.AddUint64(&driverSeq, 1))
	sql.Register(driverName, d)
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, c
}

type fakeDriver struct{ conn *fakeConn }

func (d *fakeDriver) Open(string) (driver.Conn, error) { return d.conn, nil }

// ---- 测试 ----

func TestValidateUint64(t *testing.T) {
	cases := []struct {
		id     uint64
		wantOK bool
	}{
		{1, true},
		{math.MaxInt64, true},
		{0, false},
		{math.MaxInt64 + 1, false},
		{9223372036854775813, false}, // §11.1：>MaxInt64 高值
		{math.MaxUint64, false},      // 兼容域规则下不合法（显式 roundtrip 走 identity 路径）
	}
	for _, tc := range cases {
		err := ValidateUint64(tc.id)
		if tc.wantOK && err != nil {
			t.Errorf("ValidateUint64(%d) = %v, want nil", tc.id, err)
		}
		if !tc.wantOK && !errors.Is(err, ErrOutOfSafeRange) {
			t.Errorf("ValidateUint64(%d) = %v, want ErrOutOfSafeRange", tc.id, err)
		}
	}
}

func TestAllocID_HappyPath(t *testing.T) {
	db, conn := newFakeDB(t)
	conn.lastID = 42
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	// 模拟调用方已执行单行 INSERT（RowsAffected==1）
	if _, err := tx.ExecContext(context.Background(), "INSERT INTO t (name) VALUES (?)", "a"); err != nil {
		t.Fatalf("exec insert: %v", err)
	}
	id, err := AllocID(context.Background(), tx)
	if err != nil {
		t.Fatalf("AllocID: %v", err)
	}
	if id != 42 {
		t.Fatalf("AllocID = %d, want 42", id)
	}
}

func TestAllocID_ZeroReadback(t *testing.T) {
	// §11.1：插入 >MaxInt64 高值后 LAST_INSERT_ID()=0 → 必须显式失败，不 fallback
	db, conn := newFakeDB(t)
	conn.lastID = 0
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	_, err = AllocID(context.Background(), tx)
	if !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID = %v, want ErrReadbackZero", err)
	}
}

func TestAllocID_OutOfDomain(t *testing.T) {
	// §11.1：>MaxInt64 高值即使回读成功也越域 → ErrOutOfSafeRange
	db, conn := newFakeDB(t)
	conn.lastID = 9223372036854775813
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	_, err = AllocID(context.Background(), tx)
	if !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocID = %v, want ErrOutOfSafeRange", err)
	}
}

func TestAllocIDByIdentity_HappyPath(t *testing.T) {
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	id, err := AllocIDByIdentity(context.Background(), tx,
		"SELECT id FROM t WHERE logical_key = ?", uint64(12345))
	if err != nil {
		t.Fatalf("AllocIDByIdentity: %v", err)
	}
	if id != 12345 {
		t.Fatalf("AllocIDByIdentity = %d, want 12345", id)
	}
}

func TestAllocPhysicalRowIDByIdentity_HighValue(t *testing.T) {
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	id, err := AllocPhysicalRowIDByIdentity(context.Background(), tx,
		"SELECT id FROM t WHERE logical_key = ?", "k1")
	if err != nil {
		t.Fatalf("AllocPhysicalRowIDByIdentity: %v", err)
	}
	if id != math.MaxUint64 {
		t.Fatalf("AllocPhysicalRowIDByIdentity = %d, want MaxUint64", id)
	}
}

func TestAllocPhysicalRowIDByIdentity_Zero(t *testing.T) {
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	_, err = AllocPhysicalRowIDByIdentity(context.Background(), tx,
		"SELECT id FROM t WHERE k = ?", uint64(0))
	if !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocPhysicalRowIDByIdentity = %v, want ErrReadbackZero", err)
	}
}


func TestAllocIDByIdentity_Zero(t *testing.T) {
	// 回读到 0 → 显式失败（不返回 0 给调用方）
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	_, err = AllocIDByIdentity(context.Background(), tx, "SELECT id FROM t WHERE k = ?", uint64(0))
	if !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocIDByIdentity = %v, want ErrReadbackZero", err)
	}
}

func TestAllocIDByIdentity_OutOfSafeRange(t *testing.T) {
	// §2.4.1：公开业务 ID 回读必须落在 1..MaxInt64 兼容域；>MaxInt64 返回 ErrOutOfSafeRange
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	_, err = AllocIDByIdentity(context.Background(), tx,
		"SELECT id FROM t WHERE logical_key = ?", "k1")
	if !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocIDByIdentity = %v, want ErrOutOfSafeRange", err)
	}
}

