package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

// 内存模拟 driver，用于对 database/sql.DB 进行轻量只读事务隔离测试
type mockDriver struct {
	mu           sync.Mutex
	txStarted    int
	txCommitted  int
	txRolledBack int
}

var (
	testMockDriverOnce sync.Once
	globalMockDriver   = &mockDriver{}
)

func init() {
	sql.Register("mock_locator_driver", globalMockDriver)
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{driver: d}, nil
}

type mockConn struct {
	driver *mockDriver
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{}, nil
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *mockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.driver.mu.Lock()
	c.driver.txStarted++
	c.driver.mu.Unlock()
	return &mockDriverTx{conn: c}, nil
}

type mockDriverTx struct {
	conn *mockConn
}

func (t *mockDriverTx) Commit() error {
	t.conn.driver.mu.Lock()
	t.conn.driver.txCommitted++
	t.conn.driver.mu.Unlock()
	return nil
}

func (t *mockDriverTx) Rollback() error {
	t.conn.driver.mu.Lock()
	t.conn.driver.txRolledBack++
	t.conn.driver.mu.Unlock()
	return nil
}

type mockStmt struct{}

func (s *mockStmt) Close() error { return nil }
func (s *mockStmt) NumInput() int { return -1 }
func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &mockRows{}, nil
}

type mockRows struct{}

func (r *mockRows) Columns() []string { return []string{"id"} }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	return errors.New("EOF")
}

func getMockDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mock_locator_driver", "test_dsn")
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	return db
}

func TestExecuteFreshLocator_Success(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		// 返回无序候选集与稳定身份
		return &LocatorSnapshot{
			CandidateKeys: []EntityKey{
				{Table: TableOrderCompanies, PK: 100},
				{Table: TableOrderTasks, PK: 1},
			},
			StableIdentities: map[EntityKey]string{
				{Table: TableOrderTasks, PK: 1}: "hash-v1-task",
			},
		}, nil
	}

	snapshot, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if err != nil {
		t.Fatalf("ExecuteFreshLocator failed: %v", err)
	}

	if len(snapshot.CandidateKeys) != 2 {
		t.Fatalf("expected 2 candidate keys, got %d", len(snapshot.CandidateKeys))
	}
	// 断言候选集被自动严格全序排序：Tasks(1) 先于 Companies(100)
	if snapshot.CandidateKeys[0].Table != TableOrderTasks {
		t.Errorf("expected first key Tasks, got %+v", snapshot.CandidateKeys[0])
	}
	if snapshot.CandidateKeys[1].Table != TableOrderCompanies {
		t.Errorf("expected second key Companies, got %+v", snapshot.CandidateKeys[1])
	}

	if snapshot.StableIdentities[EntityKey{Table: TableOrderTasks, PK: 1}] != "hash-v1-task" {
		t.Errorf("expected stable identity preserved")
	}
}

func TestExecuteFreshLocator_NilGuards(t *testing.T) {
	ctx := context.Background()
	_, err := ExecuteFreshLocator(ctx, nil, func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrNilDatabase) {
		t.Errorf("expected ErrNilDatabase, got %v", err)
	}

	db := getMockDB(t)
	defer db.Close()
	_, err = ExecuteFreshLocator(ctx, db, nil)
	if !errors.Is(err, ErrNilLocatorFunc) {
		t.Errorf("expected ErrNilLocatorFunc, got %v", err)
	}
}

func TestExecuteFreshLocator_CandidateLimitExceeded(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx := context.Background()
	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		keys := make([]EntityKey, MaxLocatorCandidates+1)
		for i := 0; i <= MaxLocatorCandidates; i++ {
			keys[i] = EntityKey{Table: TableOrderTasks, PK: uint64(i + 1)}
		}
		return &LocatorSnapshot{CandidateKeys: keys}, nil
	}

	_, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if !errors.Is(err, ErrCandidateLimitExceeded) {
		t.Errorf("expected ErrCandidateLimitExceeded, got %v", err)
	}
}

func TestExecuteFreshLocator_QueryErrorRollback(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx := context.Background()
	expectedErr := errors.New("simulated locator query error")
	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		return nil, expectedErr
	}

	_, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}
