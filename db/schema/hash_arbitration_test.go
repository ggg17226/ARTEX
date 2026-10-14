package schema

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeDriver 实现最小 database/sql/driver.Driver 与 database/sql/driver.DriverContext，
// 供 sql.OpenDB 使用，避免全局 sql.Register 命名冲突。
type fakeDriver struct {
	connector *fakeConnector
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("Open not implemented, use OpenDB connector")
}

type fakeConnector struct {
	driver *fakeDriver
	conn   *fakeConn
}

func (c *fakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.conn, nil
}

func (c *fakeConnector) Driver() driver.Driver {
	return c.driver
}

type queryCall struct {
	query string
	args  []driver.NamedValue
}

type fakeConn struct {
	mu           sync.Mutex
	queries      []queryCall
	inTx         bool
	beginCount   int
	commitCount  int
	rollbackCnt  int
	queryHandler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *fakeConn) Close() error {
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTx {
		return nil, errors.New("already in transaction")
	}
	c.inTx = true
	c.beginCount++
	return &fakeTx{conn: c}, nil
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, queryCall{query: query, args: args})
	if c.queryHandler != nil {
		return c.queryHandler(ctx, query, args)
	}
	return nil, errors.New("unhandled query")
}

type fakeTx struct {
	conn *fakeConn
}

func (t *fakeTx) Commit() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	if !t.conn.inTx {
		return errors.New("commit outside tx")
	}
	t.conn.inTx = false
	t.conn.commitCount++
	return nil
}

func (t *fakeTx) Rollback() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	if !t.conn.inTx {
		return errors.New("rollback outside tx")
	}
	t.conn.inTx = false
	t.conn.rollbackCnt++
	return nil
}

// fakeRows 模拟返回的一行或多行结果
type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
}

func (r *fakeRows) Columns() []string {
	return r.columns
}

func (r *fakeRows) Close() error {
	return nil
}

func (r *fakeRows) Next(dest []driver.Value) error {
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

// helper 创建测试使用的 *sql.DB 与测试 fakeConn
func newTestDB(t *testing.T, handler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)) (*sql.DB, *fakeConn) {
	t.Helper()
	fc := &fakeConn{queryHandler: handler}
	drv := &fakeDriver{}
	connector := &fakeConnector{driver: drv, conn: fc}
	drv.connector = connector
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, fc
}

// 1. 测试 26 四元组白名单精确性、完整性及对 AllTables 的一致性
func TestHashArbitrationWhitelist26Exact(t *testing.T) {
	wantTuples := []HashArbTuple{
		{Table: "companies", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
		{Table: "task_categories", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
		{Table: "task_templates", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
		{Table: "agents", PKCol: "id", IdentCol: "key_identity", HashCol: "key_hash"},
		{Table: "mcp_servers", PKCol: "id", IdentCol: "name_identity", HashCol: "name_hash"},
		{Table: "settings", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
		{Table: "tools", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
		{Table: "scheduler_state", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
		{Table: "agent_prompt_vars", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "mcp_tools_cache", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "agent_visibility", PKCol: "row_id", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "agent_skill_visibility", PKCol: "row_id", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "task_scope", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "side_question_requests", PKCol: "ordinal", IdentCol: "identity", HashCol: "identity_hash"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_root_domain", HashCol: "hash_root_domain"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_ip", HashCol: "hash_ip"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_subdomain", HashCol: "hash_subdomain"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_app_bundle", HashCol: "hash_app_bundle"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_app_name", HashCol: "hash_app_name"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_service_http", HashCol: "hash_service_http"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_service_other", HashCol: "hash_service_other"},
		{Table: "assets", PKCol: "id", IdentCol: "identity_endpoint", HashCol: "hash_endpoint"},
		{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_domain", HashCol: "hash_scope_domain"},
		{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_net", HashCol: "hash_scope_net"},
		{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_value", HashCol: "hash_scope_value"},
		{Table: "llm_profiles", PKCol: "id", IdentCol: "name_identity", HashCol: "name_hash"},
	}

	if len(wantTuples) != 26 {
		t.Fatalf("want exactly 26 tuples, got %d", len(wantTuples))
	}

	// 检查导出白名单切片/函数 ExactTuples
	allWhitelist := HashArbitrationWhitelist()
	if len(allWhitelist) != 26 {
		t.Fatalf("HashArbitrationWhitelist() length = %d, want 26", len(allWhitelist))
	}

	wantSet := make(map[HashArbTuple]bool, 26)
	for _, tup := range wantTuples {
		wantSet[tup] = true
	}
	gotSet := make(map[HashArbTuple]bool, len(allWhitelist))
	for _, tup := range allWhitelist {
		gotSet[tup] = true
	}
	if !reflect.DeepEqual(gotSet, wantSet) {
		t.Errorf("HashArbitrationWhitelist set mismatch:\ngot:  %+v\nwant: %+v", gotSet, wantSet)
	}

	// 逐项对当前 AllTables 验证 PK / identity / hash 与唯一索引
	allTbls := AllTables()
	tblMap := make(map[string]*Table, len(allTbls))
	for _, tbl := range allTbls {
		tblMap[tbl.Name] = tbl
	}

	for _, tup := range wantTuples {
		tbl, ok := tblMap[tup.Table]
		if !ok {
			t.Errorf("tuple table %q not found in AllTables()", tup.Table)
			continue
		}
		if tbl.PKColumn != tup.PKCol {
			t.Errorf("table %q PK column mismatch: got %q, want %q", tup.Table, tbl.PKColumn, tup.PKCol)
		}
		identCol := findColumn(tbl, tup.IdentCol)
		if identCol == nil {
			t.Errorf("table %q: identity column %q missing", tup.Table, tup.IdentCol)
		} else {
			if identCol.Type != TMediumBlob {
				t.Errorf("table %q: identity col %q type = %v, want TMediumBlob", tup.Table, tup.IdentCol, identCol.Type)
			}
		}
		hashCol := findColumn(tbl, tup.HashCol)
		if hashCol == nil {
			t.Errorf("table %q: hash column %q missing", tup.Table, tup.HashCol)
		} else {
			if hashCol.Type != TBinary || hashCol.Length != 32 {
				t.Errorf("table %q: hash col %q type = %v (len %d), want binary(32)", tup.Table, tup.HashCol, hashCol.Type, hashCol.Length)
			}
		}
		// 验证 hash 列存在唯一索引
		var hasUQ bool
		for _, ix := range tbl.Indexes {
			if ix.Unique && len(ix.Parts) == 1 && ix.Parts[0].Column == tup.HashCol && ix.Parts[0].Prefix == 0 {
				hasUQ = true
				break
			}
		}
		if !hasUQ {
			t.Errorf("table %q: missing single-column full UNIQUE index on %q", tup.Table, tup.HashCol)
		}
	}
}

// 2. 拒绝未在白名单的四元组（非法表、跨表混配、SQL注入、反引号注入、不存在列），且 0 次 query
func TestVerifyHashArbitration_UnwhitelistedTuples_ZeroQueries(t *testing.T) {
	db, fc := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		t.Fatalf("database query was called unexpectedly: %s", query)
		return nil, nil
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var dummyHash [32]byte
	dummyExpected := []byte("expected-payload")

	badTuples := []struct {
		table, pk, ident, hash string
		reason                 string
	}{
		{"non_existent_table", "id", "identity", "identity_hash", "non-existent table"},
		{"companies", "row_id", "nkey_identity", "nkey_hash", "wrong PK col for companies"},
		{"companies", "id", "wrong_ident", "nkey_hash", "wrong ident col for companies"},
		{"companies", "id", "nkey_identity", "wrong_hash", "wrong hash col for companies"},
		{"companies", "id", "identity", "identity_hash", "cross-table mixed columns"},
		{"assets", "id", "identity", "identity_hash", "assets does not have bare identity/identity_hash"},
		{"llm_profiles", "id", "identity", "identity_hash", "llm_profiles has name_identity/name_hash"},
		{"companies; DROP TABLE companies;--", "id", "nkey_identity", "nkey_hash", "SQL injection in table"},
		{"companies`", "id", "nkey_identity", "nkey_hash", "backtick in table"},
		{"companies", "id`", "nkey_identity", "nkey_hash", "backtick in pk"},
		{"companies", "id", "nkey_identity`", "nkey_hash", "backtick in ident"},
		{"companies", "id", "nkey_identity", "nkey_hash`", "backtick in hash"},
	}

	for _, tc := range badTuples {
		t.Run(tc.reason, func(t *testing.T) {
			pk, err := VerifyHashArbitration(ctx, tx, tc.table, tc.pk, tc.ident, tc.hash, dummyHash, dummyExpected)
			if err == nil {
				t.Fatalf("expected error for unwhitelisted tuple (%s,%s,%s,%s), got nil pk=%d", tc.table, tc.pk, tc.ident, tc.hash, pk)
			}
			if pk != 0 {
				t.Fatalf("expected pk=0 on error, got %d", pk)
			}
		})
	}

	fc.mu.Lock()
	qCount := len(fc.queries)
	fc.mu.Unlock()
	if qCount != 0 {
		t.Fatalf("expected 0 queries executed for unwhitelisted tuples, got %d", qCount)
	}
}

// 3. 测试同事务 FOR UPDATE SQL 形态、参数绑定、仅 1 次查询、无内部 commit/rollback、payload 相等返回 PK
func TestVerifyHashArbitration_ExactMatchAndTxLifecycle(t *testing.T) {
	expectedPayload := []byte("canonical-identity-payload-for-test")
	hash := CanonicalHash(expectedPayload)
	expectedPK := uint64(123456789)

	var queryExecuted atomic.Int32
	db, fc := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		queryExecuted.Add(1)
		wantSQL := "SELECT `id`, `nkey_identity` FROM `companies` WHERE `nkey_hash` = ? FOR UPDATE"
		if query != wantSQL {
			return nil, fmt.Errorf("unexpected query: got %q, want %q", query, wantSQL)
		}
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 query arg, got %d", len(args))
		}
		// 校验 hash 为单个 32 bytes 绑定参数
		argBytes, ok := args[0].Value.([]byte)
		if !ok || len(argBytes) != 32 || !bytes.Equal(argBytes, hash[:]) {
			return nil, fmt.Errorf("expected 32-byte hash arg matching input, got %v", args[0].Value)
		}

		return &fakeRows{
			columns: []string{"id", "nkey_identity"},
			rows: [][]driver.Value{
				{expectedPK, expectedPayload},
			},
		}, nil
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	// 此时连接处于事务中
	fc.mu.Lock()
	if !fc.inTx {
		t.Fatalf("expected inTx to be true")
	}
	fc.mu.Unlock()

	pk, err := VerifyHashArbitration(ctx, tx, "companies", "id", "nkey_identity", "nkey_hash", hash, expectedPayload)
	if err != nil {
		t.Fatalf("VerifyHashArbitration failed: %v", err)
	}
	if pk != expectedPK {
		t.Fatalf("got PK %d, want %d", pk, expectedPK)
	}

	if queryExecuted.Load() != 1 {
		t.Fatalf("expected exactly 1 query, got %d", queryExecuted.Load())
	}

	// 验证没有内部 commit 或 rollback，外层事务仍然存活
	fc.mu.Lock()
	if !fc.inTx {
		t.Errorf("inTx became false; helper must not commit or rollback caller's tx")
	}
	if fc.commitCount != 0 {
		t.Errorf("commitCount = %d; helper must not commit caller's tx", fc.commitCount)
	}
	if fc.rollbackCnt != 0 {
		t.Errorf("rollbackCnt = %d; helper must not rollback caller's tx", fc.rollbackCnt)
	}
	fc.mu.Unlock()

	// 由调用方负责提交
	if err := tx.Commit(); err != nil {
		t.Fatalf("outer tx.Commit() failed: %v", err)
	}
}

// 4. 测试 Hash 相同但 Payload 不同时返回 ErrIdentityHashCollision，PK=0，外层事务仍存活
func TestVerifyHashArbitration_CollisionReturnsErrIdentityHashCollision(t *testing.T) {
	expectedPayload := []byte("canonical-expected-payload")
	actualPayloadInDB := []byte("canonical-different-payload-colliding-hash")
	hash := CanonicalHash(expectedPayload) // 模拟相同 hash 命中数据库行

	db, fc := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		return &fakeRows{
			columns: []string{"id", "nkey_identity"},
			rows: [][]driver.Value{
				{uint64(999), actualPayloadInDB},
			},
		}, nil
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	pk, err := VerifyHashArbitration(ctx, tx, "companies", "id", "nkey_identity", "nkey_hash", hash, expectedPayload)
	if err == nil {
		t.Fatalf("expected ErrIdentityHashCollision, got nil error (pk=%d)", pk)
	}
	if !errors.Is(err, ErrIdentityHashCollision) {
		t.Fatalf("expected errors.Is(err, ErrIdentityHashCollision), got: %v", err)
	}
	if pk != 0 {
		t.Fatalf("expected pk=0 on collision, got %d", pk)
	}

	// 外层事务不被影响
	fc.mu.Lock()
	if !fc.inTx {
		t.Errorf("inTx became false on collision; helper must not touch tx lifecycle")
	}
	fc.mu.Unlock()
}

// 5. LLM 专例：使用 4A LLMProfileNameIdentity 与 CanonicalHash，真实 name_identity 列
func TestVerifyHashArbitration_LLMProfileNameExactAndCollision(t *testing.T) {
	name := "gpt-4o-2026-production"
	expectedPayload := LLMProfileNameIdentity(name)
	hash := CanonicalHash(expectedPayload)
	expectedPK := uint64(42)

	// Case 5a: exact match
	t.Run("LLM exact match", func(t *testing.T) {
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			wantSQL := "SELECT `id`, `name_identity` FROM `llm_profiles` WHERE `name_hash` = ? FOR UPDATE"
			if query != wantSQL {
				return nil, fmt.Errorf("unexpected query: got %q, want %q", query, wantSQL)
			}
			return &fakeRows{
				columns: []string{"id", "name_identity"},
				rows: [][]driver.Value{
					{expectedPK, expectedPayload},
				},
			}, nil
		})

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "llm_profiles", "id", "name_identity", "name_hash", hash, expectedPayload)
		if err != nil {
			t.Fatalf("LLM match failed: %v", err)
		}
		if pk != expectedPK {
			t.Fatalf("got pk %d, want %d", pk, expectedPK)
		}
	})

	// Case 5b: 篡改 1 字节 payload 导致碰撞返回 ErrIdentityHashCollision
	t.Run("LLM 1 byte modified collision", func(t *testing.T) {
		corruptedPayload := make([]byte, len(expectedPayload))
		copy(corruptedPayload, expectedPayload)
		corruptedPayload[len(corruptedPayload)-1] ^= 0x01 // 翻转最后 1 bit

		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return &fakeRows{
				columns: []string{"id", "name_identity"},
				rows: [][]driver.Value{
					{expectedPK, corruptedPayload},
				},
			}, nil
		})

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "llm_profiles", "id", "name_identity", "name_hash", hash, expectedPayload)
		if !errors.Is(err, ErrIdentityHashCollision) {
			t.Fatalf("expected ErrIdentityHashCollision on 1-byte diff, got %v", err)
		}
		if pk != 0 {
			t.Fatalf("expected pk=0, got %d", pk)
		}
	})

	// Case 5c: rawname 字节串传入 expected 而非 LLMProfileNameIdentity，即便数据库存了 payload 也必须无法通过
	t.Run("raw name passed as expected fails collision", func(t *testing.T) {
		rawNameBytes := []byte(name) // raw text, NOT canonical payload
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return &fakeRows{
				columns: []string{"id", "name_identity"},
				rows: [][]driver.Value{
					{expectedPK, expectedPayload}, // DB holds canonical payload
				},
			}, nil
		})

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "llm_profiles", "id", "name_identity", "name_hash", hash, rawNameBytes)
		if !errors.Is(err, ErrIdentityHashCollision) {
			t.Fatalf("expected ErrIdentityHashCollision when rawNameBytes is passed, got %v", err)
		}
		if pk != 0 {
			t.Fatalf("expected pk=0, got %d", pk)
		}
	})
}

// 6. 无行 (sql.ErrNoRows)、driver 查询错误、扫描类型错误、ctx 取消均显式返回非 nil 错误且 PK=0
func TestVerifyHashArbitration_ErrorsAndEdgeCases(t *testing.T) {
	var dummyHash [32]byte
	payload := []byte("sample-payload")

	// 6a. 无行 (sql.ErrNoRows) 必须报错，不能当 nil 处理
	t.Run("ErrNoRows returns explicit error and pk=0", func(t *testing.T) {
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return &fakeRows{
				columns: []string{"id", "identity"},
				rows:    [][]driver.Value{}, // 0 rows
			}, nil
		})

		ctx := context.Background()
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "task_scope", "id", "identity", "identity_hash", dummyHash, payload)
		if err == nil {
			t.Fatalf("expected error on ErrNoRows, got nil (pk=%d)", pk)
		}
		if pk != 0 {
			t.Fatalf("expected pk=0 on ErrNoRows, got %d", pk)
		}
	})

	// 6b. Driver 查询错误保留原始错误或在链中
	t.Run("driver query error propagated", func(t *testing.T) {
		driverErr := errors.New("underlying driver connection closed")
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return nil, driverErr
		})

		ctx := context.Background()
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "task_scope", "id", "identity", "identity_hash", dummyHash, payload)
		if !errors.Is(err, driverErr) {
			t.Fatalf("expected errors.Is(err, driverErr), got %v", err)
		}
		if pk != 0 {
			t.Fatalf("expected pk=0 on driver error, got %d", pk)
		}
	})

	// 6c. 扫描类型转换失败（如 PK 返回无法转成 uint64 的字符串）
	t.Run("scan conversion error", func(t *testing.T) {
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return &fakeRows{
				columns: []string{"id", "identity"},
				rows: [][]driver.Value{
					{"not-a-number", payload},
				},
			}, nil
		})

		ctx := context.Background()
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()

		pk, err := VerifyHashArbitration(ctx, tx, "task_scope", "id", "identity", "identity_hash", dummyHash, payload)
		if err == nil {
			t.Fatalf("expected scan conversion error, got nil")
		}
		if pk != 0 {
			t.Fatalf("expected pk=0 on scan error, got %d", pk)
		}
	})

	// 6d. Context 取消
	t.Run("context cancelled returns error", func(t *testing.T) {
		db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			return nil, ctx.Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		tx, _ := db.BeginTx(ctx, nil)
		if tx != nil {
			defer func() { _ = tx.Rollback() }()
		}

		pk, err := VerifyHashArbitration(ctx, tx, "task_scope", "id", "identity", "identity_hash", dummyHash, payload)
		if err == nil {
			t.Fatalf("expected error on cancelled ctx, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected errors.Is(err, context.Canceled), got %v", err)
		}
		if pk != 0 {
			t.Fatalf("expected pk=0, got %d", pk)
		}
	})
}

// 7. PK 边界值：普通 PK 表与隐藏 row_id 表均支持 MaxUint64，且支持 decimal string / []byte / uint64，无 int64 符号截断
func TestVerifyHashArbitration_MaxUint64AndPKTypes(t *testing.T) {
	payload := []byte("max-uint64-test-payload")
	hash := CanonicalHash(payload)
	maxU64 := uint64(math.MaxUint64)
	maxU64Str := strconv.FormatUint(maxU64, 10)

	cases := []struct {
		name      string
		table     string
		pkCol     string
		identCol  string
		hashCol   string
		driverVal driver.Value
	}{
		{
			name:      "companies id (MaxUint64 as decimal string)",
			table:     "companies",
			pkCol:     "id",
			identCol:  "nkey_identity",
			hashCol:   "nkey_hash",
			driverVal: maxU64Str,
		},
		{
			name:      "companies id (MaxUint64 as []byte)",
			table:     "companies",
			pkCol:     "id",
			identCol:  "nkey_identity",
			hashCol:   "nkey_hash",
			driverVal: []byte(maxU64Str),
		},
		{
			name:      "companies id (MaxUint64 as uint64)",
			table:     "companies",
			pkCol:     "id",
			identCol:  "nkey_identity",
			hashCol:   "nkey_hash",
			driverVal: maxU64,
		},
		{
			name:      "settings row_id (hidden PK, MaxUint64 as decimal string)",
			table:     "settings",
			pkCol:     "row_id",
			identCol:  "key_identity",
			hashCol:   "key_hash",
			driverVal: maxU64Str,
		},
		{
			name:      "settings row_id (hidden PK, MaxUint64 as []byte)",
			table:     "settings",
			pkCol:     "row_id",
			identCol:  "key_identity",
			hashCol:   "key_hash",
			driverVal: []byte(maxU64Str),
		},
		{
			name:      "side_question_requests ordinal (MaxUint64 as decimal string)",
			table:     "side_question_requests",
			pkCol:     "ordinal",
			identCol:  "identity",
			hashCol:   "identity_hash",
			driverVal: maxU64Str,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := newTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				return &fakeRows{
					columns: []string{tc.pkCol, tc.identCol},
					rows: [][]driver.Value{
						{tc.driverVal, payload},
					},
				}, nil
			})

			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			pk, err := VerifyHashArbitration(ctx, tx, tc.table, tc.pkCol, tc.identCol, tc.hashCol, hash, payload)
			if err != nil {
				t.Fatalf("VerifyHashArbitration failed: %v", err)
			}
			if pk != maxU64 {
				t.Fatalf("got pk %d (%x), want %d (%x)", pk, pk, maxU64, maxU64)
			}
		})
	}
}
