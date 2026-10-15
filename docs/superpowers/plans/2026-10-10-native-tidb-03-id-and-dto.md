# P03：64 位 ID 分配/回读、uint64 边界与「接口与 JS 统一 string」契约 实施计划

**致执行代理（Required Sub-Skills）：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐项落实。使用 `- [ ]` 复选框跟踪步骤。

**状态门（spec L930–931 原文引用）：** 「在新独立正式 review 完成前，本稿不进入实施计划」「**其后的三项增量（§11.1 实测证据、高值回读修正、接口/JS 统一 string 契约）必须先完成本轮独立正式 review（§10.4-T 已获用户确认），才能进入实施计划**」——本计划即该门之后产出的实施计划；§10.4-T 已获用户确认。Ready ≠ 批准 ≠ 实施 ≠ TiDB 实测通过。

**Goal:** 在既有 TiDB 8.5 LTS 外部 dev 实例与 Go `database/sql` 代码路径上，落实三项增量：① 新建 `server/idalloc` 助手包，提供 64 位 ID 的 `1..MaxInt64` 兼容域守卫、单行 INSERT 后的安全 uint64 回读（同 tx 同物理连接 `SELECT LAST_INSERT_ID()` 扫 `uint64`）与高值/隐藏 `row_id` 的 exact logical identity 回读；② 为 uint64 边界建立完整单测矩阵（`==0`、`>MaxInt64`、`MaxUint64` roundtrip、`INSERT IGNORE` miss / upsert / 多行、allocator 回绕 fail closed）；③ 将全部 64 位实体 ID/ordinal 的 HTTP/WS/JSON 契约统一为 string（后端 DTO 残留 int64 ID 字段 string 化 + 入口 string→uint64 解析逐点处理 + 前端 `Number(id)`/`parseInt` 强转消除），并以真实 TiDB 集成测试（env `ARTEX_TIDB_DSN` 门控，缺失诚实 skip 为「未验证」）完成验收。

**Architecture:**
1. `server/idalloc` 新包（净新增，`github.com/Autumn-27/artex/server/idalloc`）：`Validate`/`Guard` 守卫（`1..MaxInt64`，兼容域规则而非 JS-safe 上限）、`AllocID`（单行 INSERT + RowsAffected=1 → 同 tx 同连接 `SELECT LAST_INSERT_ID()` 扫 `uint64`）、`AllocIDByIdentity`（高值/`row_id`/非 INSERT 路径的 exact logical identity 回读）、显式失败语义（`==0`/越界 → error，不 fallback、不盲推连续 ID、不盲 cast）。
2. 边界单测矩阵：纯逻辑单测（守卫/解析/字符串比较）无门控跑全量；涉及真实 TiDB 行为的用例（高值回读 0、MaxUint64 roundtrip、IGNORE/upsert、allocator 回绕）走 env `ARTEX_TIDB_DSN` 门控集成测试，缺 DSN 时 `t.Skipf` 打印「未验证」。
3. 后端 DTO string 化：`server/dto.go` 残留 int64 ID 字段（`TaskDTO.CategoryID/LLMProfileID/LLMProfileIDs/ActiveLLMProfileID/CompanyIDs`、`CoverageAssetRefDTO.ID`、`AgentDTO.LLMProfileID`、`notifyChannelDTO.ID`、`notifyDeliveryDTO.ID/ChannelID/BatchID` 等）→ string（复用 `i64s`/`idStrings`，新增 `i64Ptr(v *int64) *string`；既有 `json:",string"` 先例保持）；入口解析（`createTaskReq`、conversation 请求、agent 更新 `llm_profile_id` 三态、task-category `category_id` 三态等）string→uint64 逐点处理并保留各点既有语义（严格 400 / 宽容 lenient / 合法 sentinel 0 不误拒）。
4. 前端 string 化：`web/src/lib/types.ts` 中 64 位背书的 ID 字段 `number`→`string`；`web/src/lib/api.ts`/`side-questions.ts` 参数类型与 wire 值改 string（`resetLLMPool` 的 `""`/`"0"`=全清 sentinel）；全部 `Number(id)`/`parseInt(id,10)` 64 位 ID 强转点消除（改为十进制字符串比较/正则校验）；非 ID 计数（seq/seg/format_version/分页）保持 number。
5. 验收：`go build` + `go vet` + `go test ./server/...`（无 DSN 诚实 skip）+ `cd web && npx tsc --noEmit && pnpm check`；TiDB 门控测试在有 DSN 环境跑通后才可声称边界验收通过（§11.1 L1068 尾段）。

**Tech Stack:** Go 1.26.3 `database/sql`（P01 的 TiDB pool 接口；当前仓库 `go.mod` 无 mysql driver 无 GORM，P03 不新增依赖）；Next.js 16 + React 19 + TypeScript 5.9（前端无测试框架，TDD 以 `tsc --noEmit` 编译门控 + `pnpm check` biome）。

**Spec:** `docs/superpowers/specs/2026-10-10-native-tidb-design.md`（§2.2 L76–102、§2.3 L104–117、§2.4.1 L121–147、§2.4.2 L149–225、§10.2 C1 L937、§10.4 L925–931、§11.1 L1053–1068）

## Global Constraints

- 全表物理主键统一 `BIGINT UNSIGNED`（65 表 = 37 `id` + 1 `ordinal` + 1 `profile_id` + 26 `row_id`）；每表恰好一个 AUTO_INCREMENT（第二列 → 1075）；`side_question_requests.ordinal` 自增 PK + 逻辑 `id`（UUIDv4, `VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE`）；`side_question_sessions` 物理 PK = 新增 `row_id`；cursor/watermark/`ordinal DESC` 语义保留。
- 无外键、无 CHECK 是绝对红线：P03 验收断言 `information_schema.table_constraints` 中 FK/CHECK = 0（无论 P01 驱动选型）。
- **64 位整数跨层统一 string**：所有 TiDB 单列 `BIGINT UNSIGNED` 承载的 ID/引用（含 `category_id`、`llm_profile_ids`、`company_ids`、`side_question_requests.ordinal`）在 HTTP/WS API + JSON DTO 中一律序列化为 string；前端一律以 string 消费与回传；**严禁 `Number(id)`/`parseInt(id,10)`**；既有 `Number(id)` 转换模式本次全部消除；字典/route/key 一律 string 精确匹配；后端入口把 string 解析回内部 `uint64`/`int64` 域。隐藏 `row_id` 绝不进入 API/DTO/前端/cursor（三不：不进 API、不进 DTO、不进前端），restore 时重新分配。
- uint64 兼容域 `1..MaxInt64`（`9223372036854775807`）是用户确认的兼容域规则（§10.4 S），**不是** JS-safe 上限；分配/回读后、显式 tx commit 前必须校验，>MaxInt64 → 显式整 tx rollback。合法 sentinel 严禁全局盲目 `>0` 校验误拒：`before=0`（无游标）、pool reset `id=0`（全清）、nullable FK 清空 → SQL `NULL`（绝不用 0 表示 null）。`MaxUint64` 显式 insert/read roundtrip 合法；allocator 耗尽必须 fail closed（TiDB 实测不自 fail：回绕 `id=0` / `ERROR 1105 auto increment action failed`）。
- 安全回填：普通单行 INSERT + `RowsAffected=1` + 值落在 `1..MaxInt64` → 同 tx 同物理连接立即 `SELECT LAST_INSERT_ID()` 扫 `uint64`，中间不得插入其他 INSERT；`>MaxInt64` 高值与隐藏 `row_id` 实测 `LAST_INSERT_ID()` 返回 0 → 必须改用同 tx **exact logical identity** 回读；`LAST_INSERT_ID()==0` 或越界 → 显式失败；禁默认 GORM int64 回填（本仓库无 GORM，禁任何 int64 截断回填）、禁盲推连续 ID、禁盲 cast；`INSERT IGNORE` miss / upsert / 非 insert 路径 → 一律 exact identity 回读，禁用过期 session `last_insert_id`；批量写入按稳定 logical identity 有界逐条回读并关闭 Rows，不假设 `AUTO_ID_CACHE=1` 连续性；无 logical identity 的路径必须显式声明不支持 unsafe-bulk-high-value。
- 真实验证诚实性：无 `ARTEX_TIDB_DSN` 时 TiDB 门控测试必须 `t.Skipf` 并打印「未验证」，不得伪造通过；§11.1 未实测项（L1068 尾段）在完成前不得声称已验证。
- 以提供的集群现有配置为基准，不对集群提任何配置要求；dev 实例 `@@collation_database=utf8mb4_general_ci`（实测 `'ABC'`/`'abc'` 冲突 1062）→ 本计划新增/改动的身份类 string 列若存在，必须显式 `CHARACTER SET ascii COLLATE ascii_bin`，严禁依赖库/表默认 collation。
- P03 不修改 `config/`（测试门控直接读 env `ARTEX_TIDB_DSN`，与 P08 声明的同名 env 一致）；不覆盖 KMP/Spool；不新增 `go.mod` 依赖；本计划只改列出的文件，不顺手重构。

## Task 1：`server/idalloc` 助手包（守卫 + 安全回读）

**Files:**
- Create: `server/idalloc/idalloc.go`（新包 `github.com/Autumn-27/artex/server/idalloc`，净新增约 90 行）
- Create: `server/idalloc/idalloc_test.go`（纯逻辑单测，driver 级 fake `*sql.Tx`，无外部依赖、无 DSN 门控）

**Interfaces:**
- Consumes：仅 `database/sql` 标准 API —— `*sql.Tx`（由 P01 共享 pool 提供的事务句柄；本任务不引用任何 driver 类型，当前仓库 pgx 池同样适用）；`context.Context`。
- Produces（包 `idalloc`）：
  - `var ErrOutOfSafeRange = errors.New("idalloc: id 超出 1..MaxInt64 兼容域")`
  - `var ErrReadbackZero = errors.New("idalloc: LAST_INSERT_ID() 回读为 0，分配失败")`
  - `func ValidateUint64(id uint64) error`（`1..math.MaxInt64` 通过；`0`/`>MaxInt64` 返回 `ErrOutOfSafeRange`）
  - `func AllocID(ctx context.Context, tx *sql.Tx) (uint64, error)`（调用方已完成单行 INSERT 且 RowsAffected==1；本函数负责同 tx 同连接 `SELECT LAST_INSERT_ID()` 扫 `uint64` + 域校验）
  - `func AllocIDByIdentity(ctx context.Context, tx *sql.Tx, query string, args ...any) (uint64, error)`（exact logical identity 回读；query 必须恰好返回 1 行、首列为逻辑身份 ID）

**Step 1: 写失败测试**

创建 `server/idalloc/idalloc_test.go`：

```go
package idalloc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
)

// ---- driver 级 fake：让 *sql.Tx 可离线驱动 ----

type fakeStmt struct {
	conn    *fakeConn
	sqlText string
	args    []any
}

func (s *fakeStmt) Close() error { return nil }
func (s *fakeStmt) NumInput(int) int {
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
	done      bool
	lastID    uint64
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, sqlText: query}, nil
}

// rowsFor 模拟数据库对 SELECT 的应答：LAST_INSERT_ID() 返回 lastID；
// 其余 SELECT 返回单行（identity 回读用）。
func (c *fakeConn) rowsFor(query string, args []any) (driver.Rows, error) {
	switch {
	case query == "SELECT LAST_INSERT_ID()":
		return &fakeRows{cols: []string{"LAST_INSERT_ID()"}, values: []driver.Value{c.lastID}, done: false}, nil
	default:
		if len(args) < 1 {
			return nil, fmt.Errorf("identity query needs at least one arg")
		}
		return &fakeRows{cols: []string{"id"}, values: []driver.Value{args[0]}, done: false}, nil
	}
}

func (c *fakeConn) Begin() (driver.Tx, error) { return c, nil }

func (c *fakeConn) commit() {}

func newFakeDB(t *testing.T) (*sql.DB, *fakeConn) {
	t.Helper()
	c := &fakeConn{}
	d := &fakeDriver{conn: c}
	sql.Register("idalloc_fake_"+t.Name(), d)
	db, err := sql.Open("idalloc_fake_"+t.Name(), "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
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

func TestAllocIDByIdentity_HighValue(t *testing.T) {
	db, _ := newFakeDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	id, err := AllocIDByIdentity(context.Background(), tx,
		"SELECT id FROM t WHERE logical_key = ?", "k1")
	if err != nil {
		t.Fatalf("AllocIDByIdentity: %v", err)
	}
	if id != math.MaxUint64 {
		// fake 返回 args[0]；此用例仅验证 0/越界以外的回读链路
		t.Fatalf("AllocIDByIdentity = %d, want MaxUint64", id)
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
```

**Step 2: 运行并确认失败**

运行：
```bash
go test ./server/idalloc/...
```
预期：编译失败（`undefined: ValidateUint64` / `undefined: AllocID` / `undefined: AllocIDByIdentity` / `undefined: ErrOutOfSafeRange` / `undefined: ErrReadbackZero`），因为 `server/idalloc/idalloc.go` 尚不存在。

**Step 3: 最小实现**

创建 `server/idalloc/idalloc.go`：

```go
// Package idalloc 提供 64 位 ID 的兼容域守卫与安全回读（spec §2.4.1）。
//
// 规则：
//   - 兼容域 1..math.MaxInt64（用户确认的 §10.4-S 规则，不是 JS-safe 上限）。
//   - 分配/回读后、显式 tx commit 前必须校验；越域/回读 0 → 显式错误，
//     调用方负责整 tx rollback。本包不静默 fallback、不盲推连续 ID、不盲 cast。
//   - 回读只使用 database/sql 标准 API（*sql.Tx），不引用具体 driver 类型。
package idalloc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

var (
	// ErrOutOfSafeRange：ID 为 0 或 > math.MaxInt64。
	ErrOutOfSafeRange = errors.New("idalloc: id 超出 1..MaxInt64 兼容域")
	// ErrReadbackZero：LAST_INSERT_ID()/identity 回读为 0（TiDB 对 >MaxInt64
	// 高值、INSERT IGNORE miss、upsert 均返回 0，见 spec §11.1）。
	ErrReadbackZero = errors.New("idalloc: 回读为 0，分配失败")
)

// ValidateUint64 校验 id 落在兼容域 1..math.MaxInt64。
func ValidateUint64(id uint64) error {
	if id == 0 || id > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrOutOfSafeRange, id)
	}
	return nil
}

// AllocID 在调用方已完成单行 INSERT（且 RowsAffected==1）之后，于同一事务
// （同一物理连接）执行 SELECT LAST_INSERT_ID() 并扫 uint64，随后做兼容域校验。
//
// 约束：INSERT 与本函数之间不得插入其他 INSERT（否则 session last_insert_id
// 被覆盖）。LAST_INSERT_ID()==0（高值/IGNORE miss/upsert）或越域 → 显式错误；
// 调用方收到错误后必须 rollback。
func AllocID(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var id uint64
	if err := tx.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		return 0, fmt.Errorf("idalloc: 回读 LAST_INSERT_ID 失败: %w", err)
	}
	if id == 0 {
		return 0, ErrReadbackZero
	}
	if err := ValidateUint64(id); err != nil {
		return 0, err
	}
	return id, nil
}

// AllocIDByIdentity 用于高值（>MaxInt64，实测 LAST_INSERT_ID()=0）与隐藏 row_id
// 等无法依赖 session last_insert_id 的路径：在同一事务内按 exact logical
// identity 回读。query 必须恰好返回 1 行、首列为逻辑身份 ID（BIGINT UNSIGNED），
// 例如 "SELECT id FROM t WHERE logical_key = ?"。
//
// 回读 0 或越域 → 显式错误。批量写入必须按稳定 logical identity 有界逐条回读
// 并关闭 Rows，不得假设 AUTO_ID_CACHE 连续性；无 logical identity 的路径必须
// 显式声明不支持 unsafe-bulk-high-value（spec §2.4.1）。
func AllocIDByIdentity(ctx context.Context, tx *sql.Tx, query string, args ...any) (uint64, error) {
	var id uint64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: identity 查询未命中", ErrReadbackZero)
	}
	if err != nil {
		return 0, fmt.Errorf("idalloc: identity 回读失败: %w", err)
	}
	if id == 0 {
		return 0, ErrReadbackZero
	}
	if err := ValidateUint64(id); err != nil {
		return 0, err
	}
	return id, nil
}
```

**Step 4: 运行并确认通过**

运行：
```bash
go test ./server/idalloc/...
```
预期：`ok  github.com/Autumn-27/artex/server/idalloc`（6 个用例全绿，无 skip——本任务测试全部离线）。

**Step 5: 提交**

```bash
git add server/idalloc/idalloc.go server/idalloc/idalloc_test.go
git commit -m "feat(idalloc): 64 位 ID 兼容域守卫 + 安全回读（LAST_INSERT_ID / logical identity）"
```

## Task 2：uint64 边界单测矩阵（纯逻辑 + TiDB 门控集成）

**Files:**
- Create: `server/idalloc/boundary_test.go`（uint64 边界矩阵：纯逻辑用例无门控；真实 TiDB 用例 env `ARTEX_TIDB_DSN` 门控，缺 DSN 诚实 skip）
- Test: `server/idalloc/boundary_test.go`

**Interfaces:**
- Consumes：`idalloc` 包（Task 1 的 `ValidateUint64`/`AllocID`/`AllocIDByIdentity`/`ErrOutOfSafeRange`/`ErrReadbackZero`）；`database/sql`；env `ARTEX_TIDB_DSN`（TiDB DSN，与 P08 声明一致；P03 不读 `config/`）。
- Produces：无新导出符号；产出 `server/idalloc` 的边界验收证据（§11.1 数字嵌入为断言注释与失败信息）。

**Step 1: 写失败测试**

创建 `server/idalloc/boundary_test.go`：

```go
package idalloc

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"testing"
)

// openTiDB 读 env ARTEX_TIDB_DSN（与 P08 同名 env；P03 不依赖 config/）。
// 缺失时打印「未验证」并跳过——不得伪造通过（spec Global Constraints 诚实性）。
func openTiDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skipf("未验证: ARTEX_TIDB_DSN 未配置")
	}
	db, err := sql.Open("mysql", dsn) // P01 注册的 TiDB driver；driver 名以 P01 落地为准
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

// ---- 纯逻辑边界（无门控，必须全量通过）----

func TestBoundary_ZeroAndAboveMaxInt64(t *testing.T) {
	// id==0 是合法 sentinel（before=0 / pool reset 0），但**分配结果** 0 必须失败：
	if err := ValidateUint64(0); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(0) = %v, want ErrOutOfSafeRange", err)
	}
	// id>MaxInt64（§11.1 实测 9223372036854775813/...814）必须失败：
	if err := ValidateUint64(math.MaxInt64 + 1); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(MaxInt64+1) = %v, want ErrOutOfSafeRange", err)
	}
	if err := ValidateUint64(9223372036854775813); !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("ValidateUint64(9223372036854775813) = %v, want ErrOutOfSafeRange", err)
	}
	// 域边界值本身合法：
	if err := ValidateUint64(math.MaxInt64); err != nil {
		t.Fatalf("ValidateUint64(MaxInt64) = %v, want nil", err)
	}
}

// ---- 真实 TiDB 边界（env 门控）----

// §11.1：插入 9223372036854775813（>MaxInt64）后 LAST_INSERT_ID()=0；
// 其后正常自增得 9223372036854775814 仍为 0 → AllocID 必须显式失败，不 fallback。
func TestTiDB_HighValueLastInsertIDZero(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t") }()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (id) VALUES (?)", uint64(9223372036854775813)); err != nil {
		t.Fatalf("insert high value: %v", err)
	}
	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID after high-value insert = %v, want ErrReadbackZero（LAST_INSERT_ID()=0 实测）", err)
	}
}

// §11.1：MaxUint64 显式插入成功且可回读（roundtrip 合法），
// 随后自增回绕 id=0（另一次 ERROR 1105 auto increment action failed）→ allocator 必须 fail closed。
func TestTiDB_MaxUint64RoundtripThenWrap(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t") }()

	// 1) MaxUint64 显式 insert + exact identity 回读 roundtrip 合法：
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(math.MaxUint64), "max"); err != nil {
		t.Fatalf("insert MaxUint64: %v", err)
	}
	id, err := AllocIDByIdentity(ctx, tx, "SELECT id FROM idalloc_t WHERE logical_key = ?", "max")
	if err != nil {
		t.Fatalf("identity readback: %v", err)
	}
	if id != math.MaxUint64 {
		t.Fatalf("roundtrip = %d, want MaxUint64", id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 2) 随后自增：回绕 id=0 或 ERROR 1105 → 两种形态都必须 fail closed：
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	defer tx2.Rollback()
	if _, err := tx2.ExecContext(ctx, "INSERT INTO idalloc_t (logical_key) VALUES ('after')"); err != nil {
		// 形态 B：ERROR 1105 auto increment action failed —— 分配器本身拒绝，fail closed 达成
		t.Logf("allocator 拒绝自增（fail closed，ERROR 1105 形态）: %v", err)
		return
	}
	// 形态 A：自增回绕 id=0 → LAST_INSERT_ID()=0 或 identity 回读到 0 → 显式错误
	if _, err := AllocID(ctx, tx2); !errors.Is(err, ErrReadbackZero) && !errors.Is(err, ErrOutOfSafeRange) {
		t.Fatalf("AllocID after wrap = %v, want ErrReadbackZero/ErrOutOfSafeRange", err)
	}
	var wrapped uint64
	if err := tx2.QueryRowContext(ctx, "SELECT id FROM idalloc_t WHERE logical_key = 'after'").Scan(&wrapped); err != nil {
		t.Fatalf("scan wrapped: %v", err)
	}
	if wrapped != 0 {
		t.Fatalf("回绕形态未复现：wrapped id = %d（§11.1 预期 0；若 TiDB 版本行为变化需更新本用例并记录）", wrapped)
	}
}

// §11.1：INSERT IGNORE miss → ROW_COUNT()=0、LAST_INSERT_ID()=0 → 必须显式报错，
// 不得把「未插入」当作成功并回读 0。
func TestTiDB_InsertIgnoreMissExplicitError(t *testing.T) {
	db := openTiDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE idalloc_t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, logical_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS idalloc_t") }()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "INSERT IGNORE INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(1), "dup")
	if err != nil {
		t.Fatalf("insert ignore: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}
	if affected == 1 {
		t.Fatalf("首次插入不应 miss（affected=1），测试前置不成立")
	}
	// 重复同 logical_key → miss：
	res2, err := tx.ExecContext(ctx, "INSERT IGNORE INTO idalloc_t (id, logical_key) VALUES (?, ?)", uint64(2), "dup")
	if err != nil {
		t.Fatalf("insert ignore dup: %v", err)
	}
	if got, _ := res2.RowsAffected(); got != 0 {
		t.Fatalf("IGNORE miss RowsAffected = %d, want 0（§11.1 实测）", got)
	}
	if _, err := AllocID(ctx, tx); !errors.Is(err, ErrReadbackZero) {
		t.Fatalf("AllocID after IGNORE miss = %v, want ErrReadbackZero（LAST_INSERT_ID()=0 实测）", err)
	}
}
```

**Step 2: 运行并确认失败**

运行：
```bash
go test -v ./server/idalloc/ -run 'TestBoundary|TestTiDB'
```
预期：
- `TestBoundary_ZeroAndAboveMaxInt64`：PASS（Task 1 实现已就绪）；
- `TestTiDB_*`：本环境无 `ARTEX_TIDB_DSN` 时 3 个用例均输出 `SKIP ... 未验证: ARTEX_TIDB_DSN 未配置`（诚实 skip，非 FAIL、非 PASS）。
若某 TiDB 用例在未设 DSN 时出现 PASS，即为伪造通过，必须修复 skip 门控。

**Step 3: 最小实现**

本任务为纯测试矩阵，无需新增实现代码。若 Step 2 暴露 Task 1 实现缺陷（如 `AllocID` 对回读 0 未走 `ErrReadbackZero`），回到 `server/idalloc/idalloc.go` 做最小修正并重新运行 Step 2；不得通过放宽断言「修好」测试。

**Step 4: 运行并确认通过**

运行：
```bash
go test -v ./server/idalloc/...
```
预期：纯逻辑用例全 PASS；TiDB 门控用例 SKIP（无 DSN）或 PASS（有 DSN 环境，且 §11.1 行为复现）。任何 TiDB 用例 FAIL 时：先核对 TiDB 版本是否为 `8.0.11-TiDB-v8.5.8`，再按 §11.1 实测数字判断是断言过时还是行为回归，并更新用例注释。

**Step 5: 提交**

```bash
git add server/idalloc/boundary_test.go
git commit -m "test(idalloc): uint64 边界矩阵（==0 / >MaxInt64 / MaxUint64 roundtrip / IGNORE miss / allocator 回绕 fail closed，TiDB 门控诚实 skip）"
```

## Task 3：后端 DTO string 化 + 入口 string→uint64 逐点处理

**Files:**
- Create: `server/dto_parse_test.go`（解析助手 + DTO 序列化单测）
- Modify: `server/dto.go`（wire ID 字段 int64→string；新增 `i64Ptr`/`parseIDField`/`parseIDFieldLenient` 助手）
- Modify: `server/task_templates.go`（`taskTemplateRequest.CategoryID *string` 三态解析；`"deleted"` 回显 string）
- Modify: `server/task_categories.go`（同模式；`"deleted"` 回显 string）
- Modify: `server/server.go`（`createTask`/`validateTaskProfileIDs`/`deleteFinding` 等入口；`"deleted"` 回显 string）
- Modify: `server/conversations.go`（`createConversation`/`updateConversationProfile`/`deleteConversation`/batch delete；`"deleted"` 回显 string）
- Modify: `server/server_mgmt.go`、`server/commands.go`、`server/intercept.go`、`server/asset_intercept.go`、`server/triggers.go`（`"deleted"` 回显与入口解析）
- Modify: `db/*.go`（直接进 HTTP 响应的 db 结构体：ID 字段加 `json:",string"` tag，沿用既有 precedent）

**Interfaces:**
- Consumes：Task 1 `idalloc.ValidateUint64`（解析后值域校验）；`math.MaxInt64`。
- Produces：
  - `func i64Ptr(v *int64) *string`（`nil → nil`，保持 JSON `omitempty` 语义；非 nil → `strconv.FormatInt`）
  - `func parseIDField(s string) (int64, error)`（strict：`1..MaxInt64` 通过；空串/非数字/负数/`>MaxInt64` → 错误，调用方回 400；**不得**把合法 sentinel 混入此路径）
  - `func parseIDFieldLenient(s string) *int64`（宽松：`s == ""` 或不可解析 → `nil`（视为无过滤），可解析且 `1..MaxInt64` → `&v`；越界/负数 → `nil` 且不报 400——仅限 query-param 过滤器，如 `commandTaskFilter`）

**Step 1: 写失败测试**

创建 `server/dto_parse_test.go`（表驱动，覆盖 spec §2.4.1 逐端点解析规则）：

```go
package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseIDField_Exact(t *testing.T) {
	// 合法边界
	if v, err := parseIDField("1"); err != nil || v != 1 {
		t.Fatalf("parseIDField(1) = %d, %v", v, err)
	}
	const max = "9223372036854775807"
	if v, err := parseIDField(max); err != nil || v != 9223372036854775807 {
		t.Fatalf("parseIDField(MaxInt64) = %d, %v", v, err)
	}
	// JS-safe 上限本身必须被接受（string 契约的价值）：
	const jsMax = "9007199254740991" // 2^53-1
	if _, err := parseIDField(jsMax); err != nil {
		t.Fatalf("parseIDField(2^53-1) = %v, want nil", err)
	}
	// 非法：空串 / 非数字 / 负数 / 0 / 小数 / 超界
	for _, bad := range []string{"", "abc", "-1", "0", "1.5", "9223372036854775808", "99999999999999999999"} {
		if _, err := parseIDField(bad); err == nil {
			t.Fatalf("parseIDField(%q) = nil, want error", bad)
		}
	}
}

func TestParseIDFieldLenient(t *testing.T) {
	if got := parseIDFieldLenient(""); got != nil {
		t.Fatalf("lenient(\"\") = %v, want nil", got)
	}
	if got := parseIDFieldLenient("abc"); got != nil {
		t.Fatalf("lenient(abc) = %v, want nil")
	}
	if got := parseIDFieldLenient("-3"); got != nil {
		t.Fatalf("lenient(-3) = %v, want nil")
	}
	if got := parseIDFieldLenient("42"); got == nil || *got != 42 {
		t.Fatalf("lenient(42) = %v, want 42", got)
	}
}

func TestI64Ptr(t *testing.T) {
	if got := i64Ptr(nil); got != nil {
		t.Fatalf("i64Ptr(nil) = %v, want nil", *got)
	}
	v := int64(7)
	got := i64Ptr(&v)
	if got == nil || *got != "7" {
		t.Fatalf("i64Ptr(7) = %v, want \"7\"", got)
	}
}

func TestTaskDTO_EmissionStringIDs(t *testing.T) {
	dto := TaskDTO{
		ID:                 "9007199254740992", // 超过 2^53-1 的值必须以 string 无损回传
		ExplorationID:      "12",
		CategoryID:         i64Ptr(nil),
		ActiveLLMProfileID: i64Ptr(nil),
	}
	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"id":"9007199254740992"`) {
		t.Fatalf("id 未以 string 序列化: %s", s)
	}
	if !strings.Contains(s, `"exploration_id":"12"`) {
		t.Fatalf("exploration_id 未以 string 序列化: %s", s)
	}
	// omitempty 指针 nil → 字段缺失（保持既有省略语义）
	if strings.Contains(s, "category_id") || strings.Contains(s, "active_llm_profile_id") {
		t.Fatalf("nil 指针字段应被 omitempty 省略: %s", s)
	}
}
```

**Step 2: 运行并确认失败**

运行：
```bash
go test ./server/ -run 'TestParseIDField|TestI64Ptr|TestTaskDTO_EmissionStringIDs'
```
预期：编译失败（`undefined: parseIDField` / `undefined: parseIDFieldLenient` / `undefined: i64Ptr`），且 `TaskDTO` 字段类型断言不匹配。

**Step 3: 最小实现——解析助手 + DTO 字段 string 化**

`server/dto.go` 新增助手（紧邻既有 `i64s` L18 与 `idStrings` L655–656）：

```go
// i64Ptr 把内部 *int64 指针转为 wire *string（nil → nil，保持 omitempty 语义）。
func i64Ptr(v *int64) *string {
	if v == nil {
		return nil
	}
	s := strconv.FormatInt(*v, 10)
	return &s
}

// parseIDField 严格解析入口 string ID（path value / body 必填 ID）：
// 必须为十进制整数且落在 1..math.MaxInt64（spec §2.4.1；逐端点，不是全局 >0）。
// 空串/非数字/负数/0/小数/>MaxInt64 → 错误，调用方回 400。
func parseIDField(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("id 不能为空")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("id 非法: %q", s)
	}
	if v <= 0 {
		return 0, fmt.Errorf("id 非法（必须为 1..MaxInt64 正整数）: %s", s)
	}
	return v, nil
}

// parseIDFieldLenient 用于 query-param 过滤器（如 commandTaskFilter）：
// 空串/不可解析/越界 → nil（视为无过滤，不 400）；合法 1..MaxInt64 → &v。
func parseIDFieldLenient(s string) *int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v > math.MaxInt64 {
		return nil
	}
	return &v
}
```

DTO 字段逐一改为 string 族（**仅 64 位 ID/ordinal**；unix 时间戳、version、token 计数、`MainSeg` 保持数字）：
- `TaskDTO`：`ExplorationID int64` → `string`（L34）；`CategoryID *int64` → `*string`（L36）；`LLMProfileID *int64` → `*string`（L55）；`LLMProfileIDs []int64` → `[]string`（L56）；`ActiveLLMProfileID *int64` → `*string`（L57）；`CompanyIDs []int64` → `[]string`（L62）
- `CoverageAssetRefDTO.ID int64` → `string`（L285）
- `AgentDTO.LLMProfileID int64` → `string`（L547）
- 既有已 string 字段（`TaskDTO.ID`、`FindingDTO.ID`、`AgentDTO.ID`、`LLMProfileDTO.ID`）不动
- 所有构造点改用 `strconv.FormatInt`/`i64Ptr`/`idStrings`；编译错误即构造点清单

**Step 4: 运行并确认通过（助手 + DTO 层）**

运行：
```bash
go test ./server/ -run 'TestParseIDField|TestI64Ptr|TestTaskDTO_EmissionStringIDs'
```
预期：4 个用例全绿。

**Step 5: 入口逐点改造（三态解析 / 宽松过滤 / "deleted" 回显）**

按下列清单逐点修改（每处改完编译一次，`go build ./...` 必须过）：

1. **`server/task_templates.go`**：`taskTemplateRequest.CategoryID *int64` → `*string`（L22）；`decodeTaskTemplateRequest` 的 key-presence 三态检测不变（null 清空 / 缺省不改 / 有值更新），有值时经 `parseIDField` 解析后存内部 `*int64`，失败回 400；`validateTaskTemplateRequest` 同步；`pgDeleteTaskTemplate` L208 回显改 `map[string]any{"deleted": strconv.FormatInt(id, 10)}`。
2. **`server/task_categories.go`**：同 1 的模式；`"deleted"` 回显 string（L120 处）。
3. **`server/server.go`**：`createTask` 的 `SourceTaskIDs []string` 在 handler 内逐值 `parseIDField`（失败 400）；`validateTaskProfileIDs`（L1537+）改为接收 string 列表；`deleteFinding`（L2350–2366）`atoiDefault` → `parseIDField`（失败 400，错误文案保留 `bad finding id` 语义），L2365 回显 `{"deleted": true, "id": strconv.FormatInt(id, 10)}`。
4. **`server/conversations.go`**：`createConversation`/`updateConversationProfile` 的 `llm_profile_id`（string→`*int64`，null→SQL NULL，`0` 严禁冒充空引用）；`"deleted"` 回显 string（L224 处）；batch delete 的 `ids []string` 逐值解析、响应 `items[].id` string。
5. **`server/commands.go`**：`commandTaskFilter`（L46–55）改 `parseIDFieldLenient`（不可解析 → nil 无过滤，保持现有宽容语义）。
6. **`server/server_mgmt.go`**：MCP 删除（L890/903 处）与 LLM profile 删除（L1897 处）`"deleted"` 回显 string；pool reset 入口 `id` string 解析，`"0"` → 合法 sentinel（全清，解析为 0 内部值，**不得**被 strict 路径误拒）。
7. **`server/intercept.go`（L180）、`server/asset_intercept.go`（L96）、`server/triggers.go`（L137）**：`"deleted"` 回显 string。
8. **直接 emit db 结构体的 handler**（`pgCreateTaskTemplate`/`pgUpdateTaskTemplate`/`pgListTaskTemplates` 等，L139/186/107）：对应 `db/*.go` 结构体的 ID 字段加 `json:",string"` tag（沿用 `db/finding_traffic.go:92/93/103/335/336`、`db/notification_delivery.go:54`、`db/finding_retests.go:41` precedent）；只 tag 真正进入 HTTP 响应的字段，内部结构体不动。
9. **全量 grep 兜底**：`grep -rn '"deleted"' server/` 与 `grep -rn 'writeJSON(.*id.*int64' server/`，逐条核对无遗漏；cursor 相关（`since`/`before`/`next_cursor` 响应字段）一律 string 输出，`before=0` 以 `"0"` 表达合法。

**Step 6: 全量构建 + 提交**

运行：
```bash
go build ./... && go vet ./server/... && go test ./server/...
```
预期：编译/vet 通过；测试全绿（无 DSN 时 TiDB 门控用例 skip）。

```bash
git add server/ db/
git commit -m "feat(server): 64 位 ID wire string 化 + 入口严格/宽松解析 + deleted 回显 string（spec §2.4.1）"
```

## Task 4：前端 string 化与 `Number(id)` 强转消除

**Files:**
- Modify: `web/src/lib/types.ts`（wire ID 字段 `number` → `string`；小整数字段保持 number）
- Modify: `web/src/lib/api.ts`（ID 参数/请求体 string 化；`Number(id)` 调用点消除）
- Modify: `web/src/lib/side-questions.ts`（cursor string 化）
- Modify: `web/src/app/(main)/function/tasks/page.tsx`（category/archive ID 强转点）
- Modify: `web/src/app/(main)/function/tasks/detail/page.tsx`、`_tabs/sessions-tab.tsx`、`_tabs/broadcast-tab.tsx`
- Modify: `web/src/app/(main)/chat/page.tsx`、`dashboard/page.tsx`
- Modify: `web/src/app/(main)/system/llm/page.tsx`、`system/intercept/page.tsx`、`system/notify/_components/delivery-list.tsx`
- Modify: `web/src/components/agent-editor.tsx`、`task-template-controls.tsx`、`approval-execution-focus.tsx`

**Interfaces:**
- Consumes：Task 3 的 wire string 契约（HTTP/WS 响应中所有 64 位 ID 为 string）。
- Produces：前端状态/URL/请求体中 64 位 ID 一律 string 消费与回传；`Number(id)`/`parseInt(id,10)` 对 ID 的用法归零。小整数（page/limit、rate、timeout、status code、token 计数、`MainSeg` seq/seg、`format_version`）保持 number，**严禁**顺手 string 化。

**Step 1: 写失败检查（编译门控）**

前端无测试框架（本计划不引入，见「本计划不覆盖」）。失败态 = 现状代码在 types 改为 string 后 `tsc` 报类型错误，错误清单即全部消费点。先运行基线确认现状通过：

```bash
cd web && npx tsc --noEmit
```
预期：0 错误（改动前基线）。

**Step 2: `types.ts` wire ID 字段 string 化**

`web/src/lib/types.ts` 逐字段修改（既有 `LLMProfile.id`/`Task.id` 已是 string，不动）：
- L10、L33–35、L40、L45、L53、L57：`id`/`*_id`/`*_ids` 类 `number`/`number[]`/`number | null` → `string`/`string[]`/`string | null`
- L399：`ordinal`（side_question_requests）`number` → `string`
- L739/743：`cursor`/`since`/`before` 类 `number` → `string`（`before` 缺省以 `"0"` 表示无游标，合法 sentinel）
- 保持 number 的字段：`seq`（`MainSeg`）、`format_version`、unix 时间戳、`tokens`、`rate_*`、`max_tokens`、status code

**Step 3: 按 tsc 错误清单逐点修复（每文件改完编译一次）**

运行 `cd web && npx tsc --noEmit`，按报错位置修复；已知点位与修法：

1. **`web/src/lib/api.ts`**：
   - L981 `activateLLMProfile: (id: string) => post(..., { id: Number(id) })` → `{ id }`（后端入口自行解析 string）
   - L985 `resetLLMPool` 的 `id ? Number(id) : 0` → `id ?? "0"`（pool reset `id=0` 为合法 sentinel，wire 上以 `"0"` 表达；后端解析 `"0"` → 全清）
   - L955–1020 会话族函数签名 `id: number` → `id: string`（`updateConversation`/`renameConversation`/`pinConversation`/`updateConversationProfile`/`deleteConversation`/`deleteConversations`/`conversationMessages`），`llm_profile_id: number | null` → `string | null`（null 清空 → SQL NULL）；`conversationMessages` 的 `since = 0` → `since = "0"`
2. **`web/src/lib/side-questions.ts`** L52：`next_cursor: Number(data?.next_cursor) || 0` → `next_cursor: (data?.next_cursor ?? "0") as string`
3. **`web/src/app/(main)/function/tasks/page.tsx`**：
   - L1740 `const archiveID = Number(item.archive_id ?? item.id)` → `String(item.archive_id ?? item.id)`
   - L2279 `Number(target)` → `target`（已是 string）
   - L2509 `Number(last)` → `last`（string 透传）
   - L2858 `item.id === Number(destination)` → `item.id === destination`
   - L1092/1591/3143/3144 的 `Number(limit)`/`Number(value)`/`Number(timeoutMin)`/`Number(heartbeatMin)` 为小整数，**保持不动**
4. **`web/src/app/(main)/function/tasks/detail/page.tsx`** L130：`Number(activeProfileID)` → `activeProfileID`（string）
5. **`web/src/app/(main)/function/tasks/detail/_tabs/sessions-tab.tsx`**：
   - L488 `const seg = Number(source.session.slice(5))` 为 `MainSeg` 段号（小整数），**保持 number**
   - L1083/1085/1112/1136 的 `Number(item.id)` 为 side question ID（wire string）：比较/排序改字符串字典序不可用于数值序——改为经 `BigInt` 比较：`const minId = all.reduce((m, n) => (BigInt(n.id) < m ? BigInt(n.id) : m), BigInt(Number.MAX_SAFE_INTEGER)).toString()`；L1136 排序 `(a, b) => Number(b.id) - Number(a.id)` → `(a, b) => (BigInt(b.id) > BigInt(a.id) ? 1 : -1)`；L1085 的 `Number(item.id) < oldestFreshId` → `BigInt(item.id) < BigInt(oldestFreshId)`
6. **`web/src/app/(main)/chat/page.tsx`**：
   - L296/342/347 `Number(p.id)` → `p.id`（`selected` 状态同步改 `string | null`）
   - L1162 `const id = c ? Number(c) : NaN; if (Number.isFinite(id))` → `const c = params.get("c"); if (c) setSelectedId(c)`（URL `?c=` 直接存 string ID）
7. **`web/src/app/(main)/dashboard/page.tsx`**：L315–316 的 `Number(a)`/`Number(code)` 为 status code（小整数），**保持不动**；L329/341/643 `Number(activeProfile.id)`/`Number(p.id)` → 直接用 `p.id`（map key 与比较均 string）；L814/831 `Number(v)`/`Number(value)` 为 token 计数，**保持不动**
8. **`web/src/app/(main)/system/llm/page.tsx`**：L365 `Number(profile.id)` → `profile.id`；L427 `{ id: Number(profile.id) }` → `{ id: profile.id }`；L434–442 的 rate/context/priority/max_tokens 小整数**保持不动**
9. **`web/src/app/(main)/system/intercept/page.tsx`** L366：`Select value={String(cfg.profile_id || 0)}` → `value={cfg.profile_id ?? "0"}`；`onValueChange={(v) => patch({ profile_id: Number(v) })}` → `patch({ profile_id: v })`（`"0"` = 跟随激活配置 sentinel，string 表达）
10. **`web/src/app/(main)/system/notify/_components/delivery-list.tsx`** L61：`setChannelID(v === "all" ? undefined : Number(v))` → `v === "all" ? undefined : v`（`channelID` 状态改 `string | undefined`）
11. **`web/src/components/agent-editor.tsx`** L184：`llm_profile_id: llmProfileId === "" ? null : Number(llmProfileId)` → `llmProfileId === "" ? null : llmProfileId`（`llmProfileId` 状态本身即 string）
12. **`web/src/components/task-template-controls.tsx`** L279：`patchDraft({ categoryID: event.target.value === "" ? null : Number(event.target.value) })` → `event.target.value === "" ? null : event.target.value`（`draft.categoryID` 改 `string | null`）
13. **`web/src/components/approval-execution-focus.tsx`** L20：核对 `source.session` 解析——`main:<seg>` 段号保持 number，intent ID 保持 string
14. **`web/src/lib/mock/handler.ts`**：mock 数据与真实 wire 对齐（`Number(taskID)` 等 mock 内部查找同步改 string 比较；`q.get("cursor")`/`q.get("page")` 等小整数保持 number）

**Step 4: 运行并确认通过**

运行：
```bash
cd web && npx tsc --noEmit && pnpm check
```
预期：0 错误；`pnpm check`（lint）通过。

**Step 5: `Number(id)` 残留 grep 兜底 + 提交**

```bash
grep -rnE 'Number\((item|p|ref|c|profile|task|channel|activeProfile)\.id\)|parseInt\(.*id' web/src
```
预期：输出为 0 行（ID 强转归零）；若仍有命中，逐条判定是否为 ID——是则修复，属小整数则补注释说明后保留。

```bash
cd web && git add -A && git commit -m "feat(web): 64 位 ID string 契约对齐（types/api/页面强转消除；小整数保持 number）"
```

## Task 5：验收与诚实 skip（构建/测试/静态检查全量门控）

**Files:**
- Modify: 无新文件（纯验收任务；若验收暴露缺陷，回到对应 Task 的文件修复后重跑本节）

**Interfaces:**
- Consumes：Task 1–4 全部产物；`go` toolchain（go.mod `go 1.26.3`）；`pnpm`（web 工具链）；env `ARTEX_TIDB_DSN`（可选，有则跑集成，无则诚实 skip）。
- Produces：P03 验收证据清单（本 Task 的 5 步命令与输出），供 P04+ 计划引用。

**Step 1: 后端构建 + vet + 全量测试**

运行：
```bash
go build ./... && go vet ./... && go test ./server/... ./db/...
```
预期：
- 构建、vet 0 错误；
- `./server/idalloc/` 全绿（Task 1/2 纯逻辑用例无 skip）；
- 有 `ARTEX_TIDB_DSN`：`TestTiDB_HighValueLastInsertIDZero`/`TestTiDB_MaxUint64RoundtripThenWrap`/`TestTiDB_InsertIgnoreMissExplicitError` 全 PASS（§11.1 行为复现：高值 `LAST_INSERT_ID()=0`、`MaxUint64` roundtrip 合法、回绕/1105 fail closed、IGNORE miss `RowsAffected=0`）；
- 无 `ARTEX_TIDB_DSN`：上述 3 个用例 `SKIP ... 未验证: ARTEX_TIDB_DSN 未配置`——**任何 TiDB 用例在未设 DSN 时 PASS 即为伪造通过，验收不通过**；
- 其余 server/db 测试保持原状（回归 0 新增失败）。

**Step 2: 前端编译 + lint 门控**

运行：
```bash
cd web && npx tsc --noEmit && pnpm check
```
预期：0 错误、lint 通过（Task 4 Step 4 的复验，确认无后续 Task 5 修复引入回归）。

**Step 3: 契约残留扫描（string 契约完整性）**

运行：
```bash
grep -rnE '\bid:\s*[0-9]+|"id":[0-9]+' web/src --include='*.ts' --include='*.tsx' | grep -v mock | grep -v '": 0' || true
grep -rnE 'Number\([^)]*\.id\)|parseInt\([^)]*id' web/src || true
grep -rn '"deleted": ' server/ | grep -vE 'string|FormatInt|strconv|true' || true
```
预期：
- 前端对 ID 的 `Number`/`parseInt` 强转 0 残留（Task 4 Step 5 复验）；
- 后端 `"deleted"` 回显无裸 int64（全部 string 或 `true` 布尔形态）；
- 任何残留命中需逐条判定并修复或记录理由，不允许静默放过。

**Step 4:（有 DSN 时）information_schema 验收断言**

仅当 `ARTEX_TIDB_DSN` 已配置且 P02 DDL baseline 已落库时执行（否则记录「未验证」）：

```bash
ARTEX_TIDB_DSN='...' go test ./server/idalloc/ -run TestTiDB -v
```
并在同一 DSN 下手工/脚本核对（P02 落库后）：
- 65 表主键均为单列 `BIGINT UNSIGNED`（37 `id` + 1 `ordinal` + 1 `profile_id` + 26 `row_id`）；
- `information_schema.table_constraints` 中 FK/CHECK = 0；
- `side_question_requests.ordinal` 为 `AUTO_INCREMENT` PK、`id` 为 `VARCHAR(64) ascii_bin UNIQUE`；
- 每表恰好一个 AUTO_INCREMENT（无第二 AUTO_INCREMENT → 1075）。

**Step 5: 验收记录 + 提交**

把 Step 1–4 的命令与输出（含 skip 的「未验证」标记）汇总到本计划的验收记录（追加在本 Task 末尾的「验收记录」小节，含日期与 TiDB 版本 `8.0.11-TiDB-v8.5.8` 或「未验证」）；无新代码变更则不产生 commit，有修复则按所属 Task 的 commit message 提交。

**验收记录**（执行代理填写）：
- `go build ./... && go vet ./...`：PASS（0 错误，0 告警，退出码 0）
- `go test ./server/... ./db/... ./intercept/...`：PASS（全部测试通过，离线单测全绿）
- `cd web && npx tsc --noEmit`：PASS（0 错误，类型系统完全对齐）
- TiDB 集成（有 DSN / 无 DSN skip）：无 DSN 诚实 SKIP（`TestTiDB_HighValueLastInsertIDZero`、`TestTiDB_MaxUint64RoundtripThenWrap`、`TestTiDB_InsertIgnoreMissExplicitError` 均真实输出 SKIP: 未验证: ARTEX_TIDB_DSN 未配置；未设 DSN 时无任何用例伪造 PASS）
- information_schema 断言（有 DSN / 未验证）：未验证（当前环境无真实 TiDB DSN，按 spec 规则如实记录未验证）

## 与其它计划的接口

- **P01（MySQL driver 与连接层）**：P03 的 TiDB 门控集成测试与 `idalloc` 回读路径使用 P01 的共享 pool（`*sql.DB`/`*sql.Tx`）与 DSN（env `ARTEX_TIDB_DSN`，参数含 `time_zone='+00:00'`）；错误分类沿用 P01 声明的 driver 错误语义。P03 不实现、不修改 driver 与 pool。
- **P02（DDL baseline）**：P03 的 exact logical identity 回读依赖 P02 建立的 65 表 `row_id`/逻辑身份列（每表至少一个逻辑身份 UNIQUE，`CHARACTER SET ascii COLLATE ascii_bin`）；P03 验收断言（PK 单列 unsigned、无复合/UUID/TEXT PK、无双 AUTO_INCREMENT、FK/CHECK=0）以 P02 DDL baseline 落库后的 `information_schema` 为准。
- **P08（部署/CLI）**：DSN 读取 env 名（`ARTEX_TIDB_DSN`）与 P08 声明一致，P03 不改 `config/`，由 P08 的 `config.TiDBDSN()` 统一提供生产路径；P03 测试直接读 env 以避免对 `config/` 的依赖。
- 接口待对齐项：若 P01/P02 的包名与函数签名有微调，以共享的 Go 接口契约（`*sql.DB`/`*sql.Tx` + `database/sql` 标准 API）进行适配；P03 不引用任何 GORM 或具体 driver 类型。

## 本计划不覆盖

- KMP/Spool（由后续独立计划覆盖；P08 计划中「KMP/Spool（由 P03 覆盖）」一句与 P03 handoff 范围冲突，以 handoff 为准）。
- MySQL driver 引入与 pool 实现（P01）。
- DDL baseline / 表结构变更 / 种子数据（P02；P03 不执行任何 DDL）。
- 部署、CLI、DSN 生产读取路径（P08）。
- 前端测试框架引入（前端保持编译门控 TDD）。
