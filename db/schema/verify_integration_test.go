package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// defaultOperationTimeout 定义真实与测试安全流程的主 context 超时上限（2 分钟）。
const defaultOperationTimeout = 2 * time.Minute

// defaultCleanupTimeout 定义清理阶段独立有界 context 的超时上限（30 秒）。
const defaultCleanupTimeout = 30 * time.Second

// strictTiDB85Regex 匹配 strict 8.5 LTS 版本线：
// 必须包含 "TiDB-v8.5." 紧跟补丁版本数字，支持发行后缀（如 -dirty, -alpha 等）。
// F18 保留说明：采用子串匹配识别发行 banner 内含 TiDB-v8.5.<patch> 即认，
// 无匹配或未知格式 fail-closed 拒绝；不是引擎认证证明，真实 banner 仍待实测。
// 示例：
//   - "5.7.25-TiDB-v8.5.8" -> 匹配
//   - "8.0.11-TiDB-v8.5.8" -> 匹配
//   - "TiDB-v8.5.0-beta"   -> 匹配
//   - "TiDB-v6.5.0"        -> 拒绝
//   - "TiDB-v8.4.0"        -> 拒绝
//   - "8.0.32-MySQL"       -> 拒绝
var strictTiDB85Regex = regexp.MustCompile(`TiDB-v8\.5\.(\d+)`)

// isStrictTiDB85 私有 test-only 守卫：解析并校验 VERSION() 字符串是否属于 TiDB-v8.5.<patch> 系列。
// 必须满足 spec §10.2 及 task-7c-brief：
//  1. 必须读取实际 VERSION()；
//  2. 确认是 TiDB-v8.5.<patch>（包含正规发行 banner 后缀，如 8.0.11-TiDB-v8.5.8）；
//  3. 拒绝 6.5 / 8.4 / 8.1 / MySQL / SQLite / 空 / 无法解析；
//  4. 未知格式 fail closed 并返回明确错误，不降级跳过；
//  5. 保留原有 IsRealTiDB 行为不变。
func isStrictTiDB85(version string) error {
	v := strings.TrimSpace(version)
	if v == "" {
		return errors.New("strict TiDB 8.5 guard: VERSION() string is empty")
	}

	matches := strictTiDB85Regex.FindStringSubmatch(v)
	if len(matches) < 2 {
		return fmt.Errorf("strict TiDB 8.5 guard: database version %q does not match required TiDB-v8.5.<patch> LTS series", v)
	}

	patchStr := matches[1]
	_, err := strconv.ParseUint(patchStr, 10, 64)
	if err != nil {
		return fmt.Errorf("strict TiDB 8.5 guard: failed to parse patch version %q in %q: %w", patchStr, v, err)
	}

	return nil
}

// runSafeStructureWithDB 串联标准的结构验收安全测试流水线（可离线测试）：
//  1. checkDB 校验：拒绝 nil db；
//  2. 8.5 Strict Guard 预检：SELECT VERSION() 并用 isStrictTiDB85 校验；
//     拒绝则立即返回，零 DDL / 零 seed / 零 DROP；
//  3. 构造真正的 newTiDBTestHarness(db)（禁止 newMockGuardedHarness 或手动置 guardsPassed/ownedTables）；
//  4. 执行 harness.CheckGuards(ctx)（内部复验 IsRealTiDB、SELECT DATABASE() 非空、TABLES count == 0）；
//  5. 注册 defer cleanup：独立有界 cleanup ctx（defaultCleanupTimeout），仅清理本次已登记的 ownedTables；
//     使用 errors.Join 保留主流程错误与 cleanup 错误；
//  6. 执行 harness.CreateAllOwnedTables(ctx)（严格无 IF NOT EXISTS，逐表创建成功后登记 owned）；
//  7. 执行 harness.VerifyBaseTableCount(ctx, 65)；
//  8. 执行 harness.ApplyInfraSeed(ctx) 与 harness.VerifyInfraSeed(ctx)（为 probe 准备必要 locks / schema_meta 基础）；
//  9. 执行 VerifyStructure(ctx, DBQuerier{DB: db}, AllTables())（包含 Layer 1 PK、Layer 2 FK/CHECK、Layer 3 自增步长 probe、Layer 4 ASCII 碰撞 probe）；
//
// 10. defer 逆序清理本次 ownedTables；caller 负责在 safeRunner 完成后安全调用 db.Close()。
func runSafeStructureWithDB(ctx context.Context, db *sql.DB) (mainErr error) {
	if db == nil {
		return errors.New("runSafeStructureWithDB: nil db")
	}

	// 1. 私有 strict 8.5 LTS guard 检查（在任何 DDL 前）
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return fmt.Errorf("strict 8.5 guard: query VERSION() failed: %w", err)
	}
	if err := isStrictTiDB85(version); err != nil {
		return fmt.Errorf("strict 8.5 guard rejected: %w", err)
	}

	// 2. 构造标准 newTiDBTestHarness，严禁使用 newMockGuardedHarness 或直写 guardsPassed/ownedTables
	h := newTiDBTestHarness(db)

	// 3. 执行标准 CheckGuards
	if err := h.CheckGuards(ctx); err != nil {
		return fmt.Errorf("CheckGuards failed: %w", err)
	}

	// 4. 注册仅针对本次 owned 表的清理逻辑（独立有界 context，先清后留给 caller Close）
	defer func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCleanupTimeout)
		defer cleanCancel()

		cleanErr := h.Cleanup(cleanCtx)
		if cleanErr != nil {
			if mainErr != nil {
				mainErr = errors.Join(mainErr, fmt.Errorf("cleanup failed: %w", cleanErr))
			} else {
				mainErr = fmt.Errorf("cleanup failed: %w", cleanErr)
			}
		}
	}()

	// 5. 严格创建 65 张表（逐表登记归属）
	if err := h.CreateAllOwnedTables(ctx); err != nil {
		mainErr = fmt.Errorf("CreateAllOwnedTables failed: %w", err)
		return mainErr
	}

	// 6. 验证基础表数量
	if err := h.VerifyBaseTableCount(ctx, 65); err != nil {
		mainErr = fmt.Errorf("VerifyBaseTableCount failed: %w", err)
		return mainErr
	}

	// 7. 应用基础设施种子数据并验证
	if err := h.ApplyInfraSeed(ctx); err != nil {
		mainErr = fmt.Errorf("ApplyInfraSeed failed: %w", err)
		return mainErr
	}
	if err := h.VerifyInfraSeed(ctx); err != nil {
		mainErr = fmt.Errorf("VerifyInfraSeed failed: %w", err)
		return mainErr
	}

	// 8. 生产 VerifyStructure 四层结构与 probe 验证（CheckPrimaryKeys, CheckNoFKNoCheck, CheckAutoIncStep, CheckAsciiCollision）
	allTables := AllTables()
	querier := DBQuerier{DB: db}
	if err := VerifyStructure(ctx, querier, allTables); err != nil {
		mainErr = fmt.Errorf("VerifyStructure failed: %w", err)
		return mainErr
	}

	return nil
}

// runRealTiDBStructure 运行真实 TiDB 集群的结构验证完整流程。
// 复用已审定接口 mustTestDSN(t) 与 sqlOpen(dsn)。
// 无 DSN 时产生真实 t.Skip 且注明“未验证”。
// caller (t.Run / TestVerify_RealTiDB_Structure) 严格保证在 cleanup 完成后 db.Close()。
func runRealTiDBStructure(t *testing.T) {
	t.Helper()

	dsn := mustTestDSN(t)
	// 防御性校验：如果 mustTestDSN 异常返回空字符串，绝不拿空 DSN 连接默认本地库
	if strings.TrimSpace(dsn) == "" {
		t.Fatal("defensive check: mustTestDSN returned empty DSN; aborting without connecting to default local DB")
	}

	db, err := sqlOpen(dsn)
	if err != nil {
		t.Fatalf("sqlOpen failed: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("db.Close failed: %v", closeErr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	if err := runSafeStructureWithDB(ctx, db); err != nil {
		t.Fatalf("runSafeStructureWithDB failed: %v", err)
	}
}

// --- 测试用例实现 ---

// TestVerify_RealTiDB_Structure 是面向真实 TiDB 8.5 LTS 集群的结构验证入口。
// 无 ARTEX_TEST_TIDB_DSN 时产生顶层真实 Skip 并注明“未验证”。
func TestVerify_RealTiDB_Structure(t *testing.T) {
	runRealTiDBStructure(t)
}

// TestVerify_SkipsWithoutDSN 使用真实 t.Run 运行整个 runRealTiDBStructure，
// 父级清空 ARTEX_TEST_TIDB_DSN，在子测试 Cleanup 中观察 child.Skipped() 且不可达 sentinel，
// 证明跳过的是整个 runRealTiDBStructure 端到端流程，而非仅 mustTestDSN 孤立函数。
// 父级自身必须正常 PASS，不得也 Skip 假装验证。
func TestVerify_SkipsWithoutDSN(t *testing.T) {
	t.Setenv("ARTEX_TEST_TIDB_DSN", "")

	var childSkipped bool
	var reachedSentinel bool

	t.Run("child_runRealTiDBStructure_skip", func(ct *testing.T) {
		ct.Cleanup(func() {
			if ct.Skipped() {
				childSkipped = true
			}
		})

		runRealTiDBStructure(ct)
		reachedSentinel = true
	})

	if !childSkipped {
		t.Fatal("expected child test running runRealTiDBStructure to be skipped when ARTEX_TEST_TIDB_DSN is unset")
	}
	if reachedSentinel {
		t.Fatal("unreachable sentinel was reached after runRealTiDBStructure; child t.Skip did not stop execution")
	}
}

// TestStrictTiDB85Guard_PositiveAndNegativeCases 针对 strict 8.5 版本守卫进行全量正反例测试：
//   - 正例：8.0.11-TiDB-v8.5.8, 5.7.25-TiDB-v8.5.0, TiDB-v8.5.8-beta
//   - 负例：6.5 / 8.4 / 8.1 / MySQL / SQLite / 空串 / 无法解析
//   - 确认不修改原有 IsRealTiDB 行为（例如 6.5 在 IsRealTiDB 为 true，但在 isStrictTiDB85 必须拒绝）
func TestStrictTiDB85Guard_PositiveAndNegativeCases(t *testing.T) {
	positives := []string{
		"8.0.11-TiDB-v8.5.8",
		"5.7.25-TiDB-v8.5.0",
		"TiDB-v8.5.8",
		"TiDB-v8.5.12-enterprise",
		"5.7.25-TiDB-v8.5.0-alpha-community",
	}

	for _, v := range positives {
		t.Run("positive_"+v, func(t *testing.T) {
			if err := isStrictTiDB85(v); err != nil {
				t.Fatalf("expected positive version %q to pass isStrictTiDB85, got: %v", v, err)
			}
			// 确保 IsRealTiDB 同样为 true
			if !IsRealTiDB(v) {
				t.Fatalf("expected IsRealTiDB to also be true for %q", v)
			}
		})
	}

	negatives := []struct {
		name       string
		version    string
		isRealTiDB bool // 原有的 IsRealTiDB 行为应保持
	}{
		{"TiDB_6.5", "5.7.25-TiDB-v6.5.0", true},
		{"TiDB_8.4", "8.0.11-TiDB-v8.4.0", true},
		{"TiDB_8.1", "5.7.25-TiDB-v8.1.0", true},
		{"MySQL_5.7", "5.7.35-MySQL", false},
		{"MySQL_8.0", "8.0.32-MySQL", false},
		{"SQLite", "SQLite 3.39.2", false},
		{"Empty", "", false},
		{"Whitespace", "   \t  \n ", false},
		{"InvalidPatch", "TiDB-v8.5.notanumber", true},
		{"MissingPatch", "TiDB-v8.5", true},
	}

	for _, tc := range negatives {
		t.Run("negative_"+tc.name, func(t *testing.T) {
			err := isStrictTiDB85(tc.version)
			if err == nil {
				t.Fatalf("expected version %q to be rejected by isStrictTiDB85, got nil", tc.version)
			}
			// 验证原 IsRealTiDB 行为没有被篡改
			if IsRealTiDB(tc.version) != tc.isRealTiDB {
				t.Fatalf("expected IsRealTiDB(%q) == %v, got %v", tc.version, tc.isRealTiDB, IsRealTiDB(tc.version))
			}
		})
	}
}

// --- 完整的 fake driver 基础设施与 runSafeStructureWithDB 深度流程测试 ---

// safeStructureFakeFixture 记录 runSafeStructureWithDB 完整“guard -> strict 65 CREATE -> metadata PK/FK -> allocator -> ASCII -> owned-only cleanup”中的执行情况。
type safeStructureFakeFixture struct {
	createdTables []string
	droppedTables []string
	seedExecs     []string
}

func newSafeStructureFakeFixture() *safeStructureFakeFixture {
	return &safeStructureFakeFixture{}
}

// TestVerify_SafeStructureFakeFlow 验证标准全流程正常通过：
// 涵盖 guard -> strict 65 CREATE -> VerifyBaseTableCount 65 -> InfraSeed -> Layer 1 PK 65张表 -> Layer 2 FK/CHECK 0 -> Layer 3 自增步长 probe -> Layer 4 ASCII 碰撞 probe -> 逆序 DROP 65 张表。
func TestVerify_SafeStructureFakeFlow(t *testing.T) {
	fixture := newSafeStructureFakeFixture()

	var showCount int
	var insertedLocksMarkers []string
	var deletedLocksMarkers []string

	// 自定义 SHOW NEXT_ROW_ID 的 query handler 使其第一次返回 100，第二次返回 102
	db, fc := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "CREATE TABLE `") {
			tbl, _ := extractTableNameFromDDL(query)
			fixture.createdTables = append(fixture.createdTables, tbl)
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "DROP TABLE `") {
			tbl := strings.TrimPrefix(query, "DROP TABLE `")
			tbl = strings.TrimSuffix(tbl, "`")
			fixture.droppedTables = append(fixture.droppedTables, tbl)
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "INSERT IGNORE INTO `locks`") || strings.HasPrefix(query, "INSERT IGNORE INTO `schema_meta`") {
			fixture.seedExecs = append(fixture.seedExecs, query)
			return driver.ResultNoRows, nil
		}

		if strings.HasPrefix(query, "INSERT INTO `locks` (`name`) VALUES") {
			if len(args) != 2 {
				return nil, fmt.Errorf("expected 2 insert args for locks marker, got %d", len(args))
			}
			for i, arg := range args {
				s, ok := arg.Value.(string)
				if !ok {
					return nil, fmt.Errorf("locks insert arg[%d] not string: %T", i, arg.Value)
				}
				if len(s) > 64 {
					return nil, fmt.Errorf("locks marker length exceeds 64: %d", len(s))
				}
				insertedLocksMarkers = append(insertedLocksMarkers, s)
			}
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "DELETE FROM `locks` WHERE `name` IN") {
			if len(args) != 2 {
				return nil, fmt.Errorf("expected 2 delete args for locks marker, got %d", len(args))
			}
			for i, arg := range args {
				s, ok := arg.Value.(string)
				if !ok {
					return nil, fmt.Errorf("locks delete arg[%d] not string: %T", i, arg.Value)
				}
				deletedLocksMarkers = append(deletedLocksMarkers, s)
			}
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "INSERT INTO `schema_meta` (`key`, `value`, `updated_at`)") {
			return driver.ResultNoRows, nil
		}
		if strings.HasPrefix(query, "DELETE FROM `schema_meta` WHERE `key` IN") {
			return driver.ResultNoRows, nil
		}
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.Contains(query, "SELECT VERSION()") {
			return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.5.8"), nil
		}
		if strings.Contains(query, "SELECT DATABASE()") {
			return newSingleValueRows("DATABASE()", "artex_test"), nil
		}
		if strings.Contains(query, "TABLE_SCHEMA = DATABASE()") && strings.Contains(query, "COUNT(*)") && !strings.Contains(query, "BASE TABLE") && !strings.Contains(query, "TABLE_CONSTRAINTS") {
			return newSingleValueRows("COUNT(*)", int64(0)), nil
		}
		if strings.Contains(query, "TABLE_TYPE = 'BASE TABLE'") {
			return newSingleValueRows("COUNT(*)", int64(65)), nil
		}
		if strings.Contains(query, "SELECT name FROM locks WHERE name = 'business_relations'") {
			return newSingleValueRows("name", "business_relations"), nil
		}
		if strings.Contains(query, "SELECT `key` FROM schema_meta WHERE `key` = 'schema_version'") {
			return newSingleValueRows("key", "schema_version"), nil
		}
		if strings.Contains(query, "KEY_COLUMN_USAGE") && strings.Contains(query, "CONSTRAINT_NAME = 'PRIMARY'") {
			tblName := fmt.Sprint(args[0].Value)
			for _, tDef := range AllTables() {
				if tDef.Name == tblName {
					return &harnessFakeRows{
						columns: []string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
						rows: [][]driver.Value{
							{tDef.PKColumn, "bigint", "bigint unsigned"},
						},
					}, nil
				}
			}
			return nil, fmt.Errorf("unknown table %s", tblName)
		}
		if strings.Contains(query, "TABLE_CONSTRAINTS") && strings.Contains(query, "FOREIGN KEY") {
			return newSingleValueRows("COUNT(*)", int64(0)), nil
		}
		if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
			showCount++
			val := uint64(100)
			if showCount > 1 {
				val = 102
			}
			return &harnessFakeRows{
				columns: []string{"NEXT_GLOBAL_ROW_ID", "ID_TYPE", "TABLE_NAME", "COLUMN_NAME"},
				rows: [][]driver.Value{
					{val, "AUTO_INCREMENT", "locks", "row_id"},
				},
			}, nil
		}
		return nil, fmt.Errorf("unexpected query: %s", query)
	})

	ctx := context.Background()
	err := runSafeStructureWithDB(ctx, db)
	if err != nil {
		t.Fatalf("expected runSafeStructureWithDB to succeed, got: %v", err)
	}

	// 验证严格 65 张表创建及其与 AllTables 前 65 顺序完全一致 (F16)
	if len(fixture.createdTables) != 65 {
		t.Fatalf("expected 65 created tables, got %d", len(fixture.createdTables))
	}
	all := AllTables()
	for i := 0; i < 65; i++ {
		if fixture.createdTables[i] != all[i].Name {
			t.Fatalf("createdTables[%d] = %q, want %q (exact AllTables order)", i, fixture.createdTables[i], all[i].Name)
		}
	}

	// 验证 seedExecs 逐条与 PrepareInfraSQL() 两条原文严格一致 (F16)
	wantSeeds := PrepareInfraSQL()
	if len(fixture.seedExecs) != len(wantSeeds) {
		t.Fatalf("expected %d seed statements, got %d", len(wantSeeds), len(fixture.seedExecs))
	}
	for i := range wantSeeds {
		if fixture.seedExecs[i] != wantSeeds[i] {
			t.Fatalf("seedExecs[%d] = %q, want %q", i, fixture.seedExecs[i], wantSeeds[i])
		}
	}

	// 验证 locks probe markers 存在、非空、成对清理 (F16)
	if len(insertedLocksMarkers) != 2 || len(deletedLocksMarkers) != 2 {
		t.Fatalf("locks markers count mismatch: inserted=%d, deleted=%d", len(insertedLocksMarkers), len(deletedLocksMarkers))
	}
	if insertedLocksMarkers[0] == insertedLocksMarkers[1] {
		t.Errorf("locks markers must be distinct: %v", insertedLocksMarkers)
	}
	if deletedLocksMarkers[0] != insertedLocksMarkers[0] || deletedLocksMarkers[1] != insertedLocksMarkers[1] {
		t.Errorf("deleted locks markers %v mismatch inserted %v", deletedLocksMarkers, insertedLocksMarkers)
	}

	// 验证逆序删除 65 张表
	if len(fixture.droppedTables) != 65 {
		t.Fatalf("expected 65 dropped tables on cleanup, got %d", len(fixture.droppedTables))
	}
	for i := 0; i < 65; i++ {
		wantTable := all[64-i].Name
		if fixture.droppedTables[i] != wantTable {
			t.Fatalf("dropped[%d] = %q, want %q (reverse order)", i, fixture.droppedTables[i], wantTable)
		}
	}

	// 验证没有未处理的未知 Exec / Query
	_ = fc
}

// TestVerify_SafeStructureFakeFlow_RejectionCases 覆盖各类失败场景：
//  1. 8.5 Strict Guard 失败（如版本为 6.5 或 8.4）：0 DDL / 0 Seed / 0 DROP；
//  2. CheckGuards 非空 schema 失败：0 DDL / 0 Seed / 0 DROP；
//  3. Layer 1 PK 结构失败：返回错误，并触发逆序清理全部已 owned 表（65 张）；
//  4. Probe 失败与 Cleanup 失败双错链（errors.Join）可见；
//  5. 创建中途失败（第 2 张表冲突）：只删第 1 张已 owned 表，绝不误删第 2 张失败表。
func TestVerify_SafeStructureFakeFlow_RejectionCases(t *testing.T) {
	// Case 1: 8.5 strict guard 拦截非 8.5 版本（如 8.4）
	t.Run("non_8.5_version_zero_DDL_zero_DROP", func(t *testing.T) {
		var execCalled bool
		db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			execCalled = true
			return nil, fmt.Errorf("unexpected exec: %s", query)
		}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			if strings.Contains(query, "SELECT VERSION()") {
				return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.4.0"), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		})

		err := runSafeStructureWithDB(context.Background(), db)
		if err == nil {
			t.Fatal("expected error on TiDB 8.4 version, got nil")
		}
		if !strings.Contains(err.Error(), "strict 8.5 guard rejected") {
			t.Fatalf("unexpected error message: %v", err)
		}
		if execCalled {
			t.Fatal("expected 0 Exec calls on version guard rejection")
		}
	})

	// Case 2: 非空 schema 拦截：0 DDL / 0 Seed / 0 DROP
	t.Run("non_empty_schema_zero_DDL_zero_DROP", func(t *testing.T) {
		var execCalled bool
		db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			execCalled = true
			return nil, fmt.Errorf("unexpected exec: %s", query)
		}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			if strings.Contains(query, "SELECT VERSION()") {
				return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.5.8"), nil
			}
			if strings.Contains(query, "SELECT DATABASE()") {
				return newSingleValueRows("DATABASE()", "artex_test"), nil
			}
			if strings.Contains(query, "TABLE_SCHEMA = DATABASE()") && strings.Contains(query, "COUNT(*)") {
				return newSingleValueRows("COUNT(*)", int64(12)), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		})

		err := runSafeStructureWithDB(context.Background(), db)
		if err == nil {
			t.Fatal("expected error on non-empty schema, got nil")
		}
		if !strings.Contains(err.Error(), "not empty, existing object count: 12") {
			t.Fatalf("unexpected error message: %v", err)
		}
		if execCalled {
			t.Fatal("expected 0 Exec calls on non-empty schema rejection")
		}
	})

	// Case 3: Layer 1 PK 结构失败：返回错误并清理全部已 owned 65 张表
	t.Run("metadata_pk_error_cleans_up_all_owned", func(t *testing.T) {
		var droppedTables []string
		db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			if strings.HasPrefix(query, "CREATE TABLE `") {
				return driver.ResultNoRows, nil
			}
			if strings.HasPrefix(query, "DROP TABLE `") {
				tbl := strings.TrimPrefix(query, "DROP TABLE `")
				tbl = strings.TrimSuffix(tbl, "`")
				droppedTables = append(droppedTables, tbl)
				return driver.ResultNoRows, nil
			}
			if strings.HasPrefix(query, "INSERT IGNORE INTO `locks`") || strings.HasPrefix(query, "INSERT IGNORE INTO `schema_meta`") {
				return driver.ResultNoRows, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			if strings.Contains(query, "SELECT VERSION()") {
				return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.5.8"), nil
			}
			if strings.Contains(query, "SELECT DATABASE()") {
				return newSingleValueRows("DATABASE()", "artex_test"), nil
			}
			if strings.Contains(query, "TABLE_SCHEMA = DATABASE()") && strings.Contains(query, "COUNT(*)") && !strings.Contains(query, "BASE TABLE") && !strings.Contains(query, "TABLE_CONSTRAINTS") {
				return newSingleValueRows("COUNT(*)", int64(0)), nil
			}
			if strings.Contains(query, "TABLE_TYPE = 'BASE TABLE'") {
				return newSingleValueRows("COUNT(*)", int64(65)), nil
			}
			if strings.Contains(query, "SELECT name FROM locks WHERE name = 'business_relations'") {
				return newSingleValueRows("name", "business_relations"), nil
			}
			if strings.Contains(query, "SELECT `key` FROM schema_meta WHERE `key` = 'schema_version'") {
				return newSingleValueRows("key", "schema_version"), nil
			}
			if strings.Contains(query, "KEY_COLUMN_USAGE") && strings.Contains(query, "CONSTRAINT_NAME = 'PRIMARY'") {
				// 模拟 PK 校验失败：PK 列类型错误 (int 而非 bigint)
				_ = fmt.Sprint(args[0].Value)
				return &harnessFakeRows{
					columns: []string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
					rows: [][]driver.Value{
						{"id", "int", "int unsigned"},
					},
				}, nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		})

		err := runSafeStructureWithDB(context.Background(), db)
		if err == nil {
			t.Fatal("expected error on PK metadata mismatch, got nil")
		}
		if !strings.Contains(err.Error(), "VerifyStructure failed") {
			t.Fatalf("unexpected error message: %v", err)
		}
		// 验证即使结构失败，defer cleanup 依然被触发并逆序清理全部 65 张 owned 表
		if len(droppedTables) != 65 {
			t.Fatalf("expected 65 tables to be dropped on metadata failure, got %d", len(droppedTables))
		}
	})

	// Case 4: Layer 1 PK 元数据查询失败与 Cleanup 失败双错链（errors.Join 保留两者，F17 准确更名）
	t.Run("metadata_error_and_cleanup_error_dual_chain", func(t *testing.T) {
		sentinelCleanupErr := errors.New("drop failed: disk full")
		db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			if strings.HasPrefix(query, "CREATE TABLE `") {
				return driver.ResultNoRows, nil
			}
			if strings.HasPrefix(query, "DROP TABLE `") {
				return nil, sentinelCleanupErr
			}
			if strings.HasPrefix(query, "INSERT IGNORE INTO `locks`") || strings.HasPrefix(query, "INSERT IGNORE INTO `schema_meta`") {
				return driver.ResultNoRows, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			if strings.Contains(query, "SELECT VERSION()") {
				return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.5.8"), nil
			}
			if strings.Contains(query, "SELECT DATABASE()") {
				return newSingleValueRows("DATABASE()", "artex_test"), nil
			}
			if strings.Contains(query, "TABLE_SCHEMA = DATABASE()") && strings.Contains(query, "COUNT(*)") && !strings.Contains(query, "BASE TABLE") && !strings.Contains(query, "TABLE_CONSTRAINTS") {
				return newSingleValueRows("COUNT(*)", int64(0)), nil
			}
			if strings.Contains(query, "TABLE_TYPE = 'BASE TABLE'") {
				return newSingleValueRows("COUNT(*)", int64(65)), nil
			}
			if strings.Contains(query, "SELECT name FROM locks WHERE name = 'business_relations'") {
				return newSingleValueRows("name", "business_relations"), nil
			}
			if strings.Contains(query, "SELECT `key` FROM schema_meta WHERE `key` = 'schema_version'") {
				return newSingleValueRows("key", "schema_version"), nil
			}
			if strings.Contains(query, "KEY_COLUMN_USAGE") && strings.Contains(query, "CONSTRAINT_NAME = 'PRIMARY'") {
				return nil, errors.New("metadata query I/O broken")
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		})

		err := runSafeStructureWithDB(context.Background(), db)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		// 验证双错均存在于返回的 error 中
		if !strings.Contains(err.Error(), "metadata query I/O broken") {
			t.Fatalf("expected main probe error to be present, got: %v", err)
		}
		if !strings.Contains(err.Error(), "drop failed: disk full") {
			t.Fatalf("expected cleanup error to be present via errors.Join, got: %v", err)
		}
		if !errors.Is(err, sentinelCleanupErr) {
			t.Fatalf("expected errors.Is(err, sentinelCleanupErr) to be true, got: %v", err)
		}
	})

	// Case 5: 创建中途第 2 张表失败：只清理已登记为 owned 的第 1 张表，决不删除第 2 张未 owned 表
	t.Run("partial_creation_failure_only_drops_owned", func(t *testing.T) {
		all := AllTables()
		firstTable := all[0].Name
		secondTable := all[1].Name
		var droppedTables []string

		db, _ := newHarnessTestDB(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			if strings.HasPrefix(query, "CREATE TABLE `") {
				if strings.Contains(query, "`"+secondTable+"`") {
					return nil, errors.New("Error 1050: Table already exists")
				}
				return driver.ResultNoRows, nil
			}
			if strings.HasPrefix(query, "DROP TABLE `") {
				tbl := strings.TrimPrefix(query, "DROP TABLE `")
				tbl = strings.TrimSuffix(tbl, "`")
				droppedTables = append(droppedTables, tbl)
				return driver.ResultNoRows, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		}, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			if strings.Contains(query, "SELECT VERSION()") {
				return newSingleValueRows("VERSION()", "8.0.11-TiDB-v8.5.8"), nil
			}
			if strings.Contains(query, "SELECT DATABASE()") {
				return newSingleValueRows("DATABASE()", "artex_test"), nil
			}
			if strings.Contains(query, "TABLE_SCHEMA = DATABASE()") && strings.Contains(query, "COUNT(*)") {
				return newSingleValueRows("COUNT(*)", int64(0)), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		})

		err := runSafeStructureWithDB(context.Background(), db)
		if err == nil {
			t.Fatal("expected creation error on 2nd table, got nil")
		}
		if !strings.Contains(err.Error(), "CreateAllOwnedTables failed") {
			t.Fatalf("unexpected error message: %v", err)
		}
		// 验证仅清理第 1 张表
		if len(droppedTables) != 1 || droppedTables[0] != firstTable {
			t.Fatalf("expected only [%s] to be dropped, got: %v", firstTable, droppedTables)
		}
	})
}
