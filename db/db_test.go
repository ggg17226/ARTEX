package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// testDSN 返回集成测试使用的 TiDB 连接串。
//
// 语义（spec §4.3）：
//   - 未设置 ARTEX_TIDB_DSN → 显式 t.Skip 声明"本用例未执行"，绝不当作通过；
//   - 已设置 → 用例必须真实连库执行并 PASS；连不上/基线缺失都属于环境违规，
//     由调用方 t.Fatal 判红，禁止再退回任何形式的无条件 t.Skip。
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("ARTEX_TIDB_DSN"))
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set — skipping TiDB integration test")
	}
	return dsn
}

// openTestDB 是集成测试唯一的"打开真实 TiDB"入口（审查 M-02 / I-05）。
//
//   - DSN 缺失：testDSN 已经诚实 t.Skip，这里不会看到空 DSN；
//   - DSN 已配置却打不开：属于环境违规，必须 t.Fatal 判红。绝不再退回
//     `Open` 失败 → t.Skip 的假绿路径（那会让"基线缺失/连不上"整批用例静默跳过）；
//   - 连接生命周期：Close 在**本函数内**登记为 t.Cleanup。因为是测试里最先登记的
//     一条，它按 LIFO 最后执行 → 用例随后登记的任何清理都跑在还活着的连接池上。
//     用例**不得**再写 `defer d.Close()`（审查 I-03/I-05：defer 先于 t.Cleanup 执行，
//     清理会在已关闭的池上静默失败并留下孤儿行）。
func openTestDB(t *testing.T) *DB {
	t.Helper()
	return openTestDBWithDSN(t, testDSN(t))
}

// openTestDBWithDSN 与 openTestDB 完全同一口径，只是 DSN 由调用方拿着：
// 锁观测类用例还要把同一个 DSN 再开成"单连接池"的独立会话。
func openTestDBWithDSN(t *testing.T, dsn string) *DB {
	t.Helper()
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("ARTEX_TIDB_DSN 已配置但打开 TiDB 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("关闭 TiDB 连接池失败: %v", err)
		}
	})
	return d
}

// mustCleanup 执行一条 fixture 清理语句并检查错误（审查 I-05：清理失败不许吞错）。
// 共享库里的孤儿行会把下一轮用例踩中，也让人无法用"跑完查库"证明终态干净。
func mustCleanup(t *testing.T, d *DB, query string, args ...any) {
	t.Helper()
	if _, err := d.Exec(query, args...); err != nil {
		t.Errorf("清理 fixture 失败: %q args=%v: %v", query, args, err)
	}
}

// cleanupExploration 逆拓扑删掉一次 exploration fixture（审查 R-02：完整逆拓扑）。
//
// TiDB 基线不建外键（不会级联删除）：只删 explorations 父行，会把 exploration_edges /
// exploration_anchors / activity / exploration_nodes 整批留在库里指向一条已经不存在的
// exploration（审查 I-05：activity_page_test.go 原来正是这样，且吞掉了错误）。
// 完整逆拓扑：exploration_edges + exploration_anchors → activity → exploration_nodes
// → explorations。exploration_anchors 没有 exploration_id 列，只能经 exploration_nodes
// JOIN 按归属删除（与产品侧 task_archives.go 的同表清理同一口径）。
// 每一步都经 mustCleanup 判红，清理失败绝不再静默。
func cleanupExploration(t *testing.T, d *DB, expID int64) {
	t.Helper()
	mustCleanup(t, d, `DELETE FROM exploration_edges WHERE exploration_id = ?`, expID)
	mustCleanup(t, d, `DELETE anchor FROM exploration_anchors anchor
 JOIN exploration_nodes node ON node.id = anchor.node_id
 WHERE node.exploration_id = ?`, expID)
	mustCleanup(t, d, `DELETE FROM activity WHERE exploration_id = ?`, expID)
	mustCleanup(t, d, `DELETE FROM exploration_nodes WHERE exploration_id = ?`, expID)
	mustCleanup(t, d, `DELETE FROM explorations WHERE id = ?`, expID)
}

// cleanupTask 用产品级级联删除（DeleteTask → DeleteTaskCascade）清掉一个任务 fixture
// 并判红（审查 R-02-R：清理失败不许吞错、不许只删父行）。
//
// DeleteTask 保留共享资产（deleteAssets=false），但在同一事务里把任务与它的
// exploration 子图（exploration_nodes/edges/anchors、activity、task_scope、
// task_asset_links 等）按逆拓扑物理删净，并带防悬空断言——比测试手写的
// `DELETE FROM tasks/explorations` 两句更完整，也天然免疫漏表。
func cleanupTask(t *testing.T, d *DB, taskID int64) {
	t.Helper()
	if err := d.DeleteTask(taskID); err != nil {
		t.Errorf("清理任务 fixture %d 失败: %v", taskID, err)
	}
}

// TestOpenSeed 打开真实 TiDB：连接池 + UTC 探测 + 只读基线校验 + seed 必须全部走通，
// 并且重复 Open 幂等（内置 agent 不会被插成两份）。
func TestOpenSeed(t *testing.T) {
	d := openTestDB(t) // Close 由 openTestDB 统一登记

	wantAgents := len(builtinAgents)
	agents := countRows(t, d, `SELECT COUNT(*) FROM agents WHERE builtin`)
	if agents != wantAgents {
		t.Fatalf("want %d builtin agents, got %d", wantAgents, agents)
	}

	// planner 至少要有 seed 进去的变量目录
	wantPlannerVars := 0
	for _, a := range builtinAgents {
		if a.key == "planner" {
			wantPlannerVars = len(a.vars)
			break
		}
	}
	plannerVars := countRows(t, d, `
SELECT COUNT(*) FROM agent_prompt_vars v
JOIN agents a ON a.id = v.agent_id
WHERE a.`+"`key`"+` = 'planner'`)
	if plannerVars < wantPlannerVars {
		t.Fatalf("want at least %d planner vars, got %d", wantPlannerVars, plannerVars)
	}

	// seed 写入的 hash 身份列必须非空：缺列时唯一键会退化成 NULL，
	// 第二次 Open 就再也挡不住重复行了（这正是 M07 的根因）。
	if bad := countRows(t, d, `
SELECT COUNT(*) FROM agents WHERE builtin AND (key_identity IS NULL OR key_hash IS NULL)`); bad != 0 {
		t.Fatalf("%d builtin agents are missing key_identity/key_hash", bad)
	}
	if bad := countRows(t, d, `
SELECT COUNT(*) FROM agent_prompt_vars WHERE identity IS NULL OR identity_hash IS NULL`); bad != 0 {
		t.Fatalf("%d prompt vars are missing identity/identity_hash", bad)
	}

	// 重开一次必须幂等（不会插出第二份内置 agent）。
	d2, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d2.Close(); err != nil {
			t.Errorf("close second DB failed: %v", err)
		}
	})
	if agents = countRows(t, d2, `SELECT COUNT(*) FROM agents WHERE builtin`); agents != wantAgents {
		t.Fatalf("after re-open want %d agents, got %d", wantAgents, agents)
	}
}

func countRows(t *testing.T, d *DB, query string) int {
	t.Helper()
	var n int
	if err := d.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// ---- 离线方言门禁（M01 / M07 / M08 / M11）----
//
// 阶段 1 的五个产品文件必须在没有数据库的情况下就能证明"不含 PG 方言"：
// 占位符、upsert 子句、RETURNING、类型转换、驱动符号都按静态特征判红，
// 免得只有连上真实 TiDB 才会暴露的方言残留漏到运行时。
var phaseOneFiles = []string{"db.go", "settings.go", "config.go", "asset_intercept.go", "intercept.go"}

// pgDialectMarkers 是 PG 专有写法及其判定原因。
var pgDialectMarkers = []struct {
	why     string
	pattern *regexp.Regexp
}{
	{"PostgreSQL positional parameter", regexp.MustCompile(`\$[0-9]`)},
	{"PostgreSQL upsert clause", regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)},
	{"PostgreSQL RETURNING clause", regexp.MustCompile(`(?i)\bRETURNING\b`)},
	{"PostgreSQL EXCLUDED reference", regexp.MustCompile(`(?i)\bEXCLUDED\s*\.`)},
	{"PostgreSQL type cast", regexp.MustCompile(`::[a-zA-Z]`)},
	{"PostgreSQL advisory lock", regexp.MustCompile(`(?i)\bpg_advisory`)},
	{"PostgreSQL jsonb operator/function", regexp.MustCompile(`(?i)\bjsonb|->>|#>>|#>`)},
	{"pgx driver symbol", regexp.MustCompile(`(?i)\bpgconn\b|\bjackc/`)},
	{"embedded schema.sql apply", regexp.MustCompile(`schema\.sql|go:embed`)},
	{"PostgreSQL now() session function", regexp.MustCompile(`(?i)\bnow\(\s*\)`)},
}

func TestPhaseOneFilesAreTiDBDialect(t *testing.T) {
	for _, name := range phaseOneFiles {
		src := stripGoComments(t, name)
		for _, marker := range pgDialectMarkers {
			if loc := marker.pattern.FindString(src); loc != "" {
				t.Errorf("%s: PostgreSQL dialect %q (%s)", name, loc, marker.why)
			}
		}
	}
}

// TestPhaseOneSeedWritesIdentityColumns 保证 hash 唯一键表的写入都补齐了身份列，
// 而不是只把 ON CONFLICT 换个关键字（M07/M08 的原始症状）。
func TestPhaseOneSeedWritesIdentityColumns(t *testing.T) {
	type want struct {
		file string
		syms []string
	}
	for _, w := range []want{
		{"db.go", []string{"key_identity", "key_hash", "identity_hash", "name_identity", "name_hash"}},
		{"settings.go", []string{"key_identity", "key_hash"}},
		{"config.go", []string{"key_identity", "key_hash", "name_identity", "name_hash", "identity_hash"}},
	} {
		src := stripGoComments(t, w.file)
		for _, sym := range w.syms {
			if !strings.Contains(src, sym) {
				t.Errorf("%s: identity column %q is never written", w.file, sym)
			}
		}
	}
}

// TestSetSettingUsesNativeUpsert 固定 settings 写入的方言形状：
// ON DUPLICATE KEY UPDATE + `key`/`value` 反引号 + 身份列，且绝不回退到 ON CONFLICT。
func TestSetSettingUsesNativeUpsert(t *testing.T) {
	src := stripGoComments(t, "settings.go")
	for _, want := range []string{"ON DUPLICATE KEY UPDATE", "`key`", "`value`", "key_identity", "key_hash"} {
		if !strings.Contains(src, want) {
			t.Errorf("settings.go must contain %q", want)
		}
	}
	if strings.Contains(src, "INSERT IGNORE") {
		t.Errorf("settings.go must not swallow write errors with INSERT IGNORE")
	}
}

// stripGoComments 读出包内文件并抹掉注释（保留字符串字面量），
// 让方言扫描只看真正会发给数据库的文本。
func stripGoComments(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		c := raw[i]
		switch {
		case c == '/' && i+1 < len(raw) && raw[i+1] == '/':
			for i < len(raw) && raw[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(raw) && raw[i+1] == '*':
			end := strings.Index(string(raw[i+2:]), "*/")
			if end < 0 {
				i = len(raw)
				continue
			}
			i += 2 + end + 2
		case c == '`':
			start := i
			i++
			for i < len(raw) && raw[i] != '`' {
				i++
			}
			if i < len(raw) {
				i++
			}
			out = append(out, raw[start:i]...)
		case c == '"':
			start := i
			i++
			for i < len(raw) && raw[i] != '"' {
				if raw[i] == '\\' && i+1 < len(raw) {
					i++
				}
				i++
			}
			if i < len(raw) {
				i++
			}
			out = append(out, raw[start:i]...)
		case c == '\'':
			start := i
			i++
			for i < len(raw) && raw[i] != '\'' {
				if raw[i] == '\\' && i+1 < len(raw) {
					i++
				}
				i++
			}
			if i < len(raw) {
				i++
			}
			out = append(out, raw[start:i]...)
		default:
			out = append(out, c)
			i++
		}
	}
	return string(out)
}

// TestPhaseOneDialectMarkersDetectPGSamples 自检方言扫描器本身不是空转：
// 每个 PG 特征都必须能在样例片段上命中（否则 TestPhaseOneFilesAreTiDBDialect
// 的绿就只是"什么都没查"）。
func TestPhaseOneDialectMarkersDetectPGSamples(t *testing.T) {
	samples := map[string]string{
		"PostgreSQL positional parameter":    "SELECT value FROM settings WHERE key=$1",
		"PostgreSQL upsert clause":           "INSERT INTO settings(key) VALUES ($1) ON CONFLICT (key) DO NOTHING",
		"PostgreSQL RETURNING clause":        "INSERT INTO agents(key) VALUES ($1) RETURNING id",
		"PostgreSQL EXCLUDED reference":      "ON CONFLICT DO UPDATE SET value = EXCLUDED.value",
		"PostgreSQL type cast":               "SELECT 'task'::text AS ref_kind",
		"PostgreSQL advisory lock":           "SELECT pg_advisory_lock($1)",
		"PostgreSQL jsonb operator/function": "SELECT jsonb_array_elements_text(agents) FROM tools",
		"pgx driver symbol":                  "var pgErr *pgconn.PgError",
		"embedded schema.sql apply":          "//go:embed schema.sql",
		"PostgreSQL now() session function":  "updated_at = now()",
	}
	for _, marker := range pgDialectMarkers {
		sample, ok := samples[marker.why]
		if !ok {
			t.Fatalf("marker %q has no self-test sample", marker.why)
		}
		if !marker.pattern.MatchString(sample) {
			t.Errorf("marker %q does not detect its own PG sample %q", marker.why, sample)
		}
	}
}
