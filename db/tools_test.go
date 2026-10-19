package db

// =====================================================================
// P08-03 Task 3.3 门禁：命令、工具、LLM 使用与日志模块（M12 / M21）
//
// 本机没有 TiDB 实例，因此本文件的判红主体是**离线静态门禁**（源码级），
// 真机集成用例在没有 ARTEX_TIDB_DSN 时显式 t.Skip 声明"未执行"，绝不当作通过。
// 离线门禁覆盖三件事：
//  1. 8 个文件里不残留任何 PostgreSQL 方言（通用 pgDialectMarkers + 本任务专属特征）；
//  2. M12：运行期 DDL 归零（无 CREATE TABLE / ALTER TABLE / CREATE INDEX / ADD COLUMN），
//     两个 Ensure 旧 DDL 入口只剩恒返回 nil 的兼容空壳；
//  3. tools 表的 hash DDL 契约（key_identity / key_hash）、MySQL 关键字反引号、
//     以及"每个静态 Exec/Query 的 `?` 与绑定参数严格同数量同顺序"。
// =====================================================================

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

// task33Files 是 Task 3.3 改造清单里的 8 个业务文件（相对 db 包目录）。
var task33Files = []string{
	"commands.go", "tools.go", "llm_usage.go", "llmhealth.go",
	"logs.go", "digest.go", "intent_admission.go", "tool_usage.go",
}

// task33DialectMarkers 是本任务专属的 PG 特征。每条都配一个"必须命中它"的样例，
// 由 TestTools_Task33DialectMarkersDetectOwnSamples 自检，防止扫描器空转。
var task33DialectMarkers = []struct {
	why     string
	pattern *regexp.Regexp
	sample  string
}{
	{"PostgreSQL case-insensitive LIKE", regexp.MustCompile(`(?i)\bILIKE\b`), `WHERE u.tool ILIKE $1`},
	{"PostgreSQL to_char formatting", regexp.MustCompile(`(?i)\bto_char\s*\(`), `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD')`},
	{"PostgreSQL AT TIME ZONE", regexp.MustCompile(`(?i)\bAT\s+TIME\s+ZONE\b`), `ts AT TIME ZONE 'UTC'`},
	{"PostgreSQL interval arithmetic", regexp.MustCompile(`(?i)\binterval\s+'`), `now() - ($1 * interval '1 day')`},
	{"PostgreSQL date_trunc", regexp.MustCompile(`(?i)\bDATE_TRUNC\s*\(`), `DATE_TRUNC('day', created_at)`},
	{"PostgreSQL extract", regexp.MustCompile(`(?i)\bEXTRACT\s*\(`), `EXTRACT(DAY FROM created_at)`},
	{"PostgreSQL filtered aggregate", regexp.MustCompile(`(?i)\bFILTER\s*\(\s*WHERE\b`), `COUNT(*) FILTER (WHERE r.is_error)`},
	{"PostgreSQL jsonb concat/remove/membership", regexp.MustCompile(`(?i)to_jsonb\s*\(|jsonb_set\s*\(|agents\s*\?\s|\bagents\s*-\s`), `SET agents = agents || to_jsonb($1::text)`},
	{"runtime table DDL", regexp.MustCompile(`(?i)\bCREATE\s+TABLE\b|\bALTER\s+TABLE\b|\bCREATE\s+(UNIQUE\s+)?INDEX\b|\bADD\s+COLUMN\b`), `CREATE TABLE IF NOT EXISTS llm_usage (id BIGSERIAL)`},
	{"PostgreSQL session now()", regexp.MustCompile(`(?i)\bnow\s*\(\s*\)`), `WHERE open_until > now()`},
}

// task33Source 读出包内某个文件的源码并抹掉注释（保留字符串字面量）。
// 唯一豁免：Go 侧的 time.Now( 会被 now() 特征命中，但它是宿主语言取时函数，
// 不是发给数据库的 SQL now()，因此统一改名后再扫。
func task33Source(t *testing.T, name string) string {
	t.Helper()
	return strings.ReplaceAll(stripGoComments(t, name), "time.Now(", "timeNowEvaluated(")
}

// TestTools_Task33FilesAreTiDBDialect 是 Step 1 的核心判红用例：8 个文件必须同时
// 通过阶段 1 的通用方言门禁与本任务专属门禁。
func TestTools_Task33FilesAreTiDBDialect(t *testing.T) {
	for _, name := range task33Files {
		src := task33Source(t, name)
		for _, marker := range pgDialectMarkers {
			if loc := marker.pattern.FindString(src); loc != "" {
				t.Errorf("%s: PostgreSQL dialect %q (%s)", name, loc, marker.why)
			}
		}
		for _, marker := range task33DialectMarkers {
			if loc := marker.pattern.FindString(src); loc != "" {
				t.Errorf("%s: PostgreSQL dialect %q (%s)", name, loc, marker.why)
			}
		}
	}
}

// TestTools_Task33DialectMarkersDetectOwnSamples 自检专属扫描器不是空转。
func TestTools_Task33DialectMarkersDetectOwnSamples(t *testing.T) {
	for _, marker := range task33DialectMarkers {
		if !marker.pattern.MatchString(marker.sample) {
			t.Errorf("marker %q does not detect its own PG sample %q", marker.why, marker.sample)
		}
	}
}

// TestTools_NoRuntimeDDLLeft 钉死 M12：8 个文件里连一条 DDL 都不许剩下，
// 两个旧 DDL 入口只能是恒返回 nil 的空壳（幂等的运行期建表本身就是缺陷：
// 它会用一张与 65 表目录不一致的表把漂移掩盖成"启动成功"）。
func TestTools_NoRuntimeDDLLeft(t *testing.T) {
	ddl := regexp.MustCompile(`(?i)\bCREATE\s+TABLE\b|\bALTER\s+TABLE\b|\bCREATE\s+(UNIQUE\s+)?INDEX\b|\bADD\s+COLUMN\b|coordinateWithSchemaMigration|llmRecordsMigrate|llmRecordsSchema|llmUsageSchema`)
	for _, name := range task33Files {
		src := task33Source(t, name)
		if loc := ddl.FindString(src); loc != "" {
			t.Errorf("%s: still issues runtime DDL %q", name, loc)
		}
	}
	for _, decl := range []string{
		"func (d *DB) EnsureLLMRecordsTable() error",
		"func (d *DB) EnsureLLMUsageTable() error",
	} {
		file := "commands.go"
		if strings.Contains(decl, "Usage") {
			file = "llm_usage.go"
		}
		body := strings.ReplaceAll(task33FuncBody(t, file, decl), "\t", " ")
		if got := strings.Join(strings.Fields(body), " "); got != decl+" { return nil }" {
			t.Errorf("%s in %s must be an empty compatibility stub, got %s", decl, file, got)
		}
	}
}

// task33FuncBody 按声明前缀从源码里抠出一个完整函数体（花括号配平扫描）。
func task33FuncBody(t *testing.T, file, decl string) string {
	t.Helper()
	src := task33Source(t, file)
	start := strings.Index(src, decl)
	if start < 0 {
		t.Fatalf("%s: declaration %q not found", file, decl)
	}
	open := strings.Index(src[start:], "{")
	if open < 0 {
		t.Fatalf("%s: no body for %q", file, decl)
	}
	open += start
	depth := 0
	for i := open; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[start : i+1]
			}
		}
	}
	t.Fatalf("%s: unterminated body for %q", file, decl)
	return ""
}

// ---- tools 表的 hash DDL 与关键字转义 ----------------------------------

// TestTools_KeyIdentityMatchesCanonicalContract 钉死 key_identity/key_hash 的算法：
// 载荷必须是 schema.SettingsKeyIdentity 的 canonical 字节（tools/settings/
// scheduler_state 共用分支 "key"），摘要必须是它的 SHA-256。同键必同值、异键必异值，
// 否则 uq_tools_key_hash 要么失效要么误撞。
func TestTools_KeyIdentityMatchesCanonicalContract(t *testing.T) {
	key := "report_finding"
	payload, hash := toolKeyIdentity(key)
	wantPayload := schema.SettingsKeyIdentity(key)
	wantHash := sha256.Sum256(wantPayload)
	if string(payload) != string(wantPayload) {
		t.Errorf("key_identity payload mismatch:\n got %v\nwant %v", payload, wantPayload)
	}
	if string(hash) != string(wantHash[:]) {
		t.Errorf("key_hash must be SHA-256(payload): got %x want %x", hash, wantHash)
	}
	if len(hash) != 32 {
		t.Errorf("key_hash must be 32 bytes for BINARY(32), got %d", len(hash))
	}
	// 幂等 + 抗碰撞（含 MySQL 侧大小写不敏感的排序规则差异）
	p1, h1 := toolKeyIdentity("Auto")
	p2, h2 := toolKeyIdentity("auto")
	if string(h1) == string(h2) || string(p1) == string(p2) {
		t.Error("identity must be byte-exact: \"Auto\" and \"auto\" may not collide")
	}
	if p3, h3 := toolKeyIdentity("auto"); string(p3) != string(p2) || string(h3) != string(h2) {
		t.Error("toolKeyIdentity must be deterministic for the same key")
	}
	// 空串也必须有身份：漏写会让唯一键退化成 NULL
	if p, h := toolKeyIdentity(""); len(p) == 0 || len(h) != 32 {
		t.Error("empty key must still carry an identity payload and a 32-byte hash")
	}
}

// TestTools_InsertsAlwaysCarryIdentityColumns 要求 tools 的每一条 INSERT 都显式写
// key_identity 与 key_hash（漏写 = 主动放弃 uq_tools_key_hash）。
func TestTools_InsertsAlwaysCarryIdentityColumns(t *testing.T) {
	src := task33Source(t, "tools.go")
	stmts := task33Statements(t, "tools.go")
	var inserts int
	for _, s := range stmts {
		if !regexp.MustCompile(`(?i)INSERT\s+INTO\s+tools\b`).MatchString(s) {
			continue
		}
		inserts++
		for _, col := range []string{"key_identity", "key_hash"} {
			if !strings.Contains(s, col) {
				t.Errorf("INSERT INTO tools is missing identity column %q: %s", col, s)
			}
		}
	}
	// SeedTool / UpsertToolForce / CreateCustomTool 三条写入路径。
	if inserts != 3 {
		t.Errorf("INSERT INTO tools count = %d, want 3 (SeedTool, UpsertToolForce, CreateCustomTool)", inserts)
	}
	if !strings.Contains(src, "toolKeyIdentity(") {
		t.Error("tools.go must derive identity via toolKeyIdentity")
	}
}

// TestTools_KeywordColumnsAreBackticked 要求每一条碰 tools 的语句都用反引号写
// `key` / `schema` / `exec` / `system`：这几个名字都是 MySQL/TiDB 关键字，
// 裸写要么 1064 语法错误，要么被解析成别的东西（key = ? 会被当索引用语）。
func TestTools_KeywordColumnsAreBackticked(t *testing.T) {
	stmts := task33Statements(t, "tools.go")
	if len(stmts) == 0 {
		t.Fatal("no tools statements found — scanner is not looking at the right file")
	}
	bare := regexp.MustCompile("(?i)[^`\\w.](key|schema|exec|system)\\s*(=|,|\\))")
	checked := 0
	for _, s := range stmts {
		if !regexp.MustCompile(`(?i)\b(tools)\b`).MatchString(s) {
			continue
		}
		checked++
		for _, kw := range []string{"key", "schema", "exec", "system"} {
			if !strings.Contains(s, "`"+kw+"`") {
				continue
			}
		}
		if loc := bare.FindString(s); loc != "" {
			t.Errorf("statement uses an unquoted MySQL keyword column %q in: %s", strings.TrimSpace(loc), s)
		}
	}
	if checked < 10 {
		t.Errorf("only %d tools statements checked, want >= 10", checked)
	}
	for _, want := range []string{"`key`", "`schema`", "`exec`", "`system`", "`enabled`", "`kind`"} {
		if !strings.Contains(task33Source(t, "tools.go"), want) {
			t.Errorf("tools.go never quotes column %s", want)
		}
	}
}

// TestTools_DuplicateKeyIsHowSeedToolStaysIdempotent 固定首插入语义的落地形状：
// SeedTool 靠 1062 判重（不是 INSERT IGNORE，也不是 ON DUPLICATE KEY UPDATE），
// UpsertToolForce 才用原生 upsert。
func TestTools_DuplicateKeyIsHowSeedToolStaysIdempotent(t *testing.T) {
	seed := task33FuncBody(t, "tools.go", "func (d *DB) SeedTool(")
	if !strings.Contains(seed, "tidb.IsDuplicateKey(err)") {
		t.Error("SeedTool must treat a 1062 on uq_tools_key_hash as \"already seeded\"")
	}
	if strings.Contains(seed, "INSERT IGNORE") || strings.Contains(seed, "ON DUPLICATE KEY") {
		t.Error("SeedTool must not swallow other write errors with INSERT IGNORE / upsert")
	}
	force := task33FuncBody(t, "tools.go", "func (d *DB) UpsertToolForce(")
	if !strings.Contains(force, "ON DUPLICATE KEY UPDATE") {
		t.Error("UpsertToolForce must use TiDB native ON DUPLICATE KEY UPDATE")
	}
	if strings.Contains(force, "key_identity") && strings.Contains(force, "VALUES(key_identity)") {
		t.Error("immutable identity columns must not be rewritten on the update branch")
	}
}

// TestTools_AgentBindingEditsJSONInGoUnderRowLock 钉死 agents 数组的改造形状：
// 没有任何 jsonb 运算符，改动一律"行锁读 → Go 解析 → 写回"，非法 JSON fail closed。
func TestTools_AgentBindingEditsJSONInGoUnderRowLock(t *testing.T) {
	for _, decl := range []string{
		"func (d *DB) addToolAgentBinding(",
		"func (d *DB) RemoveAgentFromToolBindings(",
		"func (d *DB) RemoveAgentFromTool(",
	} {
		body := task33FuncBody(t, "tools.go", decl)
		if !strings.Contains(body, "FOR UPDATE") {
			t.Errorf("%s must read the row under FOR UPDATE before the read-modify-write", decl)
		}
		if !strings.Contains(body, "toolAgents(") {
			t.Errorf("%s must parse agents via toolAgents (fail closed on invalid JSON)", decl)
		}
	}
	src := task33Source(t, "tools.go")
	if loc := regexp.MustCompile(`(?i)to_jsonb|json_array_append|json_remove|jsonb_set`).FindString(src); loc != "" {
		t.Errorf("tools.go still leans on a JSON set function %q", loc)
	}
}

// ---- 纯函数单元测试 ---------------------------------------------------

func TestTools_AgentsArrayHelpers(t *testing.T) {
	items, err := toolAgents([]byte(`["worker","planner"]`), "t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !toolAgentsContains(items, "worker") || toolAgentsContains(items, "auto") {
		t.Errorf("membership wrong on %v", items)
	}
	// 幂等追加 + 保序删除
	if _, removed := toolAgentsWithout(items, "auto"); removed {
		t.Error("removing an absent agent must report removed=false (no empty UPDATE)")
	}
	next, removed := toolAgentsWithout(items, "worker")
	if !removed {
		t.Fatal("removing a present agent must report removed=true")
	}
	text, err := toolAgentsText(next)
	if err != nil {
		t.Fatal(err)
	}
	if text != `["planner"]` {
		t.Errorf("remaining array = %s, want [\"planner\"]", text)
	}
	// 内部空白不影响判定
	spaced, err := toolAgents([]byte(`[ "auto" , "worker" ]`), "t")
	if err != nil {
		t.Fatalf("parse spaced: %v", err)
	}
	if !toolAgentsContains(spaced, "auto") {
		t.Error("membership must ignore insignificant whitespace")
	}
	// 空集合恒写成 []
	empty, err := toolAgentsText(nil)
	if err != nil || empty != "[]" {
		t.Errorf("empty agents = %q, %v; want \"[]\"", empty, err)
	}
	// 非数组载荷必须 fail closed，绝不能被重写成空数组抹掉用户绑定
	if _, err := toolAgents([]byte(`{"agent":"worker"}`), "t"); err == nil {
		t.Error("object payload must be rejected instead of being rewritten")
	}
	if _, err := toolAgents([]byte(`not json`), "t"); err == nil {
		t.Error("invalid JSON payload must be rejected")
	}
	// 空列（NULL/空串）视为空数组，不报错
	if got, err := toolAgents(nil, "t"); err != nil || len(got) != 0 {
		t.Errorf("NULL agents = %v, %v; want empty", got, err)
	}
}

func TestTools_LikePatternEscapesMetacharacters(t *testing.T) {
	cases := []struct{ in, want string }{
		{"worker", `%"worker"%`},
		{"100%_done", `%"100\%\_done"%`},
		{`back\slash`, `%"back\\slash"%`},
		{`quote"inside`, `%"quote"inside"%`},
	}
	for _, c := range cases {
		if got := toolLikePattern(c.in); got != c.want {
			t.Errorf("toolLikePattern(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestTools_CommandLikePatternIsFoldedAndEscaped(t *testing.T) {
	if got := commandsLikePattern("SQL Injection"); got != "%sql injection%" {
		t.Errorf("commandsLikePattern = %q, want %%sql injection%% (lower-cased for LOWER(col) LIKE ?)", got)
	}
	if got := commandsLikePattern(`a%b_c\d`); got != `%a\%b\_c\\d%` {
		t.Errorf("commandsLikePattern escaping = %q, want %%a\\%%b\\_c\\\\d%%", got)
	}
}

// TestTools_CommandFilterPlaceholderAlignment 直接跑 commandFilter，按分支逐个核对
// 占位符与参数的数量与顺序（TiDB 的 `?` 没有编号，顺序错一个就串台）。
func TestTools_CommandFilterPlaceholderAlignment(t *testing.T) {
	expID := int64(42)
	cases := []struct {
		name     string
		expID    *int64
		q        string
		wantArgs int
	}{
		{"no filters", nil, "", 0},
		{"exp only", &expID, "", 1},
		{"query only", nil, "nmap", 2}, // tool 与 detail 两个谓词各绑一次
		{"both", &expID, "nmap", 3},
	}
	for _, c := range cases {
		where, args := commandFilter(c.expID, c.q)
		if got := strings.Count(where, "?"); got != c.wantArgs {
			t.Errorf("%s: %d placeholders but %d args; clause=%s", c.name, got, len(args), where)
		}
		if len(args) != c.wantArgs {
			t.Errorf("%s: args = %d, want %d", c.name, len(args), c.wantArgs)
		}
	}
	// 同一模式必须绑两遍：只绑一遍会让第二个谓词静默拿到第一个值/空集合。
	where, args := commandFilter(nil, "x")
	if len(args) != 2 || args[0] != args[1] {
		t.Errorf("both LIKE predicates need their own bound copy, got %v for %s", args, where)
	}
}

// ---- 8 个文件的 `?` 与绑定参数逐一相等 --------------------------------

// task33Call 是一次静态可判定 Exec/Query 调用的形状。
type task33Call struct {
	file        string
	stmt        string
	marks       int
	args        int
	dynamicSkip bool
}

// task33Statements 抽出文件里所有会发给数据库的语句文本（已把 toolKeyCol/toolCols/
// nodeCols 常量还原成字面量），供"必须反引号"“必须写身份列"之类的形状断言使用。
func task33Statements(t *testing.T, file string) []string {
	t.Helper()
	src := task33ExpandToolCols(t, task33Source(t, file))
	var out []string
	for _, m := range regexp.MustCompile(`\.(Exec|Query|QueryRow|ExecContext|QueryContext|QueryRowContext)\(`).FindAllStringIndex(src, -1) {
		open := m[1] - 1
		body, ok := task33Balanced(src, open)
		if !ok {
			continue
		}
		parts := task33SplitArgs(body)
		if len(parts) == 0 {
			continue
		}
		out = append(out, task33LiteralText(strings.Join(parts, " ")))
	}
	return out
}

// task33Calls 核对每一次静态可判定调用的 `?` 数量与绑定参数数量。
func task33Calls(t *testing.T, file string) []task33Call {
	t.Helper()
	src := task33ExpandToolCols(t, task33Source(t, file))
	var out []task33Call
	for _, m := range regexp.MustCompile(`\.(Exec|Query|QueryRow|ExecContext|QueryContext|QueryRowContext)\(`).FindAllStringIndex(src, -1) {
		method := src[m[0]+1 : m[1]-1]
		open := m[1] - 1
		body, ok := task33Balanced(src, open)
		if !ok {
			continue
		}
		parts := task33SplitArgs(body)
		sqlIndex := 0
		if strings.HasSuffix(method, "Context") {
			sqlIndex = 1
		}
		if len(parts) <= sqlIndex {
			continue
		}
		call := task33Call{file: file, stmt: strings.Join(parts[sqlIndex:], " | ")}
		sqlText := task33LiteralText(parts[sqlIndex])
		// 非字面量部分只剩 +、空白与已知列常量时才是"静态 SQL"；出现别的
		// Go 标识符（动态 IN 列表、拼接的 where）就退出静态计数，避免误判。
		rest := strings.TrimSpace(task33StripLiterals(parts[sqlIndex]))
		rest = strings.NewReplacer("+", "", " ", "", "\n", "", "\t", "", "toolKeyCol", "", "toolCols", "", "nodeCols", "", "`", "").Replace(rest)
		if rest != "" {
			call.dynamicSkip = true
			out = append(out, call)
			continue
		}
		call.marks = strings.Count(sqlText, "?")
		for _, p := range parts[sqlIndex+1:] {
			if strings.Contains(p, "...") {
				call.dynamicSkip = true
				break
			}
			if strings.TrimSpace(p) == "" {
				continue
			}
			call.args++
		}
		if call.dynamicSkip {
			out = append(out, call)
			continue
		}
		out = append(out, call)
	}
	return out
}

// TestTools_PlaceholdersMatchBoundArgs 把 8 个文件里每一条静态 SQL 的占位符数量
// 与实参数量逐个对齐（M21 里最容易在改造中悄悄错位的一类缺陷）。
func TestTools_PlaceholdersMatchBoundArgs(t *testing.T) {
	checked := 0
	for _, file := range task33Files {
		calls := task33Calls(t, file)
		static := 0
		for _, c := range calls {
			if c.dynamicSkip {
				continue
			}
			static++
			checked++
			if c.marks != c.args {
				t.Errorf("%s: %d placeholders but %d bound args: %s", file, c.marks, c.args, c.stmt)
			}
		}
		if static == 0 {
			t.Errorf("%s: no statically checkable statement found — scanner is not seeing the SQL", file)
		}
	}
	if checked < 25 {
		t.Errorf("only %d statements checked across 8 files, want >= 25", checked)
	}
}

// task33ExpandToolCols 把 tools.go 里两个列清单常量的定义值还原进源码文本，
// 让"反引号/身份列"断言看得到真正会发出去的 SQL。
func task33ExpandToolCols(t *testing.T, src string) string {
	t.Helper()
	for _, name := range []string{"toolKeyCol", "toolCols", "nodeCols"} {
		re := regexp.MustCompile(name + `\s*=\s*("(?:[^"\\]|\\.)*")`)
		file := "tools.go"
		if name == "nodeCols" {
			file = "exploration.go"
		}
		def := task33Source(t, file)
		m := re.FindStringSubmatch(def)
		if m == nil {
			continue
		}
		var value string
		if err := json.Unmarshal([]byte(m[1]), &value); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		src = strings.ReplaceAll(src, name, "`"+strings.ReplaceAll(value, "`", "`")+"`")
	}
	return src
}

// task33Balanced 从 open（指向 '('）开始返回配对括号内的内容。
func task33Balanced(src string, open int) (string, bool) {
	depth := 0
	for i := open; i < len(src); i++ {
		switch src[i] {
		case '`':
			end := strings.IndexByte(src[i+1:], '`')
			if end < 0 {
				return "", false
			}
			i += end + 1
		case '"':
			for j := i + 1; j < len(src); j++ {
				if src[j] == '\\' {
					j++
					continue
				}
				if src[j] == '"' {
					i = j
					break
				}
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return src[open+1 : i], true
			}
		}
	}
	return "", false
}

// task33SplitArgs 按顶层逗号切分调用实参（括号/方括号/花括号/字面量内部不切）。
func task33SplitArgs(body string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '`':
			end := strings.IndexByte(body[i+1:], '`')
			if end < 0 {
				i = len(body)
				continue
			}
			i += end + 1
		case '"':
			for j := i + 1; j < len(body); j++ {
				if body[j] == '\\' {
					j++
					continue
				}
				if body[j] == '"' {
					i = j
					break
				}
			}
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, body[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, body[start:])
	return parts
}

// task33LiteralText 只保留表达式里的字符串字面量内容（拼成完整 SQL 文本）。
func task33LiteralText(expr string) string {
	var out strings.Builder
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '`':
			end := strings.IndexByte(expr[i+1:], '`')
			if end < 0 {
				return out.String()
			}
			out.WriteString(expr[i+1 : i+1+end])
			i += end + 2
		case '"':
			var b strings.Builder
			for j := i + 1; j < len(expr); j++ {
				if expr[j] == '\\' && j+1 < len(expr) {
					switch expr[j+1] {
					case 'n':
						b.WriteByte('\n')
					case 't':
						b.WriteByte('\t')
					default:
						b.WriteByte(expr[j+1])
					}
					j++
					continue
				}
				if expr[j] == '"' {
					i = j + 1
					break
				}
				b.WriteByte(expr[j])
			}
			out.WriteString(b.String())
		default:
			i++
		}
	}
	return out.String()
}

// task33StripLiterals 反过来：只留字面量之外的部分（判定 SQL 是否静态）。
func task33StripLiterals(expr string) string {
	var out strings.Builder
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '`':
			end := strings.IndexByte(expr[i+1:], '`')
			if end < 0 {
				return out.String()
			}
			i += end + 2
		case '"':
			for j := i + 1; j < len(expr); j++ {
				if expr[j] == '\\' {
					j++
					continue
				}
				if expr[j] == '"' {
					i = j + 1
					break
				}
			}
		default:
			out.WriteByte(expr[i])
			i++
		}
	}
	return out.String()
}

func TestTools_StatementsCoverAllEightFiles(t *testing.T) {
	for _, file := range task33Files {
		if len(task33Statements(t, file)) == 0 {
			t.Errorf("%s: no statement extracted", file)
		}
	}
}

// ---- 聚合形状与取号方式 ------------------------------------------------

// TestTools_AggregatesUseTiDBShapes 钉死本任务的三处聚合/取值改造：
// 日桶必须 DATE_FORMAT、错误分桶必须 COUNT(CASE WHEN …)、取号必须 LastInsertId。
func TestTools_AggregatesUseTiDBShapes(t *testing.T) {
	usage := task33Source(t, "llm_usage.go")
	if got := strings.Count(usage, `DATE_FORMAT(ts, '%Y-%m-%d')`); got < 2 {
		t.Errorf("DATE_FORMAT(ts, '%%Y-%%m-%%d') count = %d, want >= 2 (UsageDaily + JudgeUsageStats)", got)
	}
	if got := strings.Count(usage, "ts >= ?"); got < 2 {
		t.Errorf("window start must be a bound parameter computed in Go, got %d occurrence(s)", got)
	}
	cmd := task33Source(t, "commands.go")
	if !strings.Contains(cmd, "COUNT(CASE WHEN COALESCE(r.is_error, 0) <> 0 THEN 1 END)") {
		t.Error("ToolStats must count errors with COUNT(CASE WHEN … THEN 1 END)")
	}
	if !strings.Contains(task33Source(t, "logs.go"), "LastInsertId()") {
		t.Error("logs.go must take the auto id via LastInsertId() (no RETURNING in TiDB)")
	}
	digest := task33Source(t, "digest.go")
	if !strings.Contains(digest, "expInsertID(") {
		t.Error("digest.go must reuse the shared insert-id range check (expInsertID)")
	}
	if !strings.Contains(digest, "BumpRound() (int64, error)") ||
		!strings.Contains(digest, "UPDATE explorations SET round_no = round_no + 1 WHERE id = ?") {
		t.Error("BumpRound must bump and re-read inside one transaction (no UPDATE … RETURNING in TiDB)")
	}
	if !strings.Contains(task33Source(t, "intent_admission.go"), "DELETE FROM exploration_edges") ||
		!strings.Contains(task33Source(t, "intent_admission.go"), "DELETE FROM exploration_anchors") {
		t.Error("node deletion must clear edges/anchors explicitly: the new library has no foreign keys")
	}
}

// ---- 真机集成用例（无 ARTEX_TIDB_DSN 时显式跳过）----------------------

func task33DSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("ARTEX_TIDB_DSN"))
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
	}
	return dsn
}

func task33Open(t *testing.T) *DB {
	t.Helper()
	d, err := Open(task33DSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})
	return d
}

// task33ToolKey 每次跑一个唯一键，并登记按主键的清理（不靠前缀批量删）。
func task33ToolKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func task33Exec(t *testing.T, d *DB, query string, args ...any) {
	t.Helper()
	if _, err := d.Exec(query, args...); err != nil {
		t.Errorf("cleanup %q: %v", query, err)
	}
}

func task33QueryRow[T any](t *testing.T, d *DB, query string, args ...any) T {
	t.Helper()
	var v T
	if err := d.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// TestTools_RegisterTool_UnmigratedDialect 计划 Step 1 的用例：注册一个工具并读回。
func TestTools_RegisterTool_UnmigratedDialect(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
	}
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})

	toolKey := task33ToolKey(t, "p08_tool")
	if err := d.SeedTool(toolKey, "desc", json.RawMessage(`{"type":"object"}`), json.RawMessage(`["worker"]`)); err != nil {
		t.Fatalf("SeedTool failed: %v", err)
	}
	t.Cleanup(func() { task33Exec(t, d, "DELETE FROM tools WHERE `key` = ?", toolKey) })

	tool, err := d.GetTool(toolKey)
	if err != nil || tool == nil || tool.Key != toolKey {
		t.Fatalf("GetTool failed: got (%+v, %v), want key=%s", tool, err, toolKey)
	}
	if !tool.System || !tool.Enabled || tool.Kind != "builtin" {
		t.Errorf("seeded defaults wrong: %+v", tool)
	}
}

// TestTools_KeyHashUniquenessAndIdentityColumns 钉在真机上：uq_tools_key_hash 必须
// 真的拦得住重复行，且两列身份都不为空、算法与 canonical 契约一致。
func TestTools_KeyHashUniquenessAndIdentityColumns(t *testing.T) {
	d := task33Open(t)
	key := task33ToolKey(t, "p08_hash")
	t.Cleanup(func() { task33Exec(t, d, "DELETE FROM tools WHERE `key` = ?", key) })

	if err := d.SeedTool(key, "first", json.RawMessage(`{"type":"object"}`), json.RawMessage(`["worker"]`)); err != nil {
		t.Fatalf("SeedTool: %v", err)
	}
	// 首插入语义：第二次 SeedTool 既不报错也不新增行，且不覆盖页面上的编辑。
	if err := d.UpdateTool(key, "EDITED", json.RawMessage(`{"type":"object"}`), json.RawMessage(`["planner"]`), false); err != nil {
		t.Fatalf("UpdateTool: %v", err)
	}
	if err := d.SeedTool(key, "second", json.RawMessage(`{}`), json.RawMessage(`[]`)); err != nil {
		t.Fatalf("SeedTool must stay idempotent on a duplicate key: %v", err)
	}
	if rows := task33QueryRow[int](t, d, "SELECT COUNT(*) FROM tools WHERE `key` = ?", key); rows != 1 {
		t.Errorf("rows for %s = %d, want 1 (uq_tools_key_hash must reject the duplicate)", key, rows)
	}
	got, err := d.GetTool(key)
	if err != nil || got == nil {
		t.Fatalf("GetTool: %v", err)
	}
	if got.Description != "EDITED" || got.Enabled {
		t.Errorf("duplicate SeedTool overwrote the page edit: %+v", got)
	}

	var identity, hash []byte
	if err := d.QueryRow("SELECT key_identity, key_hash FROM tools WHERE `key` = ?", key).Scan(&identity, &hash); err != nil {
		t.Fatalf("read identity columns: %v", err)
	}
	wantPayload := schema.SettingsKeyIdentity(key)
	wantHash := sha256.Sum256(wantPayload)
	if string(identity) != string(wantPayload) {
		t.Errorf("key_identity = %x, want canonical %x", identity, wantPayload)
	}
	if string(hash) != string(wantHash[:]) {
		t.Errorf("key_hash = %x, want SHA-256 %x", hash, wantHash)
	}

	// 自定义工具（system=false）撞同键必须显式失败，绝不覆盖既有行。
	custom := &Tool{Key: key, Description: "dup", Schema: json.RawMessage(`{}`), Kind: "command", Exec: json.RawMessage(`{}`)}
	if err := d.CreateCustomTool(custom); err == nil {
		t.Error("CreateCustomTool on an existing key must fail instead of overwriting")
	}
	// UpsertToolForce 恢复代码默认。
	if err := d.UpsertToolForce(key, "reset", json.RawMessage(`{"type":"object"}`), json.RawMessage(`["auto"]`)); err != nil {
		t.Fatalf("UpsertToolForce: %v", err)
	}
	after, err := d.GetTool(key)
	if err != nil || after == nil {
		t.Fatalf("GetTool after force upsert: %v", err)
	}
	if after.Description != "reset" || !after.Enabled || !after.System {
		t.Errorf("force upsert did not restore code defaults: %+v", after)
	}
	if rows := task33QueryRow[int](t, d, "SELECT COUNT(*) FROM tools WHERE `key` = ?", key); rows != 1 {
		t.Errorf("force upsert must update in place, rows = %d", rows)
	}
	// 内置工具受 DeleteCustomTool 保护。
	if err := d.DeleteCustomTool(key); err != nil {
		t.Fatalf("DeleteCustomTool: %v", err)
	}
	if rows := task33QueryRow[int](t, d, "SELECT COUNT(*) FROM tools WHERE `key` = ?", key); rows != 1 {
		t.Errorf("DeleteCustomTool must refuse system=true rows, rows = %d", rows)
	}
}

// TestTools_KeywordColumnsRoundTrip 用带反引号/通配符/中文的 key 跑一遍读写，
// 证明关键字反引号与参数化对最脏的输入也成立。
func TestTools_KeywordColumnsRoundTrip(t *testing.T) {
	d := task33Open(t)
	key := task33ToolKey(t, "p08_key`%_中文")
	t.Cleanup(func() { task33Exec(t, d, "DELETE FROM tools WHERE `key` = ?", key) })

	if err := d.CreateCustomTool(&Tool{
		Key:         key,
		Description: "schema/exec/system 关键字回归",
		Schema:      json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
		Agents:      []string{"worker"},
		Enabled:     true,
		Kind:        "script",
		Exec:        json.RawMessage(`{"code":"print(1)"}`),
		Deferred:    true,
	}); err != nil {
		t.Fatalf("CreateCustomTool: %v", err)
	}

	got, err := d.GetTool(key)
	if err != nil || got == nil {
		t.Fatalf("GetTool: %v", err)
	}
	if got.Key != key || got.Kind != "script" || !got.Deferred || !got.Enabled || got.System {
		t.Errorf("row mismatch: %+v", got)
	}
	if string(got.Schema) == "" || len(got.Agents) != 1 || got.Agents[0] != "worker" {
		t.Errorf("schema/agents round trip wrong: %+v", got)
	}

	// UpdateTool 走 `schema` / `enabled` 关键字列。
	if err := d.UpdateTool(key, "edited", json.RawMessage(`{"type":"string"}`), json.RawMessage(`["worker","planner"]`), false); err != nil {
		t.Fatalf("UpdateTool: %v", err)
	}
	got2, err := d.GetTool(key)
	if err != nil || got2 == nil || got2.Enabled || len(got2.Agents) != 2 {
		t.Fatalf("UpdateTool not applied: %+v (%v)", got2, err)
	}
	// 自定义工具只出现在 ListCustomTools。
	customs, err := d.ListCustomTools()
	if err != nil {
		t.Fatalf("ListCustomTools: %v", err)
	}
	var found bool
	for _, c := range customs {
		if c.Key == key {
			found = true
			break
		}
	}
	if !found {
		t.Error("created custom tool missing from ListCustomTools")
	}
	if err := d.DeleteCustomTool(key); err != nil {
		t.Fatalf("DeleteCustomTool: %v", err)
	}
	if g, err := d.GetTool(key); err != nil || g != nil {
		t.Errorf("tool still present after delete: %+v (%v)", g, err)
	}
}

// TestTools_AgentBindingRoundTrip 覆盖 Go 端数组改写的追加/幂等/删除三条路径。
func TestTools_AgentBindingRoundTrip(t *testing.T) {
	d := task33Open(t)
	key := task33ToolKey(t, "p08_bind")
	t.Cleanup(func() { task33Exec(t, d, "DELETE FROM tools WHERE `key` = ?", key) })

	if err := d.SeedTool(key, "bind", json.RawMessage(`{}`), json.RawMessage(`["worker"]`)); err != nil {
		t.Fatalf("SeedTool: %v", err)
	}
	if err := d.AddAgentToToolBinding("auto", []string{key, "p08_never_seeded_" + key}); err != nil {
		t.Fatalf("AddAgentToToolBinding: %v", err)
	}
	got, err := d.GetTool(key)
	if err != nil || got == nil {
		t.Fatalf("GetTool: %v", err)
	}
	if len(got.Agents) != 2 || got.Agents[0] != "worker" || got.Agents[1] != "auto" {
		t.Fatalf("append wrong: %+v", got.Agents)
	}
	// 幂等：重复补绑不产生重复元素，也不改写顺序。
	if err := d.AddAgentToToolBinding("auto", []string{key}); err != nil {
		t.Fatalf("idempotent AddAgentToToolBinding: %v", err)
	}
	again, err := d.GetTool(key)
	if err != nil || again == nil {
		t.Fatalf("GetTool after idempotent add: %v", err)
	}
	if len(again.Agents) != 2 {
		t.Errorf("binding must stay idempotent, got %+v", again.Agents)
	}

	if err := d.RemoveAgentFromTool("auto", key); err != nil {
		t.Fatalf("RemoveAgentFromTool: %v", err)
	}
	after, err := d.GetTool(key)
	if err != nil || after == nil {
		t.Fatalf("GetTool after remove: %v", err)
	}
	if len(after.Agents) != 1 || after.Agents[0] != "worker" {
		t.Errorf("remove must keep the remaining order, got %+v", after.Agents)
	}
	if err := d.RemoveAgentFromToolBindings("worker"); err != nil {
		t.Fatalf("RemoveAgentFromToolBindings: %v", err)
	}
	gone, err := d.GetTool(key)
	if err != nil || gone == nil {
		t.Fatalf("GetTool after global unbind: %v", err)
	}
	if len(gone.Agents) != 0 {
		t.Errorf("global unbind must clear this tool's binding, got %+v", gone.Agents)
	}
}

// TestTools_UsageAggregatesRoundTrip 覆盖工具与 LLM 使用统计：口径全部来自
// tool_usage / llm_usage 两张账本，日桶必须落在 UTC 日历天上。
func TestTools_UsageAggregatesRoundTrip(t *testing.T) {
	d := task33Open(t)
	toolA := task33ToolKey(t, "p08_use_a")
	toolB := task33ToolKey(t, "p08_use_b")
	taskTag := fmt.Sprintf("p08-usage-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		task33Exec(t, d, "DELETE FROM tool_usage WHERE tool_key IN (?, ?)", toolA, toolB)
		task33Exec(t, d, "DELETE FROM llm_usage WHERE task_id = ?", taskTag)
	})

	for _, row := range []*ToolUsage{
		{ToolKey: toolA, AgentKey: "worker", TaskID: 991, ExplorationID: 5, IntentID: 7},
		{ToolKey: toolA, AgentKey: "planner", TaskID: 992, ExplorationID: 6},
		{ToolKey: toolA, AgentKey: "", SessionID: ""},
		{ToolKey: toolB, AgentKey: "worker", TaskID: 991},
	} {
		if err := d.InsertToolUsage(row); err != nil {
			t.Fatalf("InsertToolUsage(%s): %v", row.ToolKey, err)
		}
	}
	counts, err := d.ToolUsageCounts()
	if err != nil {
		t.Fatalf("ToolUsageCounts: %v", err)
	}
	if counts[toolA] != 3 || counts[toolB] != 1 {
		t.Errorf("tool counts = (%d, %d), want (3, 1)", counts[toolA], counts[toolB])
	}

	for _, row := range []*LLMUsage{
		{TaskID: taskTag, ExplorationID: 1, Worker: "worker", Model: "m-a", ProfileName: "prof-a", InputTokens: 10, OutputTokens: 5, CacheRead: 2},
		{TaskID: taskTag, ExplorationID: 1, Worker: "worker", Model: "m-a", ProfileName: "prof-a", InputTokens: 1, OutputTokens: 1},
		{TaskID: taskTag, ExplorationID: 1, Worker: "planner", Model: "m-b", ProfileName: "prof-b", InputTokens: 4, OutputTokens: 6},
	} {
		if err := d.InsertLLMUsage(row); err != nil {
			t.Fatalf("InsertLLMUsage: %v", err)
		}
	}
	byModel, err := d.TokenByModel(taskTag)
	if err != nil {
		t.Fatalf("TokenByModel: %v", err)
	}
	if len(byModel) != 2 {
		t.Fatalf("models = %d, want 2: %+v", len(byModel), byModel)
	}
	if byModel[0].Model != "m-a" || byModel[0].Calls != 2 || byModel[0].InputTokens != 11 || byModel[0].OutputTokens != 6 {
		t.Errorf("m-a aggregate wrong: %+v", byModel[0])
	}
	if byModel[0].CacheReadTokens != 2 {
		t.Errorf("cache_read must be summed too, got %+v", byModel[0])
	}

	profiles, err := d.UsageByProfile()
	if err != nil {
		t.Fatalf("UsageByProfile: %v", err)
	}
	var profA *ProfileUsage
	for i := range profiles {
		if profiles[i].ProfileName == "prof-a" {
			profA = &profiles[i]
		}
	}
	if profA == nil {
		t.Fatalf("prof-a missing from UsageByProfile: %+v", profiles)
	}
	if profA.Calls != 2 || profA.Tasks < 1 {
		t.Errorf("prof-a aggregate wrong: %+v", profA)
	}

	daily, err := d.UsageDaily(2)
	if err != nil {
		t.Fatalf("UsageDaily: %v", err)
	}
	var bucket *ProfileDayUsage
	for i := range daily {
		if daily[i].ProfileName == "prof-a" {
			bucket = &daily[i]
		}
	}
	if bucket == nil {
		t.Fatalf("prof-a missing from UsageDaily: %+v", daily)
	}
	if _, err := time.Parse("2006-01-02", bucket.Date); err != nil {
		t.Errorf("day bucket must be YYYY-MM-DD, got %q", bucket.Date)
	}
	if bucket.InputTokens < 11 || bucket.CacheReadTokens < 2 {
		t.Errorf("prof-a day bucket wrong: %+v", bucket)
	}
	if _, err := d.JudgeUsageStats(2); err != nil {
		t.Errorf("JudgeUsageStats: %v", err)
	}
}

// TestTools_InsertLogAndDigestHelpers 覆盖日志取号与每日摘要的读路径。
func TestTools_InsertLogAndDigestHelpers(t *testing.T) {
	d := task33Open(t)
	marker := fmt.Sprintf("p08-log-%d", time.Now().UnixNano())
	id, err := d.InsertLog("info", "p08", marker)
	if err != nil {
		t.Fatalf("InsertLog: %v", err)
	}
	if id <= 0 {
		t.Fatalf("InsertLog must return a positive auto id, got %d", id)
	}
	t.Cleanup(func() { task33Exec(t, d, "DELETE FROM server_logs WHERE id = ?", id) })

	rows, err := d.RecentLogs(50)
	if err != nil {
		t.Fatalf("RecentLogs: %v", err)
	}
	var found bool
	for _, r := range rows {
		if r.ID == id {
			found = true
			if r.Text != marker || r.CreatedAt.IsZero() {
				t.Errorf("log row wrong: %+v", r)
			}
		}
	}
	if !found {
		t.Error("inserted log missing from RecentLogs")
	}
	if _, err := d.ListLogsBefore(id+1, 10); err != nil {
		t.Errorf("ListLogsBefore: %v", err)
	}
	if _, err := d.LoadLLMHealth(); err != nil {
		t.Errorf("LoadLLMHealth: %v", err)
	}
}
