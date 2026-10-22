package server

// 本文件是 W5 任务四「Server 生产路径去 PG 方言」的**纯离线**回归测试（复审
// w5-server-rereview.md 的 I-01 / M-02 / M-03）：只调用不依赖数据库的纯函数，
// 恒定脱机执行 —— 不建连接、不读 DSN、不碰 ARTEX_CONFIG，因此真库存在与否、
// 夹具波次的 t.Skip 都与本文件无关。
//
// 覆盖的不变式：
//  1. decodeToolSchema / jsonCanonical / rewriteToolSchema 全程 json.Number，
//     大整数（9007199254740993）与高精度整数字面量（12345678901234567890）在
//     「读 → 改写 → 写回」后**逐字节保持原样**（I-01）；
//  2. 默认 float64 往返会改写这些字面量 —— 用一条对照用例把 I-01 的成因钉住，
//     谁把 UseNumber 改回 json.Unmarshal，这条就会红；
//  3. jsonCanonical 只归一键序与空白、**不做数值归一**（1 / 1.0 / 1e2 互不相等），
//     这是 M-02 记录并在注释里如实写明的口径；
//  4. workflow schema 的补属性只新增缺失键，非对象中间层 fail closed；
//  5. toolAgentsHasAll / toolAgentList 与旧 jsonb `@>` + `jsonb_array_length` 的
//     谓词等价，以及 setColumns 的「每列只绑定自己的值、占位符与 args 同序」。

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
)

// =====================================================================
// 1. decodeToolSchema：UseNumber 精度
// =====================================================================

const numberLiteralDoc = `{"a":9007199254740993,"b":12345678901234567890,"c":1.0,"d":1e2,"e":-0.5,"f":{"g":90071992547409930000},"s":"9007199254740993","t":true,"z":null,"arr":[9007199254740993,1.0,12345678901234567890]}`

// numberLiteral 断言 v 是 json.Number 且字面量恰为 want。
func numberLiteral(t *testing.T, path string, v any, want string) {
	t.Helper()
	n, ok := v.(json.Number)
	if !ok {
		t.Fatalf("%s: 期望 json.Number，实际 %T（%v）—— UseNumber 失效？", path, v, v)
	}
	if n.String() != want {
		t.Fatalf("%s: 数字字面量被改写：got %s, want %s", path, n.String(), want)
	}
}

func TestToolSchemaDecodeKeepsNumberLiterals(t *testing.T) {
	doc, err := decodeToolSchema([]byte(numberLiteralDoc), "probe_tool")
	if err != nil {
		t.Fatal(err)
	}
	numberLiteral(t, "a", doc["a"], "9007199254740993")
	numberLiteral(t, "b", doc["b"], "12345678901234567890")
	numberLiteral(t, "c", doc["c"], "1.0")
	numberLiteral(t, "d", doc["d"], "1e2")
	numberLiteral(t, "e", doc["e"], "-0.5")
	nested, _ := doc["f"].(map[string]any)
	if nested == nil {
		t.Fatalf("f 不是对象：%T", doc["f"])
	}
	numberLiteral(t, "f.g", nested["g"], "90071992547409930000")
	items, _ := doc["arr"].([]any)
	if len(items) != 3 {
		t.Fatalf("arr 长度异常：%v", items)
	}
	numberLiteral(t, "arr[0]", items[0], "9007199254740993")
	numberLiteral(t, "arr[1]", items[1], "1.0")
	numberLiteral(t, "arr[2]", items[2], "12345678901234567890")
	if s, ok := doc["s"].(string); !ok || s != "9007199254740993" {
		t.Fatalf("字符串数字必须保持字符串类型：%T %v", doc["s"], doc["s"])
	}
	if b, ok := doc["t"].(bool); !ok || !b {
		t.Fatalf("布尔被改写：%T", doc["t"])
	}
	if v, exists := doc["z"]; !exists || v != nil {
		t.Fatalf("null 丢了：%v", v)
	}

	// 再序列化一次：字面量必须逐字节回来（写回 DB 的就是这份字节）。
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"a":9007199254740993`, `"b":12345678901234567890`, `"c":1.0`, `"d":1e2`, `"e":-0.5`, `"g":90071992547409930000`, `"s":"9007199254740993"`, `[9007199254740993,1.0,12345678901234567890]`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("序列化后字面量丢失：缺 %s\n got %s", want, out)
		}
	}
	// float64 会写成的形式，一个都不许出现。
	for _, bad := range []string{"9007199254740992", "1.2345678901234567", `"c":1,`, `"c":1}`, `"d":100`, `"g":9.007199254740992e+19`} {
		if strings.Contains(string(out), bad) {
			t.Fatalf("序列化结果出现 float64 形式 %s：\n%s", bad, out)
		}
	}
	if !jsonEqualCanonical(out, []byte(numberLiteralDoc)) {
		t.Fatalf("解码 + 序列化必须是值级等值：\n%s\n%s", out, numberLiteralDoc)
	}
}

// TestToolSchemaFloat64BaselineIsLossy 把 I-01 的**成因**钉住：默认 json.Unmarshal
// 到 map[string]any 会把这些字面量过一遍 float64 并静默改写。生产路径必须走
// decodeToolSchema；这条用例红 = 有人把 UseNumber 改回去了。
func TestToolSchemaFloat64BaselineIsLossy(t *testing.T) {
	var plain map[string]any
	if err := json.Unmarshal([]byte(numberLiteralDoc), &plain); err != nil {
		t.Fatal(err)
	}
	if got, ok := plain["a"].(float64); !ok || int64(got) != 9007199254740992 {
		t.Fatalf("float64 基线变了（got %v/%T），请同步复核 I-01 的表述", plain["a"], plain["a"])
	}
	round, err := json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	// 字符串里的数字原样保留，所以只检查数字位置上的字面量。
	if strings.Contains(string(round), `"a":9007199254740993`) || strings.Contains(string(round), `"b":12345678901234567890`) {
		t.Fatalf("float64 往返居然保住了大整数字面量，请复核：\n%s", round)
	}
	// 反过来：json.Number 语义下这两个大整数是**不同**的值；float64 会把
	// 9007199254740993 / 9007199254740992 判成相等，从而掩盖用户的编辑。
	if jsonEqualCanonical([]byte(`{"n":9007199254740993}`), []byte(`{"n":9007199254740992}`)) {
		t.Fatal("json.Number 语义下相邻大整数被判等，精度已丢")
	}
}

func TestToolSchemaDecodeRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"whitespace", "   \n\t "},
		{"json null", "null"},
		{"array", `[1,2]`},
		{"scalar string", `"text"`},
		{"scalar number", `42`},
		{"truncated object", `{"a":`},
		{"garbage", `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeToolSchema([]byte(tc.raw), "probe_tool"); err == nil {
				t.Fatalf("schema=%q 必须 fail closed 返错", tc.raw)
			}
		})
	}
	// 已知取舍（复审 O-03）：只用一次 Decoder.Decode，不校验尾随垃圾，因此
	// 「首值是合法对象 + 尾部垃圾」会被接受成前缀。正常 API 把 schema 作为外层
	// JSON 的 RawMessage 解码，写不进这种数据；这里把现状钉住，避免措辞回退成
	// 「非法 JSON 一律 fail closed」。
	doc, err := decodeToolSchema([]byte(`{"a":1} trailing garbage`), "probe_tool")
	if err != nil {
		t.Fatalf("当前实现按前缀接受（O-03）：%v", err)
	}
	if len(doc) != 1 {
		t.Fatalf("前缀解析异常：%v", doc)
	}
}

// =====================================================================
// 2. jsonCanonical / jsonEqualCanonical：归一什么、不归一什么
// =====================================================================

func TestJsonCanonicalNormalizesOrderAndWhitespace(t *testing.T) {
	got, err := jsonCanonical([]byte("{ \"b\" : 1 ,\n  \"a\" : [ 1 , { \"d\" : true , \"c\" : null } ] }"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"a":[1,{"c":null,"d":true}],"b":1}`; string(got) != want {
		t.Fatalf("紧凑形不符：got %s, want %s", got, want)
	}
	// 键序 / 空白 / 缩进不同 → 等值（等价旧 jsonb 值比较的键序无关）。
	if !jsonEqualCanonical([]byte(`{"x":1,"y":{"p":1,"q":2}}`), []byte("{\n \"y\" : { \"q\":2 , \"p\":1 } ,\n\"x\":1}\n")) {
		t.Fatal("键序与空白必须归一")
	}
	if !jsonEqualCanonical([]byte(`{"a":[1,{"b":2}]}`), []byte(`{"a":[1,{ "b" : 2 }]}`)) {
		t.Fatal("嵌套结构必须归一")
	}
	if jsonEqualCanonical([]byte(`{"a":1}`), []byte(`{"a":2}`)) {
		t.Fatal("不同值判成相等")
	}
	if jsonEqualCanonical([]byte(`{"a":1}`), []byte(`{"a":1,"b":2}`)) {
		t.Fatal("多一个键判成相等")
	}
	if jsonEqualCanonical([]byte(`{`), []byte(`{}`)) || jsonEqualCanonical([]byte(`{}`), []byte(`{`)) {
		t.Fatal("非法 JSON 必须判不等")
	}
	// 数字必须保持字符串/数字的类型区分，不能被"归一"混为一谈。
	if jsonEqualCanonical([]byte(`{"a":1}`), []byte(`{"a":"1"}`)) {
		t.Fatal("数字与字符串判成相等")
	}
}

func TestJsonCanonicalKeepsNumberLiterals(t *testing.T) {
	for _, raw := range []string{
		`{"n":9007199254740993}`,
		`{"n":12345678901234567890}`,
		`{"n":90071992547409930000}`,
		`{"n":-9007199254740993}`,
		`{"n":1.0}`,
		`{"n":1e2}`,
		`{"n":0.3000000000000000000001}`,
	} {
		got, err := jsonCanonical([]byte(raw))
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if string(got) != raw {
			t.Fatalf("数字字面量必须原样保留：got %s, want %s", got, raw)
		}
	}
	// M-02 如实记录的口径：**不做数值归一**。jsonb 里 1 = 1.0 = 1e2，这里判不等；
	// 后果只是「已是目标形态的行可能被多写一次等价字节」，不会改写用户数据。
	if jsonEqualCanonical([]byte(`{"n":1}`), []byte(`{"n":1.0}`)) ||
		jsonEqualCanonical([]byte(`{"n":1}`), []byte(`{"n":1e2}`)) ||
		jsonEqualCanonical([]byte(`{"n":100}`), []byte(`{"n":1e2}`)) {
		t.Fatal("jsonCanonical 已变成数值归一，请同步更新 M-02 的注释与本用例")
	}
	// 字面量相同、键序不同 → 仍判相等（比较的是值级规范形，不是原文本）。
	if !jsonEqualCanonical([]byte(`{"z":1,"n":9007199254740993}`), []byte(`{"n":9007199254740993,"z":1}`)) {
		t.Fatal("同字面量、异键序必须判相等")
	}
}

// =====================================================================
// 3. rewriteToolSchema：I-01 的主用例（读 → 补属性 → 写回字节的精度不变式）
// =====================================================================

// userEditedHintSchema 模拟用户在页面上改过、且带大整数默认值的 add_hint schema。
const userEditedHintSchema = `{"type":"object","required":["text"],"properties":{"text":{"type":"string","description":"USER TEXT"},"limit":{"type":"integer","default":9007199254740993,"maximum":12345678901234567890,"minimum":-9007199254740993},"ratio":{"type":"number","default":1.0}}}`

func TestToolSchemaRewriteKeepsUserNumbers(t *testing.T) {
	next, changed, err := rewriteToolSchema("add_hint", []byte(userEditedHintSchema), upgradeFindingWorkflowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("缺 traffic_refs / hints.items，必须写回")
	}
	// 1) 用户的数字字面量原样。
	for _, want := range []string{
		`"default":9007199254740993`,
		`"maximum":12345678901234567890`,
		`"minimum":-9007199254740993`,
		`"ratio":{"default":1.0,"type":"number"}`,
		`"description":"USER TEXT"`,
	} {
		if !strings.Contains(next, want) {
			t.Fatalf("用户数据被改写，缺 %s\n got %s", want, next)
		}
	}
	// 2) float64 往返的产物一个都不许出现（这就是修复前的行为）。
	for _, bad := range []string{"9007199254740992", "1.2345678901234567", "12345678901234567000", `"ratio":{"default":1,`} {
		if strings.Contains(next, bad) {
			t.Fatalf("写回字节出现 float64 形式 %s：\n%s", bad, next)
		}
	}
	// 3) 该补的都补上了。
	for _, want := range []string{"traffic_refs", `"hints":{`, `"items":{`, `"type":"array"`, `"type":"object"`, "USER TEXT"} {
		if !strings.Contains(next, want) {
			t.Fatalf("缺补齐内容 %s\n got %s", want, next)
		}
	}
	// 4) 幂等：拿写回字节再跑一次，必须 changed=false（整行不动、updated_at 不跳）。
	if again, changedAgain, err := rewriteToolSchema("add_hint", []byte(next), upgradeFindingWorkflowSchema); err != nil || changedAgain || again != "" {
		t.Fatalf("第二次升级必须 no-op：changed=%v next=%q err=%v", changedAgain, again, err)
	}
	// 5) 键序归一后的等价文档同样必须 no-op（等价旧 jsonb 值比较的守卫）。
	reshuffled := mustRecanonicalize(t, next)
	if _, changedReorder, err := rewriteToolSchema("add_hint", []byte(reshuffled), upgradeFindingWorkflowSchema); err != nil || changedReorder {
		t.Fatalf("等价文档被判成需要改写：changed=%v err=%v", changedReorder, err)
	}
}

// mustRecanonicalize 把 JSON 文本变成它的规范形（键序 / 空白已归一），用来模拟
// 「库里那份只是排版不同、内容一样」的历史数据。
func mustRecanonicalize(t *testing.T, raw string) string {
	t.Helper()
	canonical, err := jsonCanonical([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return string(canonical)
}

func TestToolSchemaRewriteReportFindingKeepsUserDefault(t *testing.T) {
	raw := `{"type":"object","properties":{"vulnclass":{"type":"string"},"evidence_hint_id":{"type":"integer","default":9007199254740993,"description":"USER DEFAULT"}}}`
	next, changed, err := rewriteToolSchema("report_finding", []byte(raw), upgradeFindingWorkflowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatalf("evidence_hint_id 已存在，整行都不该动：\n%s", next)
	}
	// 反证：属性缺失时才写回，且写回字节里用户的另一个大整数默认值仍是原字面量。
	rawMissing := `{"type":"object","properties":{"vulnclass":{"type":"string"},"limit":{"type":"integer","default":12345678901234567890}}}`
	nextMissing, changedMissing, err := rewriteToolSchema("report_finding", []byte(rawMissing), upgradeFindingWorkflowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !changedMissing {
		t.Fatal("缺 evidence_hint_id，必须写回")
	}
	if !strings.Contains(nextMissing, `"evidence_hint_id"`) || !strings.Contains(nextMissing, `"default":12345678901234567890`) {
		t.Fatalf("写回缺字段或丢了用户大整数：\n%s", nextMissing)
	}
}

func TestToolSchemaRewriteFailsClosedOnBadShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		raw     string
		wantSub string
	}{
		{"properties 是字符串", "report_finding", `{"type":"object","properties":"USER"}`, "properties"},
		{"properties 是数组", "add_hint", `{"properties":[1,2]}`, "properties"},
		{"hints 是字符串", "add_hint", `{"properties":{"hints":"USER"}}`, "schema.properties.hints"},
		{"items 是数字", "add_hint", `{"properties":{"hints":{"type":"array","items":7}}}`, "schema.properties.hints.items"},
		{"schema 是空串", "add_hint", ``, "schema 列为空"},
		{"schema 是数组", "add_hint", `[1,2]`, "不是 JSON 对象"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, changed, err := rewriteToolSchema(tc.key, []byte(tc.raw), upgradeFindingWorkflowSchema)
			if err == nil {
				t.Fatalf("必须 fail closed，got next=%q changed=%v", next, changed)
			}
			if changed || next != "" {
				t.Fatalf("出错时不得写回：changed=%v next=%q", changed, next)
			}
			if !strings.Contains(err.Error(), tc.wantSub) || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("错误信息缺定位信息：%v", err)
			}
		})
	}
	// create_missing：properties / hints 为 JSON null 或缺失时新建对象（等价旧
	// jsonb_set 的 create_missing=true），并把用户已有的兄弟键原样保留。
	for _, raw := range []string{
		`{"type":"object","properties":null,"keep":{"default":9007199254740993}}`,
		`{"type":"object","keep":{"default":9007199254740993}}`,
	} {
		next, changed, err := rewriteToolSchema("add_hint", []byte(raw), upgradeFindingWorkflowSchema)
		if err != nil || !changed {
			t.Fatalf("%s: changed=%v err=%v", raw, changed, err)
		}
		if !strings.Contains(next, `"traffic_refs"`) || !strings.Contains(next, `"default":9007199254740993`) {
			t.Fatalf("create_missing 结果异常：\n%s", next)
		}
	}
	// hints 存在但缺 items.properties 时逐层 create_missing。
	next, changed, err := rewriteToolSchema("add_hint", []byte(`{"properties":{"hints":{"type":"array","items":{"type":"object"}}}}`), upgradeFindingWorkflowSchema)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	for _, want := range []string{"traffic_refs", "text", "asset_ids", `"type":"array"`, `"type":"object"`} {
		if !strings.Contains(next, want) {
			t.Fatalf("缺 %s：\n%s", want, next)
		}
	}
}

// =====================================================================
// 4. toolSchemaLayer / toolSchemaAddIfAbsent（取代旧 objectProperty 的两个 helper）
// =====================================================================

func TestToolSchemaLayerAndAddIfAbsent(t *testing.T) {
	// toolSchemaAddIfAbsent 绝不覆盖同名既有键（用户编辑是权威数据）。
	parent := map[string]any{"keep": "USER", "n": json.Number("9007199254740993")}
	if toolSchemaAddIfAbsent(parent, "keep", "NEW") {
		t.Fatal("已存在的键被覆盖")
	}
	if parent["keep"] != "USER" {
		t.Fatalf("值被改写：%v", parent["keep"])
	}
	if !toolSchemaAddIfAbsent(parent, "fresh", "NEW") {
		t.Fatal("缺失的键应当挂载")
	}
	if parent["fresh"] != "NEW" {
		t.Fatalf("挂载写到了别处：%v", parent)
	}

	// toolSchemaLayer：已有对象原样返回、缺失/null 新建、其它类型返错。
	doc := map[string]any{"properties": map[string]any{"a": json.Number("12345678901234567890")}}
	layer, err := toolSchemaLayer(doc, "properties", "schema")
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := layer["a"].(json.Number); !ok || n.String() != "12345678901234567890" {
		t.Fatalf("既有对象被换掉了：%v", layer["a"])
	}
	created, err := toolSchemaLayer(doc, "hints", "schema.properties")
	if err != nil || created == nil {
		t.Fatalf("缺失层应新建：%v %v", created, err)
	}
	if _, ok := doc["hints"].(map[string]any); !ok {
		t.Fatal("新建的层没挂回父节点")
	}
	doc["nullLayer"] = nil
	if _, err := toolSchemaLayer(doc, "nullLayer", "schema"); err != nil {
		t.Fatalf("JSON null 应按 create_missing 接受：%v", err)
	}
	doc["scalar"] = "USER"
	if _, err := toolSchemaLayer(doc, "scalar", "schema.properties"); err == nil {
		t.Fatal("标量层必须返错（旧 objectProperty 会静默清空它）")
	} else if !strings.Contains(err.Error(), "schema.properties.scalar") {
		t.Fatalf("错误缺路径：%v", err)
	}
}

// =====================================================================
// 5. agents 谓词：与旧 jsonb `@>` + `jsonb_array_length` 等价
// =====================================================================

func TestWorkflowToolAgentsHasAll(t *testing.T) {
	// 默认五元组：顺序无关（@> 不要求顺序）。
	base := []string{"pentest", "auto", "worker", "mainagent", "planner"}
	if !toolAgentsHasAll(base, "worker", "planner", "mainagent", "auto", "pentest") {
		t.Fatal("同集合乱序必须判定包含")
	}
	if !toolAgentsHasAll([]string{"worker"}, "worker") {
		t.Fatal("单元素命中失败")
	}
	if toolAgentsHasAll([]string{"worker", "reporter"}, "worker", "planner") {
		t.Fatal("缺元素必须判不包含")
	}
	if !toolAgentsHasAll([]string{"worker", "worker"}, "worker") {
		t.Fatal("重复元素按 @> 语义仍算包含")
	}
	if !toolAgentsHasAll(nil) {
		t.Fatal("want 为空按 @> 语义恒真")
	}
	if toolAgentsHasAll(nil, "worker") || toolAgentsHasAll([]string{}, "worker") {
		t.Fatal("空列表不得命中")
	}
	// 与 seedFindingWorkflowTools 的三条谓词逐条对齐。
	defaultReaders := []string{"worker", "planner", "mainagent", "auto", "pentest"}
	if len(defaultReaders) != 5 || !toolAgentsHasAll(defaultReaders, "worker", "planner", "mainagent", "auto", "pentest") {
		t.Fatal("traffic_* 的旧默认判定异常")
	}
	if toolAgentsHasAll([]string{"worker", "reporter"}, "worker", "planner", "mainagent", "auto", "pentest") {
		t.Fatal("用户自定义绑定不得被当成历史默认")
	}
	if !toolAgentsHasAll([]string{"auto", "reporter"}, "auto", "reporter") {
		t.Fatal("get_finding_traffic 的旧默认判定异常")
	}
	// 元素个数由调用方的 len(agents)==N 兜住：@> 自身允许超集。
	if !toolAgentsHasAll([]string{"auto", "reporter", "extra"}, "auto", "reporter") {
		t.Fatal("@> 语义与顺序/超集无关")
	}
}

func TestWorkflowToolAgentList(t *testing.T) {
	items, err := toolAgentList([]byte(`["worker","reporter"]`), "traffic_search")
	if err != nil || len(items) != 2 || items[0] != "worker" || items[1] != "reporter" {
		t.Fatalf("有序解析异常：%v %v", items, err)
	}
	if items, err = toolAgentList([]byte(" [] "), "traffic_search"); err != nil || len(items) != 0 {
		t.Fatalf("空数组（含空白）应为 0 项：%v %v", items, err)
	}
	for _, raw := range []string{``, `  `, `["worker",1]`, `{"a":1}`, `not json`, `["a",]`} {
		if _, err := toolAgentList([]byte(raw), "traffic_search"); err == nil {
			t.Fatalf("agents=%q 必须 fail closed 返错", raw)
		}
	}
	// 复审 O-04 记录的现状：JSON null 解到 []string 是 (nil, nil) —— 调用方的
	// len(agents) 判定把它当「不匹配历史默认」，不重写、无数据风险。
	items, err = toolAgentList([]byte(`null`), "traffic_search")
	if err != nil || len(items) != 0 {
		t.Fatalf("null 应解成空列表（O-04）：%v %v", items, err)
	}
}

// =====================================================================
// 6. setColumns：每列只绑定自己的值，占位符与 args 同序（M-03）
// =====================================================================

var placeholderDollar = regexp.MustCompile(`\$\d`)

func TestWorkflowSetColumnsBindsEachColumnItsOwnValue(t *testing.T) {
	stamp := time.Unix(1700000000, 123_000_000).UTC()
	set := &setColumns{}
	set.set("status", "running")
	set.set("queued_at", dbNow{})
	set.set("completed_at", nil)
	set.set("deadline_at", nullTimeArg(sql.NullTime{Time: stamp, Valid: true}))
	set.set("queue_mode", "bootstrap")
	set.set("paused", false)
	query, args := set.query("tasks", "id = ? AND deleted_at IS NULL", int64(42))

	if placeholderDollar.MatchString(query) {
		t.Fatalf("语句里残留 $n 占位符：\n%s", query)
	}
	// 每个 ? 必须有一个绑定值，且顺序一致（WHERE 的参数排在最后）。
	if got, want := strings.Count(query, "?"), len(args); got != want {
		t.Fatalf("占位符 %d 个、绑定值 %d 个：\n%s\n%v", got, want, query, args)
	}
	for _, want := range []string{"status = ?", "queued_at = NOW(3)", "completed_at = NULL", "deadline_at = ?", "queue_mode = ?", "paused = ?", "UPDATE tasks", "WHERE id = ? AND deleted_at IS NULL"} {
		if !strings.Contains(query, want) {
			t.Fatalf("缺 SET 片段 %s：\n%s", want, query)
		}
	}
	if len(args) != 5 || args[0] != "running" {
		t.Fatalf("args 首项异常：%v", args)
	}
	if got, ok := args[1].(time.Time); !ok || !got.Equal(stamp) {
		t.Fatalf("args[1] 应是 deadline_at 的原值： %#v", args[1])
	}
	if args[2] != "bootstrap" {
		t.Fatalf("args[2] 应是 queue_mode： %v", args[2])
	}
	if args[3] != false {
		t.Fatalf("args[3] 应是 paused： %v", args[3])
	}
	if args[4] != int64(42) {
		t.Fatalf("WHERE 参数必须排在最后： %v", args)
	}
	// SET 的每一段右侧只能是 ?、NULL、NOW(3) 之一 —— 结构上就不可能引用同一语句
	// 里被改写过的列，因此不依赖 MySQL（左到右读新值）或 TiDB（读旧行）的求值口径。
	body := query[strings.Index(query, "SET ")+len("SET ") : strings.Index(query, "\nWHERE ")]
	for _, clause := range strings.Split(body, ",\n    ") {
		parts := strings.SplitN(clause, " = ", 2)
		if len(parts) != 2 {
			t.Fatalf("SET 片段异常：%q", clause)
		}
		switch rhs := strings.TrimSpace(parts[1]); rhs {
		case "?", "NULL", "NOW(3)":
		default:
			t.Fatalf("SET 片段右侧引用了别处（%s = %s），可能依赖求值顺序", parts[0], rhs)
		}
	}
}

func TestWorkflowNullTimeArgKeepsNullAndValue(t *testing.T) {
	if got := nullTimeArg(sql.NullTime{}); got != nil {
		t.Fatalf("无效时间必须绑成 NULL：%v", got)
	}
	stamp := time.Unix(1700000999, 500_000).UTC()
	got, ok := nullTimeArg(sql.NullTime{Time: stamp, Valid: true}).(time.Time)
	if !ok || !got.Equal(stamp) {
		t.Fatalf("有效时间必须原样绑定：%v %v", got, ok)
	}
}
