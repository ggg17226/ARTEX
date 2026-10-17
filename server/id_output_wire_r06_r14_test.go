package server

// W4b-2b（P03 输出侧 string 化 · 第二批）离线 wire 断言：
// 覆盖 R06–R14 的 db 输出结构，以及 R08 在 server 侧的 assetWire 覆盖。
// 口径：
//  1. 大于 2^53 的 64 位 ID 必须序列化为**带引号的 string**，且十进制数值不失真
//     （9007199254740993 / 9223372036854775807 原样回读）；
//  2. 非 ID 字段（port / status_code / content_length / count / total / page /
//     asset_count / self / attempts 等）保持 JSON number，即便数值很大；
//  3. omitempty 的 nil / 0 语义不变（继续省略）；无 omitempty 的 *int64 nil 仍是 null；
//  4. assetWire.task_ids 是字符串数组，外层同名字段确实屏蔽了嵌入的 []int64
//     （输出里 task_ids 只出现一次），nil / 空切片的 null / [] 语义不变；
//  5. 同一结构体 Marshal → Unmarshal 能无损回读（,string 的解码对称性）。
//
// 不依赖数据库，纯 json.Marshal / json.Unmarshal 断言。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

func ptrID64(v int64) *int64 { return &v }

// assertStringArray 断言 key 的值是**只含带引号十进制 string** 的数组（顺序一致），
// 且没有任何一处该 key 以数字数组形态出现。
func assertStringArray(t *testing.T, raw, key string, want ...string) {
	t.Helper()
	tokens := make([]string, len(want))
	for i, w := range want {
		tokens[i] = `"` + w + `"`
	}
	wantToken := `"` + key + `":[` + strings.Join(tokens, ",") + `]`
	if !strings.Contains(raw, wantToken) {
		t.Fatalf("expected %s in %s", wantToken, raw)
	}
	prefix := `"` + key + `":[`
	for i := 0; i+len(prefix) <= len(raw); i++ {
		if !strings.HasPrefix(raw[i:], prefix) {
			continue
		}
		next := raw[i+len(prefix)]
		if next != '"' && next != ']' {
			t.Fatalf("key %s emitted as number array in %s", key, raw)
		}
	}
}

// assertNullField 断言 key 出现且值是 JSON null（不是被 omit）。
func assertNullField(t *testing.T, raw, key string) {
	t.Helper()
	if !strings.Contains(raw, `"`+key+`":null`) {
		t.Fatalf("expected null %s in %s", key, raw)
	}
}

// assertKeyCount 断言 key 在输出里出现的次数（用于验证嵌入字段被外层覆盖，不重复）。
func assertKeyCount(t *testing.T, raw, key string, want int) {
	t.Helper()
	got := strings.Count(raw, `"`+key+`":`)
	if got != want {
		t.Fatalf("key %s appears %d times, want %d, in %s", key, got, want, raw)
	}
}

// roundTrip 断言 v 序列化后能按同类型无损回读。
func roundTrip[T any](t *testing.T, v T) {
	t.Helper()
	raw := mustMarshal(t, v)
	var back T
	if err := json.Unmarshal([]byte(raw), &back); err != nil {
		t.Fatalf("unmarshal %T from %s: %v", v, raw, err)
	}
	b1, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	b2, err := json.Marshal(back)
	if err != nil {
		t.Fatalf("marshal round-tripped %T: %v", v, err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("round trip changed %T: %s vs %s", v, b1, b2)
	}
}

// R06 / R07：Company.ID 与 ScopeRule.ID / CompanyID → string；asset_count 保持 number。
// 返回链：listCompanies（CompanyWithScope 嵌入 Company）、getCompany（company + scope）。
func TestIDOutputWireR06R07CompanyAndScopeRule(t *testing.T) {
	withScope := db.CompanyWithScope{
		Company:    db.Company{ID: bigID, Name: "Acme", NKey: "acme"},
		Scope:      []db.ScopeRule{{ID: maxInt64, CompanyID: bigID, Kind: "keyword", Value: "acme", Raw: "acme"}},
		AssetCount: 42,
	}
	raw := mustMarshal(t, withScope)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "company_id", bigIDStr)
	assertNumberField(t, raw, "asset_count", 42)
	roundTrip(t, withScope)

	// getCompany 的 map 形状（company + scope）同样受 db tag 保护。
	rawMap := mustMarshal(t, map[string]any{
		"company": db.Company{ID: maxInt64, Name: "Big"},
		"scope":   []db.ScopeRule{{ID: bigID, CompanyID: maxInt64, Kind: "ip", Net: "1.1.1.1", Raw: "1.1.1.1"}},
	})
	assertStringID(t, rawMap, "id", maxIDStr)
	assertStringID(t, rawMap, "company_id", maxIDStr)

	// 无 omitempty：零值 ID 仍是带引号的 "0"，不会变成 number 0，也不会被 omit。
	rawZero := mustMarshal(t, db.Company{ID: 0, Name: "zero"})
	assertStringID(t, rawZero, "id", "0")
	roundTrip(t, db.ScopeRule{ID: bigID, CompanyID: maxInt64, Kind: "domain", Domain: "a.com", Raw: "a.com"})
}

// R08：Asset 的三个标量 ID → string；port / status_code / content_length 保持 number；
// db.Asset.TaskIDs 仍是 number 数组（由 assets.task_ids 列回读，不能改），
// HTTP 输出由 assetWire 覆盖成字符串数组。
func TestIDOutputWireR08AssetAndAssetWire(t *testing.T) {
	asset := db.Asset{
		ID:               bigID,
		Type:             "service",
		CompanyID:        ptrID64(maxInt64),
		TaskIDs:          []int64{bigID, 1, maxInt64},
		Domain:           "a.example.com",
		Port:             ptrInt(8443),
		StatusCode:       ptrInt(200),
		ContentLength:    ptrID64(maxInt64),
		LastSeen:         "2026-10-18T00:00:00Z",
		TaskSource:       "agent",
		TaskSourceNodeID: ptrID64(maxInt64),
	}
	raw := mustMarshal(t, asset)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "company_id", maxIDStr)
	assertStringID(t, raw, "task_source_node_id", maxIDStr)
	// 非 ID 的大数值保持 number。
	assertNumberField(t, raw, "content_length", maxInt64)
	assertNumberField(t, raw, "port", 8443)
	assertNumberField(t, raw, "status_code", 200)
	// db 层 TaskIDs 仍是数字数组：[]int64 不能加 ,string（Marshal 会报错）。
	if !strings.Contains(raw, `"task_ids":[`+bigIDStr+`,1,`+maxIDStr+`]`) {
		t.Fatalf("db.Asset.task_ids must stay a number array: %s", raw)
	}
	roundTrip(t, asset)

	// wire 覆盖：task_ids 是字符串数组，且只出现一次（外层屏蔽嵌入字段）。
	wire := assetWire{Asset: asset, TaskIDs: taskIDStrings(asset.TaskIDs)}
	rawWire := mustMarshal(t, wire)
	assertKeyCount(t, rawWire, "task_ids", 1)
	assertStringArray(t, rawWire, "task_ids", bigIDStr, "1", maxIDStr)
	assertStringID(t, rawWire, "id", bigIDStr)
	assertStringID(t, rawWire, "company_id", maxIDStr)
	assertNumberField(t, rawWire, "content_length", maxInt64)

	// listAssets 的整包形状：count / total 是 number（total 是大数值也不 string 化）。
	rawList := mustMarshal(t, map[string]any{
		"count":  1,
		"total":  maxInt64,
		"assets": assetWires([]*db.Asset{&asset}),
	})
	assertNumberField(t, rawList, "count", 1)
	assertNumberField(t, rawList, "total", maxInt64)
	assertStringArray(t, rawList, "task_ids", bigIDStr, "1", maxIDStr)

	// nil / 空切片语义：nil → null，[] → []（idStrings 会把 nil 变成 []，故单独处理）。
	rawNil := mustMarshal(t, assetWire{Asset: db.Asset{ID: 1}, TaskIDs: taskIDStrings(nil)})
	assertNullField(t, rawNil, "task_ids")
	rawEmpty := mustMarshal(t, assetWire{Asset: db.Asset{ID: 1}, TaskIDs: taskIDStrings([]int64{})})
	if !strings.Contains(rawEmpty, `"task_ids":[]`) {
		t.Fatalf("empty task_ids must stay []: %s", rawEmpty)
	}
	// company_id / task_source_node_id 为 nil 时仍按 omitempty 省略。
	assertOmitted(t, rawNil, "company_id")
	assertOmitted(t, rawNil, "task_source_node_id")
	// assetWires 跳过 nil 元素，空输入是 []。
	if got := assetWires([]*db.Asset{nil, &asset}); len(got) != 1 {
		t.Fatalf("assetWires nil element: %d", len(got))
	}
	if got := mustMarshal(t, assetWires(nil)); got != "[]" {
		t.Fatalf("assetWires(nil)=%s", got)
	}
}

// R09：IntentAsset 的 intent_id / asset_id / source_task_id → string；
// source_node_id 为 nil 时仍 omit；inherited 保持 bool。
func TestIDOutputWireR09IntentAsset(t *testing.T) {
	withNode := db.IntentAsset{
		IntentID:      bigID,
		AssetID:       maxInt64,
		Type:          "subdomain",
		Label:         "a.example.com",
		Source:        "anchor",
		SourceSummary: "s",
		SourceNodeID:  ptrID64(bigID),
		SourceTaskID:  maxInt64,
		Inherited:     true,
	}
	raw := mustMarshal(t, withNode)
	assertStringID(t, raw, "intent_id", bigIDStr)
	assertStringID(t, raw, "asset_id", maxIDStr)
	assertStringID(t, raw, "source_task_id", maxIDStr)
	assertStringID(t, raw, "source_node_id", bigIDStr)
	roundTrip(t, withNode)

	rawNil := mustMarshal(t, db.IntentAsset{IntentID: bigID, AssetID: maxInt64, SourceTaskID: 1})
	assertOmitted(t, rawNil, "source_node_id")
	assertStringID(t, rawNil, "source_task_id", "1")
	roundTrip(t, db.IntentAsset{IntentID: bigID, AssetID: maxInt64, SourceTaskID: 1})

	// taskIntentAssets 的 {assets:[...]} 包装同样受保护。
	rawWrap := mustMarshal(t, map[string]any{"assets": []db.IntentAsset{withNode}})
	assertStringID(t, rawWrap, "intent_id", bigIDStr)
}

// R10：TaskScope 的 id / task_id → string；company_id nil 仍 omit、非 nil string。
func TestIDOutputWireR10TaskScope(t *testing.T) {
	row := db.TaskScope{ID: bigID, TaskID: maxInt64, Kind: "company", CompanyID: ptrID64(bigID), CompanyName: "Acme", Source: "auto", Reason: "r"}
	raw := mustMarshal(t, row)
	assertStringID(t, raw, "id", bigIDStr)
	assertStringID(t, raw, "task_id", maxIDStr)
	assertStringID(t, raw, "company_id", bigIDStr)
	roundTrip(t, row)

	rawNil := mustMarshal(t, db.TaskScope{ID: maxInt64, TaskID: bigID, Kind: "ip", Net: "1.1.1.1", Source: "agent"})
	assertOmitted(t, rawNil, "company_id")
	assertStringID(t, rawNil, "id", maxIDStr)
	// taskScopeList 的 {scope:[...]} 包装。
	rawWrap := mustMarshal(t, map[string]any{"scope": []db.TaskScope{row}})
	assertStringID(t, rawWrap, "task_id", maxIDStr)
}

// R11：CoverageGraphNode 的 asset_id / company_id → string；port / status_code 保持 number；
// 0 值仍按 omitempty 省略（company / 合成根节点没有 asset_id）。
func TestIDOutputWireR11CoverageGraphNode(t *testing.T) {
	node := db.CoverageGraphNode{
		Key: "a:" + bigIDStr, Kind: "service", Label: "a.example.com:8443",
		Tested: true, InScope: true, AssetID: bigID, CompanyID: maxInt64,
		Port: 8443, StatusCode: 200, PageTitle: "t",
	}
	raw := mustMarshal(t, node)
	assertStringID(t, raw, "asset_id", bigIDStr)
	assertStringID(t, raw, "company_id", maxIDStr)
	assertNumberField(t, raw, "port", 8443)
	assertNumberField(t, raw, "status_code", 200)
	roundTrip(t, node)

	// 合成根域名 / company 节点：0 值继续被 omit，不会输出 "0"。
	rawZero := mustMarshal(t, db.CoverageGraphNode{Key: "r:example.com", Kind: "root_domain", Label: "example.com"})
	assertOmitted(t, rawZero, "asset_id")
	assertOmitted(t, rawZero, "company_id")
	assertOmitted(t, rawZero, "port")
	assertOmitted(t, rawZero, "status_code")

	// taskCoverageGraph 返回 CoverageGraphData.Nodes。
	rawGraph := mustMarshal(t, db.CoverageGraphData{
		Nodes: []db.CoverageGraphNode{node},
		Edges: []db.CoverageGraphEdge{{Src: node.Key, Dst: "c:1"}},
	})
	assertStringID(t, rawGraph, "asset_id", bigIDStr)
}

// R12：FindingAssetNode 的 asset_id / company_id → string；计数保持 number；0 值仍 omit。
func TestIDOutputWireR12FindingAssetNode(t *testing.T) {
	node := db.FindingAssetNode{
		Key: "a:" + bigIDStr, Parent: "c:" + maxIDStr, Kind: "service", Label: "a.example.com",
		AssetID: bigID, CompanyID: maxInt64, Self: 1, Total: 7, Critical: 1, High: 2, Medium: 3, Low: 4,
	}
	raw := mustMarshal(t, node)
	assertStringID(t, raw, "asset_id", bigIDStr)
	assertStringID(t, raw, "company_id", maxIDStr)
	assertNumberField(t, raw, "self", 1)
	assertNumberField(t, raw, "total", 7)
	roundTrip(t, node)

	rawZero := mustMarshal(t, db.FindingAssetNode{Key: "r:example.com", Kind: "root_domain", Label: "example.com"})
	assertOmitted(t, rawZero, "asset_id")
	assertOmitted(t, rawZero, "company_id")

	// findingAssetTree 返回 FindingAssetTree。
	rawTree := mustMarshal(t, db.FindingAssetTree{Nodes: []db.FindingAssetNode{node}, FindingTotal: 7})
	assertStringID(t, rawTree, "asset_id", bigIDStr)
	assertNumberField(t, rawTree, "finding_total", 7)
}

// R13：FindingGroup.task_id → string；nil 仍是 null（不是 omit、不是 0）；计数保持 number。
func TestIDOutputWireR13FindingGroup(t *testing.T) {
	group := db.FindingGroup{TaskID: ptrID64(bigID), TaskName: "n", TaskDescription: "d", TaskStatus: "done", Count: 5, Critical: 1, High: 2}
	raw := mustMarshal(t, group)
	assertStringID(t, raw, "task_id", bigIDStr)
	assertNumberField(t, raw, "count", 5)
	roundTrip(t, group)

	// 未分配 / 任务已删的桶：nil → null，语义不变。
	rawNil := mustMarshal(t, db.FindingGroup{TaskID: nil, Count: 3})
	assertNullField(t, rawNil, "task_id")
	roundTrip(t, db.FindingGroup{TaskID: nil, Count: 3})

	// findingGroups 的响应包装：items 内 string、page / total 等计数保持 number。
	rawPage := mustMarshal(t, map[string]any{
		"items":         []db.FindingGroup{group},
		"total":         2,
		"finding_total": 9007199254740993,
		"page":          1,
		"page_size":     10,
	})
	assertStringID(t, rawPage, "task_id", bigIDStr)
	assertNumberField(t, rawPage, "page", 1)
	assertNumberField(t, rawPage, "page_size", 10)
	assertNumberField(t, rawPage, "finding_total", bigID)
}

// R14：FindingTaskOption.id → string；count 保持 number；FindingStats 的聚合计数保持 number。
func TestIDOutputWireR14FindingTaskOption(t *testing.T) {
	stats := db.FindingStats{
		Total: 12, Pending: 3, Critical: 1, High: 2, Medium: 3, Low: 4,
		VulnClasses: []string{"sqli"},
		Tasks: []db.FindingTaskOption{
			{ID: bigID, Name: "big", Description: "d", Count: 2},
			{ID: maxInt64, Name: "max", Count: 1},
		},
	}
	raw := mustMarshal(t, stats)
	assertStringID(t, raw, "id", bigIDStr)
	if !strings.Contains(raw, `"id":"`+maxIDStr+`"`) {
		t.Fatalf("second task id must be string too: %s", raw)
	}
	assertNumberField(t, raw, "count", 2)
	assertNumberField(t, raw, "total", 12)
	assertNumberField(t, raw, "pending", 3)
	if strings.Contains(raw, `"total":"`) || strings.Contains(raw, `"pending":"`) {
		t.Fatalf("aggregate counters must stay numbers: %s", raw)
	}
	roundTrip(t, stats)

	// 归档 aggregate_stats 的 finding_stats 段只有 7 个聚合键、没有 tasks：
	// 加 ,string 后该形状仍能无损回读。
	var back db.FindingStats
	if err := json.Unmarshal([]byte(`{"total":1,"pending":0,"critical":1,"high":0,"medium":0,"low":0,"vulnclasses":["sqli"]}`), &back); err != nil {
		t.Fatalf("archive finding_stats shape must decode: %v", err)
	}
	if back.Total != 1 || len(back.Tasks) != 0 {
		t.Fatalf("unexpected decode: %+v", back)
	}
}
