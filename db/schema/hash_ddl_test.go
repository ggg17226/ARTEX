package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"
)

// parseCanonicalField 是测试专用解析结构，用于解构验证 identity payload。
type parseCanonicalField struct {
	tag     byte
	isNull  bool
	rawLen  uint64
	payload []byte
}

// parseCanonicalIdentity 是测试专用小型解析器，不复用生产实现编码逻辑，
// 对 canonical 编码做独立的反向解码断言。
func parseCanonicalIdentity(t *testing.T, b []byte) (byte, string, []parseCanonicalField) {
	t.Helper()
	if len(b) < 3 {
		t.Fatalf("canonical identity too short: %d bytes", len(b))
	}
	ver := b[0]
	branchLen := binary.BigEndian.Uint16(b[1:3])
	offset := 3
	if len(b) < offset+int(branchLen) {
		t.Fatalf("canonical identity truncated in branch name: got total %d, need offset %d", len(b), offset+int(branchLen))
	}
	branch := string(b[offset : offset+int(branchLen)])
	offset += int(branchLen)

	var fields []parseCanonicalField
	for offset < len(b) {
		if len(b) < offset+10 {
			t.Fatalf("canonical identity truncated in field header at offset %d", offset)
		}
		tag := b[offset]
		marker := b[offset+1]
		length := binary.BigEndian.Uint64(b[offset+2 : offset+10])
		offset += 10

		if marker != fieldNotNull && marker != fieldNull {
			t.Fatalf("invalid null marker: 0x%02x at offset %d", marker, offset-9)
		}
		isNull := (marker == fieldNull)
		if isNull && length != 0 {
			t.Fatalf("null field must have payload length 0, got %d", length)
		}
		if len(b) < offset+int(length) {
			t.Fatalf("canonical identity truncated in field payload: got total %d, need %d", len(b), offset+int(length))
		}
		payload := b[offset : offset+int(length)]
		offset += int(length)

		fields = append(fields, parseCanonicalField{
			tag:     tag,
			isNull:  isNull,
			rawLen:  length,
			payload: payload,
		})
	}

	return ver, branch, fields
}

// 1. 保留原简报 bytes 精确向量测试
func TestCanonicalIdentityBytesEncoding(t *testing.T) {
	got := CanonicalIdentity("ip", FieldBytes("1.2.3.4", false))
	want := []byte{
		0x01,       // 版本
		0x00, 0x02, // 分支名长度 2
		'i', 'p', // 分支名
		0x02,                                           // 字段 tag = bytes
		0x00,                                           // 非 NULL
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // 长度 7
		'1', '.', '2', '.', '3', '.', '4',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("CanonicalIdentity() = %x, want %x", got, want)
	}
}

// 2. C1 修复：U64 精确向量为完整 8 字节 BE（00 00 00 00 00 00 00 07），总 38 字节；
// 并覆盖 U64 边界值 0 与 MaxUint64。
func TestCanonicalIdentityU64Encoding(t *testing.T) {
	// 基础向量：scope + u64(7) + bytes("ip")
	got := CanonicalIdentity("scope", FieldU64(7, false), FieldBytes("ip", false))
	want := []byte{
		0x01,                                // version
		0x00, 0x05, 's', 'c', 'o', 'p', 'e', // branch "scope" (5 bytes)
		0x01,                                           // fieldTagU64
		0x00,                                           // fieldNotNull
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // length 8
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // payload 7 (8 bytes BE)
		0x02,                                           // fieldTagBytes
		0x00,                                           // fieldNotNull
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // length 2
		'i', 'p', // payload "ip"
	}
	if len(got) != 38 {
		t.Fatalf("u64 精确向量总长度期望 38 字节，实际得到 %d 字节", len(got))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("u64 精确向量不一致:\ngot:  %x\nwant: %x", got, want)
	}

	// 覆盖 U64 边界值 0
	gotZero := CanonicalIdentity("z", FieldU64(0, false))
	wantZero := []byte{
		0x01,
		0x00, 0x01, 'z',
		0x01,
		0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(gotZero, wantZero) {
		t.Fatalf("u64(0) 向量不一致:\ngot:  %x\nwant: %x", gotZero, wantZero)
	}

	// 覆盖 U64 边界值 MaxUint64
	gotMax := CanonicalIdentity("m", FieldU64(math.MaxUint64, false))
	wantMax := []byte{
		0x01,
		0x00, 0x01, 'm',
		0x01,
		0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
	if !bytes.Equal(gotMax, wantMax) {
		t.Fatalf("u64(MaxUint64) 向量不一致:\ngot:  %x\nwant: %x", gotMax, wantMax)
	}
}

// 3. C2 强制修订：NULL 的 Value 忽略契约、长度 0、无 payload；
// 区分 NULL 与空串/数值 0，验证脏占位值完全被忽略。
func TestCanonicalIdentityNullRules(t *testing.T) {
	// A. NULL bytes 精确静态向量（14 字节：1 ver + 2 branchLen + 1 branch + 1 tag + 1 marker + 8 len(0) + 0 payload）
	wantNullBytes := []byte{
		0x01,
		0x00, 0x01, 'b',
		0x02,                                           // fieldTagBytes
		0x01,                                           // fieldNull
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // length 0
	}
	gotNullEmpty := CanonicalIdentity("b", FieldBytes("", true))
	if !bytes.Equal(gotNullEmpty, wantNullBytes) {
		t.Fatalf("FieldBytes('', true) 精确向量不一致:\ngot:  %x\nwant: %x", gotNullEmpty, wantNullBytes)
	}

	// 带有占位非空串的 NULL bytes 必须完全忽略 Value 内容和长度，生成完全相同的 14 字节
	gotNullPlaceholder := CanonicalIdentity("b", FieldBytes("dirty_placeholder_string", true))
	if !bytes.Equal(gotNullPlaceholder, wantNullBytes) {
		t.Fatalf("FieldBytes('dirty', true) 必须忽略 Value 并产出相同字节:\ngot:  %x\nwant: %x", gotNullPlaceholder, wantNullBytes)
	}

	// 直接构造 IdentityField 带脏 Value 且 IsNull=true
	gotDirectDirtyBytes := CanonicalIdentity("b", IdentityField{
		Tag:    fieldTagBytes,
		Value:  []byte("arbitrary-payload-bytes"),
		IsNull: true,
	})
	if !bytes.Equal(gotDirectDirtyBytes, wantNullBytes) {
		t.Fatalf("直接构造带有脏 Value 的 IsNull=true bytes 字段未被正确忽略:\ngot:  %x\nwant: %x", gotDirectDirtyBytes, wantNullBytes)
	}

	// B. NULL uint64 精确静态向量（14 字节）
	wantNullU64 := []byte{
		0x01,
		0x00, 0x01, 'b',
		0x01,                                           // fieldTagU64
		0x01,                                           // fieldNull
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // length 0
	}
	gotNullU64Zero := CanonicalIdentity("b", FieldU64(0, true))
	if !bytes.Equal(gotNullU64Zero, wantNullU64) {
		t.Fatalf("FieldU64(0, true) 精确向量不一致:\ngot:  %x\nwant: %x", gotNullU64Zero, wantNullU64)
	}

	gotNullU64Seven := CanonicalIdentity("b", FieldU64(7, true))
	if !bytes.Equal(gotNullU64Seven, wantNullU64) {
		t.Fatalf("FieldU64(7, true) 必须忽略数值并产出相同字节:\ngot:  %x\nwant: %x", gotNullU64Seven, wantNullU64)
	}

	gotNullU64Max := CanonicalIdentity("b", FieldU64(math.MaxUint64, true))
	if !bytes.Equal(gotNullU64Max, wantNullU64) {
		t.Fatalf("FieldU64(MaxUint64, true) 必须忽略数值并产出相同字节:\ngot:  %x\nwant: %x", gotNullU64Max, wantNullU64)
	}

	gotDirectDirtyU64 := CanonicalIdentity("b", IdentityField{
		Tag:    fieldTagU64,
		Value:  []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04},
		IsNull: true,
	})
	if !bytes.Equal(gotDirectDirtyU64, wantNullU64) {
		t.Fatalf("直接构造带有脏 Value 的 IsNull=true u64 字段未被正确忽略:\ngot:  %x\nwant: %x", gotDirectDirtyU64, wantNullU64)
	}

	// C. 空串 vs NULL 必须可区分
	emptyStrID := CanonicalIdentity("b", FieldBytes("", false))
	nullStrID := CanonicalIdentity("b", FieldBytes("", true))
	if bytes.Equal(emptyStrID, nullStrID) {
		t.Fatal("空串与 NULL 必须编码不同（marker 0x00 vs 0x01）")
	}
	// emptyStr 长度为 14 字节，marker=0x00，len=0
	wantEmptyStr := []byte{
		0x01,
		0x00, 0x01, 'b',
		0x02,
		0x00, // fieldNotNull
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(emptyStrID, wantEmptyStr) {
		t.Fatalf("空串编码不一致:\ngot:  %x\nwant: %x", emptyStrID, wantEmptyStr)
	}

	// D. 数值 0 vs NULL U64 必须可区分
	zeroU64ID := CanonicalIdentity("b", FieldU64(0, false))
	nullU64ID := CanonicalIdentity("b", FieldU64(0, true))
	if bytes.Equal(zeroU64ID, nullU64ID) {
		t.Fatal("数值 0 与 NULL uint64 必须编码不同")
	}
	if len(zeroU64ID) != 22 {
		t.Fatalf("数值 0 编码长度期望 22 字节，实际得到 %d 字节", len(zeroU64ID))
	}
	if len(nullU64ID) != 14 {
		t.Fatalf("NULL uint64 编码长度期望 14 字节，实际得到 %d 字节", len(nullU64ID))
	}
}

// 4. Unicode 原字节长度与字段/分支长度前缀隔离（避免分隔符歧义）
func TestCanonicalIdentityUnicodeAndPrefixIsolation(t *testing.T) {
	// Unicode 原字节长度验证："京" 占 3 字节 UTF-8 (0xe4, 0xba, 0xac)，
	// "京ICP00000000" 总字节长度为 3 + 11 = 14 字节，而非字符数 12
	rawStr := "京ICP00000000"
	got := CanonicalIdentity("val", FieldBytes(rawStr, false))
	ver, branch, fields := parseCanonicalIdentity(t, got)
	if ver != 0x01 || branch != "val" {
		t.Fatalf("header mismatch: ver=%d, branch=%s", ver, branch)
	}
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	if fields[0].rawLen != 14 {
		t.Fatalf("Unicode 字符串原字节长度期望 14，得到 %d", fields[0].rawLen)
	}
	if string(fields[0].payload) != rawStr {
		t.Fatalf("Unicode payload 不匹配: %s != %s", string(fields[0].payload), rawStr)
	}

	// 分隔符歧义隔离验证：
	// 组合 1：field1="ab", field2="c"
	id1 := CanonicalIdentity("test", FieldBytes("ab", false), FieldBytes("c", false))
	// 组合 2：field1="a", field2="bc"
	id2 := CanonicalIdentity("test", FieldBytes("a", false), FieldBytes("bc", false))
	if bytes.Equal(id1, id2) {
		t.Fatal("不同字段边界编码不应碰撞（必须被 8 字节长度前缀精确隔离）")
	}

	// 分支名隔离验证：
	// branch="ab", field="c" vs branch="a", field="bc"
	b1 := CanonicalIdentity("ab", FieldBytes("c", false))
	b2 := CanonicalIdentity("a", FieldBytes("bc", false))
	if bytes.Equal(b1, b2) {
		t.Fatal("不同分支名编码不应碰撞（必须被 2 字节分支长度前缀精确隔离）")
	}
}

// 5. SHA-256 哈希计算契约验证
func TestCanonicalHashSHA256(t *testing.T) {
	id := CanonicalIdentity("key", FieldBytes("abc", false))
	got := CanonicalHash(id)
	want := sha256.Sum256(id)
	if got != want {
		t.Fatalf("CanonicalHash = %x, want %x", got, want)
	}

	// 确认 CanonicalHash 对整个 canonical payload 做 hash，不截断不加盐
	if len(got) != 32 {
		t.Fatalf("CanonicalHash 长度必须为 32 字节，得到 %d", len(got))
	}
}

// 6. LLMProfileNameIdentity 独立精确字节/branch 证据
func TestLLMProfileNameIdentityExact(t *testing.T) {
	got := LLMProfileNameIdentity("qwen-max")
	// 手算精确静态 want：
	// 1 byte version: 0x01
	// 2 bytes branchLen: 0x00, 0x10 (16 bytes)
	// 16 bytes branch: "llm_profile_name"
	// 1 byte tag: 0x02 (fieldTagBytes)
	// 1 byte marker: 0x00 (fieldNotNull)
	// 8 bytes len: 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08
	// 8 bytes payload: "qwen-max"
	// 总 1 + 2 + 16 + 1 + 1 + 8 + 8 = 37 字节
	want := []byte{
		0x01,
		0x00, 0x10,
		'l', 'l', 'm', '_', 'p', 'r', 'o', 'f', 'i', 'l', 'e', '_', 'n', 'a', 'm', 'e',
		0x02,
		0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
		'q', 'w', 'e', 'n', '-', 'm', 'a', 'x',
	}
	if len(got) != 37 {
		t.Fatalf("LLMProfileNameIdentity 精确长度期望 37 字节，得到 %d", len(got))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("LLMProfileNameIdentity 精确字节不匹配:\ngot:  %x\nwant: %x", got, want)
	}

	// SHA256 哈希精确性
	hashGot := CanonicalHash(got)
	hashWant := sha256.Sum256(want)
	if hashGot != hashWant {
		t.Fatalf("LLMProfileNameIdentity SHA256 不匹配:\ngot:  %x\nwant: %x", hashGot, hashWant)
	}
}

// 7. 测试全部 22 个 table identity builders：
// 每个独立固定分支、字段顺序、tag、value、NULL 预期，使用测试解析器严格反向检验。
func TestTableIdentityBuildersAll22(t *testing.T) {
	type expectedField struct {
		tag    byte
		isNull bool
		u64Val uint64
		strVal string
	}

	testCases := []struct {
		name       string
		gotPayload []byte
		wantBranch string
		wantFields []expectedField
	}{
		// 1. NKeyIdentity
		{
			name:       "NKeyIdentity",
			gotPayload: NKeyIdentity("acme"),
			wantBranch: "nkey",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "acme"},
			},
		},
		// 2. SettingsKeyIdentity
		{
			name:       "SettingsKeyIdentity",
			gotPayload: SettingsKeyIdentity("theme"),
			wantBranch: "key",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "theme"},
			},
		},
		// 3. AgentKeyIdentity
		{
			name:       "AgentKeyIdentity",
			gotPayload: AgentKeyIdentity("worker"),
			wantBranch: "agent_key",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "worker"},
			},
		},
		// 4. MCPServerNameIdentity
		{
			name:       "MCPServerNameIdentity",
			gotPayload: MCPServerNameIdentity("gh"),
			wantBranch: "mcp_server_name",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "gh"},
			},
		},
		// 5. AgentPromptVarIdentity
		{
			name:       "AgentPromptVarIdentity",
			gotPayload: AgentPromptVarIdentity(7, "api_base"),
			wantBranch: "agent_prompt_var",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 7},
				{tag: fieldTagBytes, isNull: false, strVal: "api_base"},
			},
		},
		// 6. MCPToolCacheIdentity
		{
			name:       "MCPToolCacheIdentity",
			gotPayload: MCPToolCacheIdentity(9, "search"),
			wantBranch: "mcp_tool_cache",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 9},
				{tag: fieldTagBytes, isNull: false, strVal: "search"},
			},
		},
		// 7. AgentVisibilityIdentity
		{
			name:       "AgentVisibilityIdentity",
			gotPayload: AgentVisibilityIdentity(3, "mcp_server", 5, ""),
			wantBranch: "agent_visibility",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 3},
				{tag: fieldTagBytes, isNull: false, strVal: "mcp_server"},
				{tag: fieldTagU64, isNull: false, u64Val: 5},
				{tag: fieldTagBytes, isNull: false, strVal: ""},
			},
		},
		// 8. AgentSkillVisibilityIdentity
		{
			name:       "AgentSkillVisibilityIdentity",
			gotPayload: AgentSkillVisibilityIdentity(3, "scrape"),
			wantBranch: "agent_skill_visibility",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 3},
				{tag: fieldTagBytes, isNull: false, strVal: "scrape"},
			},
		},
		// 9. SideQuestionRequestsIdentity
		{
			name:       "SideQuestionRequestsIdentity",
			gotPayload: SideQuestionRequestsIdentity("sess-1", 4, "client-9"),
			wantBranch: "side_question_request",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "sess-1"},
				{tag: fieldTagU64, isNull: false, u64Val: 4},
				{tag: fieldTagBytes, isNull: false, strVal: "client-9"},
			},
		},
		// 10. TaskScopeIdentity (COALESCE 由调用方完成：domain/net/value 空串, companyID=0)
		{
			name:       "TaskScopeIdentity",
			gotPayload: TaskScopeIdentity(5, "ip", "", "10.0.0.0/8", 0, ""),
			wantBranch: "task_scope",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 5},
				{tag: fieldTagBytes, isNull: false, strVal: "ip"},
				{tag: fieldTagBytes, isNull: false, strVal: ""},
				{tag: fieldTagBytes, isNull: false, strVal: "10.0.0.0/8"},
				{tag: fieldTagU64, isNull: false, u64Val: 0},
				{tag: fieldTagBytes, isNull: false, strVal: ""},
			},
		},
		// 11. CompanyScopeDomainIdentity
		{
			name:       "CompanyScopeDomainIdentity",
			gotPayload: CompanyScopeDomainIdentity(2, "example.com"),
			wantBranch: "cscope_domain",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 2},
				{tag: fieldTagBytes, isNull: false, strVal: "example.com"},
			},
		},
		// 12. CompanyScopeNetIdentity
		{
			name:       "CompanyScopeNetIdentity",
			gotPayload: CompanyScopeNetIdentity(2, "10.0.0.0/8"),
			wantBranch: "cscope_net",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 2},
				{tag: fieldTagBytes, isNull: false, strVal: "10.0.0.0/8"},
			},
		},
		// 13. CompanyScopeValueIdentity
		{
			name:       "CompanyScopeValueIdentity",
			gotPayload: CompanyScopeValueIdentity(2, "icp", "京ICP00000000"),
			wantBranch: "cscope_value",
			wantFields: []expectedField{
				{tag: fieldTagU64, isNull: false, u64Val: 2},
				{tag: fieldTagBytes, isNull: false, strVal: "icp"},
				{tag: fieldTagBytes, isNull: false, strVal: "京ICP00000000"},
			},
		},
		// 14. AssetsRootDomainIdentity
		{
			name:       "AssetsRootDomainIdentity",
			gotPayload: AssetsRootDomainIdentity("example.com"),
			wantBranch: "assets_root_domain",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "example.com"},
			},
		},
		// 15. AssetsIPIdentity
		{
			name:       "AssetsIPIdentity",
			gotPayload: AssetsIPIdentity("1.2.3.4"),
			wantBranch: "assets_ip",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "1.2.3.4"},
			},
		},
		// 16. AssetsSubdomainIdentity
		{
			name:       "AssetsSubdomainIdentity",
			gotPayload: AssetsSubdomainIdentity("a.example.com", "A"),
			wantBranch: "assets_subdomain",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "a.example.com"},
				{tag: fieldTagBytes, isNull: false, strVal: "A"},
			},
		},
		// 17. AssetsAppBundleIdentity
		{
			name:       "AssetsAppBundleIdentity",
			gotPayload: AssetsAppBundleIdentity("com.example.app"),
			wantBranch: "assets_app_bundle",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "com.example.app"},
			},
		},
		// 18. AssetsAppNameIdentity
		{
			name:       "AssetsAppNameIdentity",
			gotPayload: AssetsAppNameIdentity("ExampleApp"),
			wantBranch: "assets_app_name",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "ExampleApp"},
			},
		},
		// 19. AssetsServiceHTTPIdentity
		{
			name:       "AssetsServiceHTTPIdentity",
			gotPayload: AssetsServiceHTTPIdentity("https://example.com"),
			wantBranch: "assets_service_http",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "https://example.com"},
			},
		},
		// 20. AssetsServiceOtherIdentity (port 为 NULL, serviceName 为非 NULL)
		{
			name:       "AssetsServiceOtherIdentity",
			gotPayload: AssetsServiceOtherIdentity("example.com", "", 0, true, "ssh", false),
			wantBranch: "assets_service_other",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "example.com"},
				{tag: fieldTagBytes, isNull: false, strVal: ""},
				{tag: fieldTagU64, isNull: true, u64Val: 0},
				{tag: fieldTagBytes, isNull: false, strVal: "ssh"},
			},
		},
		// 21. AssetsEndpointIdentity
		{
			name:       "AssetsEndpointIdentity",
			gotPayload: AssetsEndpointIdentity("https://example.com/api", "GET"),
			wantBranch: "assets_endpoint",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "https://example.com/api"},
				{tag: fieldTagBytes, isNull: false, strVal: "GET"},
			},
		},
		// 22. LLMProfileNameIdentity
		{
			name:       "LLMProfileNameIdentity",
			gotPayload: LLMProfileNameIdentity("claude-3-opus"),
			wantBranch: "llm_profile_name",
			wantFields: []expectedField{
				{tag: fieldTagBytes, isNull: false, strVal: "claude-3-opus"},
			},
		},
	}

	if len(testCases) != 22 {
		t.Fatalf("builder 测试用例数量必须为 22，当前为 %d", len(testCases))
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ver, branch, fields := parseCanonicalIdentity(t, tc.gotPayload)
			if ver != 0x01 {
				t.Errorf("%s: 期望版本 0x01，实际得到 0x%02x", tc.name, ver)
			}
			if branch != tc.wantBranch {
				t.Errorf("%s: 期望分支名 %s，实际得到 %s", tc.name, tc.wantBranch, branch)
			}
			if len(fields) != len(tc.wantFields) {
				t.Fatalf("%s: 期望字段数 %d，实际得到 %d", tc.name, len(tc.wantFields), len(fields))
			}
			for i, exp := range tc.wantFields {
				f := fields[i]
				if f.tag != exp.tag {
					t.Errorf("%s 字段 %d: 期望 tag 0x%02x，实际 0x%02x", tc.name, i, exp.tag, f.tag)
				}
				if f.isNull != exp.isNull {
					t.Errorf("%s 字段 %d: 期望 isNull=%v，实际 %v", tc.name, i, exp.isNull, f.isNull)
				}
				if exp.isNull {
					if f.rawLen != 0 {
						t.Errorf("%s 字段 %d: NULL 字段 rawLen 必须为 0，得到 %d", tc.name, i, f.rawLen)
					}
					if len(f.payload) != 0 {
						t.Errorf("%s 字段 %d: NULL 字段 payload 长度必须为 0，得到 %d", tc.name, i, len(f.payload))
					}
				} else {
					if exp.tag == fieldTagU64 {
						if f.rawLen != 8 || len(f.payload) != 8 {
							t.Fatalf("%s 字段 %d: U64 payload 必须为 8 字节", tc.name, i)
						}
						val := binary.BigEndian.Uint64(f.payload)
						if val != exp.u64Val {
							t.Errorf("%s 字段 %d: 期望 u64 值为 %d，得到 %d", tc.name, i, exp.u64Val, val)
						}
					} else if exp.tag == fieldTagBytes {
						if string(f.payload) != exp.strVal {
							t.Errorf("%s 字段 %d: 期望 str 值为 %q，得到 %q", tc.name, i, exp.strVal, string(f.payload))
						}
					}
				}
			}
		})
	}
}
