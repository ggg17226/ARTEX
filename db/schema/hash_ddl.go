package schema

import (
	"crypto/sha256"
	"encoding/binary"
)

// identityVersion 是 canonical 编码版本号（spec §3.2：版本 + 分支名 + 字段类型 +
// NULL marker + 字节长度前缀 + 原字节）。
const identityVersion byte = 0x01

// 字段类型 tag。
const (
	fieldTagU64   byte = 0x01
	fieldTagBytes byte = 0x02
)

// NULL marker。
const (
	fieldNotNull byte = 0x00
	fieldNull    byte = 0x01
)

// IdentityField 是 canonical 编码的一个字段（运行期视图，与目录 HashField 分离）。
type IdentityField struct {
	Tag    byte // fieldTagU64 | fieldTagBytes
	Value  []byte
	IsNull bool
}

// FieldBytes 构造字节串字段。isNull=true 表示原值为 NULL 且未 COALESCE（Value 忽略）。
func FieldBytes(v string, isNull bool) IdentityField {
	return IdentityField{Tag: fieldTagBytes, Value: []byte(v), IsNull: isNull}
}

// FieldU64 构造无符号 64 位字段（8 字节大端）。
func FieldU64(v uint64, isNull bool) IdentityField {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return IdentityField{Tag: fieldTagU64, Value: buf, IsNull: isNull}
}

// CanonicalIdentity 计算 canonical identity 字节串（确定性；同输入必同输出）。
// 编码格式：version(0x01) + uint16 BE 分支名原字节长度 + 分支原字节 +
// 每字段(tag + marker + uint64 BE 载荷长度 + payload)。
//
// C2 强制约定：当 IsNull=true 时，字段编码为 tag + fieldNull(0x01) + uint64 BE 长度 0，
// 且无 payload，完全忽略 Value 内容与长度。
// 当 IsNull=false 时，字段编码为 tag + fieldNotNull(0x00) + uint64 BE 长度 len(Value) + Value。
func CanonicalIdentity(branch string, fields ...IdentityField) []byte {
	b := make([]byte, 0, 64)
	b = append(b, identityVersion)
	var bl [2]byte
	binary.BigEndian.PutUint16(bl[:], uint16(len(branch)))
	b = append(b, bl[:]...)
	b = append(b, branch...)
	for _, f := range fields {
		b = append(b, f.Tag)
		if f.IsNull {
			b = append(b, fieldNull)
			var l [8]byte // 8 字节 0
			b = append(b, l[:]...)
		} else {
			b = append(b, fieldNotNull)
			var l [8]byte
			binary.BigEndian.PutUint64(l[:], uint64(len(f.Value)))
			b = append(b, l[:]...)
			b = append(b, f.Value...)
		}
	}
	return b
}

// CanonicalHash 是 canonical identity 的 SHA-256（存入 BINARY(32) identity hash 列）。
func CanonicalHash(identity []byte) [32]byte {
	return sha256.Sum256(identity)
}

// ---- 每表 canonical identity builder（全部 22 个；分支名与列布局固定）----

// 1. NKeyIdentity 是 companies/task_categories/task_templates 的 canonical identity（分支 nkey）。
func NKeyIdentity(nkey string) []byte {
	return CanonicalIdentity("nkey", FieldBytes(nkey, false))
}

// 2. SettingsKeyIdentity 是 settings/tools/scheduler_state 的 canonical identity（分支 key）。
func SettingsKeyIdentity(key string) []byte {
	return CanonicalIdentity("key", FieldBytes(key, false))
}

// 3. AgentKeyIdentity 是 agents 的 canonical identity（分支 agent_key）。
func AgentKeyIdentity(key string) []byte {
	return CanonicalIdentity("agent_key", FieldBytes(key, false))
}

// 4. MCPServerNameIdentity 是 mcp_servers 的 canonical identity（分支 mcp_server_name）。
func MCPServerNameIdentity(name string) []byte {
	return CanonicalIdentity("mcp_server_name", FieldBytes(name, false))
}

// 5. AgentPromptVarIdentity 是 agent_prompt_vars 的 canonical identity（分支 agent_prompt_var）。
func AgentPromptVarIdentity(agentID uint64, varName string) []byte {
	return CanonicalIdentity("agent_prompt_var", FieldU64(agentID, false), FieldBytes(varName, false))
}

// 6. MCPToolCacheIdentity 是 mcp_tools_cache 的 canonical identity（分支 mcp_tool_cache）。
func MCPToolCacheIdentity(serverID uint64, toolName string) []byte {
	return CanonicalIdentity("mcp_tool_cache", FieldU64(serverID, false), FieldBytes(toolName, false))
}

// 7. AgentVisibilityIdentity 是 agent_visibility 的 canonical identity（分支 agent_visibility，4 元组）。
func AgentVisibilityIdentity(agentID uint64, resourceKind string, resourceID uint64, mcpToolName string) []byte {
	return CanonicalIdentity("agent_visibility",
		FieldU64(agentID, false),
		FieldBytes(resourceKind, false),
		FieldU64(resourceID, false),
		FieldBytes(mcpToolName, false))
}

// 8. AgentSkillVisibilityIdentity 是 agent_skill_visibility 的 canonical identity（分支 agent_skill_visibility）。
func AgentSkillVisibilityIdentity(agentID uint64, skillName string) []byte {
	return CanonicalIdentity("agent_skill_visibility", FieldU64(agentID, false), FieldBytes(skillName, false))
}

// 9. SideQuestionRequestsIdentity 是 side_question_requests 的 canonical identity（分支 side_question_request）。
func SideQuestionRequestsIdentity(sessionKey string, generation uint64, clientID string) []byte {
	return CanonicalIdentity("side_question_request",
		FieldBytes(sessionKey, false),
		FieldU64(generation, false),
		FieldBytes(clientID, false))
}

// 10. TaskScopeIdentity 是 task_scope 的 canonical identity（分支 task_scope）。
// 调用方先按原 uq_task_scope_v2 表达式 COALESCE：domain/net/value NULL→''、company_id NULL→0；
// kind 无 COALESCE（NOT NULL）。
func TaskScopeIdentity(taskID uint64, kind, domain, net string, companyID uint64, value string) []byte {
	return CanonicalIdentity("task_scope",
		FieldU64(taskID, false),
		FieldBytes(kind, false),
		FieldBytes(domain, false),
		FieldBytes(net, false),
		FieldU64(companyID, false),
		FieldBytes(value, false))
}

// 11. CompanyScopeDomainIdentity 是 company_scope (company_id,domain) 分支的 canonical identity。
func CompanyScopeDomainIdentity(companyID uint64, domain string) []byte {
	return CanonicalIdentity("cscope_domain", FieldU64(companyID, false), FieldBytes(domain, false))
}

// 12. CompanyScopeNetIdentity 是 company_scope (company_id,net) 分支的 canonical identity。
func CompanyScopeNetIdentity(companyID uint64, net string) []byte {
	return CanonicalIdentity("cscope_net", FieldU64(companyID, false), FieldBytes(net, false))
}

// 13. CompanyScopeValueIdentity 是 company_scope (company_id,kind,value) 分支的 canonical identity。
func CompanyScopeValueIdentity(companyID uint64, kind, value string) []byte {
	return CanonicalIdentity("cscope_value", FieldU64(companyID, false), FieldBytes(kind, false), FieldBytes(value, false))
}

// 14. AssetsRootDomainIdentity 是 assets root_domain 分支的 canonical identity（仅 domain 非 NULL 时适用）。
func AssetsRootDomainIdentity(domain string) []byte {
	return CanonicalIdentity("assets_root_domain", FieldBytes(domain, false))
}

// 15. AssetsIPIdentity 是 assets ip 分支的 canonical identity（仅 ip 非 NULL 时适用）。
func AssetsIPIdentity(ip string) []byte {
	return CanonicalIdentity("assets_ip", FieldBytes(ip, false))
}

// 16. AssetsSubdomainIdentity 是 assets subdomain 分支的 canonical identity。
// 调用方对 record_type 先 COALESCE 为 ''（原表达式含 COALESCE）；domain 不适用分支时不写该分支对。
func AssetsSubdomainIdentity(domain, recordType string) []byte {
	return CanonicalIdentity("assets_subdomain", FieldBytes(domain, false), FieldBytes(recordType, false))
}

// 17. AssetsAppBundleIdentity 是 assets app_bundle 分支的 canonical identity（仅 bundle_id 非 NULL 时适用）。
func AssetsAppBundleIdentity(bundleID string) []byte {
	return CanonicalIdentity("assets_app_bundle", FieldBytes(bundleID, false))
}

// 18. AssetsAppNameIdentity 是 assets app_name 分支的 canonical identity（仅 app_name 非 NULL 时适用）。
func AssetsAppNameIdentity(appName string) []byte {
	return CanonicalIdentity("assets_app_name", FieldBytes(appName, false))
}

// 19. AssetsServiceHTTPIdentity 是 assets service_http 分支的 canonical identity（仅 url 非 NULL 时适用）。
func AssetsServiceHTTPIdentity(url string) []byte {
	return CanonicalIdentity("assets_service_http", FieldBytes(url, false))
}

// 20. AssetsServiceOtherIdentity 是 assets service_other 分支的 canonical identity。
// 调用方对 domain/ip 先 COALESCE 为 ''；port/service_name 无 COALESCE，NULL 保留 NULL 语义。
func AssetsServiceOtherIdentity(domain, ip string, port uint64, portNull bool, serviceName string, serviceNameNull bool) []byte {
	return CanonicalIdentity("assets_service_other",
		FieldBytes(domain, false),
		FieldBytes(ip, false),
		FieldU64(port, portNull),
		FieldBytes(serviceName, serviceNameNull))
}

// 21. AssetsEndpointIdentity 是 assets endpoint 分支的 canonical identity（仅 url 非 NULL 时适用）。
func AssetsEndpointIdentity(url, method string) []byte {
	return CanonicalIdentity("assets_endpoint", FieldBytes(url, false), FieldBytes(method, false))
}

// 22. LLMProfileNameIdentity 是 llm_profiles 的 canonical identity（固定分支 llm_profile_name）。
// 持久化为 llm_profiles.name_identity=payload、name_hash=CanonicalHash(payload)。
func LLMProfileNameIdentity(name string) []byte {
	return CanonicalIdentity("llm_profile_name", FieldBytes(name, false))
}
