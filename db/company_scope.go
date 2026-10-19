package db

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"unicode"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"

	"github.com/Autumn-27/artex/db/schema"
)

// ParsedScope is one parsed asset-scope entry. Its kind selects Domain, Net, or Value.
// Used internally by the company scope parsers and CompanyStore.
type ParsedScope struct {
	Kind   string // "domain" | "ip" | "cidr" | "icp" | "keyword"
	Domain string // normalized registrable/root domain (kind=domain)
	Net    string // normalized CIDR, single IP as /32 or /128 (kind=ip|cidr)
	Value  string // normalized text (kind=icp|keyword)
	Raw    string // original input line
}

// ScopeInput is the structured API form for a company scope rule. Empty Kind
// uses the same automatic classification as the single-textarea UI.
type ScopeInput struct {
	Kind  string `json:"kind,omitempty"`
	Value string `json:"value"`
}

// NormalizeICP removes every Unicode whitespace character and folds case. ICP
// matching intentionally performs no fuzzy or punctuation normalization.
func NormalizeICP(value string) string {
	return strings.ToLower(strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(value)))
}

func normalizeKeyword(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func looksLikeIPAddress(value string) bool {
	value = strings.TrimSpace(value)
	if strings.Contains(value, "://") || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return false
	}
	if strings.Count(value, ":") >= 2 {
		// Require an IPv6-looking prefix. This still catches malformed values such
		// as 2001:db8::zz without treating ordinary colon-delimited keywords as IPs.
		parts := strings.Split(value, ":")
		validSegments := 0
		for _, part := range parts {
			if part == "" {
				if validSegments > 0 || strings.HasPrefix(value, "::") {
					return true
				}
				continue
			}
			if len(part) > 4 {
				return false
			}
			for _, r := range part {
				if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
					return false
				}
			}
			validSegments++
			if validSegments >= 2 {
				return true
			}
		}
		return false
	}
	if !strings.Contains(value, ".") {
		return false
	}
	for _, r := range value {
		if r != '.' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// ParseScopeInput validates an explicitly typed rule. Legacy callers can omit
// Kind and use the same automatic classification as the single-textarea UI.
func ParseScopeInput(input ScopeInput) (ParsedScope, error) {
	kind := strings.ToLower(strings.TrimSpace(input.Kind))
	raw := strings.TrimSpace(input.Value)
	if kind == "" {
		return ParseAutoScopeLine(raw)
	}
	switch kind {
	case "domain", "ip", "cidr":
		rule, err := ParseScopeLine(raw)
		if err != nil {
			return rule, err
		}
		if rule.Kind != kind {
			return ParsedScope{Kind: kind, Raw: raw}, fmt.Errorf("%q 不是有效的 %s 范围", raw, kind)
		}
		return rule, nil
	case "icp":
		value := NormalizeICP(raw)
		if value == "" {
			return ParsedScope{Kind: kind, Raw: raw}, fmt.Errorf("ICP 不能为空")
		}
		return ParsedScope{Kind: kind, Value: value, Raw: raw}, nil
	case "keyword":
		value := normalizeKeyword(raw)
		if value == "" {
			return ParsedScope{Kind: kind, Raw: raw}, fmt.Errorf("企业关键词不能为空")
		}
		return ParsedScope{Kind: kind, Value: value, Raw: raw}, nil
	default:
		return ParsedScope{Kind: kind, Raw: raw}, fmt.Errorf("不支持的范围类型: %s", kind)
	}
}

// ParseAutoScopeLine classifies one untyped textarea line. Network-looking and
// domain-looking values remain strict so malformed ranges do not silently become
// Agent keywords; all other non-empty text is a keyword.
func ParseAutoScopeLine(line string) (ParsedScope, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return ParsedScope{}, fmt.Errorf("空行")
	}

	if _, _, err := net.ParseCIDR(raw); err == nil {
		return ParseScopeLine(raw)
	}
	if ip := net.ParseIP(raw); ip != nil {
		return ParseScopeLine(raw)
	}
	if slash := strings.LastIndexByte(raw, '/'); slash > 0 {
		address := strings.TrimSpace(raw[:slash])
		if net.ParseIP(address) != nil || looksLikeIPAddress(address) {
			return ParsedScope{Raw: raw}, fmt.Errorf("无效 CIDR: %s", raw)
		}
	}

	if looksLikeIPAddress(raw) {
		return ParsedScope{Raw: raw}, fmt.Errorf("无效 IP: %s", raw)
	}

	looksLikeDomain := strings.Contains(raw, "://") ||
		(strings.Contains(raw, ".") && strings.IndexFunc(raw, unicode.IsSpace) < 0)
	if looksLikeDomain {
		return ParseScopeLine(raw)
	}
	// 备案号本身不含点号（如 京ICP备12345678号-1）。带点的文本多半掺了域名或版本号，
	// 按 ICP 存下来只会得到一条永远匹配不上任何资产的死规则 —— ICP 归属走的是精确
	// 相等比较（见 companies.go 的 kind='icp' 归属查询），所以这类文本归为关键词。
	lower := strings.ToLower(raw)
	if !strings.ContainsAny(raw, ".．。") &&
		(strings.Contains(lower, "icp") || strings.Contains(raw, "备案")) {
		return ParseScopeInput(ScopeInput{Kind: "icp", Value: raw})
	}
	return ParseScopeInput(ScopeInput{Kind: "keyword", Value: raw})
}

func scopeHostname(raw string) (string, error) {
	candidate := strings.TrimSpace(raw)
	if candidate == "" {
		return "", fmt.Errorf("主机名为空")
	}
	if strings.HasPrefix(candidate, "//") {
		candidate = "http:" + candidate
	} else if !strings.Contains(candidate, "://") {
		candidate = "http://" + candidate
	}
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.Host == "" {
		if err == nil {
			err = fmt.Errorf("缺少主机名")
		}
		return "", err
	}
	host := strings.TrimSuffix(strings.TrimSpace(parsed.Hostname()), ".")
	if host == "" {
		return "", fmt.Errorf("主机名为空")
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}
	host, err = idna.Lookup.ToASCII(host)
	if err != nil {
		return "", err
	}
	host = strings.ToLower(host)
	if len(host) > 253 {
		return "", fmt.Errorf("域名超过 253 个字符")
	}
	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return "", fmt.Errorf("域名至少需要两个标签")
	}
	for _, label := range labels {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("域名标签无效")
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
				return "", fmt.Errorf("域名包含无效字符")
			}
		}
	}
	return host, nil
}

// ParseScopeLine classifies and validates one scope line (root domain / IP /
// CIDR). Guardrails reject bare TLDs and over-broad networks so a rule can never
// swallow the internet. IP ranges must be expressed as CIDR.
func ParseScopeLine(line string) (ParsedScope, error) {
	raw := strings.TrimSpace(line)
	r := ParsedScope{Raw: raw}
	if raw == "" {
		return r, fmt.Errorf("空行")
	}
	// CIDR first because URL parsing treats its slash as a path separator.
	if _, ipnet, err := net.ParseCIDR(raw); err == nil {
		ones, bits := ipnet.Mask.Size()
		if bits == 32 && ones < 16 {
			return r, fmt.Errorf("网段过宽(IPv4 需 >= /16): %s", raw)
		}
		if bits == 128 && ones < 32 {
			return r, fmt.Errorf("网段过宽(IPv6 需 >= /32): %s", raw)
		}
		r.Kind, r.Net = "cidr", ipnet.String()
		return r, nil
	}
	// Single IP.
	if ip := net.ParseIP(raw); ip != nil {
		r.Kind = "ip"
		if ip.To4() != nil {
			r.Net = ip.String() + "/32"
		} else {
			r.Net = ip.String() + "/128"
		}
		return r, nil
	}
	host, err := scopeHostname(raw)
	if err != nil {
		return r, fmt.Errorf("无法识别为有效域名/IP/CIDR: %s", raw)
	}
	if ip := net.ParseIP(host); ip != nil {
		r.Kind = "ip"
		if ip.To4() != nil {
			r.Net = ip.String() + "/32"
		} else {
			r.Net = ip.String() + "/128"
		}
		return r, nil
	}
	if looksLikeIPAddress(host) {
		return r, fmt.Errorf("无效 IP: %s", raw)
	}
	if strings.Contains(raw, "-") && strings.Count(raw, ".") >= 6 {
		return r, fmt.Errorf("IP 段请用 CIDR 表示(如 1.2.3.0/24): %s", raw)
	}
	// Domain (registrable). Reject bare TLDs / public suffixes.
	d := DomainKey(host)
	if suf, icann := publicsuffix.PublicSuffix(d); icann && suf == d {
		return r, fmt.Errorf("不能用裸 TLD 作为范围: %s", raw)
	}
	r.Kind, r.Domain = "domain", d
	return r, nil
}

// =====================================================================
// hash 唯一键分支与归属裁决（P08-02 Task 2.2：M09 / M35）
// =====================================================================

// companyScopeBranch 是一条已解析范围规则在 TiDB 新库里的写入布局。
//
// company_scope 用三条 hash 唯一索引（uq_cscope_hash_domain / uq_cscope_hash_net /
// uq_cscope_hash_value）取代旧的 PG 命名约束与部分唯一索引：一行只激活与 kind 对应的
// 那一条分支，另外两分支的 identity/hash 必须保持 NULL —— MySQL 唯一索引不约束 NULL，
// 写入端漏一对就等于该分支唯一性静默失效（M09 的根因）。
type companyScopeBranch struct {
	ValueCol string // 规则载荷列名：domain | net | value（固定枚举，绝非用户输入）
	Value    string // 规范化后的规则载荷
	IdentCol string // 该分支的 identity 列名
	HashCol  string // 该分支的 hash 列名
	Identity []byte // canonical identity 载荷
	Hash     []byte // canonical identity 的 SHA-256，落 BINARY(32)
}

// companyScopeBranchFor 按 kind 选出唯一适用的 hash 分支，并算出 canonical identity
// 与 hash（M09）。三条分支的身份载荷一律带 company_id：范围规则的自然键是
// (企业, 规则)，不是规则本身。
func companyScopeBranchFor(companyID int64, rule ParsedScope) (companyScopeBranch, error) {
	if companyID <= 0 {
		return companyScopeBranch{}, fmt.Errorf("company scope: invalid company id %d", companyID)
	}
	cid := uint64(companyID)
	switch rule.Kind {
	case "domain":
		if rule.Domain == "" {
			return companyScopeBranch{}, fmt.Errorf("company scope: domain 规则 %q 缺少规范化域名", rule.Raw)
		}
		return newCompanyScopeBranch("domain", rule.Domain, "identity_scope_domain", "hash_scope_domain",
			schema.CompanyScopeDomainIdentity(cid, rule.Domain)), nil
	case "ip", "cidr":
		if rule.Net == "" {
			return companyScopeBranch{}, fmt.Errorf("company scope: %s 规则 %q 缺少规范化网段", rule.Kind, rule.Raw)
		}
		return newCompanyScopeBranch("net", rule.Net, "identity_scope_net", "hash_scope_net",
			schema.CompanyScopeNetIdentity(cid, rule.Net)), nil
	case "icp", "keyword":
		if rule.Value == "" {
			return companyScopeBranch{}, fmt.Errorf("company scope: %s 规则 %q 缺少规范化文本", rule.Kind, rule.Raw)
		}
		return newCompanyScopeBranch("value", rule.Value, "identity_scope_value", "hash_scope_value",
			schema.CompanyScopeValueIdentity(cid, rule.Kind, rule.Value)), nil
	default:
		return companyScopeBranch{}, fmt.Errorf("unsupported company scope kind %q", rule.Kind)
	}
}

// newCompanyScopeBranch 把 canonical identity 编成写入用的 (载荷, 摘要) 一对。
func newCompanyScopeBranch(valueCol, value, identCol, hashCol string, identity []byte) companyScopeBranch {
	payload, hash := canonicalIdentityPair(identity)
	return companyScopeBranch{
		ValueCol: valueCol,
		Value:    value,
		IdentCol: identCol,
		HashCol:  hashCol,
		Identity: payload,
		Hash:     hash,
	}
}

// companyNetScope 是一条已预解析的 ip/cidr 归属规则。
//
// 新库把 CIDR 落成 TEXT：TiDB 没有 inet/cidr 类型，也没有 >>= 与 masklen()，
// 因此网络归属按 spec §3.3.1 改为"SQL 取候选 + Go 内存前缀匹配"，裁决语义与原 PG
// 逐条对齐 —— **最长前缀优先，前缀同长时 company_id 升序**。
type companyNetScope struct {
	companyID int64
	prefix    *net.IPNet
	bits      int
}

// newCompanyNetScope 解析一条网段规则。ok=false 表示该条不可用（空串或非法 CIDR）：
// 调用方丢弃这一条继续处理其余规则 —— 一条脏规则既不能让整次归属重算失败，
// 更不能被解释成"匹配一切"。
func newCompanyNetScope(companyID int64, raw string) (companyNetScope, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || companyID <= 0 {
		return companyNetScope{}, false
	}
	prefix, ok := parseCompanyNetPrefix(raw)
	if !ok {
		return companyNetScope{}, false
	}
	ones, bits := prefix.Mask.Size()
	if ones == 0 && bits == 0 {
		return companyNetScope{}, false
	}
	return companyNetScope{companyID: companyID, prefix: prefix, bits: ones}, true
}

// parseCompanyNetPrefix 解析 CIDR；兼容历史遗留的裸地址（等价 /32 或 /128，
// 与资产侧 assetNetPrefixLen 的容忍口径一致）。
func parseCompanyNetPrefix(raw string) (*net.IPNet, bool) {
	if _, prefix, err := net.ParseCIDR(raw); err == nil {
		return prefix, true
	}
	ip := net.ParseIP(raw)
	if ip == nil {
		return nil, false
	}
	bits := 128
	if ip.To4() != nil {
		bits = 32
	}
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}, true
}

// matchCompanyNetScope 返回 ip 命中的企业：最长前缀优先，同长取最小 company_id。
// 未解析出的地址（nil）绝不命中任何规则。
func matchCompanyNetScope(scopes []companyNetScope, ip net.IP) (int64, bool) {
	if ip == nil {
		return 0, false
	}
	var best int64
	bestBits := -1
	for _, scope := range scopes {
		if scope.prefix == nil || !scope.prefix.Contains(ip) {
			continue
		}
		if best == 0 || scope.bits > bestBits || (scope.bits == bestBits && scope.companyID < best) {
			best, bestBits = scope.companyID, scope.bits
		}
	}
	if best == 0 {
		return 0, false
	}
	return best, true
}

// companyICPScope 是一条备案（ICP）归属规则；value 允许是原始写法，索引时统一归一化。
type companyICPScope struct {
	companyID int64
	value     string
}

// indexCompanyICPScopes 把规则压成"归一化 ICP → 最小 company_id"。
//
// 旧实现在 SQL 端用 PG 的 regexp_replace(..., '[[:space:]]+', ”, 'g') 做归一化；
// MySQL/TiDB 的 REGEXP_REPLACE 第 4 个参数是 position 而不是 flags（R4 补充函数签名
// 差异），照搬 'g' 会得到完全不同的语义。这里复用写入端同一个 Go NormalizeICP，
// 让"写进 value 列的规范形"与"匹配时用的规范形"必然是同一份实现。
func indexCompanyICPScopes(rules []companyICPScope) map[string]int64 {
	out := make(map[string]int64, len(rules))
	for _, rule := range rules {
		value := NormalizeICP(rule.value)
		if value == "" || rule.companyID <= 0 {
			continue
		}
		if prev, ok := out[value]; !ok || rule.companyID < prev {
			out[value] = rule.companyID
		}
	}
	return out
}

// matchCompanyICPScope 复刻原 SQL 的 OR 语义：资产的 icp 与 app_icp 任一命中即算匹配，
// 多条命中时取 company_id 最小者（原 ORDER BY cs.company_id ASC LIMIT 1）。
func matchCompanyICPScope(rules map[string]int64, icp, appICP string) (int64, bool) {
	var best int64
	for _, raw := range []string{icp, appICP} {
		value := NormalizeICP(raw)
		if value == "" {
			continue
		}
		if cid, ok := rules[value]; ok && (best == 0 || cid < best) {
			best = cid
		}
	}
	if best == 0 {
		return 0, false
	}
	return best, true
}
