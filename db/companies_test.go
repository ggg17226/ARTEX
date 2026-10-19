package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

// =====================================================================
// P08-02 Task 2.2（M09 / M19 / M35）
//
// 三层证据：
//  1. 纯函数离线用例 —— 范围分支 canonical identity/hash（M09）、网段与 ICP 归属
//     裁决（TiDB 无 inet/cidr 算子后必须在 Go 内存里复现的语义）；
//  2. 源码级方言守卫 —— companies.go / company_scope.go 的可执行代码里不得再有
//     任何 PG 片段，归属去重必须是"派生表 + 窗口函数"（M35）；
//  3. 真实 TiDB 集成用例 —— 未设置 ARTEX_TIDB_DSN 时诚实 skip，绝不算通过。
// =====================================================================

// cleanupCompany 反序清掉一家公司与其范围规则（TiDB 方言：? 占位符）。
func cleanupCompany(d *DB, id int64) {
	_, _ = d.Exec(`DELETE FROM company_scope WHERE company_id = ?`, id)
	_, _ = d.Exec(`DELETE FROM companies WHERE id = ?`, id)
}

// ── 1. 纯函数：范围三分支 canonical identity / hash（M09）───────────────────

func TestCompanyScopeBranchIdentityByKind(t *testing.T) {
	const companyID = int64(4242)
	cases := []struct {
		name         string
		rule         ParsedScope
		wantIdentCol string
		wantHashCol  string
		wantIdentity []byte
	}{
		{
			name:         "domain",
			rule:         ParsedScope{Kind: "domain", Domain: "example.com", Raw: "example.com"},
			wantIdentCol: "identity_scope_domain",
			wantHashCol:  "hash_scope_domain",
			wantIdentity: schema.CompanyScopeDomainIdentity(uint64(companyID), "example.com"),
		},
		{
			name:         "ip",
			rule:         ParsedScope{Kind: "ip", Net: "10.0.0.1/32", Raw: "10.0.0.1"},
			wantIdentCol: "identity_scope_net",
			wantHashCol:  "hash_scope_net",
			wantIdentity: schema.CompanyScopeNetIdentity(uint64(companyID), "10.0.0.1/32"),
		},
		{
			name:         "cidr",
			rule:         ParsedScope{Kind: "cidr", Net: "10.20.0.0/16", Raw: "10.20.0.0/16"},
			wantIdentCol: "identity_scope_net",
			wantHashCol:  "hash_scope_net",
			wantIdentity: schema.CompanyScopeNetIdentity(uint64(companyID), "10.20.0.0/16"),
		},
		{
			name:         "icp",
			rule:         ParsedScope{Kind: "icp", Value: "京icp备12345678号-1", Raw: "京ICP备12345678号-1"},
			wantIdentCol: "identity_scope_value",
			wantHashCol:  "hash_scope_value",
			wantIdentity: schema.CompanyScopeValueIdentity(uint64(companyID), "icp", "京icp备12345678号-1"),
		},
		{
			name:         "keyword",
			rule:         ParsedScope{Kind: "keyword", Value: "acme corp", Raw: "ACME   Corp"},
			wantIdentCol: "identity_scope_value",
			wantHashCol:  "hash_scope_value",
			wantIdentity: schema.CompanyScopeValueIdentity(uint64(companyID), "keyword", "acme corp"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			branch, err := companyScopeBranchFor(companyID, tc.rule)
			if err != nil {
				t.Fatalf("companyScopeBranchFor(%s): %v", tc.name, err)
			}
			if branch.IdentCol != tc.wantIdentCol || branch.HashCol != tc.wantHashCol {
				t.Fatalf("branch = (%s, %s), want (%s, %s)",
					branch.IdentCol, branch.HashCol, tc.wantIdentCol, tc.wantHashCol)
			}
			if !bytes.Equal(branch.Identity, tc.wantIdentity) {
				t.Fatalf("identity must be the canonical builder output byte-for-byte")
			}
			wantHash := schema.CanonicalHash(tc.wantIdentity)
			if !bytes.Equal(branch.Hash, wantHash[:]) {
				t.Fatalf("hash = %x, want %x", branch.Hash, wantHash)
			}
			if len(branch.Hash) != 32 {
				t.Fatalf("hash length = %d, want 32 bytes for BINARY(32)", len(branch.Hash))
			}
		})
	}
}

// TestCompanyScopeBranchIdentityFailsClosed 保证"写了行但唯一键退化成 NULL"
// 这种静默失效在写入端就被阻断：缺载荷、未知 kind、非法 company_id 都必须报错。
func TestCompanyScopeBranchIdentityFailsClosed(t *testing.T) {
	cases := []struct {
		name      string
		companyID int64
		rule      ParsedScope
	}{
		{"unknown kind", 1, ParsedScope{Kind: "wildcard", Value: "*", Raw: "*"}},
		{"empty kind", 1, ParsedScope{Kind: "", Domain: "example.com", Raw: "example.com"}},
		{"domain without payload", 1, ParsedScope{Kind: "domain", Raw: "example.com"}},
		{"net without payload", 1, ParsedScope{Kind: "cidr", Raw: "10.0.0.0/8"}},
		{"value without payload", 1, ParsedScope{Kind: "icp", Raw: "京ICP备12345678号-1"}},
		{"zero company id", 0, ParsedScope{Kind: "domain", Domain: "example.com", Raw: "example.com"}},
		{"negative company id", -1, ParsedScope{Kind: "domain", Domain: "example.com", Raw: "example.com"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			branch, err := companyScopeBranchFor(tc.companyID, tc.rule)
			if err == nil {
				t.Fatalf("want error, got branch %+v", branch)
			}
			if branch.Identity != nil || branch.Hash != nil {
				t.Fatalf("failed branch must not carry identity/hash payload: %+v", branch)
			}
		})
	}
}

// ── 2. 纯函数：网段归属裁决（TiDB 无 inet/cidr 算子，内存预筛选）─────────────

func TestCompanyNetScopePrefersLongestPrefixThenLowestCompanyID(t *testing.T) {
	seeds := []struct {
		companyID int64
		raw       string
		valid     bool
	}{
		{7, "10.0.0.0/8", true},
		{5, "10.20.0.0/16", true},
		{3, "10.20.0.0/16", true}, // 同前缀：company_id 更小者胜出
		{9, "192.168.1.0/24", true},
		{11, "not-a-cidr", false}, // 预解析失败 → 丢弃该规则，绝不吞掉整批
		{13, "2001:db8::/32", true},
	}
	var scopes []companyNetScope
	for _, seed := range seeds {
		scope, ok := newCompanyNetScope(seed.companyID, seed.raw)
		if ok != seed.valid {
			t.Fatalf("newCompanyNetScope(%d, %q) ok = %v, want %v", seed.companyID, seed.raw, ok, seed.valid)
		}
		if ok {
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) != 5 {
		t.Fatalf("parsed scopes = %d, want 5", len(scopes))
	}

	cases := []struct {
		ip   string
		want int64
		ok   bool
	}{
		{"10.20.5.1", 3, true},     // /16 压过 /8，同前缀取 company_id 最小
		{"10.30.1.1", 7, true},     // 只命中 /8
		{"192.168.1.77", 9, true},  // /24
		{"2001:db8::42", 13, true}, // IPv6 前缀
		{"172.16.0.1", 0, false},   // 不在任何规则内
		{"::1", 0, false},
		{"not-an-ip", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		ip := net.ParseIP(tc.ip)
		got, ok := matchCompanyNetScope(scopes, ip)
		if ok != tc.ok || got != tc.want {
			t.Errorf("matchCompanyNetScope(%q) = (%d, %v), want (%d, %v)", tc.ip, got, ok, tc.want, tc.ok)
		}
	}
	// nil 地址（未解析）绝不允许命中任何规则。
	if got, ok := matchCompanyNetScope(scopes, nil); ok || got != 0 {
		t.Errorf("matchCompanyNetScope(nil) = (%d, %v), want (0, false)", got, ok)
	}
}

// ── 3. 纯函数：ICP 归属裁决（Go 归一化取代 PG regexp_replace）────────────────

func TestCompanyICPScopeNormalizationAndTieBreak(t *testing.T) {
	rules := indexCompanyICPScopes([]companyICPScope{
		{companyID: 8, value: "京icp备12345678号-1"},
		{companyID: 4, value: " 京ICP备12345678号-1 "}, // 归一化后同值，company_id 更小者胜出
		{companyID: 6, value: "沪icp备99999999号-2"},
		{companyID: 0, value: "  "}, // 空规则 → 丢弃
	})
	if len(rules) != 2 {
		t.Fatalf("indexed rules = %d, want 2: %+v", len(rules), rules)
	}
	if lowest, ok := rules["京icp备12345678号-1"]; !ok || lowest != 4 {
		t.Fatalf("normalized rule must collapse to the lowest company id, got (%d, %v)", lowest, ok)
	}

	cases := []struct {
		icp    string
		appICP string
		want   int64
		ok     bool
	}{
		{"京 ICP 备 12345678 号-1", "", 4, true},
		{"", "  沪ICP备99999999号-2", 6, true},
		{"浙ICP备00000000号-1", "沪ICP备99999999号-2", 6, true},
		{"浙ICP备00000000号-1", "", 0, false},
		{"", "", 0, false},
	}
	for _, tc := range cases {
		got, ok := matchCompanyICPScope(rules, tc.icp, tc.appICP)
		if ok != tc.ok || got != tc.want {
			t.Errorf("matchCompanyICPScope(%q, %q) = (%d, %v), want (%d, %v)",
				tc.icp, tc.appICP, got, ok, tc.want, tc.ok)
		}
	}
}

// ── 4. 源码级方言守卫（M09 / M19 / M35：可执行代码里不得再有 PG 片段）────────

// TestCompaniesSourceIsTiDBDialectOnly 扫描 companies.go / company_scope.go 的
// **可执行代码**（去掉行注释），任何 PostgreSQL 片段都判红 —— 包含本任务必须
// 消除的 DISTINCT ON（M35）与 advisory lock（企业范围变更串行化）。
func TestCompaniesSourceIsTiDBDialectOnly(t *testing.T) {
	pgFragments := []string{
		"$1", "$2", "$3", "$4", "$5", "ON CONFLICT", "RETURNING", "pg_advisory",
		"array_to_json", "to_jsonb", "jsonb_array_elements", "::jsonb", "::bigint",
		"::text", "::cidr", "::inet", "ILIKE", "unnest(", "= ANY(", "DISTINCT ON",
		"masklen(", "try_inet", "xmax", "EXCLUDED", "regexp_replace",
	}
	for _, file := range []string{"companies.go", "company_scope.go"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			code, _, _ := strings.Cut(line, "//")
			for _, frag := range pgFragments {
				if strings.Contains(code, frag) {
					t.Errorf("%s:%d: PostgreSQL fragment %q in executable code: %s", file, i+1, frag, strings.TrimSpace(code))
				}
			}
			if strings.Contains(code, "GROUP BY") {
				t.Errorf("%s:%d: GROUP BY must go — TiDB ONLY_FULL_GROUP_BY 严格模式下改走相关子查询: %s",
					file, i+1, strings.TrimSpace(code))
			}
		}
	}
}

// TestCompaniesAttributionUsesDerivedWindowFunction 断言企业资产归属去重确实是
// "合法派生表 + ROW_NUMBER() OVER (PARTITION BY …)"（spec §3.3.1），并把
// tie-break 合同（域名长度 DESC、company_id ASC）锁进测试。
func TestCompaniesAttributionUsesDerivedWindowFunction(t *testing.T) {
	raw, err := os.ReadFile("companies.go")
	if err != nil {
		t.Fatalf("read companies.go: %v", err)
	}
	sql := companyExecutableSQL(string(raw))
	for _, want := range []string{
		"SELECT asset_id, company_id FROM (",
		"ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY LENGTH(cs.domain) DESC, cs.company_id ASC)",
		") t WHERE t.rn = 1",
		"cs.kind = 'domain'",
		"a.root_domain = cs.domain",
		"WHERE company_source = 'scope'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("attribution SQL must contain %q", want)
		}
	}
	if strings.Contains(strings.ToUpper(sql), "DISTINCT ON") {
		t.Error("DISTINCT ON must be gone — TiDB 只能用派生表 + ROW_NUMBER 去重")
	}

	// 删除企业必须显式级联（M19）：新库没有 FK，不可能靠 SET NULL/CASCADE 兜底。
	// 删除入口在 companies.go，物理 DML 由 ExecuteCompanyCascade（cascade_executor.go）
	// 承载，两个文件一起扫才锁得住"闭包 + 执行器"这套接线。
	executor, err := os.ReadFile("cascade_executor.go")
	if err != nil {
		t.Fatalf("read cascade_executor.go: %v", err)
	}
	deletion := companyExecutableSQL(string(raw) + "\n" + string(executor))
	for _, want := range []string{
		"UPDATE assets SET company_id = NULL WHERE company_id = ?",
		"UPDATE tasks SET company_id = NULL WHERE company_id = ?",
		"DELETE FROM company_scope WHERE company_id = ?",
		"ComputeCompanyClosure(ctx, tx, uint64(id))",
		"ExecuteCompanyCascade(ctx, tx, closure)",
	} {
		if !strings.Contains(deletion, want) {
			t.Errorf("company deletion must contain %q (M19 显式级联置空)", want)
		}
	}
}

// companyExecutableSQL 去掉行注释并把空白压成单个空格，让"SQL 形状"断言不受
// 换行/缩进/中文注释里出现的 SQL 关键字影响。
func companyExecutableSQL(source string) string {
	lines := strings.Split(source, "\n")
	code := make([]string, 0, len(lines))
	for _, line := range lines {
		code = append(code, strings.SplitN(line, "//", 2)[0])
	}
	return strings.Join(strings.Fields(strings.Join(code, " ")), " ")
}

// TestCompaniesHashUniqueKeyWritesAreComplete 锁住 M09 的写入合同：
// hash 唯一键表少写一对 identity/hash，那条唯一索引就永久退化成"全 NULL"，
// 去重彻底失效而且一声不响（MySQL 唯一索引不约束 NULL）。所以"每一处 INSERT
// 都写满了身份列"必须由测试守住，不能靠 review 的眼力。
func TestCompaniesHashUniqueKeyWritesAreComplete(t *testing.T) {
	raw, err := os.ReadFile("companies.go")
	if err != nil {
		t.Fatalf("read companies.go: %v", err)
	}
	source := companyExecutableSQL(string(raw))

	if got := strings.Count(source, "INSERT INTO companies(name, nkey, nkey_identity, nkey_hash, logo)"); got != 2 {
		t.Errorf("carrying nkey_identity/nkey_hash 的 companies INSERT = %d, want 2（upsert + create-with-scope）", got)
	}
	if !strings.Contains(source, "canonicalIdentityPair(schema.NKeyIdentity(nkey))") {
		t.Error("company nkey 身份必须来自 schema.NKeyIdentity + canonicalIdentityPair")
	}
	// 范围规则的载荷列与身份列必须同时来自按 kind 选出的那一个分支，且非激活列显式置空
	for _, want := range []string{
		"INSERT INTO company_scope(",
		"identity_scope_domain, hash_scope_domain",
		"identity_scope_net, hash_scope_net",
		"identity_scope_value, hash_scope_value",
		"companyScopeBranchFor(companyID, rule)",
	} {
		if !strings.Contains(source, want) {
			t.Errorf("scope INSERT 必须按分支写入，缺少 %q", want)
		}
	}
	// 三条分支的身份列/构造函数一个都不能少。
	scopeRaw, err := os.ReadFile("company_scope.go")
	if err != nil {
		t.Fatalf("read company_scope.go: %v", err)
	}
	scopeSource := companyExecutableSQL(string(scopeRaw))
	for _, want := range []string{
		"identity_scope_domain", "hash_scope_domain", "schema.CompanyScopeDomainIdentity",
		"identity_scope_net", "hash_scope_net", "schema.CompanyScopeNetIdentity",
		"identity_scope_value", "hash_scope_value", "schema.CompanyScopeValueIdentity",
	} {
		if !strings.Contains(scopeSource, want) {
			t.Errorf("company_scope 分支身份缺少 %q", want)
		}
	}
	// 自增 PK 只能来自 LastInsertId（TiDB 没有 RETURNING）。
	if got := strings.Count(source, "LastInsertId()"); got < 2 {
		t.Errorf("LastInsertId() 次数 = %d, want >= 2（企业插入必须走 LastInsertId）", got)
	}
}

// =====================================================================
// 5. 真实 TiDB 集成用例（未设置 ARTEX_TIDB_DSN 时诚实 skip）
// =====================================================================

// TestCompanies_RecomputeAttribution_UnmigratedDialect 是 Task 2.2 Step 1 用例：
// 两家企业登记完全相同的 domain 范围、目标企业 PK 更小，资产必须在
// ORDER BY cs.company_id ASC 的 tie-break 下确定性地归属目标企业。
func TestCompanies_RecomputeAttribution_UnmigratedDialect(t *testing.T) {
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

	cName := fmt.Sprintf("P08 Corp %d", time.Now().UnixNano())
	cid, created, err := d.Companies().UpsertCompany(cName, "")
	if err != nil {
		t.Fatalf("UpsertCompany failed: %v", err)
	}
	if !created || cid <= 0 {
		t.Fatalf("expected newly created company with positive ID, got (%v, %d)", created, cid)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM company_scope WHERE company_id = ?", cid); err != nil {
			t.Errorf("cleanup target company_scope failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM companies WHERE id = ?", cid); err != nil {
			t.Errorf("cleanup target company failed: %v", err)
		}
	})

	competitorName := fmt.Sprintf("P08 Competitor %d", time.Now().UnixNano())
	compCID, compCreated, err := d.Companies().UpsertCompany(competitorName, "")
	if err != nil {
		t.Fatalf("UpsertCompany competitor failed: %v", err)
	}
	if !compCreated || compCID <= 0 {
		t.Fatalf("expected newly created competitor company, got (%v, %d)", compCreated, compCID)
	}
	// 竞争公司校验成功后立即登记独立清理，再断言排序前提：
	// 避免 cid >= compCID 前置断言 Fatal 时，已成功创建的 compCID 缺失 Cleanup 造成 fixture 泄露。
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM company_scope WHERE company_id = ?", compCID); err != nil {
			t.Errorf("cleanup competitor company_scope failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM companies WHERE id = ?", compCID); err != nil {
			t.Errorf("cleanup competitor company failed: %v", err)
		}
	})
	if cid >= compCID {
		t.Fatalf("expected cid (%d) < compCID (%d) for deterministic tie-breaker precondition", cid, compCID)
	}

	rootBase := fmt.Sprintf("attr-%d.com", time.Now().UnixNano())
	subDomain := "sub." + rootBase
	assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: subDomain})
	if err != nil {
		t.Fatalf("UpsertRootDomain failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
			t.Errorf("cleanup asset failed: %v", err)
		}
	})

	// 竞争配置（根据 R4 现有精确匹配合约）：两家公司均添加了完全相同的 domain scope，但 cid < compCID；
	// 在 a.root_domain = cs.domain 精确匹配语义下，两家公司同时成为有效候选；
	// 根据 SQL 中 ORDER BY cs.company_id ASC tie-breaker 规则，较小 ID 的 cid 必须确定性胜出并赢得归属（已诚实对齐 tie-breaker 合同）
	added, skipped, invalid, errs := d.Companies().AddScope(cid, []string{subDomain}, "test target")
	if len(errs) > 0 || added != 1 || skipped != 0 || invalid != 0 {
		t.Fatalf("AddScope specific failed: added=%d, skipped=%d, invalid=%d, errs=%v", added, skipped, invalid, errs)
	}
	cAdded, cSkipped, cInvalid, cErrs := d.Companies().AddScope(compCID, []string{subDomain}, "test competitor")
	if len(cErrs) > 0 || cAdded != 1 || cSkipped != 0 || cInvalid != 0 {
		t.Fatalf("AddScope generic competitor failed: added=%d, skipped=%d, invalid=%d, errs=%v", cAdded, cSkipped, cInvalid, cErrs)
	}

	// 制造待修复状态：显式将该资产的 company_id 置空，确保测试严格验证 RecomputeAttribution 本身的重算与竞争裁决能力
	if _, err := d.Exec("UPDATE assets SET company_id = NULL WHERE id = ?", assetID); err != nil {
		t.Fatalf("reset asset company_id failed: %v", err)
	}

	err = d.Companies().RecomputeAttribution()
	if err != nil {
		t.Fatalf("RecomputeAttribution failed: %v", err)
	}

	var attributedID *int64
	if err := d.QueryRow("SELECT company_id FROM assets WHERE id = ?", assetID).Scan(&attributedID); err != nil {
		t.Fatalf("query attributed company_id failed: %v", err)
	}
	// 必须由 ID 更小的目标公司获胜，证明在相同范围候选存在时，窗口函数按 cs.company_id ASC tie-break 规则生效，杜绝倒退
	if attributedID == nil || *attributedID != cid {
		t.Fatalf("expected asset %d to be attributed to target company %d via tie-break (not competitor %d), got %v", assetID, cid, compCID, attributedID)
	}
}

// TestCompanies_DeleteCompanyNullifiesAssetsAndTasks 覆盖 M19：新库没有 FK，
// 删除企业必须在同一事务内显式清理 company_scope，并把 assets.company_id 与
// tasks.company_id 原子置空，绝不允许留下指向已删除企业的悬挂外键。
func TestCompanies_DeleteCompanyNullifiesAssetsAndTasks(t *testing.T) {
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

	stamp := time.Now().UnixNano()
	name := fmt.Sprintf("P08 Cascade Corp %d", stamp)
	cid, created, err := d.Companies().UpsertCompany(name, "")
	if err != nil {
		t.Fatalf("UpsertCompany failed: %v", err)
	}
	if !created || cid <= 0 {
		t.Fatalf("expected newly created company, got (%v, %d)", created, cid)
	}
	t.Cleanup(func() { cleanupCompany(d, cid) })

	domain := fmt.Sprintf("cascade-%d.com", stamp)
	if added, _, invalid, errs := d.Companies().AddScope(cid, []string{domain}, "test"); len(errs) > 0 || added != 1 || invalid != 0 {
		t.Fatalf("AddScope failed: added=%d invalid=%d errs=%v", added, invalid, errs)
	}

	assetID := assetRawInsert(t, d, map[string]any{
		"type": "root_domain", "domain": domain, "root_domain": domain, "company_id": cid,
	})
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
			t.Errorf("cleanup asset failed: %v", err)
		}
	})

	// tasks：explorations 是 tasks 的唯一强前置（uq_tasks_exploration_id），
	// NOT NULL 且无 DEFAULT 的文本列必须显式给值。
	expRes, err := d.Exec("INSERT INTO explorations(goal, status) VALUES (?, 'open')", fmt.Sprintf("p08-%d", stamp))
	if err != nil {
		t.Fatalf("seed exploration failed: %v", err)
	}
	expID, err := expRes.LastInsertId()
	if err != nil || expID <= 0 {
		t.Fatalf("exploration last insert id failed: id=%d err=%v", expID, err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", expID); err != nil {
			t.Errorf("cleanup exploration failed: %v", err)
		}
	})
	taskRes, err := d.Exec(`INSERT INTO tasks(name, description, goal, exploration_id, status, queue_mode, company_id)
VALUES (?, ?, ?, ?, 'created', '', ?)`,
		fmt.Sprintf("P08 Cascade Task %d", stamp), "desc", "goal", expID, cid)
	if err != nil {
		t.Fatalf("seed task failed: %v", err)
	}
	taskID, err := taskRes.LastInsertId()
	if err != nil || taskID <= 0 {
		t.Fatalf("task last insert id failed: id=%d err=%v", taskID, err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", taskID); err != nil {
			t.Errorf("cleanup task failed: %v", err)
		}
	})

	deleted, err := d.Companies().DeleteCompanyWithAssets(cid, false)
	if err != nil {
		t.Fatalf("DeleteCompanyWithAssets failed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleteAssets=false must keep the asset, deleted=%d", deleted)
	}

	if company, err := d.Companies().GetCompany(cid); err != nil || company != nil {
		t.Fatalf("company must be gone: company=%+v err=%v", company, err)
	}
	var scopes int
	if err := d.QueryRow("SELECT COUNT(*) FROM company_scope WHERE company_id = ?", cid).Scan(&scopes); err != nil {
		t.Fatalf("count company_scope failed: %v", err)
	}
	if scopes != 0 {
		t.Fatalf("%d company_scope rows survived company deletion", scopes)
	}
	for _, probe := range []struct {
		table string
		id    int64
	}{
		{"assets", assetID},
		{"tasks", taskID},
	} {
		var companyID *int64
		if err := d.QueryRow("SELECT company_id FROM "+probe.table+" WHERE id = ?", probe.id).Scan(&companyID); err != nil {
			t.Fatalf("read %s %d failed: %v", probe.table, probe.id, err)
		}
		if companyID != nil {
			t.Fatalf("%s %d still references deleted company %d (M19 requires SET NULL)", probe.table, probe.id, *companyID)
		}
	}
	// 已删除企业不得留下任何悬挂引用（cascade_executor 的同一断言口径）。
	if err := AssertNoOrphansForCompany(context.Background(), d, uint64(cid)); err != nil {
		t.Fatalf("dangling reference after company deletion: %v", err)
	}
}

// =====================================================================
// 6. 既有集成用例（TiDB 方言）
// =====================================================================

func TestCompanyUpsertAndGet(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, created, err := cs.UpsertCompany("Test Corp", "https://example.com/logo.png")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)
	if !created {
		t.Error("first upsert should report created=true")
	}

	// duplicate: same nkey, should not create new
	id2, created2, err := cs.UpsertCompany("Test Corp", "")
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Errorf("dedup failed: %d != %d", id2, id)
	}
	if created2 {
		t.Error("second upsert should report created=false")
	}
	// 空 logo 的重复 upsert 不得把已持久化的 logo 抹掉（原 COALESCE 合同）。
	var logo *string
	if err := d.QueryRow(`SELECT logo FROM companies WHERE id = ?`, id).Scan(&logo); err != nil {
		t.Fatal(err)
	}
	if logo == nil || *logo != "https://example.com/logo.png" {
		t.Errorf("empty logo upsert must keep persisted logo, got %v", logo)
	}
	// nkey 身份列必须写入：漏一份 hash 就等于把唯一键退化成 NULL（M09 根因）。
	if bad := countCompanyRows(t, d, `
SELECT COUNT(*) FROM companies WHERE id = ? AND (nkey_identity IS NULL OR nkey_hash IS NULL)`, id); bad != 0 {
		t.Errorf("%d company rows are missing nkey_identity/nkey_hash", bad)
	}

	c, err := cs.GetCompany(id)
	if err != nil || c == nil {
		t.Fatalf("GetCompany: %v", err)
	}
	if c.Name != "Test Corp" {
		t.Errorf("name: %q", c.Name)
	}
	// M35：DATETIME 必须以显式 UTC 文本回到 API 层，不能是驱动零值或空串。
	if _, err := time.Parse(time.RFC3339Nano, c.CreatedAt); err != nil {
		t.Errorf("created_at must be UTC RFC3339Nano text, got %q (%v)", c.CreatedAt, err)
	}
	if _, err := time.Parse(time.RFC3339Nano, c.UpdatedAt); err != nil {
		t.Errorf("updated_at must be UTC RFC3339Nano text, got %q (%v)", c.UpdatedAt, err)
	}

	// GetCompanyByName
	c2, err := cs.GetCompanyByName("test corp") // normalised
	if err != nil || c2 == nil {
		t.Fatalf("GetCompanyByName: %v", err)
	}
	if c2.ID != id {
		t.Errorf("GetCompanyByName id mismatch: %d vs %d", c2.ID, id)
	}
}

func TestCreateCompanyWithScopeRejectsNormalizedDuplicate(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	stamp := time.Now().UnixNano()
	name := fmt.Sprintf("Strict Company %d", stamp)
	id, added, _, _, validationErrors, err := cs.CreateCompanyWithScope(name, "", []ScopeInput{
		{Kind: "domain", Value: fmt.Sprintf("strict-%d.example", stamp)},
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)
	if added != 1 || len(validationErrors) != 0 {
		t.Fatalf("initial scope: added=%d errors=%v", added, validationErrors)
	}

	_, _, _, _, _, err = cs.CreateCompanyWithScope(
		"  "+strings.ToUpper(strings.ReplaceAll(name, " ", "   "))+"  ",
		"",
		[]ScopeInput{{Kind: "domain", Value: fmt.Sprintf("replacement-%d.example", stamp)}},
		"test",
	)
	if !errors.Is(err, ErrCompanyNameConflict) {
		t.Fatalf("duplicate create error=%v want ErrCompanyNameConflict", err)
	}
	scope, err := cs.GetScope(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(scope) != 1 || scope[0].Domain != fmt.Sprintf("strict-%d.example", stamp) {
		t.Fatalf("duplicate create changed existing scope: %+v", scope)
	}
	// 范围行必须带三分支中唯一适用的 identity/hash（M09）。
	if bad := countCompanyRows(t, d, `
SELECT COUNT(*) FROM company_scope WHERE company_id = ?
  AND (identity_scope_domain IS NULL OR hash_scope_domain IS NULL)`, id); bad != 0 {
		t.Errorf("%d domain scope rows are missing identity_scope_domain/hash_scope_domain", bad)
	}
}

func TestCreateCompanyWithScopeRollsBackOnScopeWriteFailure(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	name := fmt.Sprintf("Atomic Create %d", time.Now().UnixNano())
	_, _, _, _, _, err = cs.CreateCompanyWithScope(name, "", []ScopeInput{
		// utf8mb4 严格模式（STRICT_TRANS_TABLES）在写入非法 UTF-8 字节时返回 1366，
		// 用来真实触发"企业行已插入、范围写入失败"这一回滚窗口。
		{Kind: "keyword", Value: "invalid\xff-not-utf8"},
	}, "test")
	if err == nil {
		t.Fatal("expected scope database write to fail")
	}
	company, getErr := cs.GetCompanyByName(name)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if company != nil {
		defer cleanupCompany(d, company.ID)
		t.Fatalf("company row survived failed initial scope transaction: %+v", company)
	}
}

func TestCompanyScope(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("ScopeTestCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)

	lines := []string{"example.com", "192.168.1.0/24", "10.0.0.1"}
	added, skipped, invalid, errs := cs.AddScope(id, lines, "test")
	if added != 3 {
		t.Errorf("want 3 added, got %d (errs: %v)", added, errs)
	}
	if skipped != 0 || invalid != 0 {
		t.Errorf("unexpected skipped=%d invalid=%d", skipped, invalid)
	}

	// Adding again should skip (duplicate)
	added2, skipped2, invalid2, _ := cs.AddScope(id, lines, "test")
	if added2 != 0 || skipped2 != 3 {
		t.Errorf("want 0 added 3 skipped, got %d added %d skipped %d invalid", added2, skipped2, invalid2)
	}

	scope, err := cs.GetScope(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(scope) != 3 {
		t.Errorf("want 3 scope rules, got %d", len(scope))
	}
}

func TestCompanyScopeInvalid(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("InvalidScopeCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)

	// Explicitly typed TLD-only domains and overly broad CIDRs should be
	// rejected. Untyped plain text is intentionally classified as a keyword.
	inputs := []ScopeInput{
		{Kind: "domain", Value: "com"},
		{Kind: "cidr", Value: "1.2.3.4/8"},
	}
	added, _, invalid, _ := cs.AddScopeInputs(id, inputs, "test")
	if added != 0 {
		t.Errorf("want 0 added for invalid lines, got %d", added)
	}
	if invalid != 2 {
		t.Errorf("want 2 invalid, got %d", invalid)
	}
}

func TestResolveCompany(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("ResolveCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)

	cs.AddScope(id, []string{"resolve-test.io", "10.20.0.0/16"}, "test")

	// domain match
	cid, err := cs.ResolveCompany("resolve-test.io", "")
	if err != nil || cid == nil || *cid != id {
		t.Errorf("domain resolve: want %d, got %v (err %v)", id, cid, err)
	}

	// no match
	cid2, err := cs.ResolveCompany("notinscope.com", "")
	if err != nil || cid2 != nil {
		t.Errorf("no-match: want nil, got %v", cid2)
	}

	// IP/CIDR match
	cid3, err := cs.ResolveCompany("", "10.20.5.1")
	if err != nil || cid3 == nil || *cid3 != id {
		t.Errorf("cidr resolve: want %d, got %v (err %v)", id, cid3, err)
	}

	// IP outside CIDR
	cid4, err := cs.ResolveCompany("", "10.30.0.1")
	if err != nil || cid4 != nil {
		t.Errorf("cidr no-match: want nil, got %v", cid4)
	}
}

func TestUpdateScope(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("UpdateScopeCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)

	cs.AddScope(id, []string{"old-domain.com"}, "initial")

	// UpdateScope replaces
	added, invalid, errs := cs.UpdateScope(id, []string{"new-domain.com"}, "replacement")
	if added != 1 || invalid != 0 || len(errs) != 0 {
		t.Errorf("UpdateScope: added=%d invalid=%d errs=%v", added, invalid, errs)
	}

	scope, _ := cs.GetScope(id)
	if len(scope) != 1 || scope[0].Domain != "new-domain.com" {
		t.Errorf("UpdateScope: expected new-domain.com only, got %+v", scope)
	}

	// Invalid replacement input must not turn a partial validation response into
	// a destructive replacement of the existing rules.
	added, invalid, validationErrors, err := cs.UpdateScopeInputsChecked(id, []ScopeInput{
		{Kind: "domain", Value: "co.uk"},
	}, "invalid replacement")
	var validationErr *CompanyScopeValidationError
	if added != 0 || invalid != 1 || len(validationErrors) != 1 || !errors.As(err, &validationErr) {
		t.Fatalf("invalid replacement: added=%d invalid=%d validation=%v err=%v", added, invalid, validationErrors, err)
	}
	scope, err = cs.GetScope(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(scope) != 1 || scope[0].Domain != "new-domain.com" {
		t.Fatalf("invalid replacement changed existing scope: %+v", scope)
	}
}

func TestUpdateScopeRollsBackOnInsertFailure(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	name := fmt.Sprintf("Atomic Scope Update %d", time.Now().UnixNano())
	id, _, err := cs.UpsertCompany(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)
	oldDomain := fmt.Sprintf("old-%d.example", time.Now().UnixNano())
	added, _, invalid, addErrors := cs.AddScopeInputs(id, []ScopeInput{{Kind: "domain", Value: oldDomain}}, "initial")
	if added != 1 || invalid != 0 || len(addErrors) != 0 {
		t.Fatalf("seed scope: added=%d invalid=%d errors=%v", added, invalid, addErrors)
	}

	added, invalid, updateErrors := cs.UpdateScopeInputs(id, []ScopeInput{
		{Kind: "keyword", Value: "invalid\xff-not-utf8"},
	}, "replacement")
	if added != 0 || invalid != 0 || len(updateErrors) == 0 {
		t.Fatalf("failed update result: added=%d invalid=%d errors=%v", added, invalid, updateErrors)
	}
	scope, err := cs.GetScope(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(scope) != 1 || scope[0].Domain != oldDomain {
		t.Fatalf("failed replacement did not preserve old scope: %+v", scope)
	}
}

func TestDeleteCompany(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("DeleteMeCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	cs.AddScope(id, []string{"deletetest.com"}, "test")

	if err := cs.DeleteCompany(id); err != nil {
		t.Fatal(err)
	}
	c, err := cs.GetCompany(id)
	if err != nil || c != nil {
		t.Error("expected company to be gone")
	}
	// 新库没有 FK，范围规则由删除路径显式清理（M19）。
	scope, _ := cs.GetScope(id)
	if len(scope) != 0 {
		t.Errorf("expected scope cleaned up by deletion, got %d rules", len(scope))
	}
	// 不存在的企业必须报 ErrCompanyNotFound，而不是静默成功。
	if err := cs.DeleteCompany(id); !errors.Is(err, ErrCompanyNotFound) {
		t.Errorf("second delete error=%v want ErrCompanyNotFound", err)
	}
}

func TestDeleteCompanyWithAssetsDeletesBoth(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	stamp := time.Now().UnixNano()
	id, _, err := cs.UpsertCompany(fmt.Sprintf("Delete Assets Company %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)
	domain := fmt.Sprintf("delete-assets-%d.example", stamp)
	assetID := assetRawInsert(t, d, map[string]any{
		"type": "root_domain", "domain": domain, "root_domain": domain,
		"company_id": id, "company_source": "explicit",
	})
	defer d.Exec(`DELETE FROM assets WHERE id = ?`, assetID) //nolint:errcheck

	deleted, err := cs.DeleteCompanyWithAssets(id, true)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("assets deleted=%d want 1", deleted)
	}
	company, err := cs.GetCompany(id)
	if err != nil {
		t.Fatal(err)
	}
	if company != nil {
		t.Fatalf("company still exists: %+v", company)
	}
	var assetsRemaining int
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE id = ?`, assetID).Scan(&assetsRemaining); err != nil {
		t.Fatal(err)
	}
	if assetsRemaining != 0 {
		t.Fatalf("asset %d survived company deletion", assetID)
	}
}

func TestRecomputeAttribution(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()
	as := d.Assets()

	id, _, err := cs.UpsertCompany("AttributeTestCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)
	defer d.Exec(`DELETE FROM assets WHERE root_domain = 'attr-test.com'`) //nolint:errcheck

	// insert asset before adding scope
	assetID, err := as.UpsertRootDomain(UpsertRootDomainReq{Domain: "attr-test.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE id = ?`, assetID) //nolint:errcheck

	// asset should not be attributed yet
	var companyID *int64
	d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, assetID).Scan(&companyID) //nolint:errcheck
	if companyID != nil {
		t.Error("expected no company before scope added")
	}

	// add scope and recompute
	cs.AddScope(id, []string{"attr-test.com"}, "test")
	if err := cs.RecomputeAttribution(); err != nil {
		t.Fatal(err)
	}

	d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, assetID).Scan(&companyID) //nolint:errcheck
	if companyID == nil || *companyID != id {
		t.Errorf("RecomputeAttribution: expected company %d, got %v", id, companyID)
	}
}

func TestListCompanies(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	cs := d.Companies()

	id, _, err := cs.UpsertCompany("ListTestCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, id)

	companies, err := cs.ListCompanies()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range companies {
		if c.ID == id {
			found = true
		}
	}
	if !found {
		t.Error("ListCompanies: created company not found")
	}
}

// countCompanyRows 执行一个只返回单值的 COUNT 探针。
func countCompanyRows(t *testing.T, d *DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := d.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count probe %q: %v", query, err)
	}
	return n
}
