package db

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestParseScopeLine covers classification + guardrails without a DB.
func TestParseScopeLine(t *testing.T) {
	cases := []struct {
		in      string
		kind    string
		wantErr bool
	}{
		{"example.com", "domain", false},
		{"https://sub.example.com/path", "domain", false},
		{"1.2.3.4", "ip", false},
		{"10.0.0.0/8", "", true}, // over-broad IPv4 (< /16)
		{"198.51.100.0/24", "cidr", false},
		{"co.uk", "", true}, // bare public suffix
		{"not a host", "", true},
		{"1.2.3.1-1.2.3.9", "", true}, // ranges must be CIDR
	}
	for _, c := range cases {
		r, err := ParseScopeLine(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseScopeLine(%q) want error, got %+v", c.in, r)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseScopeLine(%q) unexpected error: %v", c.in, err)
			continue
		}
		if r.Kind != c.kind {
			t.Errorf("ParseScopeLine(%q) kind=%q want %q", c.in, r.Kind, c.kind)
		}
	}
}

func TestParseAutoScopeLine(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		kind       string
		normalized string
		wantErr    bool
	}{
		{name: "domain", input: "example.com", kind: "domain", normalized: "example.com"},
		{name: "url", input: "https://sub.example.com/path", kind: "domain", normalized: "sub.example.com"},
		{name: "url query", input: "https://example.com/path?source=x", kind: "domain", normalized: "example.com"},
		{name: "url credentials", input: "https://user:pass@example.com/path", kind: "domain", normalized: "example.com"},
		{name: "url ip", input: "http://203.0.113.10/path", kind: "ip", normalized: "203.0.113.10/32"},
		{name: "ipv4", input: "203.0.113.10", kind: "ip", normalized: "203.0.113.10/32"},
		{name: "ipv6", input: "2001:db8::10", kind: "ip", normalized: "2001:db8::10/128"},
		{name: "cidr", input: "198.51.100.0/24", kind: "cidr", normalized: "198.51.100.0/24"},
		{name: "icp latin", input: "京 ICP备 123号", kind: "icp", normalized: "京icp备123号"},
		{name: "icp chinese", input: "沪网备案 9988", kind: "icp", normalized: "沪网备案9988"},
		{name: "icp domain", input: "icp.example.com", kind: "domain", normalized: "icp.example.com"},
		{name: "icp url query", input: "https://example.com/path?icp=1", kind: "domain", normalized: "example.com"},
		// 备案号不含点号：掺了域名/版本号的描述性文字归关键词，否则会存成一条
		// 永远匹配不上的死 ICP 规则。
		{name: "icp with domain text", input: "备案 www.example.com", kind: "keyword", normalized: "备案 www.example.com"},
		{name: "icp with version text", input: "某公司 ICP v1.0", kind: "keyword", normalized: "某公司 icp v1.0"},
		{name: "icp fullwidth dot", input: "备案 例．com", kind: "keyword", normalized: "备案 例．com"},
		{name: "keyword", input: "  ACME   Security  ", kind: "keyword", normalized: "acme security"},
		{name: "colon keyword", input: "ACME: Cloud: Security", kind: "keyword", normalized: "acme: cloud: security"},
		{name: "empty", input: "  ", wantErr: true},
		{name: "invalid cidr", input: "10.0.0.0/not-a-prefix", wantErr: true},
		{name: "invalid ipv4", input: "999.0.0.1", wantErr: true},
		{name: "invalid ipv6", input: "2001:db8::zz", wantErr: true},
		{name: "invalid ipv6 cidr", input: "2001:db8::zz/64", wantErr: true},
		{name: "overbroad cidr", input: "10.0.0.0/8", wantErr: true},
		{name: "bare suffix", input: "co.uk", wantErr: true},
		{name: "empty domain label", input: "foo..example.com", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rule, err := ParseAutoScopeLine(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseAutoScopeLine(%q) = %+v, want error", tc.input, rule)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAutoScopeLine(%q): %v", tc.input, err)
			}
			if rule.Kind != tc.kind {
				t.Fatalf("kind=%q want %q", rule.Kind, tc.kind)
			}
			got := rule.Value
			if rule.Kind == "domain" {
				got = rule.Domain
			} else if rule.Kind == "ip" || rule.Kind == "cidr" {
				got = rule.Net
			}
			if got != tc.normalized {
				t.Fatalf("normalized=%q want %q", got, tc.normalized)
			}
		})
	}
}

func TestExplicitCompanyAttributionSurvivesScopeRebuild(t *testing.T) {
	d, as, cs := testSetup(t)

	stamp := time.Now().UnixNano()
	explicitCompany, _, err := cs.UpsertCompany(fmt.Sprintf("Explicit Attribution %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	autoCompany, _, err := cs.UpsertCompany(fmt.Sprintf("Automatic Attribution %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(t, d, explicitCompany)
	defer cleanupCompany(t, d, autoCompany)

	domain := fmt.Sprintf("explicit-%d.invalid", stamp)
	icp := fmt.Sprintf("ICP-%d", stamp)
	network := fmt.Sprintf("2001:db8:%x::/64", uint64(stamp)&0xffff)
	ip := fmt.Sprintf("2001:db8:%x::10", uint64(stamp)&0xffff)

	// A pre-existing row with company_id and no provenance value represents old
	// installations. The schema default conservatively treats it as explicit.
	//
	// TiDB 侧：assets.company_source 是 NOT NULL 且 TEXT 无 DDL 默认值，"没写来源"这个
	// 状态在 TiDB 里不存在，因此夹具直接按 PG 当年的列默认值写入 'explicit'；自增 id 用
	// LastInsertId 取回（TiDB 没有 RETURNING）。
	assetID := assetRawInsert(t, d, map[string]any{
		"type": "root_domain", "domain": domain, "root_domain": domain,
		"company_id": explicitCompany, "company_source": "explicit",
	})
	defer deleteAsset(t, d, assetID)
	appID, err := as.UpsertApp(UpsertAppReq{
		Name: fmt.Sprintf("explicit-app-%d", stamp), ICP: icp, CompanyID: &explicitCompany,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(t, d, appID)
	autoAssetID, err := as.UpsertIP(UpsertIPReq{IP: ip})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(t, d, autoAssetID)

	rules := []ScopeInput{
		{Kind: "domain", Value: domain},
		{Kind: "icp", Value: icp},
		{Kind: "cidr", Value: network},
	}
	if added, _, invalid, errs := cs.AddScopeInputs(autoCompany, rules, "test"); added != len(rules) || invalid != 0 {
		t.Fatalf("AddScopeInputs: added=%d invalid=%d errors=%v", added, invalid, errs)
	}

	assertCompany := func(id, want int64, source string) {
		t.Helper()
		var got *int64
		var gotSource string
		if err := d.QueryRow(`SELECT company_id,company_source FROM assets WHERE id=?`, id).Scan(&got, &gotSource); err != nil {
			t.Fatal(err)
		}
		if got == nil || *got != want || gotSource != source {
			t.Fatalf("asset %d company=%v source=%q, want %d/%q", id, got, gotSource, want, source)
		}
	}
	assertCompany(assetID, explicitCompany, "explicit")
	assertCompany(appID, explicitCompany, "explicit")
	assertCompany(autoAssetID, autoCompany, "scope")

	// Replacing every matching rule detaches only the automatically-owned row.
	if _, invalid, errs := cs.UpdateScopeInputs(autoCompany, []ScopeInput{
		{Kind: "domain", Value: fmt.Sprintf("replacement-%d.invalid", stamp)},
	}, "test"); invalid != 0 || len(errs) != 0 {
		t.Fatalf("UpdateScopeInputs: invalid=%d errors=%v", invalid, errs)
	}
	assertCompany(assetID, explicitCompany, "explicit")
	assertCompany(appID, explicitCompany, "explicit")
	var autoCompanyID *int64
	var autoSource string
	if err := d.QueryRow(`SELECT company_id,company_source FROM assets WHERE id=?`, autoAssetID).Scan(&autoCompanyID, &autoSource); err != nil {
		t.Fatal(err)
	}
	if autoCompanyID != nil || autoSource != "scope" {
		t.Fatalf("automatic asset was not detached: company=%v source=%q", autoCompanyID, autoSource)
	}

	if err := cs.RecomputeAttribution(); err != nil {
		t.Fatal(err)
	}
	assertCompany(assetID, explicitCompany, "explicit")
	assertCompany(appID, explicitCompany, "explicit")

	// Deleting the explicitly selected company detaches through the FK, then the
	// transactional rebuild may adopt the assets into a still-valid scope.
	if _, invalid, errs := cs.UpdateScopeInputs(autoCompany, []ScopeInput{
		{Kind: "domain", Value: domain},
		{Kind: "icp", Value: icp},
	}, "test"); invalid != 0 || len(errs) != 0 {
		t.Fatalf("restore fallback scope: invalid=%d errors=%v", invalid, errs)
	}
	if err := cs.DeleteCompany(explicitCompany); err != nil {
		t.Fatal(err)
	}
	assertCompany(assetID, autoCompany, "scope")
	assertCompany(appID, autoCompany, "scope")
}

func TestParseStructuredCompanyScope(t *testing.T) {
	icp, err := ParseScopeInput(ScopeInput{Kind: "ICP", Value: " 京ICP 备 123号-1\t"})
	if err != nil {
		t.Fatalf("parse ICP: %v", err)
	}
	if icp.Kind != "icp" || icp.Value != "京icp备123号-1" {
		t.Fatalf("unexpected normalized ICP: %+v", icp)
	}
	keyword, err := ParseScopeInput(ScopeInput{Kind: "keyword", Value: "  ACME   Security  "})
	if err != nil {
		t.Fatalf("parse keyword: %v", err)
	}
	if keyword.Value != "acme security" {
		t.Fatalf("unexpected normalized keyword: %+v", keyword)
	}
	if _, err := ParseScopeInput(ScopeInput{Kind: "ip", Value: "example.com"}); err == nil {
		t.Fatal("typed IP accepted a domain")
	}
}

func TestCompanyICPAttribution(t *testing.T) {
	// 这里不再额外登记 `t.Cleanup(func() { d.Close() })`：openTestDB 已把 Close 注册为
	// 最先一条，LIFO 保证本用例随后登记的清理都跑在还活着的连接池上，并由它把首次
	// 关闭的错误判红。先前这句重复关闭不仅多余，还会吞掉并掩盖首次关闭的错误（审查 R-03）。
	//
	// 历史注释：曾有 `defer d.Close()` 让清理全落在已关闭连接上且吞错，资产与公司永久
	// 残留；残留一旦与别的用例按 id 的断言撞上，排查成本极高。
	d := openTestDB(t)

	suffix := time.Now().UnixNano()
	// 资产写者协议要求父任务真实存活。原先这里拿 `MAX(companies.id)+1` 当假 TaskID
	// 给资产打标：TiDB 基线上它会被 lockTaskRowAlive 直接拒掉，偶尔撞上真实任务号
	// 又会假通过 —— 一个看运气的用例。改用真实任务打标，清理口径（按 task_ids 里的
	// 这个 id 反查删除）完全不变。
	fixtureTask, err := d.CreateTask("ICP attribution fixture", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	cs := d.Companies()
	as := d.Assets()
	companyID, _, err := cs.UpsertCompany(fmt.Sprintf("ICP Scope Co %d", suffix), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 不吞错误：清理失败会污染后续用例，必须让它在本次运行里显形。
		// 原 PG 写法 `task_ids @> ARRAY[$1]::bigint[]`：TiDB 的 task_ids 是 JSON 数组
		// 文本，归属判断统一走 JSON_CONTAINS（与生产侧同一口径）。
		deleteAssetsByWhere(t, d, `WHERE JSON_CONTAINS(COALESCE(task_ids,'[]'), CAST(? AS JSON))`, fixtureTask.ID)
		// company_scope 必须在 companies 之前逆拓扑删除：下方 AddScopeInputs 每次成功
		// 运行都给该企业写 icp+keyword 两条规则，企业行删掉后它们就是永久孤儿，每轮
		// 累积两条；低 id 的残留规则还会把下一轮同值资产的归属抢走（审查 R-03）。
		if _, err := d.Exec(`DELETE FROM company_scope WHERE company_id=?`, companyID); err != nil {
			t.Errorf("清理测试公司范围规则失败: %v", err)
		}
		if _, err := d.Exec(`DELETE FROM companies WHERE id=?`, companyID); err != nil {
			t.Errorf("清理测试公司失败: %v", err)
		}
		// DeleteTask 的 lockorder 定位器会按归属逐个 FOR UPDATE 复核资产；上面的删除
		// 只清了 assets 行，task_asset_links 还挂着，会被判成"成员漂移"而整笔回滚。
		if _, err := d.Exec(`DELETE FROM task_asset_links WHERE task_id=?`, fixtureTask.ID); err != nil {
			t.Errorf("清理打标任务归属失败: %v", err)
		}
		if err := d.DeleteTask(fixtureTask.ID); err != nil {
			t.Errorf("清理打标任务失败: %v", err)
		}
	})

	// ICP 号每轮唯一：归属平票时按 company_id 升序取第一条，历史运行残留的同值规则
	// 会因为 id 更小先把资产认领走，本轮新建的公司就永远对不上。
	// 空格与大小写的差异刻意保留 —— 本用例测的就是 NormalizeICP 的归一化。
	icpRule := fmt.Sprintf("京 ICP备 %d号", suffix)
	icpAsset := fmt.Sprintf("京icp备%d号", suffix)
	icpAssetSpaced := fmt.Sprintf(" 京 ICP备 %d号 ", suffix)

	// A keyword can guide an Agent, but must never claim an asset by its name.
	added, _, invalid, errs := cs.AddScopeInputs(companyID, []ScopeInput{
		{Kind: "icp", Value: icpRule},
		{Kind: "keyword", Value: fmt.Sprintf("ICP Scope %d", suffix)},
	}, "unit test")
	if added != 2 || invalid != 0 || len(errs) != 0 {
		t.Fatalf("add structured scope: added=%d invalid=%d errors=%v", added, invalid, errs)
	}

	rootID, err := as.UpsertRootDomain(UpsertRootDomainReq{
		Domain: fmt.Sprintf("icp-scope-%d.example", suffix), ICP: icpAsset, TaskID: fixtureTask.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := as.UpsertApp(UpsertAppReq{
		Name: fmt.Sprintf("ICP Scope Keyword Only %d", suffix), TaskID: fixtureTask.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	icpAppID, err := as.UpsertApp(UpsertAppReq{
		Name: fmt.Sprintf("ICP Matched App %d", suffix), ICP: icpAssetSpaced, TaskID: fixtureTask.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertCompany := func(assetID int64, want *int64) {
		t.Helper()
		var got *int64
		if err := d.QueryRow(`SELECT company_id FROM assets WHERE id=?`, assetID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if want == nil && got != nil {
			t.Fatalf("asset %d attributed by keyword: %d", assetID, *got)
		}
		if want != nil && (got == nil || *got != *want) {
			// got 是指针：直接 %v 打出来是地址，排查时会把人带偏，这里显式解引用。
			var gotText string
			if got == nil {
				gotText = "NULL"
			} else {
				gotText = fmt.Sprint(*got)
			}
			t.Fatalf("asset %d company=%s want %d", assetID, gotText, *want)
		}
	}
	assertCompany(rootID, &companyID)
	assertCompany(appID, nil)
	assertCompany(icpAppID, &companyID)
}

// TestCompanyScopeAttribution exercises the full loop against dev PG: create
// company (unique name), add scope, and verify auto-attribution at insert time,
// backfill of a pre-existing asset, CIDR + domain-suffix matching, and that
// out-of-scope assets stay unattributed.
func TestCompanyScopeAttribution(t *testing.T) {
	d := openTestDB(t)
	as := d.Assets()
	cs := d.Companies()

	// MAX(assets.id) 只用来取本用例名称/网段的递增唯一种子（uniq），与清理无关。
	// 原写法 `DELETE FROM assets WHERE id > startMax` 删的是"本用例开始后产生的全部
	// 资产"（assets 为空时 startMax=0 等价 DELETE FROM assets）：共享库上会连带删掉
	// 并发写入方与别的用例的数据，还把别人的泄漏一并抹掉、让污染不可见（审查 R-03
	// 明确禁止）。改为精确记录本用例创建的 asset id 集合，逐 id 删除。
	var startMax int64
	if err := d.QueryRow(`SELECT COALESCE(MAX(id),0) FROM assets`).Scan(&startMax); err != nil {
		t.Fatalf("startMax: %v", err)
	}
	uniq := startMax + 1

	// 清理钩子一次性登记在这里：闭包捕获 cid/createdAssetIDs，在测试结束时读取最终
	// 值。逆拓扑删除 assets → company_scope → companies，任何一步失败必须 t.Errorf
	// 判红，不许 `_, _ =` 吞错——残留的 CIDR/域名规则会持续认领后续运行的资产，低 id
	// 规则还会在归属平票时抢先胜出（审查 R-03）。
	// d.Close() 也不在这里重复：openTestDB 已最先登记 Close，重复关闭多余且吞错。
	var cid int64
	var createdAssetIDs []int64
	t.Cleanup(func() {
		for _, id := range createdAssetIDs {
			deleteAsset(t, d, id) // 先断开 link/anchor 引用，再删本体（TiDB 无外键）
		}
		if cid != 0 {
			mustCleanup(t, d, `DELETE FROM company_scope WHERE company_id = ?`, cid)
			mustCleanup(t, d, `DELETE FROM companies WHERE id = ?`, cid)
		}
	})

	root := fmt.Sprintf("scopetest%d.com", uniq)
	sub := "api." + root
	// 每轮换一个**独立的 TEST-NET /24**（仍在 198.51.0.0/16 与 203.0.113.0/24 这类
	// 保留段内）：资产按 canonical 身份去重、company_id 一旦写下不会被后来者抢走，
	// 沿用固定网段就会被上一轮残留的公司（它的 CIDR 规则还挂在 company_scope 上，
	// 且 id 更小、在归属平票时胜出）先认领掉，本轮新公司再也对不上。
	octet := uniq%250 + 1
	ipNet := fmt.Sprintf("198.51.%d.0/24", octet)
	ipIn := fmt.Sprintf("198.51.%d.9", octet)
	ipOut := fmt.Sprintf("203.0.%d.9", octet)
	outDomain := fmt.Sprintf("other%d.net", uniq)

	cid, _, err := cs.UpsertCompany(fmt.Sprintf("ScopeCo %d", uniq), "")
	if err != nil {
		t.Fatalf("UpsertCompany: %v", err)
	}

	// a pre-existing asset (inserted BEFORE any scope) — must be back-filled.
	preID, err := as.UpsertSubdomain(UpsertSubdomainReq{Domain: sub})
	if err != nil {
		t.Fatalf("pre upsert: %v", err)
	}
	createdAssetIDs = append(createdAssetIDs, preID)
	var preCompanyID *int64
	d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, preID).Scan(&preCompanyID)
	if preCompanyID != nil {
		t.Fatalf("pre-scope asset should be unattributed, got %v", *preCompanyID)
	}

	// 公司与其范围规则（cid/company_scope）已并入函数开头的统一清理钩子，
	// 按精确 id 逆拓扑删除并判红（审查 R-03）。

	cs.AddScope(cid, []string{root, ipNet}, "unit test")

	// 真机实测：AddScope 的归属重算会把规则域名**物化**成一条 root_domain 资产
	// （scopetest<uniq>.com，归属 cid）。它不是本用例显式 Upsert 出来的，精确 id 清理
	// 钩子事先拿不到它的 id，所以在 AddScope 之后按本用例专属且每轮唯一的域名精确
	// 找回、并入清理清单（审查 R-03：只删本用例造成的行，且每步判红）。
	var derivedRootID int64
	switch err := d.QueryRow(`SELECT id FROM assets WHERE type = 'root_domain' AND domain = ?`, root).
		Scan(&derivedRootID); err {
	case nil:
		createdAssetIDs = append(createdAssetIDs, derivedRootID)
	case sql.ErrNoRows:
		// 产品若不再物化该行则无需登记；不算失败。
	default:
		t.Errorf("找回 AddScope 物化的 root_domain 资产失败（可能留下残留）: %v", err)
	}

	mustCid := func(id int64, want int64, label string) {
		var cID *int64
		d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, id).Scan(&cID)
		if cID == nil {
			t.Fatalf("%s company_id = nil, want %d", label, want)
		}
		if *cID != want {
			t.Fatalf("%s company_id = %d, want %d", label, *cID, want)
		}
	}
	mustNil := func(id int64, label string) {
		var cID *int64
		d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, id).Scan(&cID)
		if cID != nil {
			t.Fatalf("%s should be unattributed, got %d", label, *cID)
		}
	}

	// backfill attributed the pre-existing subdomain (domain suffix match).
	mustCid(preID, cid, "pre-existing subdomain (backfill)")

	// insert-time attribution: ip in CIDR, another subdomain.
	ipInID, err := as.UpsertIP(UpsertIPReq{IP: ipIn})
	if err != nil {
		t.Fatalf("UpsertIP in: %v", err)
	}
	createdAssetIDs = append(createdAssetIDs, ipInID)
	mustCid(ipInID, cid, "in-CIDR ip (insert-time)")

	sub2ID, err := as.UpsertSubdomain(UpsertSubdomainReq{Domain: "www." + root})
	if err != nil {
		t.Fatalf("UpsertSubdomain: %v", err)
	}
	createdAssetIDs = append(createdAssetIDs, sub2ID)
	mustCid(sub2ID, cid, "new subdomain (insert-time)")

	// out of scope stays unattributed.
	outID, err := as.UpsertRootDomain(UpsertRootDomainReq{Domain: outDomain})
	if err != nil {
		t.Fatalf("UpsertRootDomain out: %v", err)
	}
	createdAssetIDs = append(createdAssetIDs, outID)
	mustNil(outID, "out-of-scope domain")

	ipOutID, err := as.UpsertIP(UpsertIPReq{IP: ipOut})
	if err != nil {
		t.Fatalf("UpsertIP out: %v", err)
	}
	createdAssetIDs = append(createdAssetIDs, ipOutID)
	mustNil(ipOutID, "out-of-CIDR ip")
}
