package db

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// scopeBarrierWaiters 数出"此刻正卡在行锁上、且当前语句就是企业/范围写者屏障"的会话数。
//
// TiDB 8.5 没有 information_schema.innodb_trx；等价视图是 (cluster_)tidb_trx：
// STATE='LockWaiting' 表示该会话确实排在别人的锁后面（不是"还没跑到"），
// CURRENT_SQL_DIGEST_TEXT 是归一化后的当前语句，锁等待期间不会前进。
// 优先集群视图（多实例部署下本地视图只列连到自己这台 TiDB 的会话），不可用才回退本地
// 视图 —— 与 task_context_lock_test.go 的 tidbLockWaiting 同一口径。
func scopeBarrierWaiters(t *testing.T, d *DB) int {
	t.Helper()
	const where = ` WHERE STATE = 'LockWaiting'
   AND LOWER(CURRENT_SQL_DIGEST_TEXT) LIKE '%companies%'
   AND LOWER(CURRENT_SQL_DIGEST_TEXT) LIKE '%for update%'`
	var waiters int
	if err := d.QueryRow(`SELECT COUNT(1) FROM information_schema.cluster_tidb_trx` + where).Scan(&waiters); err == nil {
		return waiters
	}
	if err := d.QueryRow(`SELECT COUNT(1) FROM information_schema.tidb_trx` + where).Scan(&waiters); err != nil {
		t.Fatalf("读取 TiDB 事务视图失败，无法证明锁等待: %v", err)
	}
	return waiters
}

// mustObserveScopeBarrierWait 建立"竞争写入确实被企业/范围屏障挡住"的**因果**屏障：
// 只有在 TiDB 事务视图里亲眼看到这条 LockWaiting 之后，调用方才允许提交 mutation；
// 等待期间竞争方一旦返回，立即判"逃逸出屏障"。
//
// 这替代的是原先"睡 1.5 秒然后当它一定被挡住了"的宽泛 sleep —— 那个写法在无锁实现
// 上同样会绿（1.5 秒足够它跑完并把结果留到 channel 里），等于没有断言。
func mustObserveScopeBarrierWait[T any](t *testing.T, d *DB, done <-chan T) {
	t.Helper()
	const budget = 20 * time.Second
	deadline := time.Now().Add(budget)
	for {
		select {
		case got := <-done:
			t.Fatalf("资产写入没有被企业/范围屏障挡住就直接返回了: %+v", got)
		default:
		}
		if scopeBarrierWaiters(t, d) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("在 %s 内没有观察到卡在 companies 写者屏障上的锁等待（屏障没有生效？）", budget)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAssetUpsertWaitsForCompanyScopeMutation 钉住"裸资产写入必须等企业/范围变更"这条
// 契约：PG 时代靠 pg_advisory_xact_lock，TiDB 侧的等价原语是 lockCompanyScopeMutation
// （companies 按主键升序 FOR UPDATE）。
//
//	mutation：持企业写者屏障，DELETE 旧范围规则 + INSERT 新范围规则
//	竞争方  ：assets.UpsertRootDomain —— **产品原有的裸资产入口**（不带 TaskID）
//
// 三道断言缺一不可：
//  1. 因果屏障：竞争方必须被**观察到**卡在屏障的加锁读上（mustObserveScopeBarrierWait），
//     不是靠 sleep 假定；
//  2. 屏障必须在产品自己的入口上生效 —— 换一个更配合的入口来"证明"屏障、或把归属断言
//     放宽成"新旧都算过"，都不构成这条回归；
//  3. mutation 提交后**立即**读回：归属必须已经是 newCompany。TiDB 悲观事务的锁等待
//     不会推进本事务快照（真实 v8.5.8 实测），所以产品必须在屏障内用当前读解析归属
//     （assets.go 的 scopeBarrier）；靠事后 RecomputeAttribution 收敛不算这条用例的
//     功劳，那是 TestRecomputeAttribution 自己的契约。
func TestAssetUpsertWaitsForCompanyScopeMutation(t *testing.T) {
	d := openTestDB(t) // 最先登记 Close → 按 LIFO 最后关，清理永远跑在活连接上
	assets, companies := d.Assets(), d.Companies()

	stamp := time.Now().UnixNano()
	domain := fmt.Sprintf("scope-lock-%d.example", stamp)
	oldCompany, _, err := companies.UpsertCompany(fmt.Sprintf("Scope Lock Old %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	newCompany, _, err := companies.UpsertCompany(fmt.Sprintf("Scope Lock New %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	// 逆拓扑清掉本用例自己的 fixture：资产 → 范围规则 → 企业。
	// Close 先登记（最后执行），所以这里跑在还活着的连接池上；错误一律判红。
	t.Cleanup(func() {
		deleteAssetsByWhere(t, d, "WHERE domain = ?", domain)
		mustCleanup(t, d, `DELETE FROM company_scope WHERE company_id IN (?,?)`, oldCompany, newCompany)
		mustCleanup(t, d, `DELETE FROM companies WHERE id IN (?,?)`, oldCompany, newCompany)
	})
	if added, _, invalid, validationErrors, err := companies.AddScopeInputsChecked(oldCompany, []ScopeInput{
		{Kind: "domain", Value: domain},
	}, "test"); err != nil || added != 1 || invalid != 0 || len(validationErrors) != 0 {
		t.Fatalf("seed old scope: added=%d invalid=%d validation=%v err=%v", added, invalid, validationErrors, err)
	}
	// 起点必须干净：别人漏网的锁等待会让下面的"观察到等待"变成假证据。
	if waiters := scopeBarrierWaiters(t, d); waiters != 0 {
		t.Fatalf("基线上已有 %d 个会话卡在 companies 屏障上，本次观察无法归属到本用例", waiters)
	}

	mutation, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer mutation.Rollback() //nolint:errcheck // 提交成功后只剩 ErrTxDone
	if err := lockCompanyScopeMutation(mutation); err != nil {
		t.Fatal(err)
	}
	if _, err := mutation.Exec(`DELETE FROM company_scope WHERE company_id=? AND kind='domain' AND domain=?`, oldCompany, domain); err != nil {
		t.Fatal(err)
	}
	if _, err := insertScopeRuleTx(mutation, newCompany, ParsedScope{
		Kind: "domain", Domain: domain, Raw: domain,
	}, "test"); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	type result struct {
		id  int64
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		close(started)
		// 被测入口 = 产品原有的裸资产写入：读 company_scope 解析归属 → 落 assets 行。
		id, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
		resultCh <- result{id: id, err: err}
	}()
	<-started
	mustObserveScopeBarrierWait(t, d, resultCh)

	if err := mutation.Commit(); err != nil {
		t.Fatal(err)
	}
	var got result
	select {
	case got = <-resultCh:
	case <-time.After(20 * time.Second):
		t.Fatal("asset upsert did not resume after scope mutation committed")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.id == 0 {
		t.Fatal("asset upsert 返回了 0 号资产")
	}
	var companyID *int64
	if err := d.QueryRow(`SELECT company_id FROM assets WHERE id=?`, got.id).Scan(&companyID); err != nil {
		t.Fatal(err)
	}
	if companyID == nil || *companyID != newCompany {
		t.Fatalf("asset retained stale company: got=%v want=%d", companyID, newCompany)
	}
}

func TestResolveAndRecomputeUseStableCompanyIDTieBreak(t *testing.T) {
	d := openTestDB(t)
	assets, companies := d.Assets(), d.Companies()

	stamp := time.Now().UnixNano()
	first, _, err := companies.UpsertCompany(fmt.Sprintf("Tie Break First %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := companies.UpsertCompany(fmt.Sprintf("Tie Break Second %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	want := min(first, second)
	domain := fmt.Sprintf("tie-break-%d.example", stamp)
	segment := uint64(stamp) & 0xffff
	network := fmt.Sprintf("2001:db8:%x:1::/64", segment)
	ip := fmt.Sprintf("2001:db8:%x:1::42", segment)
	icp := fmt.Sprintf("ICP-TIE-%d", stamp)
	appName := fmt.Sprintf("Tie Break App %d", stamp)
	// 同一套逆拓扑清理：资产 → 范围规则（本用例往两家企业各加了 3 条）→ 企业。
	t.Cleanup(func() {
		deleteAssetsByWhere(t, d, "WHERE domain=? OR ip=? OR app_name=?", domain, ip, appName)
		mustCleanup(t, d, `DELETE FROM company_scope WHERE company_id IN (?,?)`, first, second)
		mustCleanup(t, d, `DELETE FROM companies WHERE id IN (?,?)`, first, second)
	})

	rules := []ScopeInput{
		{Kind: "domain", Value: domain},
		{Kind: "cidr", Value: network},
		{Kind: "icp", Value: icp},
	}
	for _, companyID := range []int64{second, first} {
		if added, _, invalid, validationErrors, err := companies.AddScopeInputsChecked(companyID, rules, "test"); err != nil || added != len(rules) || invalid != 0 || len(validationErrors) != 0 {
			t.Fatalf("add company %d rules: added=%d invalid=%d validation=%v err=%v", companyID, added, invalid, validationErrors, err)
		}
	}

	assertResolved := func(label string, got *int64, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s resolve: %v", label, err)
		}
		if got == nil || *got != want {
			t.Fatalf("%s resolve=%v want lowest company id %d", label, got, want)
		}
	}
	got, err := companies.ResolveCompany(domain, "")
	assertResolved("domain", got, err)
	got, err = companies.ResolveCompany("", ip)
	assertResolved("network", got, err)
	got, err = companies.ResolveCompanyWithICP("", "", icp)
	assertResolved("icp", got, err)

	rootID, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
	if err != nil {
		t.Fatal(err)
	}
	ipID, err := assets.UpsertIP(UpsertIPReq{IP: ip})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := assets.UpsertApp(UpsertAppReq{Name: appName, ICP: icp})
	if err != nil {
		t.Fatal(err)
	}
	assertAssets := func(stage string) {
		t.Helper()
		for _, id := range []int64{rootID, ipID, appID} {
			var got *int64
			if err := d.QueryRow(`SELECT company_id FROM assets WHERE id=?`, id).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got == nil || *got != want {
				t.Fatalf("%s asset %d company=%v want=%d", stage, id, got, want)
			}
		}
	}
	assertAssets("live")
	if err := companies.RecomputeAttribution(); err != nil {
		t.Fatal(err)
	}
	assertAssets("recomputed")
}

func TestCompanyScopeLimitsAndCheckedErrors(t *testing.T) {
	boundary := strings.Repeat("界", MaxCompanyScopeRawRunes)
	if err := ValidateCompanyScopeInputBounds([]ScopeInput{{Kind: "keyword", Value: boundary}}); err != nil {
		t.Fatalf("exact raw rune boundary rejected: %v", err)
	}
	var validationErr *CompanyScopeValidationError
	if err := ValidateCompanyScopeInputBounds([]ScopeInput{{Kind: "keyword", Value: boundary + "界"}}); !errors.As(err, &validationErr) {
		t.Fatalf("oversized raw value error=%v want CompanyScopeValidationError", err)
	}
	// 条数不再设上限,只校验单条长度。
	if err := ValidateCompanyScopeInputBounds(make([]ScopeInput, 1000)); err != nil {
		t.Fatalf("rule count should be unbounded, got %v", err)
	}

	d := openTestDB(t)
	companies := d.Companies()
	stamp := time.Now().UnixNano()
	companyID, _, err := companies.UpsertCompany(fmt.Sprintf("Scope Limits %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupCompany(t, d, companyID) })
	// 曾经封顶 256 条,逐个 IP / 域名录范围的企业很容易撞上;现在不限条数。
	const bulk = 300
	rules := make([]ScopeInput, bulk)
	for i := range rules {
		rules[i] = ScopeInput{Kind: "keyword", Value: fmt.Sprintf("limit-%d-%d", stamp, i)}
	}
	if added, _, _, _, err := companies.AddScopeInputsChecked(companyID, rules, "test"); err != nil || added != bulk {
		t.Fatalf("bulk scope insert: added=%d want=%d err=%v", added, bulk, err)
	}
	if added, _, _, _, err := companies.AddScopeInputsChecked(companyID, []ScopeInput{
		{Kind: "keyword", Value: fmt.Sprintf("limit-%d-extra", stamp)},
	}, "test"); added != 1 || err != nil {
		t.Fatalf("append past the old cap: added=%d err=%v", added, err)
	}
	var count int
	if err := d.QueryRow(`SELECT count(*) FROM company_scope WHERE company_id=?`, companyID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != bulk+1 {
		t.Fatalf("scope count=%d want=%d", count, bulk+1)
	}

	missingID := companyID + 1_000_000_000
	if _, _, _, _, err := companies.AddScopeInputsChecked(missingID, nil, "test"); !errors.Is(err, ErrCompanyNotFound) {
		t.Fatalf("missing add error=%v want ErrCompanyNotFound", err)
	}
	if _, _, _, err := companies.UpdateScopeInputsChecked(missingID, nil, "test"); !errors.Is(err, ErrCompanyNotFound) {
		t.Fatalf("missing update error=%v want ErrCompanyNotFound", err)
	}
	if _, err := companies.DeleteCompanyWithAssets(missingID, true); !errors.Is(err, ErrCompanyNotFound) {
		t.Fatalf("missing delete error=%v want ErrCompanyNotFound", err)
	}
}

func TestCompanyScopeCheckedReturnsSystemErrorSeparately(t *testing.T) {
	// DSN 已配置却打不开 → openTestDB 直接判红（M-02），不再 Skip。
	d := openTestDB(t)
	companies := d.Companies()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, _, validationErrors, err := companies.AddScopeInputsChecked(1, nil, "test")
	if err == nil {
		t.Fatal("closed database did not return a system error")
	}
	if len(validationErrors) != 0 {
		t.Fatalf("system error leaked into validation errors: %v", validationErrors)
	}
	var validationErr *CompanyScopeValidationError
	if errors.As(err, &validationErr) || errors.Is(err, ErrCompanyNotFound) {
		t.Fatalf("system error misclassified: %v", err)
	}
}
