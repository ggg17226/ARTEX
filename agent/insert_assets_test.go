package agent

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)
// testDB 打开 TiDB（spec §3.1.1：唯一数据源是 ARTEX_TIDB_DSN / database.dsn，
// ARTEX_PG_DSN 与 postgres:// 已彻底废除）。
//
// 诚实门控三段式，绝不存在"永久 Skip"：
//   - 压根没配数据库（db.DSN() 报错）→ t.Skipf 明说"未配置"，不当作通过；
//   - 配了却打不开（连接失败 / 65 表基线缺失）→ 环境违规，t.Fatalf 判红，
//     绝不退回 Skip 假绿（那正是本轮迁移要消灭的东西）；
//   - 连接池的 Close 在这里**最先**登记为 t.Cleanup，按 LIFO 最后执行，
//     所以用例随后登记的 fixture 清理一定跑在还活着的池上。用例**不得**再
//     自己写 defer d.Close() / t.Cleanup(d.Close)（审查 I-03/I-05 同一口径）。
func testDB(t *testing.T) *db.DB {
	t.Helper()
	dsn, _, err := db.DSN()
	if err != nil {
		t.Skipf("ARTEX_TIDB_DSN not set — 未配置 TiDB 数据源，跳过集成用例 (%v)", err)
	}
	d, err := db.Open(dsn)
	if err != nil {
		t.Fatalf("数据源已配置但打开 TiDB 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := d.Close(); closeErr != nil {
			t.Errorf("关闭 TiDB 连接池失败: %v", closeErr)
		}
	})
	return d
}

// runTag 生成本轮用例的唯一标记（形如 "x1a2b3c4"），用来拼进 fixture 的域名，
// 让"这一轮到底写了哪些 assets 行"有可精确回读的物理判据；随机碰撞概率可忽略，
// 因此清理语句永远圈不到别人或历史遗留的数据。
func runTag(t *testing.T) string {
	t.Helper()
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("生成 fixture 标记失败: %v", err)
	}
	return "x" + hex.EncodeToString(raw[:])
}

// fixtureIP 从 RFC 5737 文档保留网段里取一个本轮随机主机位，避免与共享库中
// 真实业务资产的 IP 撞车（也保证清理只圈到自己写的行）。
func fixtureIP(t *testing.T, network string) string {
	t.Helper()
	var raw [1]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("生成 fixture IP 失败: %v", err)
	}
	return fmt.Sprintf("%s.%d", network, 2+int(raw[0])%250)
}

// cleanupAssets 登记一条 assets fixture 清理：先按谓词取回本轮真实主键，再按
// "引用表 → assets 本体"的逆序物理删净，最后按同一谓词回读断言归零。
//
// 为什么要把 task_asset_links / finding_assets / exploration_anchors 一起删：
// 产品里的 AssetStore.DeleteByIDs 就是一条 `DELETE FROM assets WHERE id IN (...)`，
// PG 时代靠外键 ON DELETE CASCADE 带走的引用行，在 TiDB 基线上会原样留下 —— 而
// DeleteTask 的锁定器锁不到那条资产就判定"成员漂移"，整笔回滚并**永久拒绝**删除
// 对应任务（共享库里已有一条这样的悬空 link 实证）。清理失败、回读失败、残留非零
// 一律 t.Errorf 判红：共享库留下孤儿行，下一轮用例就踩坑，"跑完查库"也证明不了终态。
func cleanupAssets(t *testing.T, d *db.DB, where string, args ...any) {
	t.Helper()
	t.Cleanup(func() {
		rows, err := d.Query(`SELECT id FROM assets WHERE `+where, args...)
		if err != nil {
			t.Errorf("回读 assets fixture 主键失败: %q args=%v: %v", where, args, err)
			return
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Errorf("扫描 assets fixture 主键失败: %v", err)
				return
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Errorf("回读 assets fixture 主键失败: %v", err)
			return
		}
		if len(ids) > 0 {
			ph := strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", ")
			refs := make([]any, 0, len(ids))
			for _, id := range ids {
				refs = append(refs, id)
			}
			for _, step := range []struct {
				table string
				sql   string
			}{
				{"task_asset_links", `DELETE FROM task_asset_links WHERE asset_id IN (` + ph + `)`},
				{"finding_assets", `DELETE FROM finding_assets WHERE asset_id IN (` + ph + `)`},
				{"exploration_anchors", `DELETE FROM exploration_anchors WHERE asset_id IN (` + ph + `)`},
			} {
				if _, err := d.Exec(step.sql, refs...); err != nil {
					t.Errorf("清理 %s fixture 失败: %q args=%v: %v", step.table, step.sql, refs, err)
				}
			}
		}
		if _, err := d.Exec(`DELETE FROM assets WHERE `+where, args...); err != nil {
			t.Errorf("清理 assets fixture 失败: %q args=%v: %v", where, args, err)
		}
		var left int
		if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE `+where, args...).Scan(&left); err != nil {
			t.Errorf("回读 assets fixture 残留失败: %q args=%v: %v", where, args, err)
			return
		}
		if left != 0 {
			t.Errorf("assets fixture 未归零: %q args=%v 仍残留 %d 行", where, args, left)
		}
	})
}

// mustQuery 跑一条"最多一行"的探测查询。查不到行（sql.ErrNoRows）保持原语义 ——
// 目标变量留零值，由用例自己的断言判红；其它错误（SQL 方言、连接、类型）立即
// 判红，绝不把"查询本身失败"伪装成"数据不对"。
func mustQuery(t *testing.T, run func() error) {
	t.Helper()
	if err := run(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("回读 fixture 状态失败: %v", err)
	}
}

// cleanupInterceptPending 登记一条 intercept_pending fixture 清理：按 task_id
// （本轮随机标记拼出来的）物理删净并回读断言归零，失败一律 t.Errorf 判红。
func cleanupInterceptPending(t *testing.T, d *db.DB, taskID string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := d.Exec(`DELETE FROM intercept_pending WHERE task_id=?`, taskID); err != nil {
			t.Errorf("清理 intercept_pending fixture 失败 (task_id=%s): %v", taskID, err)
		}
		var left int
		if err := d.QueryRow(`SELECT COUNT(*) FROM intercept_pending WHERE task_id=?`, taskID).Scan(&left); err != nil {
			t.Errorf("回读 intercept_pending fixture 残留失败 (task_id=%s): %v", taskID, err)
			return
		}
		if left != 0 {
			t.Errorf("intercept_pending fixture 未归零 (task_id=%s): 残留 %d 行", taskID, left)
		}
	})
}

// cleanupExplorationFixture 逆拓扑删净一次 exploration fixture。
//
// TiDB 基线不建外键，删掉 explorations 父行不会级联：exploration_nodes /
// exploration_edges / exploration_anchors / activity / task_constraints 会整批
// 留在库里指向一条已经不存在的 exploration。这里按"子表 → 父表"逐张物理删净
// （anchors 没有 exploration_id 列，只能经 exploration_nodes JOIN 按归属删），
// 最后回读断言归零；任何一步失败都 t.Errorf 判红。
// expID 是本轮新建 exploration 的自增主键，天生唯一，圈不到任何别人的数据。
func cleanupExplorationFixture(t *testing.T, d *db.DB, expID int64) {
	t.Helper()
	t.Cleanup(func() {
		steps := []struct {
			text string
			args []any
		}{
			{`DELETE FROM intercept_pending WHERE task_id=?`, []any{fmt.Sprint(expID)}},
			{`DELETE FROM llm_usage WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM tool_usage WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM skill_usage WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM task_constraints WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM activity WHERE exploration_id=?`, []any{expID}},
			{`DELETE anchor FROM exploration_anchors anchor
 JOIN exploration_nodes node ON node.id = anchor.node_id
 WHERE node.exploration_id=?`, []any{expID}},
			{`DELETE FROM exploration_edges WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM exploration_nodes WHERE exploration_id=?`, []any{expID}},
			{`DELETE FROM explorations WHERE id=?`, []any{expID}},
		}
		for _, step := range steps {
			if _, err := d.Exec(step.text, step.args...); err != nil {
				t.Errorf("清理 exploration fixture %d 失败: %q args=%v: %v", expID, step.text, step.args, err)
			}
		}
		var left int
		if err := d.QueryRow(`SELECT
 (SELECT COUNT(*) FROM explorations WHERE id=?)
+(SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?)
+(SELECT COUNT(*) FROM exploration_edges WHERE exploration_id=?)
+(SELECT COUNT(*) FROM activity WHERE exploration_id=?)`, expID, expID, expID, expID).Scan(&left); err != nil {
			t.Errorf("回读 exploration fixture 残留失败 (%d): %v", expID, err)
			return
		}
		if left != 0 {
			t.Errorf("exploration fixture %d 未归零: 残留 %d 行", expID, left)
		}
	})
}

// callInsertAssets calls the insert_assets tool with the given payload.
func callInsertAssets(t *testing.T, ts *ToolSet, payload any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(payload)
	tool := ts.insertAssets()
	res, err := tool.Call(context.Background(), raw, nil)
	if err != nil {
		t.Fatalf("insertAssets Call error: %v", err)
	}
	text := res.Flatten()
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal result: %v\nraw: %s", err, text)
	}
	return out
}

// =====================================================================
// TestInsertAssetsSubdomainSideEffects
// 子域名插入 → 自动创建 root_domain + IP 资产，IP 绑定域名
// =====================================================================
func TestInsertAssetsSubdomainSideEffects(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	ip := fixtureIP(t, "203.0.113")
	root := tag + "-sideeffect-test.com"
	sub := "ia-sub." + root

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `domain IN (?,?) OR ip=?`, sub, root, ip)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			map[string]any{
				"type":         "subdomain",
				"domain":       sub,
				"record_type":  "A",
				"record_value": []string{ip},
			},
		},
		"task_id": 999,
	})

	// no errors
	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	results, _ := out["results"].([]any)
	if len(results) == 0 {
		t.Fatal("no results returned")
	}

	// root_domain should exist
	var rootCnt int
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type='root_domain' AND domain=?`, root).Scan(&rootCnt)
	})
	if rootCnt != 1 {
		t.Errorf("side-effect: root_domain not created, got %d", rootCnt)
	}

	// IP asset should exist with bound_domains containing our subdomain
	// （TiDB：bound_domains 是 JSON 文本列，直接读原文再解，PG 的 array_to_json 已废除）
	var ipID int64
	var boundDomains []byte
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT id, bound_domains FROM assets WHERE type='ip' AND ip=?`, ip).Scan(&ipID, &boundDomains)
	})
	if ipID == 0 {
		t.Error("side-effect: IP asset not created")
	}
	var domains []string
	json.Unmarshal(boundDomains, &domains)
	found := false
	for _, dom := range domains {
		if dom == sub {
			found = true
		}
	}
	if !found {
		t.Errorf("side-effect: bound_domains should contain subdomain, got %v", domains)
	}

	// record_value stored as array
	var rvRaw []byte
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT record_value FROM assets WHERE type='subdomain' AND domain=?`, sub).Scan(&rvRaw)
	})
	var rv []string
	json.Unmarshal(rvRaw, &rv)
	if len(rv) == 0 || rv[0] != ip {
		t.Errorf("record_value stored incorrectly: %v", rv)
	}
}

// =====================================================================
// TestInsertAssetsMultiIPSubdomain
// 多个 IP 的子域名：所有 IP 都应存入 record_value[]，各自创建 IP 资产
// =====================================================================
func TestInsertAssetsMultiIPSubdomain(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	ip1 := fixtureIP(t, "198.51.100")
	ip2 := fixtureIP(t, "198.51.100")
	root := tag + "-multiip-test.io"
	sub := "multi." + root

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `domain IN (?,?) OR ip IN (?,?)`, sub, root, ip1, ip2)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			map[string]any{
				"type":         "subdomain",
				"domain":       sub,
				"record_type":  "A",
				"record_value": []string{ip1, ip2},
			},
		},
	})

	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}

	// Both IPs should have IP assets
	var ip1Cnt, ip2Cnt int
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type='ip' AND ip=?`, ip1).Scan(&ip1Cnt)
	})
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type='ip' AND ip=?`, ip2).Scan(&ip2Cnt)
	})
	if ip1Cnt != 1 {
		t.Error("IP " + ip1 + " asset not created")
	}
	if ip2Cnt != 1 {
		t.Error("IP " + ip2 + " asset not created")
	}

	// record_value should contain both IPs
	var rvRaw []byte
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT record_value FROM assets WHERE type='subdomain' AND domain=?`, sub).Scan(&rvRaw)
	})
	var rv []string
	json.Unmarshal(rvRaw, &rv)
	if len(rv) != 2 {
		t.Errorf("record_value: want 2 IPs, got %v", rv)
	}
}

// =====================================================================
// TestInsertAssetsHTTPServiceTechnologies
// HTTP 服务插入：technologies 存储并可读回；IP 存在时域名和端口写入 IP 资产
// =====================================================================
func TestInsertAssetsHTTPServiceTechnologies(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	ip := fixtureIP(t, "203.0.113")
	root := tag + "-techtest-test.com"
	host := "tech." + root
	url := "https://" + host

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `url IN (?,?) OR domain IN (?,?) OR ip=?`, url, url+"/", host, root, ip)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			map[string]any{
				"type":         "service",
				"url":          url,
				"service_ip":   ip,
				"technologies": []string{"Nginx", "Vue.js", "Cloudflare"},
				"status_code":  200,
				"page_title":   "Tech Test Site",
			},
		},
		"task_id": 888,
	})

	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}

	// technologies should be stored（TiDB：JSON 数组文本，JSON_LENGTH 取代 array_length）
	var techCnt int
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT JSON_LENGTH(technologies) FROM assets WHERE url=?`, url).Scan(&techCnt)
	})
	if techCnt != 3 {
		t.Errorf("technologies: want 3, got %d", techCnt)
	}

	// QueryByType should return technologies correctly (verifies JSON array scan)
	assets, err := d.Assets().QueryByType("service", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found *db.Asset
	for _, a := range assets {
		if a.URL == url {
			found = a
			break
		}
	}
	if found == nil {
		t.Fatal("service not found via QueryByType")
	}
	if len(found.Technologies) != 3 {
		t.Errorf("QueryByType: technologies roundtrip failed, got %v", found.Technologies)
	}

	// side effect: IP asset should exist with bound_domains containing the service domain
	var ipID int64
	var bdRaw []byte
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT id, bound_domains FROM assets WHERE type='ip' AND ip=?`, ip).Scan(&ipID, &bdRaw)
	})
	if ipID == 0 {
		t.Error("side-effect: IP asset not created for HTTP service IP")
	}
	var bd []string
	json.Unmarshal(bdRaw, &bd)
	hasDomain := false
	for _, dom := range bd {
		if dom == host {
			hasDomain = true
		}
	}
	if !hasDomain {
		t.Errorf("side-effect: IP bound_domains missing service domain, got %v", bd)
	}

	// side effect: IP open_ports should contain port 443
	var portCnt int
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT JSON_LENGTH(open_ports) FROM assets WHERE type='ip' AND ip=?`, ip).Scan(&portCnt)
	})
	if portCnt == 0 {
		t.Error("side-effect: IP open_ports not set for HTTP service")
	}
}

// =====================================================================
// TestInsertAssetsOtherService
// 非 HTTP 服务：c_segment 自动生成，IP 资产含 open_ports 和 bound_domains
// =====================================================================
func TestInsertAssetsOtherService(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	// /24 网段位固定，主机位随机：c_segment 断言仍按同一网段回读。
	ip := fixtureIP(t, "192.0.2")
	cSegment := "192.0.2.0/24"
	root := tag + "-othersvc-test.com"
	domain := "db." + root

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `ip=? OR domain IN (?,?)`, ip, domain, root)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			map[string]any{
				"type":         "service",
				"ip":           ip,
				"domain":       domain,
				"port":         3306,
				"service_name": "mysql",
			},
		},
	})

	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}

	// c_segment should be auto-set on the service（TiDB：mediumtext 列，无需 ::text 转换）
	var cseg *string
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT c_segment FROM assets WHERE type='service' AND ip=?`, ip).Scan(&cseg)
	})
	if cseg == nil || *cseg != cSegment {
		t.Errorf("c_segment: want %s, got %v", cSegment, cseg)
	}

	// IP side-effect: port 3306 in open_ports
	var portCnt int
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT JSON_LENGTH(open_ports) FROM assets WHERE type='ip' AND ip=?`, ip).Scan(&portCnt)
	})
	if portCnt == 0 {
		t.Error("side-effect: IP open_ports should contain port 3306")
	}

	// IP side-effect: bound_domains contains the service domain
	var bdRaw []byte
	mustQuery(t, func() error {
		return d.QueryRow(`SELECT bound_domains FROM assets WHERE type='ip' AND ip=?`, ip).Scan(&bdRaw)
	})
	var bd []string
	json.Unmarshal(bdRaw, &bd)
	hasDomain := false
	for _, dom := range bd {
		if dom == domain {
			hasDomain = true
		}
	}
	if !hasDomain {
		t.Errorf("side-effect: IP bound_domains missing service domain, got %v", bd)
	}
}

// =====================================================================
// TestInsertAssetsMixedBatch
// 混合批量插入：一次调用插入多种类型
// =====================================================================
func TestInsertAssetsMixedBatch(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	ip := fixtureIP(t, "192.0.2")
	root := tag + "-batch-test.org"
	sub := "batch-sub." + root
	apiURL := "https://" + root + "/api"
	usersURL := apiURL + "/users"

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `domain IN (?,?) OR ip=? OR url IN (?,?)`, root, sub, ip, apiURL, usersURL)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			// root_domain
			map[string]any{"type": "root_domain", "domain": root},
			// subdomain with A record
			map[string]any{"type": "subdomain", "domain": sub, "record_type": "A", "record_value": []string{ip}},
			// HTTP service
			map[string]any{"type": "service", "url": apiURL, "technologies": []string{"Go", "PostgreSQL"}, "status_code": 200},
			// endpoint
			map[string]any{"type": "endpoint", "url": usersURL, "method": "GET"},
		},
		"task_id": 777,
	})

	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	results, _ := out["results"].([]any)
	if len(results) != 4 {
		t.Errorf("mixed batch: want 4 results, got %d", len(results))
	}

	// verify all types exist in DB
	types := []string{"root_domain", "subdomain", "service", "endpoint"}
	for _, typ := range types {
		var cnt int
		switch typ {
		case "root_domain":
			mustQuery(t, func() error {
				return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type=? AND domain=?`, typ, root).Scan(&cnt)
			})
		case "subdomain":
			mustQuery(t, func() error {
				return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type=? AND domain=?`, typ, sub).Scan(&cnt)
			})
		case "service":
			mustQuery(t, func() error {
				return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type=? AND url=?`, typ, apiURL).Scan(&cnt)
			})
		case "endpoint":
			mustQuery(t, func() error {
				return d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type=? AND url=?`, typ, usersURL).Scan(&cnt)
			})
		}
		if cnt != 1 {
			t.Errorf("mixed batch: %s not found in DB", typ)
		}
	}
}

// =====================================================================
// TestInsertAssetsDedup
// 幂等写入：同一资产插入两次，返回相同 ID
// =====================================================================
func TestInsertAssetsDedup(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	root := tag + "-deduptest.net"

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `domain=?`, root)

	payload := map[string]any{
		"assets": []any{
			map[string]any{"type": "root_domain", "domain": root},
		},
	}

	out1 := callInsertAssets(t, ts, payload)
	out2 := callInsertAssets(t, ts, payload)

	getID := func(out map[string]any) float64 {
		results, _ := out["results"].([]any)
		if len(results) == 0 {
			return 0
		}
		m, _ := results[0].(map[string]any)
		id, _ := m["id"].(float64)
		return id
	}

	id1, id2 := getID(out1), getID(out2)
	if id1 == 0 || id1 != id2 {
		t.Errorf("dedup: want same ID on double insert, got %v vs %v", id1, id2)
	}
}

// =====================================================================
// TestInsertAssetsRejectsHostnameIPPerItem
// 一批里混入 ip 填了主机名的一条 → 只有那条失败，其余照常入库，
// 且错误里带得上 index 和改正方法，Agent 下一轮能自己修好。
// =====================================================================
func TestInsertAssetsRejectsHostnameIPPerItem(t *testing.T) {
	d := testDB(t)

	tag := runTag(t)
	root := tag + "-badip-test.com"
	hostnameIP := "cdn." + root
	ip := fixtureIP(t, "198.51.100")

	ts := NewToolSet(nil, "")
	ts.SetAssetStore(d.Assets(), d.Companies())
	cleanupAssets(t, d, `ip IN (?,?) OR domain=?`, hostnameIP, ip, root)

	out := callInsertAssets(t, ts, map[string]any{
		"assets": []any{
			map[string]any{"type": "root_domain", "domain": root},
			map[string]any{"type": "ip", "ip": hostnameIP},
			map[string]any{"type": "ip", "ip": ip},
		},
	})

	// The two valid entries must survive the bad one — a whole-batch failure
	// would make the agent re-send assets that were already fine.
	results, _ := out["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("results=%v, want the 2 valid assets", out["results"])
	}

	errsRaw, _ := out["errors"].([]any)
	if len(errsRaw) != 1 {
		t.Fatalf("errors=%v, want exactly the invalid entry", out["errors"])
	}
	entry, _ := errsRaw[0].(map[string]any)
	if index, _ := entry["index"].(float64); int(index) != 1 {
		t.Fatalf("error index=%v, want 1", entry["index"])
	}
	message, _ := entry["error"].(string)
	for _, want := range []string{hostnameIP, "type=subdomain", "A/AAAA"} {
		if !strings.Contains(message, want) {
			t.Fatalf("error message %q lacks %q — agent cannot act on it", message, want)
		}
	}

	// The rejected value must not have reached the table.
	var stored int
	if err := d.QueryRow(`SELECT count(*) FROM assets WHERE ip=?`, hostnameIP).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 0 {
		t.Fatalf("rejected hostname still stored in assets.ip (%d rows)", stored)
	}
}
