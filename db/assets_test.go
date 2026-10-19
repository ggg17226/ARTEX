package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

// testSetup opens a TiDB connection and returns both stores. Skips without ARTEX_TIDB_DSN.
func testSetup(t *testing.T) (*DB, *AssetStore, *CompanyStore) {
	t.Helper()
	d, err := Open(testDSN(t))
	if err != nil {
		t.Skipf("tidb unavailable (%v)", err)
	}
	return d, d.Assets(), d.Companies()
}

// deleteAsset removes one v2 asset by id.
func deleteAsset(d *DB, id int64) {
	d.Exec(`DELETE FROM assets WHERE id = ?`, id)
}

// deleteAssets removes a batch of assets by id（TiDB 没有 `id = ANY($1::bigint[])`）。
func deleteAssets(d *DB, ids []int64) {
	if len(ids) == 0 {
		return
	}
	ph := make([]string, len(ids))
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args = append(args, id)
	}
	d.Exec("DELETE FROM assets WHERE id IN ("+strings.Join(ph, ",")+")", args...) //nolint:errcheck
}

// assetRawInsert 以 TiDB 方言直接写入一行受控资产并回读自增 PK。
// 表上 NOT NULL 且无 DEFAULT 的 JSON 文本列必须是合法 JSON；TiDB 没有 RETURNING，
// 自增 PK 通过 LastInsertId 取得（与生产写路径同一口径）。
func assetRawInsert(t *testing.T, d *DB, cols map[string]any) int64 {
	t.Helper()
	values := map[string]any{
		"task_ids":       "[]",
		"bound_domains":  "[]",
		"open_ports":     "[]",
		"record_value":   "[]",
		"technologies":   "[]",
		"auth":           "[]",
		"params":         "[]",
		"extra":          "{}",
		"company_source": "scope",
	}
	for k, v := range cols {
		values[k] = v
	}
	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)
	args := make([]any, 0, len(names))
	for _, k := range names {
		args = append(args, values[k])
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")
	res, err := d.Exec("INSERT INTO assets("+strings.Join(names, ",")+") VALUES ("+ph+")", args...)
	if err != nil {
		t.Fatalf("raw insert: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id < 1 {
		t.Fatalf("raw insert returned pk %d", id)
	}
	return id
}

// =====================================================================
// RootDomain
// =====================================================================

func TestUpsertRootDomain(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "roottest.io", ICP: "A12345", TaskID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id1)
	if id1 == 0 {
		t.Fatal("expected non-zero id")
	}

	// upsert again — same id (dedup), ICP preserved
	id2, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "roottest.io", TaskID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("dedup failed: %d != %d", id2, id1)
	}

	// task_ids should now contain both 1 and 2
	var taskIDs []byte
	d.QueryRow(`SELECT task_ids FROM assets WHERE id = ?`, id1).Scan(&taskIDs)

	// ICP should still be set (COALESCE keeps existing)
	var icp *string
	d.QueryRow(`SELECT icp FROM assets WHERE id = ?`, id1).Scan(&icp)
	if icp == nil || *icp != "A12345" {
		t.Errorf("ICP not preserved: %v", icp)
	}
}

func TestUpsertRootDomainEmpty(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	_, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: ""})
	if err == nil {
		t.Error("expected error for empty domain")
	}
}

// =====================================================================
// IP
// =====================================================================

func TestUpsertIP(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertIP(UpsertIPReq{
		IP:        "192.168.10.5",
		OpenPorts: []PortService{{Port: 22, Service: "ssh"}, {Port: 80, Service: "http"}},
		TaskID:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id1)

	// c_segment should be 192.168.10.0/24
	var cseg *string
	d.QueryRow(`SELECT c_segment FROM assets WHERE id = ?`, id1).Scan(&cseg)
	if cseg == nil || *cseg != "192.168.10.0/24" {
		t.Errorf("c_segment: want 192.168.10.0/24, got %v", cseg)
	}

	// append another port via UpsertIP (merge)
	id2, err := av2.UpsertIP(UpsertIPReq{
		IP:        "192.168.10.5",
		OpenPorts: []PortService{{Port: 443, Service: "https"}},
		TaskID:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("dedup failed: %d != %d", id2, id1)
	}

	// verify all 3 ports present
	var cnt int
	d.QueryRow(`SELECT JSON_LENGTH(open_ports) FROM assets WHERE id = ?`, id1).Scan(&cnt)
	if cnt != 3 {
		t.Errorf("open_ports merge: want 3 ports, got %d", cnt)
	}
}

func TestUpsertIPBoundDomains(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id, err := av2.UpsertIP(UpsertIPReq{
		IP:           "10.1.2.3",
		BoundDomains: []string{"a.example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	// append a second domain
	id2, err := av2.UpsertIP(UpsertIPReq{
		IP:           "10.1.2.3",
		BoundDomains: []string{"b.example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Errorf("dedup failed: %d != %d", id2, id)
	}

	var cnt int
	d.QueryRow(`SELECT JSON_LENGTH(bound_domains) FROM assets WHERE id = ?`, id).Scan(&cnt)
	if cnt != 2 {
		t.Errorf("bound_domains merge: want 2, got %d", cnt)
	}
}

func TestAppendIPPort(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id, err := av2.UpsertIP(UpsertIPReq{IP: "10.9.8.7"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	if err := av2.AppendIPPort("10.9.8.7", 3306, "mysql"); err != nil {
		t.Fatal(err)
	}
	if err := av2.AppendIPPort("10.9.8.7", 22, "ssh"); err != nil {
		t.Fatal(err)
	}

	var cnt int
	d.QueryRow(`SELECT JSON_LENGTH(open_ports) FROM assets WHERE id = ?`, id).Scan(&cnt)
	if cnt != 2 {
		t.Errorf("AppendIPPort: want 2 ports, got %d", cnt)
	}
}

// =====================================================================
// Subdomain
// =====================================================================

func TestUpsertSubdomain(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id, err := av2.UpsertSubdomain(UpsertSubdomainReq{
		Domain:      "sub.subdtest.com",
		RecordType:  "A",
		RecordValue: []string{"1.2.3.4"},
		ICP:         "B99",
		TaskID:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE domain IN ('sub.subdtest.com', 'subdtest.com') OR ip = '1.2.3.4'`)

	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	// verify root_domain auto-populated
	var rootDomain string
	d.QueryRow(`SELECT COALESCE(root_domain,'') FROM assets WHERE id = ?`, id).Scan(&rootDomain)
	if rootDomain != "subdtest.com" {
		t.Errorf("root_domain: want subdtest.com, got %q", rootDomain)
	}

	// side effect: root domain asset should exist
	var rootCnt int
	d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type = 'root_domain' AND domain = 'subdtest.com'`).Scan(&rootCnt)
	if rootCnt != 1 {
		t.Error("side-effect root domain not created")
	}

	// side effect: IP asset with bound_domain
	var ipCnt int
	d.QueryRow(`SELECT COUNT(*) FROM assets WHERE type = 'ip' AND ip = '1.2.3.4'`).Scan(&ipCnt)
	if ipCnt != 1 {
		t.Error("side-effect IP asset not created")
	}
}

func TestUpsertSubdomainDedup(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertSubdomain(UpsertSubdomainReq{Domain: "dedup.subdtest2.net", RecordType: "A", RecordValue: []string{"5.5.5.5"}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE domain IN ('dedup.subdtest2.net', 'subdtest2.net') OR ip = '5.5.5.5'`)

	id2, err := av2.UpsertSubdomain(UpsertSubdomainReq{Domain: "dedup.subdtest2.net", RecordType: "A", RecordValue: []string{"5.5.5.5"}})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("subdomain dedup failed: %d vs %d", id2, id1)
	}
}

// =====================================================================
// App
// =====================================================================

func TestUpsertAppByBundle(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertApp(UpsertAppReq{Name: "MyApp", BundleID: "com.example.myapp", Category: "tools"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id1)

	// dedup by bundle
	id2, err := av2.UpsertApp(UpsertAppReq{Name: "MyApp Updated", BundleID: "com.example.myapp"})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("app bundle dedup failed: %d vs %d", id2, id1)
	}
}

func TestUpsertAppByName(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertApp(UpsertAppReq{Name: "UniqueAppNoBundle", Category: "utility"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id1)

	id2, err := av2.UpsertApp(UpsertAppReq{Name: "UniqueAppNoBundle"})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("app name dedup failed: %d vs %d", id2, id1)
	}
}

// =====================================================================
// HTTPService
// =====================================================================

func TestUpsertHTTPService(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	sc := 200
	cl := int64(1024)
	id1, err := av2.UpsertHTTPService(UpsertHTTPServiceReq{
		URL:           "https://www.httptest.example.com/",
		Technologies:  []string{"nginx", "vue"},
		StatusCode:    &sc,
		ContentLength: &cl,
		PageTitle:     "Test Site",
		TaskID:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE url = 'https://www.httptest.example.com' OR domain = 'www.httptest.example.com' OR domain = 'httptest.example.com'`)

	if id1 == 0 {
		t.Fatal("expected non-zero id")
	}

	// verify technology was stored
	var techCnt int
	d.QueryRow(`SELECT JSON_LENGTH(technologies) FROM assets WHERE id = ?`, id1).Scan(&techCnt)
	if techCnt != 2 {
		t.Errorf("technologies: want 2, got %d", techCnt)
	}

	// upsert again with additional tech → should merge (append)
	id2, err := av2.UpsertHTTPService(UpsertHTTPServiceReq{
		URL:          "https://www.httptest.example.com/",
		Technologies: []string{"react", "webpack"},
		TaskID:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("http service dedup failed: %d vs %d", id2, id1)
	}

	d.QueryRow(`SELECT JSON_LENGTH(technologies) FROM assets WHERE id = ?`, id1).Scan(&techCnt)
	if techCnt != 4 {
		t.Errorf("technologies merge: want 4, got %d", techCnt)
	}
}

func TestUpsertHTTPServiceAuthAppend(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	auth1 := []map[string]any{{"type": "basic", "username": "admin", "password": "pass"}}
	id1, err := av2.UpsertHTTPService(UpsertHTTPServiceReq{
		URL:    "http://authtest.example.org/",
		Auth:   auth1,
		TaskID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE url = 'http://authtest.example.org' OR domain IN ('authtest.example.org', 'example.org')`)

	auth2 := []map[string]any{{"type": "bearer", "token": "tok123"}}
	id2, err := av2.UpsertHTTPService(UpsertHTTPServiceReq{
		URL:    "http://authtest.example.org/",
		Auth:   auth2,
		TaskID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("dedup failed: %d vs %d", id2, id1)
	}

	// auth should have 2 items now
	var authCnt int
	d.QueryRow(`SELECT JSON_LENGTH(auth) FROM assets WHERE id = ?`, id1).Scan(&authCnt)
	if authCnt != 2 {
		t.Errorf("auth append: want 2, got %d", authCnt)
	}
}

// =====================================================================
// OtherService
// =====================================================================

func TestUpsertOtherService(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id1, err := av2.UpsertOtherService(UpsertOtherServiceReq{
		IP:          "172.16.0.1",
		Port:        22,
		ServiceName: "ssh",
		TaskID:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE (type = 'service' AND service_name = 'ssh' AND ip = '172.16.0.1') OR (type = 'ip' AND ip = '172.16.0.1')`)

	if id1 == 0 {
		t.Fatal("expected non-zero id")
	}

	// dedup
	id2, err := av2.UpsertOtherService(UpsertOtherServiceReq{
		IP:          "172.16.0.1",
		Port:        22,
		ServiceName: "ssh",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("other service dedup failed: %d vs %d", id2, id1)
	}

	// side-effect: IP asset should have port 22 in open_ports
	var cnt int
	d.QueryRow(`SELECT JSON_LENGTH(open_ports) FROM assets WHERE type='ip' AND ip='172.16.0.1'`).Scan(&cnt)
	if cnt == 0 {
		t.Error("side-effect: IP open_ports not populated")
	}
}

func TestUpsertOtherServiceAuthAppend(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	auth1 := []map[string]any{{"username": "admin", "password": "secret"}}
	id1, err := av2.UpsertOtherService(UpsertOtherServiceReq{
		IP:          "10.5.5.5",
		Port:        3306,
		ServiceName: "mysql",
		Auth:        auth1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE (type = 'service' AND ip = '10.5.5.5') OR (type = 'ip' AND ip = '10.5.5.5')`)

	auth2 := []map[string]any{{"username": "root", "password": "root"}}
	id2, err := av2.UpsertOtherService(UpsertOtherServiceReq{
		IP:          "10.5.5.5",
		Port:        3306,
		ServiceName: "mysql",
		Auth:        auth2,
	})
	if err != nil || id2 != id1 {
		t.Fatalf("dedup or error: %v, ids %d vs %d", err, id2, id1)
	}

	var authCnt int
	d.QueryRow(`SELECT JSON_LENGTH(auth) FROM assets WHERE id = ?`, id1).Scan(&authCnt)
	if authCnt != 2 {
		t.Errorf("auth append: want 2, got %d", authCnt)
	}
}

// =====================================================================
// Endpoint
// =====================================================================

func TestUpsertEndpoint(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	params1 := []map[string]any{{"location": "query", "name": "id", "value": "1"}}
	id1, err := av2.UpsertEndpoint(UpsertEndpointReq{
		URL:    "https://api.eptest.com/users?id=1",
		Method: "GET",
		Params: params1,
		TaskID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM assets WHERE url LIKE '%eptest.com%' OR domain IN ('api.eptest.com', 'eptest.com')`)

	if id1 == 0 {
		t.Fatal("expected non-zero id")
	}

	// dedup (same URL + method = same endpoint)
	id2, err := av2.UpsertEndpoint(UpsertEndpointReq{
		URL:    "https://api.eptest.com/users?id=1",
		Method: "GET",
		Params: []map[string]any{{"location": "query", "name": "filter", "value": "active"}},
		TaskID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Errorf("endpoint dedup failed: %d vs %d", id2, id1)
	}

	// params should be merged (2 distinct params)
	var paramCnt int
	d.QueryRow(`SELECT JSON_LENGTH(params) FROM assets WHERE id = ?`, id1).Scan(&paramCnt)
	if paramCnt != 2 {
		t.Errorf("params merge: want 2, got %d", paramCnt)
	}
}

func TestUpsertEndpointRequiredFields(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	_, err := av2.UpsertEndpoint(UpsertEndpointReq{URL: "", Method: "GET"})
	if err == nil {
		t.Error("expected error for empty URL")
	}

	_, err = av2.UpsertEndpoint(UpsertEndpointReq{URL: "https://example.com/", Method: ""})
	if err == nil {
		t.Error("expected error for empty method")
	}
}

// =====================================================================
// Query helpers
// =====================================================================

func TestQueryByType(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "querytest.example.net"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	assets, err := av2.QueryByType("root_domain", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range assets {
		if a.ID == id {
			found = true
		}
	}
	if !found {
		t.Error("QueryByType: inserted asset not found")
	}
}

// TestDeleteByTaskID: 独有资产被删,与其他任务共享的资产仅解除关联(保留),host 反查正确。
func TestDeleteByTaskID(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	const taskA = int64(90001)
	const taskB = int64(90002)
	// solo:仅属 taskA
	solo, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "solo-del.test", TaskID: taskA})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, solo)
	// shared:先 taskA 再 taskB → task_ids={A,B}
	shared, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "shared-del.test", TaskID: taskA})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, shared)
	if _, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "shared-del.test", TaskID: taskB}); err != nil {
		t.Fatal(err)
	}

	// host 反查(删资产前):应含两个域名
	hosts, err := av2.HostsByTask(taskA)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(hosts, "solo-del.test") || !slices.Contains(hosts, "shared-del.test") {
		t.Fatalf("HostsByTask 缺 host: %v", hosts)
	}

	n, err := av2.DeleteByTaskID(taskA)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("DeleteByTaskID: 应删 1 个独有资产,实删 %d", n)
	}
	// solo 已删
	if a, _ := av2.GetByIDs([]int64{solo}); len(a) != 0 {
		t.Fatalf("solo 资产应被删除")
	}
	// shared 保留,且 task_ids 只剩 taskB
	sa, _ := av2.GetByIDs([]int64{shared})
	if len(sa) != 1 {
		t.Fatalf("shared 资产应保留")
	}
	if slices.Contains(sa[0].TaskIDs, taskA) || !slices.Contains(sa[0].TaskIDs, taskB) {
		t.Fatalf("shared task_ids 应解除 A 保留 B,得 %v", sa[0].TaskIDs)
	}
}

func TestQueryByTask(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	const taskID = int64(99999)
	id, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "taskquery.net", TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	assets, err := av2.QueryByTask(taskID, "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range assets {
		if a.ID == id {
			found = true
		}
	}
	if !found {
		t.Error("QueryByTask: inserted asset not found")
	}

	n, err := av2.CountByTask(taskID, "")
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Errorf("CountByTask: got %d, want >= 1", n)
	}
	if counts, err := av2.CountsByTypeForTask(taskID); err != nil {
		t.Fatal(err)
	} else if counts["root_domain"] < 1 {
		t.Errorf("CountsByTypeForTask: root_domain = %d, want >= 1", counts["root_domain"])
	}

	// offset past the end must return nothing, not fall back to page 1
	rest, err := av2.QueryByTask(taskID, "", 10, n)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Errorf("QueryByTask offset=%d: got %d rows, want 0", n, len(rest))
	}
}

// 任务资产列表按页取,不再被固定条数截断:60 条资产用 25/页要能完整翻出来。
func TestQueryByTaskPaging(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	const taskID = int64(99998)
	const n = 60
	for i := 0; i < n; i++ {
		id, err := av2.UpsertRootDomain(UpsertRootDomainReq{
			Domain: fmt.Sprintf("paging-%02d.querybytask.test", i),
			TaskID: taskID,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer deleteAsset(d, id)
	}

	total, err := av2.CountByTask(taskID, "root_domain")
	if err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Fatalf("CountByTask: got %d, want %d", total, n)
	}

	const size = 25
	seen := map[int64]bool{}
	for offset := 0; offset < total; offset += size {
		page, err := av2.QueryByTask(taskID, "root_domain", size, offset)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range page {
			if seen[a.ID] {
				t.Errorf("offset=%d: asset %d returned twice", offset, a.ID)
			}
			seen[a.ID] = true
		}
	}
	if len(seen) != n {
		t.Errorf("paged through %d assets, want %d", len(seen), n)
	}
}

func TestQueryByCompany(t *testing.T) {
	d, av2, cs := testSetup(t)
	defer d.Close()

	companyID, _, err := cs.UpsertCompany("QueryByCompanyCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, companyID)

	cs.AddScope(companyID, []string{"qbc-test.io"}, "test")

	id, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "qbc-test.io"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	if err := cs.RecomputeAttribution(); err != nil {
		t.Fatal(err)
	}

	assets, err := av2.QueryByCompany(companyID, "root_domain", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range assets {
		if a.ID == id {
			found = true
		}
	}
	if !found {
		t.Error("QueryByCompany: attributed asset not found")
	}

	n, err := av2.CountByCompany(companyID, "root_domain")
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Errorf("CountByCompany: got %d, want >= 1", n)
	}

	// offset past the end must return nothing, not fall back to page 1
	rest, err := av2.QueryByCompany(companyID, "root_domain", 10, n)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Errorf("QueryByCompany offset=%d: got %d rows, want 0", n, len(rest))
	}
}

// 企业资产列表同样按页取,不被固定条数截断。
func TestQueryByCompanyPaging(t *testing.T) {
	d, av2, cs := testSetup(t)
	defer d.Close()

	companyID, _, err := cs.UpsertCompany("QueryByCompanyPagingCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, companyID)

	if added, _, _, errs := cs.AddScope(companyID, []string{"qbc-paging.io"}, "test"); added != 1 {
		t.Fatalf("AddScope: added=%d, errors=%v", added, errs)
	}

	// UpsertSubdomain 会顺带建根域名资产,一并清掉
	defer d.Exec(`DELETE FROM assets WHERE root_domain = 'qbc-paging.io'`)

	const n = 60
	for i := 0; i < n; i++ {
		if _, err := av2.UpsertSubdomain(UpsertSubdomainReq{
			Domain: fmt.Sprintf("paging-%02d.qbc-paging.io", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	total, err := av2.CountByCompany(companyID, "subdomain")
	if err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Fatalf("CountByCompany: got %d, want %d", total, n)
	}

	const size = 25
	seen := map[int64]bool{}
	for offset := 0; offset < total; offset += size {
		page, err := av2.QueryByCompany(companyID, "subdomain", size, offset)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range page {
			if seen[a.ID] {
				t.Errorf("offset=%d: asset %d returned twice", offset, a.ID)
			}
			seen[a.ID] = true
		}
	}
	if len(seen) != n {
		t.Errorf("paged through %d assets, want %d", len(seen), n)
	}
}

func TestCountsByType(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	id, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "counttest.example.biz"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, id)

	counts, err := av2.CountsByType()
	if err != nil {
		t.Fatal(err)
	}
	if counts["root_domain"] == 0 {
		t.Error("CountsByType: root_domain count should be > 0")
	}
}

// =====================================================================
// Company attribution at insert time
// =====================================================================

func TestCompanyAttributionAtInsertTime(t *testing.T) {
	d, av2, cs := testSetup(t)
	defer d.Close()

	companyID, _, err := cs.UpsertCompany("InsertAttrCorp", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCompany(d, companyID)

	cs.AddScope(companyID, []string{"insertattr.com"}, "test")

	// Now insert a root domain — should be auto-attributed
	assetID, err := av2.UpsertRootDomain(UpsertRootDomainReq{Domain: "insertattr.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer deleteAsset(d, assetID)

	var cid *int64
	d.QueryRow(`SELECT company_id FROM assets WHERE id = ?`, assetID).Scan(&cid)
	if cid == nil || *cid != companyID {
		t.Errorf("attribution at insert time: want company %d, got %v", companyID, cid)
	}
}

// =====================================================================
// calcCSegment
// =====================================================================

func TestCalcCSegment(t *testing.T) {
	tests := []struct {
		ip   string
		want string
	}{
		{"192.168.1.5", "192.168.1.0/24"},
		{"10.0.0.255", "10.0.0.0/24"},
		{"", ""},
		{"notanip", ""},
	}
	for _, tc := range tests {
		got := calcCSegment(tc.ip)
		if got != tc.want {
			t.Errorf("calcCSegment(%q): want %q, got %q", tc.ip, tc.want, got)
		}
	}
}

// =====================================================================
// JSON 编码与 Go 内存数组合并（取代原 PG 侧 array_to_json / || 去重）
// =====================================================================

// TiDB 侧数组列是 JSON 文本：编码必须是**合法 JSON**（不再是 PG 的 {"a","b"} 字面量）。
func TestMarshalStringArray(t *testing.T) {
	got := marshalStringArray([]string{"a", "b", "c"})
	if got != `["a","b","c"]` {
		t.Errorf("unexpected: %q", got)
	}
	got2 := marshalStringArray(nil)
	if got2 != "[]" {
		t.Errorf("empty: %q", got2)
	}
	if got3 := marshalStringArray([]string{`a"b`, `c\d`}); got3 != `["a\"b","c\\d"]` {
		t.Errorf("escaping: %q", got3)
	}
	if got4 := marshalTaskIDs([]int64{7, 11}); got4 != "[7,11]" {
		t.Errorf("task ids must stay numeric JSON: %q", got4)
	}
}

// open_ports 合并规则（spec §3.3.1）：同端口只保留一条，service 非空优先。
func TestAssetMergeOpenPortsServiceWins(t *testing.T) {
	merged, err := assetMergeObjects(`[{"port":22,"service":"ssh"},{"port":80,"service":""}]`,
		[]map[string]any{{"port": 22, "service": "ignored"}, {"port": 80, "service": "http"}, {"port": 443, "service": "https"}},
		assetPortKey, assetPreferService)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var got []map[string]any
	if err := json.Unmarshal([]byte(merged), &got); err != nil {
		t.Fatalf("merged must be legal JSON: %v (%s)", err, merged)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 ports, got %d: %s", len(got), merged)
	}
	if fmt.Sprintf("%v", got[0]["service"]) != "ssh" {
		t.Errorf("已有 service 不能被覆盖: %s", merged)
	}
	if fmt.Sprintf("%v", got[1]["service"]) != "http" {
		t.Errorf("空 service 必须被新值补上: %s", merged)
	}
	if fmt.Sprintf("%v", got[2]["port"]) != "443" {
		t.Errorf("新端口必须追加: %s", merged)
	}
}

// params 合并规则：同 (location, name) 视为同一引用，元素整体 JSON 文本 DESC 者优先。
func TestAssetMergeParamsDescWins(t *testing.T) {
	merged, err := assetMergeObjects(`[{"location":"query","name":"id","value":"a"}]`,
		[]map[string]any{{"location": "query", "name": "id", "value": "b"}, {"location": "body", "name": "id"}},
		assetParamKey, func(cur, next map[string]any) bool {
			return assetJSONText(next) > assetJSONText(cur)
		})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var got []map[string]any
	if err := json.Unmarshal([]byte(merged), &got); err != nil {
		t.Fatalf("merged must be legal JSON: %v (%s)", err, merged)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 params, got %d: %s", len(got), merged)
	}
	if fmt.Sprintf("%v", got[0]["value"]) != "b" {
		t.Errorf("DESC 较大的元素必须胜出: %s", merged)
	}
}

// task_ids 合并必须保持数值 JSON 且按值去重（原 PG 侧 `|| ARRAY[...]` 去重的等价实现）。
func TestAssetUnionTaskIDsDedupes(t *testing.T) {
	got := assetUnionTaskIDs(`[7,11]`, []int64{11, 13})
	if got != "[7,11,13]" {
		t.Errorf("want [7,11,13], got %s", got)
	}
	if got := assetUnionStrings(`["x","y"]`, []string{"y", "z"}); got != `["x","y","z"]` {
		t.Errorf("strings union: %s", got)
	}
}

// =====================================================================
// normalizeURL
// =====================================================================

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"HTTPS://Example.COM/path", "https://example.com/path"},
		{"http://example.com/", "http://example.com"},
		{"http://example.com/page", "http://example.com/page"},
	}
	for _, tc := range tests {
		got := normalizeURL(tc.raw)
		if got != tc.want {
			t.Errorf("normalizeURL(%q): want %q, got %q", tc.raw, tc.want, got)
		}
	}
}

func TestAssetPaginationUsesStableIDTieBreaker(t *testing.T) {
	d, assets, _ := testSetup(t)
	defer d.Close()

	stamp := time.Now().UnixNano()
	taskID := stamp
	marker := fmt.Sprintf("stable-page-%d", stamp)
	sharedSeen := time.Date(2026, time.August, 21, 9, 0, 0, 0, time.UTC)
	ids := make([]int64, 0, 5)
	for i := 0; i < 5; i++ {
		domain := fmt.Sprintf("%s-%d.invalid", marker, i)
		id := assetRawInsert(t, d, map[string]any{
			"type":        "root_domain",
			"domain":      domain,
			"root_domain": domain,
			"task_ids":    fmt.Sprintf("[%d]", taskID),
			"last_seen":   sharedSeen,
		})
		ids = append(ids, id)
	}
	defer func() { deleteAssets(d, ids) }()

	want := slices.Clone(ids)
	slices.Reverse(want)
	collect := func(query func(limit, offset int) ([]*Asset, error)) []int64 {
		t.Helper()
		var got []int64
		for offset := 0; offset < len(ids); offset += 2 {
			page, err := query(2, offset)
			if err != nil {
				t.Fatal(err)
			}
			for _, asset := range page {
				got = append(got, asset.ID)
			}
		}
		return got
	}
	if got := collect(func(limit, offset int) ([]*Asset, error) {
		return assets.QueryByTask(taskID, "root_domain", limit, offset)
	}); !slices.Equal(got, want) {
		t.Fatalf("task pagination order=%v want=%v", got, want)
	}
	if got := collect(func(limit, offset int) ([]*Asset, error) {
		return assets.QueryDSL(marker, "root_domain", 0, limit, offset)
	}); !slices.Equal(got, want) {
		t.Fatalf("DSL pagination order=%v want=%v", got, want)
	}
}

// TestAssetIPRejectsHostname pins the write-side guard added after a hostname in
// assets.ip broke company attribution: every asset type that persists an ip must
// refuse a non-address, and the message must tell the caller how to fix it.
func TestAssetIPRejectsHostname(t *testing.T) {
	d, av2, _ := testSetup(t)
	defer d.Close()

	hostname := "d63cd476.cdn.ucloud.com.cn"
	cases := []struct {
		name string
		call func() (int64, error)
	}{
		{"ip", func() (int64, error) {
			return av2.UpsertIP(UpsertIPReq{IP: hostname})
		}},
		{"http service", func() (int64, error) {
			return av2.UpsertHTTPService(UpsertHTTPServiceReq{URL: "https://example.com/", IP: hostname})
		}},
		{"other service", func() (int64, error) {
			return av2.UpsertOtherService(UpsertOtherServiceReq{IP: hostname, Port: 22, ServiceName: "ssh"})
		}},
		{"endpoint", func() (int64, error) {
			return av2.UpsertEndpoint(UpsertEndpointReq{URL: "https://example.com/a", Method: "GET", IP: hostname})
		}},
	}
	for _, tc := range cases {
		id, err := tc.call()
		if !errors.Is(err, ErrAssetIPInvalid) {
			deleteAsset(d, id)
			t.Fatalf("%s: err=%v, want %v", tc.name, err, ErrAssetIPInvalid)
		}
		// The caller has to learn the remedy, not just that something was wrong.
		if !strings.Contains(err.Error(), hostname) || !strings.Contains(err.Error(), "type=subdomain") {
			t.Fatalf("%s: message is not actionable: %v", tc.name, err)
		}
	}

	// An empty ip stays legal for the types where it is optional, and a real
	// address is still accepted.
	id, err := av2.UpsertHTTPService(UpsertHTTPServiceReq{URL: "https://ip-guard.example.com/"})
	if err != nil {
		t.Fatalf("empty ip rejected: %v", err)
	}
	deleteAsset(d, id)
	id, err = av2.UpsertIP(UpsertIPReq{IP: "203.0.113.7"})
	if err != nil {
		t.Fatalf("valid ip rejected: %v", err)
	}
	deleteAsset(d, id)
}

// TestQueryDSLInScopeMembership pins the agent-facing list_assets behavior: it
// returns assets that BELONG to the task's declared scope (membership, not literal
// value) — a root_domain scope surfaces its subdomains/services/endpoints even when
// those rows were produced by a different task — and honors ip/cidr scope for
// IP-literal hosts (the try_inet(domain) arm). Out-of-scope assets, including by id,
// are excluded. Direct source tasks' scope is included.
func TestQueryDSLInScopeMembership(t *testing.T) {
	d, assets, _ := testSetup(t)
	defer d.Close()

	task, err := d.CreateTask("scope membership", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	src, err := d.CreateTask("scope source", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.DeleteTask(task.ID); _ = d.DeleteTask(src.ID) })
	// task ← src is a direct source relation.
	if _, err := d.Exec(`INSERT INTO task_relations(task_id, source_task_id) VALUES (?, ?)`, task.ID, src.ID); err != nil {
		t.Fatal(err)
	}

	stamp := time.Now().UnixNano()
	root := fmt.Sprintf("sc%d.invalid", stamp)     // in-scope root domain (task)
	srcRoot := fmt.Sprintf("src%d.invalid", stamp) // in-scope via source task
	out := fmt.Sprintf("out%d.invalid", stamp)     // out of every scope
	marker := fmt.Sprintf("mk%d", stamp)           // bare-text token present in all rows
	foreignTask := stamp + 777                     // asset produced by an unrelated task

	// Scope: task owns root; source task owns srcRoot; task owns an IP /24.
	for _, sc := range []struct {
		tid  int64
		kind string
		dom  string
		net  string
	}{
		{task.ID, "root_domain", root, ""},
		{src.ID, "root_domain", srcRoot, ""},
		{task.ID, "ip", "", "198.51.100.0/24"},
	} {
		if err := assets.upsertTaskScope(TaskScope{TaskID: sc.tid, Kind: sc.kind, Domain: sc.dom, Net: sc.net}); err != nil {
			t.Fatalf("seed scope: %v", err)
		}
	}

	// Insert rows directly so root_domain/task_ids are controlled exactly. Every
	// in-scope row carries foreignTask in task_ids (never the current task) to prove
	// membership is by scope, not by producer.
	type row struct {
		typ, domain, rootDom, url, method, ip, title string
	}
	rows := []row{
		{"subdomain", "api." + root, root, "", "", "", marker},                        // under task root
		{"service", "www." + root, root, "https://www." + root + "/", "", "", marker}, // service under task root
		{"endpoint", "www." + root, root, "https://www." + root + "/a?" + marker + "=1", "GET", "", ""},
		{"subdomain", "dev." + srcRoot, srcRoot, "", "", "", marker},                                      // under source-task root
		{"endpoint", "198.51.100.9", "198.51.100.9", "http://198.51.100.9:8080/" + marker, "GET", "", ""}, // IP-literal host, ip col empty
		{"subdomain", "x." + out, out, "", "", "", marker},                                                // out of scope
	}
	var inScopeIDs, outIDs []int64
	nullable := func(v string) any {
		if v == "" {
			return nil
		}
		return v
	}
	for _, r := range rows {
		id := assetRawInsert(t, d, map[string]any{
			"type":        r.typ,
			"domain":      nullable(r.domain),
			"root_domain": nullable(r.rootDom),
			"url":         nullable(r.url),
			"method":      nullable(r.method),
			"ip":          nullable(r.ip),
			"page_title":  nullable(r.title),
			"task_ids":    fmt.Sprintf("[%d]", foreignTask),
			"last_seen":   time.Now().UTC(),
		})
		if r.rootDom == out {
			outIDs = append(outIDs, id)
		} else {
			inScopeIDs = append(inScopeIDs, id)
		}
	}
	t.Cleanup(func() {
		deleteAssets(d, inScopeIDs)
		deleteAssets(d, outIDs)
	})

	got, err := assets.QueryDSLInScope(marker, "", task.ID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := map[int64]bool{}
	for _, a := range got {
		gotIDs[a.ID] = true
	}
	for _, id := range inScopeIDs {
		if !gotIDs[id] {
			t.Fatalf("in-scope asset %d missing from QueryDSLInScope result %v", id, gotIDs)
		}
	}
	for _, id := range outIDs {
		if gotIDs[id] {
			t.Fatalf("out-of-scope asset %d leaked into scoped query", id)
		}
	}

	// GetByIDsInScope: in-scope id returns, out-of-scope id is dropped.
	mixed := append(append([]int64{}, inScopeIDs[0]), outIDs[0])
	byID, err := assets.GetByIDsInScope(task.ID, mixed)
	if err != nil {
		t.Fatal(err)
	}
	if len(byID) != 1 || byID[0].ID != inScopeIDs[0] {
		t.Fatalf("GetByIDsInScope mixed ids → %+v, want only %d", byID, inScopeIDs[0])
	}

	// taskID<=0 (non-task context) falls back to global: the out-of-scope row is reachable.
	global, err := assets.QueryDSLInScope(marker, "", 0, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sawOut bool
	for _, a := range global {
		if a.ID == outIDs[0] {
			sawOut = true
		}
	}
	if !sawOut {
		t.Fatalf("taskID<=0 should fall back to global and include out-of-scope asset %d", outIDs[0])
	}
}

// =============================================================================
// P08-02 Task 2.1 — 资产 8 分支写前仲裁协议测试基建（离线 mock 引擎）
// =============================================================================
//
// 本机没有 TiDB 实例时集成用例会 honest skip，因此本节的 4 个核心协议用例改由一个
// "迷你 assets 唯一索引引擎"（内存 mock driver）驱动：它按真实 DDL 的 8 个 hash 唯一
// 索引语义回应 SQL，并强制校验
//   - identity/hash 必须自洽（hash 长度 32 且 sha256(identity) == hash）；
//   - 每条 INSERT 至多激活一对 identity_*/hash_*，其余 7 对必须显式绑定 NULL；
//   - 命中已存在 hash 的 INSERT 必须以 MySQL 1062 失败（真实唯一索引行为）；
//   - 列数与占位符数必须一致（杜绝参数错位）。
//
// 这样 Path A/B 两阶段协议、写前仲裁、碰撞零写入、1062 新事务重试全部可在离线环境
// 判红/判绿；对应的 *_LiveTiDB 用例在 ARTEX_TIDB_DSN 存在时再用真实 TiDB 端到端复跑。

const (
	assetKindQuery    = "query"
	assetKindExec     = "exec"
	assetKindBegin    = "begin"
	assetKindCommit   = "commit"
	assetKindRollback = "rollback"
)

// assetBranchColumns 是 assets 表 8 个身份分支的 (identity 列, hash 列) 对。
// assetNotNullNoDefaultCols 来自真实 DDL：这些列 NOT NULL 且没有 DEFAULT，
// 任何 INSERT 都必须显式写入（JSON 文本列写成合法 JSON）。
var assetNotNullNoDefaultCols = []string{
	"type", "company_source", "task_ids", "bound_domains", "open_ports",
	"record_value", "technologies", "auth", "params", "extra",
}

var assetBranchColumns = [][2]string{
	{"identity_root_domain", "hash_root_domain"},
	{"identity_ip", "hash_ip"},
	{"identity_subdomain", "hash_subdomain"},
	{"identity_app_bundle", "hash_app_bundle"},
	{"identity_app_name", "hash_app_name"},
	{"identity_service_http", "hash_service_http"},
	{"identity_service_other", "hash_service_other"},
	{"identity_endpoint", "hash_endpoint"},
}

// assetMockCall 是驱动侧一次可观察调用（含所属事务序号，TxID=0 表示事务外直发）。
type assetMockCall struct {
	TxID  int
	Kind  string
	Query string
	Args  []driver.Value
}

func (c assetMockCall) has(frag string) bool { return strings.Contains(c.Query, frag) }
func (c assetMockCall) isQuery() bool        { return c.Kind == assetKindQuery }
func (c assetMockCall) isExec() bool         { return c.Kind == assetKindExec }
func (c assetMockCall) isInsert() bool {
	return c.isExec() && strings.Contains(c.Query, "INSERT INTO assets")
}
func (c assetMockCall) isUpdate() bool {
	return c.isExec() && strings.Contains(c.Query, "UPDATE assets")
}
func (c assetMockCall) isSelectForUpdate() bool {
	return c.isQuery() && strings.Contains(c.Query, "FOR UPDATE")
}
func (c assetMockCall) arg(i int) driver.Value {
	if i < len(c.Args) {
		return c.Args[i]
	}
	return nil
}
func (c assetMockCall) line() string {
	return fmt.Sprintf("%s tx=%d %s args=%v", c.Kind, c.TxID, strings.Join(strings.Fields(c.Query), " "), c.Args)
}

var (
	assetInsertColsRe = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+assets\s*\(([^)]*)\)`)
	assetUpdateSetRe  = regexp.MustCompile(`(?is)^\s*UPDATE\s+assets\s+SET\s+(.*?)\s+WHERE\s`)
	assetSelectColsRe = regexp.MustCompile(`(?is)^\s*SELECT\s+(.*?)\s+FROM\s+`)
	assetHashColRe    = regexp.MustCompile("(?is)hash_[a-z_]+")
	assetIdentityRe   = regexp.MustCompile("(?is)identity_[a-z_]+")
)

// assetSQLCols 把一段列清单（可能带反引号/表别名）解析成小写列名切片。
func assetSQLCols(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.ReplaceAll(p, "`", "")
		if p == "" {
			continue
		}
		out = append(out, strings.ToLower(p))
	}
	return out
}

func assetInsertColumns(query string) []string {
	m := assetInsertColsRe.FindStringSubmatch(query)
	if m == nil {
		return nil
	}
	return assetSQLCols(m[1])
}

// assetAssign 是 UPDATE SET 的一项：Param=true 表示绑定占位符，false 表示裸表达式
// （例如 last_seen = NOW(3)），后者不占用参数位。
type assetAssign struct {
	Col   string
	Param bool
}

func assetUpdateAssignments(query string) []assetAssign {
	m := assetUpdateSetRe.FindStringSubmatch(query)
	if m == nil {
		return nil
	}
	var out []assetAssign
	for _, assign := range strings.Split(m[1], ",") {
		name, val, ok := strings.Cut(assign, "=")
		if !ok {
			continue
		}
		out = append(out, assetAssign{
			Col:   strings.ToLower(strings.TrimSpace(strings.ReplaceAll(name, "`", ""))),
			Param: strings.TrimSpace(val) == "?",
		})
	}
	return out
}

func assetUpdateColumns(query string) []string {
	var cols []string
	for _, a := range assetUpdateAssignments(query) {
		cols = append(cols, a.Col)
	}
	return cols
}

func assetSelectColumns(query string) []string {
	m := assetSelectColsRe.FindStringSubmatch(query)
	if m == nil {
		return nil
	}
	return assetSQLCols(m[1])
}

// assetHashColumn 返回语句 WHERE 条件里的 hash 列名（无则空串）。
func assetHashColumn(query string) string {
	// 只看 WHERE 之后，避免把 INSERT 列清单里的 hash 列当成定位列。
	where := query
	if i := strings.Index(strings.ToUpper(query), " WHERE "); i >= 0 {
		where = query[i:]
	} else {
		return ""
	}
	m := assetHashColRe.FindString(where)
	if m == "" {
		return ""
	}
	return strings.ToLower(m)
}

// assetIdentityColumn 返回语句里的 identity 列名（无则空串）。
func assetIdentityColumn(query string) string {
	m := assetIdentityRe.FindString(query)
	if m == "" {
		return ""
	}
	return strings.ToLower(m)
}

// assetMockReply 是一条语句的应答。
type assetMockReply struct {
	cols         []string
	rows         [][]driver.Value
	lastInsertID int64
	rowsAffected int64
	err          error
}

func assetReplyNoRows(cols ...string) assetMockReply { return assetMockReply{cols: cols} }

func assetReplyRows(cols []string, rows ...[]driver.Value) assetMockReply {
	return assetMockReply{cols: cols, rows: rows, rowsAffected: int64(len(rows))}
}

func assetReplyInsert(id int64) assetMockReply {
	return assetMockReply{lastInsertID: id, rowsAffected: 1}
}

func assetReplyErr(err error) assetMockReply { return assetMockReply{err: err} }

// assetDefaultColumnValues 给出 mock 回读业务列时的默认持久值，未列出的列一律
// 让引擎显式报错，避免"默认值悄悄兜住拼错的列名"。
var assetDefaultColumnValues = map[string]driver.Value{
	"task_ids":        "[]",
	"bound_domains":   "[]",
	"open_ports":      "[]",
	"record_value":    "[]",
	"technologies":    "[]",
	"auth":            "[]",
	"params":          "[]",
	"extra":           "{}",
	"company_source":  "scope",
	"company_id":      nil,
	"icp":             nil,
	"app_icp":         nil,
	"app_name":        nil,
	"category":        nil,
	"app_description": nil,
	"c_segment":       nil,
	"ip":              nil,
	"status_code":     nil,
	"content_length":  nil,
	"page_title":      nil,
	"favicon_mmh3":    nil,
	"domain":          nil,
	"root_domain":     nil,
	"record_type":     nil,
}

// assetMockRow 是 mock 引擎里的一行 assets。
type assetMockRow struct {
	id         int64
	identities map[string][]byte       // identity 列 → canonical payload
	values     map[string]driver.Value // 非身份业务列
}

// assetMockEngine 是内存版 assets 唯一索引引擎 + 语句记录器。
type assetMockEngine struct {
	mu     sync.Mutex
	rows   map[int64]*assetMockRow
	byHash map[string]int64 // hash 列 + "\x00" + hex(hash) → 物理 PK
	nextID int64
	// corruptCol 非空时，下一次 UPDATE 之后把该列改写成外来字节（模拟身份漂移）。
	corruptCol string
	calls      []assetMockCall
	// hook 可在引擎默认分派前拦截/改写应答（用于注入 1062、碰撞等故障）。
	hook func(assetMockCall) (assetMockReply, bool)
}

func newAssetMockEngine() *assetMockEngine {
	return &assetMockEngine{
		rows:   map[int64]*assetMockRow{},
		byHash: map[string]int64{},
		nextID: 100,
	}
}

// corruptIdentityAfterUpdate 让下一次 UPDATE 落库后把指定身份列改成**外来字节**，
// 用来判定实现方是否真的做了"更新后身份漂移复核"：少了这步复核的实现会盲目提交。
func (e *assetMockEngine) corruptIdentityAfterUpdate(col string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.corruptCol = col
}

func (e *assetMockEngine) setHook(h func(assetMockCall) (assetMockReply, bool)) {
	e.mu.Lock()
	e.hook = h
	e.mu.Unlock()
}

func (e *assetMockEngine) record(c assetMockCall) {
	e.mu.Lock()
	e.calls = append(e.calls, c)
	e.mu.Unlock()
}

// resetCalls 清空语句记录（用于把"上一阶段"的轨迹从断言里剔除）。
func (e *assetMockEngine) resetCalls() {
	e.mu.Lock()
	e.calls = nil
	e.mu.Unlock()
}

// rowCount 返回 mock 表里的物理行数（"零新增行"断言用）。
func (e *assetMockEngine) rowCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.rows)
}

// hashRows 返回某 hash 列上命中指定 hash 值的物理行数（唯一索引 → 期望 ≤ 1）。
func (e *assetMockEngine) hashRows(hashCol string, hash []byte) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.byHash[assetHashKey(hashCol, hash)]; ok {
		return 1
	}
	return 0
}

func (e *assetMockEngine) snapshot() []assetMockCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]assetMockCall, len(e.calls))
	copy(out, e.calls)
	return out
}

// txCounts 返回 begin/commit/rollback 次数，用于"Tx2 != Tx1 且重试计数==1"类断言。
func (e *assetMockEngine) txCounts() (begins, commits, rollbacks int) {
	for _, c := range e.snapshot() {
		switch c.Kind {
		case assetKindBegin:
			begins++
		case assetKindCommit:
			commits++
		case assetKindRollback:
			rollbacks++
		}
	}
	return begins, commits, rollbacks
}

// callsFor 返回满足谓词的调用（顺序保持）。
func (e *assetMockEngine) callsFor(pred func(assetMockCall) bool) []assetMockCall {
	var out []assetMockCall
	for _, c := range e.snapshot() {
		if pred(c) {
			out = append(out, c)
		}
	}
	return out
}

func (e *assetMockEngine) row(id int64) *assetMockRow {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rows[id]
}

// injectHashCollision 把已存在行的 identity 改写为 foreign，但保留其 hash 列不变，
// 从而构造"hash 命中但完整 identity 字节不相等"的强制碰撞前提。
func (e *assetMockEngine) injectHashCollision(t *testing.T, pk int64, identityCol string, foreign []byte) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	row, ok := e.rows[pk]
	if !ok {
		t.Fatalf("mock asset row %d not found for collision injection", pk)
	}
	row.identities[identityCol] = foreign
}

func assetHashKey(hashCol string, hash []byte) string {
	return hashCol + "\x00" + fmt.Sprintf("%x", hash)
}

func assetBytes(v driver.Value) ([]byte, bool) {
	switch t := v.(type) {
	case nil:
		return nil, false
	case []byte:
		return t, true
	case string:
		return []byte(t), true
	default:
		return nil, false
	}
}

// handle 按真实 DDL 语义回应一条语句。
func (e *assetMockEngine) handle(c assetMockCall) assetMockReply {
	if c.Kind == assetKindBegin || c.Kind == assetKindCommit || c.Kind == assetKindRollback {
		return assetMockReply{}
	}
	e.mu.Lock()
	hook := e.hook
	e.mu.Unlock()
	if hook != nil {
		if reply, ok := hook(c); ok {
			return reply
		}
	}

	q := c.Query
	upper := strings.ToUpper(q)

	// 1) 公司归属解析：一律"无匹配企业"。
	if strings.Contains(q, "FROM company_scope") {
		return assetReplyNoRows("company_id")
	}

	// 2) INSERT：严格校验列激活 + identity/hash 自洽 + 唯一索引。
	if c.isInsert() {
		cols := assetInsertColumns(q)
		if len(cols) == 0 {
			return assetReplyErr(fmt.Errorf("mock: cannot parse INSERT columns: %s", q))
		}
		if len(cols) != len(c.Args) {
			return assetReplyErr(fmt.Errorf("mock: INSERT columns=%d args=%d mismatch: %s", len(cols), len(c.Args), q))
		}
		if strings.Contains(upper, "ON DUPLICATE KEY UPDATE") || strings.Contains(upper, "RETURNING") {
			return assetReplyErr(fmt.Errorf("mock: strict INSERT must not carry ODKU/RETURNING: %s", q))
		}
		if strings.Contains(q, "$") {
			return assetReplyErr(fmt.Errorf("mock: PostgreSQL placeholder leaked into %s", q))
		}
		// 8 对 identity/hash 必须**全部出现在列清单**里（未激活的显式绑 NULL）：
		// 少写一列等于让该分支的 hash 唯一索引静默失效（唯一索引对 NULL 不去重）。
		inList := make(map[string]bool, len(cols))
		for _, col := range cols {
			inList[strings.ToLower(col)] = true
		}
		for _, pair := range assetBranchColumns {
			for _, col := range pair {
				if !inList[col] {
					return assetReplyErr(fmt.Errorf("mock: INSERT must bind every identity/hash pair, %s missing: %s", col, q))
				}
			}
		}
		// NOT NULL 且无 DEFAULT 的列漏写，真库直接 1364。
		for _, col := range assetNotNullNoDefaultCols {
			if !inList[col] {
				return assetReplyErr(fmt.Errorf("mock: INSERT misses NOT NULL column %s: %s", col, q))
			}
		}
		activated := 0
		activatedCol := ""
		var activatedIdentity []byte
		for _, pair := range assetBranchColumns {
			identVal := assetColumnArg(cols, c.Args, pair[0])
			hashVal := assetColumnArg(cols, c.Args, pair[1])
			if identVal == nil && hashVal == nil {
				continue
			}
			if identVal == nil || hashVal == nil {
				return assetReplyErr(fmt.Errorf("mock: branch %s must bind identity+hash together", pair[0]))
			}
			identity, _ := assetBytes(identVal)
			hash, _ := assetBytes(hashVal)
			if len(hash) != 32 {
				return assetReplyErr(fmt.Errorf("mock: branch %s hash must be BINARY(32), got %d bytes", pair[1], len(hash)))
			}
			if sha256.Sum256(identity) != *(*[32]byte)(hash) {
				return assetReplyErr(fmt.Errorf("mock: branch %s hash does not match identity bytes", pair[1]))
			}
			activated++
			activatedCol = pair[1]
			activatedIdentity = identity
		}
		if activated > 1 {
			return assetReplyErr(fmt.Errorf("mock: %d branches activated in one INSERT, want exactly 1", activated))
		}
		if activated == 0 {
			return assetReplyErr(fmt.Errorf("mock: INSERT activated no identity branch: %s", q))
		}
		hashVal, _ := assetBytes(assetColumnArg(cols, c.Args, activatedCol))
		e.mu.Lock()
		key := assetHashKey(activatedCol, hashVal)
		if pk, dup := e.byHash[key]; dup {
			e.mu.Unlock()
			return assetReplyErr(&mysql.MySQLError{Number: 1062,
				Message: fmt.Sprintf("Duplicate entry for key 'uq_assets_%s' (row %d)", activatedCol, pk)})
		}
		id := e.nextID
		e.nextID++
		row := &assetMockRow{id: id, identities: map[string][]byte{}, values: map[string]driver.Value{}}
		row.identities["identity_"+strings.TrimPrefix(activatedCol, "hash_")] = activatedIdentity
		for i, col := range cols {
			if strings.HasPrefix(col, "identity_") || strings.HasPrefix(col, "hash_") {
				continue
			}
			row.values[col] = c.Args[i]
		}
		e.rows[id] = row
		e.byHash[key] = id
		e.mu.Unlock()
		return assetReplyInsert(id)
	}

	// 3) UPDATE：身份不可变更新（SET 列不得含 identity_*/hash_*）。
	if c.isUpdate() {
		cols := assetUpdateColumns(q)
		for _, col := range cols {
			if strings.HasPrefix(col, "identity_") || strings.HasPrefix(col, "hash_") {
				return assetReplyErr(fmt.Errorf("mock: UPDATE must never mutate identity column %q: %s", col, q))
			}
		}
		assigns := assetUpdateAssignments(q)
		idVal := c.arg(len(c.Args) - 1)
		id, _ := assetInt64(idVal)
		e.mu.Lock()
		row, ok := e.rows[id]
		if !ok {
			e.mu.Unlock()
			return assetMockReply{rowsAffected: 0}
		}
		argIdx := 0
		for _, a := range assigns {
			if !a.Param {
				continue
			}
			if argIdx >= len(c.Args)-1 {
				break
			}
			row.values[a.Col] = c.Args[argIdx]
			argIdx++
		}
		if e.corruptCol != "" {
			col := e.corruptCol
			e.corruptCol = ""
			forged := []byte("\x01forged-by-another-writer")
			if strings.HasPrefix(col, "identity_") {
				row.identities[col] = forged
			} else {
				row.values[col] = forged
			}
		}
		e.mu.Unlock()
		return assetMockReply{rowsAffected: 1}
	}

	// 4) 仲裁读取：SELECT `id`, `identity_x` FROM `assets` WHERE `hash_x` = ? FOR UPDATE
	hashCol := assetHashColumn(q)
	identCol := assetIdentityColumn(q)
	if c.isQuery() && hashCol != "" && identCol != "" {
		hash, _ := assetBytes(c.arg(0))
		e.mu.Lock()
		pk, ok := e.byHash[assetHashKey(hashCol, hash)]
		var identity []byte
		if ok {
			identity = e.rows[pk].identities[identCol]
		}
		e.mu.Unlock()
		if !ok {
			return assetReplyNoRows("id", identCol)
		}
		return assetReplyRows([]string{"id", identCol}, []driver.Value{pk, identity})
	}

	// 5) 按物理 PK 的行级悲观锁：SELECT id FROM assets WHERE id = ? FOR UPDATE
	if c.isSelectForUpdate() {
		id, _ := assetInt64(c.arg(0))
		e.mu.Lock()
		row, ok := e.rows[id]
		e.mu.Unlock()
		if !ok {
			return assetReplyNoRows("id")
		}
		return assetReplyRows([]string{"id"}, []driver.Value{row.id})
	}

	// 6) 普通回读：按 hash 定位（候选查询）或按 PK 回读业务/身份列。
	if c.isQuery() {
		cols := assetSelectColumns(q)
		if len(cols) == 0 {
			return assetReplyErr(fmt.Errorf("mock: cannot parse SELECT columns: %s", q))
		}
		if hashCol != "" {
			hash, _ := assetBytes(c.arg(0))
			e.mu.Lock()
			pk, ok := e.byHash[assetHashKey(hashCol, hash)]
			e.mu.Unlock()
			if !ok {
				return assetReplyNoRows(cols...)
			}
			return assetReplyRows(cols, e.rowValues(pk, cols))
		}
		normalized := strings.ReplaceAll(strings.ToUpper(q), "`", "")
		if strings.Contains(normalized, " WHERE ID = ?") {
			id, _ := assetInt64(c.arg(0))
			e.mu.Lock()
			_, ok := e.rows[id]
			e.mu.Unlock()
			if !ok {
				return assetReplyNoRows(cols...)
			}
			return assetReplyRows(cols, e.rowValues(id, cols))
		}
		return assetReplyErr(fmt.Errorf("mock: unsupported SELECT (no id/hash predicate): %s", q))
	}

	return assetReplyErr(fmt.Errorf("mock: unsupported statement: %s", q))
}

// rowValues 按列清单取回一行的持久值（identity 列优先，其次业务列，最后默认值）。
func (e *assetMockEngine) rowValues(pk int64, cols []string) []driver.Value {
	e.mu.Lock()
	defer e.mu.Unlock()
	row, ok := e.rows[pk]
	if !ok {
		return nil
	}
	out := make([]driver.Value, 0, len(cols))
	for _, col := range cols {
		switch {
		case col == "id":
			out = append(out, row.id)
		case strings.HasPrefix(col, "identity_"):
			out = append(out, row.identities[col])
		case strings.HasPrefix(col, "hash_"):
			ident := row.identities["identity_"+strings.TrimPrefix(col, "hash_")]
			if ident == nil {
				out = append(out, nil)
				continue
			}
			sum := sha256.Sum256(ident)
			out = append(out, sum[:])
		default:
			if v, ok := row.values[col]; ok {
				out = append(out, v)
				continue
			}
			v, ok := assetDefaultColumnValues[col]
			if !ok {
				out = append(out, fmt.Errorf("mock: no default value for column %q", col))
				continue
			}
			out = append(out, v)
		}
	}
	return out
}

// assetColumnArg 返回 INSERT/UPDATE 参数里指定列绑定的值（未出现则 nil）。
func assetColumnArg(cols []string, args []driver.Value, col string) driver.Value {
	for i, c := range cols {
		if c == col && i < len(args) {
			return args[i]
		}
	}
	return nil
}

func assetInt64(v driver.Value) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case []byte:
		var n int64
		_, err := fmt.Sscanf(string(t), "%d", &n)
		return n, err == nil
	case string:
		var n int64
		_, err := fmt.Sscanf(t, "%d", &n)
		return n, err == nil
	default:
		return 0, false
	}
}

// ── database/sql driver 侧实现 ────────────────────────────────────────────────

type assetMockRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *assetMockRows) Columns() []string { return r.cols }
func (r *assetMockRows) Close() error      { return nil }
func (r *assetMockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

type assetMockResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r assetMockResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r assetMockResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type assetMockConn struct {
	engine *assetMockEngine
	mu     sync.Mutex
	txSeq  int
	curTx  int
}

func (c *assetMockConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("asset mock: Prepare unused (queryer/execer path only)")
}
func (c *assetMockConn) Close() error { return nil }
func (c *assetMockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(nil, driver.TxOptions{})
}

func (c *assetMockConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	c.txSeq++
	c.curTx = c.txSeq
	txID := c.txSeq
	c.mu.Unlock()
	c.engine.record(assetMockCall{TxID: txID, Kind: assetKindBegin})
	return &assetMockTx{conn: c, id: txID}, nil
}

func (c *assetMockConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	txID := c.currentTx()
	c.engine.record(assetMockCall{TxID: txID, Kind: assetKindQuery, Query: query, Args: driverValues(args)})
	reply := c.engine.handle(assetMockCall{TxID: txID, Kind: assetKindQuery, Query: query, Args: driverValues(args)})
	if reply.err != nil {
		return nil, reply.err
	}
	return &assetMockRows{cols: reply.cols, rows: reply.rows}, nil
}

func (c *assetMockConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	txID := c.currentTx()
	call := assetMockCall{TxID: txID, Kind: assetKindExec, Query: query, Args: driverValues(args)}
	c.engine.record(call)
	reply := c.engine.handle(call)
	if reply.err != nil {
		return nil, reply.err
	}
	return assetMockResult{lastInsertID: reply.lastInsertID, rowsAffected: reply.rowsAffected}, nil
}

func (c *assetMockConn) currentTx() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curTx
}

type assetMockTx struct {
	conn *assetMockConn
	id   int
}

func (t *assetMockTx) Commit() error {
	t.conn.mu.Lock()
	if t.conn.curTx == t.id {
		t.conn.curTx = 0
	}
	t.conn.mu.Unlock()
	t.conn.engine.record(assetMockCall{TxID: t.id, Kind: assetKindCommit})
	return nil
}

func (t *assetMockTx) Rollback() error {
	t.conn.mu.Lock()
	if t.conn.curTx == t.id {
		t.conn.curTx = 0
	}
	t.conn.mu.Unlock()
	t.conn.engine.record(assetMockCall{TxID: t.id, Kind: assetKindRollback})
	return nil
}

func driverValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, 0, len(args))
	for _, a := range args {
		out = append(out, a.Value)
	}
	return out
}

type assetMockDriver struct{}

func (assetMockDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("asset mock driver: use OpenDB connector")
}

type assetMockConnector struct{ conn *assetMockConn }

func (c *assetMockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *assetMockConnector) Driver() driver.Driver                        { return assetMockDriver{} }

// openAssetMockDB 打开单连接 mock 库。MaxOpenConns(1) 是刻意约束：任何"持事务又
// 另取连接"的实现都会在这里暴露成超时，而不是悄悄在真实池上留下锁序风险。
func openAssetMockDB(t *testing.T, engine *assetMockEngine) *DB {
	t.Helper()
	conn := &assetMockConn{engine: engine}
	sqlDB := sql.OpenDB(&assetMockConnector{conn: conn})
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close mock db: %v", err)
		}
	})
	return &DB{DB: sqlDB}
}

// assetAssertTiDBDialect 断言一批语句里没有 PostgreSQL 方言残留。
func assetAssertTiDBDialect(t *testing.T, calls []assetMockCall) {
	t.Helper()
	for _, c := range calls {
		if strings.Contains(c.Query, "$1") || strings.Contains(c.Query, "$2") {
			t.Fatalf("PostgreSQL placeholder in %s", c.line())
		}
		for _, frag := range []string{"RETURNING", "ON CONFLICT", "ON DUPLICATE KEY UPDATE", "array_to_json", "::jsonb", "::bigint", "::cidr", "unnest(", "ILIKE", "DISTINCT ON"} {
			if strings.Contains(strings.ToUpper(c.Query), strings.ToUpper(frag)) {
				t.Fatalf("PostgreSQL dialect fragment %q in %s", frag, c.line())
			}
		}
	}
}

// assetIndexOf 返回第一条满足谓词的调用下标（无则 -1）。
func assetIndexOf(calls []assetMockCall, pred func(assetMockCall) bool) int {
	for i, c := range calls {
		if pred(c) {
			return i
		}
	}
	return -1
}

// assetCallsString 把调用轨迹渲染成便于失败诊断的多行文本。
func assetCallsString(calls []assetMockCall) string {
	parts := make([]string, 0, len(calls))
	for i, c := range calls {
		parts = append(parts, fmt.Sprintf("  [%d] %s", i, c.line()))
	}
	return "\n" + strings.Join(parts, "\n")
}

func assetWantString(t *testing.T, v driver.Value, want, what string) {
	t.Helper()
	got, _ := assetBytes(v)
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", what, string(got), want)
	}
}

// assetIsArbitration 判定 schema.VerifyHashArbitration 发出的那条带 hash 列的行锁读取。
func assetIsArbitration(c assetMockCall) bool {
	return c.isSelectForUpdate() && assetHashColumn(c.Query) != "" && assetIdentityColumn(c.Query) != ""
}

// assetIsCandidateLookup 判定按 hash 定位候选物理 PK 的非锁定读。
func assetIsCandidateLookup(c assetMockCall) bool {
	return c.isQuery() && assetHashColumn(c.Query) != "" && !c.isSelectForUpdate()
}

// assetIsPKLock 判定 `SELECT id FROM assets WHERE id = ? FOR UPDATE` 行级悲观锁。
func assetIsPKLock(c assetMockCall) bool {
	return c.isSelectForUpdate() && strings.Contains(strings.ReplaceAll(c.Query, "`", ""), "WHERE id = ? FOR UPDATE")
}

// =============================================================================
// ① Path B 首次插入：严格 INSERT（无 ODKU / 无 RETURNING）+ 同事务仲裁自洽
// =============================================================================

func TestAssets_RootDomainUpsert_VerifyBeforeUpdate(t *testing.T) {
	domain := fmt.Sprintf("p08-verify-%d.example.com", time.Now().UnixNano())
	identity := schema.AssetsRootDomainIdentity(domain)
	sum := schema.CanonicalHash(identity)

	eng := newAssetMockEngine()
	d := openAssetMockDB(t, eng)

	id, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: 7})
	if err != nil {
		t.Fatalf("UpsertRootDomain (Path B) failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive asset ID, got %d", id)
	}

	calls := eng.snapshot()
	begins, commits, rollbacks := eng.txCounts()
	if begins != 1 || commits != 1 || rollbacks != 0 {
		t.Fatalf("Path B must run in exactly one committed transaction: begins=%d commits=%d rollbacks=%d%s",
			begins, commits, rollbacks, assetCallsString(calls))
	}
	assetAssertTiDBDialect(t, calls)

	inserts := eng.callsFor(assetMockCall.isInsert)
	if len(inserts) != 1 {
		t.Fatalf("Path B must issue exactly one strict INSERT, got %d%s", len(inserts), assetCallsString(calls))
	}
	ins := inserts[0]
	cols := assetInsertColumns(ins.Query)
	assetWantString(t, assetColumnArg(cols, ins.Args, "type"), "root_domain", "insert type")
	assetWantString(t, assetColumnArg(cols, ins.Args, "domain"), domain, "insert domain")
	assetWantString(t, assetColumnArg(cols, ins.Args, "root_domain"), domain, "insert root_domain")
	assetWantString(t, assetColumnArg(cols, ins.Args, "identity_root_domain"), string(identity), "insert identity_root_domain")
	assetWantString(t, assetColumnArg(cols, ins.Args, "hash_root_domain"), string(sum[:]), "insert hash_root_domain")
	assetWantString(t, assetColumnArg(cols, ins.Args, "task_ids"), "[7]", "insert task_ids must be valid JSON")
	assetWantString(t, assetColumnArg(cols, ins.Args, "company_source"), "scope", "insert company_source")

	// 严格列激活：其余 7 个分支的 identity/hash 必须显式绑定 NULL。
	for _, pair := range assetBranchColumns {
		if pair[0] == "identity_root_domain" {
			continue
		}
		if v := assetColumnArg(cols, ins.Args, pair[0]); v != nil {
			t.Fatalf("branch %s must be bound NULL on a root_domain insert, got %v", pair[0], v)
		}
		if v := assetColumnArg(cols, ins.Args, pair[1]); v != nil {
			t.Fatalf("branch %s must be bound NULL on a root_domain insert, got %v", pair[1], v)
		}
	}

	// 仲裁自洽必须在同一事务、提交之前发生。
	insIdx := assetIndexOf(calls, assetMockCall.isInsert)
	arbIdx := assetIndexOf(calls, assetIsArbitration)
	commitIdx := assetIndexOf(calls, func(c assetMockCall) bool { return c.Kind == assetKindCommit })
	if arbIdx < 0 {
		t.Fatalf("Path B must self-verify the fresh row with schema.VerifyHashArbitration%s", assetCallsString(calls))
	}
	if !(insIdx >= 0 && insIdx < arbIdx && arbIdx < commitIdx) {
		t.Fatalf("arbitration must land between INSERT and COMMIT (insert=%d arb=%d commit=%d)%s",
			insIdx, arbIdx, commitIdx, assetCallsString(calls))
	}
	if u := eng.callsFor(assetMockCall.isUpdate); len(u) != 0 {
		t.Fatalf("first insert must not issue any UPDATE: %s", u[0].line())
	}

	row := eng.row(id)
	if row == nil {
		t.Fatalf("asset row %d missing after Path B insert", id)
	}
	if got, ok := assetBytes(row.values["extra"]); ok && string(got) != "{}" {
		t.Fatalf("extra must be a valid JSON object, got %q", string(got))
	}
}

// =============================================================================
// ② Path A 现存行更新：PK 预锁 → 写前仲裁 → 身份不可变更新
// =============================================================================

func TestAssets_PathAUpdate_IdentityImmutable(t *testing.T) {
	domain := fmt.Sprintf("p08-patha-%d.example.com", time.Now().UnixNano())
	identity := schema.AssetsRootDomainIdentity(domain)

	eng := newAssetMockEngine()
	d := openAssetMockDB(t, eng)

	id1, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: 11})
	if err != nil {
		t.Fatalf("first UpsertRootDomain failed: %v", err)
	}

	// 大写输入经 DomainKey 规范化后命中同一 canonical 身份，必须走 Path A 复用同一物理 PK。
	eng.resetCalls()
	rowsBefore := eng.rowCount()
	id2, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: strings.ToUpper(domain), TaskID: 12})
	if err != nil {
		t.Fatalf("second UpsertRootDomain (Path A) failed: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("Path A must reuse the physical row: want id %d, got %d", id1, id2)
	}

	calls := eng.snapshot()
	assetAssertTiDBDialect(t, calls)
	if eng.rowCount() != rowsBefore {
		t.Fatalf("Path A must not create a new row: rows %d -> %d", rowsBefore, eng.rowCount())
	}

	// 规范顺序：候选定位 → PK 行锁 → 写前仲裁 → 身份不可变 UPDATE → 提交。
	candIdx := assetIndexOf(calls, assetIsCandidateLookup)
	lockIdx := assetIndexOf(calls, assetIsPKLock)
	arbIdx := assetIndexOf(calls, assetIsArbitration)
	updIdx := assetIndexOf(calls, assetMockCall.isUpdate)
	commitIdx := assetIndexOf(calls, func(c assetMockCall) bool { return c.Kind == assetKindCommit })
	for name, idx := range map[string]int{"candidate": candIdx, "pk-lock": lockIdx, "arbitration": arbIdx, "update": updIdx, "commit": commitIdx} {
		if idx < 0 {
			t.Fatalf("Path A missing %s step:%s", name, assetCallsString(calls))
		}
	}
	if !(candIdx < lockIdx && lockIdx < arbIdx && arbIdx < updIdx && updIdx < commitIdx) {
		t.Fatalf("Path A order must be candidate < pk-lock < arbitration < update < commit (got %d/%d/%d/%d/%d)%s",
			candIdx, lockIdx, arbIdx, updIdx, commitIdx, assetCallsString(calls))
	}

	// 身份不可变：UPDATE 的 SET 里严禁出现 identity_*/hash_* 与任一分支自然键列。
	updates := eng.callsFor(assetMockCall.isUpdate)
	if len(updates) != 1 {
		t.Fatalf("Path A must issue exactly one UPDATE, got %d", len(updates))
	}
	forbidden := []string{"identity_", "hash_", "domain", "root_domain", "url", "method", "bundle_id", "app_name", "port", "service_name", "service_type", "record_type"}
	for _, col := range assetUpdateColumns(updates[0].Query) {
		if slices.Contains(forbidden, col) {
			t.Fatalf("Path A UPDATE must never mutate identity/natural-key column %q: %s", col, updates[0].line())
		}
	}
	if got := eng.row(id1).identities["identity_root_domain"]; string(got) != string(identity) {
		t.Fatalf("persisted identity drifted after Path A update: %q", string(got))
	}
	if got, _ := assetBytes(eng.row(id1).values["task_ids"]); !strings.Contains(string(got), "11") || !strings.Contains(string(got), "12") {
		t.Fatalf("Path A must merge task_ids (want 11 and 12), got %q", string(got))
	}
	if _, commits, rollbacks := eng.txCounts(); commits == 0 || rollbacks != 0 {
		t.Fatalf("Path A must commit without rollback (commits=%d rollbacks=%d)", commits, rollbacks)
	}
}

// =============================================================================
// ③ 强制 hash 碰撞：写前仲裁必须 ErrIdentityHashCollision 且零写入
// =============================================================================

func TestAssets_HashCollision_RejectedWithZeroWrite(t *testing.T) {
	domain := fmt.Sprintf("p08-coll-%d.example.com", time.Now().UnixNano())
	foreign := fmt.Sprintf("p08-foreign-%d.example.com", time.Now().UnixNano())
	identity := schema.AssetsRootDomainIdentity(domain)
	sum := schema.CanonicalHash(identity)
	foreignIdentity := schema.AssetsRootDomainIdentity(foreign)

	eng := newAssetMockEngine()
	d := openAssetMockDB(t, eng)

	id, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
	if err != nil {
		t.Fatalf("seed UpsertRootDomain failed: %v", err)
	}

	// 碰撞注入：hash_root_domain 仍指向输入 domain 的 canonical hash，
	// identity_root_domain 改写为另一域名的 canonical identity。
	eng.injectHashCollision(t, id, "identity_root_domain", foreignIdentity)
	eng.resetCalls()
	rowsBefore := eng.rowCount()

	if _, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain}); !errors.Is(err, schema.ErrIdentityHashCollision) {
		t.Fatalf("expected schema.ErrIdentityHashCollision from write-ahead arbitration, got %v", err)
	}

	calls := eng.snapshot()
	assetAssertTiDBDialect(t, calls)
	if got := eng.callsFor(func(c assetMockCall) bool { return c.isInsert() || c.isUpdate() }); len(got) != 0 {
		t.Fatalf("collision arbitration must write zero rows, first write: %s%s", got[0].line(), assetCallsString(calls))
	}
	if _, commits, rollbacks := eng.txCounts(); commits != 0 || rollbacks != 1 {
		t.Fatalf("collision must roll the whole transaction back (commits=%d rollbacks=%d)", commits, rollbacks)
	}
	if eng.rowCount() != rowsBefore {
		t.Fatalf("collision must not add rows: %d -> %d", rowsBefore, eng.rowCount())
	}
	if got := eng.row(id).identities["identity_root_domain"]; string(got) != string(foreignIdentity) {
		t.Fatalf("collision path overwrote persisted identity: write-before-overwrite is forbidden")
	}
	if eng.hashRows("hash_root_domain", sum[:]) != 1 {
		t.Fatalf("collision hash must still match exactly the single pre-existing row")
	}
}

// =============================================================================
// ④ Path B 并发 1062：本轮整体回滚 + 全新事务转 Path A 重演
// =============================================================================

func TestAssets_PathB_ConcurrentSameDomain_1062PathARetry(t *testing.T) {
	domain := fmt.Sprintf("p08-race-%d.example.com", time.Now().UnixNano())
	identity := schema.AssetsRootDomainIdentity(domain)

	eng := newAssetMockEngine()
	d := openAssetMockDB(t, eng)

	// 并发赢家先行提交（同一 canonical 身份）。
	winnerID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
	if err != nil {
		t.Fatalf("concurrent winner upsert failed: %v", err)
	}

	// 受控时序：输家的候选查询快照仍看不到赢家 → 走 Path B 严格 INSERT，
	// 真实撞上唯一索引 1062。
	blocked := true
	eng.setHook(func(c assetMockCall) (assetMockReply, bool) {
		if blocked && assetIsCandidateLookup(c) {
			blocked = false
			return assetReplyNoRows(assetSelectColumns(c.Query)...), true
		}
		return assetMockReply{}, false
	})

	eng.resetCalls()
	rowsBefore := eng.rowCount()
	loserID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: strings.ToUpper(domain)})
	if err != nil {
		t.Fatalf("1062 conflict must rollback the losing attempt and retry via Path A, got err %v", err)
	}

	calls := eng.snapshot()
	assetAssertTiDBDialect(t, calls)
	if loserID != winnerID || loserID <= 0 {
		t.Fatalf("the racing upsert must resolve to the winner's physical id %d, got %d", winnerID, loserID)
	}
	if eng.rowCount() != rowsBefore {
		t.Fatalf("exactly one asset row must survive the 1062 race: rows %d -> %d", rowsBefore, eng.rowCount())
	}
	if got := eng.row(winnerID).identities["identity_root_domain"]; string(got) != string(identity) {
		t.Fatalf("identity drifted through the 1062 retry")
	}

	begins, commits, rollbacks := eng.txCounts()
	if begins != 2 {
		t.Fatalf("1062 must retry in a brand-new transaction (Tx2 != Tx1): begins=%d%s", begins, assetCallsString(calls))
	}
	if rollbacks != 1 || commits != 1 {
		t.Fatalf("the failed attempt must fully roll back and the retry must commit (commits=%d rollbacks=%d)%s",
			commits, rollbacks, assetCallsString(calls))
	}
	rollbackIdx := assetIndexOf(calls, func(c assetMockCall) bool { return c.Kind == assetKindRollback })
	commitIdx := assetIndexOf(calls, func(c assetMockCall) bool { return c.Kind == assetKindCommit })
	secondBeginIdx := -1
	if n := 0; true {
		for i, c := range calls {
			if c.Kind == assetKindBegin {
				n++
				if n == 2 {
					secondBeginIdx = i
					break
				}
			}
		}
	}
	if !(rollbackIdx >= 0 && secondBeginIdx > rollbackIdx && commitIdx > secondBeginIdx) {
		t.Fatalf("order must be Tx1 rollback < Tx2 begin < Tx2 commit (rollback=%d tx2=%d commit=%d)%s",
			rollbackIdx, secondBeginIdx, commitIdx, assetCallsString(calls))
	}
	// 新事务必须转入 Path A：重试事务里不得再出现 INSERT，且必须重新拿 PK 行锁 + 写前仲裁。
	retry := calls[secondBeginIdx:]
	if idx := assetIndexOf(retry, assetMockCall.isInsert); idx >= 0 {
		t.Fatalf("retry transaction must switch to Path A, still inserting: %s", retry[idx].line())
	}
	for name, pred := range map[string]func(assetMockCall) bool{"pk-lock": assetIsPKLock, "arbitration": assetIsArbitration} {
		if assetIndexOf(retry, pred) < 0 {
			t.Fatalf("retry transaction must replay Path A step %s:%s", name, assetCallsString(retry))
		}
	}
	if idx := assetIndexOf(retry, assetMockCall.isUpdate); idx < 0 {
		t.Fatalf("retry transaction must apply the identity-immutable UPDATE:%s", assetCallsString(retry))
	}
}

// =============================================================================
// ⑤ 八分支严格列激活矩阵：仅激活对应分支的 identity/hash 对
// =============================================================================

func TestAssets_BranchColumnActivation(t *testing.T) {
	stamp := time.Now().UnixNano()
	cases := []struct {
		name      string
		identCol  string
		hashCol   string
		upsert    func(*AssetStore) (int64, error)
		identity  []byte
		checkCols map[string]string
	}{
		{
			name: "root_domain", identCol: "identity_root_domain", hashCol: "hash_root_domain",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertRootDomain(UpsertRootDomainReq{Domain: fmt.Sprintf("p08-b%d.example.com", stamp)})
			},
			identity: schema.AssetsRootDomainIdentity(fmt.Sprintf("p08-b%d.example.com", stamp)),
		},
		{
			name: "ip", identCol: "identity_ip", hashCol: "hash_ip",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertIP(UpsertIPReq{IP: fmt.Sprintf("10.%d.%d.7", (stamp/1000)%200, stamp%200)})
			},
			identity: schema.AssetsIPIdentity(fmt.Sprintf("10.%d.%d.7", (stamp/1000)%200, stamp%200)),
		},
		{
			name: "subdomain", identCol: "identity_subdomain", hashCol: "hash_subdomain",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertSubdomain(UpsertSubdomainReq{Domain: fmt.Sprintf("p08-b%d.example.com", stamp), RecordType: "CNAME"})
			},
			identity: schema.AssetsSubdomainIdentity(fmt.Sprintf("p08-b%d.example.com", stamp), "CNAME"),
		},
		{
			name: "app_bundle", identCol: "identity_app_bundle", hashCol: "hash_app_bundle",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertApp(UpsertAppReq{Name: "artex-app", BundleID: fmt.Sprintf("io.artex.b%d", stamp)})
			},
			identity: schema.AssetsAppBundleIdentity(fmt.Sprintf("io.artex.b%d", stamp)),
		},
		{
			name: "app_name", identCol: "identity_app_name", hashCol: "hash_app_name",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertApp(UpsertAppReq{Name: fmt.Sprintf("artex-app-%d", stamp)})
			},
			identity: schema.AssetsAppNameIdentity(fmt.Sprintf("artex-app-%d", stamp)),
		},
		{
			name: "service_http", identCol: "identity_service_http", hashCol: "hash_service_http",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertHTTPService(UpsertHTTPServiceReq{URL: fmt.Sprintf("HTTP://P08-H%d.Example.COM:80/api/", stamp)})
			},
			identity: schema.AssetsServiceHTTPIdentity(fmt.Sprintf("http://p08-h%d.example.com/api", stamp)),
		},
		{
			name: "service_other", identCol: "identity_service_other", hashCol: "hash_service_other",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertOtherService(UpsertOtherServiceReq{
					Domain: fmt.Sprintf("p08-s%d.example.com", stamp), Port: 6379, ServiceName: "  ReDiS  ",
				})
			},
			identity: schema.AssetsServiceOtherIdentity(fmt.Sprintf("p08-s%d.example.com", stamp), "", 6379, false, "redis", false),
		},
		{
			name: "endpoint", identCol: "identity_endpoint", hashCol: "hash_endpoint",
			upsert: func(a *AssetStore) (int64, error) {
				return a.UpsertEndpoint(UpsertEndpointReq{URL: fmt.Sprintf("http://P08-E%d.Example.COM/v1/ping", stamp), Method: "post"})
			},
			identity: schema.AssetsEndpointIdentity(fmt.Sprintf("http://p08-e%d.example.com/v1/ping", stamp), "POST"),
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			eng := newAssetMockEngine()
			d := openAssetMockDB(t, eng)

			if _, err := tc.upsert(d.Assets()); err != nil {
				t.Fatalf("upsert %s failed: %v", tc.name, err)
			}
			calls := eng.snapshot()
			assetAssertTiDBDialect(t, calls)

			// 主分支 INSERT（按激活列定位）必须存在。
			var main assetMockCall
			found := false
			// 每条 INSERT 都会把 8 对 identity/hash 列写进列清单（未激活的绑定 NULL），
			// 所以"主分支 INSERT"必须按**绑定值**识别，而不是按列名是否出现。
			for _, c := range eng.callsFor(assetMockCall.isInsert) {
				if assetColumnArg(assetInsertColumns(c.Query), c.Args, tc.identCol) != nil {
					main, found = c, true
					break
				}
			}
			if !found {
				t.Fatalf("no INSERT activated %s:%s", tc.identCol, assetCallsString(calls))
			}
			cols := assetInsertColumns(main.Query)
			assetWantString(t, assetColumnArg(cols, main.Args, tc.identCol), string(tc.identity), "identity column")
			sum := schema.CanonicalHash(tc.identity)
			assetWantString(t, assetColumnArg(cols, main.Args, tc.hashCol), string(sum[:]), "hash column")

			inList := map[string]bool{}
			for _, col := range cols {
				inList[strings.ToLower(col)] = true
			}
			for _, pair := range assetBranchColumns {
				for _, col := range pair {
					if !inList[col] {
						t.Fatalf("%s insert must bind all 8 identity/hash pairs, %s missing", tc.name, col)
					}
					if pair[0] == tc.identCol {
						continue
					}
					if v := assetColumnArg(cols, main.Args, col); v != nil {
						t.Fatalf("%s insert must bind %s NULL, got %v", tc.name, col, v)
					}
				}
			}
			for _, c := range eng.callsFor(assetMockCall.isInsert) {
				colsOfC := assetInsertColumns(c.Query)
				activated := 0
				for _, pair := range assetBranchColumns {
					if assetColumnArg(colsOfC, c.Args, pair[0]) != nil {
						activated++
					}
				}
				if activated != 1 {
					t.Fatalf("every INSERT must activate exactly one branch, got %d: %s", activated, c.line())
				}
			}
			for col, want := range tc.checkCols {
				assetWantString(t, assetColumnArg(cols, main.Args, col), want, col)
			}
		})
	}
}

// =====================================================================
// 真机集成变体（需 ARTEX_TIDB_DSN，否则显式 skip）
// =====================================================================
//
// 上面的 4 组核心用例是**离线 mock 协议测试**：它们在没有 TiDB 实例的环境里也能
// 判定"先覆盖后仲裁""ODKU 覆盖写""改写身份列""1062 原地重试"这些错误实现为红。
// 下面的 *_LiveTiDB 变体补上真库视角：真实 8 条 hash 唯一索引、真实 1062、真实
// 行锁与 AUTO_INCREMENT 行为，只能在连到 TiDB 时执行。

// openLiveTiDB 打开真库并在测试结束时关闭。
func openLiveTiDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open tidb: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestAssets_PathAUpdate_IdentityImmutable_LiveTiDB 在真库上验证 Path A 复用同一
// 物理 PK、身份列逐字节不变、非身份字段正常合并。
func TestAssets_PathAUpdate_IdentityImmutable_LiveTiDB(t *testing.T) {
	d := openLiveTiDB(t)
	assets := d.Assets()

	domain := fmt.Sprintf("p08-live-%d.io", time.Now().UnixNano())
	defer func() { _, _ = assets.DeleteByHost(domain) }()

	identity := schema.AssetsRootDomainIdentity(domain)
	sum := schema.CanonicalHash(identity)

	first, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: 9001})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// 大小写变体必须命中同一 canonical 身份 → 同一 PK（Path A）。
	second, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: strings.ToUpper(domain), TaskID: 9002})
	if err != nil {
		t.Fatalf("path A update: %v", err)
	}
	if second != first {
		t.Fatalf("path A must reuse pk: %d != %d", second, first)
	}

	var persisted []byte
	var taskIDs string
	if err := d.QueryRow(`SELECT identity_root_domain, task_ids FROM assets WHERE id = ?`, first).
		Scan(&persisted, &taskIDs); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(persisted) != string(identity) {
		t.Errorf("identity columns must stay byte-exact after update")
	}
	for _, want := range []string{"9001", "9002"} {
		if !strings.Contains(taskIDs, want) {
			t.Errorf("task_ids must merge %s, got %s", want, taskIDs)
		}
	}
	var rows int
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE hash_root_domain = ?`, sum[:]).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("canonical identity must occupy exactly one row, got %d", rows)
	}
}

// TestAssets_HashCollision_RejectedWithZeroWrite_LiveTiDB 伪造"hash 命中、identity
// 不符"的行：真库必须让写入以 ErrIdentityHashCollision 结束，且伪造行零改动。
func TestAssets_HashCollision_RejectedWithZeroWrite_LiveTiDB(t *testing.T) {
	d := openLiveTiDB(t)
	assets := d.Assets()

	domain := fmt.Sprintf("p08-collide-%d.io", time.Now().UnixNano())
	defer func() { _, _ = assets.DeleteByHost(domain) }()

	victim := fmt.Sprintf("victim-%d", time.Now().UnixNano())
	foreignKey := schema.AssetsRootDomainIdentity(victim)
	sum := schema.CanonicalHash(schema.AssetsRootDomainIdentity(domain))

	forgedID := assetRawInsert(t, d, map[string]any{
		"type":                 "root_domain",
		"domain":               victim,
		"root_domain":          victim,
		"identity_root_domain": foreignKey,
		"hash_root_domain":     sum[:],
	})
	t.Cleanup(func() { deleteAssets(d, []int64{forgedID}) })

	_, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, ICP: "SHOULD-NOT-BE-WRITTEN"})
	if !errors.Is(err, schema.ErrIdentityHashCollision) {
		t.Fatalf("want ErrIdentityHashCollision, got %v", err)
	}

	var persisted []byte
	var icp any
	if err := d.QueryRow(`SELECT identity_root_domain, icp FROM assets WHERE id = ?`, forgedID).
		Scan(&persisted, &icp); err != nil {
		t.Fatalf("read forged row: %v", err)
	}
	if string(persisted) != string(foreignKey) {
		t.Errorf("collision must not overwrite the existing identity")
	}
	if icp != nil {
		t.Errorf("collision must write zero columns, icp = %v", icp)
	}
	var rows int
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE hash_root_domain = ?`, sum[:]).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rejected write must not add rows, got %d", rows)
	}
}

// TestAssets_PathB_ConcurrentSameDomain_1062PathARetry_LiveTiDB 用真实并发把
// "唯一索引裁决 + 整笔回滚 + 新事务转 Path A"逼出来：所有 goroutine 必须拿到同一个
// PK，且 hash 唯一索引里只有一行。
func TestAssets_PathB_ConcurrentSameDomain_1062PathARetry_LiveTiDB(t *testing.T) {
	d := openLiveTiDB(t)
	assets := d.Assets()

	domain := fmt.Sprintf("p08-race-%d.io", time.Now().UnixNano())
	defer func() { _, _ = assets.DeleteByHost(domain) }()
	sum := schema.CanonicalHash(schema.AssetsRootDomainIdentity(domain))

	const workers = 8
	start := make(chan struct{})
	ids := make([]int64, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, TaskID: int64(7000 + i)})
			ids[i], errs[i] = id, err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("all workers must converge on one pk: %d != %d", ids[i], ids[0])
		}
		if ids[i] < 1 {
			t.Fatalf("worker %d returned pk %d", i, ids[i])
		}
	}
	var rows int
	if err := d.QueryRow(`SELECT COUNT(*) FROM assets WHERE hash_root_domain = ?`, sum[:]).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("concurrent same-identity writes must leave exactly one row, got %d", rows)
	}
}

// TestAssets_RootDomainUpsert_VerifyBeforeUpdate_LiveTiDB 在真库上验证仲裁的时序
// 可观察结果：更新前后身份都必须与输入 canonical 身份逐字节一致（不存在"先覆盖
// 后仲裁"能留下的中间态），且身份列在 UPDATE 后仍是同一个值。
func TestAssets_RootDomainUpsert_VerifyBeforeUpdate_LiveTiDB(t *testing.T) {
	d := openLiveTiDB(t)
	assets := d.Assets()

	domain := fmt.Sprintf("p08-order-%d.io", time.Now().UnixNano())
	defer func() { _, _ = assets.DeleteByHost(domain) }()
	identity := schema.AssetsRootDomainIdentity(domain)

	if _, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// 第二次写入必须走"PK 行锁 → 写前仲裁 → 身份不可变 UPDATE"，任何一步失败都会
	// 留下身份漂移或额外行。
	id, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain, ICP: "LIVE-ICP", TaskID: 4242})
	if err != nil {
		t.Fatalf("verify-before-update: %v", err)
	}

	var persisted []byte
	var icp, source string
	if err := d.QueryRow(`SELECT identity_root_domain, COALESCE(icp,''), company_source FROM assets WHERE id = ?`, id).
		Scan(&persisted, &icp, &source); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(persisted) != string(identity) {
		t.Errorf("persisted identity must equal the canonical identity byte-for-byte")
	}
	if icp != "LIVE-ICP" {
		t.Errorf("non-identity field must be updated, icp = %q", icp)
	}
	if source != "scope" {
		t.Errorf("company_source must stay scope-derived, got %q", source)
	}
}

// =====================================================================
// 方言静态守卫（M05 / M10 / M34：生产 SQL 里不得再出现 PG 片段）
// =====================================================================

// TestAssets_SourceIsTiDBDialectOnly 扫描 assets.go / asset_dsl.go 的**可执行代码**
// （去掉行注释），任何 PostgreSQL 片段都判红。这条守卫比 mock 的逐语句断言更严：
// 它连"写出来但当前没有被调用到"的 PG SQL 都能抓到。
func TestAssets_SourceIsTiDBDialectOnly(t *testing.T) {
	pgFragments := []string{
		"$1", "$2", "$3", "ON CONFLICT", "RETURNING", "pg_advisory",
		"array_to_json", "to_jsonb", "jsonb_array_elements", "::jsonb", "::bigint",
		"::text", "::cidr", "::inet", "ILIKE", "unnest(", "= ANY(", "DISTINCT ON",
		"masklen(",
	}
	for _, file := range []string{"assets.go", "asset_dsl.go"} {
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
		}
	}
}

// TestAssets_PathAIdentityDriftRolledBack 模拟"UPDATE 之后身份列被改写成外来字节"
// 这一最坏情况：实现方必须在提交前复核持久化身份，发现漂移就整笔回滚，
// 绝不允许把被覆盖的身份提交出去。
func TestAssets_PathAIdentityDriftRolledBack(t *testing.T) {
	eng := newAssetMockEngine()
	d := openAssetMockDB(t, eng)
	assets := d.Assets()

	domain := "p08-drift.io"
	if _, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: domain}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	eng.resetCalls()
	eng.corruptIdentityAfterUpdate("identity_root_domain")

	if _, err := assets.UpsertRootDomain(UpsertRootDomainReq{Domain: strings.ToUpper(domain), ICP: "X"}); err == nil {
		t.Fatal("identity drift after UPDATE must fail the write")
	}
	_, commits, rollbacks := eng.txCounts()
	if commits != 0 {
		t.Errorf("drifted write must never commit (commits=%d)", commits)
	}
	if rollbacks == 0 {
		t.Errorf("drifted write must roll back the whole attempt")
	}
}
