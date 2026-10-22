package evidence

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/traffic"
)

// evidenceDSN 返回本轮集成测试使用的 TiDB 连接串（spec §3.1.1 / §4.3）。
//
//   - 未设置 ARTEX_TIDB_DSN → 诚实 t.Skip 声明"本用例未执行"，绝不当作通过；
//   - 已设置 → 用例必须真实连库跑，连不上/基线缺失都是环境违规，由调用方判红。
//     ARTEX_PG_DSN 与 postgres:// 协议已彻底废除，这里不再读取。
func evidenceDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("ARTEX_TIDB_DSN"))
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set")
	}
	return dsn
}

// newRunTag 生成本轮 fixture 的唯一标记。它被写进证据快照的 url 里
// （https://fixture.local/<tag>/<id>），于是"这一轮到底造了哪些快照行"就有了
// 可精确回读的物理判据 —— 既不依赖时间窗口，也绝不圈到别人/历史的数据。
func newRunTag(t *testing.T) string {
	t.Helper()
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("生成 fixture 标记失败: %v", err)
	}
	return "ev" + hex.EncodeToString(raw[:])
}

// evidenceFixture 开一个真实 TiDB 连接 + 一个一次性任务 + 一个本地流量索引。
//
// 返回的 tag 是本轮唯一标记：seedExchange 必须把它拼进 url，清理阶段再按它
// 精确回读并物理删净本轮的 traffic_evidence_snapshots 行。
func evidenceFixture(t *testing.T) (*Store, db.RecordFindingInput, string, string) {
	t.Helper()
	pg, err := db.Open(evidenceDSN(t))
	if err != nil {
		t.Fatalf("ARTEX_TIDB_DSN 已配置但打开 TiDB 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	// 连接池 Close 是本函数最先登记的一条 cleanup，按 LIFO 最后执行，
	// 于是下面的 fixture 清理一定跑在还活着的池上。
	t.Cleanup(func() {
		if err := pg.Close(); err != nil {
			t.Errorf("关闭 TiDB 连接池失败: %v", err)
		}
	})
	tag := newRunTag(t)
	task, err := pg.CreateTask("evidence integration "+tag+" "+t.Name(), "local fixtures", nil, 0, 0)
	if err != nil {
		t.Fatalf("创建任务 fixture 失败: %v", err)
	}
	t.Cleanup(func() { cleanupEvidenceFixture(t, pg, task.ID, tag) })
	dir := t.TempDir()
	tr, err := traffic.Open(filepath.Join(dir, "traffic"), ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err = tr.Close(); err != nil {
			t.Errorf("关闭流量索引失败: %v", err)
		}
	})
	store := New(pg, tr, filepath.Join(dir, "evidence"))
	in := db.RecordFindingInput{TaskID: task.ID, ExplorationID: task.ExplorationID, Worker: "test", VulnClass: "TEST", Name: "Evidence fixture", Severity: "low", Summary: "local test"}
	return store, in, dir, tag
}

// evidenceURL 把本轮 tag 写进 fixture 的 url：快照 id 是内容的 SHA-256，
// 带上 tag 后本轮每一条快照行都拥有独一无二的物理主键，清理才谈得上"精确"。
func evidenceURL(tag, id string) string { return "https://fixture.local/" + tag + "/" + id }

// cleanupEvidenceFixture 逆拓扑删净本轮 fixture：
// 漏洞+绑定（产品级 DeleteFindingsByTask）→ 任务及其 exploration 子图
// （DeleteTask 的完整物理闭包）→ 证据快照（按本轮 tag 精确回读出的物理主键）。
// 任何一步失败都 t.Errorf 判红，绝不静默留下孤儿行。
func cleanupEvidenceFixture(t *testing.T, pg *db.DB, taskID int64, tag string) {
	t.Helper()
	// 快照主键必须在删漏洞之前取：绑定一没，就没有别的路径能认出这些行了。
	// tag 是随机 12 位十六进制，`%/<tag>/%` 只会命中本轮写入的 url。
	pattern := "%/" + tag + "/%"
	snapshots := evidenceSnapshotIDs(t, pg, pattern)
	if _, err := pg.DeleteFindingsByTask(taskID); err != nil {
		t.Errorf("清理任务 %d 的漏洞 fixture 失败: %v", taskID, err)
	}
	if err := pg.DeleteTask(taskID); err != nil {
		t.Errorf("清理任务 fixture %d 失败: %v", taskID, err)
	}
	deleteEvidenceSnapshots(t, pg, snapshots)
	if left := evidenceSnapshotIDs(t, pg, pattern); len(left) != 0 {
		t.Errorf("证据快照未归零: task=%d 残留 %d 行 (示例 %s)", taskID, len(left), strings.Join(left, ","))
	}
}

func evidenceSnapshotIDs(t *testing.T, pg *db.DB, urlPattern string) []string {
	t.Helper()
	rows, err := pg.Query(`SELECT id FROM traffic_evidence_snapshots WHERE url LIKE ? ORDER BY id`, urlPattern)
	if err != nil {
		t.Errorf("回读证据快照 fixture 失败: %v", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Errorf("扫描证据快照主键失败: %v", err)
			return out
		}
		out = append(out, id)
	}
	if err = rows.Err(); err != nil {
		t.Errorf("遍历证据快照 fixture 失败: %v", err)
	}
	return out
}

func deleteEvidenceSnapshots(t *testing.T, pg *db.DB, ids []string) {
	t.Helper()
	const batch = 200
	for start := 0; start < len(ids); start += batch {
		end := min(start+batch, len(ids))
		args := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			args = append(args, id)
		}
		place := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
		if _, err := pg.Exec(`DELETE FROM traffic_evidence_snapshots WHERE id IN (`+place+`)`, args...); err != nil {
			t.Errorf("清理证据快照 fixture 失败 (%d 行): %v", len(args), err)
		}
	}
}

// seedExchange 往本轮一次性 SQLite 流量索引里写一条 exchange。
// url 带上 tag（见 evidenceURL），host 保持 fixture.local —— 用例里的
// DeleteHost("fixture.local") 语义不变。
func seedExchange(t *testing.T, s *Store, tag, id string, body []byte, spill bool) {
	t.Helper()
	var blob any
	inline := body
	if spill {
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		blob = hash
		path := filepath.Join(filepath.Dir(s.Dir), "traffic", "_blobs", "sha256", hash[:2], hash+".bin")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		inline = []byte("TRUNCATED PREVIEW")
	}
	_, err := s.Traffic.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path) VALUES(?,?,'fixture.local','POST','/test',?,200,'application/octet-stream',2,?,'')`, id, time.Now().Unix(), evidenceURL(tag, id), len(body))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Traffic.DB().Exec(`INSERT INTO exchange_bodies(id,req_head,req_body,resp_head,resp_body,resp_blob) VALUES(?,?,?, ?,?,?)`, id, "POST /test HTTP/1.1\nHost: fixture.local\n", []byte("{}"), "HTTP 200\nContent-Type: application/octet-stream\n", inline, blob)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceBindingLifecycle(t *testing.T) {
	s, in, _, tag := evidenceFixture(t)
	ctx := context.Background()
	large := bytes.Repeat([]byte{0, 1, 2, 255, 'a', 'b'}, 180000)
	for i := 0; i < 3; i++ {
		seedExchange(t, s, tag, fmt.Sprint(i), large, i == 2)
	}
	refs := []db.TrafficRef{{TrafficID: "0", Role: "baseline", Note: "normal"}, {TrafficID: "1", Role: "proof", Note: "proof"}, {TrafficID: "2", Role: "verification", Note: "binary"}}
	r, err := s.Record(ctx, in, refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Traffic.Bindings) != 3 || r.Traffic.Version != 1 {
		t.Fatalf("record=%+v", r)
	}
	again, err := s.Bind(ctx, r.FindingID, []db.TrafficRef{{TrafficID: "1", Note: "must not overwrite"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Bindings) != 3 || again.Version != 1 || again.Bindings[1].Note != "proof" {
		t.Fatalf("duplicate changed bindings: %+v", again)
	}
	other, err := s.Record(ctx, in, []db.TrafficRef{refs[2]})
	if err != nil {
		t.Fatal(err)
	}
	if other.Traffic.Bindings[0].SnapshotID != r.Traffic.Bindings[2].SnapshotID {
		t.Fatal("snapshot was not shared")
	}
	if _, err = s.Traffic.DeleteHost("fixture.local"); err != nil {
		t.Fatal(err)
	}
	for _, binding := range r.Traffic.Bindings {
		if err = s.WithBinding(ctx, r.FindingID, binding.ID, func(b db.FindingTrafficBinding) error {
			f, _, err := s.OpenBody(b.Snapshot, "response")
			if err != nil {
				return err
			}
			defer f.Close()
			got, err := io.ReadAll(f)
			if !bytes.Equal(got, large) {
				t.Fatal("body was truncated/changed")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	v := again.Version
	if _, err = s.DB.SetFindingReportVersionByNodeID(ctx, r.NodeID, "report", &v); err != nil {
		t.Fatal(err)
	}
	note := "updated"
	if err = s.DB.EditFindingTraffic(ctx, r.FindingID, again.Bindings[0].ID, v, nil, &note, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.SetFindingReportVersionByNodeID(ctx, r.NodeID, "stale report", &v); !errors.Is(err, db.ErrEvidenceConflict) {
		t.Fatalf("stale report: %v", err)
	}
	f, err := s.DB.GetFinding(r.FindingID)
	if err != nil {
		t.Fatal(err)
	}
	if f.Report != "report" || f.EvidenceVersion == f.ReportEvidenceVersion || f.TrafficCount != 3 {
		t.Fatalf("finding versions: %+v", f)
	}
	if err = s.DB.EditFindingTraffic(ctx, r.FindingID, again.Bindings[0].ID, v, nil, nil, true, nil); !errors.Is(err, db.ErrEvidenceConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	if _, err = s.DB.SetFindingReportByNodeID(r.NodeID, "legacy report"); err != nil {
		t.Fatal(err)
	}
	f, err = s.DB.GetFinding(r.FindingID)
	if err != nil {
		t.Fatal(err)
	}
	if f.ReportEvidenceVersion != -1 {
		t.Fatal("legacy write claimed current evidence")
	}
	if _, err = s.DB.DeleteFinding(r.FindingID); err != nil {
		t.Fatal(err)
	}
	if err = s.Collect(ctx, time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.WithBinding(ctx, other.FindingID, other.Traffic.Bindings[0].ID, func(b db.FindingTrafficBinding) error {
		f, _, err := s.OpenBody(b.Snapshot, "response")
		if f != nil {
			f.Close()
		}
		return err
	}); err != nil {
		t.Fatal("GC removed shared body:", err)
	}
}

func TestEvidenceAtomicFailuresAndLegacy(t *testing.T) {
	s, in, dir, tag := evidenceFixture(t)
	ctx := context.Background()
	seedExchange(t, s, tag, "valid", []byte("OK"), false)
	counts := func(t *testing.T) (findings, nodes int) {
		t.Helper()
		// 计数查询本身失败也必须判红：吞掉错误会让"零残留"断言永远成立。
		if err := s.DB.QueryRow(`SELECT count(*) FROM findings WHERE task_id=?`, in.TaskID).Scan(&findings); err != nil {
			t.Fatalf("回读 findings 计数失败: %v", err)
		}
		if err := s.DB.QueryRow(`SELECT count(*) FROM exploration_nodes WHERE exploration_id=? AND kind='finding'`, in.ExplorationID).Scan(&nodes); err != nil {
			t.Fatalf("回读 exploration_nodes 计数失败: %v", err)
		}
		return
	}
	for _, kind := range []string{"missing_id", "missing_body", "disk_failure", "database_failure"} {
		t.Run(kind, func(t *testing.T) {
			input := in
			refs := []db.TrafficRef{{TrafficID: "valid"}, {TrafficID: "absent"}}
			local := *s
			switch kind {
			case "missing_body":
				seedExchange(t, s, tag, "broken", []byte("more than preview"), false)
				s.Traffic.DB().Exec(`UPDATE exchange_bodies SET resp_body=? WHERE id='broken'`, []byte("x"))
				refs = []db.TrafficRef{{TrafficID: "valid"}, {TrafficID: "broken"}}
			case "disk_failure":
				local.Dir = filepath.Join(dir, "file-not-dir")
				if err := os.WriteFile(local.Dir, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				refs = refs[:1]
			case "database_failure":
				// 注入一个不存在的资产引用逼停在落库途中失败。PG 时代这条由
				// exploration_anchors 的外键兜住；TiDB 基线不建外键，改由
				// RecordFindingTx 的引用校验（validateAnchorAssetsTx）在同一事务
				// 落第一行之前拒绝 —— 失败点更靠前，但"整笔原子回滚、零残留"的
				// 断言完全同一口径。
				input.AssetIDs = []int64{9223372036854775807}
				refs = refs[:1]
			}
			if _, err := local.Record(ctx, input, refs); err == nil {
				t.Fatal("expected failure")
			}
			if f, n := counts(t); f != 0 || n != 0 {
				t.Fatalf("partial record: findings=%d nodes=%d", f, n)
			}
		})
	}
	legacy := filepath.Join(dir, "traffic", "legacy")
	os.MkdirAll(legacy, 0o700)
	os.WriteFile(filepath.Join(legacy, "request.http"), []byte("GET / HTTP/1.1\nHost: old.local\n"), 0o600)
	os.WriteFile(filepath.Join(legacy, "response.http"), []byte("HTTP 200\n\nlegacy body"), 0o600)
	// url 带本轮 tag（与 seedExchange 同一判据）：这条 legacy 快照才能被精确回读清理。
	_, err := s.Traffic.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path) VALUES('old',1,'old.local','GET','/',?,200,'text/plain',0,11,'legacy')`, "http://old.local/"+tag+"/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Record(ctx, in, []db.TrafficRef{{TrafficID: "old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Record(ctx, in, nil); err != nil {
		t.Fatal("legacy no-traffic report failed:", err)
	}
}

func TestEvidenceConcurrentBindAndDelete(t *testing.T) {
	s, in, _, tag := evidenceFixture(t)
	ctx := context.Background()
	seedExchange(t, s, tag, "race", bytes.Repeat([]byte("large"), 90000), true)
	r, err := s.Record(ctx, in, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Bind(ctx, r.FindingID, []db.TrafficRef{{TrafficID: "race"}})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.DB.GetFindingTraffic(ctx, r.FindingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Bindings) != 1 || list.Version != 1 {
		t.Fatalf("concurrent duplicates: %+v", list)
	}
	// Race a new capture copy with source deletion: either the full copy commits
	// or nothing does. An already-bound snapshot remains readable in both cases.
	wg.Add(2)
	go func() { defer wg.Done(); s.Bind(ctx, r.FindingID, []db.TrafficRef{{TrafficID: "race"}}) }()
	go func() { defer wg.Done(); s.Traffic.DeleteHost("fixture.local") }()
	wg.Wait()
	if err = s.WithBinding(ctx, r.FindingID, list.Bindings[0].ID, func(b db.FindingTrafficBinding) error {
		f, _, err := s.OpenBody(b.Snapshot, "response")
		if f != nil {
			f.Close()
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceGCGraceAndActiveRestore(t *testing.T) {
	s, in, _, tag := evidenceFixture(t)
	ctx := context.Background()
	seedExchange(t, s, tag, "gc", []byte("unique unreferenced gc body"), false)
	f, err := s.Record(ctx, in, []db.TrafficRef{{TrafficID: "gc"}})
	if err != nil {
		t.Fatal(err)
	}
	snap := f.Traffic.Bindings[0].Snapshot
	archive := t.TempDir()
	if err = s.CopySnapshots(ctx, []db.TrafficEvidenceSnapshot{snap}, archive); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.DeleteFinding(f.FindingID); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = s.Collect(ctx, now); err != nil {
		t.Fatal(err)
	}
	path, err := hashPath(s.Dir, snap.RespHash)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Collect(ctx, now.Add(23*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("GC grace ignored", err)
	}
	if err = s.Collect(ctx, now.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("orphan was not collected", err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- s.WithInstalledSnapshots(ctx, []db.TrafficEvidenceSnapshot{snap}, archive, func() error { close(entered); <-release; return nil })
	}()
	<-entered
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err = s.Collect(short, now.Add(72*time.Hour))
	cancel()
	close(release)
	if err == nil {
		t.Fatal("GC entered during active restore")
	}
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("active restore lost body", err)
	}
	// A portable package cannot substitute metadata while retaining its old ID.
	bad := snap
	bad.URL = "http://tampered.local/"
	if err = s.InstallSnapshots(ctx, []db.TrafficEvidenceSnapshot{bad}, archive); err == nil {
		t.Fatal("accepted tampered snapshot")
	}
}
