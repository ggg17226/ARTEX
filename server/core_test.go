package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
)

// TestCoreTaskLifecyclePG exercises the migrated core (tasks/exploration on PG)
// through the real HTTP mux: create → goal nodes seeded → list → delete cascade.
func TestCoreTaskLifecyclePG(t *testing.T) {
	m := newTestManager(t)
	td := t.TempDir()
	s := New(context.Background(), m, td, td, td)
	h := s.Handler()
	tok, err := signJWT(s.jwtKey)
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}

	do := func(method, path string, body any) (int, map[string]any) {
		var r *http.Request
		if body != nil {
			b, _ := json.Marshal(body)
			r = httptest.NewRequest(method, path, bytes.NewReader(b))
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		r.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		var out map[string]any
		json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}

	// doRetry retries on 5xx (transient DB conflicts from parallel test packages).
	doRetry := func(method, path string, body any) (int, map[string]any) {
		var code int
		var out map[string]any
		for i := 0; i < 5; i++ {
			code, out = do(method, path, body)
			if code < 500 {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		return code, out
	}

	// create a task → 201, returns PG task (string id + exploration_id)
	code, out := doRetry("POST", "/api/tasks", map[string]string{"description": "smoke", "goal": "测试 SQLi/XSS"})
	if code != 201 {
		t.Fatalf("create task: %d (%v)", code, out)
	}
	id, _ := out["id"].(string)
	// 迁移后的 wire 契约：整型 id 一律以十进制字符串出（见 dto.i64s）。
	expIDRaw, ok := out["exploration_id"].(string)
	if !ok {
		t.Fatalf("exploration_id 必须是字符串 id，实际 %T (%v)", out["exploration_id"], out["exploration_id"])
	}
	expID, parseErr := strconv.ParseInt(expIDRaw, 10, 64)
	if parseErr != nil {
		t.Fatalf("解析 exploration_id %q: %v", expIDRaw, parseErr)
	}
	if id == "" || expID == 0 {
		t.Fatalf("bad task payload: %v", out)
	}
	// 兜底清理：本用例任何一步 t.Fatal 都必须把这个 task 连子图一起删掉，
	// 否则共享库里会留下没人认领的 tasks/explorations/exploration_nodes 行。
	// t.Cleanup 是 LIFO，这里最先登记 → 最后执行，仍在活的连接池上。
	nid, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil {
		t.Fatalf("解析 task id %q: %v", id, parseErr)
	}
	t.Cleanup(func() {
		if _, err := m.DeleteTask(id, DeleteTaskOptions{
			DeleteAssets: true, DeleteFindings: true, DeleteLLMRecords: true, DeleteFiles: true,
		}); err != nil {
			t.Errorf("兜底删除任务 %s 失败: %v", id, err)
		}
		assertZero(t, m.pg, `SELECT COUNT(*) FROM tasks WHERE id=?`, nid)
		assertZero(t, m.pg, `SELECT COUNT(*) FROM exploration_nodes WHERE exploration_id=?`, expID)
	})

	// the exploration owns goal node(s) — goal seeding is async, poll briefly
	var goals []*db.Node
	for i := 0; i < 30; i++ {
		goals, err = m.pg.Exploration(expID).ListByKind("goal", 10)
		if err != nil || len(goals) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(goals) < 1 {
		t.Fatalf("expected goal nodes seeded, got %d", len(goals))
	}

	// it shows in the task list
	code, out = doRetry("GET", "/api/tasks", nil)
	if code != 200 {
		t.Fatalf("list tasks: %d", code)
	}

	// The task detail header consumes the top-level engine mode while some clients
	// read the active-task snapshot. Keep both representations in sync.
	code, out = doRetry("GET", "/api/stats?task="+id, nil)
	if code != 200 {
		t.Fatalf("task stats: %d (%v)", code, out)
	}
	activeTask, ok := out["active_task"].(map[string]any)
	if !ok || activeTask["engine_mode"] != out["engine_mode"] {
		t.Fatalf("engine mode mismatch: top=%v active_task=%v", out["engine_mode"], activeTask)
	}

	// delete → cascade removes the exploration subgraph and selected related data
	taskDir := filepath.Join(m.dir, "tasks", id)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "artifact.txt"), []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	transcriptBase := "exp" + strconv.FormatInt(expID, 10) + "-main"
	transcriptPath := filepath.Join(m.dir, "transcripts", transcriptBase+".jsonl")
	sidechainPath := filepath.Join(m.dir, "transcripts", transcriptBase)
	if err := os.MkdirAll(filepath.Join(sidechainPath, "subagents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(transcriptPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sidechainPath, "subagents", "child.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	findingID, err := m.pg.AddFinding(nid, 0, "__core_task_delete__", "", db.SeverityHigh, "summary", "evidence", "tester", nil)
	if err != nil {
		t.Fatal(err)
	}
	// llm_records 由 65 表基线目录 + schema.VerifyBaseline 保障，启动期建表调用已随 M12 删除。
	if err := m.pg.InsertLLMRecord(&db.LLMRecord{TaskID: id, SessionID: "delete-test", Status: "ok", RequestBody: "secret"}); err != nil {
		t.Fatal(err)
	}
	// A non-canonical path still resolves to the one canonical task key for the
	// barrier, workspace, in-memory registry and DB delete.
	code, out = doRetry("DELETE", "/api/tasks/000"+id, map[string]bool{
		"delete_files":       true,
		"delete_findings":    true,
		"delete_llm_records": true,
	})
	if code != 200 {
		t.Fatalf("delete task: %d", code)
	}
	if deleted, _ := out["files_deleted"].(bool); !deleted {
		t.Fatalf("expected files_deleted response, got %v", out)
	}
	if deleted, _ := out["findings_deleted"].(float64); deleted != 1 {
		t.Fatalf("expected one deleted finding, got %v", out)
	}
	if deleted, _ := out["llm_records_deleted"].(float64); deleted != 1 {
		t.Fatalf("expected one deleted LLM record, got %v", out)
	}
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Fatalf("task directory should be deleted, stat err=%v", err)
	}
	for _, path := range []string{transcriptPath, sidechainPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("task transcript should be deleted (%s), stat err=%v", path, err)
		}
	}
	if got, _ := m.pg.GetTask(nid); got != nil {
		t.Fatalf("task should be deleted from PG")
	}
	if finding, err := m.pg.GetFinding(findingID); err != nil || finding != nil {
		t.Fatalf("finding should be deleted, got finding=%+v err=%v", finding, err)
	}
	var llmRecords int
	if err := m.pg.QueryRow(`SELECT count(*) FROM llm_records WHERE task_id=?`, id).Scan(&llmRecords); err != nil || llmRecords != 0 {
		t.Fatalf("task LLM records should be deleted, count=%d err=%v", llmRecords, err)
	}
	var n int
	m.pg.QueryRow(`SELECT count(*) FROM exploration_nodes WHERE exploration_id=?`, expID).Scan(&n)
	if n != 0 {
		t.Fatalf("exploration nodes should be cascade-deleted, got %d", n)
	}
}
