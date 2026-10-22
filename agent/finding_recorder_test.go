package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/evidence"
)

type failingFindingRecorder struct{}

func (failingFindingRecorder) Record(context.Context, db.RecordFindingInput, []db.TrafficRef) (*db.RecordedFinding, error) {
	return nil, errors.New("fixture persistence failed")
}

func TestReportFindingOptionalTrafficWithoutCapture(t *testing.T) {
	d := testDB(t) // 连接池由 testDB 统一在 t.Cleanup 关闭，勿再 defer d.Close()
	task, err := d.CreateTask("TCP evidence", "fixture", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 清理钩子先登记、闭包在测试结束时才读取 recorded：addFinding 落下的 findings 行
	// task_id 为空（RecordFindingTx 的 NULLIF(?,0)），DeleteTask 级联带不走它们，必须
	// 按本轮真实主键逐条删除并回读归零；静默失败的历史版本把 3 条 TCP fixture 漏在了
	// 共享开发库里。
	var recorded []int64
	t.Cleanup(func() {
		for _, id := range recorded {
			if _, err := d.DeleteFinding(id); err != nil {
				t.Errorf("清理 finding fixture %d 失败: %v", id, err)
			}
		}
		if left := countRecordedFindings(t, d, recorded); left != 0 {
			t.Errorf("finding fixture 残留 %d 行", left)
		}
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("清理任务 fixture %d 失败: %v", task.ID, err)
		}
	})
	ts := NewToolSet(nil, "")
	ts.ts, ts.taskID = d.Exploration(task.ExplorationID), task.ID
	ts.SetFindingRecorder(evidence.New(d, nil, t.TempDir()))
	notices := 0
	ts.notifyFinding = func(int64, string) { notices++ }
	for _, refs := range []string{"", `,"traffic_refs":[]`, `,"traffic_refs":null`} {
		res, err := ts.addFinding().Call(t.Context(), json.RawMessage(`{"vulnclass":"TCP","severity":"low","summary":"verified TCP fixture","evidence":"command output proves the finding"`+refs+`}`), nil)
		if err != nil || !strings.HasPrefix(res.Flatten(), "finding recorded: ") {
			t.Fatalf("optional traffic rejected: %v %s", err, res.Flatten())
		}
		var out db.RecordedFinding
		if err := json.Unmarshal([]byte(strings.SplitN(res.Flatten(), "\n", 2)[1]), &out); err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, out.FindingID)
		f, err := d.GetFinding(out.FindingID)
		if err != nil || f == nil || len(out.Traffic.Bindings) != 0 || f.Evidence != "command output proves the finding" {
			t.Fatalf("lost non-HTTP evidence: %+v %v", f, err)
		}
	}
	if notices != 3 {
		t.Fatalf("successful reports notified %d times", notices)
	}
}

func TestReportFindingAtomicContract(t *testing.T) {
	old := FindingTrafficBindingEnabled
	FindingTrafficBindingEnabled = func() bool { return true }
	t.Cleanup(func() { FindingTrafficBindingEnabled = old })
	d := testDB(t) // 连接池由 testDB 统一在 t.Cleanup 关闭，勿再 defer d.Close()
	task, err := d.CreateTask("report contract", "fixture", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 同上一个用例：成功上报的那条 findings 行要靠本轮主键精确回收，失败必须标红。
	var recorded []int64
	t.Cleanup(func() {
		for _, id := range recorded {
			if _, err := d.DeleteFinding(id); err != nil {
				t.Errorf("清理 finding fixture %d 失败: %v", id, err)
			}
		}
		if left := countRecordedFindings(t, d, recorded); left != 0 {
			t.Errorf("finding fixture 残留 %d 行", left)
		}
		if err := d.DeleteTask(task.ID); err != nil {
			t.Errorf("清理任务 fixture %d 失败: %v", task.ID, err)
		}
	})
	ts := NewToolSet(nil, "")
	ts.ts = d.Exploration(task.ExplorationID)
	ts.taskID = task.ID
	ts.worker = "fixture"
	notices := 0
	ts.notifyFinding = func(int64, string) { notices++ }
	call := func(body string) string {
		t.Helper()
		res, err := ts.addFinding().Call(t.Context(), json.RawMessage(body), nil)
		if err != nil {
			t.Fatal(err)
		}
		return res.Flatten()
	}
	ts.SetFindingRecorder(failingFindingRecorder{})
	if got := call(`{"vulnclass":"TEST","summary":"fail","severity":"low","traffic_refs":[{"traffic_id":"x"}]}`); !strings.Contains(got, "persistence failed") {
		t.Fatal(got)
	}
	if notices != 0 || ts.writes.Findings != 0 {
		t.Fatal("notified before commit")
	}
	ts.SetFindingRecorder(nil)
	got := call(`{"vulnclass":"TEST","summary":"legacy report","severity":"low"}`)
	lines := strings.SplitN(got, "\n", 2)
	if len(lines) != 2 {
		t.Fatal(got)
	}
	var out db.RecordedFinding
	if err = json.Unmarshal([]byte(lines[1]), &out); err != nil {
		t.Fatal(err)
	}
	recorded = append(recorded, out.FindingID)
	if lines[0] != fmt.Sprintf("finding recorded: %d", out.NodeID) || out.FindingID <= 0 || notices != 1 {
		t.Fatal(got)
	}
	f, err := d.GetFinding(out.FindingID)
	if err != nil || f == nil || f.NodeID == nil || *f.NodeID != out.NodeID {
		t.Fatalf("wrong finding/node mapping: %+v %v", f, err)
	}
	if got = call(`{"vulnclass":"TEST","summary":"no storage","severity":"low","traffic_refs":[{"traffic_id":"x"}]}`); !strings.Contains(got, "未登记") {
		t.Fatal(got)
	}
	if notices != 1 {
		t.Fatal("failed tool triggered reporter")
	}
}

// countRecordedFindings 回读本轮登记的 findings 主键在库里还剩几行（空集合直接 0）。
// 清理钩子用它把"确实删干净了"变成硬断言，而不是相信 DeleteFinding 的返回行数。
func countRecordedFindings(t *testing.T, d *db.DB, ids []int64) int {
	t.Helper()
	if len(ids) == 0 {
		return 0
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := d.Query(`SELECT id FROM findings WHERE id IN (`+strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", ")+`)`, args...)
	if err != nil {
		t.Errorf("回读 findings 残留失败: %v", err)
		return 0
	}
	defer rows.Close()
	var left int
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Errorf("扫描 findings 残留失败: %v", err)
			return 0
		}
		left++
	}
	if err := rows.Err(); err != nil {
		t.Errorf("回读 findings 残留失败: %v", err)
	}
	return left
}
