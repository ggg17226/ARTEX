package db

import (
	"errors"
	"fmt"
	"testing"
)

func TestInterceptExecutionNavigation(t *testing.T) {
	// 统一走 openTestDB：DSN 缺失诚实 Skip、DSN 已配置连不上判红；
	// Close 由其登记为最先一条 t.Cleanup（LIFO 最后执行）并判红，
	// 替换原先 `t.Cleanup(func() { _ = d.Close() })` 的吞错关闭（审查 R-02-R）。
	d := openTestDB(t)
	approval := func(conv int64, task, id string, exact bool) int64 {
		t.Helper()
		audit := &InterceptAudit{ToolUseID: id, Correlation: "exact"}
		if !exact {
			audit.Correlation = "ambiguous"
		}
		n, err := d.CreateInterceptPending(0, conv, task, "display-name-is-not-a-session-id", "Bash", []byte(`{"command":"pwd"}`), "review", audit)
		if err != nil {
			t.Fatal(err)
		}
		// 创建即登记、判红（原 `_, _ =` 吞错）。
		t.Cleanup(func() { mustCleanup(t, d, `DELETE FROM intercept_pending WHERE id=?`, n) })
		return n
	}
	conv := func() int64 {
		t.Helper()
		c, err := d.CreateConversation("test", "navigation", nil)
		if err != nil {
			t.Fatal(err)
		}
		// 原 `_, _ = d.Exec("DELETE FROM conversations…")` 吞错且只删父行，本用例
		// 每个会话写入 200+ 条 conversation_activities 全部残留；改为产品级
		// DeleteConversation 完整闭包（conversation_activities / intercept_pending /
		// 旁路会话…逆拓扑）并判红。
		t.Cleanup(func() {
			if err := d.DeleteConversation(c.ID); err != nil {
				t.Errorf("清理会话 fixture %d 失败: %v", c.ID, err)
			}
		})
		return c.ID
	}
	addConv := func(c int64, kind, id string) int64 {
		t.Helper()
		seq, err := d.AppendConvActivity(c, Activity{Worker: "test", Kind: kind, Tool: "Bash", ToolUseID: id, Summary: "pwd"})
		if err != nil {
			t.Fatal(err)
		}
		return seq
	}
	t.Run("conversation scope and old paginated call", func(t *testing.T) {
		c1, c2 := conv(), conv()
		seq := addConv(c1, "tool_use", "same-id")
		addConv(c1, "tool_result", "same-id")
		addConv(c2, "tool_use", "same-id")
		addConv(c2, "tool_result", "same-id")
		for range 210 {
			addConv(c1, "text", "")
		}
		id := approval(c1, "", "same-id", true)
		got, err := d.GetInterceptExecution(id)
		if err != nil || got.Seq != seq || len(got.Items) != 2 || *got.ConversationID != c1 {
			t.Fatalf("wrong conversation target: %+v %v", got, err)
		}
		addConv(c1, "tool_use", "same-id")
		if _, err := d.GetInterceptExecution(id); !errors.Is(err, ErrInterceptExecutionUnavailable) {
			t.Fatal("duplicate call ID selected an arbitrary execution")
		}
	})
	t.Run("task sessions and result pairing", func(t *testing.T) {
		task, err := d.CreateTask("navigation", "fixture", nil, 0, 600)
		if err != nil {
			t.Fatal(err)
		}
		exp := task.ExplorationID
		// 原 `_, _ = d.Exec(DELETE FROM tasks…)` + `DELETE FROM explorations…` 吞错且
		// 只删两张父表：本用例写入的 intent 节点、task 会话 activity、task_scope
		// 全部残留（审查 R-02-R）。改为产品级 DeleteTask 逆拓扑级联（含防悬空断言）
		// 并判红。
		t.Cleanup(func() { cleanupTask(t, d, task.ID) })
		es := d.Exploration(exp)
		intent, err := es.AddIntent(map[string]any{"summary": "do not parse agent label"}, 1, nil, "planner")
		if err != nil {
			t.Fatal(err)
		}
		seg := 3
		for _, tc := range []struct {
			worker, key string
			node        *int64
			segment     *int
		}{{"work#7", fmt.Sprintf("intent:%d", intent), &intent, nil}, {"planner", "plan", nil, nil}, {"mainagent", "main:3", nil, &seg}, {"mainagent", "main:0", nil, nil}} {
			callID := "call-" + tc.key
			a := Activity{Worker: tc.worker, NodeID: tc.node, MainSeg: tc.segment, Kind: "tool_use", Tool: "Bash", ToolUseID: callID, Summary: "pwd"}
			seq, err := es.AppendActivity(a)
			if err != nil {
				t.Fatal(err)
			}
			a.Kind = "tool_result"
			if _, err := es.AppendActivity(a); err != nil {
				t.Fatal(err)
			}
			got, err := d.GetInterceptExecution(approval(0, fmt.Sprint(task.ID), callID, true))
			if err != nil || got.Seq != seq || got.Session != tc.key || len(got.Items) != 2 {
				t.Fatalf("wrong task session: %+v %v", got, err)
			}
		}
		deletedCall := "deleted-session-call"
		deletedSeq, err := es.AppendActivity(Activity{Worker: "work#9", NodeID: &intent, Kind: "tool_use", Tool: "Bash", ToolUseID: deletedCall})
		if err != nil {
			t.Fatal(err)
		}
		deletedApproval := approval(0, fmt.Sprint(task.ID), deletedCall, true)
		if _, err = d.Exec(`DELETE FROM activity WHERE id=?`, deletedSeq); err != nil {
			t.Fatal(err)
		}
		if _, err = d.GetInterceptExecution(deletedApproval); !errors.Is(err, ErrInterceptSessionDeleted) {
			t.Fatalf("deleted session: %v", err)
		}
		if _, err = d.Exec(`UPDATE tasks SET archived_at=NOW() WHERE id=?`, task.ID); err != nil {
			t.Fatal(err)
		}
		if _, err = d.GetInterceptExecution(deletedApproval); !errors.Is(err, ErrInterceptTaskDeleted) {
			t.Fatalf("archived task: %v", err)
		}
		if _, err = d.Exec(`UPDATE tasks SET archived_at=NULL WHERE id=?`, task.ID); err != nil {
			t.Fatal(err)
		}
		a := Activity{Worker: "work#7", NodeID: &intent, Kind: "tool_use", Tool: "Bash", ToolUseID: "unpaired", Summary: "pwd"}
		if _, err := es.AppendActivity(a); err != nil {
			t.Fatal(err)
		}
		id := approval(0, fmt.Sprint(task.ID), "unpaired", true)
		got, err := d.GetInterceptExecution(id)
		if err != nil || len(got.Items) != 1 {
			t.Fatal("pending call is not navigable")
		}
		a.Kind = "tool_result"
		a.Worker = "different-worker"
		if _, err := es.AppendActivity(a); err != nil {
			t.Fatal(err)
		}
		if _, err := d.GetInterceptExecution(id); !errors.Is(err, ErrInterceptExecutionUnavailable) {
			t.Fatal("cross-worker result was paired")
		}
	})
	if _, err := d.GetInterceptExecution(approval(conv(), "", "missing", true)); !errors.Is(err, ErrInterceptSessionDeleted) {
		t.Fatalf("missing call: %v", err)
	}
	for _, id := range []int64{approval(conv(), "", "", true), approval(conv(), "", "duplicate", false)} {
		if _, err := d.GetInterceptExecution(id); !errors.Is(err, ErrInterceptExecutionUnavailable) {
			t.Fatalf("invented execution for %d", id)
		}
	}
	if got, err := d.GetInterceptExecution(-1); err != nil || got != nil {
		t.Fatal("missing approval")
	}
}
