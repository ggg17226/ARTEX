package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/sidequestion"
	"github.com/Autumn-27/norma/llm"
)

func sideFixture(t *testing.T) (*DB, sidequestion.Snapshot) {
	t.Helper()
	d := openTestDB(t)
	c, err := d.CreateConversation("mainagent", "side persistence", nil)
	if err != nil {
		t.Fatal(err)
	}
	// 会话删除走产品的完整闭包（side_question_requests → side_question_sessions →
	// conversations 逆拓扑）；错误必须判红，吞掉就是往共享库里留旁路子表孤儿。
	t.Cleanup(func() {
		if err := d.DeleteConversation(c.ID); err != nil {
			t.Errorf("删除旁路 fixture 会话 %d 失败: %v", c.ID, err)
		}
	})
	s := sidequestion.Snapshot{Parent: sidequestion.Parent{ConversationID: c.ID}, RunID: 1, Version: 1, CapturedAt: time.Now().UTC(), Model: sidequestion.Model{Model: "fixture"}, Request: llm.CompletionRequest{Messages: []llm.Message{llm.UserText("main-only")}}}
	if err = d.SaveSideSnapshot(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	return d, s
}

func TestSideHistoryIdempotencyPagingAndRecovery(t *testing.T) {
	d, s := sideFixture(t)
	ctx := t.Context()
	first, created, err := d.StartSideRequest(ctx, s, "request-0", "question-0")
	if err != nil || !created {
		t.Fatalf("start %v %v", created, err)
	}
	again, created, err := d.StartSideRequest(ctx, s, "request-0", "question-0")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("dedup %v %v", created, err)
	}
	if _, _, err = d.StartSideRequest(ctx, s, "request-0", "different"); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if _, _, err = d.StartSideRequest(ctx, s, "request-1", "question-1"); !errors.Is(err, ErrSideBusy) {
		t.Fatalf("busy %v", err)
	}
	for i := 0; i < 24; i++ {
		e := first
		if i > 0 {
			e, _, err = d.StartSideRequest(ctx, s, fmt.Sprintf("request-%d", i), fmt.Sprintf("question-%d", i))
			if err != nil {
				t.Fatal(err)
			}
		}
		e.Answer = fmt.Sprintf("answer-%d", i)
		e.Sequence = 1
		e.Status = "completed"
		if ok, err := d.UpdateSideRequest(ctx, *e); err != nil || !ok {
			t.Fatalf("finish %v %v", ok, err)
		}
	}
	page, err := d.SideHistory(ctx, s.Parent.Key(), 0, 20)
	if err != nil || len(page) != 20 {
		t.Fatalf("page %d %v", len(page), err)
	}
	tail, err := d.SideHistory(ctx, s.Parent.Key(), page[19].Ordinal, 20)
	if err != nil || len(tail) != 4 || tail[0].Ordinal >= page[19].Ordinal {
		t.Fatalf("tail %+v %v", tail, err)
	}
	replay, err := d.SideReplay(ctx, s.Parent.Key())
	if err != nil || len(replay) != 20 || replay[0].Question != "question-4" || replay[19].Question != "question-23" {
		t.Fatalf("replay %+v %v", replay, err)
	}
	e, _, err := d.StartSideRequest(ctx, s, "unfinished", "partial question")
	if err != nil {
		t.Fatal(err)
	}
	e.Answer = "saved partial"
	e.Sequence = 1
	e.Usage.InputTokens = 17
	if _, err = d.UpdateSideRequest(ctx, *e); err != nil {
		t.Fatal(err)
	}
	if err = d.InterruptSideRequests(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := d.SideRequest(ctx, e.ID)
	if err != nil || got.Status != "interrupted" || got.Answer != "saved partial" || got.Usage.InputTokens != 17 {
		t.Fatalf("recovery %+v %v", got, err)
	}
	saved, err := d.SideSnapshot(ctx, s.Parent.Key())
	if err != nil || saved.Request.Messages[0].Text() != "main-only" {
		t.Fatalf("snapshot %+v %v", saved, err)
	}
	if _, _, err = d.StartSideRequest(ctx, *saved, "after-restart", "continue"); err != nil {
		t.Fatal(err)
	}
}

func TestSideMemoryPagingClearAndRestart(t *testing.T) {
	d, s := sideFixture(t)
	ctx := t.Context()
	var ordinal int64
	for i := 0; i < 50; i++ {
		e, _, err := d.StartSideRequest(ctx, s, fmt.Sprint(i), "history")
		if err != nil {
			t.Fatal(err)
		}
		e.Status = "completed"
		e.Sequence = 1
		e.Answer = "saved"
		if i == 2 {
			e.Status = "failed"
		}
		if _, err = d.UpdateSideRequest(ctx, *e); err != nil {
			t.Fatal(err)
		}
		if i == 29 {
			ordinal = e.Ordinal
		}
	}
	e, _, err := d.StartSideRequest(ctx, s, "admitted", "question")
	if err != nil {
		t.Fatal(err)
	}
	memory := sidequestion.Memory{History: "old decision", Through: ordinal, SnapshotKey: "snapshot", SnapshotSummary: "main evidence", TailStart: 3}
	if err = d.SaveSideMemory(ctx, *e, memory); err != nil {
		t.Fatal(err)
	}
	var all []sidequestion.Exchange
	for after := int64(0); ; {
		page, err := d.SideReplayPage(ctx, *e, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) > 20 {
			t.Fatal("unbounded page")
		}
		all = append(all, page...)
		after = page[len(page)-1].Ordinal
	}
	if len(all) != 49 {
		t.Fatalf("history missing/duplicated: %d", len(all))
	}
	if err = d.InterruptSideRequests(ctx); err != nil {
		t.Fatal(err)
	}
	next, _, err := d.StartSideRequest(ctx, s, "restart", "continue")
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.SideMemory(ctx, *next)
	if err != nil || got != memory {
		t.Fatalf("memory after restart: %+v %v", got, err)
	}
	page, err := d.SideReplayPage(ctx, *next, memory.Through)
	if err != nil || len(page) != 20 || page[0].Ordinal <= ordinal {
		t.Fatalf("summary cursor: %+v %v", page, err)
	}
	if err = d.ClearSideHistory(ctx, s.Parent.Key()); err != nil {
		t.Fatal(err)
	}
	if err = d.SaveSideMemory(ctx, *next, memory); !errors.Is(err, ErrSideParentGone) {
		t.Fatalf("late memory resurrected: %v", err)
	}
	fresh, _, err := d.StartSideRequest(ctx, s, "after-clear", "fresh")
	if err != nil {
		t.Fatal(err)
	}
	got, err = d.SideMemory(ctx, *fresh)
	if err != nil || got != (sidequestion.Memory{}) {
		t.Fatalf("memory survived clear: %+v %v", got, err)
	}
	if snapshot, err := d.SideSnapshot(ctx, s.Parent.Key()); err != nil || snapshot.Request.Messages[0].Text() != "main-only" {
		t.Fatal("memory changed main snapshot")
	}
}

func TestSideMemoryClearRace(t *testing.T) {
	d, s := sideFixture(t)
	for i := 0; i < 10; i++ {
		e, _, err := d.StartSideRequest(t.Context(), s, "race", "question")
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := d.ClearSideHistory(t.Context(), s.Parent.Key()); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			err := d.SaveSideMemory(t.Context(), *e, sidequestion.Memory{History: "late", Through: e.Ordinal - 1})
			if err != nil && !errors.Is(err, ErrSideParentGone) {
				t.Error(err)
			}
		}()
		wg.Wait()
		var raw []byte
		if err = d.QueryRow(`SELECT memory FROM side_question_sessions WHERE session_key=?`, s.Parent.Key()).Scan(&raw); err != nil || string(raw) != "{}" {
			t.Fatalf("late cache write: %s %v", raw, err)
		}
	}
}

// TestSideArchiveRowsWithoutNewFields 钉住 v3 那批老冷包"整列还没有 memory /
// context_info"的历史兼容恢复路径（审查 I-04）。三条都必须成立：
//
//   - 冷包里是真的**没有**这一列（用例把它从投影结果里删掉，而不是自己再塞一个 "{}"
//     再喂回恢复层 —— 那样等于把被测行为写进测试里，恢复层什么都不做也能绿）；
//   - 补齐发生在**产品的恢复入口**（restoreSideQuestionSessions /
//     restoreSideQuestionRequests）里：PG 时代靠 `ADD COLUMN ... DEFAULT '{}'` 的列
//     默认值，TiDB 的 MEDIUMTEXT 不允许列默认值（Error 1101），缺列直接 1364，
//     所以默认值必须由 archiveLegacyCompatColumns 显式补回；
//   - 冷包带着原值回来时，原值必须**原样保住**，不能被默认值覆盖。
//
// 白名单之外的必填列依旧 fail-closed，见 TestArchiveRestoreRejectsMissingRequiredColumn。
func TestSideArchiveRowsWithoutNewFields(t *testing.T) {
	d, s := sideFixture(t)
	ctx := t.Context()
	e, _, err := d.StartSideRequest(ctx, s, "legacy", "legacy question")
	if err != nil {
		t.Fatal(err)
	}
	key := s.Parent.Key()
	const (
		legacyMemory  = `{"summary":"legacy memory"}`
		legacyContext = `{"budget":1234}`
	)
	// 热表里先把两个新字段写成**非默认值**：缺列要补 '{}'，带原值不能被覆盖，
	// 两个方向都钉住才算真的恢复了这两个字段。
	if _, err = d.ExecContext(ctx, `UPDATE side_question_sessions SET memory=? WHERE session_key=?`, legacyMemory, key); err != nil {
		t.Fatal(err)
	}
	if _, err = d.ExecContext(ctx, `UPDATE side_question_requests SET context_info=? WHERE id=?`, legacyContext, e.ID); err != nil {
		t.Fatal(err)
	}

	// export 借生产自己的冷包投影导出（形状与 manifest 完全一致），并按场景把指定列
	// 整列删掉，模拟"这个归档包导出时还没有这一列"。
	export := func(t *testing.T, tx *sql.Tx, table string, drop ...string) json.RawMessage {
		t.Helper()
		raw, _, err := archiveSelectProjection(tx, table, "session_key = ?", []any{key}, "")
		if err != nil {
			t.Fatalf("export %s: %v", table, err)
		}
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			t.Fatalf("decode %s: %v", table, err)
		}
		if len(items) != 1 {
			t.Fatalf("%s fixture rows=%d want 1", table, len(items))
		}
		for _, column := range drop {
			if _, ok := items[0][column]; !ok {
				t.Fatalf("%s 的投影里没有 %s 列，用例前提不成立", table, column)
			}
			delete(items[0], column)
		}
		out, err := json.Marshal(items)
		if err != nil {
			t.Fatalf("encode %s: %v", table, err)
		}
		return out
	}

	for _, tc := range []struct {
		name        string
		dropMemory  bool
		dropContext bool
		wantMemory  string
		wantContext string
	}{
		{name: "只缺 memory", dropMemory: true, wantMemory: "{}", wantContext: legacyContext},
		{name: "只缺 context_info", dropContext: true, wantMemory: legacyMemory, wantContext: "{}"},
		{name: "两列都缺", dropMemory: true, dropContext: true, wantMemory: "{}", wantContext: "{}"},
		{name: "两列都带原值", wantMemory: legacyMemory, wantContext: legacyContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 每个场景一个独立事务、收尾 Rollback：热表里的 fixture 原行始终留在库里，
			// 下一个场景仍从同一份数据出发，且整件事在共享库上不留任何痕迹。
			tx, err := d.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback() //nolint:errcheck // 回滚是预期收尾

			var sessionDrop, requestDrop []string
			if tc.dropMemory {
				sessionDrop = append(sessionDrop, "memory")
			}
			if tc.dropContext {
				requestDrop = append(requestDrop, "context_info")
			}
			sessions := export(t, tx, "side_question_sessions", sessionDrop...)
			requests := export(t, tx, "side_question_requests", requestDrop...)

			// 回灌前要清掉热表原行（否则撞 uq_side_requests_id / 会话唯一键）。
			// 逆拓扑：先子表 side_question_requests，再父表 side_question_sessions。
			if _, err = tx.ExecContext(ctx, `DELETE FROM side_question_requests WHERE session_key=?`, key); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM side_question_sessions WHERE session_key=?`, key); err != nil {
				t.Fatal(err)
			}
			if _, err := restoreSideQuestionSessions(ctx, tx, sessions); err != nil {
				t.Fatalf("restore side_question_sessions: %v", err)
			}
			if err := restoreSideQuestionRequests(ctx, tx, requests); err != nil {
				t.Fatalf("restore side_question_requests: %v", err)
			}

			var memory, info string
			if err := tx.QueryRowContext(ctx, `
SELECT s.memory, r.context_info
FROM side_question_sessions s JOIN side_question_requests r USING(session_key)
WHERE r.id=?`, e.ID).Scan(&memory, &info); err != nil {
				t.Fatal(err)
			}
			if memory != tc.wantMemory || info != tc.wantContext {
				t.Fatalf("memory=%s context_info=%s；want memory=%s context_info=%s",
					memory, info, tc.wantMemory, tc.wantContext)
			}
		})
	}
}

// TestArchiveRestoreRejectsMissingRequiredColumn 与上面那条成对：历史兼容默认值只覆盖
// archiveLegacyCompatColumns 登记在案的列，其他必填列在冷包里整列缺席时必须照旧
// fail-closed（TiDB 严格模式 1364），绝不能被"顺手补个默认值"悄悄蒙过去。
func TestArchiveRestoreRejectsMissingRequiredColumn(t *testing.T) {
	d, s := sideFixture(t)
	ctx := t.Context()
	if _, _, err := d.StartSideRequest(ctx, s, "required-column", "question"); err != nil {
		t.Fatal(err)
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck
	raw, _, err := archiveSelectProjection(tx, "side_question_sessions", "session_key = ?", []any{s.Parent.Key()}, "")
	if err != nil {
		t.Fatal(err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("fixture rows=%d want 1", len(items))
	}
	// run_id 是 BIGINT NOT NULL 且没有应用层默认值，也不在历史兼容白名单里。
	if _, ok := items[0]["run_id"]; !ok {
		t.Fatal("投影里没有 run_id 列，用例前提不成立")
	}
	delete(items[0], "run_id")
	patched, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM side_question_requests WHERE session_key=?`, s.Parent.Key()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM side_question_sessions WHERE session_key=?`, s.Parent.Key()); err != nil {
		t.Fatal(err)
	}
	_, err = restoreSideQuestionSessions(ctx, tx, patched)
	if err == nil {
		t.Fatal("缺失非白名单必填列却恢复成功，fail-closed 失守")
	}
	if !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("失败原因不是缺失的 run_id 列（等于没有断到点上）: %v", err)
	}
}

func TestSideClearLateWritersAndDeletedParent(t *testing.T) {
	d, s := sideFixture(t)
	ctx := t.Context()
	for i := 0; i < 10; i++ {
		e, _, err := d.StartSideRequest(ctx, s, "same-client", "question")
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := d.ClearSideHistory(ctx, s.Parent.Key()); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			copy := *e
			copy.Sequence = 1
			copy.Answer = "late"
			copy.Status = "completed"
			if _, err := d.UpdateSideRequest(ctx, copy); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		if row, err := d.SideRequest(ctx, e.ID); err != nil || row != nil {
			t.Fatalf("cleared answer resurrected: %+v %v", row, err)
		}
		e.Sequence = 2
		e.Status = "completed"
		if ok, err := d.UpdateSideRequest(ctx, *e); err != nil || ok {
			t.Fatalf("late update %v %v", ok, err)
		}
	}
	s.Version = 3
	if err := d.SaveSideSnapshot(ctx, s); err != nil {
		t.Fatal(err)
	}
	s.Version = 2
	if err := d.SaveSideSnapshot(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := d.SideSnapshot(ctx, s.Parent.Key())
	if err != nil || got.Version != 3 {
		t.Fatalf("older version won: %+v %v", got, err)
	}
	if err = d.DeleteConversation(s.Parent.ConversationID); err != nil {
		t.Fatal(err)
	}
	if err = d.SaveSideSnapshot(ctx, s); !errors.Is(err, ErrSideParentGone) {
		t.Fatalf("deleted parent restored: %v", err)
	}
	if got, err = d.SideSnapshot(ctx, s.Parent.Key()); err != nil || got != nil {
		t.Fatalf("delete cascade: %+v %v", got, err)
	}
}

func TestSideTaskArchiveVersions(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			d := openTestDB(t)
			if err := d.EnsureLLMRecordsTable(); err != nil {
				t.Fatal(err)
			}
			if err := d.EnsureLLMUsageTable(); err != nil {
				t.Fatal(err)
			}
			task, err := d.CreateTask("btw archive", "restore context", nil, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			// 逆拓扑 + 检查错误：冷包行先删，再走产品的任务级联删除。
			t.Cleanup(func() {
				mustCleanup(t, d, `DELETE FROM task_archives WHERE task_id=?`, task.ID)
				if err := d.DeleteTask(task.ID); err != nil {
					t.Errorf("删除 fixture 任务 %d 失败: %v", task.ID, err)
				}
			})
			iid, err := d.Exploration(task.ExplorationID).AddNode(KindIntent, map[string]any{"summary": "worker"}, 1, "paused", "planner", nil)
			if err != nil {
				t.Fatal(err)
			}
			var snapshots []sidequestion.Snapshot
			memories := make(map[string]sidequestion.Memory)
			contextInfo := sidequestion.ContextInfo{Phase: "answering", RecentExchanges: 20, HistorySummarized: true, SnapshotSummarized: true, EstimatedInputTokens: 12000, InputBudget: 16000, OutputTokens: 2048}
			for _, intent := range []int64{0, iid} {
				s := sidequestion.Snapshot{Parent: sidequestion.Parent{TaskID: task.ID, ExplorationID: task.ExplorationID, IntentID: intent}, RunID: 1, Version: 2, CapturedAt: time.Now().UTC(), Request: llm.CompletionRequest{Messages: []llm.Message{llm.UserText("archived main context")}}}
				if err = d.SaveSideSnapshot(t.Context(), s); err != nil {
					t.Fatal(err)
				}
				e, _, err := d.StartSideRequest(t.Context(), s, "client", "archive question")
				if err != nil {
					t.Fatal(err)
				}
				memory := sidequestion.Memory{History: "archived early decision", Through: e.Ordinal, SnapshotKey: s.Parent.Key(), SnapshotSummary: "archived evidence", TailStart: 1}
				if err = d.SaveSideMemory(t.Context(), *e, memory); err != nil {
					t.Fatal(err)
				}
				memories[s.Parent.Key()] = memory
				e.Answer = "archive answer"
				e.Sequence = 1
				e.Status = "completed"
				e.Context = contextInfo
				if _, err = d.UpdateSideRequest(t.Context(), *e); err != nil {
					t.Fatal(err)
				}
				snapshots = append(snapshots, s)
			}
			if err := d.SetPaused(task.ID, true); err != nil {
				t.Fatal(err)
			}
			job, err := d.QueueTaskArchive(task.ID)
			if err != nil {
				t.Fatal(err)
			}
			// 只领取自己那条记录：全局 FIFO 领取会先把**别人的**队首改成活跃态（RR-I03）。
			claimOwnArchiveJob(t, t.Context(), d, job.ID, ArchiveQueued, Archiving)
			archive, err := d.SnapshotTaskArchive(task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if archive.FormatVersion != 3 || archive.DataCounts["side_question_sessions"] != 2 || archive.DataCounts["side_question_requests"] != 2 {
				t.Fatalf("missing side archive: %+v", archive.DataCounts)
			}
			if err = d.CompleteTaskArchive(job.ID, archive, "/tmp/side-fixture.tar.zst", "fixture", 1, 1); err != nil {
				t.Fatal(err)
			}
			if err = d.SaveSideSnapshot(t.Context(), snapshots[0]); !errors.Is(err, ErrSideParentGone) {
				t.Fatalf("late archived snapshot: %v", err)
			}
			if got, err := d.SideSnapshot(t.Context(), snapshots[0].Parent.Key()); err != nil || got != nil {
				t.Fatalf("archive retained hot snapshot %+v %v", got, err)
			}
			if _, err = d.QueueTaskArchiveRestore(job.ID); err != nil {
				t.Fatal(err)
			}
			claimOwnArchiveJob(t, t.Context(), d, job.ID, RestoreQueued, Restoring)
			archive.FormatVersion = version
			if version < 3 {
				delete(archive.Tables, "side_question_sessions")
				delete(archive.Tables, "side_question_requests")
				delete(archive.DataCounts, "side_question_sessions")
				delete(archive.DataCounts, "side_question_requests")
			}
			if _, err = d.RestoreTaskArchive(job.ID, archive, 0); err != nil {
				t.Fatal(err)
			}
			for _, s := range snapshots {
				got, err := d.SideSnapshot(context.Background(), s.Parent.Key())
				if err != nil {
					t.Fatal(err)
				}
				if version < 3 {
					if got != nil {
						t.Fatal("legacy archive fabricated snapshot")
					}
				} else {
					if got == nil || got.Request.Messages[0].Text() != "archived main context" {
						t.Fatalf("restored snapshot: %+v", got)
					}
					history, err := d.SideHistory(t.Context(), s.Parent.Key(), 0, 20)
					if err != nil || len(history) != 1 || history[0].Answer != "archive answer" {
						t.Fatalf("restored history %+v %v", history, err)
					}
					if history[0].Context != contextInfo {
						t.Fatalf("restored context metadata: %+v", history[0].Context)
					}
					next, _, err := d.StartSideRequest(t.Context(), *got, "after-restore", "continue")
					if err != nil {
						t.Fatal(err)
					}
					memory, err := d.SideMemory(t.Context(), *next)
					if err != nil || memory != memories[s.Parent.Key()] {
						t.Fatalf("restored summary cache: %+v %v", memory, err)
					}
				}
			}
		})
	}
}
