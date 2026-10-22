package db

// =====================================================================
// P08-04 Task 4.3 —— 辅助模块方言改造与彻底切除 pgx（M13 / M17 / M20 / M28 / M31）
//
// 三层守卫，缺一层都会漏：
//  1. 源码级守卫（TestAuxModules_SourceIsTiDBDialectOnly /
//     TestPgxDependencyIsFullyRemoved）：连"写出来但当前没有调用到"的 PG 片段与
//     `github.com/jackc/pgx` 依赖都要抓到，是 201 处 Blocker 里本任务 65 处与
//     pgx 依赖清零的唯一判据；
//  2. 语句级替身（复用 exploration_test.go 的 expMock* 驱动）：断言真实经过
//     database/sql 的语句形状 —— 占位符一律 `?` 且与参数逐位对齐、自增主键一律
//     LastInsertId、scheduler_state/side_question_requests 补齐 identity/hash、
//     旁路快照在锁内做元组比较防版本倒退、MySQL changed-rows 语义下的 0 行复核、
//     以及 DeleteConversation 的完整闭包顺序与 Retest 停止/置空语义；
//  3. TiDB 真机集成（ARTEX_TIDB_DSN 未设置时显式 t.Skip，绝不当作通过）。
// =====================================================================

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/sidequestion"

	// SQLite（纯 Go，已在 go.mod 里，traffic 包生产使用）只用于**离线语义探针**：
	// 把"真实闭包 + 真实级联执行器"发出的 SQL 打进一个内存库，验证 SQL 的实际语义
	// （审查 F03：并发完成的终态是否会被覆写）。它不是 TiDB，也不替代真机门禁，
	// 只在没有 DSN 的环境里给出可判定的语义证据。
	_ "modernc.org/sqlite"
)

// =====================================================================
// 1) 计划 Step 1 —— TiDB 真机完整会话闭包删除与 Retest 语义
// =====================================================================

func TestConversation_DeleteConversation_FullClosureAndRetestSemantics(t *testing.T) {
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

	// 1. 创建 conversation 根
	cTitle := fmt.Sprintf("P08 Conv %d", time.Now().UnixNano())
	conv, err := d.CreateConversation("test_agent", cTitle, nil)
	if err != nil {
		t.Fatalf("CreateConversation failed: %v", err)
	}
	convKey := fmt.Sprintf("conv-%d", conv.ID)

	// 注册 conversation 根及其级联子树的清理
	t.Cleanup(func() {
		var exists bool
		queryErr := d.QueryRow("SELECT true FROM conversations WHERE id = ?", conv.ID).Scan(&exists)
		if queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
			t.Errorf("query conversation residual state failed: %v", queryErr)
		} else if queryErr == nil && exists {
			if err := d.DeleteConversation(conv.ID); err != nil {
				t.Errorf("cleanup conversation failed: %v", err)
			}
		}
		if _, err := d.Exec("DELETE FROM side_question_requests WHERE session_key = ?", convKey); err != nil {
			t.Errorf("cleanup residual side_question_requests failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM side_question_sessions WHERE conversation_id = ?", conv.ID); err != nil {
			t.Errorf("cleanup residual side_question_sessions failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM intercept_pending WHERE conversation_id = ?", conv.ID); err != nil {
			t.Errorf("cleanup residual intercept_pending failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM conversation_activities WHERE conversation_id = ?", conv.ID); err != nil {
			t.Errorf("cleanup residual conversation_activities failed: %v", err)
		}
		if _, err := d.Exec("DELETE FROM conversations WHERE id = ?", conv.ID); err != nil {
			t.Errorf("cleanup residual conversation root failed: %v", err)
		}
		var rCount, sCount, iCount, aCount, cCount int
		if err := d.QueryRow("SELECT count(*) FROM side_question_requests WHERE session_key = ?", convKey).Scan(&rCount); err != nil {
			t.Errorf("query residual side_question_requests failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM side_question_sessions WHERE conversation_id = ?", conv.ID).Scan(&sCount); err != nil {
			t.Errorf("query residual side_question_sessions failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM intercept_pending WHERE conversation_id = ?", conv.ID).Scan(&iCount); err != nil {
			t.Errorf("query residual intercept_pending failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM conversation_activities WHERE conversation_id = ?", conv.ID).Scan(&aCount); err != nil {
			t.Errorf("query residual conversation_activities failed: %v", err)
		}
		if err := d.QueryRow("SELECT count(*) FROM conversations WHERE id = ?", conv.ID).Scan(&cCount); err != nil {
			t.Errorf("query residual conversations failed: %v", err)
		}
		if rCount != 0 || sCount != 0 || iCount != 0 || aCount != 0 || cCount != 0 {
			t.Errorf("residual conversation fixtures found after cleanup: requests=%d, side_sessions=%d, intercept=%d, activities=%d, conversations=%d", rCount, sCount, iCount, aCount, cCount)
		}
	})

	// 2. 创建真实 finding 供 retest 引用 (NOT NULL 无默认值列完整填充)
	fName := fmt.Sprintf("Finding for conv %d", conv.ID)
	res, err := d.Exec("INSERT INTO findings (name, severity, status, vulnclass, summary, evidence, worker, asset_ids, report, created_at) VALUES (?, 'low', 'confirmed', 'sqli', 'test vuln', 'evidence', 'worker', '[]', '', NOW(3))", fName)
	if err != nil {
		t.Fatalf("seed finding failed: %v", err)
	}
	findingID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get finding last insert id failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM findings WHERE id = ?", findingID); err != nil {
			t.Errorf("cleanup residual findings failed: %v", err)
		}
		var fCount int
		if err := d.QueryRow("SELECT count(*) FROM findings WHERE id = ?", findingID).Scan(&fCount); err != nil {
			t.Errorf("query residual findings failed: %v", err)
		}
		if fCount != 0 {
			t.Errorf("residual finding found after cleanup: findings=%d", fCount)
		}
	})

	// 3. 挂载真实活跃 retest (status='pending')
	// Catalog 约束规范：
	// - finding_retests.conversation_id 具有 UNIQUE 索引 (uq_finding_retests_conversation)；
	// - active_finding_id 具有 UNIQUE 索引 (uq_finding_retests_active: IF(status IN ('pending','running'), finding_id, NULL))；
	// 因此当前 conversation 恰好绑定 1 个 active pending retest，关联 findingID；
	// 另行创建无会话绑定的已完成 retest (conversation_id IS NULL, status='completed')
	// 验证 finding 实体存活与历史重测数据不受跨会话级联影响。
	res, err = d.Exec("INSERT INTO finding_retests (finding_id, conversation_id, status, snapshot, verdict, notes, summary, evidence, error, created_at) VALUES (?, ?, 'pending', '{}', '', '', '', '', '', NOW(3))", findingID, conv.ID)
	if err != nil {
		t.Fatalf("seed pending retest failed: %v", err)
	}
	pendingRetestID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get pending retest ID failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM finding_retests WHERE id = ?", pendingRetestID); err != nil {
			t.Errorf("cleanup pending retest failed: %v", err)
		}
		var rtCount int
		if err := d.QueryRow("SELECT count(*) FROM finding_retests WHERE id = ?", pendingRetestID).Scan(&rtCount); err != nil {
			t.Errorf("query residual pending retest failed: %v", err)
		}
		if rtCount != 0 {
			t.Errorf("residual pending retest found after cleanup: retests=%d", rtCount)
		}
	})

	res, err = d.Exec("INSERT INTO finding_retests (finding_id, conversation_id, status, snapshot, verdict, notes, summary, evidence, error, created_at) VALUES (?, NULL, 'completed', '{}', 'confirmed', 'notes', 'retest pass', '', '', NOW(3))", findingID)
	if err != nil {
		t.Fatalf("seed completed retest failed: %v", err)
	}
	completedRetestID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get completed retest ID failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM finding_retests WHERE id = ?", completedRetestID); err != nil {
			t.Errorf("cleanup completed retest failed: %v", err)
		}
		var rtCount int
		if err := d.QueryRow("SELECT count(*) FROM finding_retests WHERE id = ?", completedRetestID).Scan(&rtCount); err != nil {
			t.Errorf("query residual completed retest failed: %v", err)
		}
		if rtCount != 0 {
			t.Errorf("residual completed retest found after cleanup: retests=%d", rtCount)
		}
	})

	// 4. 挂载真实 intercept_pending
	res, err = d.Exec("INSERT INTO intercept_pending (conversation_id, tool_name, agent_name, tool_input, status, reason, decision_source, created_at) VALUES (?, 'bash', 'test_agent', '{}', 'pending', '', '', NOW(3))", conv.ID)
	if err != nil {
		t.Fatalf("seed intercept_pending failed: %v", err)
	}
	interceptID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get intercept_pending ID failed: %v", err)
	}
	if interceptID <= 0 {
		t.Fatalf("expected positive intercept_pending ID, got %d", interceptID)
	}

	// 5. 挂载真实 conversation_activities
	actID, err := d.AppendConvActivity(conv.ID, Activity{Worker: "test_worker", Kind: "msg", Summary: "test summary"})
	if err != nil {
		t.Fatalf("AppendConvActivity failed: %v", err)
	}
	if actID <= 0 {
		t.Fatalf("expected positive activity ID, got %d", actID)
	}

	// 6. 挂载真实 side_question_sessions 与关联 side_question_requests
	ctx := context.Background()
	if err := d.SaveSideSnapshot(ctx, sidequestion.Snapshot{
		Parent:  sidequestion.Parent{ConversationID: conv.ID},
		RunID:   1,
		Version: 1,
	}); err != nil {
		t.Fatalf("SaveSideSnapshot failed: %v", err)
	}

	// 验证 side session 正向落盘且行数恰好为 1，杜绝 no-op 假绿
	var preSideCount int
	if err := d.QueryRow("SELECT count(*) FROM side_question_sessions WHERE session_key = ? AND conversation_id = ?", convKey, conv.ID).Scan(&preSideCount); err != nil {
		t.Fatalf("query pre side session failed: %v", err)
	}
	if preSideCount != 1 {
		t.Fatalf("expected exactly 1 side_question_session persisted before delete, got %d", preSideCount)
	}

	// 挂载真实 side_question_requests（带 NOT NULL 列与 canonical identity 计算）
	reqID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	reqIdent := schema.SideQuestionRequestsIdentity(convKey, 1, "test_client")
	reqHash := schema.CanonicalHash(reqIdent)
	if _, err := d.Exec("INSERT INTO side_question_requests (id, session_key, generation, client_id, identity, identity_hash, question, answer, status, error, model, `usage`, context_info, snapshot_at, created_at) VALUES (?, ?, 1, 'test_client', ?, ?, 'test question', '', 'running', '', '{}', '{}', '{}', NOW(3), NOW(3))", reqID, convKey, reqIdent, reqHash[:]); err != nil {
		t.Fatalf("seed side_question_requests failed: %v", err)
	}

	// 7. 执行被测 DeleteConversation 操作
	err = d.DeleteConversation(conv.ID)
	if err != nil {
		t.Fatalf("DeleteConversation failed: %v", err)
	}

	// 8. 失败判据断言①：conversation 根行被彻底删除
	var exists bool
	queryErr := d.QueryRow("SELECT true FROM conversations WHERE id = ?", conv.ID).Scan(&exists)
	if queryErr == nil {
		t.Fatalf("conversation %d still exists after deletion", conv.ID)
	} else if !errors.Is(queryErr, sql.ErrNoRows) {
		t.Fatalf("query conversation existence failed: %v", queryErr)
	}

	// 9. 失败判据断言②：四类级联依赖全部归零
	var actLeft, sessionLeft, reqLeft, interceptLeft int
	if err := d.QueryRow("SELECT count(*) FROM conversation_activities WHERE conversation_id = ?", conv.ID).Scan(&actLeft); err != nil {
		t.Fatalf("query residual activities failed: %v", err)
	}
	if actLeft != 0 {
		t.Fatalf("expected 0 residual conversation_activities, got %d", actLeft)
	}
	if err := d.QueryRow("SELECT count(*) FROM side_question_sessions WHERE conversation_id = ?", conv.ID).Scan(&sessionLeft); err != nil {
		t.Fatalf("query residual side_question_sessions failed: %v", err)
	}
	if sessionLeft != 0 {
		t.Fatalf("expected 0 residual side_question_sessions, got %d", sessionLeft)
	}
	if err := d.QueryRow("SELECT count(*) FROM side_question_requests WHERE session_key = ?", convKey).Scan(&reqLeft); err != nil {
		t.Fatalf("query residual side_question_requests failed: %v", err)
	}
	if reqLeft != 0 {
		t.Fatalf("expected 0 residual side_question_requests, got %d", reqLeft)
	}
	if err := d.QueryRow("SELECT count(*) FROM intercept_pending WHERE conversation_id = ?", conv.ID).Scan(&interceptLeft); err != nil {
		t.Fatalf("query residual intercept_pending failed: %v", err)
	}
	if interceptLeft != 0 {
		t.Fatalf("expected 0 residual intercept_pending, got %d", interceptLeft)
	}

	// 10. 失败判据断言③：finding_retests 实体行必须保留，但 conversation_id 必须置空为 NULL
	var pConvID *int64
	var pStatus, cStatus string
	if err := d.QueryRow("SELECT conversation_id, status FROM finding_retests WHERE id = ?", pendingRetestID).Scan(&pConvID, &pStatus); err != nil {
		t.Fatalf("query pending retest state failed: %v", err)
	}
	if pConvID != nil {
		t.Fatalf("expected pending retest conversation_id to be NULL after conv deletion, got %d", *pConvID)
	}

	// 11. 失败判据断言④：活跃 retest 状态转为停止态，历史已完成 retest 状态不受影响
	if pStatus != "cancelled" && pStatus != "stopped" {
		t.Fatalf("expected active pending retest to transition to stopped/cancelled state, got %q", pStatus)
	}
	if err := d.QueryRow("SELECT status FROM finding_retests WHERE id = ?", completedRetestID).Scan(&cStatus); err != nil {
		t.Fatalf("query completed retest state failed: %v", err)
	}
	if cStatus != "completed" {
		t.Fatalf("completed retest status must remain 'completed' untouched, got %q", cStatus)
	}

	// 12. 失败判据断言⑤：共享 finding 实体存活不受影响
	var fCountAfter int
	if err := d.QueryRow("SELECT count(*) FROM findings WHERE id = ?", findingID).Scan(&fCountAfter); err != nil {
		t.Fatalf("query shared finding count failed: %v", err)
	}
	if fCountAfter != 1 {
		t.Fatalf("shared finding %d must survive conversation deletion, got %d rows", findingID, fCountAfter)
	}
}

// =====================================================================
// 2) 计划 Step 1 分态矩阵 —— 真机：活跃 / 终态 / 未绑定三态与会话隔离
// =====================================================================

// TestConversation_DeleteConversation_PerStateRetestMatrix 落地计划的"会话闭包分态
// 分类验收矩阵"：会话 A（pending）、会话 B（running）、会话 C（completed）各挂一个
// 独立 finding 的 retest，外加一条完全不绑定会话的历史 retest。删除 A/B/C 之后：
//   - A、B 的 retest 必须保留实体、置空 conversation_id、状态转停止态；
//   - C 的 retest 必须保留实体、置空 conversation_id、状态**绝对保持 completed**；
//   - 未绑定会话的历史 retest 在任何会话删除前后都不得被波及。
func TestConversation_DeleteConversation_PerStateRetestMatrix(t *testing.T) {
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

	type fixture struct {
		convID    int64
		findingID int64
		retestID  int64
		wantStop  bool
	}

	newFixture := func(t *testing.T, status string, bindConversation bool) fixture {
		t.Helper()
		var f fixture
		conv, err := d.CreateConversation("test_agent", fmt.Sprintf("P08 matrix %s %d", status, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("CreateConversation failed: %v", err)
		}
		f.convID = conv.ID
		t.Cleanup(func() {
			if _, err := d.Exec("DELETE FROM conversations WHERE id = ?", f.convID); err != nil {
				t.Errorf("cleanup residual conversation failed: %v", err)
			}
		})
		res, err := d.Exec("INSERT INTO findings (name, severity, status, vulnclass, summary, evidence, worker, asset_ids, report, created_at) VALUES (?, 'low', 'confirmed', 'sqli', 's', 'e', 'w', '[]', '', NOW(3))",
			fmt.Sprintf("P08 matrix finding %d", time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("seed finding failed: %v", err)
		}
		f.findingID, err = res.LastInsertId()
		if err != nil {
			t.Fatalf("finding LastInsertId failed: %v", err)
		}
		t.Cleanup(func() {
			if _, err := d.Exec("DELETE FROM findings WHERE id = ?", f.findingID); err != nil {
				t.Errorf("cleanup residual finding failed: %v", err)
			}
		})
		var convArg any
		if bindConversation {
			convArg = f.convID
		}
		res, err = d.Exec("INSERT INTO finding_retests (finding_id, conversation_id, status, snapshot, verdict, notes, summary, evidence, error, created_at) VALUES (?, ?, ?, '{}', '', '', '', '', '', NOW(3))",
			f.findingID, convArg, status)
		if err != nil {
			t.Fatalf("seed %s retest failed: %v", status, err)
		}
		f.retestID, err = res.LastInsertId()
		if err != nil {
			t.Fatalf("retest LastInsertId failed: %v", err)
		}
		t.Cleanup(func() {
			if _, err := d.Exec("DELETE FROM finding_retests WHERE id = ?", f.retestID); err != nil {
				t.Errorf("cleanup residual retest failed: %v", err)
			}
		})
		return f
	}

	pending := newFixture(t, "pending", true)
	running := newFixture(t, "running", true)
	completed := newFixture(t, "completed", true)
	history := newFixture(t, "completed", false) // conversation_id IS NULL 的历史对照行

	historyBefore, err := func() (int64, error) {
		var id int64
		err := d.QueryRow("SELECT id FROM finding_retests WHERE id = ?", history.retestID).Scan(&id)
		return id, err
	}()
	if err != nil {
		t.Fatalf("query history retest failed: %v", err)
	}

	for _, f := range []fixture{pending, running, completed} {
		if err := d.DeleteConversation(f.convID); err != nil {
			t.Fatalf("DeleteConversation(%d) failed: %v", f.convID, err)
		}
	}

	assertState := func(label string, f fixture, wantStop bool) {
		t.Helper()
		var convIDArg *int64
		var status string
		if err := d.QueryRow("SELECT conversation_id, status FROM finding_retests WHERE id = ?", f.retestID).Scan(&convIDArg, &status); err != nil {
			t.Fatalf("%s: query retest state failed: %v", label, err)
		}
		if convIDArg != nil {
			t.Fatalf("%s: retest %d conversation_id must be NULL, got %d", label, f.retestID, *convIDArg)
		}
		if wantStop {
			if status != "stopped" && status != "cancelled" {
				t.Fatalf("%s: active retest must switch to a stopped state, got %q", label, status)
			}
		} else if status != "completed" {
			t.Fatalf("%s: terminal retest status must stay unchanged, got %q", label, status)
		}
		var fCount int
		if err := d.QueryRow("SELECT count(*) FROM findings WHERE id = ?", f.findingID).Scan(&fCount); err != nil {
			t.Fatalf("%s: query finding failed: %v", label, err)
		}
		if fCount != 1 {
			t.Fatalf("%s: shared finding must survive, got %d rows", label, fCount)
		}
	}

	assertState("会话 A(pending)", pending, true)
	assertState("会话 B(running)", running, true)
	assertState("会话 C(completed)", completed, false)

	var historyStatus string
	var historyConv *int64
	if err := d.QueryRow("SELECT conversation_id, status FROM finding_retests WHERE id = ?", history.retestID).Scan(&historyConv, &historyStatus); err != nil {
		t.Fatalf("history retest must survive conversation deletions: %v", err)
	}
	if historyStatus != "completed" || historyConv != nil || historyBefore != history.retestID {
		t.Fatalf("cross-conversation cascade must not touch unbound history retest: status=%q conv=%v", historyStatus, historyConv)
	}

	// 根行确已删除，且会话侧的四类依赖归零
	for _, f := range []fixture{pending, running, completed} {
		var root int
		if err := d.QueryRow("SELECT count(*) FROM conversations WHERE id = ?", f.convID).Scan(&root); err != nil {
			t.Fatalf("query conversation root failed: %v", err)
		}
		if root != 0 {
			t.Fatalf("conversation %d root must be deleted", f.convID)
		}
	}
}

// =====================================================================
// 3) 源码级守卫 —— 8 个辅助文件的 PG 方言必须清零（含自检，防"空转绿"）
// =====================================================================

// auxModuleFiles 是 Task 4.3 清单里的 8 个产品业务文件。
var auxModuleFiles = []string{
	"conversation.go",
	"triggers.go",
	"chat_mentions.go",
	"skill_usage.go",
	"side_questions.go",
	"constraints.go",
	"intercept_detail.go",
	"intercept_execution.go",
}

// auxPGFragments 是 PG 专有写法 → 一条必须命中的 PG 样例片段。
// 键集合本身就是判红清单；样例让 TestAuxModules_DialectFragmentsAreNotVacuous
// 能证明扫描器真的在起作用（否则"绿"只是什么都没查）。
var auxPGFragments = map[string]string{
	"$1":               "SELECT value FROM scheduler_state WHERE `key`=$1",
	"$2":               "INSERT INTO settings(`key`) VALUES ($1) ON CONFLICT DO NOTHING -- $2",
	"$5":               "SELECT id FROM conversations WHERE id=$1 AND agent_key=$2 AND title=$3 AND x=$4 AND y=$5",
	"RETURNING":        "INSERT INTO conversations(agent_key) VALUES (?) RETURNING id",
	"ON CONFLICT":      "INSERT INTO side_question_sessions(session_key) VALUES (?) ON CONFLICT (session_key) DO UPDATE SET run_id=EXCLUDED.run_id",
	"EXCLUDED.":        "ON CONFLICT DO UPDATE SET value = EXCLUDED.value",
	"::":               "SELECT id::text FROM findings",
	"::JSONB":          "UPDATE intercept_pending SET audit = audit || ?::jsonb",
	"ILIKE":            "SELECT id FROM findings WHERE name ILIKE ?",
	"ANY(":             "DELETE FROM conversations WHERE id=ANY(?)",
	"ARRAY[":           "INSERT INTO assets(task_ids) VALUES (ARRAY[?])",
	"STRING_AGG":       "SELECT STRING_AGG(DISTINCT agent_key, ',') FROM skill_usage",
	"JSONB":            "SELECT jsonb_array_elements_text(agents)",
	"->>":              "SELECT audit->>'run_id' FROM intercept_pending",
	"MAKE_INTERVAL":    "SELECT now() - make_interval(days => ?)",
	"ROW_TO_JSON":      "SELECT row_to_json(ip) FROM intercept_pending ip",
	"PG_BACKEND_PID":   "SELECT pg_backend_pid()",
	"PG_BLOCKING_PIDS": "SELECT cardinality(pg_blocking_pids(?))",
	"NOW()":            "UPDATE conversations SET updated_at=now() WHERE id=?",
	"NULLS LAST":       "ORDER BY pinned_at DESC NULLS LAST",
	"FOR SHARE":        "SELECT id FROM conversations WHERE id=? FOR SHARE",
	"DISTINCT ON":      "SELECT DISTINCT ON (id) id FROM findings",
	"AT TIME ZONE":     "SELECT to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD')",
	"TO_CHAR(":         "SELECT TO_CHAR(created_at, 'YYYY-MM-DD')",
	"LATERAL":          "SELECT 1 FROM t JOIN LATERAL (SELECT 1) x ON TRUE",
	"FILTER (WHERE":    "SELECT COUNT(*) FILTER (WHERE found) FROM skill_usage",
}

// auxSQLChunk 是源文件里一个字符串字面量的内容及其起始行号。
type auxSQLChunk struct {
	line int
	text string
}

// auxStringLiterals 抽出源文件里所有字符串字面量（反引号原文串与双引号转义串）的内容。
//
// 判据只盯"发给数据库的 SQL 文本"，不看注释、也不看 Go 标识符，理由是这两类命中都是
// 假阳性：`jsonbClean` 是清洗 NUL 的历史命名（TiDB 的 JSON 列同样不接受 \u0000），
// `time.Now().UTC()` 是 Go 取时间，不是 SQL 的 now()。真阳性只会出现在字面量里 ——
// 我们发出去的每一条语句都由字面量拼成。
func auxStringLiterals(src string) []auxSQLChunk {
	var out []auxSQLChunk
	line, i := 1, 0
	for i < len(src) {
		switch c := src[i]; c {
		case '\n':
			line++
			i++
		case '/': // 注释整体跳过（下一个 switch 分支处理两种形式）
			if i+1 < len(src) && src[i+1] == '/' {
				for i < len(src) && src[i] != '\n' {
					i++
				}
			} else if i+1 < len(src) && src[i+1] == '*' {
				end := strings.Index(src[i+2:], "*/")
				if end < 0 {
					return out
				}
				line += strings.Count(src[i:i+2+end+2], "\n")
				i += 2 + end + 2
			} else {
				i++
			}
		case '`': // 原始串：可跨行，内容原样取
			start := i
			i++
			for i < len(src) && src[i] != '`' {
				i++
			}
			body := src[start+1 : auxMinInt(i, len(src))]
			out = append(out, auxSQLChunk{line: line, text: body})
			line += strings.Count(body, "\n")
			if i < len(src) {
				i++
			}
		case '"': // 转义串：Go 里不跨行
			start := i
			i++
			for i < len(src) && src[i] != '"' {
				if src[i] == '\\' {
					i++
				}
				if i < len(src) && src[i] == '\n' {
					break // 非法 Go 源，保守终止
				}
				i++
			}
			out = append(out, auxSQLChunk{line: line, text: src[start+1 : auxMinInt(i, len(src))]})
			if i < len(src) {
				i++
			}
		default:
			i++
		}
	}
	return out
}

// TestAuxModules_SourceIsTiDBDialectOnly 扫描 8 个辅助文件里所有 SQL 字面量，任何
// PostgreSQL 片段都判红。这是 Task 4.3 那 65 处 Blocker 的清零判据。
func TestAuxModules_SourceIsTiDBDialectOnly(t *testing.T) {
	for _, file := range auxModuleFiles {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		hits := 0
		for _, chunk := range auxStringLiterals(string(raw)) {
			upper := strings.ToUpper(chunk.text)
			for fragment := range auxPGFragments {
				if strings.Contains(upper, strings.ToUpper(fragment)) {
					hits++
					snippet := auxCollapse(chunk.text)
					if len(snippet) > 120 {
						snippet = snippet[:120] + "…"
					}
					t.Errorf("%s:%d: PostgreSQL fragment %q in SQL literal: %s", file, chunk.line, fragment, snippet)
				}
			}
		}
		_ = hits
	}
}

// TestAuxModules_DialectFragmentsAreNotVacuous 自检每个 PG 特征片段都能命中自己的样例，
// 并额外证明这些片段确实**不在**已改造的 TiDB 代码里（防止把片段清单改成空集蒙绿）。
func TestAuxModules_DialectFragmentsAreNotVacuous(t *testing.T) {
	if len(auxPGFragments) < 20 {
		t.Fatalf("PG 片段清单只剩 %d 项，明显被削弱", len(auxPGFragments))
	}
	for fragment, sample := range auxPGFragments {
		if !strings.Contains(strings.ToUpper(sample), strings.ToUpper(fragment)) {
			t.Errorf("片段 %q 命不中自己的 PG 样例 %q", fragment, sample)
			continue
		}
		// 端到端自检：把样例塞进一个真实的 Go 原始串字面量，扫描器必须抓到；
		// 再把同样内容放进注释，必须不抓到（否则"绿"可能只是扫描器根本不工作）。
		src := "package x\n\nconst q = `" + sample + "`\n"
		if !auxLiteralContains(src, fragment) {
			t.Errorf("扫描器命不中原始串字面量里的片段 %q（样例 %q）", fragment, sample)
		}
		commented := "package x\n\n// " + sample + "\nconst q = `SELECT 1`\n"
		if auxLiteralContains(commented, fragment) {
			t.Errorf("扫描器把注释里的 %q 当成了 SQL，注释剥离失效", fragment)
		}
	}
}

// auxLiteralContains 用真实抽取逻辑判断某个片段能否在源码的字符串字面量里被命中。
func auxLiteralContains(src, fragment string) bool {
	for _, chunk := range auxStringLiterals(src) {
		if strings.Contains(strings.ToUpper(chunk.text), strings.ToUpper(fragment)) {
			return true
		}
	}
	return false
}

// TestAuxModules_HashKeyedWritesCarryIdentityColumns 固定两张 hash 唯一键表的写入形状：
//   - scheduler_state 的唯一键是 uq_scheduler_state_key_hash(key_hash)（M13）：
//     不写 key_identity/key_hash 会让唯一键退化成 NULL，重复水位再也挡不住；
//   - side_question_requests 的唯一键是 uq_side_requests_identity(identity_hash)（M17）：
//     请求身份必须由 canonical 计算补齐，缺列等于同一 (session_key,generation,client_id)
//     可以无限重复插入。
//
// 以及 `key`/`value` 这类 MySQL 关键字必须反引号包裹。
func TestAuxModules_HashKeyedWritesCarryIdentityColumns(t *testing.T) {
	for _, want := range []struct {
		file    string
		symbols []string
	}{
		{"triggers.go", []string{"scheduler_state", "key_identity", "key_hash", "ON DUPLICATE KEY UPDATE", "`key`", "`value`"}},
		{"side_questions.go", []string{"identity_hash", "SideQuestionRequestsIdentity", "canonicalIdentityPair"}},
	} {
		src := stripGoComments(t, want.file)
		for _, sym := range want.symbols {
			if !strings.Contains(src, sym) {
				t.Errorf("%s: 必须写入/使用 %q", want.file, sym)
			}
		}
	}
}

// TestAuxModules_KeywordsAreBackquoted 证明关键字列确实被反引号包住：`key` 在 MySQL/TiDB
// 是保留字，裸写 `WHERE key=?` 直接 1064 语法错。
func TestAuxModules_KeywordsAreBackquoted(t *testing.T) {
	src := stripGoComments(t, "triggers.go")
	for _, forbidden := range []string{"WHERE key=", "WHERE key =", "scheduler_state(key,", "SET value=", "value=EXCLUDED"} {
		if strings.Contains(strings.ToLower(src), strings.ToLower(forbidden)) {
			t.Errorf("triggers.go: 关键字列必须反引号包裹，发现裸写 %q", forbidden)
		}
	}
}

// =====================================================================
// 4) 依赖级守卫 —— github.com/jackc/pgx 必须从仓库与 go.mod 彻底消失
// =====================================================================

// auxPruneDirs 是仓库扫描时要跳过的目录（前端产物、依赖缓存、审查快照等）。
var auxPruneDirs = map[string]bool{
	".git": true, ".superpowers": true, ".next": true, ".turbo": true,
	"node_modules": true, "vendor": true, "dist": true, "build": true,
	"screenshots": true, "coverage": true,
}

// auxStripLineComments 把 // 与 /* */ 注释替换成等量空白（保留行号与字符串字面量）。
// 与 db_test.go 的 stripGoComments 同源；这里独立实现是为了能接受内存内容并跳过
// t.Fatalf（仓库扫描里读不到的文件要能继续）。
func auxStripLineComments(src string) string {
	out := make([]byte, 0, len(src))
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == '/' && i+1 < len(src) && src[i+1] == '/':
			for i < len(src) && src[i] != '\n' {
				out = append(out, ' ')
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return string(out)
			}
			for _, r := range src[i : i+2+end+2] {
				if r == '\n' {
					out = append(out, '\n')
				} else {
					out = append(out, ' ')
				}
			}
			i += 2 + end + 2
		default:
			out = append(out, c)
			i++
		}
	}
	return string(out)
}

// auxPgxScanExempt 是"文本里必然要提到 pgx 字样"的守卫文件：它们把被禁符号当作
// **被搜索的样例**写出来（本守卫自己的符号清单、Task 4.1 的 PG 方言标记清单）。
// 豁免只放宽到"仍禁止真实 import"，且每一条都必须是存在的文件，防止豁免表变成
// 万能后门。生产代码一律不得出现在这里。
var auxPgxScanExempt = map[string]string{
	"db/aux_modules_test.go": "本守卫自身声明被禁符号清单",
	"db/db_test.go":          "Task 4.1 的 PG 方言标记样例清单",
}

func auxRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Dir(wd) // 测试运行在 <repo>/db
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s 看起来不是仓库根（缺 go.mod）: %v", root, err)
	}
	return root
}

// TestPgxDependencyIsFullyRemoved 是全仓级依赖切除门禁：
//  1. go.mod / go.sum 里不得再出现 github.com/jackc/pgx（任何版本、任何间接引用）；
//  2. 任何 .go 源码（含测试）都不得 import jackc/pgx，也不得出现 pgconn 符号或
//     sql.Open("pgx", …) 的驱动名；
//  3. 反向自检：TiDB 驱动 github.com/go-sql-driver/mysql 必须在依赖清单里，
//     否则上面两条会因为"依赖清单被整体删空"而假绿。
func TestPgxDependencyIsFullyRemoved(t *testing.T) {
	root := auxRepoRoot(t)

	goModRaw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	goMod := string(goModRaw)
	if strings.Contains(goMod, "jackc/pgx") {
		t.Errorf("go.mod 仍声明 pgx 依赖:\n%s", goMod)
	}
	if !strings.Contains(goMod, "github.com/go-sql-driver/mysql") {
		t.Errorf("go.mod 缺少 TiDB 官方协议驱动 go-sql-driver/mysql —— 依赖清单本身可能已损坏")
	}
	if _, err := os.Stat(filepath.Join(root, "go.sum")); err == nil {
		goSumRaw, err := os.ReadFile(filepath.Join(root, "go.sum"))
		if err != nil {
			t.Fatalf("read go.sum: %v", err)
		}
		for i, line := range strings.Split(string(goSumRaw), "\n") {
			if strings.Contains(line, "jackc/pgx") {
				t.Errorf("go.sum:%d 仍残留 pgx 校验和: %s", i+1, line)
			}
		}
	}

	// importForm 是"真的又把 pgx 引进来"的唯一无歧义形态：带引号的完整 import 路径。
	// 依赖守卫豁免表里的文件（它们必须把被禁符号当作**样例文本**写出来）只查这一种，
	// 其余文件查全部符号。
	importForm := `"github.com/jackc` + "/pgx"
	// 被禁符号在运行时拼装：守卫自身也是被扫描的文件，写成字面量会立刻自命中。
	forbiddenSymbols := []string{"jackc" + "/pgx", "pgconn" + ".", `sql.Open(` + `"pgx`, `Open(` + `"pgx`}
	var offenders []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if auxPruneDirs[entry.Name()] {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		symbols := forbiddenSymbols
		if _, exempt := auxPgxScanExempt[filepath.ToSlash(rel)]; exempt {
			symbols = []string{importForm}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// 注释里出现 pgx 字样是历史说明（"迁移前用的是 X"），不是依赖；先剥注释再匹配，
		// 既不放过 import / 驱动名 / 选择器这些真实代码形态，也不必给每处说明开豁免。
		for i, line := range strings.Split(auxStripLineComments(string(raw)), "\n") {
			for _, symbol := range symbols {
				if strings.Contains(line, symbol) {
					offenders = append(offenders, fmt.Sprintf("%s:%d: %s", rel, i+1, strings.TrimSpace(line)))
				}
			}
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("扫描仓库 .go 失败: %v", walkErr)
	}
	for path := range auxPgxScanExempt {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); err != nil {
			t.Errorf("pgx 扫描豁免表里的 %s 已不存在，请同步清理豁免表", path)
		}
	}
	if len(offenders) > 0 {
		t.Errorf("仓库仍有 %d 处 pgx 残留：\n%s", len(offenders), strings.Join(offenders, "\n"))
	}
}

// =====================================================================
// 5) 语句级替身 —— 完整会话闭包删除与 Retest 语义（M20，闭环 R51-F07-01）
// =====================================================================

func auxCollapse(s string) string { return strings.Join(strings.Fields(s), " ") }

// auxUpper 返回语句的归一化大写形态，便于跨换行/缩进比较。
func auxUpper(c expMockCall) string { return strings.ToUpper(auxCollapse(c.Query)) }

// auxWrites 返回所有写语句（Exec），顺序即真实执行顺序。
func auxWrites(calls []expMockCall) []expMockCall {
	out := make([]expMockCall, 0, len(calls))
	for _, c := range calls {
		if c.Kind == "exec" {
			out = append(out, c)
		}
	}
	return out
}

// auxFind 返回第一条（大写）包含 substrs 全部片段的语句；找不到即判红。
func auxFind(t *testing.T, calls []expMockCall, substrs ...string) expMockCall {
	t.Helper()
	for _, c := range calls {
		if auxContainsAll(auxUpper(c), substrs...) {
			return c
		}
	}
	t.Fatalf("未找到包含 %v 的语句，实际执行：\n%s", substrs, auxDump(calls))
	return expMockCall{}
}

func auxContainsAll(upper string, substrs ...string) bool {
	for _, s := range substrs {
		if !strings.Contains(upper, strings.ToUpper(s)) {
			return false
		}
	}
	return true
}

func auxIndexOf(t *testing.T, calls []expMockCall, substr string) int {
	t.Helper()
	for i, c := range calls {
		if strings.Contains(auxUpper(c), strings.ToUpper(substr)) {
			return i
		}
	}
	t.Fatalf("未找到包含 %q 的语句，实际执行：\n%s", substr, auxDump(calls))
	return -1
}

func auxDump(calls []expMockCall) string {
	out := make([]string, 0, len(calls))
	for i, c := range calls {
		out = append(out, fmt.Sprintf("  [%d] %s | args=%v", i, auxCollapse(c.Query), c.Args))
	}
	return strings.Join(out, "\n")
}

// auxAssertAllOrdinal 是本文件的核心守卫：`?` 个数必须与实参数逐位相等，且不得残留
// PG 序号占位符。机械替换最常见的事故就是重复引用的那个参数少绑一位。
func auxAssertAllOrdinal(t *testing.T, calls []expMockCall) {
	t.Helper()
	for _, c := range calls {
		expAssertOrdinal(t, c)
	}
}

// auxConvRow 是 convCols 的一行回读结果。
func auxConvRow(id int64, title string) []driver.Value {
	return []driver.Value{
		id, "test_agent", title, nil, nil,
		time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC),
		time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC),
	}
}

func TestConversation_DeleteConversation_ClosureAndRetestStatements(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	const convID int64 = 77

	// 规则注册顺序 = 命中优先级：更具体的谓词必须排在更泛的之前。
	eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{convID})
	eng.on("SELECT 1 FROM conversations WHERE id = ?", []string{"ok"}, []driver.Value{int64(1)})
	eng.on("SELECT id FROM conversation_activities", []string{"id"}, []driver.Value{int64(501)}, []driver.Value{int64(502)})
	eng.on("SELECT id FROM intercept_pending", []string{"id"}, []driver.Value{int64(601)})
	eng.on("SELECT session_key FROM side_question_sessions", []string{"session_key"}, []driver.Value{"conv-77"})
	eng.on("SELECT ordinal FROM side_question_requests", []string{"ordinal"}, []driver.Value{int64(701)})
	// 活跃 retest 只有 901（pending/running）；902 是同会话的 completed 终态行。
	eng.on("SELECT id FROM finding_retests WHERE conversation_id = ? AND status IN", []string{"id"}, []driver.Value{int64(901)})
	eng.on("SELECT id FROM finding_retests WHERE conversation_id = ?", []string{"id"}, []driver.Value{int64(901)}, []driver.Value{int64(902)})
	eng.on("SELECT COUNT(1) FROM", []string{"cnt"}, []driver.Value{int64(0)})

	if err := d.DeleteConversation(convID); err != nil {
		t.Fatalf("DeleteConversation failed: %v", err)
	}

	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	writes := auxWrites(calls)
	if len(writes) == 0 {
		t.Fatalf("DeleteConversation 没有发出任何写语句：\n%s", auxDump(calls))
	}

	// 判据①：整个闭包跑在一个事务里，且必须先锁会话根行（与旁路快照写者互斥）。
	if counts := eng.txCounts(); counts.Commits != 1 || counts.Rollbacks != 0 {
		t.Fatalf("闭包删除必须恰好提交一次，实际 %+v", counts)
	}
	lockIdx := auxIndexOf(t, calls, "SELECT ID FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
	firstDelete := len(calls)
	for i, c := range calls {
		if strings.Contains(auxUpper(c), "DELETE FROM") {
			firstDelete = i
			break
		}
	}
	if lockIdx > firstDelete {
		t.Fatalf("根行 FOR UPDATE 锁必须在任何 DELETE 之前：lock=%d firstDelete=%d", lockIdx, firstDelete)
	}

	// 判据②（审查 F03 修复轮次 1）：停止语句必须**同时**带两道闸门 ——
	//   ① 作用域：打在闭包收集好的活跃 id 集合上（同会话 completed 的 902 绝不在里面）；
	//   ② 当前状态守卫 status IN ('pending','running')：闭包收集是普通读，收集之后
	//      FinishFindingRetest 完全可能把同一条复测提交成 completed。少了这道守卫，
	//      无条件 SET status='stopped' 就把已提交的终态篡改成停止态 —— 直接违反
	//      R51-F07-01「终态不可篡改」的铁律。
	stop := auxFind(t, writes, "UPDATE FINDING_RETESTS SET STATUS = 'STOPPED'")
	normStop := auxUpper(stop)
	for _, want := range []string{"WHERE ID IN (", "AND STATUS IN ('PENDING', 'RUNNING')"} {
		if !strings.Contains(normStop, want) {
			t.Fatalf("停止活跃 retest 缺少 %s（id 集合 + 当前状态守卫缺一不可）：%s", want, auxCollapse(stop.Query))
		}
	}
	if got := auxSetClause(t, stop.Query); got != "STATUS = 'STOPPED', CONVERSATION_ID = NULL" {
		t.Errorf("停止语句只允许写 status/conversation_id 两列，实际 %q：%s", got, auxCollapse(stop.Query))
	}
	if len(stop.Args) != 1 || !auxNumEq(stop.Args[0], 901) {
		t.Fatalf("停止语句的参数必须恰好是活跃 retest 901，实际 %v", stop.Args)
	}

	// 判据③（审查 F03）：收集之后才被并发收尾成终态的那条，只能被**脱钩**、不能被改状态。
	// 因此必须存在一条 SET 列表里**没有 status** 的置空语句，作用域覆盖收集到的全部 id
	// （含 902 这条 completed）：外键被清掉，状态原样保留。
	detach := auxFind(t, writes, "UPDATE FINDING_RETESTS SET CONVERSATION_ID = NULL WHERE ID IN (")
	if strings.Contains(auxSetClause(t, detach.Query), "STATUS") {
		t.Fatalf("按 id 脱钩的语句绝不得写 status（会覆写并发提交的终态）：%s", auxCollapse(detach.Query))
	}
	if len(detach.Args) != 2 || !auxNumEq(detach.Args[0], 901) || !auxNumEq(detach.Args[1], 902) {
		t.Fatalf("按 id 脱钩必须覆盖收集到的全部 retest（901/902），实际 %v", detach.Args)
	}
	// 兜底：再按会话作用域清一次，捕获"闭包收集之后才绑定本会话"的迟到引用，
	// 让无孤儿断言不因这条竞态窗口误判成删除失败。
	scope := auxFind(t, writes, "UPDATE FINDING_RETESTS SET CONVERSATION_ID = NULL WHERE CONVERSATION_ID = ?")
	if strings.Contains(auxSetClause(t, scope.Query), "STATUS") {
		t.Fatalf("会话作用域置空绝不得写 status：%s", auxCollapse(scope.Query))
	}
	if len(scope.Args) != 1 || !auxNumEq(scope.Args[0], uint64(convID)) {
		t.Fatalf("作用域置空参数必须是会话 id 本身，实际 %v", scope.Args)
	}
	// 三条 finding_retests 写语句的谓词都必须是等值（绝不 NULL-safe、绝不 IS NULL），
	// 未绑定会话（conversation_id IS NULL）的历史 retest 不得被波及。
	for _, c := range writes {
		if !strings.Contains(auxUpper(c), "FINDING_RETESTS") {
			continue
		}
		u := auxUpper(c)
		if strings.Contains(u, "<=>") || strings.Contains(u, "CONVERSATION_ID IS NULL") {
			t.Errorf("finding_retests 写语句谓词必须等值，不得波及未绑定会话的历史行：%s", auxCollapse(c.Query))
		}
	}
	// 顺序：先停止活跃（顺手脱钩）→ 再按 id 脱钩全部 → 最后作用域兜底。
	stopAt := auxIndexOf(t, writes, "SET STATUS = 'STOPPED'")
	detachAt := auxIndexOf(t, writes, "SET CONVERSATION_ID = NULL WHERE ID IN")
	scopeAt := auxIndexOf(t, writes, "SET CONVERSATION_ID = NULL WHERE CONVERSATION_ID = ?")
	if !(stopAt < detachAt && detachAt < scopeAt) {
		t.Errorf("retest 写序必须是 停止(%d) < 按 id 脱钩(%d) < 作用域兜底(%d)", stopAt, detachAt, scopeAt)
	}

	// 判据④：四类级联依赖全部显式删除，且都带会话作用域谓词。
	type want struct {
		stmt    string
		numArg  uint64
		strArg  string
		wantNum bool
	}
	for _, w := range []want{
		{"DELETE FROM CONVERSATION_ACTIVITIES WHERE CONVERSATION_ID = ?", uint64(convID), "", true},
		{"DELETE FROM SIDE_QUESTION_SESSIONS WHERE CONVERSATION_ID = ?", uint64(convID), "", true},
		{"DELETE FROM SIDE_QUESTION_REQUESTS WHERE SESSION_KEY IN (", 0, "conv-77", false},
		{"DELETE FROM INTERCEPT_PENDING WHERE CONVERSATION_ID = ?", uint64(convID), "", true},
	} {
		call := auxFind(t, writes, w.stmt)
		if len(call.Args) == 0 {
			t.Fatalf("%s 没有参数（会话作用域丢失）", w.stmt)
		}
		found := false
		for _, a := range call.Args {
			if w.wantNum {
				if auxNumEq(a, w.numArg) {
					found = true
				}
			} else if s, ok := a.(string); ok && s == w.strArg {
				found = true
			}
		}
		if !found {
			t.Errorf("%s 参数里找不到作用域 %v，实际 %v", w.stmt, w.strArg, call.Args)
		}
	}

	// 判据⑤：retest / finding 实体绝不被物理删除（弱关联只置空）。
	for _, c := range writes {
		u := auxUpper(c)
		if strings.Contains(u, "DELETE FROM FINDING_RETESTS") {
			t.Errorf("finding_retests 实体行绝不能被删除：%s", auxCollapse(c.Query))
		}
		if strings.Contains(u, "DELETE FROM FINDINGS") {
			t.Errorf("共享 findings 实体绝不能被会话级联删除：%s", auxCollapse(c.Query))
		}
	}

	// 判据⑥：会话根行是整个闭包的最后一条 DELETE。
	lastDelete, rootDelete := -1, -1
	for i, c := range writes {
		u := auxUpper(c)
		if strings.Contains(u, "DELETE FROM") {
			lastDelete = i
		}
		if strings.Contains(u, "DELETE FROM CONVERSATIONS WHERE ID = ?") {
			rootDelete = i
		}
	}
	if rootDelete < 0 {
		t.Fatalf("缺少 DELETE FROM conversations：\n%s", auxDump(writes))
	}
	if rootDelete != lastDelete {
		t.Errorf("会话根行必须是最后一条 DELETE：root=%d last=%d", rootDelete, lastDelete)
	}

	// 判据⑦：闭包执行完必须做无孤儿断言（四类引用各一条）。
	orphanChecks := 0
	for _, c := range calls {
		if strings.Contains(auxUpper(c), "SELECT COUNT(1) FROM") {
			orphanChecks++
		}
	}
	if orphanChecks < 4 {
		t.Errorf("闭包后必须断言无孤儿引用（>=4 条 COUNT 查询），实际 %d", orphanChecks)
	}

	// 判据⑧：整个闭包不得残留 PG 方言。
	for _, c := range calls {
		u := auxUpper(c)
		for _, frag := range []string{"RETURNING", "ON CONFLICT", "EXCLUDED.", "$1", "$2", "::", "ILIKE", "ANY("} {
			if strings.Contains(u, frag) {
				t.Errorf("闭包路径残留 PG 方言 %q：%s", frag, auxCollapse(c.Query))
			}
		}
	}
}

func TestConversation_DeleteConversation_MissingRowIsNoOp(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	// 不注册任何规则：根行锁返回空集 → 会话不存在。
	if err := d.DeleteConversation(4242); err != nil {
		t.Fatalf("删除不存在的会话必须是 no-op，实际报错：%v", err)
	}
	if writes := auxWrites(eng.snapshotCalls()); len(writes) != 0 {
		t.Errorf("会话不存在时不得发出任何写语句：\n%s", auxDump(writes))
	}
	if counts := eng.txCounts(); counts.Commits != 0 {
		t.Errorf("会话不存在时不应提交事务，实际 %+v", counts)
	}
}

func TestConversation_DeleteConversations_ReportsOnlyPresentIDs(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng.on("SELECT 1 FROM conversations WHERE id = ?", []string{"ok"}, []driver.Value{int64(1)})
	eng.on("SELECT COUNT(1) FROM", []string{"cnt"}, []driver.Value{int64(0)})

	deleted, err := d.DeleteConversations([]int64{77, 77})
	if err != nil {
		t.Fatalf("DeleteConversations failed: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != 77 {
		t.Fatalf("重复 id 必须去重后各删一次，实际 %v", deleted)
	}
	rootDeletes := 0
	for _, c := range auxWrites(eng.snapshotCalls()) {
		if strings.Contains(auxUpper(c), "DELETE FROM CONVERSATIONS WHERE ID = ?") {
			rootDeletes++
		}
	}
	if rootDeletes != 1 {
		t.Errorf("重复入参只能删一次根行，实际 %d 次", rootDeletes)
	}

	// 全部不存在 → 空集合，且零写入。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	deleted2, err := d2.DeleteConversations([]int64{1, 2, 3})
	if err != nil {
		t.Fatalf("DeleteConversations(全部不存在) 失败: %v", err)
	}
	if len(deleted2) != 0 {
		t.Errorf("不存在的 id 不得出现在返回值里：%v", deleted2)
	}
	if writes := auxWrites(eng2.snapshotCalls()); len(writes) != 0 {
		t.Errorf("全部不存在时不得有写语句：\n%s", auxDump(writes))
	}
	if _, err := d2.DeleteConversations(nil); err != nil {
		t.Errorf("空入参必须直接返回空集合，实际 %v", err)
	}
}

// TestConversation_CreateAndUpdateDialect 覆盖 RETURNING → LastInsertId + 回读，
// 以及 pin 语义里 `now()` → NOW(3)、`$n::boolean` → `? = 1`。
func TestConversation_CreateAndUpdateDialect(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(4001)
	eng.on("FROM conversations WHERE id = ?", []string{"id", "agent_key", "title", "llm_profile_id", "pinned_at", "created_at", "updated_at"}, auxConvRow(4001, "hello"))

	conv, err := d.CreateConversation("mainagent", "hello", nil)
	if err != nil {
		t.Fatalf("CreateConversation failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	if conv.ID != 4001 {
		t.Fatalf("CreateConversation 必须用 LastInsertId 定位自增主键，实际 id=%d", conv.ID)
	}
	insert := auxFind(t, calls, "INSERT INTO CONVERSATIONS")
	if strings.Contains(auxUpper(insert), "RETURNING") {
		t.Errorf("INSERT 仍带 RETURNING：%s", auxCollapse(insert.Query))
	}
	reread := auxFind(t, calls, "FROM CONVERSATIONS WHERE ID = ?")
	if len(reread.Args) != 1 || reread.Args[0] != driver.Value(int64(4001)) {
		t.Errorf("回读必须打在 LastInsertId 上，实际 %v", reread.Args)
	}

	// pin：NOW(3) + 布尔门以 int 传入；解除 pin 置 NULL。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.on("FROM conversations WHERE id = ?", []string{"id", "agent_key", "title", "llm_profile_id", "pinned_at", "created_at", "updated_at"}, auxConvRow(9, "t"))
	pinned := true
	if _, err := d2.UpdateConversation(9, ConversationPatch{Pinned: &pinned}); err != nil {
		t.Fatalf("UpdateConversation failed: %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	update := auxFind(t, calls2, "UPDATE CONVERSATIONS SET")
	if !strings.Contains(auxUpper(update), "NOW(3)") {
		t.Errorf("时间列必须由 NOW(3) 提供：%s", auxCollapse(update.Query))
	}
	for _, frag := range []string{"::BOOLEAN", "NOW()", "RETURNING"} {
		if strings.Contains(auxUpper(update), frag) {
			t.Errorf("UPDATE conversations 残留 PG 方言 %q：%s", frag, auxCollapse(update.Query))
		}
	}

	// touch：updated_at 由 NOW(3) 提供，且不再有 now()。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	if err := d3.TouchConversation(11); err != nil {
		t.Fatalf("TouchConversation failed: %v", err)
	}
	calls3 := eng3.snapshotCalls()
	auxAssertAllOrdinal(t, calls3)
	touch := auxFind(t, calls3, "UPDATE CONVERSATIONS SET")
	if !strings.Contains(auxUpper(touch), "UPDATED_AT=NOW(3)") && !strings.Contains(auxUpper(touch), "UPDATED_AT = NOW(3)") {
		t.Errorf("TouchConversation 必须写 NOW(3)：%s", auxCollapse(touch.Query))
	}
}

// TestConversation_AppendConvActivityUsesLastInsertId 覆盖 RETURNING id → LastInsertId，
// 以及审查 F04 的父锁协议：会话子实体的 INSERT 之前必须已经在同一事务里锁住会话根行。
func TestConversation_AppendConvActivityUsesLastInsertId(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng.setNextInsertID(6001)
	id, err := d.AppendConvActivity(77, Activity{Worker: "w", Kind: "msg", Summary: "s"})
	if err != nil {
		t.Fatalf("AppendConvActivity failed: %v", err)
	}
	if id != 6001 {
		t.Fatalf("活动 id 必须来自 LastInsertId，实际 %d", id)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	insert := auxFind(t, calls, "INSERT INTO CONVERSATION_ACTIVITIES")
	for _, frag := range []string{"RETURNING", "$1", "NULLIF($"} {
		if strings.Contains(auxUpper(insert), frag) {
			t.Errorf("INSERT conversation_activities 残留 PG 方言 %q：%s", frag, auxCollapse(insert.Query))
		}
	}
	// 父锁必须是这条写入的第一条语句，且整笔写入恰好提交一次。
	lockAt := auxIndexOf(t, calls, "SELECT ID FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
	insertAt := auxIndexOf(t, calls, "INSERT INTO CONVERSATION_ACTIVITIES")
	if lockAt > insertAt {
		t.Errorf("会话根行锁必须在 INSERT 之前：lock=%d insert=%d", lockAt, insertAt)
	}
	if counts := eng.txCounts(); counts.Commits != 1 || counts.Rollbacks != 0 {
		t.Errorf("活动写入必须提交恰好一次，实际 %+v", counts)
	}
}

// =====================================================================
// 6) 语句级替身 —— scheduler_state 键 hash（M13）与触发器方言
// =====================================================================

func TestTriggers_SchedulerStateUpsertCarriesKeyIdentity(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)

	const key = "trigger.findings.watermark"
	if err := d.SetSchedState(key, "42"); err != nil {
		t.Fatalf("SetSchedState failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	insert := auxFind(t, calls, "INSERT INTO SCHEDULER_STATE")
	norm := auxUpper(insert)
	for _, want := range []string{"KEY_IDENTITY", "KEY_HASH", "ON DUPLICATE KEY UPDATE", "`KEY`", "`VALUE`"} {
		if !strings.Contains(norm, want) {
			t.Errorf("scheduler_state 写入缺少 %s：%s", want, auxCollapse(insert.Query))
		}
	}
	for _, forbidden := range []string{"ON CONFLICT", "EXCLUDED.", "INSERT IGNORE"} {
		if strings.Contains(norm, forbidden) {
			t.Errorf("scheduler_state 写入残留 PG/降级写法 %q：%s", forbidden, auxCollapse(insert.Query))
		}
	}
	// 唯一键落在 key_hash 上：identity/hash 必须是 canonical 计算出来的那一对。
	wantIdentity := schema.SettingsKeyIdentity(key)
	wantHash := schema.CanonicalHash(wantIdentity)
	if len(insert.Args) != 4 {
		t.Fatalf("SetSchedState 参数必须是 (key, identity, hash, value)，实际 %v", insert.Args)
	}
	if got, ok := insert.Args[1].([]byte); !ok || string(got) != string(wantIdentity) {
		t.Errorf("key_identity 与 canonical 载荷不一致：%v", insert.Args[1])
	}
	if got, ok := insert.Args[2].([]byte); !ok || string(got) != string(wantHash[:]) {
		t.Errorf("key_hash 与 SHA-256(identity) 不一致：%v", insert.Args[2])
	}
	if insert.Args[0] != driver.Value(key) || insert.Args[3] != driver.Value("42") {
		t.Errorf("(key, value) 参数错位：%v", insert.Args)
	}

	// 读取：`key` 是 MySQL 保留字，裸写直接 1064。
	eng.on("FROM scheduler_state", []string{"value"}, []driver.Value{"7"})
	value, err := d.GetSchedState(key)
	if err != nil {
		t.Fatalf("GetSchedState failed: %v", err)
	}
	if value != "7" {
		t.Errorf("GetSchedState 返回值错误：%q", value)
	}
	selectStmt := auxFind(t, eng.snapshotCalls(), "FROM SCHEDULER_STATE")
	if !strings.Contains(auxUpper(selectStmt), "`KEY`") {
		t.Errorf("scheduler_state 读取必须反引号包裹 `key`：%s", auxCollapse(selectStmt.Query))
	}
}

func TestTriggers_CreateTriggerUsesLastInsertIdAndNow3(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(8801)
	eng.on("FROM agent_triggers WHERE id = ?", strings.FieldsFunc(triggerCols, func(r rune) bool { return r == ',' }),
		[]driver.Value{int64(8801), "custom", true, int64(60), false, false, false, false, false,
			"", "", "", "", "", "", `["bash"]`, nil})

	got, err := d.CreateTrigger(&AgentTrigger{AgentKey: "custom", Enabled: true, IntervalSec: 60, ToolNames: []string{"bash"}})
	if err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	if got.ID != 8801 {
		t.Fatalf("CreateTrigger 必须用 LastInsertId 回读，实际 id=%d", got.ID)
	}
	if len(got.ToolNames) != 1 || got.ToolNames[0] != "bash" {
		t.Errorf("tool_names 必须回读为 JSON 数组：%v", got.ToolNames)
	}
	insert := auxFind(t, calls, "INSERT INTO AGENT_TRIGGERS")
	if strings.Contains(auxUpper(insert), "RETURNING") {
		t.Errorf("INSERT agent_triggers 残留 RETURNING：%s", auxCollapse(insert.Query))
	}

	// last_fire 时序列：now() → NOW(3)。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	if err := d2.TouchTriggerFire(5); err != nil {
		t.Fatalf("TouchTriggerFire failed: %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	fire := auxFind(t, calls2, "UPDATE AGENT_TRIGGERS SET LAST_FIRE")
	if !strings.Contains(auxUpper(fire), "NOW(3)") || strings.Contains(auxUpper(fire), "NOW()") {
		t.Errorf("last_fire 必须由 NOW(3) 提供：%s", auxCollapse(fire.Query))
	}

	// 启用中的触发器：布尔列必须显式 = 1（裸 WHERE enabled 在 TiDB 上可用但语义含糊，
	// 且与 r4 审计的"布尔列显式比较"口径不一致）。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	if _, err := d3.ListEnabledTriggers(); err != nil {
		t.Fatalf("ListEnabledTriggers failed: %v", err)
	}
	calls3 := eng3.snapshotCalls()
	auxAssertAllOrdinal(t, calls3)
	list := auxFind(t, calls3, "WHERE ENABLED = 1")
	if strings.Contains(auxUpper(list), "NULLS LAST") {
		t.Errorf("TiDB 不支持 NULLS LAST：%s", auxCollapse(list.Query))
	}
}

// =====================================================================
// 7) 语句级替身 —— 约束 CRUD 的 changed-rows 适配（M31）
// =====================================================================

func TestConstraints_CRUDTiDBDialectAndChangedRowsFallback(t *testing.T) {
	// AddConstraint：INSERT + LastInsertId（无 RETURNING）。
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(3301)
	id, err := d.Exploration(501).AddConstraint("allow", "只做只读探测", "human")
	if err != nil {
		t.Fatalf("AddConstraint failed: %v", err)
	}
	if id != 3301 {
		t.Fatalf("AddConstraint 必须返回 LastInsertId，实际 %d", id)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	insert := auxFind(t, calls, "INSERT INTO TASK_CONSTRAINTS")
	if strings.Contains(auxUpper(insert), "RETURNING") {
		t.Errorf("INSERT task_constraints 残留 RETURNING：%s", auxCollapse(insert.Query))
	}
	if _, err := d.Exploration(501).AddConstraint("maybe", "x", ""); err == nil {
		t.Errorf("非法 kind 必须被拒绝")
	}

	// UpdateConstraint：MySQL 的 ROW_COUNT() 是 changed rows。原样重写（同一毫秒内
	// kind/text 未变）会得到 0，绝不能据此误判"约束不存在"。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.forceAffectedRows(0)
	eng2.on("FROM task_constraints WHERE", []string{"ok"}, []driver.Value{int64(1)})
	if err := d2.Exploration(501).UpdateConstraint(9, "deny", "禁止扫描"); err != nil {
		t.Fatalf("行存在但 changed rows=0 时必须成功，实际 %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	update := auxFind(t, calls2, "UPDATE TASK_CONSTRAINTS SET")
	if !strings.Contains(auxUpper(update), "UPDATED_AT=NOW(3)") && !strings.Contains(auxUpper(update), "UPDATED_AT = NOW(3)") {
		t.Errorf("updated_at 必须由 NOW(3) 提供：%s", auxCollapse(update.Query))
	}
	if strings.Contains(auxUpper(update), "NOW()") {
		t.Errorf("task_constraints 残留 now()：%s", auxCollapse(update.Query))
	}

	// changed rows=0 且回读确认不存在 → 仍然是"约束不存在"。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	eng3.forceAffectedRows(0)
	if err := d3.Exploration(501).UpdateConstraint(9, "deny", "禁止扫描"); err == nil {
		t.Errorf("行确实不存在时必须报错")
	}

	// DeleteConstraint：DELETE 的 changed rows 与命中行数恒等，0 即不存在。
	eng4 := newExpMockEngine()
	d4 := openExpMockDB(t, eng4)
	eng4.forceAffectedRows(0)
	if err := d4.Exploration(501).DeleteConstraint(9); err == nil {
		t.Errorf("删除不存在的约束必须报错")
	}
	eng5 := newExpMockEngine()
	d5 := openExpMockDB(t, eng5)
	if err := d5.Exploration(501).DeleteConstraint(9); err != nil {
		t.Errorf("删除存在的约束失败: %v", err)
	}
	calls5 := eng5.snapshotCalls()
	auxAssertAllOrdinal(t, calls5)
	del := auxFind(t, calls5, "DELETE FROM TASK_CONSTRAINTS")
	if !strings.Contains(auxUpper(del), "EXPLORATION_ID = ?") && !strings.Contains(auxUpper(del), "EXPLORATION_ID=?") {
		t.Errorf("删除必须限定在本 exploration：%s", auxCollapse(del.Query))
	}

	// ListConstraints：COALESCE + 排序形状保持，且无 PG 方言。
	eng6 := newExpMockEngine()
	d6 := openExpMockDB(t, eng6)
	if _, err := d6.Exploration(501).ListConstraints(); err != nil {
		t.Fatalf("ListConstraints failed: %v", err)
	}
	calls6 := eng6.snapshotCalls()
	auxAssertAllOrdinal(t, calls6)
	list := auxFind(t, calls6, "FROM TASK_CONSTRAINTS")
	for _, frag := range []string{"$1", "::", "ILIKE", "NULLS LAST"} {
		if strings.Contains(strings.ToUpper(auxCollapse(list.Query)), frag) {
			t.Errorf("ListConstraints 残留 PG 方言 %q：%s", frag, auxCollapse(list.Query))
		}
	}
}

// =====================================================================
// 8) 语句级替身 —— 旁路问题：锁内元组比较防版本倒退（M28）+ 请求 identity（M17）
// =====================================================================

var auxSideColumns = strings.Split(sideCols, ",")

func auxSideRow(id string, ordinal int64) []driver.Value {
	at := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	return []driver.Value{id, ordinal, "conv-77", int64(7), "client-1", "q", "a", "running", "",
		[]byte(`{"profile_id":1}`), at, at, int64(3), []byte(`{}`), []byte(`{}`)}
}

func auxSnapshot() sidequestion.Snapshot {
	return sidequestion.Snapshot{
		Parent:     sidequestion.Parent{ConversationID: 77},
		RunID:      3,
		Version:    9,
		CapturedAt: time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC),
	}
}

func TestSideQuestions_SaveSnapshotLocksParentAndGuardsVersion(t *testing.T) {
	// 会话行已存在 → 必须锁父行 → 锁会话行 → 带元组守卫的 UPDATE（绝不倒退写入）。
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng.on("SELECT row_id FROM side_question_sessions WHERE session_key = ? FOR UPDATE", []string{"row_id"}, []driver.Value{int64(5001)})

	if err := d.SaveSideSnapshot(context.Background(), auxSnapshot()); err != nil {
		t.Fatalf("SaveSideSnapshot failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	parentLock := auxIndexOf(t, calls, "FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
	sessionLock := auxIndexOf(t, calls, "FROM SIDE_QUESTION_SESSIONS WHERE SESSION_KEY = ? FOR UPDATE")
	updateIdx := auxIndexOf(t, calls, "UPDATE SIDE_QUESTION_SESSIONS SET")
	if !(parentLock < sessionLock && sessionLock < updateIdx) {
		t.Fatalf("必须按 父行锁 → 会话行锁 → 守卫更新 的顺序执行：parent=%d session=%d update=%d\n%s",
			parentLock, sessionLock, updateIdx, auxDump(calls))
	}
	update := calls[updateIdx]
	guard := auxUpper(update)
	if !strings.Contains(guard, "RUN_ID < ?") || !strings.Contains(guard, "(RUN_ID = ? AND VERSION < ?)") {
		t.Errorf("更新必须带 (run_id, version) 字典序前进守卫：%s", auxCollapse(update.Query))
	}
	for _, forbidden := range []string{"ON CONFLICT", "EXCLUDED.", "INSERT IGNORE"} {
		if strings.Contains(guard, forbidden) {
			t.Errorf("旁路快照残留 PG upsert 写法 %q：%s", forbidden, auxCollapse(update.Query))
		}
	}
	// 守卫参数就是本次快照的 (run_id, version)，且带上了刚锁到的 row_id。
	if len(update.Args) != 7 {
		t.Fatalf("守卫更新的参数必须是 (run, version, snapshot, row_id, run, run, version)，实际 %v", update.Args)
	}
	if update.Args[0] != driver.Value(int64(3)) || update.Args[1] != driver.Value(int64(9)) {
		t.Errorf("SET 侧 (run_id, version) 错位：%v", update.Args)
	}
	if update.Args[3] != driver.Value(int64(5001)) {
		t.Errorf("守卫更新必须锁定到刚读到的 row_id：%v", update.Args)
	}
	for _, i := range []int{4, 5, 6} {
		if update.Args[i] != driver.Value(int64(3)) && update.Args[i] != driver.Value(int64(9)) {
			t.Errorf("守卫侧的比较元组必须是 (3, 9)：%v", update.Args)
		}
	}
	if update.Args[4] != driver.Value(int64(3)) || update.Args[5] != driver.Value(int64(3)) || update.Args[6] != driver.Value(int64(9)) {
		t.Errorf("守卫侧必须按 (run_id, run_id, version) 绑定：%v", update.Args)
	}
	if counts := eng.txCounts(); counts.Commits != 1 {
		t.Errorf("守卫更新后必须提交，实际 %+v", counts)
	}

	// changed rows=0（版本倒退被守卫挡下）不是错误：与迁移期 WHERE 过滤后的 upsert 同义。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng2.on("SELECT row_id FROM side_question_sessions WHERE session_key = ? FOR UPDATE", []string{"row_id"}, []driver.Value{int64(5001)})
	eng2.forceAffectedRows(0)
	if err := d2.SaveSideSnapshot(context.Background(), auxSnapshot()); err != nil {
		t.Errorf("版本倒退被守卫挡下时必须静默返回，实际 %v", err)
	}

	// 会话行不存在 → 首写 INSERT，且必须补齐 NOT NULL 无 DDL 默认值的 memory 列。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	eng3.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	if err := d3.SaveSideSnapshot(context.Background(), auxSnapshot()); err != nil {
		t.Fatalf("首写 SaveSideSnapshot failed: %v", err)
	}
	calls3 := eng3.snapshotCalls()
	auxAssertAllOrdinal(t, calls3)
	insert := auxFind(t, calls3, "INSERT INTO SIDE_QUESTION_SESSIONS")
	if strings.Contains(auxUpper(insert), "ON CONFLICT") {
		t.Errorf("首写必须是裸 INSERT + 唯一键仲裁：%s", auxCollapse(insert.Query))
	}
	for _, col := range []string{"SESSION_KEY", "CONVERSATION_ID", "RUN_ID", "VERSION", "SNAPSHOT", "MEMORY"} {
		if !strings.Contains(auxUpper(insert), col) {
			t.Errorf("首写缺少 NOT NULL 列 %s：%s", col, auxCollapse(insert.Query))
		}
	}
	if auxHasWrite(calls3, "UPDATE SIDE_QUESTION_SESSIONS SET") {
		t.Errorf("首写路径不得再补一次 UPDATE：%s", auxDump(calls3))
	}

	// INSERT 失败（含并发首写撞唯一键）必须整笔回滚，绝不提交。
	eng4 := newExpMockEngine()
	d4 := openExpMockDB(t, eng4)
	eng4.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng4.failOnExec("INSERT INTO side_question_sessions")
	if err := d4.SaveSideSnapshot(context.Background(), auxSnapshot()); err == nil {
		t.Errorf("首写失败必须返回错误")
	}
	if counts := eng4.txCounts(); counts.Commits != 0 || counts.Rollbacks == 0 {
		t.Errorf("首写失败必须回滚，实际 %+v", counts)
	}

	// 父会话已被删除 → ErrSideParentGone，且不得留下任何写入。
	eng5 := newExpMockEngine()
	d5 := openExpMockDB(t, eng5)
	if err := d5.SaveSideSnapshot(context.Background(), auxSnapshot()); !errors.Is(err, ErrSideParentGone) {
		t.Errorf("父会话不存在必须返回 ErrSideParentGone，实际 %v", err)
	}
	if writes := auxWrites(eng5.snapshotCalls()); len(writes) != 0 {
		t.Errorf("父会话不存在时不得写入：\n%s", auxDump(writes))
	}
}

func auxIndexOfString(calls []expMockCall, substr string) string {
	for _, c := range calls {
		if strings.Contains(auxUpper(c), strings.ToUpper(substr)) {
			return c.Query
		}
	}
	return ""
}

// auxHasWrite 判定是否存在命中子串的写语句。
func auxHasWrite(calls []expMockCall, substr string) bool {
	upper := strings.ToUpper(substr)
	for _, c := range auxWrites(calls) {
		if strings.Contains(auxUpper(c), upper) {
			return true
		}
	}
	return false
}

// auxNumEq 比较 driver 实参与无符号整数：闭包侧的 id 走 uint64、根行侧走 int64，
// 两种整型都可能出现，必须归一后比较（直接 == driver.Value 会把等价当成不等）。
func auxNumEq(v driver.Value, want uint64) bool {
	switch n := v.(type) {
	case int64:
		return n >= 0 && uint64(n) == want
	case uint64:
		return n == want
	case int:
		return n >= 0 && uint64(n) == want
	}
	return false
}

func TestSideQuestions_StartRequestCarriesCanonicalIdentity(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(7777)
	eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
	eng.on("SELECT generation FROM side_question_sessions WHERE session_key = ? FOR UPDATE", []string{"generation"}, []driver.Value{int64(7)})
	eng.on("FROM side_question_requests WHERE ordinal = ?", auxSideColumns, auxSideRow("req-1", 4001))
	eng.on("FROM side_question_requests WHERE session_key = ? AND generation = ? AND client_id = ?", auxSideColumns)
	eng.on("AND status = 'running'", []string{"busy"}, []driver.Value{int64(0)})

	exchange, created, err := d.StartSideRequest(context.Background(), auxSnapshot(), "client-1", "问题")
	if err != nil {
		t.Fatalf("StartSideRequest failed: %v", err)
	}
	if !created || exchange == nil {
		t.Fatalf("全新请求必须 created=true，实际 created=%v exchange=%+v", created, exchange)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	insert := auxFind(t, calls, "INSERT INTO SIDE_QUESTION_REQUESTS")
	upper := auxUpper(insert)
	if strings.Contains(upper, "RETURNING") {
		t.Errorf("INSERT side_question_requests 残留 RETURNING：%s", auxCollapse(insert.Query))
	}
	// M17：唯一键 uq_side_requests_identity(identity_hash) 的两列必须由 canonical 补齐。
	for _, col := range []string{"IDENTITY", "IDENTITY_HASH", "ANSWER", "ERROR", "USAGE", "CONTEXT_INFO", "MODEL"} {
		if !strings.Contains(upper, col) {
			t.Errorf("INSERT side_question_requests 缺少列 %s：%s", col, auxCollapse(insert.Query))
		}
	}
	wantIdentity := schema.SideQuestionRequestsIdentity("conv-77", 7, "client-1")
	wantHash := schema.CanonicalHash(wantIdentity)
	if len(insert.Args) != 14 {
		t.Fatalf("参数必须是 (id, session_key, generation, client_id, identity, identity_hash, question, answer, status, error, model, `usage`, context_info, snapshot_at)，实际 %d 个：%v", len(insert.Args), insert.Args)
	}
	if got, ok := insert.Args[4].([]byte); !ok || string(got) != string(wantIdentity) {
		t.Errorf("identity 必须是 canonical 载荷 (session_key, generation, client_id)，实际 %v", insert.Args[4])
	}
	if got, ok := insert.Args[5].([]byte); !ok || string(got) != string(wantHash[:]) {
		t.Errorf("identity_hash 必须是 SHA-256(identity)，实际 %v", insert.Args[5])
	}
	if insert.Args[2] != driver.Value(int64(7)) {
		t.Errorf("generation 必须取自锁内读到的会话代次，实际 %v", insert.Args[2])
	}
	// 回读必须打在 LastInsertId（自增 ordinal）上。
	reread := auxFind(t, calls, "FROM SIDE_QUESTION_REQUESTS WHERE ORDINAL = ?")
	if len(reread.Args) != 1 || reread.Args[0] != driver.Value(int64(7777)) {
		t.Errorf("回读必须用 LastInsertId，实际 %v", reread.Args)
	}
}

func TestSideQuestions_UpdateSideRequestChangedRowsContract(t *testing.T) {
	// changed rows=1 → 更新成功；0（终态/迟到回调/代次已切）→ false，但不是错误。
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	exchange := auxSideExchange()
	ok, err := d.UpdateSideRequest(context.Background(), exchange)
	if err != nil || !ok {
		t.Fatalf("命中 running 行必须返回 true，实际 %v %v", ok, err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	update := auxFind(t, calls, "UPDATE SIDE_QUESTION_REQUESTS")
	upper := auxUpper(update)
	if !strings.Contains(upper, "R.STATUS='RUNNING'") {
		t.Errorf("更新必须限定 running 行：%s", auxCollapse(update.Query))
	}
	if !strings.Contains(upper, "R.SEQUENCE < ?") {
		t.Errorf("更新必须带 sequence 单调守卫（保证 changed rows 与命中行数恒等）：%s", auxCollapse(update.Query))
	}
	if !strings.Contains(upper, "EXISTS(SELECT 1 FROM SIDE_QUESTION_SESSIONS") &&
		!strings.Contains(upper, "EXISTS (SELECT 1 FROM SIDE_QUESTION_SESSIONS") {
		t.Errorf("更新必须校验代次仍存活：%s", auxCollapse(update.Query))
	}

	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.forceAffectedRows(0)
	ok2, err2 := d2.UpdateSideRequest(context.Background(), exchange)
	if err2 != nil || ok2 {
		t.Errorf("未命中时必须返回 (false, nil)，实际 %v %v", ok2, err2)
	}
}

func auxSideExchange() sidequestion.Exchange {
	return sidequestion.Exchange{ID: "req-1", SessionKey: "conv-77", ClientID: "client-1",
		Generation: 7, Answer: "a", Status: "completed", Sequence: 8}
}

func TestSideQuestions_SaveSideMemoryRejectsOnlyDeadRequests(t *testing.T) {
	// changed rows=0 有两种可能：memory 原样重写（成功）与代次已死（父会话没了）。
	// MySQL 的 ROW_COUNT() 分不开，必须回读"请求仍在 running 且代次仍被会话持有"。
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.forceAffectedRows(0)
	eng.on("FROM side_question_requests r JOIN side_question_sessions s", []string{"ok"}, []driver.Value{int64(1)})
	if err := d.SaveSideMemory(context.Background(), auxSideExchange(), sidequestion.Memory{History: "h"}); err != nil {
		t.Errorf("memory 未变化（changed rows=0）但请求仍存活时必须成功，实际 %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.forceAffectedRows(0)
	if err := d2.SaveSideMemory(context.Background(), auxSideExchange(), sidequestion.Memory{History: "h"}); !errors.Is(err, ErrSideParentGone) {
		t.Errorf("请求已死时必须 ErrSideParentGone，实际 %v", err)
	}
}

func TestSideQuestions_HistoryAndClearDialect(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM side_question_requests", auxSideColumns, auxSideRow("req-1", 5))
	if _, err := d.SideHistory(context.Background(), "conv-77", 100, 20); err != nil {
		t.Fatalf("SideHistory failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	history := auxFind(t, calls, "FROM SIDE_QUESTION_REQUESTS")
	if strings.Contains(auxUpper(history), "$2::BIGINT") || strings.Contains(auxUpper(history), "::BIGINT") {
		t.Errorf("游标条件残留 PG 转型：%s", auxCollapse(history.Query))
	}
	if !strings.Contains(auxUpper(history), "? = 0 OR ORDINAL < ?") {
		t.Errorf("游标必须是 (? = 0 OR ordinal < ?)：%s", auxCollapse(history.Query))
	}

	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	if err := d2.ClearSideHistory(context.Background(), "conv-77"); err != nil {
		t.Fatalf("ClearSideHistory failed: %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	if auxIndexOf(t, calls2, "UPDATE SIDE_QUESTION_SESSIONS SET GENERATION=GENERATION+1") < 0 {
		t.Errorf("清历史必须推进代次水位")
	}
	if auxIndexOf(t, calls2, "DELETE FROM SIDE_QUESTION_REQUESTS") < 0 {
		t.Errorf("清历史必须删除请求行")
	}
	if counts := eng2.txCounts(); counts.Commits != 1 {
		t.Errorf("清历史必须在同一事务里提交，实际 %+v", counts)
	}
}

// =====================================================================
// 9) 语句级替身 —— 技能统计、提及搜索、拦截详情/执行
// =====================================================================

func TestSkillUsage_DialectAndArgAlignment(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.setNextInsertID(9001)
	if err := d.InsertSkillUsage(&SkillUsage{Skill: "recon", AgentKey: "planner", SessionID: "s", ArgsLen: 3, Found: true}); err != nil {
		t.Fatalf("InsertSkillUsage failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	insert := auxFind(t, calls, "INSERT INTO SKILL_USAGE")
	if !strings.Contains(auxUpper(insert), "NULLIF(?,'')") {
		t.Errorf("空 task_id 仍须落 NULL：%s", auxCollapse(insert.Query))
	}

	// 技能统计：STRING_AGG → GROUP_CONCAT，裸布尔列 → 显式 = 1。
	eng.on("WHERE found = 1", []string{"skill", "calls", "tasks", "agents", "last_used"},
		[]driver.Value{"recon", int64(2), int64(1), "planner,mainagent", time.Now().UTC()})
	stats, err := d.SkillStats()
	if err != nil {
		t.Fatalf("SkillStats failed: %v", err)
	}
	if len(stats) != 1 || stats[0].Agents[0] != "planner" || stats[0].Agents[1] != "mainagent" {
		t.Fatalf("技能统计解析错误：%+v", stats)
	}
	calls = eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	agg := auxFind(t, calls, "FROM SKILL_USAGE WHERE FOUND = 1")
	upper := auxUpper(agg)
	if !strings.Contains(upper, "GROUP_CONCAT") {
		t.Errorf("聚合必须换成 GROUP_CONCAT：%s", auxCollapse(agg.Query))
	}
	for _, forbidden := range []string{"STRING_AGG", "FILTER (WHERE", "ARRAY_AGG"} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("技能统计残留 PG 聚合 %q：%s", forbidden, auxCollapse(agg.Query))
		}
	}

	// 未命中清单：裸 WHERE NOT found → found = 0。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.on("WHERE found = 0", []string{"skill", "calls", "agents", "last_used"},
		[]driver.Value{"gone", int64(1), "", nil})
	missing, err := d2.MissingSkillStats(10)
	if err != nil {
		t.Fatalf("MissingSkillStats failed: %v", err)
	}
	if len(missing) != 1 || missing[0].Skill != "gone" {
		t.Fatalf("未命中清单解析错误：%+v", missing)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	if strings.Contains(auxUpper(auxFind(t, calls2, "FROM SKILL_USAGE WHERE FOUND = 0")), "NOT FOUND") {
		t.Errorf("布尔列必须显式比较")
	}

	// 近况与按任务聚合：布尔列同样要显式。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	if _, err := d3.RecentSkillCalls("recon", 20); err != nil {
		t.Fatalf("RecentSkillCalls failed: %v", err)
	}
	if _, err := d3.SkillCallsByTask(7); err != nil {
		t.Fatalf("SkillCallsByTask failed: %v", err)
	}
	calls3 := eng3.snapshotCalls()
	auxAssertAllOrdinal(t, calls3)
	byTask := auxFind(t, calls3, "FROM SKILL_USAGE WHERE TASK_ID = ?")
	if !strings.Contains(auxUpper(byTask), "FOUND = 1") {
		t.Errorf("按任务聚合必须显式 found = 1：%s", auxCollapse(byTask.Query))
	}
	recent := auxFind(t, calls3, "FROM SKILL_USAGE WHERE SKILL = ?")
	if !strings.Contains(auxUpper(recent), "LIMIT ?") {
		t.Errorf("近况 LIMIT 必须是占位符：%s", auxCollapse(recent.Query))
	}
}

func TestChatMentions_SearchIsTiDBDialectAndArgAligned(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM FINDINGS", []string{"kind", "id", "label", "description"})
	eng.on("FROM COMPANIES", []string{"kind", "id", "label", "description"})
	eng.on("FROM ASSETS", []string{"kind", "id", "label", "description"})

	if _, err := d.SearchChatMentionsPage(context.Background(), "", "SQLi", ""); err != nil {
		t.Fatalf("SearchChatMentionsPage failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	search := auxFind(t, calls, "FROM (")
	upper := auxUpper(search)
	for _, forbidden := range []string{"ILIKE", "::TEXT", "$1"} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("提及搜索残留 PG 方言 %q：%s", forbidden, auxCollapse(search.Query))
		}
	}
	if !strings.Contains(upper, "CAST(ID AS CHAR)") {
		t.Errorf("id 精确匹配必须走 CAST(id AS CHAR)：%s", auxCollapse(search.Query))
	}
	if !strings.Contains(upper, "LOWER(") {
		t.Errorf("大小写不敏感包含匹配必须显式 LOWER(…)：%s", auxCollapse(search.Query))
	}
	// 搜索模式必须在 Go 侧预小写，并对 % _ \ 做转义（ESCAPE '\\'）。
	var sawPattern bool
	for _, a := range search.Args {
		s, ok := a.(string)
		if !ok || !strings.HasPrefix(s, "%") || !strings.HasSuffix(s, "%") {
			continue
		}
		sawPattern = true
		if s != "%sqli%" {
			t.Errorf("LIKE 模式必须预小写，实际 %q", s)
		}
	}
	if !sawPattern {
		t.Errorf("搜索语句没有带上 LIKE 模式：%v", search.Args)
	}
	if !strings.Contains(upper, `ESCAPE '\\'`) && !strings.Contains(upper, `ESCAPE "\\"`) {
		t.Errorf("用户输入里的 %%/_ 必须按字面量匹配（缺少 ESCAPE 子句）：%s", auxCollapse(search.Query))
	}

	// 游标必须与 kind/query 绑定，否则直接判 Invalid，绝不带病查库。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	if _, err := d2.SearchChatMentionsPage(context.Background(), "asset", "x", "not-base64!!"); !errors.Is(err, ErrInvalidChatMentionCursor) {
		t.Errorf("坏游标必须判 ErrInvalidChatMentionCursor，实际 %v", err)
	}
	if len(eng2.snapshotCalls()) != 0 {
		t.Errorf("坏游标不得触库")
	}
}

func TestInterceptDetail_AuditMergeIsJSONFunctions(t *testing.T) {
	approvalCols := []string{"id", "rule_id", "conversation_id", "task_id", "agent_name", "tool_name",
		"tool_input", "status", "reason", "decided_at", "created_at", "decision_source",
		"conv_title", "conv_agent_key", "rule_name", "audit"}
	at := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	audit := []byte(`{"run_id":"run-1","tool_use_id":"call-1","correlation":"exact","initial_action":"ask","effective_action":"allow","execution_status":"awaiting_result"}`)
	approvalRow := func() []driver.Value {
		return []driver.Value{int64(5), nil, int64(77), nil, "agent", "bash", []byte(`{}`), "pending", "",
			nil, at, "model", "", "", "", audit}
	}

	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM intercept_pending ip", approvalCols, approvalRow())
	got, err := d.GetInterceptDetail(5)
	if err != nil || got == nil {
		t.Fatalf("GetInterceptDetail failed: %v", err)
	}
	if got.Audit == nil || got.Audit.Correlation != "exact" {
		t.Fatalf("audit 载荷必须解回结构：%+v", got.Audit)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	detail := auxFind(t, calls, "FROM INTERCEPT_PENDING IP")
	for _, forbidden := range []string{"ROW_TO_JSON", "TO_JSONB", "$1", "::JSON"} {
		if strings.Contains(auxUpper(detail), forbidden) {
			t.Errorf("详情读取残留 PG 方言 %q：%s", forbidden, auxCollapse(detail.Query))
		}
	}
	if !strings.Contains(auxUpper(detail), "WHERE IP.ID = ?") {
		t.Errorf("详情必须按 id 点查：%s", auxCollapse(detail.Query))
	}

	// 决策：audit 合并必须走 JSON_MERGE_PATCH，条件判定走 JSON_VALID + JSON_UNQUOTE。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	if _, err := d2.ResolveIntercept(5, "allowed", "allow", "人工允许执行"); err != nil {
		t.Fatalf("ResolveIntercept failed: %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	resolve := auxFind(t, calls2, "UPDATE INTERCEPT_PENDING SET")
	upper := auxUpper(resolve)
	if !strings.Contains(upper, "JSON_MERGE_PATCH") {
		t.Errorf("audit 合并必须用 JSON_MERGE_PATCH：%s", auxCollapse(resolve.Query))
	}
	for _, forbidden := range []string{"||", "::JSONB", "NOW()", "$1"} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("决策更新残留 PG 方言 %q：%s", forbidden, auxCollapse(resolve.Query))
		}
	}
	if !strings.Contains(upper, "AUDIT IS NULL THEN NULL") {
		t.Errorf("legacy 空 audit 必须保持 NULL（不得凭空造对象）：%s", auxCollapse(resolve.Query))
	}
	if !strings.Contains(upper, "DECIDED_AT=NOW(3)") && !strings.Contains(upper, "DECIDED_AT = NOW(3)") {
		t.Errorf("decided_at 必须由 NOW(3) 提供：%s", auxCollapse(resolve.Query))
	}
	if !strings.Contains(upper, "WHERE ID = ? AND STATUS='PENDING'") {
		t.Errorf("决策必须 CAS 在 pending 行上：%s", auxCollapse(resolve.Query))
	}
	if !strings.Contains(upper, "JSON_VALID(AUDIT)") || !strings.Contains(upper, "JSON_UNQUOTE(JSON_EXTRACT(AUDIT, '$.CORRELATION'))") {
		t.Errorf("correlation 判定必须走 JSON_VALID 守卫 + JSON_UNQUOTE(JSON_EXTRACT(…))：%s", auxCollapse(resolve.Query))
	}
	// 变更行恒等：pending → 目标态一定改到 status，changed rows 与命中行数一致。
	if counts := eng2.txCounts(); counts.Commits != 0 {
		t.Errorf("非事务路径不应开事务，实际 %+v", counts)
	}

	// 执行结果回填：同一条守卫，按 run_id/tool_use_id/effective_action/execution_status 精确匹配。
	eng3 := newExpMockEngine()
	d3 := openExpMockDB(t, eng3)
	if err := d3.CompleteIntercept(5, "run-1", "call-1", "succeeded", "ok", false); err != nil {
		t.Fatalf("CompleteIntercept failed: %v", err)
	}
	calls3 := eng3.snapshotCalls()
	auxAssertAllOrdinal(t, calls3)
	complete := auxFind(t, calls3, "UPDATE INTERCEPT_PENDING SET")
	upperComplete := auxUpper(complete)
	for _, want := range []string{"JSON_MERGE_PATCH", "JSON_VALID(AUDIT)", "'$.RUN_ID'", "'$.TOOL_USE_ID'", "'$.EFFECTIVE_ACTION'", "'$.EXECUTION_STATUS'"} {
		if !strings.Contains(upperComplete, want) {
			t.Errorf("执行结果回填缺少 %s：%s", want, auxCollapse(complete.Query))
		}
	}
	for _, forbidden := range []string{"->>", "::JSONB", "$2"} {
		if strings.Contains(upperComplete, forbidden) {
			t.Errorf("执行结果回填残留 PG 方言 %q：%s", forbidden, auxCollapse(complete.Query))
		}
	}
}

func TestInterceptExecution_ConversationQueryIsTiDBDialect(t *testing.T) {
	at := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	audit := []byte(`{"run_id":"run-1","tool_use_id":"call-1","correlation":"exact","effective_action":"allow","execution_status":"awaiting_result"}`)

	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	eng.on("FROM intercept_pending ip", []string{"id", "rule_id", "conversation_id", "task_id", "agent_name",
		"tool_name", "tool_input", "status", "reason", "decided_at", "created_at", "decision_source",
		"conv_title", "conv_agent_key", "rule_name", "audit"},
		[]driver.Value{int64(5), nil, int64(77), nil, "agent", "bash", []byte(`{}`), "allowed", "",
			nil, at, "model", "", "", "", audit})
	eng.on("FROM conversation_activities", []string{"id", "node_id", "worker", "kind", "tool", "tool_use_id",
		"is_error", "summary", "created_at", "main_seg"},
		[]driver.Value{int64(400), nil, "mainagent", "tool_use", "bash", "call-1", false, "", at, nil})

	exec, err := d.GetInterceptExecution(5)
	if err != nil {
		t.Fatalf("GetInterceptExecution failed: %v", err)
	}
	if exec == nil || exec.Seq != 400 {
		t.Fatalf("执行定位必须回到原始 activity 行，实际 %+v", exec)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)
	activity := auxFind(t, calls, "FROM CONVERSATION_ACTIVITIES")
	for _, forbidden := range []string{"::BIGINT", "::INTEGER", "$1", "$2"} {
		if strings.Contains(auxUpper(activity), forbidden) {
			t.Errorf("执行定位查询残留 PG 方言 %q：%s", forbidden, auxCollapse(activity.Query))
		}
	}
	if !strings.Contains(auxUpper(activity), "TOOL_USE_ID = ?") || !strings.Contains(auxUpper(activity), "KIND IN ('TOOL_USE','TOOL_RESULT')") {
		t.Errorf("执行定位必须按 tool_use_id + kind 精确取行：%s", auxCollapse(activity.Query))
	}

	// 任务侧分支：EXISTS 存活探测 + 无 :: 转型。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	taskID := "4242"
	eng2.on("FROM intercept_pending ip", []string{"id", "rule_id", "conversation_id", "task_id", "agent_name",
		"tool_name", "tool_input", "status", "reason", "decided_at", "created_at", "decision_source",
		"conv_title", "conv_agent_key", "rule_name", "audit"},
		[]driver.Value{int64(6), nil, nil, taskID, "agent", "bash", []byte(`{}`), "allowed", "",
			nil, at, "model", "", "", "", audit})
	eng2.on("SELECT EXISTS(SELECT 1 FROM tasks", []string{"ok"}, []driver.Value{int64(0)})
	if _, err := d2.GetInterceptExecution(6); !errors.Is(err, ErrInterceptTaskDeleted) {
		t.Errorf("任务已删除/归档必须 ErrInterceptTaskDeleted，实际 %v", err)
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	probe := auxFind(t, calls2, "SELECT EXISTS(SELECT 1 FROM TASKS")
	if strings.Contains(auxUpper(probe), "IF(ARCHIVED") {
		t.Errorf("PG 的 IF() 必须换成 COALESCE：%s", auxCollapse(probe.Query))
	}
}

// auxMinInt 包一层最小值：包内已有面向 int64 的 min 辅助函数，不能占用这个名字。
func auxMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// =====================================================================
// 9) 审查修复轮次 1 的回归守卫
//    F01 会话历史分页的重复游标绑定 / F02 UpdateTrigger 参数顺序 /
//    F03 并发完成后的 Retest 终态不可篡改 / F04 会话子实体写者父锁
// =====================================================================

// auxSetClause 返回 UPDATE 语句 SET 与 WHERE 之间的赋值列表（归一化大写、单行）。
// 参数顺序错位的直接表现就是"SET 列 ↔ 绑定值"配错，所以逐位断言必须先拿到列的次序。
func auxSetClause(t *testing.T, query string) string {
	t.Helper()
	u := strings.ToUpper(auxCollapse(query))
	start := strings.Index(u, " SET ")
	end := strings.Index(u, " WHERE ")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("不是可解析的 UPDATE 语句：%s", auxCollapse(query))
	}
	return strings.TrimSpace(u[start+len(" SET ") : end])
}

// auxSetColumns 按出现顺序返回 UPDATE SET 子句里的列名（小写）。
func auxSetColumns(t *testing.T, query string) []string {
	t.Helper()
	parts := strings.Split(auxSetClause(t, query), ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		eq := strings.Index(p, "=")
		if eq < 0 {
			t.Fatalf("SET 子句里出现非赋值片段 %q：%s", p, auxCollapse(query))
		}
		cols = append(cols, strings.ToLower(strings.TrimSpace(p[:eq])))
	}
	return cols
}

// auxConvActivityCols 是 ConvActivityList/ConvActivityPage 那 12 列摘要列的名字。
var auxConvActivityCols = []string{
	"id", "worker", "kind", "tool", "tool_use_id", "is_error",
	"summary", "created_at", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens",
}

// auxConvActivityRow 造一行摘要活动（列序与 auxConvActivityCols 一致）。
func auxConvActivityRow(id int64, created time.Time) []driver.Value {
	tokens := int64(7)
	return []driver.Value{id, "w", "text", "", "", false, fmt.Sprintf("s-%d", id), created, tokens, tokens, tokens, tokens}
}

// TestConversation_ConvActivityPageBindsRepeatedCursor 锁死审查 F01：
//
//	`conversation_id = ? AND (? <= 0 OR id < ?) ORDER BY id DESC LIMIT ?`
//
// 里 `before` 被引用了两次，PG 可以复用同一个编号参数，MySQL/TiDB 的 `?` 是位置参数，
// **每个占位符必须单独绑定**。少绑一位就是运行期绑定错误，任意一次历史加载都直接 500。
func TestConversation_ConvActivityPageBindsRepeatedCursor(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)
	created := time.Date(2026, 10, 20, 3, 4, 5, 0, time.UTC)
	// 3 行 > limit=2 → hasMore 必须为 true，且返回窗口按 id 升序。
	eng.on("FROM conversation_activities", auxConvActivityCols,
		auxConvActivityRow(int64(5), created),
		auxConvActivityRow(int64(4), created),
		auxConvActivityRow(int64(3), created))

	activities, hasMore, err := d.ConvActivityPage(77, 0, 2)
	if err != nil {
		t.Fatalf("首屏历史加载失败：%v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	page := auxFind(t, calls, "FROM CONVERSATION_ACTIVITIES")
	normalized := auxUpper(page)
	for _, want := range []string{"CONVERSATION_ID = ?", "(? <= 0 OR ID < ?)", "ORDER BY ID DESC LIMIT ?"} {
		if !strings.Contains(normalized, want) {
			t.Errorf("反向分页缺少 %s：%s", want, auxCollapse(page.Query))
		}
	}
	// 逐位绑定：(conversation_id, before 门, before 上界, limit+1)。
	if len(page.Args) != 4 {
		t.Fatalf("分页必须绑定 4 个参数，实际 %d 个：%v", len(page.Args), page.Args)
	}
	if !auxNumEq(page.Args[0], 77) {
		t.Errorf("参数 0 必须是会话 id 77，实际 %v", page.Args[0])
	}
	if !auxNumEq(page.Args[1], 0) || !auxNumEq(page.Args[2], 0) {
		t.Errorf("游标被引用两次，两个占位符都必须绑定 before：%v", page.Args)
	}
	if page.Args[1] != page.Args[2] {
		t.Errorf("两个游标占位符必须绑同一个 before：%v", page.Args)
	}
	if !auxNumEq(page.Args[3], 3) {
		t.Errorf("LIMIT 必须绑 limit+1=%d 以探测更早历史，实际 %v", 3, page.Args[3])
	}
	if !hasMore {
		t.Errorf("取回 limit+1 行时 hasMore 必须为 true")
	}
	if len(activities) != 2 || activities[0].ID != 4 || activities[1].ID != 5 {
		ids := make([]int64, 0, len(activities))
		for _, a := range activities {
			ids = append(ids, a.ID)
		}
		t.Errorf("窗口必须截断到 limit 并按 id 升序返回，实际 %v", ids)
	}

	// 非零游标：同一个 before 依然要出现在两个位置上。
	eng2 := newExpMockEngine()
	d2 := openExpMockDB(t, eng2)
	eng2.on("FROM conversation_activities", auxConvActivityCols,
		auxConvActivityRow(int64(3), created), auxConvActivityRow(int64(2), created))
	if _, hasMore2, err := d2.ConvActivityPage(77, 123, 2); err != nil {
		t.Fatalf("翻页历史加载失败：%v", err)
	} else if hasMore2 {
		t.Errorf("取回不足 limit+1 行时 hasMore 必须为 false")
	}
	calls2 := eng2.snapshotCalls()
	auxAssertAllOrdinal(t, calls2)
	page2 := auxFind(t, calls2, "FROM CONVERSATION_ACTIVITIES")
	if len(page2.Args) != 4 || !auxNumEq(page2.Args[1], 123) || !auxNumEq(page2.Args[2], 123) {
		t.Fatalf("游标 123 必须在第 1、2 位各绑定一次，实际 %v", page2.Args)
	}
}

// TestTriggers_UpdateTriggerBindsSetColumnsInOrderAndIDLast 锁死审查 F02：
// PG 的 `SET enabled=$2 … tool_names=$15 WHERE id=$1` 换成位置占位符后，实参必须
// 按 SET 列的出现顺序重排，主键 id 落到**最后一位**。
func TestTriggers_UpdateTriggerBindsSetColumnsInOrderAndIDLast(t *testing.T) {
	eng := newExpMockEngine()
	d := openExpMockDB(t, eng)

	tr := &AgentTrigger{
		ID: 77, AgentKey: "custom", Enabled: true, IntervalSec: 300,
		OnFinding: true, OnGoalMet: false, OnTaskTimeout: true, OnToolCall: false, OnTaskCreate: true,
		IntervalMessage: "im", FindingMessage: "fm", GoalMessage: "gm",
		TaskTimeoutMessage: "tm", ToolCallMessage: "cm", TaskCreateMessage: "nm",
		ToolNames: []string{"bash"},
	}
	if err := d.UpdateTrigger(tr); err != nil {
		t.Fatalf("UpdateTrigger failed: %v", err)
	}
	calls := eng.snapshotCalls()
	auxAssertAllOrdinal(t, calls)

	update := auxFind(t, calls, "UPDATE AGENT_TRIGGERS SET")
	wantCols := []string{
		"enabled", "interval_sec", "on_finding", "on_goal_met", "on_task_timeout",
		"on_tool_call", "on_task_create", "interval_message", "finding_message",
		"goal_message", "task_timeout_message", "tool_call_message", "task_create_message", "tool_names",
	}
	if got := auxSetColumns(t, update.Query); !reflect.DeepEqual(got, wantCols) {
		t.Fatalf("SET 列次序变了（参数逐位对齐的前提）：%v", got)
	}
	if !strings.Contains(auxUpper(update), "WHERE ID=?") {
		t.Fatalf("必须按主键更新：%s", auxCollapse(update.Query))
	}
	if len(update.Args) != len(wantCols)+1 {
		t.Fatalf("参数数量应为 14 列 + 1 主键，实际 %d：%v", len(update.Args), update.Args)
	}
	// 逐位比对：SET 的第 i 列 ↔ 实参第 i 位，最后一位是主键。
	want := []driver.Value{
		true, int64(300), true, false, true, false, true,
		"im", "fm", "gm", "tm", "cm", "nm", `["bash"]`,
		int64(77),
	}
	for i := range want {
		if !auxValueEq(update.Args[i], want[i]) {
			t.Errorf("参数 %d（列 %s）错绑：want %v got %v", i, wantCols[i], want[i], update.Args[i])
		}
	}
	// 反向守卫：trigger id 只能出现在最后一位，绝不允许出现在 SET 的参数里。
	for i := 0; i < len(update.Args)-1; i++ {
		if auxNumEq(update.Args[i], 77) {
			t.Errorf("trigger id 漏进 SET 第 %d 位（PG 参数排列残留）：%v", i, update.Args)
			break
		}
	}
}

func auxValueEq(got driver.Value, want driver.Value) bool {
	if b, ok := want.([]byte); ok {
		g, ok2 := got.([]byte)
		return ok2 && string(g) == string(b)
	}
	if bs, ok := want.(string); ok {
		if g, ok2 := got.([]byte); ok2 {
			return string(g) == bs
		}
	}
	return reflect.DeepEqual(got, want)
}

// auxOpenSQLite 开一个只带会话闭包相关列的内存库，供 SQL 语义探针使用。
func auxOpenSQLite(t *testing.T) *sql.DB {
	t.Helper()
	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("打开内存 SQLite 失败：%v", err)
	}
	sqliteDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqliteDB.Close() })
	for _, ddl := range []string{
		`CREATE TABLE conversations (id INTEGER PRIMARY KEY, agent_key TEXT, title TEXT)`,
		`CREATE TABLE conversation_activities (id INTEGER PRIMARY KEY, conversation_id INTEGER)`,
		`CREATE TABLE intercept_pending (id INTEGER PRIMARY KEY, conversation_id INTEGER)`,
		`CREATE TABLE side_question_sessions (row_id INTEGER PRIMARY KEY, session_key TEXT, conversation_id INTEGER)`,
		`CREATE TABLE side_question_requests (ordinal INTEGER, session_key TEXT)`,
		`CREATE TABLE finding_retests (id INTEGER PRIMARY KEY, conversation_id INTEGER, status TEXT)`,
	} {
		if _, err := sqliteDB.Exec(ddl); err != nil {
			t.Fatalf("建表失败 %s: %v", ddl, err)
		}
	}
	return sqliteDB
}

func auxRetestRow(t *testing.T, sqliteDB *sql.DB, id int64) (string, sql.NullInt64) {
	t.Helper()
	var status string
	var conv sql.NullInt64
	err := sqliteDB.QueryRow(`SELECT status, conversation_id FROM finding_retests WHERE id = ?`, id).Scan(&status, &conv)
	if err != nil {
		t.Fatalf("回读 retest %d 失败：%v", id, err)
	}
	return status, conv
}

// TestConversation_DeleteConversation_TerminalRetestSurvivesConcurrentFinish 是审查
// F03 的语义证据：跑**真实**的 ComputeConversationClosure + ExecuteConversationCascade，
// 在"闭包收集之后、级联执行之前"把那条 running 的复测提交成 completed（正是
// FinishFindingRetest 与会话删除的真实交错，它只锁 findings→finding_retests，不锁会话），
// 断言级联**只脱钩、不篡改终态**；未绑定会话的历史 retest 一条都不许被波及。
func TestConversation_DeleteConversation_TerminalRetestSurvivesConcurrentFinish(t *testing.T) {
	for _, tc := range []struct {
		name           string
		raceFinish     bool
		wantActiveOpen string
	}{
		{"concurrent completion wins", true, "completed"},
		{"still running gets stopped", false, "stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqliteDB := auxOpenSQLite(t)
			if _, err := sqliteDB.Exec(`INSERT INTO conversations(id,agent_key,title) VALUES (77,'test_agent','to delete')`); err != nil {
				t.Fatal(err)
			}
			for _, ddl := range []string{
				`INSERT INTO conversation_activities(id,conversation_id) VALUES (501,77)`,
				`INSERT INTO intercept_pending(id,conversation_id) VALUES (601,77)`,
				`INSERT INTO side_question_sessions(row_id,session_key,conversation_id) VALUES (1,'conv-77',77)`,
				`INSERT INTO side_question_requests(ordinal,session_key) VALUES (1,'conv-77')`,
				`INSERT INTO finding_retests(id,conversation_id,status) VALUES (901,77,'running')`,
				`INSERT INTO finding_retests(id,conversation_id,status) VALUES (902,77,'completed')`,
				// 未绑定任何会话的历史复测：任何会话删除都不许碰它。
				`INSERT INTO finding_retests(id,conversation_id,status) VALUES (903,NULL,'running')`,
			} {
				if _, err := sqliteDB.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}

			ctx := context.Background()
			tx, err := sqliteDB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			closure, err := ComputeConversationClosure(ctx, tx, 77)
			if err != nil {
				t.Fatalf("ComputeConversationClosure failed: %v", err)
			}
			if len(closure.ActiveRetestIDsToStop) != 1 || closure.ActiveRetestIDsToStop[0] != 901 {
				t.Fatalf("闭包收集到的活跃 retest 应为 [901]，实际 %v", closure.ActiveRetestIDsToStop)
			}
			if tc.raceFinish {
				// 模拟并发 FinishFindingRetest 在收集之后提交终态。
				if _, err := tx.ExecContext(ctx, `UPDATE finding_retests SET status='completed' WHERE id = 901`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ExecuteConversationCascade(ctx, tx, closure); err != nil {
				t.Fatalf("ExecuteConversationCascade failed: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("闭包提交失败：%v", err)
			}

			// 终态铁律：并发完成方赢 → 901 必须仍是 completed；没被完成 → 才允许转停止态。
			if status, conv := auxRetestRow(t, sqliteDB, 901); status != tc.wantActiveOpen {
				t.Errorf("901 状态应为 %s，实际 %s（终态不可篡改被违反）", tc.wantActiveOpen, status)
			} else if conv.Valid {
				t.Errorf("901 的弱引用必须置空，实际仍绑定 %d", conv.Int64)
			}
			if status, conv := auxRetestRow(t, sqliteDB, 902); status != "completed" {
				t.Errorf("902 的 completed 终态必须原样保留，实际 %s", status)
			} else if conv.Valid {
				t.Errorf("902 的弱引用必须置空，实际仍绑定 %d", conv.Int64)
			}
			if status, conv := auxRetestRow(t, sqliteDB, 903); status != "running" || conv.Valid {
				t.Errorf("未绑定会话的历史 retest 不得被波及，实际 status=%s conv=%v", status, conv)
			}
			var left int64
			for _, q := range []string{
				`SELECT COUNT(1) FROM conversations WHERE id = 77`,
				`SELECT COUNT(1) FROM conversation_activities WHERE conversation_id = 77`,
				`SELECT COUNT(1) FROM intercept_pending WHERE conversation_id = 77`,
				`SELECT COUNT(1) FROM side_question_sessions WHERE conversation_id = 77`,
				`SELECT COUNT(1) FROM side_question_requests WHERE session_key = 'conv-77'`,
				`SELECT COUNT(1) FROM finding_retests WHERE conversation_id = 77`,
			} {
				if err := sqliteDB.QueryRow(q).Scan(&left); err != nil {
					t.Fatal(err)
				}
				if left != 0 {
					t.Errorf("%s 残留 %d 行，闭包不完整", q, left)
				}
			}
			// finding_retests 实体一行都不能少（弱关联只置空）。
			if err := sqliteDB.QueryRow(`SELECT COUNT(1) FROM finding_retests`).Scan(&left); err != nil {
				t.Fatal(err)
			}
			if left != 3 {
				t.Errorf("复测实体必须全部存活，实际剩 %d 行", left)
			}
		})
	}
}

// TestConversation_ParentLockRejectsWritesToDeletedConversation 是审查 F04 的语句级守卫：
// 会话子实体的每一个产品写入口都必须"同一事务内先锁/校验会话根行，再写子行"，
// 根行已经不在了就**拒绝写入**，绝不留下引用已删会话的孤儿行。
func TestConversation_ParentLockRejectsWritesToDeletedConversation(t *testing.T) {
	t.Run("append activity after deletion is refused", func(t *testing.T) {
		eng := newExpMockEngine()
		d := openExpMockDB(t, eng)
		// 不注册会话行规则：根行 SELECT … FOR UPDATE 返回空集 == 会话已被删除。
		if _, err := d.AppendConvActivity(77, Activity{Kind: "text", Summary: "late emit"}); !errors.Is(err, ErrConversationNotFound) {
			t.Fatalf("会话已删除时迟到的活动写入必须被拒绝，实际 %v", err)
		}
		calls := eng.snapshotCalls()
		auxAssertAllOrdinal(t, calls)
		lock := auxFind(t, calls, "SELECT ID FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
		if !strings.Contains(auxUpper(lock), "FOR UPDATE") {
			t.Fatalf("父会话校验必须是行锁而非普通读：%s", auxCollapse(lock.Query))
		}
		for _, c := range calls {
			if strings.Contains(auxUpper(c), "INSERT INTO CONVERSATION_ACTIVITIES") {
				t.Errorf("父行校验失败后绝不得落库孤儿活动：%s", auxCollapse(c.Query))
			}
		}
		if counts := eng.txCounts(); counts.Commits != 0 {
			t.Errorf("被拒绝的写入不得提交，实际 %+v", counts)
		}
	})

	for _, decided := range []bool{false, true} {
		name := map[bool]string{false: "pending approval", true: "decided detail"}[decided]
		t.Run("intercept "+name+" after deletion is refused", func(t *testing.T) {
			eng := newExpMockEngine()
			d := openExpMockDB(t, eng)
			var err error
			if decided {
				_, err = d.CreateDecidedIntercept(0, 77, "", "agent", "bash", []byte(`{}`), "allowed", "r")
			} else {
				_, err = d.CreateInterceptPending(0, 77, "", "agent", "bash", []byte(`{}`), "r")
			}
			if !errors.Is(err, ErrConversationNotFound) {
				t.Fatalf("会话已删除时迟到的审批写入必须被拒绝，实际 %v", err)
			}
			calls := eng.snapshotCalls()
			auxAssertAllOrdinal(t, calls)
			auxFind(t, calls, "SELECT ID FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
			for _, c := range calls {
				if strings.Contains(auxUpper(c), "INSERT INTO INTERCEPT_PENDING") {
					t.Errorf("父会话校验失败后绝不得落库孤儿审批：%s", auxCollapse(c.Query))
				}
			}
		})
	}

	t.Run("lock order is intercept_rules then conversations", func(t *testing.T) {
		eng := newExpMockEngine()
		d := openExpMockDB(t, eng)
		eng.on("SELECT id FROM intercept_rules WHERE id=? FOR UPDATE", []string{"id"}, []driver.Value{int64(9)})
		eng.on("SELECT id FROM conversations WHERE id = ? FOR UPDATE", []string{"id"}, []driver.Value{int64(77)})
		eng.setNextInsertID(3001)
		if _, err := d.CreateInterceptPending(9, 77, "", "agent", "bash", []byte(`{}`), "r"); err != nil {
			t.Fatalf("父规则与会话都存活时写入必须成功：%v", err)
		}
		calls := eng.snapshotCalls()
		auxAssertAllOrdinal(t, calls)
		ruleAt := auxIndexOf(t, calls, "FROM INTERCEPT_RULES WHERE ID=? FOR UPDATE")
		convAt := auxIndexOf(t, calls, "FROM CONVERSATIONS WHERE ID = ? FOR UPDATE")
		insertAt := auxIndexOf(t, calls, "INSERT INTO INTERCEPT_PENDING")
		if !(ruleAt < convAt && convAt < insertAt) {
			t.Errorf("锁序必须是 intercept_rules(%d) → conversations(%d) → INSERT(%d)", ruleAt, convAt, insertAt)
		}
		if counts := eng.txCounts(); counts.Commits != 1 {
			t.Errorf("成功路径必须提交恰好一次，实际 %+v", counts)
		}
	})
}
