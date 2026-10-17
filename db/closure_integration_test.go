package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Autumn-27/artex/db/tidb"
)

// getTestTiDB 获取外部 TiDB 实例连接；若未配置环境变量则诚实跳过并标记未验证。
// 连接统一走 tidb.OpenTiDB（内部 NormalizeDSN 强制 UTC/time_zone + Ping + 能力探测），
// 并注册 t.Cleanup 关闭连接池；OpenTiDB 失败即 t.Fatalf（DSN 经 RedactDSN 脱敏）。
func getTestTiDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("跳过：环境变量 ARTEX_TIDB_DSN 未配置，标记为「未验证」(Honest Skip)")
		return nil
	}
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("failed to open tidb: %v (dsn=%s)", err, tidb.RedactDSN(dsn))
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// cleanupRow 注册「按主键删除本用例插入行」的 t.Cleanup。
// t.Cleanup 为 LIFO：按插入顺序注册即得到依赖逆序删除（先子后父）。
// 目标库是共享持久库，禁止 TRUNCATE / DROP TABLE；级联已删掉的行删除 0 行属正常。
func cleanupRow(t *testing.T, db *sql.DB, table string, id uint64) {
	t.Helper()
	if id == 0 {
		t.Fatalf("cleanupRow: table %s got zero id", table)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "DELETE FROM "+table+" WHERE id = ?", id); err != nil {
			t.Errorf("cleanup: delete %s id=%d failed: %v", table, id, err)
		}
	})
}

func TestClosureTaskEndToEndVerification(t *testing.T) {
	db := getTestTiDB(t)
	if db == nil {
		return
	}
	ctx := context.Background()

	// 1. 建立测试数据：Task 与 Exploration
	var expID, taskID uint64
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	// 插入 exploration (goal 为 NOT NULL 列)
	res, err := tx.ExecContext(ctx, `INSERT INTO explorations (goal, status, created_at, updated_at) VALUES ('test goal', 'open', NOW(3), NOW(3))`)
	if err != nil {
		t.Fatalf("insert exploration failed: %v", err)
	}
	expIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get exploration last insert id failed: %v", err)
	}
	expID = uint64(expIDInt)
	cleanupRow(t, db, "explorations", expID)

	// 插入 task (queue_mode 为 TEXT NOT NULL 无默认，必须显式提供)
	res, err = tx.ExecContext(ctx, `INSERT INTO tasks (name, goal, description, status, queue_mode, exploration_id, created_at, updated_at) VALUES ('test_task', 'g', 'd', 'running', '', ?, NOW(3), NOW(3))`, expID)
	if err != nil {
		t.Fatalf("insert task failed: %v", err)
	}
	taskIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get task last insert id failed: %v", err)
	}
	taskID = uint64(taskIDInt)
	cleanupRow(t, db, "tasks", taskID)

	// 插入关联的 exploration_nodes (对齐 catalog：无 label 列；payload 为 JSON 载荷 NOT NULL 无默认)
	res, err = tx.ExecContext(ctx, `INSERT INTO exploration_nodes (exploration_id, kind, state, payload, created_at, updated_at) VALUES (?, 'intent', 'open', '{}', NOW(3), NOW(3))`, expID)
	if err != nil {
		t.Fatalf("insert node failed: %v", err)
	}
	nodeIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get node last insert id failed: %v", err)
	}
	nodeID := uint64(nodeIDInt)
	cleanupRow(t, db, "exploration_nodes", nodeID)

	// 插入关联的 findings (弱关联，对齐 catalog：无 title、无 updated_at，使用 name；
	// vulnclass/summary/evidence/worker/asset_ids/report 均为 NOT NULL 无默认)
	res, err = tx.ExecContext(ctx, `INSERT INTO findings (task_id, node_id, name, severity, status, vulnclass, summary, evidence, worker, asset_ids, report, created_at) VALUES (?, ?, 'title', 'low', 'confirmed', '', '', '', '', '[]', '', NOW(3))`, taskID, nodeID)
	if err != nil {
		t.Fatalf("insert finding failed: %v", err)
	}
	findingIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get finding last insert id failed: %v", err)
	}
	cleanupRow(t, db, "findings", uint64(findingIDInt))

	// 2. 计算闭包
	closure, err := ComputeTaskClosure(ctx, tx, taskID)
	if err != nil {
		t.Fatalf("ComputeTaskClosure failed: %v", err)
	}

	if closure.ExplorationID != expID {
		t.Fatalf("expected exploration ID %d, got %d", expID, closure.ExplorationID)
	}
	if len(closure.NodeIDs) != 1 || closure.NodeIDs[0] != nodeID {
		t.Fatalf("expected 1 node ID %d, got %v", nodeID, closure.NodeIDs)
	}

	// 3. 执行原子级联清理
	audit, err := ExecuteTaskCascade(ctx, tx, closure)
	if err != nil {
		t.Fatalf("ExecuteTaskCascade failed: %v", err)
	}

	if audit.EntitiesDeleted["tasks"] != 1 {
		t.Fatalf("expected 1 task deleted, got %d", audit.EntitiesDeleted["tasks"])
	}
	if audit.EntitiesDeleted["explorations"] != 1 {
		t.Fatalf("expected 1 exploration deleted, got %d", audit.EntitiesDeleted["explorations"])
	}
	if audit.EntitiesDeleted["exploration_nodes"] != 1 {
		t.Fatalf("expected 1 node deleted, got %d", audit.EntitiesDeleted["exploration_nodes"])
	}
	if audit.EntitiesUpdated["findings.node_id"] != 1 {
		t.Fatalf("expected 1 finding.node_id updated to null, got %d", audit.EntitiesUpdated["findings.node_id"])
	}
	if audit.EntitiesUpdated["findings.task_id"] != 1 {
		t.Fatalf("expected 1 finding.task_id updated to null, got %d", audit.EntitiesUpdated["findings.task_id"])
	}

	// 4. 断言全库零孤儿
	if err := AssertNoOrphansForTask(ctx, tx, taskID, expID); err != nil {
		t.Fatalf("orphan assertion failed: %v", err)
	}
}

func TestClosureConcurrentOrphanPrevention(t *testing.T) {
	db := getTestTiDB(t)
	if db == nil {
		return
	}
	ctx := context.Background()

	// 并发模拟：Goroutine A 执行任务删除，Goroutine B 尝试基于该任务创建子节点/关联
	var expID, taskID uint64
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO explorations (goal, status, created_at, updated_at) VALUES ('test goal', 'open', NOW(3), NOW(3))`)
	if err != nil {
		t.Fatalf("insert exploration failed: %v", err)
	}
	expIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get exploration last insert id failed: %v", err)
	}
	expID = uint64(expIDInt)
	cleanupRow(t, db, "explorations", expID)

	res, err = tx.ExecContext(ctx, `INSERT INTO tasks (name, goal, description, status, queue_mode, exploration_id, created_at, updated_at) VALUES ('test_concurrent', 'g', 'd', 'running', '', ?, NOW(3), NOW(3))`, expID)
	if err != nil {
		t.Fatalf("insert task failed: %v", err)
	}
	taskIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get task last insert id failed: %v", err)
	}
	taskID = uint64(taskIDInt)
	cleanupRow(t, db, "tasks", taskID)

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit setup tx failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	deleteDone := make(chan struct{})

	// Goroutine A: 执行级联删除
	go func() {
		defer wg.Done()
		defer close(deleteDone) // M1: 防止提前 return 导致 Goroutine B 永远阻塞超时

		delTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("delete begin tx: %v", err)
			return
		}
		defer delTx.Rollback()

		closure, err := ComputeTaskClosure(ctx, delTx, taskID)
		if err != nil {
			t.Errorf("compute closure: %v", err)
			return
		}
		_, err = ExecuteTaskCascade(ctx, delTx, closure)
		if err != nil {
			t.Errorf("execute cascade: %v", err)
			return
		}
		if err := delTx.Commit(); err != nil {
			t.Errorf("delTx commit failed: %v", err)
			return
		}
	}()

	// Goroutine B: 尝试插入子任务关联，通过入口校验器前置防御
	go func() {
		defer wg.Done()
		<-deleteDone // 确保在删除完成后执行或并发竞争

		childTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("child begin tx: %v", err)
			return
		}
		defer childTx.Rollback()

		// 入口前置校验：验证父 Task 是否存在
		err = ValidateTaskExists(ctx, childTx, taskID)
		if err == nil {
			t.Errorf("expected ValidateTaskExists to fail for deleted task id=%d, but succeeded", taskID)
		} else if !errors.Is(err, ErrForeignKeyViolation) {
			t.Errorf("expected ErrForeignKeyViolation, got: %v", err)
		}
	}()

	wg.Wait()
}

func TestClosureCompanyEndToEndVerification(t *testing.T) {
	db := getTestTiDB(t)
	if db == nil {
		return
	}
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	// 1. 建立测试数据：Company、关联的 company_scope、tasks (需 exploration)、task_scope、assets
	compIdent := []byte("test_company_identity")
	compHash := sha256.Sum256(compIdent)
	res, err := tx.ExecContext(ctx, `INSERT INTO companies (name, nkey, nkey_identity, nkey_hash, created_at, updated_at) VALUES ('test_company', 'test_nkey', ?, ?, NOW(3), NOW(3))`, compIdent, compHash[:])
	if err != nil {
		t.Fatalf("insert company failed: %v", err)
	}
	compIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get company last insert id failed: %v", err)
	}
	compID := uint64(compIDInt)
	cleanupRow(t, db, "companies", compID)

	// 插入 company_scope (级联删除表)
	res, err = tx.ExecContext(ctx, `INSERT INTO company_scope (company_id, kind, raw, created_at) VALUES (?, 'domain', 'example.com', NOW(3))`, compID)
	if err != nil {
		t.Fatalf("insert company_scope failed: %v", err)
	}
	scopeIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get company_scope last insert id failed: %v", err)
	}
	scopeID := uint64(scopeIDInt)
	cleanupRow(t, db, "company_scope", scopeID)

	// 插入 exploration (为创建 task 所需)
	res, err = tx.ExecContext(ctx, `INSERT INTO explorations (goal, status, created_at, updated_at) VALUES ('company task exploration', 'open', NOW(3), NOW(3))`)
	if err != nil {
		t.Fatalf("insert exploration failed: %v", err)
	}
	expIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get exploration last insert id failed: %v", err)
	}
	expID := uint64(expIDInt)
	cleanupRow(t, db, "explorations", expID)

	// 插入 task (弱关联置空表：company_id = compID；queue_mode NOT NULL 无默认)
	res, err = tx.ExecContext(ctx, `INSERT INTO tasks (name, goal, description, status, queue_mode, exploration_id, company_id, created_at, updated_at) VALUES ('company_task', 'g', 'd', 'running', '', ?, ?, NOW(3), NOW(3))`, expID, compID)
	if err != nil {
		t.Fatalf("insert task failed: %v", err)
	}
	taskIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get task last insert id failed: %v", err)
	}
	taskID := uint64(taskIDInt)
	cleanupRow(t, db, "tasks", taskID)

	// 插入 task_scope (级联删除表：kind='company' 且 company_id = compID；source NOT NULL 无默认)
	tsIdent := []byte("test_task_scope_identity")
	tsHash := sha256.Sum256(tsIdent)
	res, err = tx.ExecContext(ctx, `INSERT INTO task_scope (task_id, kind, company_id, source, identity, identity_hash, created_at) VALUES (?, 'company', ?, 'auto', ?, ?, NOW(3))`, taskID, compID, tsIdent, tsHash[:])
	if err != nil {
		t.Fatalf("insert task_scope failed: %v", err)
	}
	taskScopeIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get task_scope last insert id failed: %v", err)
	}
	taskScopeID := uint64(taskScopeIDInt)
	cleanupRow(t, db, "task_scope", taskScopeID)

	// 插入 asset (弱关联置空表：company_id = compID；
	// company_source 与全部 JSON 载荷列均 NOT NULL 无默认，需显式提供语义等价默认值)
	res, err = tx.ExecContext(ctx, `INSERT INTO assets (type, company_id, company_source, task_ids, bound_domains, open_ports, record_value, technologies, auth, params, extra, created_at, last_seen, updated_at) VALUES ('domain', ?, 'explicit', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '{}', NOW(3), NOW(3), NOW(3))`, compID)
	if err != nil {
		t.Fatalf("insert asset failed: %v", err)
	}
	assetIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get asset last insert id failed: %v", err)
	}
	assetID := uint64(assetIDInt)
	cleanupRow(t, db, "assets", assetID)

	// 2. 计算闭包
	closure, err := ComputeCompanyClosure(ctx, tx, compID)
	if err != nil {
		t.Fatalf("ComputeCompanyClosure failed: %v", err)
	}

	if closure.CompanyID != compID {
		t.Fatalf("expected company ID %d, got %d", compID, closure.CompanyID)
	}
	if len(closure.ScopeIDs) != 1 || closure.ScopeIDs[0] != scopeID {
		t.Fatalf("expected scope ID %d, got %v", scopeID, closure.ScopeIDs)
	}
	if len(closure.TaskScopeIDs) != 1 || closure.TaskScopeIDs[0] != taskScopeID {
		t.Fatalf("expected task_scope ID %d, got %v", taskScopeID, closure.TaskScopeIDs)
	}
	if len(closure.NullifyAssetIDs) != 1 || closure.NullifyAssetIDs[0] != assetID {
		t.Fatalf("expected nullify asset ID %d, got %v", assetID, closure.NullifyAssetIDs)
	}
	if len(closure.NullifyTaskIDs) != 1 || closure.NullifyTaskIDs[0] != taskID {
		t.Fatalf("expected nullify task ID %d, got %v", taskID, closure.NullifyTaskIDs)
	}

	// 3. 执行原子级联清理
	audit, err := ExecuteCompanyCascade(ctx, tx, closure)
	if err != nil {
		t.Fatalf("ExecuteCompanyCascade failed: %v", err)
	}

	if audit.EntitiesDeleted["companies"] != 1 {
		t.Fatalf("expected 1 company deleted, got %d", audit.EntitiesDeleted["companies"])
	}
	if audit.EntitiesDeleted["company_scope"] != 1 {
		t.Fatalf("expected 1 company_scope deleted, got %d", audit.EntitiesDeleted["company_scope"])
	}
	if audit.EntitiesDeleted["task_scope"] != 1 {
		t.Fatalf("expected 1 task_scope deleted, got %d", audit.EntitiesDeleted["task_scope"])
	}
	if audit.EntitiesUpdated["assets.company_id"] != 1 {
		t.Fatalf("expected 1 assets.company_id updated to null, got %d", audit.EntitiesUpdated["assets.company_id"])
	}
	if audit.EntitiesUpdated["tasks.company_id"] != 1 {
		t.Fatalf("expected 1 tasks.company_id updated to null, got %d", audit.EntitiesUpdated["tasks.company_id"])
	}

	// 4. 断言全库零孤儿
	if err := AssertNoOrphansForCompany(ctx, tx, compID); err != nil {
		t.Fatalf("AssertNoOrphansForCompany failed: %v", err)
	}
}

func TestClosureConversationEndToEndVerification(t *testing.T) {
	db := getTestTiDB(t)
	if db == nil {
		return
	}
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	// 1. 建立测试数据：Conversation、conversation_activities、intercept_pending、finding_retests (需 finding)
	res, err := tx.ExecContext(ctx, `INSERT INTO conversations (agent_key, title, created_at, updated_at) VALUES ('test_agent', 'test conversation', NOW(3), NOW(3))`)
	if err != nil {
		t.Fatalf("insert conversation failed: %v", err)
	}
	convIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get conversation last insert id failed: %v", err)
	}
	convID := uint64(convIDInt)
	cleanupRow(t, db, "conversations", convID)

	// 插入 conversation_activities (级联删除表)
	res, err = tx.ExecContext(ctx, `INSERT INTO conversation_activities (conversation_id, kind, created_at) VALUES (?, 'message', NOW(3))`, convID)
	if err != nil {
		t.Fatalf("insert conversation_activities failed: %v", err)
	}
	activityIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get conversation_activities last insert id failed: %v", err)
	}
	activityID := uint64(activityIDInt)
	cleanupRow(t, db, "conversation_activities", activityID)

	// 插入 intercept_pending (级联删除表；agent_name/tool_input/status/reason/decision_source
	// 均 NOT NULL 无默认)
	res, err = tx.ExecContext(ctx, `INSERT INTO intercept_pending (conversation_id, tool_name, agent_name, tool_input, status, reason, decision_source, created_at) VALUES (?, 'bash', '', '{}', 'pending', '', '', NOW(3))`, convID)
	if err != nil {
		t.Fatalf("insert intercept_pending failed: %v", err)
	}
	interceptIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get intercept_pending last insert id failed: %v", err)
	}
	interceptID := uint64(interceptIDInt)
	cleanupRow(t, db, "intercept_pending", interceptID)

	// 插入 finding (为 finding_retests 外键所需；NOT NULL 无默认列显式补齐)
	res, err = tx.ExecContext(ctx, `INSERT INTO findings (name, severity, status, vulnclass, summary, evidence, worker, asset_ids, report, created_at) VALUES ('test vuln', 'low', 'confirmed', '', '', '', '', '[]', '', NOW(3))`)
	if err != nil {
		t.Fatalf("insert finding failed: %v", err)
	}
	findingIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get finding last insert id failed: %v", err)
	}
	findingID := uint64(findingIDInt)
	cleanupRow(t, db, "findings", findingID)

	// 插入 finding_retests (弱关联置空表：conversation_id = convID；
	// verdict/notes/summary/evidence/error 均 NOT NULL 无默认；
	// active_finding_id 是 STORED 生成列，不得出现在 INSERT 中)
	res, err = tx.ExecContext(ctx, `INSERT INTO finding_retests (finding_id, conversation_id, status, snapshot, verdict, notes, summary, evidence, error, created_at) VALUES (?, ?, 'pending', '{}', '', '', '', '', '', NOW(3))`, findingID, convID)
	if err != nil {
		t.Fatalf("insert finding_retests failed: %v", err)
	}
	retestIDInt, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("get finding_retests last insert id failed: %v", err)
	}
	retestID := uint64(retestIDInt)
	cleanupRow(t, db, "finding_retests", retestID)

	// 2. 计算闭包
	closure, err := ComputeConversationClosure(ctx, tx, convID)
	if err != nil {
		t.Fatalf("ComputeConversationClosure failed: %v", err)
	}

	if closure.ConversationID != convID {
		t.Fatalf("expected conversation ID %d, got %d", convID, closure.ConversationID)
	}
	if len(closure.ActivityIDs) != 1 || closure.ActivityIDs[0] != activityID {
		t.Fatalf("expected activity ID %d, got %v", activityID, closure.ActivityIDs)
	}
	if len(closure.InterceptPendingIDs) != 1 || closure.InterceptPendingIDs[0] != interceptID {
		t.Fatalf("expected intercept_pending ID %d, got %v", interceptID, closure.InterceptPendingIDs)
	}
	if len(closure.NullifyRetestIDs) != 1 || closure.NullifyRetestIDs[0] != retestID {
		t.Fatalf("expected nullify retest ID %d, got %v", retestID, closure.NullifyRetestIDs)
	}
	if len(closure.ActiveRetestIDsToStop) != 1 || closure.ActiveRetestIDsToStop[0] != retestID {
		t.Fatalf("expected active retest ID %d, got %v", retestID, closure.ActiveRetestIDsToStop)
	}

	// 3. 执行原子级联清理
	audit, err := ExecuteConversationCascade(ctx, tx, closure)
	if err != nil {
		t.Fatalf("ExecuteConversationCascade failed: %v", err)
	}

	if audit.EntitiesDeleted["conversations"] != 1 {
		t.Fatalf("expected 1 conversation deleted, got %d", audit.EntitiesDeleted["conversations"])
	}
	if audit.EntitiesDeleted["conversation_activities"] != 1 {
		t.Fatalf("expected 1 conversation_activities deleted, got %d", audit.EntitiesDeleted["conversation_activities"])
	}
	if audit.EntitiesDeleted["intercept_pending"] != 1 {
		t.Fatalf("expected 1 intercept_pending deleted, got %d", audit.EntitiesDeleted["intercept_pending"])
	}
	if audit.EntitiesUpdated["finding_retests.conversation_id"] != 1 {
		t.Fatalf("expected 1 finding_retests.conversation_id updated to null, got %d", audit.EntitiesUpdated["finding_retests.conversation_id"])
	}

	// 4. 断言全库零孤儿
	if err := AssertNoOrphansForConversation(ctx, tx, convID); err != nil {
		t.Fatalf("AssertNoOrphansForConversation failed: %v", err)
	}
}
