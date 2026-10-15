package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// getTestTiDB 获取外部 TiDB 实例连接；若未配置环境变量则诚实跳过并标记未验证
func getTestTiDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("跳过：环境变量 ARTEX_TIDB_DSN 未配置，标记为「未验证」(Honest Skip)")
		return nil
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open mysql: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("跳过：TiDB 连接无法 Ping 通 (%v)，标记为「未验证」", err)
	}
	return db
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

	// 插入 exploration
	res, err := tx.ExecContext(ctx, `INSERT INTO explorations (status, created_at, updated_at) VALUES ('open', NOW(3), NOW(3))`)
	if err != nil {
		t.Fatalf("insert exploration failed: %v", err)
	}
	expIDInt, _ := res.LastInsertId()
	expID = uint64(expIDInt)

	// 插入 task
	res, err = tx.ExecContext(ctx, `INSERT INTO tasks (name, goal, description, status, exploration_id, created_at, updated_at) VALUES ('test_task', 'g', 'd', 'running', ?, NOW(3), NOW(3))`, expID)
	if err != nil {
		t.Fatalf("insert task failed: %v", err)
	}
	taskIDInt, _ := res.LastInsertId()
	taskID = uint64(taskIDInt)

	// 插入关联的 exploration_nodes
	res, err = tx.ExecContext(ctx, `INSERT INTO exploration_nodes (exploration_id, kind, state, label, created_at, updated_at) VALUES (?, 'intent', 'open', 'label', NOW(3), NOW(3))`, expID)
	if err != nil {
		t.Fatalf("insert node failed: %v", err)
	}
	nodeID, _ := res.LastInsertId()

	// 插入关联的 findings (弱关联，应被置空)
	_, err = tx.ExecContext(ctx, `INSERT INTO findings (task_id, node_id, title, severity, status, created_at, updated_at) VALUES (?, ?, 'title', 'low', 'confirmed', NOW(3), NOW(3))`, taskID, nodeID)
	if err != nil {
		t.Fatalf("insert finding failed: %v", err)
	}

	// 2. 计算闭包
	closure, err := ComputeTaskClosure(ctx, tx, taskID)
	if err != nil {
		t.Fatalf("ComputeTaskClosure failed: %v", err)
	}

	if closure.ExplorationID != expID {
		t.Fatalf("expected exploration ID %d, got %d", expID, closure.ExplorationID)
	}
	if len(closure.NodeIDs) != 1 || closure.NodeIDs[0] != uint64(nodeID) {
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
	res, _ := tx.ExecContext(ctx, `INSERT INTO explorations (status, created_at, updated_at) VALUES ('open', NOW(3), NOW(3))`)
	expIDInt, _ := res.LastInsertId()
	expID = uint64(expIDInt)
	res, _ = tx.ExecContext(ctx, `INSERT INTO tasks (name, goal, description, status, exploration_id, created_at, updated_at) VALUES ('test_concurrent', 'g', 'd', 'running', ?, NOW(3), NOW(3))`, expID)
	taskIDInt, _ := res.LastInsertId()
	taskID = uint64(taskIDInt)
	_ = tx.Commit()

	var wg sync.WaitGroup
	wg.Add(2)

	deleteDone := make(chan struct{})

	// Goroutine A: 执行级联删除
	go func() {
		defer wg.Done()
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
		_ = delTx.Commit()
		close(deleteDone)
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
