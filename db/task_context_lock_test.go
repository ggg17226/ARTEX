package db

import (
	"fmt"
	"testing"
	"time"
)

func TestTaskLLMProfileMutationsLockTaskBeforeProfile(t *testing.T) {
	dsn := testDSN(t)
	d, err := Open(dsn)
	if err != nil {
		t.Skipf("TiDB unavailable (%v) - skipping", err)
	}
	defer d.Close()

	suffix := time.Now().UnixNano()
	first, err := d.SaveProfile(&LLMProfile{
		Name: fmt.Sprintf("lock-order-first-%d", suffix), Format: "openai", Model: "first", APIKey: "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.SaveProfile(&LLMProfile{
		Name: fmt.Sprintf("lock-order-second-%d", suffix), Format: "openai", Model: "second", APIKey: "test-key",
	})
	if err != nil {
		_ = d.DeleteProfile(first)
		t.Fatal(err)
	}
	task, err := d.CreateTaskWithOptions("LLM lock order", "verify concurrent mutation locks", TaskCreateOptions{
		LLMProfileIDs: []int64{first, second},
	})
	if err != nil {
		_ = d.DeleteProfile(first)
		_ = d.DeleteProfile(second)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = d.DeleteTask(task.ID)
		_ = d.DeleteProfile(first)
		_ = d.DeleteProfile(second)
	})

	replaceDB, replacePID := openSingleConnectionTestDB(t, dsn)
	replaceBlocker, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer replaceBlocker.Rollback()
	if _, err := replaceBlocker.Exec(`SELECT id FROM tasks WHERE id = ? FOR UPDATE`, task.ID); err != nil {
		replaceBlocker.Rollback()
		t.Fatal(err)
	}
	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- replaceDB.ReplaceTaskLLMProfiles(task.ID, []int64{second, first}, second)
	}()
	if err := waitForBackendBlock(d, replacePID, replaceDone); err != nil {
		replaceBlocker.Rollback()
		t.Fatal(err)
	}
	assertProfilesUnlocked(t, d, first, second)
	if err := replaceBlocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := waitForMutationResult(replaceDone); err != nil {
		t.Fatalf("replace chain: %v", err)
	}

	deleteDB, deletePID := openSingleConnectionTestDB(t, dsn)
	deleteBlocker, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer deleteBlocker.Rollback()
	if _, err := deleteBlocker.Exec(`SELECT id FROM tasks WHERE id = ? FOR UPDATE`, task.ID); err != nil {
		deleteBlocker.Rollback()
		t.Fatal(err)
	}
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- deleteDB.DeleteProfile(second)
	}()
	if err := waitForBackendBlock(d, deletePID, deleteDone); err != nil {
		deleteBlocker.Rollback()
		t.Fatal(err)
	}
	assertProfilesUnlocked(t, d, second)
	if err := deleteBlocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := waitForMutationResult(deleteDone); err != nil {
		t.Fatalf("delete profile: %v", err)
	}

	got, err := d.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLLMProfileID == nil || *got.ActiveLLMProfileID != first {
		t.Fatalf("deleting the active profile did not select its successor: %+v", got)
	}
}

func TestDeleteProfileLocksNonTaskReferencesBeforeProfile(t *testing.T) {
	dsn := testDSN(t)

	t.Run("agent", func(t *testing.T) {
		d, err := Open(dsn)
		if err != nil {
			t.Skipf("TiDB unavailable (%v) - skipping", err)
		}
		defer d.Close()

		suffix := time.Now().UnixNano()
		profileID, err := d.SaveProfile(&LLMProfile{
			Name: fmt.Sprintf("agent-lock-profile-%d", suffix), Format: "openai", Model: "agent-lock", APIKey: "test-key",
		})
		if err != nil {
			t.Fatal(err)
		}
		agent, err := d.CreateAgent(fmt.Sprintf("lock_agent_%d", suffix), "lock agent", "")
		if err != nil {
			_ = d.DeleteProfile(profileID)
			t.Fatal(err)
		}
		if err := d.SetAgentLLMProfile(agent.Key, &profileID); err != nil {
			_ = d.DeleteAgent(agent.Key)
			_ = d.DeleteProfile(profileID)
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = d.DeleteAgent(agent.Key)
			_ = d.DeleteProfile(profileID)
		})

		blocker, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		if _, err := blocker.Exec(`SELECT id FROM agents WHERE id = ? FOR UPDATE`, agent.ID); err != nil {
			t.Fatal(err)
		}
		deleteDB, deletePID := openSingleConnectionTestDB(t, dsn)
		deleteDone := make(chan error, 1)
		go func() { deleteDone <- deleteDB.DeleteProfile(profileID) }()
		if err := waitForBackendBlock(d, deletePID, deleteDone); err != nil {
			blocker.Rollback()
			t.Fatal(err)
		}
		assertProfilesUnlocked(t, d, profileID)
		if err := blocker.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := waitForMutationResult(deleteDone); err != nil {
			t.Fatalf("delete agent profile: %v", err)
		}
		got, err := d.GetAgentByKey(agent.Key)
		if err != nil || got == nil || got.LLMProfileID != nil {
			t.Fatalf("agent binding was not cleared: agent=%+v err=%v", got, err)
		}
	})

	t.Run("conversation", func(t *testing.T) {
		d, err := Open(dsn)
		if err != nil {
			t.Skipf("TiDB unavailable (%v) - skipping", err)
		}
		defer d.Close()

		suffix := time.Now().UnixNano()
		profileID, err := d.SaveProfile(&LLMProfile{
			Name: fmt.Sprintf("conversation-lock-profile-%d", suffix), Format: "openai", Model: "conversation-lock", APIKey: "test-key",
		})
		if err != nil {
			t.Fatal(err)
		}
		conversation, err := d.CreateConversation("planner", "lock conversation", &profileID)
		if err != nil {
			_ = d.DeleteProfile(profileID)
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = d.DeleteConversation(conversation.ID)
			_ = d.DeleteProfile(profileID)
		})

		blocker, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		if _, err := blocker.Exec(`SELECT id FROM conversations WHERE id = ? FOR UPDATE`, conversation.ID); err != nil {
			t.Fatal(err)
		}
		deleteDB, deletePID := openSingleConnectionTestDB(t, dsn)
		deleteDone := make(chan error, 1)
		go func() { deleteDone <- deleteDB.DeleteProfile(profileID) }()
		if err := waitForBackendBlock(d, deletePID, deleteDone); err != nil {
			blocker.Rollback()
			t.Fatal(err)
		}
		assertProfilesUnlocked(t, d, profileID)
		if err := blocker.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := waitForMutationResult(deleteDone); err != nil {
			t.Fatalf("delete conversation profile: %v", err)
		}
		got, err := d.GetConversation(conversation.ID)
		if err != nil || got == nil || got.LLMProfileID != nil {
			t.Fatalf("conversation binding was not cleared: conversation=%+v err=%v", got, err)
		}
	})
}

func TestNonTaskProfileMutationsLockReferenceBeforeProfile(t *testing.T) {
	dsn := testDSN(t)

	t.Run("agent", func(t *testing.T) {
		d, err := Open(dsn)
		if err != nil {
			t.Skipf("TiDB unavailable (%v) - skipping", err)
		}
		defer d.Close()

		suffix := time.Now().UnixNano()
		profileID, err := d.SaveProfile(&LLMProfile{
			Name: fmt.Sprintf("agent-write-lock-profile-%d", suffix), Format: "openai", Model: "agent-write-lock", APIKey: "test-key",
		})
		if err != nil {
			t.Fatal(err)
		}
		agent, err := d.CreateAgent(fmt.Sprintf("write_lock_agent_%d", suffix), "write lock agent", "")
		if err != nil {
			_ = d.DeleteProfile(profileID)
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = d.DeleteAgent(agent.Key)
			_ = d.DeleteProfile(profileID)
		})

		blocker, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		if _, err := blocker.Exec(`SELECT id FROM agents WHERE id = ? FOR UPDATE`, agent.ID); err != nil {
			t.Fatal(err)
		}
		mutationDB, mutationPID := openSingleConnectionTestDB(t, dsn)
		mutationDone := make(chan error, 1)
		go func() { mutationDone <- mutationDB.SetAgentLLMProfile(agent.Key, &profileID) }()
		if err := waitForBackendBlock(d, mutationPID, mutationDone); err != nil {
			blocker.Rollback()
			t.Fatal(err)
		}
		assertProfilesUnlocked(t, d, profileID)
		if err := blocker.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := waitForMutationResult(mutationDone); err != nil {
			t.Fatalf("bind agent profile: %v", err)
		}
	})

	t.Run("conversation", func(t *testing.T) {
		d, err := Open(dsn)
		if err != nil {
			t.Skipf("TiDB unavailable (%v) - skipping", err)
		}
		defer d.Close()

		suffix := time.Now().UnixNano()
		profileID, err := d.SaveProfile(&LLMProfile{
			Name: fmt.Sprintf("conversation-write-lock-profile-%d", suffix), Format: "openai", Model: "conversation-write-lock", APIKey: "test-key",
		})
		if err != nil {
			t.Fatal(err)
		}
		conversation, err := d.CreateConversation("planner", "write lock conversation", nil)
		if err != nil {
			_ = d.DeleteProfile(profileID)
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = d.DeleteConversation(conversation.ID)
			_ = d.DeleteProfile(profileID)
		})

		blocker, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		if _, err := blocker.Exec(`SELECT id FROM conversations WHERE id = ? FOR UPDATE`, conversation.ID); err != nil {
			t.Fatal(err)
		}
		mutationDB, mutationPID := openSingleConnectionTestDB(t, dsn)
		mutationDone := make(chan error, 1)
		go func() { mutationDone <- mutationDB.UpdateConversationProfile(conversation.ID, &profileID) }()
		if err := waitForBackendBlock(d, mutationPID, mutationDone); err != nil {
			blocker.Rollback()
			t.Fatal(err)
		}
		assertProfilesUnlocked(t, d, profileID)
		if err := blocker.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := waitForMutationResult(mutationDone); err != nil {
			t.Fatalf("bind conversation profile: %v", err)
		}
	})
}

func TestCreateConversationAndDeleteProfileDoNotDeadlock(t *testing.T) {
	dsn := testDSN(t)
	d, err := Open(dsn)
	if err != nil {
		t.Skipf("TiDB unavailable (%v) - skipping", err)
	}
	defer d.Close()

	suffix := time.Now().UnixNano()
	profileID, err := d.SaveProfile(&LLMProfile{
		Name: fmt.Sprintf("conversation-create-race-profile-%d", suffix), Format: "openai", Model: "conversation-create-race", APIKey: "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	title := fmt.Sprintf("conversation create race %d", suffix)
	t.Cleanup(func() {
		_, _ = d.Exec(`DELETE FROM conversations WHERE title = ?`, title)
		_ = d.DeleteProfile(profileID)
	})

	profileBlocker, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer profileBlocker.Rollback()
	if _, err := profileBlocker.Exec(`SELECT id FROM llm_profiles WHERE id = ? FOR UPDATE`, profileID); err != nil {
		t.Fatal(err)
	}

	createDB, createPID := openSingleConnectionTestDB(t, dsn)
	type createResult struct {
		conversation *Conversation
		err          error
	}
	createDone := make(chan createResult, 1)
	createStatus := make(chan error, 1)
	go func() {
		conversation, err := createDB.CreateConversation("planner", title, &profileID)
		createDone <- createResult{conversation: conversation, err: err}
		createStatus <- err
	}()
	if err := waitForBackendBlock(d, createPID, createStatus); err != nil {
		profileBlocker.Rollback()
		t.Fatal(err)
	}

	deleteDB, deletePID := openSingleConnectionTestDB(t, dsn)
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- deleteDB.DeleteProfile(profileID) }()
	if err := waitForBackendBlock(d, deletePID, deleteDone); err != nil {
		profileBlocker.Rollback()
		t.Fatal(err)
	}
	if err := profileBlocker.Rollback(); err != nil {
		t.Fatal(err)
	}

	var created createResult
	select {
	case created = <-createDone:
	case <-time.After(12 * time.Second):
		t.Fatal("timed out waiting for conversation creation")
	}
	if err := waitForMutationResult(deleteDone); err != nil {
		t.Fatalf("delete profile during conversation creation: %v", err)
	}
	var persisted int
	if err := d.QueryRow(`SELECT count(*) FROM conversations WHERE title = ?`, title).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if created.err == nil {
		if created.conversation == nil || persisted != 1 {
			t.Fatalf("successful creation was not committed atomically: conversation=%+v count=%d", created.conversation, persisted)
		}
	} else if persisted != 0 {
		t.Fatalf("failed creation left a partial conversation row: err=%v count=%d", created.err, persisted)
	}
}

func TestDeleteProfileRetriesReferenceCommittedAfterInitialScan(t *testing.T) {
	dsn := testDSN(t)
	d, err := Open(dsn)
	if err != nil {
		t.Skipf("TiDB unavailable (%v) - skipping", err)
	}
	defer d.Close()

	suffix := time.Now().UnixNano()
	profileID, err := d.SaveProfile(&LLMProfile{
		Name: fmt.Sprintf("late-reference-profile-%d", suffix), Format: "openai", Model: "late-reference", APIKey: "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := d.CreateTask("late profile reference", "exercise delete retry", nil, 0, 0)
	if err != nil {
		_ = d.DeleteProfile(profileID)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = d.DeleteTask(task.ID)
		_ = d.DeleteProfile(profileID)
	})

	// Keep the new reference uncommitted while deletion takes its initial
	// READ COMMITTED snapshot. The task is therefore absent from the first lock
	// set, while its FK KEY SHARE lock makes deletion wait at the profile row.
	referenceTx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer referenceTx.Rollback()
	if _, err := referenceTx.Exec(`UPDATE tasks
SET llm_profile_id=?, active_llm_profile_id=?
WHERE id = ?`, profileID, profileID, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := referenceTx.Exec(`INSERT INTO task_llm_profiles(task_id, profile_id, position)
VALUES (?, ?, 0)`, task.ID, profileID); err != nil {
		t.Fatal(err)
	}

	// TiDB 上没有 FK 派生的 KEY SHARE 锁可以把删除挡在 profile 行上（见 waitForBackendBlock
	// 的说明），"未提交引用 + 观察阻塞"这一半在 TiDB 上不成立。因此这个用例把时序改成确定
	// 形态：先把引用**提交**，再发起删除 —— 这里根本没有 blocker 持有锁，所以它不能、也不该
	// 借用"证明阻塞"的共享 helper（审查 F05：依赖 PG 外键派生锁的个别场景单独适配，不许
	// 反过来改写所有调用者共享的 helper 语义）。它验证的产品契约不变 ——
	// 删除必须正确处理"初始扫描之后才出现的引用"，绝不能留下悬挂引用。
	if err := referenceTx.Commit(); err != nil {
		t.Fatal(err)
	}
	deleteDB, _ := openSingleConnectionTestDB(t, dsn)
	lateDone := make(chan error, 1)
	go func() { lateDone <- deleteDB.DeleteProfile(profileID) }()
	if err := waitForMutationResult(lateDone); err != nil {
		t.Fatalf("delete profile after late reference: %v", err)
	}

	got, err := d.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLLMProfileID != nil || len(got.LLMProfileIDs) != 0 || got.LLMChainRevision != 1 {
		t.Fatalf("late task reference was not handled by a locked retry: %+v", got)
	}
}

// openSingleConnectionTestDB 开一条**只有 1 个物理连接**的会话，用来构造确定的锁序。
//
// TiDB 侧对应关系：连接统一由 db 包自己的 Open 建立（go-sql-driver/mysql + DSN 规范
// 化），不再按驱动名直接建连；PG 专有的会话变量 statement_timeout，在 TiDB 侧的同类
// 保护是 innodb_lock_wait_timeout（压到 10s：真死锁时让语句报错，而不是把整个测试永久
// 挂住）；PG 的后端进程号由 MySQL/TiDB 的 CONNECTION_ID() 承担（只用于日志定位）。
func openSingleConnectionTestDB(t *testing.T, dsn string) (*DB, int) {
	d, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := d.DB
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if _, err := sqlDB.Exec(`SET SESSION innodb_lock_wait_timeout = 10`); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := sqlDB.QueryRow(`SELECT CONNECTION_ID()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return d, pid
}

// lockObservationProbe 非阻塞地探测 done：一旦读到结果，就说明被观察的操作**没有**停在
// 预期的锁上（要么提前成功返回，要么带着错误返回）。
//
// 关键约束：本函数只在"抓到提前返回"这一种情况下读 done。证明阻塞成功的分支绝不读 done，
// 那唯一一次的完成结果必须留给调用方的 waitForMutationResult（审查 F05：迁移期这里被改成
// "等到 done 有值就算观察到阻塞"，等于把"证明阻塞"偷换成"证明完成"—— 正确实现真的被挡住
// 时必然超时，而即便操作侥幸跑完，结果也被这里吃掉，调用方第二次等待还是超时）。
func lockObservationProbe(done <-chan error) error {
	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("mutation returned successfully before reaching the expected lock")
		}
		return fmt.Errorf("mutation failed before reaching the expected lock: %v", err)
	default:
		return nil
	}
}

// tidbLockWaiting 尝试用 MySQL/TiDB 侧的锁等待视图**强证明** pid 上的语句正在等行锁。
//
// 返回 available=false 表示这个 TiDB 版本没有提供可查询的锁等待视图（表/列不存在、权限
// 不足、视图不维护等待状态），调用方据此降级到保守观察。
func tidbLockWaiting(observer *DB, pid int) (waiting bool, available bool) {
	var pending int
	err := observer.QueryRow(`SELECT COUNT(1) FROM information_schema.innodb_trx
 WHERE trx_mysql_thread_id = ? AND trx_state = 'LOCK WAIT'`, pid).Scan(&pending)
	if err != nil {
		return false, false
	}
	return pending > 0, true
}

// waitForBackendBlock 证明被观察的写操作**确实被对手事务挡在锁上**，返回后调用方再释放
// blocker，并**唯一一次**消费 done（审查 F05 恢复的协议）。
//
// 调用方契约（每个用例都遵守）：
//  1. 先用自己的一条事务对被观察操作**必需**的那一行取 FOR UPDATE（任务行 / agent 行 /
//     会话行 / profile 行，锁的都是产品代码确实要加锁的行，不是 PG 外键派生的隐式锁）；
//  2. 启动被观察操作 → waitForBackendBlock 证明它真的停在锁上（不消费 done）；
//     期间用例还可断言 assertProfilesUnlocked —— 只有"确实阻塞了"这条断言才有意义；
//  3. 回滚 blocker → 操作继续 → waitForMutationResult 收唯一那个完成结果。
//
// 观察分两级：
//   - 强观察：information_schema.innodb_trx 里该连接处于 LOCK WAIT（TiDB 若维护该状态）；
//   - 保守观察：视图不可用、或预算的一半内始终没有 LOCK WAIT（部分 TiDB 版本不在视图里
//     维护等待状态）。此时退到"settle 窗口内操作仍未返回"：blocker 持的正是被观察操作
//     必需的那把行锁，没被挡住的实现会在窗口内直接跑完并把结果放进带缓冲的 done，
//     所以"根本没阻塞"依然判红；只是它证明不了"等的恰好是那把锁"，这一点写进实施报告。
func waitForBackendBlock(observer *DB, pid int, done <-chan error) error {
	const (
		budget       = 10 * time.Second
		settleWindow = 500 * time.Millisecond
	)
	start := time.Now()
	viewTrusted := true
	for time.Since(start) < budget {
		if err := lockObservationProbe(done); err != nil {
			return err
		}
		if viewTrusted {
			waiting, available := tidbLockWaiting(observer, pid)
			switch {
			case waiting:
				return nil
			case !available:
				viewTrusted = false
			}
		}
		if !viewTrusted {
			// 保守观察：等一个 settle 窗口，再确认它**仍然**没有返回。
			time.Sleep(settleWindow)
			if err := lockObservationProbe(done); err != nil {
				return err
			}
			return nil
		}
		time.Sleep(10 * time.Millisecond)
		if time.Since(start) > budget/2 {
			viewTrusted = false
		}
	}
	return fmt.Errorf("connection %d did not block within %s", pid, budget)
}

// assertProfilesUnlocked 断言这些 profile 行此刻没有被持有者锁住（即删除路径没有在
// 任务行之前长期占有 profile 行锁）。
//
// PG 用"拿不到锁立刻失败"的加锁变体区分"拿到锁"与"锁被别人持有"；TiDB 不认该关键字，
// 等价写法是把本会话 innodb_lock_wait_timeout 压到 1s 再 FOR UPDATE：无人持有 → 立即
// 返回；被别人持有 → 1s 内报 1205 锁等待超时，同样能立刻判定。
func assertProfilesUnlocked(t *testing.T, d *DB, profileIDs ...int64) {
	probe, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback()
	if _, err := probe.Exec(`SET SESSION innodb_lock_wait_timeout = 1`); err != nil {
		t.Fatal(err)
	}
	for _, profileID := range profileIDs {
		var lockedID int64
		if err := probe.QueryRow(`SELECT id FROM llm_profiles WHERE id = ? FOR UPDATE`, profileID).Scan(&lockedID); err != nil {
			t.Fatalf("profile %d was locked before the task row: %v", profileID, err)
		}
	}
}

func waitForMutationResult(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(12 * time.Second):
		return fmt.Errorf("timed out waiting for task LLM mutation")
	}
}
