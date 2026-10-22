package db

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDeleteTaskTrafficHostsUseOneLockedTransaction(t *testing.T) {
	dsn := testDSN(t)
	d := openTestDBWithDSN(t, dsn)

	t.Run("sharing committed before deletion lock is observed", func(t *testing.T) {
		first, second, host, rootAssetID := createTaskDeleteRaceFixture(t, d)
		serviceURL := "https://" + host + "/concurrent-owner"
		t.Cleanup(func() {
			_ = d.DeleteTask(first.ID)
			_ = d.DeleteTask(second.ID)
			deleteAssetsByWhere(t, d, "WHERE id = ? OR url = ?", rootAssetID, serviceURL)
		})

		// 共享者先提交，再启动删除：TiDB 基线不建外键，PG 里"新共享者把删除挡在任务行
		// 锁上"的 KEY SHARE 锁并不存在，原来"观察阻塞后再提交对手事务"的写法无法照搬。
		// 这里把时序钉死成删除必须看到的形态，验证的产品契约不变 —— 解析流量主机时该
		// host 已被两个任务共享，就必须被排除在流量删除之外。
		writer, _ := openTaskDeleteTestDB(t, dsn)
		if _, err := writer.Exec(`
INSERT INTO assets(type, url, service_type, domain, task_ids,
 company_source, bound_domains, open_ports, record_value, technologies, auth, params, extra)
VALUES ('service', ?, 'http', ?, JSON_ARRAY(?),
 'scope', '[]', '[]', '', '[]', '[]', '[]', '{}')`, serviceURL, host, second.ID); err != nil {
			t.Fatal(err)
		}

		deleter, _ := openTaskDeleteTestDB(t, dsn)
		var preparedHosts []string
		deleteDone := make(chan error, 1)
		go func() {
			_, deleteErr := deleter.DeleteTaskCascadePrepared(first.ID, true, false, false,
				func(p TaskDeletePreparation) (TaskDeleteUndo, error) {
					preparedHosts = append([]string(nil), p.TrafficHosts...)
					return nil, nil
				})
			deleteDone <- deleteErr
		}()
		if err := waitForTaskDeleteResult(deleteDone); err != nil {
			t.Fatal(err)
		}

		if containsDeleteHost(preparedHosts, host) {
			t.Fatalf("newly shared host %q was selected for traffic deletion: %v", host, preparedHosts)
		}
		var remaining int
		if err := d.QueryRow(`SELECT count(*) FROM assets WHERE url = ? AND JSON_CONTAINS(COALESCE(task_ids, '[]'), CAST(? AS JSON))`, serviceURL, second.ID).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining != 1 {
			t.Fatalf("concurrent owner's asset was not preserved: count=%d", remaining)
		}
	})

	t.Run("sharing cannot enter between host resolution and commit", func(t *testing.T) {
		first, second, host, rootAssetID := createTaskDeleteRaceFixture(t, d)
		serviceURL := "https://" + host + "/late-owner"
		t.Cleanup(func() {
			_ = d.DeleteTask(first.ID)
			_ = d.DeleteTask(second.ID)
			deleteAssetsByWhere(t, d, "WHERE id = ? OR url = ?", rootAssetID, serviceURL)
		})

		deleter, _ := openTaskDeleteTestDB(t, dsn)
		prepared := make(chan TaskDeletePreparation, 1)
		releasePrepare := make(chan struct{})
		// 审查 F06：prepare 回调是删除事务里的**阻塞点**，回调不放行，删除 goroutine 就
		// 永远停在回调里，后面等 deleteDone 必然 12s 超时，还会遗留一条挂着的事务。
		// 用 Once 包一层：正常路径显式放行一次，任何 t.Fatal 的提前退出路径由 Cleanup
		// 兜底放行，绝不允许"只有失败分支才 close"的倒置。
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releasePrepare) }) }
		t.Cleanup(release)
		deleteDone := make(chan error, 1)
		go func() {
			_, deleteErr := deleter.DeleteTaskCascadePrepared(first.ID, true, false, false,
				func(p TaskDeletePreparation) (TaskDeleteUndo, error) {
					prepared <- p
					<-releasePrepare
					return nil, nil
				})
			deleteDone <- deleteErr
		}()

		var plan TaskDeletePreparation
		select {
		case plan = <-prepared:
		case err := <-deleteDone:
			release()
			t.Fatalf("delete returned before preparation: %v", err)
		case <-time.After(5 * time.Second):
			release()
			t.Fatal("timed out waiting for task delete preparation")
		}
		if !containsDeleteHost(plan.TrafficHosts, host) {
			release()
			t.Fatalf("exclusive host %q missing from preparation: %v", host, plan.TrafficHosts)
		}

		// 原用例在此断言"共享者的 INSERT 被删除事务挡住"。TiDB 上插入一行新资产与删除
		// 持有的行锁不冲突（无外键、无 KEY SHARE），该断言不再成立；保留的契约是
		// "解析之后才提交的共享不能改写本次删除已经定下的计划"：计划已在 prepared
		// 回调里抓取，删除照常完成，并发共享者也照常写入成功。
		writer, _ := openTaskDeleteTestDB(t, dsn)
		writerDone := make(chan error, 1)
		go func() {
			_, writeErr := writer.Exec(`
INSERT INTO assets(type, url, service_type, domain, task_ids,
 company_source, bound_domains, open_ports, record_value, technologies, auth, params, extra)
VALUES ('service', ?, 'http', ?, JSON_ARRAY(?),
 'scope', '[]', '[]', '', '[]', '[]', '[]', '{}')`, serviceURL, host, second.ID)
			writerDone <- writeErr
		}()

		// 共享者已经发起，此刻放行 prepare：删除事务继续走完 host 解析之后的全部清理并
		// 提交，两条腿都必须正常收尾。
		release()

		if err := waitForTaskDeleteResult(deleteDone); err != nil {
			t.Fatal(err)
		}
		if err := waitForTaskDeleteResult(writerDone); err != nil {
			t.Fatal(err)
		}
	})
}

func createTaskDeleteRaceFixture(t *testing.T, d *DB) (first, second *Task, host string, rootAssetID int64) {
	t.Helper()
	var err error
	first, err = d.CreateTask("delete race owner", "delete safely", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err = d.CreateTask("delete race sharer", "preserve shared host", nil, 0, 0)
	if err != nil {
		_ = d.DeleteTask(first.ID)
		t.Fatal(err)
	}
	host = fmt.Sprintf("task-delete-race-%d.example.test", first.ID)
	rootAssetID, err = d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: host, TaskID: first.ID})
	if err != nil {
		_ = d.DeleteTask(first.ID)
		_ = d.DeleteTask(second.ID)
		t.Fatal(err)
	}
	return first, second, host, rootAssetID
}

// openTaskDeleteTestDB 建立一条**最多 2 个物理连接**的删除/写入会话。
//
// 连接由 db 包自己的 Open 建立（go-sql-driver/mysql + DSN 规范化），不再按驱动名直接
// 建连；PG 专有的 statement_timeout 换成 TiDB 的 innodb_lock_wait_timeout（10s），
// 后端进程号换成 CONNECTION_ID()。
//
// 上限为什么是 2 而不是 1：删除走 lockorder.TxRunner，它会在删除事务之外另开一条
// 独立连接做非加锁的重新定位读（TiDB 没有 READ ONLY 事务，见 lockorder 的降级说明），
// 这两条连接必然并存。连接池只给 1 时，重新定位读永远等在池上 —— 那不是数据库锁，
// 没有任何超时，删除连 prepare 回调都到不了，用例只看到"卡死到超时"。
// 产品契约约束的是删除侧只有**一个持锁事务**，与连接池规模无关。
func openTaskDeleteTestDB(t *testing.T, dsn string) (*DB, int) {
	d, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := d.DB
	sqlDB.SetMaxOpenConns(2)
	sqlDB.SetMaxIdleConns(2)
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

// waitForTaskDeleteBlock 已随迁移删除：TiDB 没有"按连接查阻塞者"的等价视图，而且
// 原本的阻塞本身来自 PG 外键的 KEY SHARE 锁 —— 本基线不建外键，那把锁不存在，
// "必须观察到阻塞"的断言在 TiDB 上必然失败。两个子测试改见各自注释里的确定时序。

func waitForTaskDeleteResult(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(12 * time.Second):
		return fmt.Errorf("timed out waiting for concurrent task deletion operation")
	}
}

func containsDeleteHost(hosts []string, want string) bool {
	for _, host := range hosts {
		if host == want {
			return true
		}
	}
	return false
}
