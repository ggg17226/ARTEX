package lockorder_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	_ "github.com/go-sql-driver/mysql"
)

// TestDeadlockImmunity_UnitReverseOrder 在纯单元测试层验证全序排序器对并发反序请求的死锁免疫
func TestDeadlockImmunity_UnitReverseOrder(t *testing.T) {
	// 模拟两个 goroutine 以相反的业务顺序请求锁：
	// Goroutine A 请求顺序: Tasks(10), Companies(5)
	// Goroutine B 请求顺序: Companies(5), Tasks(10)
	// 若无全序排序器直接加锁将形成循环等待死锁；
	// 经过 SortEntityKeys 排序后，二者均按 Tasks(10) -> Companies(5) 顺序执行，零死锁。

	reqA := []lockorder.EntityKey{
		{Table: lockorder.TableOrderTasks, PK: 10},
		{Table: lockorder.TableOrderCompanies, PK: 5},
	}
	reqB := []lockorder.EntityKey{
		{Table: lockorder.TableOrderCompanies, PK: 5},
		{Table: lockorder.TableOrderTasks, PK: 10},
	}

	sortedA := lockorder.SortEntityKeys(reqA)
	sortedB := lockorder.SortEntityKeys(reqB)

	if len(sortedA) != 2 || len(sortedB) != 2 {
		t.Fatalf("unexpected sorted lengths")
	}

	for i := 0; i < 2; i++ {
		if sortedA[i] != sortedB[i] {
			t.Fatalf("expected identical total order at slot %d: A=%+v, B=%+v", i, sortedA[i], sortedB[i])
		}
	}

	// 确认第一位均为 Tasks (TableOrder 1)，第二位均为 Companies (TableOrder 5)
	if sortedA[0].Table != lockorder.TableOrderTasks || sortedA[1].Table != lockorder.TableOrderCompanies {
		t.Errorf("expected Tasks then Companies, got %+v", sortedA)
	}
}

// TestDeadlockImmunity_TiDBIntegration 在真实外部 TiDB 环境下执行高并发交叉行锁压力测试
func TestDeadlockImmunity_TiDBIntegration(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("跳过：环境变量 ARTEX_TIDB_DSN 未配置，标记为「未验证」(Honest Skip)")
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open tidb dsn failed: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("跳过：TiDB 连接无法 Ping 通 (%v)，标记为「未验证」(Honest Skip)", err)
		return
	}

	// 准备临时测试行，使用 locks 表的实体行隔离测试
	_, err = db.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES ('deadlock_test_1'), ('deadlock_test_2')")
	if err != nil {
		t.Fatalf("prepare test rows in locks table: %v", err)
	}

	metaLocks, err := lockorder.LookupTableName("locks")
	if err != nil {
		t.Fatalf("lookup locks meta: %v", err)
	}

	var rowID1, rowID2 uint64
	if err := db.QueryRowContext(ctx, "SELECT row_id FROM locks WHERE name = 'deadlock_test_1'").Scan(&rowID1); err != nil {
		t.Fatalf("query rowID1: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT row_id FROM locks WHERE name = 'deadlock_test_2'").Scan(&rowID2); err != nil {
		t.Fatalf("query rowID2: %v", err)
	}

	runner := lockorder.NewTxRunner(db)
	const concurrentWorkers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, concurrentWorkers)

	for i := 0; i < concurrentWorkers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			// 奇数 worker 业务序 (row1, row2)，偶数 worker 反序 (row2, row1)
			rawCandidates := []lockorder.EntityKey{
				{Table: metaLocks.Order, PK: rowID1},
				{Table: metaLocks.Order, PK: rowID2},
			}
			if workerID%2 == 1 {
				rawCandidates = []lockorder.EntityKey{
					{Table: metaLocks.Order, PK: rowID2},
					{Table: metaLocks.Order, PK: rowID1},
				}
			}

			err := runner.Run(ctx, lockorder.TxRunnerOptions{
				RequiresBusinessGate: false,
			}, func(txCtx context.Context, tx *sql.Tx, lockCtx *lockorder.LockContext) error {
				// 模拟通过 Locator 收集并排序候选集
				sorted := lockorder.SortEntityKeys(rawCandidates)
				snapshot := &lockorder.LocatorSnapshot{
					CandidateKeys: sorted,
				}

				// 严格全序加锁并验证当前态
				verifier := func(vCtx context.Context, q lockorder.QueryRower, k lockorder.EntityKey) (*lockorder.EntityState, error) {
					return &lockorder.EntityState{Key: k, Live: true}, nil
				}

				_, lockErr := lockorder.LockAndVerify(txCtx, tx, lockCtx, snapshot, verifier)
				if lockErr != nil {
					return lockErr
				}

				// 模拟事务工作耗时
				time.Sleep(10 * time.Millisecond)
				return nil
			})

			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("worker encountered error under concurrent load: %v", err)
	}
}
