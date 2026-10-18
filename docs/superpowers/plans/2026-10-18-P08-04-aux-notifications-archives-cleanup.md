# P08-04 实施计划：辅助模块、通知租赁、归档白名单与依赖切除

> **计划编号**：`P08-04`  
> **所属阶段**：阶段 4（辅助模块、通知租赁、归档白名单与依赖切除）  
> **关联 Spec**：`docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md` §3.4  
> **目标**：重构会话、触发器与拦截执行方言；通知队列改用 CAS 租约状态机（基于 `notification_deliveries` 与 `next_attempt_at` 真实列，并引入带精确截止时间的代次 Fencing 保护）；建立任务归档快照显式列投影双白名单（严格对齐真实 Catalog），彻底排除隐藏主键 `row_id` 与生成列；从 `go.mod` 彻底切除 `github.com/jackc/pgx/v5` 依赖。

---

## 涵盖的产品业务文件清单（12 个产品业务文件，精准映射）

| 文件路径 | R4 Blocker 数 | 核心职责与 M 项 |
|---|---:|---|
| `db/conversation.go` | 16 | 会话 CRUD、消息方言重构与删除显式闭包清理（**M20**） |
| `db/triggers.go` | 13 | 触发器调度方言，补齐 `scheduler_state` 键 hash（**M13**） |
| `db/chat_mentions.go` | 1 | 提及记录方言重构（**M20**） |
| `db/notification.go` | 13 | 通知渠道 CRUD 与存在性判断（**M33**） |
| `db/notification_delivery.go` | 15 | 租约状态机，对齐 `notification_deliveries` 与 `next_attempt_at`，支持代次 Fencing（**M33**） |
| `db/task_archives.go` | 56 | 归档快照导出，建立 7 表真实列投影白名单排除 `row_id`（**M06**） |
| `db/task_archives_restore.go` | 52 | 归档恢复导入，重算自增物理键并保持逻辑关联（**M25**） |
| `db/skill_usage.go` | 4 | 技能使用记录方言重构（**M20**） |
| `db/side_questions.go` | 20 | 旁路问题 CRUD，补齐请求 identity/hash，消除快照版本倒退（**M17, M28**） |
| `db/constraints.go` | 4 | 约束条件 CRUD 与 RowsAffected 适配（**M31**） |
| `db/intercept_detail.go` | 3 | 拦截详情记录方言重构（**M20**） |
| `db/intercept_execution.go` | 4 | 拦截执行状态与日志方言重构（**M20**） |
| **小计** | **201** | **覆盖阶段 4 全部 201 处 Blocker 与核心 M 项** |

---

## Task 4.1: 通知队列两阶段 CAS 租约状态机与代次 Fencing 保护重构

### 任务定义
- **修改文件**：`db/notification_delivery.go`, `db/notification.go`
- **新建测试**：`db/notification_delivery_test.go`
- **核心逻辑与 M 项绑定（M33）**：
  1. 真实表为 `notification_deliveries`，重试与租约字段为 `next_attempt_at`；
  2. 废除 `SKIP LOCKED`，采用两阶段有界 CAS 抢占：
     - Step 1: `SELECT id, channel_id, attempts FROM notification_deliveries WHERE (state = 'pending' AND next_attempt_at <= NOW(3)) OR (state = 'sending' AND next_attempt_at < NOW(3)) ORDER BY id ASC LIMIT ? FOR UPDATE`；
     - Step 2: `UPDATE notification_deliveries SET state = 'sending', next_attempt_at = ?, attempts = attempts + 1 WHERE id IN (...)`，其中 `?` 传入计算得到的截止时间 `assignedDeadline`；
  3. **代次 Fencing 回填契约（闭环 F05）**：
     - `MarkDeliveriesSent`, `RescheduleDeliveries`, `DeferDeliveries`, `FailDeliveries` 必须携带原 `assignedDeadline` 作为版本令牌：
       `WHERE id = ? AND state = 'sending' AND next_attempt_at = ?`；
     - 若超时已被下个 worker 抢占，该行的 `next_attempt_at` 已变更为新截止时间，原 worker 迟到 ACK 匹配行数为 0，丢弃迟到 ACK，绝不覆盖新租约。
  4. **四类 Fencing 回填可执行失败判据（R51-F07-02 整改项，必须由 Step 1 测试落地）**：
     - **签名改造**：四个回填方法改为显式携带租约截止时间代次令牌并返回影响行数，供测试观察 fencing 效果：`MarkDeliveriesSent(ctx, ids, leaseDeadline) (int64, error)`、`RescheduleDeliveries(ctx, ids, leaseDeadline, delay, errMsg) (int64, error)`、`DeferDeliveries(ctx, ids, leaseDeadline, reason) (int64, error)`、`FailDeliveries(ctx, ids, leaseDeadline, errMsg) (int64, error)`；
     - **标准场景时序**：Worker A 领取（记录 A 的截止时间与 `attempts`）→ A 租约超时到期（截止时间被移到过去）→ Worker B 重领（产生**新截止时间**且 `attempts` 恰好 +1）→ Worker A 滞后回填（携带 A 的旧截止时间 fence 条件）→ **四类回填方法影响行数必须严格为 0**，且 Worker B 的当前租约（`next_attempt_at`）与状态（`state='sending'`、`attempts`）完全不受影响；
     - **正向路径**：Worker B 以正确新令牌回填成功（影响行数 = 1，状态正常前进为 `sent`），证明 fencing 未误杀当前租约；
     - **判红口径**：**任何不带 fencing 条件（缺 `state = 'sending' AND next_attempt_at = ?` 谓词）的更新实现必须判红**；迟到 ACK 覆盖 B 的截止时间、`attempts` 或终态同样判红。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写通知队列并发租约抢占与 Fencing 拦截测试（`db/notification_delivery_test.go`）**
  ```go
  package db

  import (
      "context"
      "encoding/json"
      "fmt"
      "os"
      "testing"
      "time"
      "github.com/Autumn-27/artex/notify"
  )

  func TestNotification_ClaimRealtimeDeliveries_TiDBDialect(t *testing.T) {
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

      ctx := context.Background()
      chName := fmt.Sprintf("p08_ch_%d", time.Now().UnixNano())
      ch := &NotificationChannel{
          Name:       chName,
          Kind:       "dingtalk",
          Mode:       "realtime",
          Config:     json.RawMessage(`{"webhook":"http://127.0.0.1:1"}`),
          Filter:     json.RawMessage(`{}`),
          RatePerMin: 100,
      }
      chID, err := d.SaveNotificationChannel(ctx, ch)
      if err != nil {
          t.Fatalf("SaveNotificationChannel failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel_id = ?", chID); err != nil {
              t.Errorf("cleanup channel deliveries failed: %v", err)
          }
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_channels WHERE id = ?", chID); err != nil {
              t.Errorf("cleanup notification channel root failed: %v", err)
          }
          var dCount, cCount int
          if err := d.QueryRow("SELECT count(*) FROM notification_deliveries WHERE channel_id = ?", chID).Scan(&dCount); err != nil {
              t.Errorf("query residual channel deliveries failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM notification_channels WHERE id = ?", chID).Scan(&cCount); err != nil {
              t.Errorf("query residual notification channel failed: %v", err)
          }
          if dCount != 0 || cCount != 0 {
              t.Errorf("residual channel fixtures found: deliveries=%d, channels=%d", dCount, cCount)
          }
      })

      evID, err := d.AddNotificationEvent(ctx, "finding_created", 1001, notify.Snapshot{Severity: "high", VulnClass: "XSS"})
      if err != nil {
          t.Fatalf("AddNotificationEvent failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel_id = ? OR event_id = ?", chID, evID); err != nil {
              t.Errorf("cleanup notification_deliveries failed: %v", err)
          }
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_events WHERE id = ?", evID); err != nil {
              t.Errorf("cleanup notification_events failed: %v", err)
          }
          var delCount, eCount int
          if err := d.QueryRow("SELECT count(*) FROM notification_deliveries WHERE event_id = ?", evID).Scan(&delCount); err != nil {
              t.Errorf("query residual event deliveries failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM notification_events WHERE id = ?", evID).Scan(&eCount); err != nil {
              t.Errorf("query residual notification event failed: %v", err)
          }
          if delCount != 0 || eCount != 0 {
              t.Errorf("residual event fixtures found: deliveries=%d, events=%d", delCount, eCount)
          }
      })

      _, _, err = d.FanOutPendingEvents(ctx, 10)
      if err != nil {
          t.Fatalf("FanOutPendingEvents failed: %v", err)
      }

      ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
      defer cancel()
      deliveries, err := d.ClaimRealtimeDeliveries(ctxTimeout, chID, 10, 10*time.Second)
      if err != nil {
          t.Fatalf("ClaimRealtimeDeliveries failed: %v", err)
      }
      if len(deliveries) == 0 {
          t.Fatalf("expected at least 1 claimed delivery for channel %d", chID)
      }
      var matched bool
      for _, dl := range deliveries {
          if dl.EventID == evID && dl.ChannelID == chID && dl.State == "sending" {
              matched = true
              break
          }
      }
      if !matched {
          t.Fatalf("expected claimed delivery matching EventID %d and ChannelID %d with state 'sending', got %+v", evID, chID, deliveries)
      }
  }
  ```

  测试① 仅覆盖首次领取与三元匹配，**不是四种 Fencing 回填测试**；Step 1 必须补充测试②（`MarkDeliveriesSent`、`RescheduleDeliveries`、`DeferDeliveries`、`FailDeliveries` 四类回填逐一覆盖 A 领取 → 到期 → B 重领 → A 旧令牌回填判 0 行的完整时序）：

  **② 四类 Fencing 回填：A 租约过期后旧令牌回填必须 0 行影响（`TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods`）**

  ```go
  package db

  import (
      "context"
      "encoding/json"
      "fmt"
      "os"
      "testing"
      "time"

      "github.com/Autumn-27/artex/notify"
  )

  func TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods(t *testing.T) {
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

      ctx := context.Background()
      chName := fmt.Sprintf("p08_fence_ch_%d", time.Now().UnixNano())
      ch := &NotificationChannel{
          Name:       chName,
          Kind:       "dingtalk",
          Mode:       "realtime",
          Config:     json.RawMessage(`{"webhook":"http://127.0.0.1:1"}`),
          Filter:     json.RawMessage(`{}`),
          RatePerMin: 100,
      }
      chID, err := d.SaveNotificationChannel(ctx, ch)
      if err != nil {
          t.Fatalf("SaveNotificationChannel failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel_id = ?", chID); err != nil {
              t.Errorf("cleanup channel deliveries failed: %v", err)
          }
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_channels WHERE id = ?", chID); err != nil {
              t.Errorf("cleanup notification channel root failed: %v", err)
          }
          var dCount, cCount int
          if err := d.QueryRow("SELECT count(*) FROM notification_deliveries WHERE channel_id = ?", chID).Scan(&dCount); err != nil {
              t.Errorf("query residual channel deliveries failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM notification_channels WHERE id = ?", chID).Scan(&cCount); err != nil {
              t.Errorf("query residual notification channel failed: %v", err)
          }
          if dCount != 0 || cCount != 0 {
              t.Errorf("residual channel fixtures found: deliveries=%d channels=%d", dCount, cCount)
          }
      })

      evID, err := d.AddNotificationEvent(ctx, "finding_created", 2002, notify.Snapshot{Severity: "high", VulnClass: "SQLI"})
      if err != nil {
          t.Fatalf("AddNotificationEvent failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel_id = ? OR event_id = ?", chID, evID); err != nil {
              t.Errorf("cleanup notification_deliveries failed: %v", err)
          }
          if _, err := d.ExecContext(ctx, "DELETE FROM notification_events WHERE id = ?", evID); err != nil {
              t.Errorf("cleanup notification_events failed: %v", err)
          }
          var delCount, eCount int
          if err := d.QueryRow("SELECT count(*) FROM notification_deliveries WHERE event_id = ?", evID).Scan(&delCount); err != nil {
              t.Errorf("query residual event deliveries failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM notification_events WHERE id = ?", evID).Scan(&eCount); err != nil {
              t.Errorf("query residual notification event failed: %v", err)
          }
          if delCount != 0 || eCount != 0 {
              t.Errorf("residual event fixtures found: deliveries=%d events=%d", delCount, eCount)
          }
      })

      if _, _, err := d.FanOutPendingEvents(ctx, 10); err != nil {
          t.Fatalf("FanOutPendingEvents failed: %v", err)
      }

      // Worker A 领取：记录 A 的租约代次（截止时间与 attempts）
      claimedA, err := d.ClaimRealtimeDeliveries(ctx, chID, 10, 10*time.Second)
      if err != nil {
          t.Fatalf("worker A ClaimRealtimeDeliveries failed: %v", err)
      }
      var targetA *NotificationDelivery
      for _, dl := range claimedA {
          if dl.EventID == evID && dl.ChannelID == chID && dl.State == "sending" {
              targetA = dl
              break
          }
      }
      if targetA == nil {
          t.Fatalf("worker A must claim the sending delivery for event %d", evID)
      }
      deadlineA, attemptsA := targetA.NextAttemptAt, targetA.Attempts

      // A 租约超时到期：截止时间移到过去，模拟 Worker A 处理过慢
      if _, err := d.ExecContext(ctx, "UPDATE notification_deliveries SET next_attempt_at = ? WHERE id = ?", time.Now().Add(-2*time.Minute), targetA.ID); err != nil {
          t.Fatalf("expire worker A lease failed: %v", err)
      }

      // Worker B 重领：必须产生新截止时间且 attempts 恰好 +1
      claimedB, err := d.ClaimRealtimeDeliveries(ctx, chID, 10, 60*time.Second)
      if err != nil {
          t.Fatalf("worker B ClaimRealtimeDeliveries failed: %v", err)
      }
      var targetB *NotificationDelivery
      for _, dl := range claimedB {
          if dl.ID == targetA.ID {
              targetB = dl
              break
          }
      }
      if targetB == nil {
          t.Fatalf("worker B must reclaim delivery %d after expiry", targetA.ID)
      }
      deadlineB, attemptsB := targetB.NextAttemptAt, targetB.Attempts
      if !deadlineB.After(deadlineA) || attemptsB != attemptsA+1 {
          t.Fatalf("reclaim must issue a newer deadline and increment attempts exactly once: deadline %v -> %v, attempts %d -> %d", deadlineA, deadlineB, attemptsA, attemptsB)
      }

      // Worker A 带旧 fence 滞后回填：四类回填方法影响行数必须逐一严格为 0，且不触碰 B 的租约
      lateCalls := []struct {
          name string
          call func() (int64, error)
      }{
          {"MarkDeliveriesSent", func() (int64, error) { return d.MarkDeliveriesSent(ctx, []int64{targetA.ID}, deadlineA) }},
          {"RescheduleDeliveries", func() (int64, error) { return d.RescheduleDeliveries(ctx, []int64{targetA.ID}, deadlineA, 30*time.Second, "late ack") }},
          {"DeferDeliveries", func() (int64, error) { return d.DeferDeliveries(ctx, []int64{targetA.ID}, deadlineA, "late defer") }},
          {"FailDeliveries", func() (int64, error) { return d.FailDeliveries(ctx, []int64{targetA.ID}, deadlineA, "late fail") }},
      }
      for _, late := range lateCalls {
          affected, err := late.call()
          if err != nil {
              t.Fatalf("%s with stale fence must not error, got: %v", late.name, err)
          }
          if affected != 0 {
              t.Fatalf("%s with stale fence must affect exactly 0 rows (fencing violated), got %d", late.name, affected)
          }
          var stateNow string
          var deadlineNowAt time.Time
          var attemptsNow int
          if err := d.QueryRow("SELECT state, next_attempt_at, attempts FROM notification_deliveries WHERE id = ?", targetA.ID).Scan(&stateNow, &deadlineNowAt, &attemptsNow); err != nil {
              t.Fatalf("query delivery after %s failed: %v", late.name, err)
          }
          if stateNow != "sending" || !deadlineNowAt.UTC().Equal(deadlineB.UTC()) || attemptsNow != attemptsB {
              t.Fatalf("%s with stale fence mutated worker B lease: state=%s deadline=%v attempts=%d (want sending/%v/%d)", late.name, stateNow, deadlineNowAt, attemptsNow, deadlineB, attemptsB)
          }
      }

      // 正向路径：B 以正确当前令牌回填成功，证明 fencing 未误杀当前租约
      affected, err := d.MarkDeliveriesSent(ctx, []int64{targetA.ID}, deadlineB)
      if err != nil {
          t.Fatalf("worker B MarkDeliveriesSent with current fence failed: %v", err)
      }
      if affected != 1 {
          t.Fatalf("worker B MarkDeliveriesSent with current fence must affect exactly 1 row, got %d", affected)
      }
      var finalState string
      if err := d.QueryRow("SELECT state FROM notification_deliveries WHERE id = ?", targetA.ID).Scan(&finalState); err != nil {
          t.Fatalf("query final delivery state failed: %v", err)
      }
      if finalState != "sent" {
          t.Fatalf("delivery must transition to sent under the current lease token, got %s", finalState)
      }
  }
  ```

  **F05 判红口径汇总**：任何不带 `state = 'sending' AND next_attempt_at = ?` fencing 谓词的回填实现（A 旧令牌回填后 affected != 0，或 B 的 `state`/`next_attempt_at`/`attempts` 被迟到 ACK 篡改）必须在测试② 判红；四类回填方法中任一方法缺失 fencing 条件，同样会被对应 `lateCalls` 分支检出。

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestNotification_ClaimRealtimeDeliveries_TiDBDialect|TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods' -count=1)
  ```

- [ ] **Step 3: 重构 `db/notification_delivery.go` 与 `db/notification.go`**
  1. 实现上述 CAS 抢占与租约防过期更新逻辑；
  2. 将 `MarkDeliveriesSent`、`RescheduleDeliveries`、`DeferDeliveries`、`FailDeliveries` 签名改造为携带 `leaseDeadline time.Time` 代次令牌并返回 `int64` 影响行数，UPDATE 语句强制绑定 `WHERE id = ? AND state = 'sending' AND next_attempt_at = ?`；同步更新全部调用方。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestNotification_ClaimRealtimeDeliveries_TiDBDialect|TestNotification_FencingBackfill_ALateWriteIsRejectedByFourMethods' -count=1)
  ```

---

## Task 4.2: 任务归档真实 Catalog 双白名单与全图冲突裁决

### 任务定义
- **修改文件**：`db/task_archives.go`, `db/task_archives_restore.go`
- **新建测试**：`db/task_archives_test.go`
- **核心逻辑与 M 项绑定（M06, M25，严格对齐真实 Catalog，闭环 F06）**：
  1. 静态投影双白名单（导出与写入列）：
     - `task_relations`：`[task_id, source_task_id, created_at]`（严格排除不存在的 `kind`）
     - `task_asset_links`：`[task_id, asset_id, source_node_id, source, source_summary, created_at, updated_at]`（完整保留来源信息）
     - `task_llm_profiles`：`[task_id, profile_id, position, status, last_error, exhausted_at, created_at, updated_at]`（严格排除不存在的 `role`）
     - `exploration_edges`：`[exploration_id, src_id, dst_id, rel, created_at]`（对齐真实列名）
     - `exploration_anchors`：`[node_id, asset_id]`（对齐真实列名，严格排除 `anchor_type`）
     - `side_question_sessions`：`[session_key, conversation_id, task_id, exploration_id, intent_id, run_id, version, snapshot, generation, memory]`（对齐真实列名，排除不存在的 `status/created_at/updated_at`）
     - `traffic_evidence_snapshots`：`[id, source_traffic_id, captured_at, url, method, status, content_type, req_head, resp_head, req_hash, resp_hash, req_len, resp_len, unreferenced_at, created_at]`（对齐真实列名）
     - `side_question_requests`：严格排除生成列 `running_session_key`；
  2. 全图冲突裁决：
     - 核心图实体（task, exploration, nodes, findings）：冲突时报错拒绝（fail-closed，除非是恢复目标所属的合法归档 stub）；
     - 纯关联表与叶子：导入时缺省物理 `row_id`，由 TiDB 自动重新分配；
     - 导入前必须对带 hash 的实体重新计算 canonical identity 与 hash，在事务内执行 `VerifyHashArbitration` 验证。
  3. **七表非空 round-trip 与冲突 Fail-Closed 可执行失败判据（R51-F07-02 整改项，必须由 Step 1 落地）**：
     - **非空 fixture**：被验收任务必须在 7 表（`task_relations`、`task_asset_links`、`task_llm_profiles`、`exploration_edges`、`exploration_anchors`、`side_question_sessions`、`traffic_evidence_snapshots`）各灌入至少 1 行真实数据（含所需父实体：exploration 节点、资产、`llm_profiles` 配置、finding + `finding_traffic_bindings`），导出前逐表预断言非空，防 no-op 假绿；
     - **Manifest 严格白名单**：`SnapshotTaskArchive` 导出 Manifest 必须严格仅含上述 14 份白名单列（逐表列集合 == 白名单集合，合计 0 个 `row_id`、0 个生成列）；多一列（含 `row_id`/生成列）或少一列均判红；
     - **恢复 round-trip 保真**：源任务物理删除后由快照恢复：7 表全量恢复；`task_asset_links` 的 `source`、`source_summary`、`source_node_id` 三列保真；物理主键（隐藏 `row_id` 等自增物理键）不得重取重用、由 TiDB 重新分配；逻辑关联（`task_id`/`exploration_id`/`asset_id`/`session_key`/`src_id`/`dst_id` 映射）完整一致；
     - **冲突 Fail-Closed**：恢复前预置唯一键冲突（如预先占用 `uq_task_asset_links` 的 `(task_id, asset_id)` 组合），恢复**必须报错且整个事务回滚，目标任务 7 表新增行数全部为 0**；部分写入后提交的实现判红。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写归档快照与恢复真实 Catalog 白名单测试（`db/task_archives_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestTaskArchive_ExportManifest_UnmigratedFails(t *testing.T) {
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

      tName := fmt.Sprintf("P08 Archive Task %d", time.Now().UnixNano())
      task, err := d.CreateTaskWithOptions(tName, "goal", TaskCreateOptions{})
      if err != nil {
          t.Fatalf("CreateTaskWithOptions failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.DeleteTaskCascade(task.ID, false, false); err != nil {
              t.Errorf("cleanup task failed: %v", err)
          }
          // 独立逆拓扑兜底清理，杜绝孤儿残留
          if _, err := d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID); err != nil {
              t.Errorf("cleanup residual exploration_nodes failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", task.ExplorationID); err != nil {
              t.Errorf("cleanup residual exploration failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", task.ID); err != nil {
              t.Errorf("cleanup residual task root failed: %v", err)
          }
          var nCount, eCount, tCount int
          if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID).Scan(&nCount); err != nil {
              t.Errorf("query residual exploration_nodes failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM explorations WHERE id = ?", task.ExplorationID).Scan(&eCount); err != nil {
              t.Errorf("query residual explorations failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", task.ID).Scan(&tCount); err != nil {
              t.Errorf("query residual tasks failed: %v", err)
          }
          if nCount != 0 || eCount != 0 || tCount != 0 {
              t.Errorf("residual task fixtures found after cleanup: nodes=%d, explorations=%d, tasks=%d", nCount, eCount, tCount)
          }
      })

      snap, err := d.SnapshotTaskArchive(task.ID)
      if err != nil {
          t.Fatalf("SnapshotTaskArchive failed: %v", err)
      }
      if snap == nil || snap.TaskID != task.ID {
          t.Fatalf("expected non-nil archive snapshot matching TaskID %d, got %+v", task.ID, snap)
      }
  }
  ```

  测试① 仅覆盖空图快照冒烟，**不构成七表验收**；Step 1 必须补充测试②（七表非空灌入 + Manifest 严格 14 列白名单）与恢复 round-trip / 冲突 Fail-Closed 验收矩阵：

  **② 七表非空灌入与 Manifest 严格白名单（`TestTaskArchive_SevenTableManifestWhitelistStrict_UnmigratedFails`）**

  ```go
  package db

  import (
      "context"
      "encoding/json"
      "fmt"
      "os"
      "testing"
      "time"

      "github.com/Autumn-27/artex/db/schema"
      "github.com/Autumn-27/artex/sidequestion"
  )

  func TestTaskArchive_SevenTableManifestWhitelistStrict_UnmigratedFails(t *testing.T) {
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

      ctx := context.Background()
      ts := time.Now().UnixNano()
      task, err := d.CreateTaskWithOptions(fmt.Sprintf("P08 Archive 7T %d", ts), "goal", TaskCreateOptions{})
      if err != nil {
          t.Fatalf("CreateTaskWithOptions failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.DeleteTaskCascade(task.ID, false, false); err != nil {
              t.Errorf("cleanup task failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID); err != nil {
              t.Errorf("cleanup residual exploration_nodes failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", task.ExplorationID); err != nil {
              t.Errorf("cleanup residual exploration failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", task.ID); err != nil {
              t.Errorf("cleanup residual task root failed: %v", err)
          }
          var nCount, eCount, tCount int
          if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID).Scan(&nCount); err != nil {
              t.Errorf("query residual exploration_nodes failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM explorations WHERE id = ?", task.ExplorationID).Scan(&eCount); err != nil {
              t.Errorf("query residual explorations failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", task.ID).Scan(&tCount); err != nil {
              t.Errorf("query residual tasks failed: %v", err)
          }
          if nCount != 0 || eCount != 0 || tCount != 0 {
              t.Errorf("residual task fixtures found after cleanup: nodes=%d, explorations=%d, tasks=%d", nCount, eCount, tCount)
          }
      })

      // 父实体：两个探索节点、共享资产、llm_profiles 配置、finding（七表数据的外键前提）
      node1, err := d.Exploration(task.ExplorationID).AddNode("intent", map[string]any{"label": "arch-1"}, 1, "pending", "agent", nil)
      if err != nil {
          t.Fatalf("AddNode node1 failed: %v", err)
      }
      node2, err := d.Exploration(task.ExplorationID).AddNode("asset", map[string]any{"label": "arch-2"}, 2, "pending", "agent", nil)
      if err != nil {
          t.Fatalf("AddNode node2 failed: %v", err)
      }
      domain := fmt.Sprintf("p08-arch-%d.com", ts)
      assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
      if err != nil {
          t.Fatalf("UpsertRootDomain failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
              t.Errorf("cleanup asset failed: %v", err)
          }
          var aCount int
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", assetID).Scan(&aCount); err != nil {
              t.Errorf("query residual asset failed: %v", err)
          }
          if aCount != 0 {
              t.Errorf("residual asset found after cleanup: assets=%d", aCount)
          }
      })

      // 2. 创建真实 llm_profiles 与 finding (带完整 NOT NULL 字段与 name_identity/name_hash)
      profileName := fmt.Sprintf("p08-arch-profile-%d", ts)
      profileIdent := schema.LLMProfileNameIdentity(profileName)
      profileHash := schema.CanonicalHash(profileIdent)
      profileRes, err := d.Exec("INSERT INTO llm_profiles (name, name_identity, name_hash, format, model, reasoning_effort, thinking_type, max_tokens_field, session_header_key, created_at, updated_at) VALUES (?, ?, ?, 'openai', 'archive-test-model', '', '', '', '', NOW(3), NOW(3))", profileName, profileIdent, profileHash[:])
      if err != nil {
          t.Fatalf("insert llm_profiles fixture failed: %v", err)
      }
      profileID, err := profileRes.LastInsertId()
      if err != nil {
          t.Fatalf("llm_profiles LastInsertId failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM llm_profiles WHERE id = ?", profileID); err != nil {
              t.Errorf("cleanup llm_profiles failed: %v", err)
          }
          var pCount int
          if err := d.QueryRow("SELECT count(*) FROM llm_profiles WHERE id = ?", profileID).Scan(&pCount); err != nil {
              t.Errorf("query residual llm_profiles failed: %v", err)
          }
          if pCount != 0 {
              t.Errorf("residual llm_profile found after cleanup: profiles=%d", pCount)
          }
      })

      findingRes, err := d.Exec("INSERT INTO findings (name, severity, status, vulnclass, summary, evidence, worker, asset_ids, report, task_id, created_at) VALUES ('p08 archive vuln', 'low', 'confirmed', '', '', '', '', '[]', '', ?, NOW(3))", task.ID)
      if err != nil {
          t.Fatalf("insert findings fixture failed: %v", err)
      }
      findingID, err := findingRes.LastInsertId()
      if err != nil {
          t.Fatalf("findings LastInsertId failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM findings WHERE id = ?", findingID); err != nil {
              t.Errorf("cleanup findings failed: %v", err)
          }
          var fCount int
          if err := d.QueryRow("SELECT count(*) FROM findings WHERE id = ?", findingID).Scan(&fCount); err != nil {
              t.Errorf("query residual findings failed: %v", err)
          }
          if fCount != 0 {
              t.Errorf("residual finding found after cleanup: findings=%d", fCount)
          }
      })
      snapshotID := fmt.Sprintf("p08-snap-%d", ts)
      hash64 := fmt.Sprintf("%064x", ts)

      // 七表 fixture：严格仅白名单列，无 row_id、无生成列
      if _, err := d.Exec("INSERT INTO task_relations (task_id, source_task_id, created_at) VALUES (?, 0, NOW(3))", task.ID); err != nil {
          t.Fatalf("insert task_relations fixture failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO task_asset_links (task_id, asset_id, source, source_summary, source_node_id, created_at, updated_at) VALUES (?, ?, 'discovered', 'archive-source-summary', ?, NOW(3), NOW(3))", task.ID, assetID, node1); err != nil {
          t.Fatalf("insert task_asset_links fixture failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO task_llm_profiles (task_id, profile_id, position, status, last_error, exhausted_at, created_at, updated_at) VALUES (?, ?, 0, 'ready', '', NULL, NOW(3), NOW(3))", task.ID, profileID); err != nil {
          t.Fatalf("insert task_llm_profiles fixture failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO exploration_edges (exploration_id, src_id, dst_id, rel, created_at) VALUES (?, ?, ?, 'leads_to', NOW(3))", task.ExplorationID, node1, node2); err != nil {
          t.Fatalf("insert exploration_edges fixture failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO exploration_anchors (node_id, asset_id) VALUES (?, ?)", node2, assetID); err != nil {
          t.Fatalf("insert exploration_anchors fixture failed: %v", err)
      }
      if err := d.SaveSideSnapshot(ctx, sidequestion.Snapshot{Parent: sidequestion.Parent{TaskID: task.ID, ExplorationID: task.ExplorationID}, RunID: 1, Version: 1}); err != nil {
          t.Fatalf("SaveSideSnapshot fixture failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO traffic_evidence_snapshots (id, source_traffic_id, captured_at, url, method, status, content_type, req_head, resp_head, req_hash, resp_hash, req_len, resp_len, unreferenced_at, created_at) VALUES (?, ?, ?, '/api/ping', 'GET', 200, 'text/html', 'REQ-HEAD', 'RESP-HEAD', ?, ?, 3, 4, NULL, NOW(3))", snapshotID, fmt.Sprintf("p08-src-%d", ts), ts, hash64, hash64); err != nil {
          t.Fatalf("insert traffic_evidence_snapshots fixture failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM traffic_evidence_snapshots WHERE id = ?", snapshotID); err != nil {
              t.Errorf("cleanup traffic_evidence_snapshots failed: %v", err)
          }
          var snapCount int
          if err := d.QueryRow("SELECT count(*) FROM traffic_evidence_snapshots WHERE id = ?", snapshotID).Scan(&snapCount); err != nil {
              t.Errorf("query residual traffic_evidence_snapshots failed: %v", err)
          }
          if snapCount != 0 {
              t.Errorf("residual snapshot found after cleanup: snapshots=%d", snapCount)
          }
      })

      if _, err := d.Exec("INSERT INTO finding_traffic_bindings (finding_id, snapshot_id, role, note, position, created_at) VALUES (?, ?, 'primary', '', 0, NOW(3))", findingID, snapshotID); err != nil {
          t.Fatalf("insert finding_traffic_bindings fixture failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM finding_traffic_bindings WHERE finding_id = ?", findingID); err != nil {
              t.Errorf("cleanup finding_traffic_bindings failed: %v", err)
          }
          var bCount int
          if err := d.QueryRow("SELECT count(*) FROM finding_traffic_bindings WHERE finding_id = ?", findingID).Scan(&bCount); err != nil {
              t.Errorf("query residual finding_traffic_bindings failed: %v", err)
          }
          if bCount != 0 {
              t.Errorf("residual binding found after cleanup: bindings=%d", bCount)
          }
      })

      // fixture 独立逆拓扑兜底清理（先注册先保底），覆盖全部新建实体
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM finding_traffic_bindings WHERE finding_id = ?", findingID); err != nil {
              t.Errorf("cleanup finding_traffic_bindings failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM traffic_evidence_snapshots WHERE id = ?", snapshotID); err != nil {
              t.Errorf("cleanup traffic_evidence_snapshots failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM side_question_requests WHERE session_key IN (SELECT session_key FROM side_question_sessions WHERE task_id = ?)", task.ID); err != nil {
              t.Errorf("cleanup side_question_requests failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM side_question_sessions WHERE task_id = ?", task.ID); err != nil {
              t.Errorf("cleanup side_question_sessions failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM exploration_anchors WHERE node_id IN (?, ?)", node1, node2); err != nil {
              t.Errorf("cleanup exploration_anchors failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM exploration_edges WHERE exploration_id = ?", task.ExplorationID); err != nil {
              t.Errorf("cleanup exploration_edges failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM task_llm_profiles WHERE task_id = ?", task.ID); err != nil {
              t.Errorf("cleanup task_llm_profiles failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM task_relations WHERE task_id = ?", task.ID); err != nil {
              t.Errorf("cleanup task_relations failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM task_asset_links WHERE task_id = ?", task.ID); err != nil {
              t.Errorf("cleanup task_asset_links failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM findings WHERE id = ?", findingID); err != nil {
              t.Errorf("cleanup findings fixture failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM llm_profiles WHERE id = ?", profileID); err != nil {
              t.Errorf("cleanup llm_profiles fixture failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
              t.Errorf("cleanup archive asset failed: %v", err)
          }
          var linkLeft, sessLeft, snapLeft, profileLeft int
          if err := d.QueryRow("SELECT count(*) FROM task_asset_links WHERE task_id = ?", task.ID).Scan(&linkLeft); err != nil {
              t.Errorf("query residual task_asset_links failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM side_question_sessions WHERE task_id = ?", task.ID).Scan(&sessLeft); err != nil {
              t.Errorf("query residual side_question_sessions failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM traffic_evidence_snapshots WHERE id = ?", snapshotID).Scan(&snapLeft); err != nil {
              t.Errorf("query residual traffic_evidence_snapshots failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM llm_profiles WHERE id = ?", profileID).Scan(&profileLeft); err != nil {
              t.Errorf("query residual llm_profiles failed: %v", err)
          }
          if linkLeft != 0 || sessLeft != 0 || snapLeft != 0 || profileLeft != 0 {
              t.Errorf("residual seven-table fixtures found: links=%d sessions=%d snapshots=%d profiles=%d", linkLeft, sessLeft, snapLeft, profileLeft)
          }
      })

      // 导出前逐表预断言非空（防 no-op 假绿）
      probes := []struct {
          table string
          query string
          args  []any
      }{
          {"task_relations", "SELECT count(*) FROM task_relations WHERE task_id = ?", []any{task.ID}},
          {"task_asset_links", "SELECT count(*) FROM task_asset_links WHERE task_id = ?", []any{task.ID}},
          {"task_llm_profiles", "SELECT count(*) FROM task_llm_profiles WHERE task_id = ?", []any{task.ID}},
          {"exploration_edges", "SELECT count(*) FROM exploration_edges WHERE exploration_id = ?", []any{task.ExplorationID}},
          {"exploration_anchors", "SELECT count(*) FROM exploration_anchors anchor JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id = ?", []any{task.ExplorationID}},
          {"side_question_sessions", "SELECT count(*) FROM side_question_sessions WHERE task_id = ?", []any{task.ID}},
          {"traffic_evidence_snapshots", "SELECT count(*) FROM traffic_evidence_snapshots WHERE id = ?", []any{snapshotID}},
      }
      for _, probe := range probes {
          var n int
          if err := d.QueryRow(probe.query, probe.args...).Scan(&n); err != nil {
              t.Fatalf("probe fixture %s failed: %v", probe.table, err)
          }
          if n < 1 {
              t.Fatalf("fixture table %s must be non-empty before export, got %d", probe.table, n)
          }
      }

      snap, err := d.SnapshotTaskArchive(task.ID)
      if err != nil {
          t.Fatalf("SnapshotTaskArchive failed: %v", err)
      }
      if snap == nil {
          t.Fatalf("expected non-nil archive snapshot")
      }

      // Manifest 严格 14 份白名单：逐表列集合 == 白名单集合，0 个 row_id、0 个生成列
      whitelist := map[string][]string{
          "task_relations":             {"task_id", "source_task_id", "created_at"},
          "task_asset_links":           {"task_id", "asset_id", "source_node_id", "source", "source_summary", "created_at", "updated_at"},
          "task_llm_profiles":          {"task_id", "profile_id", "position", "status", "last_error", "exhausted_at", "created_at", "updated_at"},
          "exploration_edges":          {"exploration_id", "src_id", "dst_id", "rel", "created_at"},
          "exploration_anchors":        {"node_id", "asset_id"},
          "side_question_sessions":     {"session_key", "conversation_id", "task_id", "exploration_id", "intent_id", "run_id", "version", "snapshot", "generation", "memory"},
          "traffic_evidence_snapshots": {"id", "source_traffic_id", "captured_at", "url", "method", "status", "content_type", "req_head", "resp_head", "req_hash", "resp_hash", "req_len", "resp_len", "unreferenced_at", "created_at"},
      }
      for table, columns := range whitelist {
          allowed := map[string]bool{}
          for _, column := range columns {
              allowed[column] = true
          }
          raw, ok := snap.Tables[table]
          if !ok {
              t.Fatalf("manifest must contain table %s", table)
          }
          if snap.DataCounts[table] < 1 {
              t.Fatalf("manifest DataCounts[%s] must be non-zero for the seeded rows, got %d", table, snap.DataCounts[table])
          }
          var rows []map[string]any
          if err := json.Unmarshal(raw, &rows); err != nil {
              t.Fatalf("unmarshal manifest table %s failed: %v", table, err)
          }
          if len(rows) == 0 {
              t.Fatalf("manifest table %s must round-trip the seeded non-empty rows", table)
          }
          for i := range rows {
              for column := range rows[i] {
                  if !allowed[column] {
                      t.Fatalf("manifest table %s leaked non-whitelist column %q (must exclude row_id and generated columns)", table, column)
                  }
              }
              for _, column := range columns {
                  if _, present := rows[i][column]; !present {
                      t.Fatalf("manifest table %s misses whitelist column %q", table, column)
                  }
              }
          }
      }
  }
  ```

  **七表恢复与冲突 Fail-Closed 验收矩阵（`TestTaskArchive_SevenTableRestoreAndConflictFailClosed`，必须实现为可执行测试）**

  | 验收项 | fixture 与可控时序 | 通过（绿）判据 | 判红（变异实现）判据 |
  |---|---|---|---|
  | 导出与物理删除 | 复用测试② fixture → `QueueTaskArchive` + 归档 worker（`ClaimTaskArchiveJob`/`CompleteTaskArchive`）完成冷包归档 → `DeleteTaskCascade` 物理删除源任务 | `task_archives` 到达归档终态；冷包 manifest 与测试② 白名单断言一致 | 源任务未物理删除；或 manifest 含 `row_id`/生成列 |
  | 七表全量恢复 | `QueueTaskArchiveRestore(archiveID)` → 恢复 worker 调 `RestoreTaskArchive(archiveID, snap, remainingTimeoutSeconds)` 恢复至目标任务 | 目标任务 7 表行数与导出 `DataCounts` 逐表一致；`source`、`source_summary`、`source_node_id` 三列取值与导出行逐一相同 | 任一导出表缺行/行数不符；来源三列缺失或改写 |
  | 物理键重分配与逻辑关联 | 逐表比对恢复行与导出行 | 物理主键（`row_id` 等）全部由 TiDB 重新分配、不重取导出快照物理键；逻辑列（`task_id`/`exploration_id`/`asset_id`/`session_key`/`src_id`/`dst_id`）经资产/节点映射表重映射且相互关联一致 | 原样重用导出快照的物理 `row_id`；逻辑列指向旧物理键（关联断裂） |
  | 冲突 Fail-Closed | 另起恢复目标重放恢复，恢复前预置唯一键冲突：预先插入与快照 `task_asset_links` 同 `(task_id, asset_id)` 组合的冲突行（命中 `uq_task_asset_links`） | `RestoreTaskArchive` 返回非 nil 错误；恢复目标任务 7 表新增行数全部为 0，整个事务回滚、无部分写入残留 | 恢复误成功；或报错但部分表已落行（事务未整体回滚） |

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTaskArchive_ExportManifest_UnmigratedFails|TestTaskArchive_SevenTableManifestWhitelistStrict_UnmigratedFails|TestTaskArchive_SevenTableRestoreAndConflictFailClosed' -count=1)
  ```

- [ ] **Step 3: 重构 `db/task_archives.go` 与 `db/task_archives_restore.go`**
  1. 实现双白名单与冲突防护机制；`snapshotTaskArchive` 的 7 表查询从 `SELECT *` 改为显式白名单列投影（0 `row_id`、0 生成列）；
  2. 恢复导入按写入白名单列集合写行、缺省物理 `row_id`；核心图实体冲突 fail-closed，错误返回前整体事务回滚（含已恢复的部分行）；
  3. 测试② 的恢复矩阵（`QueueTaskArchiveRestore` → `RestoreTaskArchive`）作为恢复路径唯一验收入口。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTaskArchive_ExportManifest_UnmigratedFails|TestTaskArchive_SevenTableManifestWhitelistStrict_UnmigratedFails|TestTaskArchive_SevenTableRestoreAndConflictFailClosed' -count=1)
  ```

---

## Task 4.3: 辅助模块方言改造与彻底切除 pgx 依赖

### 任务定义
- **修改文件**：
  - `db/conversation.go`, `db/triggers.go`, `db/chat_mentions.go`
  - `db/skill_usage.go`, `db/side_questions.go`, `db/constraints.go`
  - `db/intercept_detail.go`, `db/intercept_execution.go`
  - `go.mod`, `go.sum`
- **新建测试**：`db/aux_modules_test.go`
- **核心逻辑与 M 项绑定（M13, M17, M20, M28, M31，彻底闭环完整会话依赖与 Retest 语义）**：
  1. 消除 8 个辅助文件中的全部 PG 方言；
  2. `conversation.go`：删除会话时显式级联清理 activities、side sessions、side requests、intercept_pending（M20）；
  3. **完整会话闭包删除与 Retest 语义契约（闭环 R51-F07-01）**：
     - **级联删除集合**：`conversation_activities`、`side_question_sessions`、`side_question_requests`（通过 session_key 级联）、`intercept_pending` 必须与 `conversations` 根行一同删除，count 必须归 0；
     - **弱关联保留与置空集合**：`finding_retests` 实体行绝对不能被删除（count 保持不变），其弱关联外键 `conversation_id` 必须被原子置空为 `NULL`；
     - **状态机停止规则**：处于 `pending` 或 `running` 状态的活跃 retest 必须转换为停止态（`status = 'cancelled'` 或 `'stopped'`），而处于 `completed` 等终态的 retest 状态必须原样保留，绝对禁止被篡改；
  4. `side_questions.go`：锁内元组比较防版本倒退（M28）；
  5. 彻底从 `go.mod` 移除 `github.com/jackc/pgx/v5`。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写辅助模块方言与完整会话闭包级联清理测试（`db/aux_modules_test.go`）**
  ```go
  package db

  import (
      "context"
      "database/sql"
      "errors"
      "fmt"
      "os"
      "testing"
      "time"

      "github.com/Autumn-27/artex/db/schema"
      "github.com/Autumn-27/artex/sidequestion"
  )

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
      // 另行创建无会话绑定的已完成 retest (conversation_id IS NULL, status='completed') 验证 finding 实体存活与历史重测数据不受跨会话级联影响。
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
      if _, err := d.Exec("INSERT INTO side_question_requests (id, session_key, generation, client_id, identity, identity_hash, question, answer, status, error, model, usage, context_info, snapshot_at, created_at) VALUES (?, ?, 1, 'test_client', ?, ?, 'test question', '', 'running', '', '{}', '{}', '{}', NOW(3), NOW(3))", reqID, convKey, reqIdent, reqHash[:]); err != nil {
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

      // 9. 失败判据断言②：四类级联依赖（activities, side_sessions, side_requests, intercept_pending）全部归零
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
  ```

  **会话闭包分态分类验收矩阵（`TestConversation_DeleteConversation_FullClosureAndRetestSemantics` 与独立分态测试）**

  | 验收用例 / 会话挂载场景 | fixture 与状态 | 期望结果（绿）判据 | 判红（变异实现）判据 |
  |---|---|---|---|
  | 会话 A（活跃 pending） | 关联 finding 且绑定 convA，`status = 'pending'` | convA 删除后：根行及子实体全删；该 retest 实体保留，`conversation_id` 置 NULL，`status` 确定性转换为停止态（`'stopped'` 或 `'cancelled'`） | retest 实体被误删；`conversation_id` 未置 NULL；状态仍保持 `'pending'` |
  | 会话 B（活跃 running） | 关联独立 finding 且绑定 convB，`status = 'running'` | convB 删除后：根行及子实体全删；该 retest 实体保留，`conversation_id` 置 NULL，`status` 确定性转换为停止态（`'stopped'` 或 `'cancelled'`） | 漏转停止态；状态仍保持 `'running'` 或被误删 |
  | 会话 C（已完成 completed） | 关联独立 finding 且绑定 convC，`status = 'completed'` | convC 删除后：根行及子实体全删；该 retest 实体保留，`conversation_id` 置 NULL，`status` **绝对保持 `'completed'` 不变** | 错误写成无条件置空并取消（`status` 被误改为 `'stopped'`/`'cancelled'`）；或实体被误删 |
  | 非会话历史对照 | `conversation_id IS NULL`，`status = 'completed'` | 任何会话删除均不影响该行：行保留且状态依然为 `'completed'` | 跨会话级联误伤未绑定的历史 retest 行 |

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestConversation_DeleteConversation_FullClosureAndRetestSemantics' -count=1)
  ```

- [ ] **Step 3: 改造 8 个辅助业务模块并执行 `go mod tidy`**
  1. 改写 8 个业务文件为纯 TiDB 语法；
  2. 实现 `DeleteConversation` 完整闭包删除与 retest 停止/置空逻辑；
  3. 清除全仓代码中的 `github.com/jackc/pgx/v5` 引用；
  4. 运行 `go mod tidy`，验证 `go.mod` 中无 `pgx`。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestConversation_DeleteConversation_FullClosureAndRetestSemantics' -count=1)
  ```

---

## 阶段 4 验收门禁

```bash
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./db/... -run 'TestNotification_|TestTaskArchive_|TestConversation_' -count=1)
```
