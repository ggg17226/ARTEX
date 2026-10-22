# P08-02 实施计划：核心业务主干（资产、任务与级联闭包删除）

---

## 实施状态：已完成（2026-10-22）

> **本计划的代码实现与测试已全部落地，最终验收见
> [P08-05 实施完成记录](2026-10-18-P08-05-w5-final-regression.md#实施完成记录2026-10-22-终态核验)。**
>
> 下方的 `- [ ]` 复选框**保留为 TDD 执行流程的历史记录**，不做批量回勾，原因：
> 1. `Step 2: 运行测试见红（FAIL）` 是当时的过程状态，事后无法回溯验证，强行勾选等于伪造证据；
> 2. `Step 5: 提交（commit）` 未获授权执行，工作区改动仍未提交，不适用。
>
> 可以核验的客观事实：本计划引用的实现文件与测试文件**全部存在**；`go build ./...`/`go vet ./...` 退出码 0；
> 全仓离线测试与真实 TiDB 8.5 业务库全量回归均通过（2163 PASS / 0 FAIL）；全仓非注释行 PG 方言残留为 **0**。


> **计划编号**：`P08-02`  
> **所属阶段**：阶段 2（核心业务主干：资产、任务与级联闭包）  
> **关联 Spec**：`docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md` §3.2  
> **目标**：重构资产 8 分支规范化写入与写前仲裁 `schema.VerifyHashArbitration`；完成企业、范围、分类与模板的 TiDB 化；废除原 PG 表锁，落地全体写者 Runner Gate 与父锁协议；接线 `cascade_executor.go` 与 `assets.task_ids`/`task_asset_links` 双写同步，实现闭包级联删除与成员漂移回滚，锁闭全部跨根物理副作用。

---

## 涵盖的产品业务文件清单（11 个产品业务文件，精准映射）

| 文件路径 | R4 Blocker 数 | 核心职责与 M 项 |
|---|---:|---|
| `db/assets.go` | 28 | 8 分支规范化、物理预锁、写前仲裁与身份不可变更新（**M05, M10, M34**） |
| `db/asset_dsl.go` | 19 | 资产 DSL 动态查询方言重构（**M05**） |
| `db/companies.go` | 23 | 企业 CRUD、`nkey_hash` 写入与去重窗口函数化（**M09, M19, M35**） |
| `db/tasks.go` | 28 | 废除 PG 锁表，接线 `cascade_executor.go`，支持 `DeleteTaskOptions`（**M18, M27, M36**） |
| `db/task_assets.go` | 11 | `assets.task_ids` 与 `task_asset_links` 事务内原子双写同步（**M26, M30**） |
| `db/task_assets_context.go` | 9 | 任务资产上下文绑定与方言重构（**M26**） |
| `db/task_scope.go` | 10 | 任务范围 CRUD 与 `identity_hash` 维护（**M16**） |
| `db/task_context.go` | 18 | 任务上下文查询与更新方言重构（**M27**） |
| `db/task_categories.go` | 12 | 任务分类 CRUD、`nkey_hash` 写入与 1062 映射（**M03, M14, M24, M32**） |
| `db/task_templates.go` | 4 | 任务模板 CRUD、`nkey_hash` 写入（**M04, M15**） |
| `db/task_intercept.go` | 6 | 任务级拦截规则 CRUD 与方言重构（**M27**） |
| **小计** | **168** | **覆盖阶段 2 全部 168 处 Blocker 与核心 M 项** |

---

## Task 2.1: 资产表 8 分支规范化写入与写前仲裁协议

### 任务定义
- **修改文件**：`db/assets.go`, `db/asset_dsl.go`
- **新建/修改测试**：`db/assets_test.go`
- **核心逻辑与 M 项绑定（M05, M10, M34，闭环 F02）**：
  1. **输入谓词规范化**：
     - `root_domain`：`DomainKey` 规范化，调用 `schema.AssetsRootDomainIdentity`；
     - `ip`：规范化 Canonical IP 文本，调用 `schema.AssetsIPIdentity`；
     - `subdomain`：调用 `schema.AssetsSubdomainIdentity`；
     - `app_bundle` 与 `app_name`：互斥激活；
     - `service_http`：`NormalizeURL` 后调用 `schema.AssetsServiceHTTPIdentity`；
     - `service_other`：六参数调用 `schema.AssetsServiceOtherIdentity`，包含 domain/ip COALESCE 与 port/serviceName 处理；
     - `endpoint`：URL 与大写 METHOD。
  2. **写前仲裁与身份不可变更新两阶段协议**（严格对齐 Spec §3.2.1）：
     - **候选存在（Path A）**：
       1. 按候选 PK 预锁：`SELECT id FROM assets WHERE id = ? FOR UPDATE`；
       2. **写前仲裁**：在持锁保护下，必须先调用 `schema.VerifyHashArbitration` 核验持久化 identity 与输入完全一致且返回 PK 等于候选 PK；若碰撞立即 `tx.Rollback()` 退出；
       3. **身份不可变更新**：仲裁通过后执行 `UPDATE assets SET ... WHERE id = ?`，**严禁修改 `identity_*`、`hash_*` 及分支自然键列**，仅允许更新 `last_seen`、`technologies` 等非身份业务字段；
     - **无候选（Path B）**：直接执行严格 `INSERT`（不带 ODKU）；若遇到 MySQL 1062 并发赢家冲突，立即回滚并转入 Path A（查询新赢家 PK 并预锁重演写前仲裁与更新）；
     - 若 `VerifyHashArbitration` 返回 `ErrIdentityHashCollision`，显式 `tx.Rollback()`。
  3. **F02 可执行失败判据（R51-F07-02 整改项，必须由 Step 1 测试落地）**：
     - **Path A 完整链路判据**：现存行写入必须先按物理 PK 执行 `SELECT id FROM assets WHERE id = ? FOR UPDATE` 行级悲观锁，再在持锁保护下调用 `schema.VerifyHashArbitration` 写前仲裁，最后仅更新 `last_seen`、`technologies`、`company_id` 等非身份业务字段；**严禁篡改 `identity_*`/`hash_*`（canonical 身份列，nkey identity 语义）或任一分支自然键列**，现存行更新路径改写身份/自然键的实现判红；
     - **强制 Hash 碰撞失败判据**：注入“相同 hash 但不同 identity”的碰撞行（保持 `hash_root_domain` 指向输入的 canonical hash，将 `identity_root_domain` 改写为另一域名的 canonical identity），随后同输入 upsert 的写前仲裁**必须返回 `schema.ErrIdentityHashCollision` 且事务立即回滚，验零回读确认 0 写入**（持久化 identity 未被覆盖、未新增任何行）；**任何“先覆盖后仲裁”（先覆盖旧身份再自证）的实现必须判红**；
     - **Path B 1062 失败判据**：Path B 必须为严格 `INSERT`（无 ODKU）；MySQL 1062 冲突必须**本轮尝试整体回滚**，并在事务运行器重试循环的**新事务**中转 Path A 重演写前仲裁与更新；**任何采用 ODKU 覆盖写、未回滚直接原地重试、或将 1062 透传调用方的实现必须判红**；
     - **八分支激活判据**：分支激活谓词（规范化 `root_domain`、Canonical IP、`app_bundle`/`app_name` 互斥、URL 规范化、`port` 1..65535、`method` 大写）须配套“仅激活对应分支 identity/hash 列，其余 7 分支必须为 NULL”的列激活断言；
     - **测试基建**：Task 2.1 须在 `db/schema` 提供 8 分支 canonical identity builder（如 `schema.AssetsRootDomainIdentity(domain string) ([]byte, [32]byte)`，复用 `CanonicalIdentity`/`CanonicalHash`），供写路径与碰撞注入测试调用。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写资产 8 分支写入与冲突仲裁测试（`db/assets_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestAssets_RootDomainUpsert_VerifyBeforeUpdate(t *testing.T) {
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
      
      domain := fmt.Sprintf("p08-%d.example.com", time.Now().UnixNano())
      id, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
      if err != nil {
          t.Fatalf("UpsertRootDomain failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", id); err != nil {
              t.Errorf("cleanup asset failed: %v", err)
          }
      })

      if id <= 0 {
          t.Fatalf("expected positive asset ID, got %d", id)
      }
  }
  ```

  测试① 仅覆盖首次插入与正 ID，**不足以单独通过 F02**；Step 1 必须同时具备②③④ 三组可执行测试（“先覆盖后仲裁”“ODKU 覆盖写”“改写身份列”等变异实现必须至少被一条测试判红）：

  **② Path A 现存行更新：身份不可变与严格分支激活（`TestAssets_PathAUpdate_IdentityImmutable`）**

  ```go
  package db

  import (
      "bytes"
      "fmt"
      "os"
      "strings"
      "testing"
      "time"
  )

  func TestAssets_PathAUpdate_IdentityImmutable(t *testing.T) {
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

      domain := fmt.Sprintf("p08-patha-%d.example.com", time.Now().UnixNano())
      id1, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
      if err != nil {
          t.Fatalf("first UpsertRootDomain failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", id1); err != nil {
              t.Errorf("cleanup asset failed: %v", err)
          }
          var rowLeft, domainLeft int
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", id1).Scan(&rowLeft); err != nil {
              t.Errorf("query residual asset failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE root_domain = ?", domain).Scan(&domainLeft); err != nil {
              t.Errorf("query residual domain rows failed: %v", err)
          }
          if rowLeft != 0 || domainLeft != 0 {
              t.Errorf("residual Path A fixtures found: rows=%d domainRows=%d", rowLeft, domainLeft)
          }
      })

      var root1 string
      var identity1, hash1 []byte
      if err := d.QueryRow("SELECT root_domain, identity_root_domain, hash_root_domain FROM assets WHERE id = ?", id1).Scan(&root1, &identity1, &hash1); err != nil {
          t.Fatalf("query identity snapshot failed: %v", err)
      }

      // 大写输入经 DomainKey 规范化后命中同一 canonical 身份，必须走 Path A 并复用同一物理 PK
      id2, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: strings.ToUpper(domain)})
      if err != nil {
          t.Fatalf("second UpsertRootDomain (Path A) failed: %v", err)
      }
      if id2 != id1 {
          t.Fatalf("Path A must reuse physical row: want id %d, got %d", id1, id2)
      }

      var root2 string
      var identity2, hash2 []byte
      if err := d.QueryRow("SELECT root_domain, identity_root_domain, hash_root_domain FROM assets WHERE id = ?", id2).Scan(&root2, &identity2, &hash2); err != nil {
          t.Fatalf("query identity snapshot after Path A failed: %v", err)
      }
      if root2 != root1 || !bytes.Equal(identity1, identity2) || !bytes.Equal(hash1, hash2) {
          t.Fatalf("Path A update mutated identity columns: root %q -> %q, identityChanged=%v hashChanged=%v",
              root1, root2, !bytes.Equal(identity1, identity2), !bytes.Equal(hash1, hash2))
      }

      var branchRows int
      if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ? AND (identity_ip IS NOT NULL OR hash_ip IS NOT NULL OR identity_subdomain IS NOT NULL OR hash_subdomain IS NOT NULL OR identity_app_bundle IS NOT NULL OR hash_app_bundle IS NOT NULL OR identity_app_name IS NOT NULL OR hash_app_name IS NOT NULL OR identity_service_http IS NOT NULL OR hash_service_http IS NOT NULL OR identity_service_other IS NOT NULL OR hash_service_other IS NOT NULL OR identity_endpoint IS NOT NULL OR hash_endpoint IS NOT NULL)", id1).Scan(&branchRows); err != nil {
          t.Fatalf("query strict branch activation failed: %v", err)
      }
      if branchRows != 0 {
          t.Fatalf("expected only root_domain branch activated on asset %d, other 7 branches must stay NULL", id1)
      }
  }
  ```

  **③ 强制 Hash 碰撞：写前仲裁必须 `ErrIdentityHashCollision` 且零写入（`TestAssets_HashCollision_RejectedWithZeroWrite`）**

  ```go
  package db

  import (
      "errors"
      "fmt"
      "os"
      "testing"
      "time"

      "github.com/Autumn-27/artex/db/schema"
  )

  func TestAssets_HashCollision_RejectedWithZeroWrite(t *testing.T) {
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

      domain := fmt.Sprintf("p08-coll-%d.example.com", time.Now().UnixNano())
      id, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
      if err != nil {
          t.Fatalf("seed UpsertRootDomain failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", id); err != nil {
              t.Errorf("cleanup collision fixture failed: %v", err)
          }
          var rowLeft, domainLeft int
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", id).Scan(&rowLeft); err != nil {
              t.Errorf("query residual collision asset failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE root_domain = ?", domain).Scan(&domainLeft); err != nil {
              t.Errorf("query residual collision domain rows failed: %v", err)
          }
          if rowLeft != 0 || domainLeft != 0 {
              t.Errorf("residual collision fixtures found: rows=%d domainRows=%d", rowLeft, domainLeft)
          }
      })

      // 碰撞注入：保持行的 hash_root_domain 指向输入 domain 的 canonical hash，
      // 而将 identity_root_domain 改写为另一域名的 canonical identity，
      // 构造“hash 命中但完整 identity 字节不相等”的物理前提。
      foreign := fmt.Sprintf("p08-foreign-%d.example.com", time.Now().UnixNano())
      _, inputHash := schema.AssetsRootDomainIdentity(domain)
      foreignIdentity, _ := schema.AssetsRootDomainIdentity(foreign)
      if _, err := d.Exec("UPDATE assets SET identity_root_domain = ?, hash_root_domain = ? WHERE id = ?", foreignIdentity, inputHash[:], id); err != nil {
          t.Fatalf("inject forced hash collision failed: %v", err)
      }

      // 写前仲裁必须拒绝：返回 schema.ErrIdentityHashCollision 是唯一合格结局；
      // “先覆盖后仲裁”的实现（返回 nil、覆盖持久化 identity 或写入新行）必须在此判红。
      if _, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain}); !errors.Is(err, schema.ErrIdentityHashCollision) {
          t.Fatalf("expected schema.ErrIdentityHashCollision from write-ahead arbitration, got %v", err)
      }

      // 验零回读（0 写入）：持久化 identity 未被覆盖，碰撞 hash 与输入域名仍各只命中原始 1 行。
      var persisted []byte
      if err := d.QueryRow("SELECT identity_root_domain FROM assets WHERE id = ?", id).Scan(&persisted); err != nil {
          t.Fatalf("query persisted identity after collision failed: %v", err)
      }
      if string(persisted) != string(foreignIdentity) {
          t.Fatalf("collision path overwrote persisted identity: write-before-overwrite is forbidden")
      }
      var hashRows, domainRows int
      if err := d.QueryRow("SELECT count(*) FROM assets WHERE hash_root_domain = ?", inputHash[:]).Scan(&hashRows); err != nil {
          t.Fatalf("query collision hash rows failed: %v", err)
      }
      if err := d.QueryRow("SELECT count(*) FROM assets WHERE root_domain = ?", domain).Scan(&domainRows); err != nil {
          t.Fatalf("query collision domain rows failed: %v", err)
      }
      if hashRows != 1 || domainRows != 1 {
          t.Fatalf("collision path must write zero rows: hashRows=%d domainRows=%d (want 1 and 1, both pre-existing)", hashRows, domainRows)
      }
  }
  ```

  **④ Path B 并发 1062：本轮整体回滚并在新事务转 Path A 重试（`TestAssets_PathB_ConcurrentSameDomain_1062PathARetry`）**

  ```go
  package db

  import (
      "fmt"
      "os"
      "sync"
      "testing"
      "time"
  )

  func TestAssets_PathB_ConcurrentSameDomain_1062PathARetry(t *testing.T) {
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

      domain := fmt.Sprintf("p08-race-%d.example.com", time.Now().UnixNano())
      ids := []int64{0, 0}
      errs := []error{nil, nil}
      startGate := make(chan struct{})
      var wg sync.WaitGroup
      for i := 0; i < 2; i++ {
          wg.Add(1)
          go func(i int) {
              defer wg.Done()
              <-startGate
              ids[i], errs[i] = d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
          }(i)
      }
      close(startGate)
      wg.Wait()

      // 竞争结果登记后第一时间注册独立兜底清理（无论仲裁成败都不留残留行），再断言竞争语义。
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE root_domain = ?", domain); err != nil {
              t.Errorf("cleanup raced assets failed: %v", err)
          }
          var domainLeft, rowLeft int
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE root_domain = ?", domain).Scan(&domainLeft); err != nil {
              t.Errorf("query residual raced domain rows failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", ids[0]).Scan(&rowLeft); err != nil {
              t.Errorf("query residual raced asset row failed: %v", err)
          }
          if domainLeft != 0 || rowLeft != 0 {
              t.Errorf("residual raced fixtures found: domainRows=%d idRows=%d", domainLeft, rowLeft)
          }
      })

      if errs[0] != nil || errs[1] != nil {
          t.Fatalf("1062 conflict must rollback the losing attempt fully and retry via Path A in a fresh transaction, got errs %v / %v", errs[0], errs[1])
      }
      if ids[0] != ids[1] || ids[0] <= 0 {
          t.Fatalf("both racing upserts must resolve to the same positive physical id, got %d and %d", ids[0], ids[1])
      }

      var rowCount int
      if err := d.QueryRow("SELECT count(*) FROM assets WHERE root_domain = ?", domain).Scan(&rowCount); err != nil {
          t.Fatalf("query raced row count failed: %v", err)
      }
      if rowCount != 1 {
          t.Fatalf("exactly one asset row must survive the 1062 race, got %d (duplicate rows prove missing full rollback)", rowCount)
      }
  }
  ```

  **Path B 并发冲突与事务重试可观察验收矩阵（`TestAssets_PathB_ConcurrentSameDomain_1062PathARetry` 必须满足）**

  | 验收项 | 受控时序与注入方式 | 期望通过（绿）轨迹 | 判红（变异实现）判据 |
  |---|---|---|---|
  | 确定性 1062 碰撞触发 | 候选查询快照确认无候选行后通过测试钩子/屏障挂起；由并发连接插入并成功提交相同自然键与 canonical 身份行；恢复被挂起连接执行严格 `INSERT` | 必须收到并捕获 MySQL 1062 唯一索引冲突错误；严禁使用 ODKU 绕过冲突 | 采用 `ON DUPLICATE KEY UPDATE` 原地更新返回已有 ID 判红；错误透传给外层调用方判红 |
  | 第一轮事务完整 Rollback | 1062 触发后，外层事务运行器必须对该失败尝试（Tx1）执行完整 `Rollback` | Tx1 立即整体回滚，不遗留任何未完成的锁或部分脏数据；记录 `Tx1.RollbackComplete` | 在同一事务 Tx1 内捕获 1062 后直接执行 SELECT/UPDATE 伪重试判红；漏调用 Rollback 判红 |
  | 全新事务重新开始 (Tx2 != Tx1) | Tx1 回滚完成后，事务运行器以新事务凭证（全新 TxID / 连接）开启全新重试周期（Tx2） | 本次 Upsert 调用的实际重试次数增量恰好等于 1（`txRetryCount == 1`）；Tx2 与 Tx1 为完全独立事务 | 重试计数未递增；或在原事务对象内重放判红 |
  | 新事务确定性转入 Path A 重演 | 在全新事务 Tx2 中，再次执行候选查询，此时候选物理行已可见（`candidatePK > 0`） | 必须按规范执行 Path A：`SELECT id FROM assets WHERE id = ? FOR UPDATE` 行锁 → `schema.VerifyHashArbitration` 写前仲裁 → 仅更新非身份业务列 → `Tx2.Commit` | 依然尝试 Path B 插入；或未获取行锁更新；或先更新后仲裁；或篡改 `identity_*`/`hash_*` 判红 |
  | 最终状态与唯一性保真 | 重试提交后，两并发调用返回结果及数据终态比对 | 两并发调用均无错误返回，返回相同正物理 PK；表中存在唯一的物理行；回读该行的 `identity_*` 与输入 Canonical Identity 100% 字节一致 | 返回 ID 不一致；产生重复行；身份列发生漂移 |

  **F02 判红口径汇总**：② 在现存行更新路径篡改 `identity_*`/`hash_*` 或分支自然键、或激活非本分支的 identity/hash 列即判红；③ 只要“先覆盖后仲裁”（碰撞行 identity 被覆盖、或碰撞后新增行、或返回错误不是 `schema.ErrIdentityHashCollision`）即判红；④ 采用 ODKU 覆盖写、未回滚当前尝试原地重试、或将 1062 透传调用方（任一 goroutine 报错）或产生重复物理行即判红。

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestAssets_RootDomainUpsert_VerifyBeforeUpdate|TestAssets_PathAUpdate_IdentityImmutable|TestAssets_HashCollision_RejectedWithZeroWrite|TestAssets_PathB_ConcurrentSameDomain_1062PathARetry' -count=1)
  ```

- [ ] **Step 3: 重构 `db/assets.go` 与 `db/asset_dsl.go`**
  1. 重构 7 个方法（8 个分支）的 SQL 方言；
  2. 消除所有的 `$1` 与 `RETURNING id`；
  3. 落实写前仲裁与身份不可变更新。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestAssets_RootDomainUpsert_VerifyBeforeUpdate|TestAssets_PathAUpdate_IdentityImmutable|TestAssets_HashCollision_RejectedWithZeroWrite|TestAssets_PathB_ConcurrentSameDomain_1062PathARetry' -count=1)
  ```

---

## Task 2.2: 企业与范围模型改造及真实窗口函数去重

### 任务定义
- **修改文件**：`db/companies.go`, `db/company_scope.go`
- **新建测试**：`db/companies_test.go`
- **核心逻辑与 M 项绑定（M09, M19, M35）**：
  1. 企业写入计算 `nkey_identity` 与 `nkey_hash`；
  2. `company_scope` 按 `kind`（`domain`, `net`, `value`）计算对应的 identity 与 hash；
  3. 资产自动归属企业查询：消除 `DISTINCT ON`，改写为合法派生表窗口函数：
     ```sql
     SELECT asset_id, company_id FROM (
         SELECT a.id AS asset_id, cs.company_id,
                ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY LENGTH(cs.domain) DESC, cs.company_id ASC) AS rn
         FROM assets a JOIN company_scope cs ON cs.kind = 'domain' AND a.root_domain = cs.domain
         WHERE a.company_id IS NULL AND a.type IN ('root_domain','subdomain','service','endpoint')
     ) t WHERE t.rn = 1
     ```
  4. 企业删除：显式清理关联范围并置空资产与任务的企业外键（M19）。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写企业与范围归属窗口函数测试（`db/companies_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestCompanies_RecomputeAttribution_UnmigratedDialect(t *testing.T) {
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

      cName := fmt.Sprintf("P08 Corp %d", time.Now().UnixNano())
      cid, created, err := d.Companies().UpsertCompany(cName, "")
      if err != nil {
          t.Fatalf("UpsertCompany failed: %v", err)
      }
      if !created || cid <= 0 {
          t.Fatalf("expected newly created company with positive ID, got (%v, %d)", created, cid)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM company_scope WHERE company_id = ?", cid); err != nil {
              t.Errorf("cleanup target company_scope failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM companies WHERE id = ?", cid); err != nil {
              t.Errorf("cleanup target company failed: %v", err)
          }
      })

      competitorName := fmt.Sprintf("P08 Competitor %d", time.Now().UnixNano())
      compCID, compCreated, err := d.Companies().UpsertCompany(competitorName, "")
      if err != nil {
          t.Fatalf("UpsertCompany competitor failed: %v", err)
      }
      if !compCreated || compCID <= 0 {
          t.Fatalf("expected newly created competitor company, got (%v, %d)", compCreated, compCID)
      }
      // 竞争公司校验成功后立即登记独立清理，再断言排序前提：
      // 避免 cid >= compCID 前置断言 Fatal 时，已成功创建的 compCID 缺失 Cleanup 造成 fixture 泄露。
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM company_scope WHERE company_id = ?", compCID); err != nil {
              t.Errorf("cleanup competitor company_scope failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM companies WHERE id = ?", compCID); err != nil {
              t.Errorf("cleanup competitor company failed: %v", err)
          }
      })
      if cid >= compCID {
          t.Fatalf("expected cid (%d) < compCID (%d) for deterministic tie-breaker precondition", cid, compCID)
      }

      rootBase := fmt.Sprintf("attr-%d.com", time.Now().UnixNano())
      subDomain := "sub." + rootBase
      assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: subDomain})
      if err != nil {
          t.Fatalf("UpsertRootDomain failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
              t.Errorf("cleanup asset failed: %v", err)
          }
      })

      // 竞争配置（根据 R4 现有精确匹配合约）：两家公司均添加了完全相同的 domain scope，但 cid < compCID；
      // 在 a.root_domain = cs.domain 精确匹配语义下，两家公司同时成为有效候选；
      // 根据 SQL 中 ORDER BY cs.company_id ASC tie-breaker 规则，较小 ID 的 cid 必须确定性胜出并赢得归属（已诚实对齐 tie-breaker 合同）
      added, skipped, invalid, errs := d.Companies().AddScope(cid, []string{subDomain}, "test target")
      if len(errs) > 0 || added != 1 || skipped != 0 || invalid != 0 {
          t.Fatalf("AddScope specific failed: added=%d, skipped=%d, invalid=%d, errs=%v", added, skipped, invalid, errs)
      }
      cAdded, cSkipped, cInvalid, cErrs := d.Companies().AddScope(compCID, []string{subDomain}, "test competitor")
      if len(cErrs) > 0 || cAdded != 1 || cSkipped != 0 || cInvalid != 0 {
          t.Fatalf("AddScope generic competitor failed: added=%d, skipped=%d, invalid=%d, errs=%v", cAdded, cSkipped, cInvalid, cErrs)
      }

      // 制造待修复状态：显式将该资产的 company_id 置空，确保测试严格验证 RecomputeAttribution 本身的重算与竞争裁决能力
      if _, err := d.Exec("UPDATE assets SET company_id = NULL WHERE id = ?", assetID); err != nil {
          t.Fatalf("reset asset company_id failed: %v", err)
      }

      err = d.Companies().RecomputeAttribution()
      if err != nil {
          t.Fatalf("RecomputeAttribution failed: %v", err)
      }

      var attributedID *int64
      if err := d.QueryRow("SELECT company_id FROM assets WHERE id = ?", assetID).Scan(&attributedID); err != nil {
          t.Fatalf("query attributed company_id failed: %v", err)
      }
      // 必须由 ID 更小的目标公司获胜，证明在相同范围候选存在时，窗口函数按 cs.company_id ASC tie-break 规则生效，杜绝倒退
      if attributedID == nil || *attributedID != cid {
          t.Fatalf("expected asset %d to be attributed to target company %d via tie-break (not competitor %d), got %v", assetID, cid, compCID, attributedID)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestCompanies_RecomputeAttribution_UnmigratedDialect' -count=1)
  ```

- [ ] **Step 3: 改造 `db/companies.go` 与 `db/company_scope.go`**
  消除 23 处方言，实现窗口函数归属与企业删除级联置空。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestCompanies_RecomputeAttribution_UnmigratedDialect' -count=1)
  ```

---

## Task 2.3: 任务状态机、全体写者 Runner Gate 与全物理副作用级联闭包删除接线

### 任务定义
- **修改文件**：`db/tasks.go`, `db/task_assets.go`, `db/task_assets_context.go`, `db/task_scope.go`, `db/task_context.go`, `db/task_categories.go`, `db/task_templates.go`, `db/task_intercept.go`, `db/task_closure.go`, `db/cascade_executor.go`
- **新建测试**：`db/task_cascade_test.go`
- **核心逻辑与 M 项绑定（M03, M04, M14, M15, M16, M18, M24, M26, M27, M30, M32, M36，闭环 F03）**：
  1. 废除原 PG 表锁 `LOCK TABLE tasks`；
  2. **写者协议**：使用 `TxRunner.Run` 配置 `RequiresBusinessGate: true`，在事务内先锁住父任务行，核验其处于活动状态；
  3. **级联删除闭包四阶段与全物理副作用锁定**：
     - Fresh Locator：在 Gate 保护下调用 `ComputeTaskClosure`；
     - **完整物理计划映射**：
       - `side_question_sessions` 查询转换为物理 `row_id`；
       - `AssetIDsToDetach` 识别出共享资产并从物理删除列表中剥离；
       - **跨根物理副作用闭包（闭环 F03）**：在闭包收集阶段，必须将即将被 `executor` 置空的跨根弱关联物理行（如其他 exploration 的 `activity.id`，其他任务的 `task_asset_links.row_id`）完整枚举并纳入锁计划；
       - `cascade_executor.go` 改造：严格限制 UPDATE/DELETE 仅作用于已锁定的 PK 集合，杜绝开放范围 DML 突破已锁集合；
     - 全序悲观锁：将所有实体按 `(TableOrder, PK 升序)` 排序，调用 `lockorder.LockEntitiesInOrder` 加锁；
     - 成员漂移复核：若发现新成员或更低 PK，抛出 `ErrMembershipDrift` 由 Runner 整体回滚重试；
     - 逆拓扑删除：接入 `db/cascade_executor.go` 物理逆拓扑清理；
  4. **双写维护**：`assets.task_ids` 与 `task_asset_links` 同步原子更新；
  5. **删除选项分层与 unknown 保护**：
     - 事务内完成 `DeleteAssets`、`DeleteFindings`、`DeleteLLMRecords`；
     - 提交后由 manager 执行 `DeleteTraffic`、`DeleteFiles`（支持 staging 补偿）；
     - 若提交返回 `lockorder.ErrOutcomeUnknown`，挂起进入人工对账，绝不执行外部补偿。
  6. **F03 可执行失败判据（R51-F07-02 整改项，必须由 Step 1 测试落地）**：
     - **双根共享资产 + 跨根弱引用纳锁判据**：fixture 必须构造两任务（taskA/taskB）共享同一资产（`task_asset_links` 各一行、`assets.task_ids` 同时含两任务 ID），并构造跨根弱引用（taskB exploration 下的 `activity.id` 弱引用 taskA 闭包内节点、taskB 的 `task_asset_links.row_id` 弱引用共享资产）；`ComputeTaskClosure` 必须将这些跨根弱引用物理 PK（`activity.id`、`task_asset_links.row_id`）完整收集进锁计划，并按 `(TableOrder, PK ASC)` 全局全序调用 `lockorder.LockEntitiesInOrder` 悲观锁定；
     - **写集收敛判据（开放范围 DML 判红）**：`cascade_executor` 的全部 UPDATE/DELETE 必须收敛于已锁物理 PK 集合（按表 Catalog 物理 PK 列限定，如 `WHERE id IN (?)`、`WHERE row_id IN (?)`、`WHERE ordinal IN (?)`，绑定完整物理 PK 列表）；出现任何不带已锁 PK 集合限定的开放范围 DML（如 `UPDATE activity SET node_id = NULL WHERE node_id IN (...)`、`DELETE FROM task_asset_links WHERE task_id = ? AND asset_id = ?`）即判红；数据侧判据：删除 taskA 后 taskB 的 `task_asset_links` 行与跨根 activity 行必须**原样存活**（开放范围 DML 越界删/清空即判红）；
     - **共享实体存活判据**：删除 taskA 后共享资产行必须存活（因 taskB 引用仅允许 detach，不得物理删除）、taskB 根/exploration/关联行全部存活，且 `assets.task_ids` 原子剥离 taskA 并保留 taskB（双写一致）；
     - **成员漂移注入判据**：在闭包快照与持锁成员复核之间，经第二连接注入一个更低 PK 的新成员行（如 `task_asset_links` 行），本轮尝试必须返回包裹 `lockorder.ErrMembershipDrift` 的错误，`TxRunner` **整体回滚 + 指数退避整体重试**，最终一次尝试成功完成删除；**逆序补锁（发现新成员后不整体回滚、按插入顺序在原事务内追加补锁）的实现必须判红**。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写任务级联删除与写者父锁测试（`db/task_cascade_test.go`）**
  ```go
  package db

  import (
      "database/sql"
      "errors"
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestTask_DeleteTaskCascade_UnmigratedDialect(t *testing.T) {
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
      
      tName := fmt.Sprintf("P08 Cascade Task %d", time.Now().UnixNano())
      task, err := d.CreateTaskWithOptions(tName, "goal", TaskCreateOptions{})
      if err != nil {
          t.Fatalf("CreateTaskWithOptions failed: %v", err)
      }
      t.Cleanup(func() {
          var exists bool
          queryErr := d.QueryRow("SELECT true FROM tasks WHERE id = ?", task.ID).Scan(&exists)
          if queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
              t.Errorf("query task residual state failed: %v", queryErr)
          } else if queryErr == nil && exists {
              if _, err := d.DeleteTaskCascade(task.ID, false, false); err != nil {
                  t.Errorf("cleanup residual task failed: %v", err)
              }
          }
          // 独立逆拓扑兜底清理关联 exploration_nodes、explorations 以及 tasks 根，彻底杜绝任何孤儿与残存
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
      
      _, err = d.DeleteTaskCascade(task.ID, false, false)
      if err != nil {
          t.Fatalf("DeleteTaskCascade failed: %v", err)
      }

      var exists bool
      queryErr := d.QueryRow("SELECT true FROM tasks WHERE id = ?", task.ID).Scan(&exists)
      if queryErr == nil {
          t.Fatalf("task %d still exists after cascade deletion", task.ID)
      } else if !errors.Is(queryErr, sql.ErrNoRows) {
          t.Fatalf("query task existence failed: %v", queryErr)
      }

      var expExists bool
      expErr := d.QueryRow("SELECT true FROM explorations WHERE id = ?", task.ExplorationID).Scan(&expExists)
      if expErr == nil {
          t.Fatalf("exploration %d still exists after cascade deletion", task.ExplorationID)
      } else if !errors.Is(expErr, sql.ErrNoRows) {
          t.Fatalf("query exploration existence failed: %v", expErr)
      }

      var nodeCount int
      if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", task.ExplorationID).Scan(&nodeCount); err != nil {
          t.Fatalf("query exploration_nodes count failed: %v", err)
      }
      if nodeCount != 0 {
          t.Fatalf("expected 0 residual exploration_nodes, got %d", nodeCount)
      }
  }
  ```

  测试① 仅覆盖单任务/探索/节点删除 happy path，**不足以单独通过 F03**；Step 1 必须补充测试②（双根共享资产跨根弱引用闭包删除）与漂移注入验收矩阵（见后），开放范围 DML、漏纳跨根 PK、逆序补锁等变异实现必须至少被一条验收检出判红：

  **② 双根共享资产与跨根弱引用闭包删除（`TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML`）**

  ```go
  package db

  import (
      "database/sql"
      "errors"
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML(t *testing.T) {
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

      ts := time.Now().UnixNano()
      taskA, err := d.CreateTaskWithOptions(fmt.Sprintf("P08 Closure A %d", ts), "goal", TaskCreateOptions{})
      if err != nil {
          t.Fatalf("CreateTaskWithOptions taskA failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.DeleteTaskCascade(taskA.ID, false, false); err != nil {
              t.Errorf("cleanup taskA cascade failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", taskA.ExplorationID); err != nil {
              t.Errorf("cleanup taskA residual exploration_nodes failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", taskA.ExplorationID); err != nil {
              t.Errorf("cleanup taskA residual exploration failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", taskA.ID); err != nil {
              t.Errorf("cleanup taskA residual root failed: %v", err)
          }
          var nCount, tCount int
          if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", taskA.ExplorationID).Scan(&nCount); err != nil {
              t.Errorf("query residual taskA nodes failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", taskA.ID).Scan(&tCount); err != nil {
              t.Errorf("query residual taskA root failed: %v", err)
          }
          if nCount != 0 || tCount != 0 {
              t.Errorf("residual taskA fixtures found: nodes=%d tasks=%d", nCount, tCount)
          }
      })

      taskB, err := d.CreateTaskWithOptions(fmt.Sprintf("P08 Closure B %d", ts), "goal", TaskCreateOptions{})
      if err != nil {
          t.Fatalf("CreateTaskWithOptions taskB failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.DeleteTaskCascade(taskB.ID, false, false); err != nil {
              t.Errorf("cleanup taskB cascade failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM exploration_nodes WHERE exploration_id = ?", taskB.ExplorationID); err != nil {
              t.Errorf("cleanup taskB residual exploration_nodes failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM explorations WHERE id = ?", taskB.ExplorationID); err != nil {
              t.Errorf("cleanup taskB residual exploration failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM tasks WHERE id = ?", taskB.ID); err != nil {
              t.Errorf("cleanup taskB residual root failed: %v", err)
          }
          var nCount, tCount int
          if err := d.QueryRow("SELECT count(*) FROM exploration_nodes WHERE exploration_id = ?", taskB.ExplorationID).Scan(&nCount); err != nil {
              t.Errorf("query residual taskB nodes failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", taskB.ID).Scan(&tCount); err != nil {
              t.Errorf("query residual taskB root failed: %v", err)
          }
          if nCount != 0 || tCount != 0 {
              t.Errorf("residual taskB fixtures found: nodes=%d tasks=%d", nCount, tCount)
          }
      })

      // 双根共享资产：taskA/taskB 各持一行 task_asset_links，assets.task_ids 同时含两任务 ID（原子双写）
      domain := fmt.Sprintf("p08-shared-%d.com", ts)
      assetID, err := d.Assets().UpsertRootDomain(UpsertRootDomainReq{Domain: domain})
      if err != nil {
          t.Fatalf("UpsertRootDomain shared asset failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM task_asset_links WHERE asset_id = ?", assetID); err != nil {
              t.Errorf("cleanup shared asset links failed: %v", err)
          }
          if _, err := d.Exec("DELETE FROM assets WHERE id = ?", assetID); err != nil {
              t.Errorf("cleanup shared asset failed: %v", err)
          }
          var linkLeft, assetLeft int
          if err := d.QueryRow("SELECT count(*) FROM task_asset_links WHERE asset_id = ?", assetID).Scan(&linkLeft); err != nil {
              t.Errorf("query residual shared links failed: %v", err)
          }
          if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", assetID).Scan(&assetLeft); err != nil {
              t.Errorf("query residual shared asset failed: %v", err)
          }
          if linkLeft != 0 || assetLeft != 0 {
              t.Errorf("residual shared asset fixtures found: links=%d assets=%d", linkLeft, assetLeft)
          }
      })

      if _, err := d.Exec("INSERT INTO task_asset_links (task_id, asset_id, source, source_summary, source_node_id, created_at, updated_at) VALUES (?, ?, 'manual', 'closure-a', NULL, NOW(3), NOW(3))", taskA.ID, assetID); err != nil {
          t.Fatalf("link shared asset to taskA failed: %v", err)
      }
      if _, err := d.Exec("INSERT INTO task_asset_links (task_id, asset_id, source, source_summary, source_node_id, created_at, updated_at) VALUES (?, ?, 'manual', 'cross-root-b', NULL, NOW(3), NOW(3))", taskB.ID, assetID); err != nil {
          t.Fatalf("link shared asset to taskB failed: %v", err)
      }
      if _, err := d.Exec("UPDATE assets SET task_ids = ? WHERE id = ?", fmt.Sprintf("[%d,%d]", taskA.ID, taskB.ID), assetID); err != nil {
          t.Fatalf("seed assets.task_ids with both task ids failed: %v", err)
      }

      // 跨根弱引用：taskA 闭包内 intent 节点 + taskB exploration 下的 activity 行弱引用该节点；
      // 该跨根 activity.id 与 taskB 的 task_asset_links.row_id 必须被 taskA 删除闭包纳锁。
      nodeA, err := d.Exploration(taskA.ExplorationID).AddNode("intent", map[string]any{"label": "cross-root"}, 1, "pending", "agent", nil)
      if err != nil {
          t.Fatalf("AddNode intent in taskA failed: %v", err)
      }
      // 插入跨根 activity (exploration_id, node_id, kind, summary, metadata, created_at)
      if _, err := d.Exec("INSERT INTO activity (exploration_id, node_id, kind, summary, metadata, created_at) VALUES (?, ?, 'asset', 'cross-root-weak-ref', '{}', NOW(3))", taskB.ExplorationID, nodeA); err != nil {
          t.Fatalf("seed cross-root activity weak reference failed: %v", err)
      }

      // 防 no-op 前置断言：共享资产确有两行 links，跨根 activity 确以 node_id 弱引用 taskA 节点
      var linkCount, weakRefCount int
      if err := d.QueryRow("SELECT count(*) FROM task_asset_links WHERE asset_id = ?", assetID).Scan(&linkCount); err != nil {
          t.Fatalf("query shared link count failed: %v", err)
      }
      if err := d.QueryRow("SELECT count(*) FROM activity WHERE exploration_id = ? AND node_id = ? AND summary = 'cross-root-weak-ref'", taskB.ExplorationID, nodeA).Scan(&weakRefCount); err != nil {
          t.Fatalf("query cross-root weak ref count failed: %v", err)
      }
      if linkCount != 2 || weakRefCount != 1 {
          t.Fatalf("expected 2 shared links and 1 cross-root activity weak ref before deletion, got %d and %d", linkCount, weakRefCount)
      }

      if _, err := d.DeleteTaskCascade(taskA.ID, true, false); err != nil {
          t.Fatalf("DeleteTaskCascade(taskA, deleteAssets=true) failed: %v", err)
      }

      // 1) taskA 根被物理删除
      var gone bool
      queryErr := d.QueryRow("SELECT true FROM tasks WHERE id = ?", taskA.ID).Scan(&gone)
      if queryErr == nil {
          t.Fatalf("task %d still exists after cascade deletion", taskA.ID)
      }
      if !errors.Is(queryErr, sql.ErrNoRows) {
          t.Fatalf("query taskA existence failed: %v", queryErr)
      }

      // 2) 共享资产行必须存活：被 taskB 引用只允许 detach，开放范围物理删除必须判红
      var assetAlive int
      if err := d.QueryRow("SELECT count(*) FROM assets WHERE id = ?", assetID).Scan(&assetAlive); err != nil {
          t.Fatalf("query shared asset survival failed: %v", err)
      }
      if assetAlive != 1 {
          t.Fatalf("shared asset %d must survive deletion of one referencing task (open-range physical delete detected)", assetID)
      }

      // 3) taskB 的 link 行必须原样存活；跨根 activity 行必须保留且 node_id 被精确置空
      var bLinkAlive int
      if err := d.QueryRow("SELECT count(*) FROM task_asset_links WHERE task_id = ? AND asset_id = ?", taskB.ID, assetID).Scan(&bLinkAlive); err != nil {
          t.Fatalf("query taskB link survival failed: %v", err)
      }
      if bLinkAlive != 1 {
          t.Fatalf("taskB link row must survive (open-range delete from taskA closure detected)")
      }
      var weakRefNull, weakRefRows int
      if err := d.QueryRow("SELECT count(*) FROM activity WHERE exploration_id = ? AND summary = 'cross-root-weak-ref' AND node_id IS NULL", taskB.ExplorationID).Scan(&weakRefNull); err != nil {
          t.Fatalf("query cross-root activity nullify failed: %v", err)
      }
      if err := d.QueryRow("SELECT count(*) FROM activity WHERE exploration_id = ? AND summary = 'cross-root-weak-ref'", taskB.ExplorationID).Scan(&weakRefRows); err != nil {
          t.Fatalf("query cross-root activity survival failed: %v", err)
      }
      if weakRefRows != 1 || weakRefNull != 1 {
          t.Fatalf("cross-root activity row must survive exactly once with node_id nullified (rows=%d nullified=%d)", weakRefRows, weakRefNull)
      }

      // 4) detach 原子双写：assets.task_ids 剥离 taskA 保留 taskB；taskA 的 link 行删净
      var detachOK int
      if err := d.QueryRow("SELECT JSON_CONTAINS(task_ids, CAST(? AS JSON)) AND NOT JSON_CONTAINS(task_ids, CAST(? AS JSON)) FROM assets WHERE id = ?", fmt.Sprint(taskB.ID), fmt.Sprint(taskA.ID), assetID).Scan(&detachOK); err != nil {
          t.Fatalf("verify atomic detach in task_ids failed: %v", err)
      }
      if detachOK != 1 {
          t.Fatalf("expected assets.task_ids to drop taskA %d and keep taskB %d atomically", taskA.ID, taskB.ID)
      }
      var aLinkLeft int
      if err := d.QueryRow("SELECT count(*) FROM task_asset_links WHERE task_id = ? AND asset_id = ?", taskA.ID, assetID).Scan(&aLinkLeft); err != nil {
          t.Fatalf("query taskA residual links failed: %v", err)
      }
      if aLinkLeft != 0 {
          t.Fatalf("expected 0 residual taskA links, got %d", aLinkLeft)
      }

      // 5) taskB 根与 exploration 全部存活
      var bAlive, bExpAlive int
      if err := d.QueryRow("SELECT count(*) FROM tasks WHERE id = ?", taskB.ID).Scan(&bAlive); err != nil {
          t.Fatalf("query taskB survival failed: %v", err)
      }
      if err := d.QueryRow("SELECT count(*) FROM explorations WHERE id = ?", taskB.ExplorationID).Scan(&bExpAlive); err != nil {
          t.Fatalf("query taskB exploration survival failed: %v", err)
      }
      if bAlive != 1 || bExpAlive != 1 {
          t.Fatalf("taskB root/exploration must both survive deletion of taskA (tasks=%d explorations=%d)", bAlive, bExpAlive)
      }
  }
  ```

  **F03 锁计划与成员漂移验收矩阵（`TestTask_DeleteTaskCascade_MembershipDriftRetryRollback`，必须实现为可执行测试；时序注入依赖 Step 3 新增的测试钩子）**

  | 验收项 | fixture 与可控时序 | 通过（绿）判据 | 判红（变异实现）判据 |
  |---|---|---|---|
  | 跨根物理 PK 纳锁 | 复用测试② fixture；在闭包收集与加锁处埋观测断言 | 闭包锁计划完整包含跨根 `activity.id` 与 taskB `task_asset_links.row_id` 物理 PK，且加锁请求严格按 `(TableOrder, PK ASC)` 全序发出 | 漏收任一跨根物理 PK；同一 TableOrder 内未按 PK 升序、或跨表未按 TableOrder 排序的乱序补锁 |
  | 写集收敛 | 对 `cascade_executor` 全部 UPDATE/DELETE 做语句级审计（静态审查 + 运行期钩子日志断言） | 每条 DML 均带 `WHERE <Catalog 物理 PK 列> IN (?)`（如 id/row_id/ordinal）且参数 PK 集合 ⊆ 本轮已锁 PK 集合 | 任何按 `node_id`/`asset_id`/`exploration_id` 等逻辑列过滤的开放范围 DML 判红 |
  | 共享实体存活 | `DeleteTaskCascade(taskA.ID, true, false)` 后回读 | 共享资产行存活、taskB link/根/exploration 存活、`task_ids` 仅剥离 taskA | 共享资产被物理删除，或 taskB 关联被范围删除/置空 |
  | 成员漂移整体回滚重试 | Step 3 在 `db/lockorder/tx_runner.go` 新增导出测试钩子 `TestHookAfterSnapshot func()`（默认 nil，仅在闭包快照完成后、成员复核加锁前非 nil 时同步调用）；测试在钩子内用第二连接注入更低 PK 的 `task_asset_links` 新成员行 | 本轮尝试返回包裹 `lockorder.ErrMembershipDrift` 的错误；随后 `TxRunner` 整体回滚 + 指数退避重试，最终一次尝试成功、taskA 删除完成且验收项“共享实体存活”全部成立 | 未报漂移假绿通过；或逆序补锁（发现新成员后不回滚、在现事务内按插入顺序追加锁）；或仅释放部分已持锁重试 |

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTask_DeleteTaskCascade_UnmigratedDialect|TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML|TestTask_DeleteTaskCascade_MembershipDriftRetryRollback' -count=1)
  ```

- [ ] **Step 3: 实施代码改写与接线**
  - 改写 `db/tasks.go` 的删除与状态变更逻辑，接入 `TxRunner` 与 `cascade_executor.go`；
  - 改造 `db/task_closure.go` 补全 `AssetIDsToDetach`、物理 `row_id` 映射以及跨根弱关联物理行收集（`activity.id`、`task_asset_links.row_id` 等跨根物理 PK 全部纳入锁计划）；
  - 改造 `db/cascade_executor.go` 约束范围 DML：全部 UPDATE/DELETE 收敛为按表 Catalog 物理 PK 列限定 `WHERE <Catalog 物理 PK 列> IN (?)`（如 id/row_id/ordinal，严格限制于已锁 PK 集合）；
  - 在 `db/lockorder/tx_runner.go` 新增测试注入钩子 `TestHookAfterSnapshot func()`（默认 nil，仅测试非 nil 时在闭包快照后、成员复核加锁前同步调用），为漂移注入测试提供确定时序；
  - 改写其余任务相关文件的方言与双写维护。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTask_DeleteTaskCascade_UnmigratedDialect|TestTask_DeleteTaskCascade_CrossRootLockPlanAndNarrowDML|TestTask_DeleteTaskCascade_MembershipDriftRetryRollback' -count=1)
  ```

---

## 阶段 2 验收门禁

```bash
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./db/... -run 'TestAssets_|TestCompanies_|TestTask_' -count=1)
```
