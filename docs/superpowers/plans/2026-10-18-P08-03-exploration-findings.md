# P08-03 实施计划：探索图谱、意图推进与漏洞管理

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


> **计划编号**：`P08-03`  
> **所属阶段**：阶段 3（探索图谱、意图推进与漏洞管理）  
> **关联 Spec**：`docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md` §3.3  
> **目标**：重构 `db/exploration.go` 等 15 个探索与漏洞管理产品文件，消除全部 226 处 Blocker 方言；改写 `$1` 为 `?`，移除 `RETURNING`；改写 `DISTINCT ON` 为合法 JSON 派生表窗口函数；消除旧 DDL 运行时调用（`EnsureLLMRecordsTable`/`EnsureLLMUsageTable`）；重构聚合查询与流量关联。

---

## 涵盖的产品业务文件清单（15 个产品业务文件，精准映射）

| 文件路径 | R4 Blocker 数 | 核心职责与 M 项 |
|---|---:|---|
| `db/exploration.go` | 95 | 消除 95 处 SQL 方言，LastInsertId 替代 RETURNING，显式处理删除依赖（**M21**） |
| `db/exploration_sources.go` | 2 | 探索源关联查询与插入方言改造（**M21**） |
| `db/findings.go` | 28 | 漏洞 CRUD、聚合统计 `COUNT(CASE WHEN ... THEN 1 END)` 与删除置空（**M22**） |
| `db/finding_assets.go` | 7 | 漏洞资产绑定方言改造（**M22**） |
| `db/finding_retests.go` | 13 | 漏洞复测记录方言改造，适配 MySQL RowsAffected（**M29**） |
| `db/finding_traffic.go` | 24 | 流量关联绑定方言重构（**M22**） |
| `db/finding_traffic_archive.go` | 2 | 归档流量绑定方言重构（**M22**） |
| `db/commands.go` | 15 | 移除旧 DDL helper，改用预置基线；重构命令记录方言（**M21**） |
| `db/tools.go` | 12 | 工具状态与配置 CRUD，补齐 `key_identity` 与 `key_hash`（**M12**） |
| `db/llm_usage.go` | 5 | 移除旧 DDL helper，改写 LLM 使用账本统计方言（**M12**） |
| `db/llmhealth.go` | 2 | LLM 模型健康状态方言重构（**M12**） |
| `db/logs.go` | 3 | 日志记录方言重构（**M21**） |
| `db/digest.go` | 15 | 每日摘要与报告聚合查询方言重构（**M21**） |
| `db/intent_admission.go` | 2 | 意图准入状态机方言重构（**M21**） |
| `db/tool_usage.go` | 1 | 工具使用记录方言重构（**M12**） |
| **小计** | **226** | **覆盖阶段 3 全部 226 处 Blocker 与核心 M 项** |

---

## Task 3.1: 探索引擎与会话图谱方言重构（`db/exploration.go`, `db/exploration_sources.go`）

### 任务定义
- **修改文件**：`db/exploration.go`, `db/exploration_sources.go`
- **新建测试**：`db/exploration_test.go`
- **核心逻辑与 M 项绑定（M21）**：
  1. 95 处 SQL 中的 `$1, $2` 替换为 `?`，按实际顺序严格重新展开并对齐参数；
  2. 移除节点、边、活动写入的 `RETURNING id`，统一通过 `res.LastInsertId()` 获取，并校验在 `[1, math.MaxInt64]` 范围内；
  3. 消除 `pg_advisory_xact_lock`；
  4. 修复 `StampFirstRun` 等单表时序 UPDATE，改为加锁事务中在 Go 端内存计算后显式写入；
  5. 删除意图/目标节点时显式清理 edges/anchors 并置空 activity/finding/source_node 弱关联。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写探索图谱节点创建测试（`db/exploration_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestExploration_CreateNode_UnmigratedDialect(t *testing.T) {
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

      tName := fmt.Sprintf("P08 Exp Task %d", time.Now().UnixNano())
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
              t.Errorf("residual exploration fixtures found after cleanup: nodes=%d, explorations=%d, tasks=%d", nCount, eCount, tCount)
          }
      })

      nodeID, err := d.Exploration(task.ExplorationID).AddNode("intent", map[string]any{"label": "test"}, 1, "pending", "user", nil)
      if err != nil {
          t.Fatalf("AddNode failed: %v", err)
      }
      if nodeID <= 0 {
          t.Fatalf("expected positive nodeID, got %d", nodeID)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestExploration_CreateNode_UnmigratedDialect' -count=1)
  ```

- [ ] **Step 3: 重构 `db/exploration.go` 与 `db/exploration_sources.go`**
  消除 97 处 Blocker 方言，清理旧锁与 RETURNING 语句，实现 LastInsertId 替换。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestExploration_CreateNode_UnmigratedDialect' -count=1)
  ```

---

## Task 3.2: 漏洞管理与流量关联方言改造（5 个文件）

### 任务定义
- **修改文件**：`db/findings.go`, `db/finding_assets.go`, `db/finding_retests.go`, `db/finding_traffic.go`, `db/finding_traffic_archive.go`
- **新建测试**：`db/findings_test.go`
- **核心逻辑与 M 项绑定（M22, M29）**：
  1. `FindingStats` 聚合查询：将 `COUNT(*) FILTER (WHERE ...)` 全面改写为 `COUNT(CASE WHEN ... THEN 1 END)`，确保在空表时返回数值 0 而不是 SQL NULL；
  2. 动态参数过滤查询：使用参数化拼接 `IN (?, ?, ...)`；
  3. `RecordFindingRetestResult` 适配 MySQL `RowsAffected`，杜绝因未变更值返回 0 行误判为未找到记录；
  4. 漏洞删除时显式解除 evidence bindings 与 retest 关联。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写漏洞聚合统计测试（`db/findings_test.go`）**
  ```go
  package db

  import (
      "os"
      "testing"
  )

  func TestFindings_FindingStats_UnmigratedDialect(t *testing.T) {
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

      stats, err := d.FindingStats()
      if err != nil {
          t.Fatalf("FindingStats failed: %v", err)
      }
      if stats == nil || stats.Total < 0 {
          t.Fatalf("expected valid non-negative FindingStats, got %+v", stats)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestFindings_FindingStats_UnmigratedDialect' -count=1)
  ```

- [ ] **Step 3: 改造 5 个漏洞与流量文件**
  改写全部 SQL 方言，修复聚合查询与复测记录更新。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestFindings_FindingStats_UnmigratedDialect' -count=1)
  ```

---

## Task 3.3: 命令、工具、LLM 使用与日志模块改造（8 个文件）

### 任务定义
- **修改文件**：`db/commands.go`, `db/tools.go`, `db/llm_usage.go`, `db/llmhealth.go`, `db/logs.go`, `db/digest.go`, `db/intent_admission.go`, `db/tool_usage.go`
- **新建测试**：`db/tools_test.go`
- **核心逻辑与 M 项绑定（M12）**：
  1. 彻底移除 `commands.go` 与 `llm_usage.go` 中的运行时旧 DDL helper（`EnsureLLMRecordsTable`/`EnsureLLMUsageTable`），改由 `schema.VerifyBaseline` 保障；
  2. `tools.go`：补齐 `key_identity` 与 `key_hash`，并转义 MySQL 关键字反引号（如 `` `key`, `schema` ``）；
  3. 重构日志记录、意图准入与每日摘要聚合方言。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写工具注册测试（`db/tools_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestTools_RegisterTool_UnmigratedDialect(t *testing.T) {
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

      toolKey := fmt.Sprintf("p08_tool_%d", time.Now().UnixNano())
      err = d.SeedTool(toolKey, "desc", []byte("{}"), []byte("[]"))
      if err != nil {
          t.Fatalf("SeedTool failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM tools WHERE `key` = ?", toolKey); err != nil {
              t.Errorf("cleanup tool failed: %v", err)
          }
      })

      tool, err := d.GetTool(toolKey)
      if err != nil || tool == nil || tool.Key != toolKey {
          t.Fatalf("GetTool failed: got (%+v, %v), want key=%s", tool, err, toolKey)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTools_RegisterTool_UnmigratedDialect' -count=1)
  ```

- [ ] **Step 3: 改造 8 个模块文件**
  移除旧 DDL 函数，改写所有方言为纯 TiDB 语法。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestTools_RegisterTool_UnmigratedDialect' -count=1)
  ```

---

## 阶段 3 验收门禁

```bash
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./db/... -run 'TestExploration_|TestFindings_|TestTools_' -count=1)
```
