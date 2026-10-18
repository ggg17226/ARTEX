# P08 与原生 TiDB 8.5 全量方言迁移实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 彻底切断 PostgreSQL (`pgx`) 依赖，接入原生 TiDB 8.5 连接池；升级配置系统为 Viper+YAML；提供 `artex db-migrate` 运维 CLI；完成全部 43 个业务文件的 SQL 方言改造与级联闭包删除接线。

**Architecture:** 
1. 阶段 1：使用 `spf13/viper` 重构配置解析为 `config.yaml`，实现 `schema.VerifyBaseline` 只读元数据比对器，增加 `artex db-migrate` CLI 并改造 `db.Open` 及种子数据，实现纯 TiDB 独立启动闭环；
2. 阶段 2：重写 `db/assets.go` 等核心业务文件，落地 8 分支 Canonical Identity 字节仲裁与 ODKU，正式接线 `cascade_executor.go` 级联删除闭包引擎并遵守全体写者父锁协议；
3. 阶段 3：重构 `db/exploration.go` 等探索与漏洞文件，消除 `$1`、`RETURNING` 与 `DISTINCT ON`（改窗口函数）；
4. 阶段 4：重构通知队列为 CAS 租约状态机（对齐 `notification_deliveries` 与 `next_attempt_at`），建立归档显式列投影剔除 `row_id`，彻底移除 `pgx` 驱动并运行 W5 全量回归。

**Tech Stack:** Go 1.26.3, `database/sql` + `github.com/go-sql-driver/mysql`, `github.com/spf13/viper`, TiDB v8.5.8 (MySQL 8.0 协议), Next.js 16 / TypeScript 5.9.

**Spec:** `docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md`

## Global Constraints

- 严禁任何子代理执行 `git commit`、`git push`、`git merge` 或派生新的子代理。
- 严禁在测试中执行 `SET GLOBAL` 或修改任何 TiDB 全局参数；必须原生适配严格模式 `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES`。
- 环境变量 `ARTEX_TIDB_DSN` 拥有最高优先级；所有 DSN 必须最终通过 `db/tidb.NormalizeDSN()` 规范化并以 `db/tidb.RedactDSN()` 脱敏记录日志。
- 离线全仓测试必须保证环境变量未配置时通过 `t.Skip` 诚实跳过外部数据库测试，有真实配置时 100% 真实执行并通过。
- 43 个业务文件与 R4 审计项（M01–M37）必须严格按照阶段表闭合，不得遗漏任何一个文件。

---

## 阶段 1：核心骨架、Viper 配置、CLI 与启动闭环

### Task 1.1: 引入 Viper 与 `config/config.go` 重构

**Files:**
- Modify: `go.mod`, `go.sum`
- Modify: `config/config.go`
- Test: `config/config_test.go`

**Interfaces:**
- Produces: `config.TiDBDSN() (dsn, source string, err error)`
- Produces: `config.Load() Config`
- Consumes: `github.com/spf13/viper`, `db/tidb.NormalizeDSN`, `db/tidb.RedactDSN`

- [ ] **Step 1: 引入 `github.com/spf13/viper` 依赖**

Run: `go get github.com/spf13/viper@v1.19.0 && go mod tidy`

- [ ] **Step 2: 编写针对 Viper + YAML 及 DSN 解析的失败测试**

在 `config/config_test.go` 中编写用例：
```go
func TestTiDBDSN_EnvPrecedence(t *testing.T) {
    want := "artex:pass@tcp(192.168.27.79:6000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'"
    t.Setenv("ARTEX_TIDB_DSN", want)
    got, src, err := TiDBDSN()
    if err != nil || got != want {
        t.Fatalf("expected %s, got %s, err: %v", want, got, err)
    }
    if !strings.Contains(src, "ARTEX_TIDB_DSN") {
        t.Fatalf("expected env source, got %s", src)
    }
}
func TestTiDBDSN_RejectPostgres(t *testing.T) {
    t.Setenv("ARTEX_TIDB_DSN", "postgres://user:pass@127.0.0.1:5432/artex")
    _, _, err := TiDBDSN()
    if err == nil || !strings.Contains(err.Error(), "postgres") {
        t.Fatalf("expected rejection of postgres dsn, got %v", err)
    }
}
```

- [ ] **Step 3: 运行测试验证失败**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./config/... -run 'TestTiDBDSN' -count=1`
Expected: FAIL (TiDBDSN 未定义)

- [ ] **Step 4: 实现 `config/config.go`**

实现 Viper 读取 `config.yaml`、绑定 `ARTEX_` 环境变量前缀、支持 `ARTEX_TIDB_DSN` 优先级，拒绝 `postgres://`，并调用 `tidb.NormalizeDSN`。

- [ ] **Step 5: 运行测试验证通过**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./config/... -count=1`
Expected: PASS

---

### Task 1.2: 实现纯只读元数据基线比较器 `schema.VerifyBaseline`

**Files:**
- Create: `db/schema/verify_baseline.go`
- Test: `db/schema/verify_baseline_test.go`

**Interfaces:**
- Produces: `schema.VerifyBaseline(ctx context.Context, db *sql.DB) error`
- Consumes: `db/schema.AllTables()`, `information_schema.tables`, `columns`, `statistics`

- [ ] **Step 1: 编写只读元数据基线检查的测试**

编写 `db/schema/verify_baseline_test.go`：
- 在未连接或表缺失时返回明确错误；
- 容忍 21 处 DESC 索引差异；
- 严禁执行任何写入探针。

- [ ] **Step 2: 运行测试验证失败**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./db/schema/... -run 'TestVerifyBaseline' -count=1`
Expected: FAIL (`VerifyBaseline` 未实现)

- [ ] **Step 3: 实现 `db/schema/verify_baseline.go`**

通过 SQL 查询 `information_schema` 校验 65 表表名、列类型、可空性、主键及 62 个唯一索引（对比 `AllTables()`）。

- [ ] **Step 4: 运行测试验证通过**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./db/schema/... -run 'TestVerifyBaseline' -count=1`
Expected: PASS

---

### Task 1.3: 重构 `cmd/artex` 增加 `db-migrate` CLI

**Files:**
- Modify: `cmd/artex/main.go`
- Create: `cmd/artex/migrate.go`
- Test: `cmd/artex/migrate_test.go`

**Interfaces:**
- Produces: `artex db-migrate --prepare`, `--verify`, `--status`
- Consumes: `config.TiDBDSN()`, `tidb.OpenTiDB()`, `schema.ApplyDDL()`, `schema.PrepareInfraSQL()`, `tidb.EnsureLockInfrastructure()`, `schema.VerifyBaseline()`

- [ ] **Step 1: 编写 CLI 子命令分派测试**

测试在启动阶段对 `db-migrate --status` 等参数的正确捕获与分派，确保早于 server/worker 启动。

- [ ] **Step 2: 实现 `cmd/artex/migrate.go` 并接入 `main.go`**

实现 `--prepare`（DDL+InfraSeed+Verify）、`--verify`（调用 VerifyBaseline）、`--status`（输出版本、sql_mode、表数与 maintenance 锁持有状态）。

- [ ] **Step 3: 运行测试验证**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./cmd/artex/... -count=1`
Expected: PASS

---

### Task 1.4: 重构 `db/db.go` 切除 pgx 并完成启动种子 TiDB 化

**Files:**
- Modify: `db/db.go`
- Modify: `db/settings.go`
- Modify: `db/config.go`
- Modify: `server/manager.go`
- Test: `db/db_test.go`

**Interfaces:**
- Produces: `db.Open(dsn string) (*DB, error)`
- Consumes: `tidb.OpenTiDB()`, `schema.VerifyBaseline()`, `schema.AssertIdentityHashMatch()`

- [ ] **Step 1: 编写切断 pgx 后的 db.Open 测试**

断言 `db.Open` 不再引用 pgx，使用 TiDB 驱动，校验 baseline，并在测试中验证 `seedBuiltins` 幂等写入成功。

- [ ] **Step 2: 移除 pgx 并改造 `db.Open` 与 `seedBuiltins`**

- 移除 `_ "github.com/jackc/pgx/v5/stdlib"` 与 `schema.sql` 嵌入；
- 移除 `SELECT pg_advisory_lock`；
- 改造 `seedBuiltins`、`db/settings.go`、`db/config.go` 中的种子与查询语句为 `?` 方言，计算 `key_identity` 与 `key_hash`；
- 移除 `server/manager.go` 中对 `EnsureLLMRecordsTable` 与 `EnsureLLMUsageTable` 的启动调用。

- [ ] **Step 3: 验证阶段 1 启动闭环**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./db/... ./server/... -run 'TestOpen|TestSeed|TestManager' -count=1`
Expected: PASS

---

## 阶段 2：核心业务主干（资产、任务与级联删除闭包）

### Task 2.1: 重构 `db/assets.go` 实现 8 分支 Hash 仲裁与 ODKU

**Files:**
- Modify: `db/assets.go`
- Modify: `db/asset_dsl.go`
- Test: `db/assets_test.go`

**Interfaces:**
- Produces: `as.UpsertRootDomain`, `as.UpsertIP`, `as.UpsertSubdomain`, `as.UpsertApp`, `as.UpsertHTTPService`, `as.UpsertOtherService`, `as.UpsertEndpoint`
- Consumes: `schema.Assets*Identity`, `schema.CanonicalHash`, `schema.VerifyHashArbitration`

- [ ] **Step 1: 编写 8 分支资产 Upsert 与 Hash 碰撞回滚测试**

编写对 8 类资产分别插入、更新及构造相同 Hash 不同 Identity 触发 `ErrIdentityHashCollision` 回滚的测试。

- [ ] **Step 2: 重构 `db/assets.go` 全部 8 个 Upsert 函数**

- 消除 `$1`、PG 数组字面量与 `RETURNING id`；
- 计算对应分支的 `identity_*` 与 `hash_*`，其余 7 分支显式赋 `NULL`；
- `ON DUPLICATE KEY UPDATE` 配合 `LastInsertId()`，并在同事务内调用 `schema.VerifyHashArbitration` 严格仲裁；
- 将 `task_ids`、`open_ports` 等复杂字段序列化为标准 JSON 字符串写入。

- [ ] **Step 3: 运行资产单测验证**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./db/ -run 'TestAssets' -count=1`
Expected: PASS

---

### Task 2.2: 改造 `db/companies.go` 与 `db/company_scope.go`

**Files:**
- Modify: `db/companies.go`
- Modify: `db/company_scope.go`
- Test: `db/companies_test.go`

- [ ] **Step 1: 编写企业与范围表 Hash 唯一键测试**
- [ ] **Step 2: 改造 SQL 占位符为 `?`，写入 `nkey_identity`/`nkey_hash` 及范围表 3 分支 Hash**
- [ ] **Step 3: 运行企业测试验证通过**

---

### Task 2.3: 重写 `db/tasks.go` 并正式接线级联删除闭包引擎

**Files:**
- Modify: `db/tasks.go`
- Modify: `db/task_assets.go`
- Modify: `db/task_scope.go`
- Modify: `db/task_context.go`
- Modify: `db/task_categories.go`
- Modify: `db/task_templates.go`
- Modify: `db/task_intercept.go`
- Test: `db/tasks_test.go`, `db/cascade_executor_test.go`

**Interfaces:**
- Produces: `d.DeleteTaskCascadePrepared(taskID, opts, ...)`
- Consumes: `db/task_closure.ComputeTaskClosure`, `db/lockorder.LockEntitiesInOrder`, `db/cascade_executor.ExecuteTaskCascade`

- [ ] **Step 1: 编写生产级联删除包含 delete_assets/delete_findings 选项与共享资产保护的测试**

- [ ] **Step 2: 改造 `db/tasks.go`**
- 移除表级锁 `LOCK TABLE tasks IN SHARE ROW EXCLUSIVE MODE`；
- 改造任务创建与状态更新 DML 为 `?` 方言；
- 在删除入口接线四阶段闭环（Fresh Locator -> 悲观锁序 -> 成员漂移核验 -> 物理逆拓扑清理）；
- 实现 `assets.task_ids` 与 `task_asset_links` 在同一事务内的显式双写同步。

- [ ] **Step 3: 运行任务与级联删除集成测试验证**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./db/ -run 'TestTask|TestClosure' -count=1`
Expected: PASS

---

## 阶段 3：探索图谱、意图推进与漏洞管理

### Task 3.1: 改造 `db/exploration.go` 消除 95 处 PG 方言

**Files:**
- Modify: `db/exploration.go`
- Modify: `db/exploration_sources.go`
- Test: `db/exploration_test.go`

- [ ] **Step 1: 编写最新会话步骤窗口函数与节点插入 LastInsertId 测试**
- [ ] **Step 2: 改造 95 处 SQL**
- `$1` 全面转 `?` 并按出现顺序重排参数；
- 移除 `RETURNING id`，改用 `LastInsertId()`；
- `DISTINCT ON (session)` 改写为 `ROW_NUMBER() OVER (PARTITION BY session ORDER BY id DESC) AS rn` 派生表；
- `payload->>'summary'` 改写为 `JSON_UNQUOTE(JSON_EXTRACT(payload, '$.summary'))`；
- `StampFirstRun` 消除逐列更新时序副作用，先读后写。
- [ ] **Step 3: 运行探索图谱测试验证通过**

---

### Task 3.2: 改造 `db/findings.go`, `db/finding_assets.go`, `db/finding_retests.go`

**Files:**
- Modify: `db/findings.go`
- Modify: `db/finding_assets.go`
- Modify: `db/finding_retests.go`
- Modify: `db/finding_traffic.go`
- Modify: `db/finding_traffic_archive.go`
- Test: `db/findings_test.go`

- [ ] **Step 1: 改造 `FindingStats` 的 `COUNT(*) FILTER` 为 `COUNT(CASE WHEN ... THEN 1 END)`**
- [ ] **Step 2: 改造漏洞去重、资产树与重测 CRUD 为 `?` 方言与 `LastInsertId`**
- [ ] **Step 3: 运行漏洞测试验证通过**

---

### Task 3.3: 改造 `db/commands.go`, `db/tools.go`, `db/llm_usage.go`, `db/logs.go`, `db/digest.go`

**Files:**
- Modify: `db/commands.go`
- Modify: `db/tools.go`
- Modify: `db/llm_usage.go`
- Modify: `db/logs.go`
- Modify: `db/digest.go`
- Modify: `db/llmhealth.go`
- Modify: `db/nkey.go`
- Modify: `db/intent_admission.go`
- Modify: `db/tool_usage.go`

- [ ] **Step 1: 改造命令记录与 LLM 消耗账本 SQL 为 `?` 方言**
- [ ] **Step 2: 彻底清除启动期历史建表代码 `EnsureLLMRecordsTable` 与 `EnsureLLMUsageTable`**
- [ ] **Step 3: 运行相关模块测试验证通过**

---

## 阶段 4：辅助模块、通知队列 CAS、归档物理隔离与全量依赖清理

### Task 4.1: 重构 `db/notification_delivery.go` 为 CAS 租约状态机

**Files:**
- Modify: `db/notification_delivery.go`
- Modify: `db/notification.go`
- Test: `db/notification_test.go`

- [ ] **Step 1: 编写多 Worker 并发领取投递批次零重复与退避时间锁定的测试**
- [ ] **Step 2: 改造通知队列**
- 表名严格对齐真实 catalog `notification_deliveries`，退避时间列使用 `next_attempt_at`；
- 移除 `SKIP LOCKED`，实现两阶段 CAS 租约更新（限定 `next_attempt_at <= NOW(3)`）；
- 处理超时未响应 sending 的重领与 fencing 机制。
- [ ] **Step 3: 运行通知测试验证通过**

---

### Task 4.2: 改造 `db/task_archives.go` 与 `db/task_archives_restore.go` 实现物理主键隔离

**Files:**
- Modify: `db/task_archives.go`
- Modify: `db/task_archives_restore.go`
- Modify: `db/task_archive_aggregate_stats.go`
- Test: `db/task_archives_test.go`

- [ ] **Step 1: 编写归档 manifest 严格排除 row_id 且恢复重分配物理主键测试**
- [ ] **Step 2: 改造归档与恢复**
- 建立涉及隐藏 `row_id` 的 7 张表显式列白名单投影，消除 `SELECT *` 与 `row_to_json`；
- 生成列与 `row_id` 绝对不输出至快照；
- 恢复时显式省略物理自增主键，交由 TiDB 重新分配，并同步维护逻辑引用映射。
- [ ] **Step 3: 运行归档与恢复测试验证通过**

---

### Task 4.3: 改造剩余辅助模块并彻底移除 `pgx` 依赖

**Files:**
- Modify: `db/conversation.go`
- Modify: `db/triggers.go`
- Modify: `db/chat_mentions.go`
- Modify: `db/skill_usage.go`
- Modify: `db/side_questions.go`
- Modify: `db/constraints.go`
- Modify: `db/intercept_detail.go`
- Modify: `db/intercept_execution.go`
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: 改造会话、触发器、旁路提问与拦截详情 SQL 为 `?` 方言**
- [ ] **Step 2: 检查全仓并彻底移除 `github.com/jackc/pgx/v5`**

Run: `go mod tidy`
验证 `go.mod` 与 `go.sum` 中没有任何 `pgx`、`pgconn`、`puddle` 依赖。

- [ ] **Step 3: 运行全仓离线测试验证**

Run: `export GOCACHE=$PWD/.gocache-p08 && go test ./... -count=1 -skip '^TestAcquireMaintenance_InvalidOpID$'`
Expected: PASS

---

## 阶段 5：全量真实库回归验收（终局验证）

### Task 5.1: 运行全量真实库回归测试并留存凭证

- [ ] **Step 1: 执行全量离线验证门禁**
`env -u ARTEX_TIDB_DSN -u ARTEX_TEST_TIDB_DSN go build ./... && go vet ./... && go test ./... -count=1`
- [ ] **Step 2: 执行前端验证门禁**
`cd web && npx tsc --noEmit && node --test src/lib/activity-merge.test.mjs src/lib/chat-mentions.test.mjs`
- [ ] **Step 3: 执行真实 TiDB 集成回归（`artex_dev`）**
`ARTEX_TIDB_DSN="..." go test -v ./... -count=1`，核验业务全生命周期通过、死锁免疫防线有效、集群现场无临时数据残留。
- [ ] **Step 4: 执行空库 Schema 冒烟（`artex`）**
`ARTEX_TEST_TIDB_DSN="..." go test -v ./db/schema/ -run 'TestSmokeRealTiDBDDL|TestVerify_RealTiDB_Structure' -count=1`，核验 65 表从零创建与自清理还原。
- [ ] **Step 5: 输出终局验收报告**
将所有命令执行日志汇总至 `.superpowers/sdd/tidb-real-fix/p08-final-report.md`，更新进度账本。
