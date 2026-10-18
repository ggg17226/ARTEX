# ARTEX 原生 TiDB 8.5 架构迁移与生产部署技术规格说明书 (P08 & Native TiDB Migration Spec)

> 日期：2026-10-18（第 2 版，经 R27 独立审查意见全面修订）  
> 状态：APPROVED  
> 目标：彻底切断 PostgreSQL 依赖，全仓接入原生 TiDB 8.5，升级配置机制为 Viper+YAML，完成 43 个业务文件的方言转换与级联闭包删除接线。

---

## 1. 目标、范围与非目标

### 1.1 核心目标
1. **纯 TiDB 原生运行时**：全仓移除 `github.com/jackc/pgx/v5` 及其全部依赖，底层统一收敛至已验收的 `db/tidb.OpenTiDB`（标准 `*sql.DB` + 32 连接池 + 能力探测）。
2. **配置机制现代化**：基于 `github.com/spf13/viper` 管理，配置文件统一为 `config.yaml`，原生支持环境变量映射与连接串自动组装。
3. **运维与启动工具**：提供 `artex db-migrate` 子命令体系（`--prepare`, `--verify`, `--status`），替代所有临时建表脚本；启动时执行纯只读元数据基线校验。
4. **全量方言与写者协议重构**：消除 43 个业务文件中的 693 处 PG SQL 方言与 M01–M37 语义项（依据 `r4-pg-dialect-audit.md`）。
5. **完整级联闭包与锁序接线**：接入 `cascade_executor.go` 与 `db/lockorder`，建立全体写者共同遵守的父锁与成员漂移协议，消除孤儿与死锁。
6. **归档与物理隔离**：归档快照建立显式列投影白名单，严禁物理主键 `row_id` 泄漏到 manifest 中。

### 1.2 非目标（明确声明）
- **存量数据搬迁**：本次工程聚焦于代码实现、新环境 DDL 部署及全新 TiDB 集群的运行。历史生产 PostgreSQL 数据导出、清洗、回填到 TiDB 的搬迁脚本属于独立运维任务，不包含在本次代码仓库改造范围内。`artex db-migrate --prepare` 仅用于新实例初始化，不做存量数据导入。
- **TiDB 集群外部参数修改**：严禁向 TiDB 执行任何 `SET GLOBAL` 或修改系统变量操作；所有功能完全适配 TiDB 8.5 严格模式默认参数（`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES` 等）。

---

## 2. 43 个业务文件与 R4 迁移矩阵（四阶段映射）

依据 `r4-pg-dialect-audit.md` 的 43 个非测试业务文件（共 693 条方言 SQL 及 M01–M37 语义项），划分四个串行阶段并建立严格的依赖归属矩阵：

| 阶段 | 阶段主题 | 覆盖的业务文件与模块 | 解决的 R4 核心 M 项与职责 |
|---|---|---|---|
| **阶段 1** | **核心骨架、配置、CLI 与启动闭环** | `config/config.go`, `cmd/artex/main.go`, `db/db.go`, `db/settings.go`, `db/constants.go`, `db/config.go`（仅种子部分）, `db/intercept.go`（仅内置规则种子部分）, `db/asset_intercept.go`（仅内置规则种子部分） | **M01, M02, M07, M08, M11**：切除 pgx 驱动；引入 Viper+YAML 与 CLI；纯只读 baseline comparator；重构 `seedBuiltins`，计算必要 hash 与 identity，实现无 PG 环境下的纯 TiDB 启动闭环。 |
| **阶段 2** | **核心业务主干：资产、任务与级联闭包** | `db/assets.go`, `db/asset_dsl.go`, `db/companies.go`, `db/company_scope.go`, `db/tasks.go`, `db/task_assets.go`, `db/task_assets_context.go`, `db/task_scope.go`, `db/task_context.go` | **M05, M09, M10, M14, M19, M26, M27**：资产 8 分支 Canonical Identity 字节仲裁与 ODKU；企业及范围表 hash 转换；任务状态机与 `tasks.go` 废除表锁；接线 `cascade_executor.go` 与 `task_ids`/`task_asset_links` 双写同步；建立父锁与漂移回滚协议。 |
| **阶段 3** | **探索图谱与漏洞管理** | `db/exploration.go`, `db/exploration_sources.go`, `db/findings.go`, `db/finding_assets.go`, `db/finding_retests.go`, `db/commands.go`, `db/tools.go`, `db/llm_usage.go`, `db/llmhealth.go`, `db/logs.go`, `db/nkey.go` | **M03, M04, M12, M13, M15, M16, M20, M21, M28**：消除 95 处 exploration SQL 中的 `$1`、`RETURNING` 与 `DISTINCT ON`（改窗口函数）；移除启动期旧 DDL（`EnsureLLMRecordsTable`/`EnsureLLMUsageTable`）；`FindingStats` 聚合 `COUNT FILTER` 改写；UPDATE/DELETE 锁后回查替代 RETURNING。 |
| **阶段 4** | **辅助模块、归档隔离与收尾瘦身** | `db/intercept.go`（业务 CRUD）, `db/intercept_detail.go`, `db/intercept_execution.go`, `db/notification.go`, `db/notification_delivery.go`, `db/task_archives.go`, `db/task_archives_restore.go`, `db/task_archive_aggregate_stats.go`, `db/skill_usage.go`, `db/side_questions.go`, `db/constraints.go` | **M06, M17, M18, M22–M25, M29–M37**：拦截流与审计 CRUD；通知系统改用 CAS 租约状态机替代 `SKIP LOCKED`；归档快照建立白名单彻底排除 `row_id`；彻底从 `go.mod` 移除 `pgx` 依赖。 |

---

## 3. 详细架构与技术设计规范

### 3.1 阶段 1：配置机制、纯 TiDB 启动与运维 CLI

#### 3.1.1 Viper 配置架构与 DSN 归一化（解决 F09）
1. **文件与寻址**：
   - 采用 `config.yaml`。寻址优先级：
     1. 环境变量 `ARTEX_CONFIG`（若设置则必须存在，不存在或语法错误直接 fail-closed 退出，不静默回退）；
     2. `./config.yaml`（当前工作目录）；
     3. `config.BaseDir()/config.yaml`（二进制所在目录）。
   - 若无配置文件，但环境变量完整提供了连接信息，允许无文件启动；若均未提供，报错退出。
2. **环境变量绑定与键映射**：
   - Viper 开启环境变量绑定，设置 EnvKeyReplacer 将 `.` 替换为 `_`（`database.dsn` 映射为 `ARTEX_DATABASE_DSN`）；
   - **最高优先级保留**：显式检查环境变量 `ARTEX_TIDB_DSN`，若非空则直接以此为基础；
   - 彻底废除 `ARTEX_PG_DSN` 与 `postgres://` 协议，若配置提供 `postgres://` 显式报错拒绝，严禁静默连错库。
3. **DSN 组装与统一规范化**：
   - 无论是直接填写 `database.dsn` 还是通过 `host/port/user/password/dbname` 散列字段组装，**所有 DSN 必须最终调用 `db/tidb.NormalizeDSN()`**；
   - 强制补充 `charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'`；
   - 默认端口为 TiDB 标准端口 4000；密码不得随意 TrimSpace；日志输出统一使用 `db/tidb.RedactDSN()` 脱敏，禁止使用旧的 URL Redact。

#### 3.1.2 启动时序与纯只读元数据校验（解决 F01, F04）
1. **切断 pgx 与启动闭环**：
   - 移除 `_ "github.com/jackc/pgx/v5/stdlib"`；
   - `db.Open(dsn)` 仅调用 `tidb.OpenTiDB(dsn)`，共享 32 最大连接池并完成基础能力探测（要求会话时区必须为 UTC）；
   - 移除旧内嵌 `schema.sql`、移除 PG Advisory Lock（`pg_advisory_lock`）与重试逻辑。
2. **纯只读 Baseline 比较器（`schema.VerifyBaseline`）**：
   - 在 `db/schema/verify.go` 中实现纯只读元数据检查函数 `VerifyBaseline(ctx context.Context, db *sql.DB) error`（严禁执行任何写入探针）；
   - 从 `information_schema.tables`、`columns`、`statistics` 中核查 65 表是否存在、列数（729 列）、列类型、UNSIGNED、NOT NULL、生成列表达式与 62 个 UNIQUE 索引；
   - 明确对齐已知事实：对 21 处 DESC 索引容忍真实库的 ASC 等价存储，其余任何列缺失、类型不符、唯一键丢失均返回不可忽略的错误；
   - 若校验失败，启动直接中止并提示用户运行 `artex db-migrate --prepare`。
3. **种子数据原生化（`seedBuiltins`）**：
   - 将启动必需的 `settings`、内置 `agents`、`prompt_vars`、`mcp_servers` 及内置拦截规则的写入语句全面改为 `?` 方言；
   - 计算各表所需的 canonical identity 和 hash（如 `name_identity`/`name_hash`、`key_identity`/`key_hash`）；
   - 采用 `ON DUPLICATE KEY UPDATE` 保证启动期幂等。

#### 3.1.3 命令行运维工具体系（`cmd/artex db-migrate`）（解决 F05）
1. **分派时机**：
   - 在 `cmd/artex/main.go` 中，在启动任何 HTTP 服务、worker、代理或自更新检查**之前**，优先解析并分派子命令；
2. **命令行为规范**：
   - `artex db-migrate --prepare`：
     - 调用 `schema.GenerateStatements()` 获取 65 表 DDL 并调用 `ApplyDDL`；
     - 调用 `tidb.EnsureLockInfrastructure` 补齐 `maintenance` 2 行 singleton 种子及 `locks` 表 infra seed；
     - 执行后自动调用 `VerifyBaseline` 确保完整性；
     - 遇到部分表已建或执行中断，保持已建表，重跑需幂等支持（`CREATE TABLE IF NOT EXISTS`），严禁自动 DROP 既有表。
   - `artex db-migrate --verify`：调用 `VerifyBaseline` 输出全表结构核验报告。
   - `artex db-migrate --status`：输出 TiDB 版本、sql_mode、BASE TABLE 数量、`schema_meta.schema_version` 及 maintenance 当前 owner 状态（只读检查，不执行强制释放）。

---

### 3.2 阶段 2：资产 8 分支 Hash 唯一键与写者/删除闭包协议

#### 3.2.1 资产表 8 分支 Canonical Identity 字节仲裁与 ODKU（解决 F03）
1. **8 分支匹配与列激活**：
   - 覆盖 8 分支：`root_domain`, `ip`, `subdomain`, `app_bundle`, `app_name`, `service_http`, `service_other`, `endpoint`；
   - 写入前必须调用已有的 `Assets*Identity` 生成对应的 `identity_*`（MEDIUMBLOB）与 `CanonicalHash`（BINARY(32)）；
   - 严格约束：每次插入仅激活当前分支对应的一对 `identity_*` 与 `hash_*` 列，其余 7 个分支必须显式绑定为 `NULL`，严禁写入空 hash 字节导致非随机冲突。
2. **冲突仲裁与原子事务协议**：
   - 消除 `RETURNING id`；
   - **两阶段写入与仲裁**：在事务内执行 `INSERT ... ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), last_seen=..., ...`；
   - 拿到 `res.LastInsertId()` 后，若属于已有行更新，必须在同事务内通过 `hash_*` 索引回查该行的 `identity_*`，执行 `bytes.Equal` 字节级仲裁（复用 `schema.AssertIdentityHashMatch`）；
   - 若出现 hash 相同但 identity 不匹配，立即判定为 `ErrIdentityHashCollision` 并**显式 Rollback 事务**，严禁覆盖他人数据。
3. **复杂列写入规范**：
   - `task_ids`, `bound_domains`, `open_ports`, `technologies`, `auth`, `params` 必须在 Go 端使用 `json.Marshal` 编码为有效 JSON 字符串；
   - 空值一律写入 `[]` 或 `{}`，严禁写入 Go nil 导致的 SQL NULL 或 PG 数组文字 `{...}`。

#### 3.2.2 全体写者/删除者锁序与级联删除闭包接线（解决 F02, F06, F07）
1. **全体写者/删除者契约**：
   - 废除原 PG 的 `LOCK TABLE tasks IN SHARE ROW EXCLUSIVE MODE`；
   - **写者协议**：任何向任务追加资产、意图、锚点、漏洞、会话的操作，必须在事务内先通过 `lockorder` 锁住父任务行，核验父任务处于活动状态（未被标记删除），防止向正在被删除的任务并发挂载孤儿节点；
   - **删除者协议（四阶段闭环）**：
     1. **新鲜定位（Fresh Locator）**：在独立只读事务中调用 `ComputeTaskClosure` 枚举所有子孙实体候选物理 PK；
     2. **全序加锁（Ordered Locking）**：在外层数据事务中，通过 `lockorder.LockEntitiesInOrder` 严格按照 `(table-order, physical PK 升序)` 对目标实体加 `FOR UPDATE` 行级悲观锁；
     3. **成员漂移核验（Drift Verification）**：持锁后重新核验当前成员集合。若发现新成员或更低 PK 行插入，严禁逆序追加补锁，必须抛出 `ErrMembershipDrift` 由事务运行器触发整体回滚并指数退避重试；
     4. **物理逆拓扑清理**：接入 `db/cascade_executor.go` 的生产包装器，按严格依赖逆序执行物理删除/解除关联。
2. **生产删除选项与双写维护**：
   - 产品包装器必须完整实现 `DeleteTaskOptions`（`delete_assets`, `delete_findings`, `delete_llm_records`）；
   - **共享资产保护**：若资产被多个任务引用，仅从资产的 `task_ids` JSON 数组中移除当前 task_id，并同步物理删除 `task_asset_links` 对应行；只有当资产为该任务独占且选项允许时才执行资产物理删除；
   - **无触发器双写同步**：TiDB 不支持数据库触发器，`tasks.task_ids` 的更新与 `task_asset_links` 的物理增删必须在同一数据事务中显式原子维护。

---

### 3.3 阶段 3：探索引擎与漏洞管理 SQL 方言改造

#### 3.3.1 探索图谱与会话 SQL 重构（`db/exploration.go`）（解决 F12）
1. **占位符展开与返回值重构**：
   - 95 处 SQL 中的 `$1, $2` 替换为 `?`，按 SQL 文本实际出现顺序严格重新展开并对齐参数；
   - 移除节点、边、活动写入的 `RETURNING id`，统一通过 `res.LastInsertId()` 获取，并校验其在 `1..math.MaxInt64` 安全范围内。
2. **真实 DISTINCT ON 消除**：
   - 会话最新步与资产去重查询：废除 `DISTINCT ON`，统一改写为派生表窗口函数：
     ```sql
     SELECT ... FROM (
         SELECT *, ROW_NUMBER() OVER (PARTITION BY session ORDER BY id DESC) AS rn
         FROM activity WHERE exploration_id = ?
     ) t WHERE t.rn = 1
     ```
   - 资产端口与参数去重：保留非空 service 优先、前缀特异性优先的排序规则。
3. **JSON 算子改造**：
   - 旧 PG 的 `payload->>'summary'` 改写为 `JSON_UNQUOTE(JSON_EXTRACT(payload, '$.summary'))`；
   - 移除所有 `pg_advisory_xact_lock`。
4. **单表 UPDATE 赋值顺序安全**：
   - 如 `StampFirstRun` 等在同一个 UPDATE 中既修改时间又判断旧时间的逻辑，改为在加锁事务中先读取旧值，在 Go 内存计算后显式更新，杜绝 MySQL/TiDB 单表逐列更新带来的赋值时序副作用。

#### 3.3.2 漏洞管理与聚合（`db/findings.go`）
- `FindingStats` 聚合查询：将 `COUNT(*) FILTER (WHERE ...)` 全面改写为 `COUNT(CASE WHEN ... THEN 1 END)`，确保在空表时返回数值 0 而不是 SQL NULL；
- 动态参数过滤查询：使用参数化拼接 `IN (?, ?, ...)`。
- 清理 `EnsureLLMRecordsTable` 与 `EnsureLLMUsageTable` 旧启动代码。

---

### 3.4 阶段 4：辅助模块、通知队列、归档物理隔离与依赖切除

#### 3.4.1 通知队列 CAS 租约状态机（解决 F08）
1. **对齐真实 Catalog**：
   - 真实表名为 **`notification_deliveries`**，租约与退避时间列为 **`next_attempt_at`**；
2. **状态机与重领协议**：
   - 废除不存在的 `lease_expires_at` 与 `SKIP LOCKED`；
   - **两阶段 CAS 领取**：
     1. 在事务内执行带条件的租约领取：
        ```sql
        UPDATE notification_deliveries
        SET state = 'sending', next_attempt_at = TIMESTAMPADD(SECOND, ?, NOW(3)), attempts = attempts + 1
        WHERE state = 'pending' OR (state = 'sending' AND next_attempt_at < NOW(3))
        ORDER BY id ASC LIMIT ?
        ```
     2. 配合独占 DB Gate 或同事务按主键回读锁定的行集合，保证只有成功 CAS 抢占到租约的 worker 才能获得投递任务；
   - 遇到租约超时崩溃的残留任务，由下一轮轮询作为过期 `sending` 安全重领；投递成功后更新为 `sent`，失败根据重试策略更新 `next_attempt_at` 并重置为 `pending` 或终态 `failed`。

#### 3.4.2 任务归档快照显式投影与物理隔离（解决 F10）
1. **白名单静态投影定义**：
   - 废除 `SELECT *`、`row_to_json` 与 `to_jsonb`；
   - 对涉及隐藏 `row_id` 的 7 张表（`exploration_edges`, `exploration_anchors`, `task_relations`, `task_asset_links`, `task_llm_profiles`, `traffic_evidence_snapshots`, `side_question_sessions`）逐表建立显式列白名单；
   - 生成列（如 `side_question_requests.running_session_key`）与物理 `row_id` 绝对禁止出现在导出列与快照 JSON 中。
2. **恢复（Restore）重分配协议**：
   - 废除 `json_populate_recordset`；
   - 恢复导入数据时，必须显式省略自增主键和 `row_id` 列，由 TiDB 自动重新分配全新的物理自增主键与 `row_id`；
   - 恢复过程中遇到资产或关联对象冲突，通过逻辑身份键完成重映射，并在恢复完成后由 Go 内存重新计算该实体的 Canonical Hash 写入。

#### 3.4.3 全量移除 pgx
- 检查并删除全仓所有 `.go` 文件中的 `github.com/jackc/pgx/v5` 及其子包引用；
- 执行 `go mod tidy`，确保 `go.mod` 与 `go.sum` 彻底清除全部 PostgreSQL 驱动。

---

## 4. 质量门禁与回归验收标准（解决 F11）

全量迁移的验收必须严格区分三类不同执行环境，杜绝假绿：

### 4.1 离线全量门禁（不配置任何 DSN）
```bash
env -u ARTEX_TIDB_DSN -u ARTEX_TEST_TIDB_DSN go build ./...     # 必须 BUILD_OK
env -u ARTEX_TIDB_DSN -u ARTEX_TEST_TIDB_DSN go vet ./...       # 必须 VET_OK
env -u ARTEX_TIDB_DSN -u ARTEX_TEST_TIDB_DSN go test ./... -count=1 -skip '^TestAcquireMaintenance_InvalidOpID$'
```
- 全仓 100% 编译通过，0 vet 诊断；
- 离线测试中除明确标记依赖外部 DB 的测试允许 `t.Skip` 外，其余所有单元测试必须全部 PASS。

### 4.2 前端对齐门禁
```bash
cd web
npx tsc --noEmit --incremental false                            # 必须 0 错误
node --test src/lib/activity-merge.test.mjs src/lib/chat-mentions.test.mjs  # 必须 10/10 PASS
```

### 4.3 真实 TiDB 集成与业务回归门禁（`artex_dev`）
```bash
ARTEX_TIDB_DSN="artex:2fq_n1lVpdVBj1vmo@tcp(192.168.27.79:6000)/artex_dev?..." go test -v ./... -count=1
```
- **业务测试真实验收**：各阶段迁移完成后，对应的业务测试在 `ARTEX_TIDB_DSN` 开启时必须**真实执行且 PASS**（禁止将业务 CRUD 测试永久 skip 伪造通过）；
- **全链路场景**：覆盖系统启动、Viper 配置解析、资产 8 分支写入与 hash 仲裁、任务推进、死锁免疫防线、级联闭包删除与归档恢复；
- **残留口径**：测试结束后，持久 65 表完整保留，`locks` 仅保留 1 行 seed，`maintenance` 无残留 owner，测试产生的临时业务数据完全清理干净。

### 4.4 独立 Schema 冒烟门禁（`artex` 独占空库）
```bash
ARTEX_TEST_TIDB_DSN="artex:2fq_n1lVpdVBj1vmo@tcp(192.168.27.79:6000)/artex?..." go test -v ./db/schema/ -run 'TestSmokeRealTiDBDDL|TestVerify_RealTiDB_Structure' -count=1
```
- 验证从零建 65 表成功；测试结束后 `artex` 库的 BASE TABLE 数量必须准确回到 **0**。
