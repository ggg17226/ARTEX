# ARTEX 原生 TiDB 8.5 架构迁移与生产部署技术规格说明书 (P08 & Native TiDB Migration Spec)

> 日期：2026-10-18
> 状态：APPROVED (基于用户多轮架构审查确认)
> 目标：彻底切断 PostgreSQL 依赖，全仓接入原生 TiDB 8.5，升级配置机制为 Viper+YAML，完成 43 个业务文件的方言转换与级联闭包删除接线。

---

## 1. 目标与背景

由于当前部署环境无 PostgreSQL 可用，ARTEX 必须从当前的过渡状态（底层 65 表/锁机制/发号器/API 契约已 TiDB 化，但业务 CRUD 仍使用 pgx 及 PG 方言）彻底升级为**纯原生 TiDB 8.5 运行时**。

### 核心指标
1. **配置现代化**：采用 Viper 管理，配置文件统一为 `config.yaml`，原生支持环境变量映射与连接串自动装配。
2. **纯 TiDB 连接**：完全移除 `github.com/jackc/pgx/v5` 及其全部依赖，底层统一使用已验收的 `db/tidb.OpenTiDB`（`*sql.DB` + 32 连接池 + 能力探测）。
3. **运维与启动工具**：提供 `artex db-migrate` 子命令体系（`--prepare`, `--verify`, `--status`），替代临时脚本；启动时自动校验 65 表结构基线。
4. **全量方言重构**：消除 43 个业务文件中 693 处 PG SQL 方言，严格遵守 R4 最小改写规则。
5. **完整闭包接线**：将已验收的 `cascade_executor.go` 接管生产删除链路，消除对 PG 数据库级物理外键级联与表锁的依赖。
6. **归档与物理隔离**：归档快照建立显式字段白名单，杜绝内部隐藏物理主键 `row_id` 泄漏到 manifest 中。

---

## 2. 系统架构与分阶段实施

为保证工程可控、低风险，全流程分为四个串行阶段推进：

```
[阶段 1: 核心骨架与运行时]
  - config/ 升级为 Viper + config.yaml
  - cmd/artex/ 接入 db-migrate CLI
  - db/db.go 切除 pgx，接入 tidb.OpenTiDB，重构内置种子
       │
       ▼
[阶段 2: 核心业务主干]
  - db/assets.go 8 分支 Canonical Identity + Hash 写入与 ON DUPLICATE KEY UPDATE
  - db/companies.go & company_scope.go 方言转换
  - db/tasks.go 接线 cascade_executor & task_closure
       │
       ▼
[阶段 3: 探索与执行引擎]
  - db/exploration.go (节点/活动/边) 消除 $1、RETURNING 与 DISTINCT ON
  - db/findings.go & db/finding_assets.go 聚合重构 (COUNT FILTER -> CASE WHEN)
  - db/commands.go 消除启动旧 DDL
       │
       ▼
[阶段 4: 辅助模块与收尾]
  - db/intercept*.go & db/notification*.go (CAS 租约替代 SKIP LOCKED)
  - db/task_archives*.go (显式列投影剔除 row_id)
  - 全仓清理 pgx 依赖，go mod tidy 瘦身
```

---

## 3. 详细设计规范

### 3.1 阶段 1：配置、运行时与 CLI

#### 3.1.1 配置机制（`config/config.go`）
- 引入 `github.com/spf13/viper`，使用 `config.yaml` 替代 `config.json`；
- 寻址优先级：
  1. `ARTEX_CONFIG` 指定的绝对/相对路径；
  2. `./config.yaml`（当前工作目录）；
  3. `config.BaseDir()/config.yaml`（可执行文件所在目录）。
- 环境变量自动映射：
  - 前缀：`ARTEX`（自动映射形如 `ARTEX_DATABASE_DSN`）；
  - 显式兼容映射：优先读取现有测试广泛使用的 `ARTEX_TIDB_DSN`。
- YAML 结构：
  ```yaml
  database:
    dsn: "artex:pass@tcp(192.168.27.79:6000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'"
    host: "192.168.27.79"
    port: 6000
    user: "artex"
    password: "..."
    dbname: "artex_dev"
  skill_dir: "./skills"
  ```
- 接口定义：
  - `config.TiDBDSN() (dsn, source string, err error)`：若 `dsn` 字段提供则直接返回，否则由散列字段组装，自动补齐 `charset`, `parseTime`, `loc`, `time_zone`。

#### 3.1.2 启动与数据源重构（`db/db.go`）
- 移除 `_ "github.com/jackc/pgx/v5/stdlib"` 与 `pgconn`；
- `db.Open(dsn)`：
  1. 调用 `tidb.OpenTiDB(dsn)` 获取经过能力探测并限制 32 并发连接的 `*sql.DB`；
  2. 调用 `schema.Verify(ctx, sqlDB)` 校验 65 表完整性；若表缺失，明确报错并引导用户运行 `artex db-migrate --prepare`；
  3. 种子数据初始化（`seedBuiltins`）：移除旧 SQL 嵌入，改写内置 agent、prompt_vars、mcp_servers 等插入语句为 `?` 方言，计算 `key_identity` / `key_hash`，使用 `ON DUPLICATE KEY UPDATE` 保证幂等。

#### 3.1.3 迁移命令行（`cmd/artex`）
- 在 `artex` 入口增加 `db-migrate` 命令处理：
  - `artex db-migrate --prepare`：调用 `schema.GenerateStatements()` 与 `schema.ApplyDDL()`，幂等创建 65 表基线与 infra seed；
  - `artex db-migrate --verify`：对比 catalog baseline 与目标库 `information_schema`；
  - `artex db-migrate --status`：输出集群版本、sql_mode、当前 BASE TABLE 数量及 maintenance 锁持有状态。
- `server/manager.go`：删除启动时调用 `EnsureLLMRecordsTable` 与 `EnsureLLMUsageTable` 执行旧 DDL 的逻辑。

---

### 3.2 阶段 2：资产与任务主干重构

#### 3.2.1 资产表 8 分支 Hash 唯一键与 Upsert（`db/assets.go`）
- 覆盖 8 分支：`root_domain`, `ip`, `subdomain`, `app_bundle`, `app_name`, `service_http`, `service_other`, `endpoint`；
- 写入逻辑：调用 `schema.Identity*` 生成对应的 `identity_*`（MEDIUMBLOB）与 `hash_*`（BINARY(32)）；
- 消除 `RETURNING id`：使用 `ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), ...`，并通过 `res.LastInsertId()` 获取稳定主键；
- 复杂列存储：`task_ids`, `bound_domains`, `open_ports`, `technologies`, `auth`, `params` 统一使用 `json.Marshal` 生成合法的 JSON 文本写入，废除 PG 数组字面量 `{...}`。

#### 3.2.2 任务管理与级联闭包删除（`db/tasks.go`）
- 移除 `LOCK TABLE tasks IN SHARE ROW EXCLUSIVE MODE`；
- 任务删除正式接线 `db/cascade_executor.go`：
  1. 调用 `ComputeTaskClosure` 计算待删除的所有依赖项（子任务、意图、漏洞、资产关联等）；
  2. 在 `db/lockorder` 锁序保护下逐一加悲观锁；
  3. 按严格逆拓扑顺序物理删除，确保无孤儿数据残留。

---

### 3.3 阶段 3：探索引擎与漏洞管理

#### 3.3.1 探索图谱与会话（`db/exploration.go`）
- 将 95 处 SQL 中的 `$1, $2` 替换为 `?`，按出现顺序解构绑定参数；
- 移除节点/边/活动的 `RETURNING id`，改用 `LastInsertId()`；
- 会话最新步查询：废除 `DISTINCT ON (session) ... ORDER BY session, id DESC`，统一改写为派生表窗口函数：
  ```sql
  SELECT ... FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY session ORDER BY id DESC) AS rn
      FROM activity WHERE exploration_id = ?
  ) t WHERE t.rn = 1
  ```
- 移除 `pg_advisory_xact_lock`。

#### 3.3.2 漏洞与统计（`db/findings.go`）
- `FindingStats` 聚合查询：将 `COUNT(*) FILTER (WHERE ...)` 改为 `COUNT(CASE WHEN ... THEN 1 END)`，保证空结果返回 0；
- 参数集合查询改用动态拼接的 `IN (?, ?, ...)`。

---

### 3.4 阶段 4：辅助模块与收尾清理

#### 3.4.1 安全拦截与通知（`db/intercept*.go` & `db/notification*.go`）
- 审批流与审计记录改用 `?` 方言；
- 批次汇总推送（`ClaimDigestBatch`）：使用事务 CAS 租约更新（`UPDATE notification_delivery SET state='sending', lease_expires_at=? WHERE state='pending' LIMIT ?`）替代不支持的 `SKIP LOCKED` 语义。

#### 3.4.2 任务归档与 manifest 物理隔离（`db/task_archives*.go`）
- 消除 `row_to_json` 与 `SELECT *`；
- 建立严格的**显式字段白名单投影**，物理自增主键 `row_id` 严禁出现在归档 JSON / manifest 中；
- 恢复（Restore）时重新分配物理 `row_id`。

#### 3.4.3 依赖清理
- `go mod tidy` 彻底移除 `github.com/jackc/pgx/v5`。

---

## 4. 验收与质量门限

每个阶段的提交必须满足以下双门禁：
1. **静态代码与编译门禁**：
   - `go build ./...` 0 错误；
   - `go vet ./...` 0 警告；
   - `gofmt -l` 干净。
2. **测试与回归门禁**：
   - 全仓离线测试 `go test ./...` 100% PASS；
   - 前端 `cd web && npx tsc --noEmit` 保持 0 错误；
   - 真实 TiDB 集群（`artex_dev`）端到端集成测试通过，测试后集群元数据与业务表 0 残留。
