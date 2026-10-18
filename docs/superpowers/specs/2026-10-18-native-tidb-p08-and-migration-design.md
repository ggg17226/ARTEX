# ARTEX 原生 TiDB 8.5 架构迁移与生产部署技术规格说明书 (P08 & Native TiDB Migration Spec)

> 日期：2026-10-18（第 8 版终审稿，彻底闭环 R37 全部审查项）  
> 状态：APPROVED  
> 目标：彻底切断 PostgreSQL (`pgx`) 依赖，全仓原生接入 TiDB 8.5，升级配置机制为 Viper+YAML，完成全部 43 个业务文件（693 处方言 SQL 与 M01–M37 语义项）的方言转换与全写者锁序级联闭包删除接线。

---

## 1. 目标、范围与非目标

### 1.1 核心目标
1. **纯 TiDB 原生运行时**：全仓移除 `github.com/jackc/pgx/v5` 及其全部依赖，底层统一收敛至已验收的 `db/tidb.OpenTiDB`（标准 `*sql.DB` + 32 连接池 + 能力探测 + UTC 会话时区）。
2. **配置机制现代化**：基于 `github.com/spf13/viper` 管理，配置文件统一为 `config.yaml`，原生支持环境变量映射与连接串自动组装。
3. **运维与启动工具**：提供 `cmd/artex` `db-migrate` 子命令体系（`--prepare`, `--verify`, `--status`），替代所有临时建表脚本；启动时执行纯只读元数据基线校验（`schema.VerifyBaseline`）。
4. **全量 43 产品文件方言重构**：消除 43 个业务产品文件中的 693 处 PG SQL 方言与 M01–M37 语义项（严格依据 `r4-pg-dialect-audit.md` 单一真源）。
5. **完整级联闭包与全写者冻结**：接入 `db/cascade_executor.go` 与 `db/lockorder`，写者使用 `TxRunner` 获取全局 Business Gate 与父任务行锁，删除者执行全序悲观锁与成员漂移回滚，消除孤儿与死锁。
6. **资产 8 分支写前仲裁协议**：规范 8 分支输入谓词矩阵；采用“写前仲裁与不可变身份列”两阶段协议，杜绝绕过物理 PK 预锁或覆盖自证；接入 `schema.VerifyHashArbitration` 严格比对持久化身份。
7. **通知 CAS 租约状态机与代次 Fencing**：以 `notification_deliveries` 和 `next_attempt_at` 为基础实现租约抢占，回填时严格绑定 `next_attempt_at = assignedDeadline` 作为租约版本令牌，杜绝迟到 ACK 覆盖重领。
8. **归档真实 Catalog 双白名单**：建立严格对齐真实数据库 catalog 的 7 表导出与导入白名单，严格排除物理主键 `row_id` 与生成列；核心图实体冲突采取 fail-closed 防线。

### 1.2 非目标（明确声明）
- **存量数据搬迁**：本次工程聚焦于代码实现、新环境 DDL 部署及全新 TiDB 集群的运行。历史生产 PostgreSQL 数据导出、清洗、回填到 TiDB 的搬迁脚本属于独立运维任务，不包含在本次代码仓库改造范围内。`artex db-migrate --prepare` 仅用于新实例初始化，不做存量数据导入。
- **TiDB 集群外部参数修改**：严禁向 TiDB 执行任何 `SET GLOBAL` 或修改系统变量操作；所有功能完全适配 TiDB 8.5 严格模式默认参数（`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES` 等）。
- **外部真实内网凭据**：文档与测试用例中一律杜绝真实内网 IP 与明文密码，统一使用通用模板或 `127.0.0.1:4000` / `PASSWORD` 占位符。

---

## 2. 43 个产品业务文件全量迁移矩阵（四阶段严格映射）

依据 `r4-pg-dialect-audit.md` 清点的全部 **43 个产品业务文件**（精准 693 处方言 SQL 与 M01–M37 语义项），建立无缝、零遗漏的四阶段归属映射表。额外 6 个支持/非产品 SQL 文件另列说明，不计入 43 个产品文件：

### 2.1 43 个产品业务文件全量映射表（单一真源）

| 阶段 | 阶段主题 | 包含的产品文件清单（精准 43 文件无遗漏） | R4 Blocker 数 | 对应 M 项与核心职责 |
|---|---|---|---:|---|
| **阶段 1** | **核心骨架、配置、CLI 与启动闭环** | 1. `db/db.go`<br>2. `db/settings.go`<br>3. `db/config.go`<br>4. `db/asset_intercept.go`<br>5. `db/intercept.go` | 15<br>4<br>63<br>4<br>12<br>（小计：98） | **M01, M02, M07, M08, M11, M23, M37**：切除 pgx 驱动与旧 schema 嵌入；改用 `tidb.OpenTiDB` 与 `schema.VerifyBaseline`；改造 `seedBuiltins` 补齐 identity/hash；重构配置与拦截规则 CRUD；支持配置删除时引用安全清理。 |
| **阶段 2** | **核心业务主干：资产、任务与级联闭包** | 6. `db/assets.go`<br>7. `db/asset_dsl.go`<br>8. `db/companies.go`<br>9. `db/tasks.go`<br>10. `db/task_assets.go`<br>11. `db/task_assets_context.go`<br>12. `db/task_scope.go`<br>13. `db/task_context.go`<br>14. `db/task_categories.go`<br>15. `db/task_templates.go`<br>16. `db/task_intercept.go` | 28<br>19<br>23<br>28<br>11<br>9<br>10<br>18<br>12<br>4<br>6<br>（小计：168） | **M03, M04, M05, M09, M10, M14, M15, M16, M18, M19, M24, M26, M27, M30, M32, M34, M35, M36**：资产 8 分支 canonical identity 与 hash 写入，写前仲裁与 `VerifyHashArbitration`；企业与范围表 hash 转换；任务状态机与废除表锁；接线 `cascade_executor.go` 与 `assets.task_ids`/`task_asset_links` 双写；全写者父锁与漂移回滚；分类/模板/范围 TiDB 化。 |
| **阶段 3** | **探索图谱、意图推进与漏洞管理** | 17. `db/exploration.go`<br>18. `db/exploration_sources.go`<br>19. `db/findings.go`<br>20. `db/finding_assets.go`<br>21. `db/finding_retests.go`<br>22. `db/finding_traffic.go`<br>23. `db/finding_traffic_archive.go`<br>24. `db/commands.go`<br>25. `db/tools.go`<br>26. `db/llm_usage.go`<br>27. `db/llmhealth.go`<br>28. `db/logs.go`<br>29. `db/digest.go`<br>30. `db/intent_admission.go`<br>31. `db/tool_usage.go` | 95<br>2<br>28<br>7<br>13<br>24<br>2<br>15<br>12<br>5<br>2<br>3<br>15<br>2<br>1<br>（小计：226） | **M12, M21, M22, M29**：消除 95 处 exploration SQL 中的 `$1`、`RETURNING` 与真实窗口函数改造；移除启动期旧 DDL（`EnsureLLMRecordsTable`/`EnsureLLMUsageTable`）；`FindingStats` 聚合改用 `COUNT(CASE WHEN ... THEN 1 END)`；流量关联改造；UPDATE/DELETE 锁后回查替代 RETURNING。 |
| **阶段 4** | **辅助模块、会话、触发器、归档与收尾** | 32. `db/conversation.go`<br>33. `db/triggers.go`<br>34. `db/chat_mentions.go`<br>35. `db/notification.go`<br>36. `db/notification_delivery.go`<br>37. `db/task_archives.go`<br>38. `db/task_archives_restore.go`<br>39. `db/skill_usage.go`<br>40. `db/side_questions.go`<br>41. `db/constraints.go`<br>42. `db/intercept_detail.go`<br>43. `db/intercept_execution.go` | 16<br>13<br>1<br>13<br>15<br>56<br>52<br>4<br>20<br>4<br>3<br>4<br>（小计：201） | **M06, M13, M17, M20, M25, M28, M31, M33**：会话与触发器方言 TiDB 化；通知系统改用 CAS 租约状态机（带 Fencing 令牌）；归档快照建立真实列双白名单排除 `row_id`；拦截详情与执行方言改造；从 `go.mod` 彻底清除 `pgx` 依赖。 |
| **总计** | **全 4 阶段** | **43 个产品业务文件** | **693** | **覆盖全部 M01–M37 语义项** |

### 2.2 额外支持文件清单（不计入 43 个产品 SQL 文件）
1. `cmd/artex/main.go`（CLI 入口分派）
2. `config/config.go`（Viper 配置加载与规范化）
3. `db/company_scope.go`（范围辅助逻辑，无独立 PG 方言）
4. `db/constants.go`（数据库常量定义，无独立 PG 方言）
5. `db/nkey.go`（NKey 辅助计算，无独立 PG 方言）
6. `db/task_archive_aggregate_stats.go`（归档统计结构定义，无独立 PG 方言）

---

## 3. 核心技术设计与架构规范

### 3.1 阶段 1：配置机制、纯 TiDB 启动与运维 CLI

#### 3.1.1 Viper 配置架构与 DSN 归一化
1. **文件寻址优先级**：
   1. 环境变量 `ARTEX_CONFIG`（若设置则必须存在，不存在或解析错误直接 fail-closed 退出，严禁静默回退）；
   2. `./config.yaml`（当前工作目录）；
   3. `config.BaseDir()/config.yaml`（二进制所在目录）。
   - 若无配置文件，但环境变量完整提供了连接信息，允许无配置启动；若均未提供，报错退出。
2. **环境变量绑定与键映射**：
   - Viper 开启环境变量绑定，设置 EnvKeyReplacer 将 `.` 替换为 `_`（如 `database.dsn` 映射为 `ARTEX_DATABASE_DSN`）；
   - **最高优先级保留**：显式检查环境变量 `ARTEX_TIDB_DSN`，若非空则直接以此为基础；
   - 彻底废除 `ARTEX_PG_DSN` 与 `postgres://` 协议，若配置提供 `postgres://` 显式报错拒绝，严禁静默连错库。
3. **DSN 组装与统一规范化**：
   - 所有 DSN 必须最终调用 `db/tidb.NormalizeDSN()`，强制补充 `charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'`；
   - 默认端口为 TiDB 标准端口 4000；密码保留原始字符，严禁随意 TrimSpace；日志输出统一使用 `db/tidb.RedactDSN()` 脱敏。
   - **配置测试断言规范**：测试断言必须使用 `tidb.NormalizeDSN(want)` 进行比对，杜绝因 query 排序编码造成的错误断言。

#### 3.1.2 启动时序与纯只读元数据校验（`schema.VerifyBaseline`）
1. **切断 pgx 与连接池接线**：
   - 移除 `_ "github.com/jackc/pgx/v5/stdlib"`；
   - `db.Open(dsn)` 仅调用 `tidb.OpenTiDB(dsn)`，共享 32 最大连接池并完成基础能力探测（要求会话时区必须为 UTC）；
   - 移除旧内嵌 `schema.sql`、移除 PG Advisory Lock（`pg_advisory_lock`）与重试逻辑。
2. **纯只读 Baseline 比较器（`schema.VerifyBaseline`）**：
   - 纯只读元数据检查函数 `VerifyBaseline(ctx context.Context, db *sql.DB) error`（严禁执行任何写入探针）；
   - 从 `information_schema.tables`、`columns`、`statistics` 中核查 65 表是否存在、列数（729 列）、列类型、UNSIGNED、NOT NULL、生成列表达式与 62 个 UNIQUE 索引；
   - 容忍已知差异：对 21 处 DESC 索引容忍真实库的 ASC 等价存储；
   - 若校验失败，启动直接中止并提示用户运行 `artex db-migrate --prepare`。
3. **版本绑定生命周期（闭环 F10）**：
   - `PrepareInfraSQL` 写入初始行：`('schema_version', 0, NOW(3))`；
   - `artex db-migrate --prepare` 在 DDL 执行与元数据基线核验完全通过后，在同一事务中针对 key/value 结构执行原子更新锁定：
     ```sql
     UPDATE schema_meta
     SET `value` = 1, updated_at = NOW(3)
     WHERE `key` = 'schema_version' AND `value` = 0
     ```
   - 若影响行数不为 1，必须立即报错回滚，杜绝未绑定环境启动。

---

### 3.2 阶段 2：资产 8 分支 Hash 唯一键与写者/删除闭包协议

#### 3.2.1 资产表 8 分支规范化输入与写前仲裁协议
1. **8 分支规范化输入谓词矩阵**：
   - `root_domain`：`domain` 必须经过 `DomainKey` 规范化（小写、去空格），非空；输入调用 `schema.AssetsRootDomainIdentity(domain)`；
   - `ip`：`ip` 必须经过 Canonical IP 格式化（IPv4/IPv6 标准化文本），输入调用 `schema.AssetsIPIdentity(ip)`；
   - `subdomain`：`domain` 规范化，`recordType` 若为 NULL 转换为 `""`；输入调用 `schema.AssetsSubdomainIdentity(domain, recordType)`；
   - `app_bundle`：当 `bundleID != ""` 时激活，输入调用 `schema.AssetsAppBundleIdentity(bundleID)`；
   - `app_name`：当 `bundleID == ""` 时 fallback 激活（两者严格互斥，不可同时激活），输入调用 `schema.AssetsAppNameIdentity(appName)`；
   - `service_http`：`url` 经过 `NormalizeURL`（小写 scheme/host，移除默认端口），输入调用 `schema.AssetsServiceHTTPIdentity(url)`；
   - `service_other`：`domain` 与 `ip` 经过 COALESCE，`port` 必须在 1..65535 范围内，`serviceName` 经过 lower/trim 处理；输入调用 `schema.AssetsServiceOtherIdentity(domain, ip, port, portNull, serviceName, serviceNameNull)`；
   - `endpoint`：`url` 规范化，`method` 大写；输入调用 `schema.AssetsEndpointIdentity(url, method)`。
   - **严格列激活**：每次插入仅激活对应分支的 `identity_*` 与 `hash_*` 列，其余 7 个分支必须显式绑定为 `NULL`。
2. **两阶段写前仲裁与防自证更新协议（闭环 F02）**：
   为彻底杜绝“更新覆盖旧身份后再自证”的漏洞，写入严格遵循写前仲裁协议：
   - **路径 A（已知候选行更新）**：若在查询中发现候选物理 PK（`candidatePK > 0`）：
     1. 先按物理 PK 执行 `SELECT id FROM assets WHERE id = ? FOR UPDATE` 获得行级悲观锁；
     2. **写前身份仲裁**：在持锁保护下，先执行 `schema.VerifyHashArbitration`，核验当前持久化的 identity 与输入完全一致且返回 PK 等于 `candidatePK`；若发生碰撞（返回 `ErrIdentityHashCollision`），立即 `tx.Rollback()` 退出，绝不修改该行；
     3. **身份列不可变更新**：仲裁通过后，执行 `UPDATE assets SET ... WHERE id = ?`，**严禁在 UPDATE 语句中修改 `identity_*`、`hash_*` 及分支自然键列**，仅允许更新 `last_seen`、`technologies`、`company_id` 等非身份业务字段；
     4. 更新完成后再次核查，确保身份未发生漂移。
   - **路径 B（无候选行新插入）**：
     1. 执行严格的 `INSERT INTO assets(...) VALUES(...)`（不带 ODKU）；
     2. 若插入成功，通过 `res.LastInsertId()` 获得新行 PK（校验在 `[1, math.MaxInt64]` 范围），同事务执行 `VerifyHashArbitration` 验证自洽；
     3. 若插入遇到 MySQL 1062（主键或唯一索引冲突，说明存在并发插入赢家）：
        - 立即回滚当前尝试；
        - 在事务运行器重试循环中转入**路径 A**：此时候选行已存在，查询候选 PK 并获取行锁，重放写前仲裁与更新。
   - **错误处理**：任何死锁（1213）或锁超时（1205），由外层事务运行器退避重试。

#### 3.2.2 全体写者全局门禁与级联删除闭包接线
1. **全体写者 Runner Gate 与父锁协议**（闭环 F03）：
   - 全体写者与删除者统一接入 `db/lockorder/tx_runner.go` 执行，配置 `RequiresBusinessGate: true`，获取全局 Business Gate 保护；
   - 任何向任务追加子实体的写者，必须在事务内先通过 `lockorder` 锁住父任务行，核验父任务未被标记删除，杜绝跨任务挂载孤儿。
2. **删除者四阶段闭环协议与全物理副作用锁定**：
   1. **新鲜定位（Fresh Locator）**：在 Gate 保护下，调用 `ComputeTaskClosure` 收集关联子实体；
   2. **完整物理副作用转换与全序锁**：
      - 闭包实体转换：`side_question_sessions` 查询转换出物理 `row_id`；`AssetIDsToDetach` 识别出共享资产并从物理删除列表中剥离；
      - **跨根物理副作用闭包**：必须将即将被 `executor` 置空的跨根弱关联物理行（如其他 exploration 的 `activity.id`，其他任务的 `task_asset_links.row_id`）完整纳入锁计划，杜绝范围 DML 突破已锁集合；
      - 将所有 `EntityKey` 严格按照 `(TableOrder, PK 升序)` 进行排序；
      - 调用 `lockorder.LockEntitiesInOrder` 执行行级悲观锁加锁；
   3. **成员漂移核验（Drift Verification）**：持锁后重新复核成员集合。若发现新成员或更低 PK 行插入，严禁逆序追加补锁，必须抛出 `ErrMembershipDrift` 由 `TxRunner` 触发整体回滚并指数退避重试；
   4. **物理逆拓扑清理**：接入 `db/cascade_executor.go` 执行物理逆拓扑清理。
3. **生产删除选项分层与 unknown 挂起保护**：
   - `DeleteTaskOptions` 五项分层：
     - 数据库事务内：`DeleteAssets`（独占资产物理删除）、`DeleteFindings`（独占漏洞物理删除）、`DeleteLLMRecords`（删除记录，保留 `llm_usage` 账本）；
     - 存储文件层（提交后执行）：`DeleteTraffic`、`DeleteFiles`（提交前 staging，提交后 purge，回滚时补偿；若提交返回 `lockorder.ErrOutcomeUnknown` 则进入挂起对账保护，严禁补偿或重放）；
   - **共享资产保护**：若资产被其他活跃任务引用或被 anchor 引用，仅从 `assets.task_ids` 移除 task_id 并物理删除 `task_asset_links`，绝不物理删除资产；
   - **原子双写**：`assets.task_ids` 更新与 `task_asset_links` 增删必须在同一数据事务中显式原子同步。

---

### 3.3 阶段 3：探索引擎与漏洞管理 SQL 方言改造

#### 3.3.1 真实业务去重与窗口函数改造（消除 DISTINCT ON）
1. **消除伪 session 泛例，落实三大真实去重场景**：
   - **资产 open_ports 去重**（`db/assets.go`）：按 `port` 分组，`service` 非空优先（使用 TiDB 合法 JSON 路径 `$.port` 与 `$.service`）：
     ```sql
     ROW_NUMBER() OVER (
         PARTITION BY JSON_EXTRACT(elem, '$.port') 
         ORDER BY CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(elem, '$.service')) <> '' THEN 0 ELSE 1 END ASC
     )
     ```
   - **资产 params 去重**（`db/assets.go`）：按 `location, name` 分组，JSON 字符串 DESC 优先：
     ```sql
     ROW_NUMBER() OVER (
         PARTITION BY JSON_UNQUOTE(JSON_EXTRACT(elem, '$.location')), JSON_UNQUOTE(JSON_EXTRACT(elem, '$.name')) 
         ORDER BY elem DESC
     )
     ```
   - **企业资产关联去重**（`db/companies.go`）：按根域名长度 DESC、企业 ID ASC 优先；网络匹配在 Go 内存预筛选后在事务内原子归属：
     ```sql
     ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY LENGTH(cs.domain) DESC, cs.company_id ASC)
     ```
2. **95 处探索 SQL 方言展开**：
   - 将 `$1, $2` 替换为 `?`，按实际出现顺序严格重新对齐参数；
   - 移除节点、边、活动写入的 `RETURNING id`，统一通过 `res.LastInsertId()` 获取并校验；
   - 移除 `EnsureLLMRecordsTable` 与 `EnsureLLMUsageTable` 运行时旧 DDL。

---

### 3.4 阶段 4：辅助模块、通知队列、归档物理隔离与依赖切除

#### 3.4.1 通知队列 CAS 租约状态机与代次 Fencing 保护
1. **真实 Catalog 对齐**：
   - 真实表为 `notification_deliveries`，时间列为 `next_attempt_at`；
2. **两阶段 CAS 抢占与代次 Fencing**（闭环 F05）：
   - 阶段 1：在事务内执行有界候选锁定：
     ```sql
     SELECT id, channel_id, attempts FROM notification_deliveries
     WHERE (state = 'pending' AND next_attempt_at <= NOW(3))
        OR (state = 'sending' AND next_attempt_at < NOW(3))
     ORDER BY id ASC LIMIT ? FOR UPDATE
     ```
   - 阶段 2：执行原子状态更新，记录本次分配的租约截止时间 `assignedDeadline`：
     ```sql
     UPDATE notification_deliveries
     SET state = 'sending', next_attempt_at = ?, attempts = attempts + 1
     WHERE id IN (...)
     ```
   - **代次 Fencing 回填契约**：
     - `MarkDeliveriesSent`, `RescheduleDeliveries`, `DeferDeliveries`, `FailDeliveries` 必须携带租约截止时间作为精确代次令牌：
       ```sql
       UPDATE notification_deliveries
       SET state = 'sent', sent_at = NOW(3)
       WHERE id = ? AND state = 'sending' AND next_attempt_at = ?
       ```
     - 若租约已超时被 Worker B 重领，`next_attempt_at` 已被 Worker B 更新为新的截止时间，Worker A 迟到回填时匹配行数为 0，丢弃迟到 ACK，绝不覆盖新租约。

#### 3.4.2 任务归档真实 Catalog 双白名单与全图冲突裁决
1. **7 表真实列导出与写入双白名单（严格依据 Catalog 对齐，闭环 F06）**：
   导出与写入仅允许以下真实存在的白名单列（严格排除 `row_id` 与生成列）：
   - `task_relations`：`[task_id, source_task_id, created_at]`（无不存在的 `kind`）
   - `task_asset_links`：`[task_id, asset_id, source_node_id, source, source_summary, created_at, updated_at]`（完整保留来源信息）
   - `task_llm_profiles`：`[task_id, profile_id, position, status, last_error, exhausted_at, created_at, updated_at]`（无不存在的 `role`）
   - `exploration_edges`：`[exploration_id, src_id, dst_id, rel, created_at]`（对齐真实列名）
   - `exploration_anchors`：`[node_id, asset_id]`（对齐真实列名）
   - `side_question_sessions`：`[session_key, conversation_id, task_id, exploration_id, intent_id, run_id, version, snapshot, generation, memory]`（对齐真实结构，无不存在的 `status/created_at/updated_at`）
   - `traffic_evidence_snapshots`：`[id, source_traffic_id, captured_at, url, method, status, content_type, req_head, resp_head, req_hash, resp_hash, req_len, resp_len, unreferenced_at, created_at]`（对齐真实列名）
   - `side_question_requests`：严格排除生成列 `running_session_key`。
2. **全图实体冲突裁决**：
   - 核心图实体（task, exploration, nodes, findings）：采取 fail-closed 策略，主键冲突时报错拒绝（除非是恢复目标所属的合法归档 stub）；
   - 纯关联表与叶子：导入时缺省物理 `row_id`，由 TiDB 自动重新分配；
   - 导入前必须对带 hash 实体重新计算 canonical identity 与 hash，在事务内执行仲裁后再行插入。

---

## 4. 质量门禁与回归验收标准

全量迁移的验收必须严格区分三类不同执行环境，杜绝假绿：

### 4.1 离线全量门禁（不配置任何外部 DSN，使用安全子 shell 彻底净化环境，防止 YAML 回落）
```bash
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./... -count=1 -skip '^TestAcquireMaintenance_InvalidOpID$')
```
- 全仓 100% 编译通过，0 vet 诊断；单元测试全部 PASS。

### 4.2 前端对齐门禁
```bash
(cd web && npx tsc --noEmit --incremental false && node --test src/lib/activity-merge.test.mjs src/lib/chat-mentions.test.mjs)
```
- 前端 TypeScript 编译 0 错误；单元测试 10/10 PASS。

### 4.3 真实 TiDB 集成与业务回归门禁（业务库 `artex_dev`）
```bash
(unset ARTEX_TEST_TIDB_DSN && ARTEX_TIDB_DSN="artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'" go test -v ./... -count=1)
```
- 业务测试真实验收：各阶段迁移完成后，在 `ARTEX_TIDB_DSN` 开启时必须真实执行且 PASS；
- **专用业务库独占与干净性强制条款**：业务库 `artex_dev` 必须为**独占、干净的专用测试业务库**：初始状态仅含 65 表完整结构与必要种子数据（`locks` 初始恰好 1 行 seed）；测试执行期间严禁任何外部并发 server、worker、dispatcher（含被测业务进程的常驻实例）对该库产生脏数据或抢占 `locks`/`maintenance` 租约；执行前必须记录并核验空白/必要 seed 初态清单，被外部写者污染即视同环境违规，本轮门禁整体判红；
- **套件内部串行化与连接隔离**：测试套件内执行必须保持串行化（全局重算、通知 fan-out、归档扫描等触碰全局写集的用例禁止 `t.Parallel`，跨包含全局写者的测试包按包串行）或使用独立的连接/独立 DSN 隔离策略，杜绝套件内部并发写者互相覆盖租约或污染断言计数；
- **终态残留判据（违者测试标红）**：测试结束后持久 65 表完整保留，`locks` 仅保留 1 行 seed，`maintenance` 无残留 owner，业务临时数据彻底清理干净；任何残留 fixture、未归还的租约 owner 或外部脏数据对断言的污染，均必须将被污染的测试直接标红，禁止以“环境噪声”为由放行。

### 4.4 独立 Schema 冒烟门禁（独占空库 `artex`）
```bash
(unset ARTEX_TIDB_DSN && ARTEX_TEST_TIDB_DSN="artex:PASSWORD@tcp(127.0.0.1:4000)/artex?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'" go test -v ./db/schema/ -run 'TestSmokeRealTiDBDDL|TestVerify_RealTiDB_Structure' -count=1)
```
- 验证从零建 65 表成功；测试结束后空库的 BASE TABLE 数量必须准确回到 **0**。
