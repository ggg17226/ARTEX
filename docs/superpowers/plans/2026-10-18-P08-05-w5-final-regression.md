# P08-05 实施计划：W5 全量集成回归验证与质量门禁

> **计划编号**：`P08-05`  
> **所属阶段**：阶段 5（终态全量回归验证与交付）  
> **关联 Spec**：`docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md` §4  
> **目标**：在完成阶段 1 至阶段 4 全部代码重构与单测绿化后，执行最终端到端全量回归验收。涵盖离线环境彻底净化门禁、前端对齐门禁、独占空库 Schema 冒烟验证，以及真实 TiDB 业务库的全量端到端场景回归与数据清理口径验证。

---

## 门禁 1：离线纯净化全量门禁（不依赖外部数据库，彻底阻断 YAML 默认回落）

### 目标与要求
- 确保代码在未配置任何数据库环境变量的情况下，能够正常构建、静态检查通过，并且单元测试全部正常运行。
- 诚实跳过：外部 DB 相关测试必须通过 `t.Skip` 明确声明跳过，单元测试必须 100% 执行且 PASS。
- 使用严格的子 shell 隔离并 `unset` 全部相关环境变量，同时注入 `export ARTEX_CONFIG=/dev/null`，彻底阻断从当前目录或二进制目录加载默认 `config.yaml`。

### 执行步骤与验收命令

```bash
# 1. 离线全量构建检查
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
# 判定标准：退出码 0，无编译错误

# 2. 离线全量静态诊断检查
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
# 判定标准：退出码 0，0 警告，0 错误

# 3. 离线全量单元测试（排除尝试主动建立本地网络连接的孤立无效测试）
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./... -count=1 -skip '^TestAcquireMaintenance_InvalidOpID$')
# 判定标准：PASS，所有单元测试 100% 运行通过，无 panic 或意外失败
```

---

## 门禁 2：前端类型与交互逻辑对齐门禁

### 目标与要求
- 保证前端 Next.js / TypeScript 代码与重构后的后端数据库/API 字段定义保持绝对一致。
- 保证前端提及与活动合并等关键算法测试 100% 通过。
- 使用受控子 shell，防止目录漂移影响后续终端命令执行。

### 执行步骤与验收命令

```bash
(cd web && npx tsc --noEmit --incremental false && node --test src/lib/activity-merge.test.mjs src/lib/chat-mentions.test.mjs)
# 判定标准：TypeScript 0 错误，单元测试 10/10 PASS
```

---

## 门禁 3：独占空库 Schema 冒烟门禁（`artex` 独占空库）

### 目标与要求
- 验证从零开始执行全部 65 表 DDL 的完整性、外键约束缺省特性与索引正确性。
- 测试必须以独占空库作为目标，测试时显式清理掉业务数据库环境变量（`unset ARTEX_TIDB_DSN`），杜绝串库。
- 测试结束后空库的 BASE TABLE 数量必须准确回到 **0**。

### 执行步骤与验收命令

```bash
# 环境变量注入独占空库 DSN，同时清除业务库 DSN
(unset ARTEX_TIDB_DSN && ARTEX_TEST_TIDB_DSN="artex:PASSWORD@tcp(127.0.0.1:4000)/artex?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'" \
go test -v ./db/schema/ -run 'TestSmokeRealTiDBDDL|TestVerify_RealTiDB_Structure' -count=1)
# 判定标准：PASS，且 artex 库 BASE TABLE 数量重置归零
```

---

## 门禁 4：真实 TiDB 业务库全量端到端回归门禁（`artex_dev`）

### 目标与要求
- 在已通过 `artex db-migrate --prepare` 部署基准架构的真实 TiDB 业务库上，全量运行后端业务测试套件。
- 测试必须在独占、干净的专用测试业务库上执行（仅包含 65 表结构与必要种子），严禁存在外部并发 server、worker 或 dispatcher 产生脏数据或抢占租约。
- 测试时显式清理掉独占空库环境变量（`unset ARTEX_TEST_TIDB_DSN`），防止两个测试环境互相干扰。
- 业务测试真实验收：核心 CRUD 与业务测试在 `ARTEX_TIDB_DSN` 开启时必须 100% 真实执行并通过（禁止伪造全局 Skip）。
- 验证全流程：
  1. 系统启动与 Viper 配置解析（含 YAML 与环境变量）；
  2. 纯只读元数据基线核验（`schema.VerifyBaseline`）；
  3. 资产 8 分支写前仲裁写入与 `schema.VerifyHashArbitration` 仲裁；
  4. 探索图谱推进与真实窗口函数去重；
  5. 全写者 Runner Gate 与父锁保护，级联闭包删除与漂移重试；
  6. 通知队列两阶段 CAS 抢占与 Fencing 状态转移；
  7. 归档快照导出（排除 `row_id`）与导入恢复。
- 验收残留口径：测试结束后，持久 65 表完整保留，`locks` 仅保留 1 行 seed，`maintenance` 无残留 owner，业务临时数据彻底清理干净。

### 执行步骤与验收命令

```bash
# 注入业务数据库 DSN，同时清除空库 DSN
(unset ARTEX_TEST_TIDB_DSN && ARTEX_TIDB_DSN="artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'" \
go test -v ./... -count=1)
# 判定标准：全仓业务测试 100% 真实执行并通过，退出码 0
```

---

## 最终交付验收核查表

- [ ] 1. 43 个产品业务文件无一遗漏完成方言改造；
- [ ] 2. `go.mod` 中无任何 `github.com/jackc/pgx/v5` 依赖；
- [ ] 3. 门禁 1、门禁 2、门禁 3、门禁 4 全部 PASS；
- [ ] 4. 真实数据库连接与元数据基线核验纯只读通过；
- [ ] 5. 测试结束后无残留孤儿进程或脏数据。
