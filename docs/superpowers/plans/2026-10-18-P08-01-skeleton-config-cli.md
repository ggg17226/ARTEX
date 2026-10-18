# P08-01 实施计划：核心骨架、Viper 配置与运维 CLI

> **计划编号**：`P08-01`  
> **所属阶段**：阶段 1（核心骨架、配置、CLI 与启动闭环）  
> **关联 Spec**：`docs/superpowers/specs/2026-10-18-native-tidb-p08-and-migration-design.md` §3.1  
> **目标**：引入 `github.com/spf13/viper` 管理 `config.yaml`，实现 DSN 规范化与脱敏；实现纯只读元数据基线检查器 `schema.VerifyBaseline`；提供 `cmd/artex` `db-migrate` 运维子命令体系；重构 `db/db.go` 切断 `pgx` 驱动，并原生化启动配置、设置与拦截规则种子 CRUD（覆盖 `db/settings.go`, `db/config.go`, `db/asset_intercept.go`, `db/intercept.go`）。

---

## 涵盖的产品与支持文件清单

| 文件路径 | 类型 | R4 方言数 | 核心职责与 M 项 |
|---|---|---:|---|
| `config/config.go` | 支持文件 | 0 | Viper 加载 `config.yaml`，环境变量绑定与 DSN 规范化 |
| `cmd/artex/main.go` | 支持文件 | 0 | CLI 分派入口，启动前优先处理 `db-migrate` 子命令 |
| `cmd/artex/migrate.go` | 支持文件 | 0 | `db-migrate` 命令实现（`--prepare`, `--verify`, `--status`） |
| `db/db.go` | 产品文件 1/43 | 15 | 切除 pgx，统一使用 `tidb.OpenTiDB`，废除旧 schema 嵌入（M01, M02, M37） |
| `db/settings.go` | 产品文件 2/43 | 4 | 设置项 CRUD，补齐 `key_identity` 与 `key_hash`（M11） |
| `db/config.go` | 产品文件 3/43 | 63 | 全量配置与模型/代理 CRUD，补齐 hash 并支持引用安全清理（M08, M23） |
| `db/asset_intercept.go` | 产品文件 4/43 | 4 | 资产拦截规则 CRUD 与种子数据原生化（M07） |
| `db/intercept.go` | 产品文件 5/43 | 12 | 拦截规则 CRUD 与审批状态机 TiDB 方言化（M07） |

---

## Task 1.1: 引入 Viper 与 `config/config.go` 重构

### 任务定义
- **修改文件**：`go.mod`, `go.sum`, `config/config.go`
- **新建测试**：`config/config_test.go`
- **接口契约**：
  - `config.TiDBDSN() (dsn, source string, err error)`
  - `config.Load() (Config, error)`
  - 依赖：`github.com/spf13/viper`, `github.com/Autumn-27/artex/db/tidb`

### 实施步骤与 TDD 验证

- [ ] **Step 1: 引入 `github.com/spf13/viper` 依赖**
  ```bash
  go get github.com/spf13/viper@v1.19.0 && go mod tidy
  ```

- [ ] **Step 2: 编写失败测试（`config/config_test.go`）**
  ```go
  package config

  import (
      "strings"
      "testing"
      "github.com/Autumn-27/artex/db/tidb"
  )

  func TestTiDBDSN_EnvPrecedence(t *testing.T) {
      rawWant := "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'"
      wantNormalized, err := tidb.NormalizeDSN(rawWant)
      if err != nil {
          t.Fatalf("NormalizeDSN failed: %v", err)
      }
      t.Setenv("ARTEX_TIDB_DSN", rawWant)
      got, src, err := TiDBDSN()
      if err != nil {
          t.Fatalf("TiDBDSN returned error: %v", err)
      }
      if got != wantNormalized {
          t.Fatalf("expected normalized DSN %s, got %s", wantNormalized, got)
      }
      if !strings.Contains(src, "ARTEX_TIDB_DSN") {
          t.Fatalf("expected env source, got %s", src)
      }
  }

  func TestTiDBDSN_RejectPostgres(t *testing.T) {
      t.Setenv("ARTEX_TIDB_DSN", "postgres://user:pass@127.0.0.1:5432/artex")
      _, _, err := TiDBDSN()
      if err == nil || !strings.Contains(err.Error(), "postgres") {
          t.Fatalf("expected rejection of postgres DSN, got %v", err)
      }
  }
  ```

- [ ] **Step 3: 运行测试验证见红（FAIL）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./config/... -run 'TestTiDBDSN' -count=1)
  ```
  预期结果：FAIL（`TiDBDSN` 未定义）。

- [ ] **Step 4: 实现 `config/config.go`**
  初始化 Viper，绑定 `ARTEX_` 环境变量前缀；设置寻址优先级；优先读取 `ARTEX_TIDB_DSN`；拒绝 `postgres://`；统一通过 `tidb.NormalizeDSN`。

- [ ] **Step 5: 运行测试验证见绿（PASS）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./config/... -count=1)
  ```

---

## Task 1.2: 实现纯只读元数据基线检查器 `schema.VerifyBaseline`

### 任务定义
- **新建文件**：`db/schema/verify_baseline.go`
- **新建测试**：`db/schema/verify_baseline_test.go`
- **接口契约**：
  - `schema.VerifyBaseline(ctx context.Context, db *sql.DB) error`
  - 依赖：`information_schema.tables`, `columns`, `statistics`, `github.com/Autumn-27/artex/db/schema.AllTables()`

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写失败测试（`db/schema/verify_baseline_test.go`）**
  ```go
  package schema

  import (
      "context"
      "testing"
  )

  func TestVerifyBaseline_NilDBFails(t *testing.T) {
      ctx := context.Background()
      if err := VerifyBaseline(ctx, nil); err == nil {
          t.Fatalf("expected error for nil db, got nil")
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./db/schema/... -run 'TestVerifyBaseline' -count=1)
  ```

- [ ] **Step 3: 实现 `db/schema/verify_baseline.go`**
  实现纯只读核查，校验 65 表、729 列、UNSIGNED、NOT NULL、生成列与 62 个 UNIQUE 索引，容忍 21 处 DESC 索引差异。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./db/schema/... -run 'TestVerifyBaseline' -count=1)
  ```

---

## Task 1.3: 实现运维 CLI 子命令体系 `cmd/artex db-migrate`

### 任务定义
- **修改文件**：`cmd/artex/main.go`
- **新建文件**：`cmd/artex/migrate.go`
- **新建测试**：`cmd/artex/migrate_test.go`

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写 CLI 分派测试（`cmd/artex/migrate_test.go`）**
  ```go
  package main

  import (
      "testing"
  )

  func TestMigrateCommand_ParseArgs(t *testing.T) {
      cmd, err := parseMigrateCommand([]string{"db-migrate", "--status"})
      if err != nil {
          t.Fatalf("parse failed: %v", err)
      }
      if cmd != "status" {
          t.Fatalf("expected status, got %s", cmd)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./cmd/artex/... -run 'TestMigrate' -count=1)
  ```

- [ ] **Step 3: 实现 `cmd/artex/migrate.go` 并在 `main.go` 挂接**
  实现 `--prepare`（建表、种子、只读验证并在同一事务更新 `schema_meta` 键值锁定：`UPDATE schema_meta SET value = 1, updated_at = NOW(3) WHERE key = 'schema_version' AND value = 0`）、`--verify`、`--status`。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./cmd/artex/... -count=1)
  ```

---

## Task 1.4: 重构 `db/db.go`、种子数据与拦截规则 CRUD 原生化

### 任务定义
- **修改文件**：`db/db.go`, `db/settings.go`, `db/config.go`, `db/asset_intercept.go`, `db/intercept.go`
- **修改测试**：`db/db_test.go`, `db/config_test.go`
- **核心职责**：解决 M01, M02, M07, M08, M11, M23, M37。移除 `db_test.go` 中的永久 `t.Skip`，确保在有 DSN 时真实执行；无 DSN 时通过 `t.Skip` 明确声明。

### 实施步骤与 TDD 验证

- [ ] **Step 1: 编写设置项原生持久化语法测试（`db/config_test.go`）**
  ```go
  package db

  import (
      "fmt"
      "os"
      "testing"
      "time"
  )

  func TestSettings_GetSetting_TiDBRequirement(t *testing.T) {
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
      
      testKey := fmt.Sprintf("p08_setting_%d", time.Now().UnixNano())
      testVal := "p08_val"
      if err := d.SetSetting(testKey, testVal); err != nil {
          t.Fatalf("SetSetting failed: %v", err)
      }
      t.Cleanup(func() {
          if _, err := d.Exec("DELETE FROM settings WHERE `key` = ?", testKey); err != nil {
              t.Errorf("cleanup setting failed: %v", err)
          }
      })

      val, ok, err := d.GetSetting(testKey)
      if err != nil || !ok || val != testVal {
          t.Fatalf("GetSetting failed: got (%q, %v, %v), want (%q, true, nil)", val, ok, err, testVal)
      }
  }
  ```

- [ ] **Step 2: 运行测试见红（FAIL）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestSettings_GetSetting_TiDBRequirement' -count=1)
  ```

- [ ] **Step 3: 实施代码改写**
  - 改写 `db/db.go` 切除 pgx；
  - 改写 `db/settings.go`, `db/config.go`, `db/asset_intercept.go`, `db/intercept.go` 为纯 TiDB 方言并补齐 identity/hash；
  - 改造 `db/db_test.go` 移除永久 Skip，当有 `ARTEX_TIDB_DSN` 时必须真实连接并 PASS。

- [ ] **Step 4: 运行测试见绿（PASS）**
  ```bash
  (export ARTEX_CONFIG=/dev/null ARTEX_TIDB_DSN="${ARTEX_TIDB_DSN:?required for integration test}" && go test ./db/... -run 'TestSettings_GetSetting_TiDBRequirement' -count=1)
  ```

---

## 阶段 1 验收门禁

```bash
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go build ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go vet ./...)
(unset ARTEX_TIDB_DSN ARTEX_TEST_TIDB_DSN ARTEX_CONFIG ARTEX_DATABASE_DSN ARTEX_DATABASE_HOST ARTEX_DATABASE_PORT ARTEX_DATABASE_USER ARTEX_DATABASE_PASSWORD ARTEX_DATABASE_NAME ARTEX_DATABASE_DBNAME && export ARTEX_CONFIG=/dev/null && go test ./config/... ./cmd/artex/... ./db/schema/... -count=1)
```
