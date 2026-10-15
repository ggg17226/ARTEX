# P03 Task 2 修复轮 R1 实施报告

## 1. 任务背景与目标

依据审查报告 `.superpowers/sdd/2026-10-10-native-tidb-03-id-and-dto/task-2-review.md` 与修复简报 `.superpowers/sdd/2026-10-10-native-tidb-03-id-and-dto/task-2-fix1-brief.md`，实施 P03 Task 2 的 R1 修复工作。
BASE: `353b7a3aa2f7fa98f1bdf65d01100b9114aab4e7`。
修改范围严格限定于 `server/idalloc/boundary_test.go`。

## 2. 核心修复内容与代码证据

### I-01：补充必填列 `logical_key` 并覆盖后续自增断言
- **位置**：`server/idalloc/boundary_test.go` 中的 `TestTiDB_HighValueLastInsertIDZero`。
- **改动**：
  1. 显式插入 `9223372036854775813` 时，同时提供必填非空唯一列 `logical_key`（`"high_1"`），避免在规格要求的 `STRICT_TRANS_TABLES` 模式下因缺少默认值提前报错 1364；
  2. 随后执行自增插入 `INSERT INTO idalloc_t (logical_key) VALUES ('high_2')`，断言连续自增场景下获取的 `LAST_INSERT_ID()` 依然回读为 0，并断言 `AllocID` 返回 `ErrReadbackZero`，忠实覆盖 §11.1 实测事实。

### I-02：精准断言 Allocator 耗尽错误，杜绝假绿
- **位置**：`server/idalloc/boundary_test.go` 中的 `TestTiDB_MaxUint64RoundtripThenWrap`。
- **改动**：
  在第二次自增插入发生错误时，使用 `*mysql.MySQLError` 与错误文本进行显式校验：
  - 判断是否为 MySQL 错误码 1105（`mysqlErr.Number == 1105`）；
  - 或错误文本是否包含 `"auto increment action failed"`；
  - 若为其他非预期错误，直接执行 `t.Fatalf("unexpected insert error: %v", err)`，坚决杜绝假绿。

### M-01：连接池资源释放与清理可见性
- **位置**：`server/idalloc/boundary_test.go` 中的 `openTiDB` 及各 TiDB 测试用例的 `defer` 清理。
- **改动**：
  1. 在 `openTiDB(t)` 中，`sql.Open` 成功后立即注册 `t.Cleanup(func() { _ = db.Close() })`，保证连接池在测试生命周期结束时被完整关闭；
  2. 三个 TiDB 测试用例中的 `defer DROP TABLE` 清理逻辑若发生错误，使用 `t.Errorf("cleanup drop table: %v", err)` 记录，杜绝静默吞错。

## 3. 验证结果

所有命令严格在离线模式下执行（未配置 DSN，不读 `secret/`，不连接真实库）：

| 命令 | 退出码 | 结果概要 |
| --- | --- | --- |
| `gofmt -l server/idalloc/` | 0 | 格式完全符合要求，无输出 |
| `go vet ./server/idalloc/...` | 0 | 静态检查通过，无诊断警告 |
| `env -u ARTEX_TEST_TIDB_DSN -u ARTEX_TIDB_DSN -u ARTEX_PG_DSN GOCACHE=/tmp/gocache-artex go test -v ./server/idalloc/...` | 0 | 纯逻辑边界用例全部 PASS；3 个 TiDB 用例诚实输出 `未验证: ARTEX_TIDB_DSN 未配置` 并 SKIP |
| `go test -count=2 ./server/idalloc/...` | 0 | 重复执行一致通过 |
| `go test -race ./server/idalloc/...` | 0 | Race 检测器通过，无并发竞争 |

## 4. 提交信息

- Commit Message: `fix(idalloc): ensure strict sql mode compatibility and explicit allocator exhaustion assertions`
- 修改文件清单（单文件）：
  - `server/idalloc/boundary_test.go`
