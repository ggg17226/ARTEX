# ARTEX 原生 TiDB 改造实施计划 P04b：全序行级锁、fresh locator 快照与死锁规避

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


> **致执行代理：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐项落实。使用 `- [ ]` 复选框跟踪步骤。本计划 5 个 Task 必须按顺序严格落实（Task 1 提供 65 表 TableOrder 枚举、锁排序器与锁上下文，Task 2 提供独立只读事务 fresh locator 快照提取器，Task 3 提供按 `(table-order, 物理 PK 升序)` 执行实体锁与当前态核验防漂移执行器，Task 4 提供包含 100/250/500ms/1s 指数退避与全事务回滚逆序释放门禁的重试运行器，Task 5 提供交叉并发死锁免疫与真实 TiDB 死锁防线集成测试）；每 Task 内 5 步 TDD 不得跳步，严禁任何占位符与 TODO。

**Goal:** 在 TiDB 8.5 LTS 悲观事务模式下，建立无死锁、无脏读、无陈旧快照泄漏的应用层全序并发控制与事务防线：在计划/locator 阶段**不取实体锁**，通过门禁后独立 fresh 只读 locator 事务快照收集候选实体；外层数据事务按全局严格的 `(table-order, 物理 PK 升序)` 批次加行级锁（`FOR UPDATE`）；持锁后立即重验实体当前态与逻辑身份，遇成员漂移立即整体 `ROLLBACK` 并逆序释放所有锁，彻底杜绝逆序低序补锁；针对死锁（1213）、锁等待超时（1205）、唯一键冲突（1062）和漂移实施 100ms/250ms/500ms/1s 有界指数退避重试，退避耗尽显式 fail closed。

**Architecture:**
1. **表全序定义与锁排序器（Table Order & Lock Sequencer）**：在 `db/lockorder/order.go` 中固化规格 §4.3 定义的 65 张全库表全局唯一全序 `TableOrder`（10 张优先核心表 + 55 张字典序表），为每张表关联底层物理主键列名（`id` / `ordinal` / `profile_id` / `row_id`）；提供 `EntityKey` 排序比较器与 `LockContext` 锁上下文，强制单向获取并在逆序释放时记录回溯。
2. **Fresh 只读 Locator 快照提取器（Fresh Readonly Locator Snapshot）**：在 `db/lockorder/locator.go` 中实现隔离的只读快照提取器；在外层事务持有全部门禁锁之后、执行实体锁加锁之前，通过同库共享连接池另开独立只读事务（`sql.TxOptions{ReadOnly: true}`），在无锁状态下运行纯读查询提取 bounded 物理 PK 候选集与逻辑身份哈希；显式关闭 Rows 与 locator 事务，杜绝长读混写与陈旧 MVCC 视图。
3. **实体锁有序执行器与当前态验证器（Ordered Entity Locker & Current Verifier）**：在 `db/lockorder/locker.go` 中，将 locator 候选集按 `(table-order, 物理 PK 升序)` 严格排序，在原外层数据事务中按序执行 `SELECT <pk_col> FROM <table> WHERE <pk_col> = ? FOR UPDATE`；持有锁后立即进行当前态读（current read），比对逻辑身份与活性；若发现新成员、更低物理 PK 或实体不存在，立即抛出 `ErrMembershipDrift`，禁止追加补锁。
4. **事务退避重试包装器（Tx Runner & Exponential Backoff）**：在 `db/lockorder/tx_runner.go` 中整合错误分类器（1062 唯一键冲突、1213 死锁、1205 锁等待超时、`ErrMembershipDrift` 成员漂移），触发全事务回滚，逆序释放全部 DB 锁与本地门禁，按 100ms -> 250ms -> 500ms -> 1s 执行指数退避重试，耗尽后明确 fail closed。
5. **并发交叉锁与死锁免疫集成测试（Deadlock Immunity Concurrency Tests）**：在 `db/lockorder/deadlock_test.go` 中并发反序业务加锁请求，验证全序加锁下的零死锁（零 1213）免疫特性；支持环境变量 `ARTEX_TIDB_DSN` 真实 TiDB 门控，缺失时诚实 `t.Skip` 并标记为“未验证”。

**Tech Stack:** Go 1.26.3，`database/sql` 标准库，`github.com/go-sql-driver/mysql`，无 GORM，无第三方 ORM 依赖。

**Spec:** `docs/superpowers/specs/2026-10-10-native-tidb-design.md`（重点研读 §4.3 全序行级锁与 fresh 只读 locator 快照验证、§10.2 I4 事务与并发全序规则、§11.1 实测事实）。

---

## Global Constraints

1. **以集群现有配置为基准，不对集群做调参要求**：ARTEX 仅连接外部 TiDB 8.5 LTS，不要求关闭 TiKV `pipelined`/`in-memory` 悲观锁优化，不要求固定 `max_allowed_packet`，不以 `SHOW CONFIG` 权限缺失阻断启动。悲观锁降级为减少排队的调度手段，正确性由单实例 OS 锁 + 本地门禁 + 命名 UNIQUE + 条件写 `RowsAffected` + 不可变 outcome 共同承担（规格 §4.3、§10.4-T）。
2. **锁全序唯一规范（绝对红线）**：全序固定为 `local business gate（仅关系写）→ 按名称排序的全部所需 namespace localgate → 同一外层 data tx 的 business_relations DB 锁 → 按名称排序的 namespace DB 锁 → 门禁后独立 fresh 只读 locator TX 快照定位 → 外层 data tx 实体 (table-order, 物理 PK 升序) current FOR UPDATE 锁`。本地 gate 一律先于所有 DB 锁；禁止在已持 DB 锁后取得 localgate；禁止保留第二套 Trygate 机制。
3. **计划/Locator 阶段严禁取实体锁**：计划阶段仅提取 stable logical identity 与 bounded 物理 PK 候选集；严禁通过 `SELECT ... WHERE uq = ? FOR UPDATE` 抢先锁定逻辑唯一键，避免因逻辑索引顺序与物理 PK 顺序相反引发死锁。
4. **Fresh 只读 Locator 快照隔离**：必须在外层事务持有全部门禁之后、加实体锁之前，通过同一共享连接池另开独立只读事务（`ReadOnly: true`），执行非 locking 查询收集候选集；必须显式关闭 Rows 并提交/回滚结束该 locator TX，不持锁、不跑 DML、不混写事务。
5. **碰撞与漂移整体回滚，严禁追加低序补锁**：实体锁取得后立即 current 复核。若发现新 member、更低物理 PK、实体缺失或逻辑身份变化，必须立即整体 `ROLLBACK` 外层事务，逆序释放全部 DB 锁与 local gates，清空旧 `lockCtx`，回到非加锁 Locator 重新开始；绝对禁止在已持实体集合上反向追加补锁。
6. **隐式冲突有界退避重试**：遇到 1062（唯一键冲突）、1213（死锁）、1205（锁超时）或成员漂移，执行全事务回滚与逆序释放，按 100ms / 250ms / 500ms / 1s 进行最多 4 次退避重试（总计 5 次尝试），耗尽后显式 fail closed。严禁 partial commit。
7. **接口与 JS 统一 string，内部映射 uint64**：所有 64 位数字 ID 在对外 API/DTO 与前端一律表现为 string；Go 内部实体主键与引用统一映射为 `uint64`；隐藏 `row_id` 仅用于底层物理加锁与聚集索引，绝不进入对外接口。
8. **机密与代码隔离**：本计划所有文件均位于 `db/lockorder/` 目录下，不得修改工作区内既有 Go/TS/SQL/配置文件；绝对禁止读取 `secret/` 目录；缺真实 TiDB 环境时测试诚实 skip 并标注“未验证”。

---

### Task 1: 表全序定义与锁排序器

**Files:**
- Create: `db/lockorder/order.go`
- Create: `db/lockorder/order_test.go`

**Interfaces:**
- Consumes:
  - `context`
  - `fmt`
  - `sort`
  - `sync`
  - `errors`
- Produces:
  - `type TableOrder uint16`
  - `const TableOrderTasks TableOrder = 1` ... `const TableOrderTrafficStores TableOrder = 65`
  - `type TableMeta struct { Order TableOrder; Name string; PKColumn string }`
  - `func LookupTableOrder(order TableOrder) (TableMeta, error)`
  - `func LookupTableName(name string) (TableMeta, error)`
  - `func AllTableMetas() []TableMeta`
  - `type EntityKey struct { Table TableOrder; PK uint64 }`
  - `func (k EntityKey) String() string`
  - `func CompareEntityKey(a, b EntityKey) int`
  - `func SortEntityKeys(keys []EntityKey) []EntityKey`
  - `func IsSorted(keys []EntityKey) bool`
  - `type LockPhase int`
  - `const PhaseInit LockPhase = 0`
  - `const PhaseLocalBusiness LockPhase = 1`
  - `const PhaseLocalNamespace LockPhase = 2`
  - `const PhaseDBBusiness LockPhase = 3`
  - `const PhaseDBNamespace LockPhase = 4`
  - `const PhaseEntityLock LockPhase = 5`
  - `type LockEvent struct { Phase LockPhase; Name string; Key EntityKey }`
  - `type LockContext struct`
  - `func NewLockContext() *LockContext`
  - `func (c *LockContext) RecordLocalBusinessGate() error`
  - `func (c *LockContext) RecordLocalNamespaceGate(ns string) error`
  - `func (c *LockContext) RecordDBBusinessGate() error`
  - `func (c *LockContext) RecordDBNamespaceGate(ns string) error`
  - `func (c *LockContext) RecordEntityLock(key EntityKey) error`
  - `func (c *LockContext) ReleaseEvents() []LockEvent`
  - `func (c *LockContext) Reset()`
  - `var ErrOutOfOrderLock error`
  - `var ErrDuplicateLock error`
  - `var ErrUnknownTable error`

- [ ] **Step 1: 编写失败测试**

创建 `db/lockorder/order_test.go`：

```go
package lockorder

import (
	"errors"
	"testing"
)

func TestTableCatalog_All65Tables(t *testing.T) {
	metas := AllTableMetas()
	if len(metas) != 65 {
		t.Fatalf("expected exactly 65 tables, got %d", len(metas))
	}

	seenOrders := make(map[TableOrder]string)
	seenNames := make(map[string]TableOrder)

	for i, m := range metas {
		expectedOrder := TableOrder(i + 1)
		if m.Order != expectedOrder {
			t.Errorf("table %s has order %d, expected %d", m.Name, m.Order, expectedOrder)
		}
		if prev, ok := seenOrders[m.Order]; ok {
			t.Errorf("duplicate order %d for tables %s and %s", m.Order, prev, m.Name)
		}
		seenOrders[m.Order] = m.Name

		if prevOrder, ok := seenNames[m.Name]; ok {
			t.Errorf("duplicate table name %s with orders %d and %d", m.Name, prevOrder, m.Order)
		}
		seenNames[m.Name] = m.Order

		if m.PKColumn == "" {
			t.Errorf("table %s has empty PKColumn", m.Name)
		}
	}

	// 验证规格 §4.3 前 10 张核心表顺序
	expectedFirst10 := []string{
		"tasks",
		"agents",
		"conversations",
		"llm_profiles",
		"companies",
		"explorations",
		"exploration_nodes",
		"assets",
		"findings",
		"agent_prompts",
	}

	for i, name := range expectedFirst10 {
		meta, err := LookupTableOrder(TableOrder(i + 1))
		if err != nil {
			t.Fatalf("LookupTableOrder(%d) failed: %v", i+1, err)
		}
		if meta.Name != name {
			t.Errorf("slot %d: expected table %s, got %s", i+1, name, meta.Name)
		}
	}

	// 验证后续 55 张表严格按字典序排列
	for i := 10; i < 64; i++ {
		cur := metas[i].Name
		next := metas[i+1].Name
		if cur >= next {
			t.Errorf("alphabetical order violation at %d: %s >= %s", i, cur, next)
		}
	}
}

func TestLookupTableByName(t *testing.T) {
	meta, err := LookupTableName("tasks")
	if err != nil {
		t.Fatalf("expected tasks lookup to succeed, got %v", err)
	}
	if meta.Order != TableOrderTasks || meta.PKColumn != "id" {
		t.Errorf("unexpected tasks meta: %+v", meta)
	}

	metaOrd, err := LookupTableName("side_question_requests")
	if err != nil {
		t.Fatalf("expected side_question_requests lookup to succeed, got %v", err)
	}
	if metaOrd.PKColumn != "ordinal" {
		t.Errorf("expected side_question_requests PKColumn 'ordinal', got '%s'", metaOrd.PKColumn)
	}

	metaProf, err := LookupTableName("llm_profile_health")
	if err != nil {
		t.Fatalf("expected llm_profile_health lookup to succeed, got %v", err)
	}
	if metaProf.PKColumn != "profile_id" {
		t.Errorf("expected llm_profile_health PKColumn 'profile_id', got '%s'", metaProf.PKColumn)
	}

	metaRow, err := LookupTableName("locks")
	if err != nil {
		t.Fatalf("expected locks lookup to succeed, got %v", err)
	}
	if metaRow.PKColumn != "row_id" {
		t.Errorf("expected locks PKColumn 'row_id', got '%s'", metaRow.PKColumn)
	}

	_, err = LookupTableName("non_existent_table")
	if !errors.Is(err, ErrUnknownTable) {
		t.Errorf("expected ErrUnknownTable, got %v", err)
	}
}

func TestCompareEntityKey(t *testing.T) {
	k1 := EntityKey{Table: TableOrderTasks, PK: 10}
	k2 := EntityKey{Table: TableOrderTasks, PK: 20}
	k3 := EntityKey{Table: TableOrderAgents, PK: 5}

	if CompareEntityKey(k1, k2) >= 0 {
		t.Errorf("expected k1 < k2")
	}
	if CompareEntityKey(k2, k1) <= 0 {
		t.Errorf("expected k2 > k1")
	}
	if CompareEntityKey(k1, k1) != 0 {
		t.Errorf("expected k1 == k1")
	}
	if CompareEntityKey(k2, k3) >= 0 {
		t.Errorf("expected k2 < k3 because tasks < agents")
	}
}

func TestSortEntityKeys(t *testing.T) {
	input := []EntityKey{
		{Table: TableOrderCompanies, PK: 100},
		{Table: TableOrderTasks, PK: 50},
		{Table: TableOrderAgents, PK: 2},
		{Table: TableOrderTasks, PK: 10},
		{Table: TableOrderAgents, PK: 2}, // 重复项
	}

	sorted := SortEntityKeys(input)
	if len(sorted) != 4 {
		t.Fatalf("expected deduplicated length 4, got %d", len(sorted))
	}

	expected := []EntityKey{
		{Table: TableOrderTasks, PK: 10},
		{Table: TableOrderTasks, PK: 50},
		{Table: TableOrderAgents, PK: 2},
		{Table: TableOrderCompanies, PK: 100},
	}

	for i, k := range sorted {
		if k != expected[i] {
			t.Errorf("slot %d: expected %+v, got %+v", i, expected[i], k)
		}
	}

	if !IsSorted(sorted) {
		t.Errorf("expected IsSorted to be true")
	}
}

func TestLockContext_ValidTotalOrder(t *testing.T) {
	lc := NewLockContext()

	if err := lc.RecordLocalBusinessGate(); err != nil {
		t.Fatalf("RecordLocalBusinessGate failed: %v", err)
	}
	if err := lc.RecordLocalNamespaceGate("ns-a"); err != nil {
		t.Fatalf("RecordLocalNamespaceGate(ns-a) failed: %v", err)
	}
	if err := lc.RecordLocalNamespaceGate("ns-b"); err != nil {
		t.Fatalf("RecordLocalNamespaceGate(ns-b) failed: %v", err)
	}
	if err := lc.RecordDBBusinessGate(); err != nil {
		t.Fatalf("RecordDBBusinessGate failed: %v", err)
	}
	if err := lc.RecordDBNamespaceGate("ns-a"); err != nil {
		t.Fatalf("RecordDBNamespaceGate(ns-a) failed: %v", err)
	}
	if err := lc.RecordDBNamespaceGate("ns-b"); err != nil {
		t.Fatalf("RecordDBNamespaceGate(ns-b) failed: %v", err)
	}
	if err := lc.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 1}); err != nil {
		t.Fatalf("RecordEntityLock failed: %v", err)
	}
	if err := lc.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 2}); err != nil {
		t.Fatalf("RecordEntityLock failed: %v", err)
	}
	if err := lc.RecordEntityLock(EntityKey{Table: TableOrderAgents, PK: 1}); err != nil {
		t.Fatalf("RecordEntityLock failed: %v", err)
	}

	events := lc.ReleaseEvents()
	if len(events) != 9 {
		t.Fatalf("expected 9 release events, got %d", len(events))
	}

	// 逆序释放断言：第一个释放的应当是最后加的实体锁 Agents:1
	if events[0].Phase != PhaseEntityLock || events[0].Key.Table != TableOrderAgents {
		t.Errorf("expected first release to be Agents:1 entity lock, got %+v", events[0])
	}
	// 最后一个释放的应当是最初加的 local business gate
	if events[8].Phase != PhaseLocalBusiness {
		t.Errorf("expected last release to be LocalBusiness gate, got %+v", events[8])
	}
}

func TestLockContext_OutOfOrderViolations(t *testing.T) {
	// 测试：实体锁之后再加 gate（违规）
	lc := NewLockContext()
	_ = lc.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 1})
	err := lc.RecordLocalBusinessGate()
	if !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock for gate after entity, got %v", err)
	}

	// 测试：实体锁低序追加（后加更小 PK，违规）
	lc2 := NewLockContext()
	_ = lc2.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 10})
	err = lc2.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 5})
	if !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock for backwards PK, got %v", err)
	}

	// 测试：实体锁低序追加（后加更小 TableOrder，违规）
	lc3 := NewLockContext()
	_ = lc3.RecordEntityLock(EntityKey{Table: TableOrderAgents, PK: 1})
	err = lc3.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 100})
	if !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock for backwards TableOrder, got %v", err)
	}

	// 测试：Namespace gate 未按字典序获取
	lc4 := NewLockContext()
	_ = lc4.RecordLocalNamespaceGate("ns-z")
	err = lc4.RecordLocalNamespaceGate("ns-a")
	if !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock for reverse namespace gate, got %v", err)
	}
}

func TestLockContext_DuplicateViolations(t *testing.T) {
	lc := NewLockContext()
	_ = lc.RecordLocalBusinessGate()
	err := lc.RecordLocalBusinessGate()
	if !errors.Is(err, ErrDuplicateLock) {
		t.Errorf("expected ErrDuplicateLock for repeated business gate, got %v", err)
	}

	lc2 := NewLockContext()
	_ = lc2.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 1})
	err = lc2.RecordEntityLock(EntityKey{Table: TableOrderTasks, PK: 1})
	if !errors.Is(err, ErrDuplicateLock) {
		t.Errorf("expected ErrDuplicateLock for duplicate entity lock, got %v", err)
	}
}
```

- [ ] **Step 2: 运行并确认失败**

运行：`go test -v ./db/lockorder/ -run "TestTableCatalog.*"`
预期：FAIL，提示 `package github.com/Autumn-27/artex/db/lockorder is not in std` 或 `cannot find package`。

- [ ] **Step 3: 最小实现**

创建 `db/lockorder/order.go`：

```go
package lockorder

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrOutOfOrderLock = errors.New("lockorder: out of order lock acquisition")
	ErrDuplicateLock  = errors.New("lockorder: duplicate lock acquisition")
	ErrUnknownTable   = errors.New("lockorder: unknown table name or order")
)

// TableOrder 表示全局 65 张表的固定锁顺序枚举
type TableOrder uint16

const (
	// 规格 §4.3 显式固定的前 10 张核心业务表
	TableOrderTasks            TableOrder = 1
	TableOrderAgents           TableOrder = 2
	TableOrderConversations    TableOrder = 3
	TableOrderLLMProfiles      TableOrder = 4
	TableOrderCompanies        TableOrder = 5
	TableOrderExplorations     TableOrder = 6
	TableOrderExplorationNodes TableOrder = 7
	TableOrderAssets           TableOrder = 8
	TableOrderFindings         TableOrder = 9
	TableOrderAgentPrompts     TableOrder = 10

	// 其余 55 张表严格按表名字典序排列
	TableOrderActivity                 TableOrder = 11
	TableOrderAgentPromptVars          TableOrder = 12
	TableOrderAgentSkillVisibility     TableOrder = 13
	TableOrderAgentTriggers            TableOrder = 14
	TableOrderAgentVisibility          TableOrder = 15
	TableOrderAssetInterceptRules      TableOrder = 16
	TableOrderBusinessDeployment       TableOrder = 17
	TableOrderCompanyScope             TableOrder = 18
	TableOrderConversationActivities   TableOrder = 19
	TableOrderExplorationAnchors       TableOrder = 20
	TableOrderExplorationEdges         TableOrder = 21
	TableOrderFindingAssets            TableOrder = 22
	TableOrderFindingRetests           TableOrder = 23
	TableOrderFindingTrafficBindings   TableOrder = 24
	TableOrderInterceptPending         TableOrder = 25
	TableOrderInterceptRules           TableOrder = 26
	TableOrderLLMProfileHealth         TableOrder = 27
	TableOrderLLMRecords               TableOrder = 28
	TableOrderLLMUsage                 TableOrder = 29
	TableOrderLocks                    TableOrder = 30
	TableOrderMainSessions             TableOrder = 31
	TableOrderMaintenance              TableOrder = 32
	TableOrderMCPServers               TableOrder = 33
	TableOrderMCPToolsCache            TableOrder = 34
	TableOrderNotificationChannels     TableOrder = 35
	TableOrderNotificationDeliveries   TableOrder = 36
	TableOrderNotificationEvents       TableOrder = 37
	TableOrderOpOutcomes               TableOrder = 38
	TableOrderSchedulerState           TableOrder = 39
	TableOrderSchemaMeta               TableOrder = 40
	TableOrderServerLogs               TableOrder = 41
	TableOrderSettings                 TableOrder = 42
	TableOrderSideQuestionRequests     TableOrder = 43
	TableOrderSideQuestionSessions     TableOrder = 44
	TableOrderSkillUsage               TableOrder = 45
	TableOrderStorageIDMap             TableOrder = 46
	TableOrderStorageJobPins           TableOrder = 47
	TableOrderStorageJobs              TableOrder = 48
	TableOrderTaskArchives             TableOrder = 49
	TableOrderTaskAssetLinks           TableOrder = 50
	TableOrderTaskCategories           TableOrder = 51
	TableOrderTaskConstraints          TableOrder = 52
	TableOrderTaskInterceptRules       TableOrder = 53
	TableOrderTaskLLMProfiles          TableOrder = 54
	TableOrderTaskRelations            TableOrder = 55
	TableOrderTaskScope                TableOrder = 56
	TableOrderTaskTemplates            TableOrder = 57
	TableOrderToolUsage                TableOrder = 58
	TableOrderTools                    TableOrder = 59
	TableOrderTrafficBlobRefs          TableOrder = 60
	TableOrderTrafficEvidenceSnapshots TableOrder = 61
	TableOrderTrafficExchangeBodies    TableOrder = 62
	TableOrderTrafficExchanges         TableOrder = 63
	TableOrderTrafficIndexChunks       TableOrder = 64
	TableOrderTrafficStores            TableOrder = 65
)

// TableMeta 记录表的元信息与主键物理列名
type TableMeta struct {
	Order    TableOrder
	Name     string
	PKColumn string
}

var allTables = []TableMeta{
	// 核心 10 表
	{Order: TableOrderTasks, Name: "tasks", PKColumn: "id"},
	{Order: TableOrderAgents, Name: "agents", PKColumn: "id"},
	{Order: TableOrderConversations, Name: "conversations", PKColumn: "id"},
	{Order: TableOrderLLMProfiles, Name: "llm_profiles", PKColumn: "id"},
	{Order: TableOrderCompanies, Name: "companies", PKColumn: "id"},
	{Order: TableOrderExplorations, Name: "explorations", PKColumn: "id"},
	{Order: TableOrderExplorationNodes, Name: "exploration_nodes", PKColumn: "id"},
	{Order: TableOrderAssets, Name: "assets", PKColumn: "id"},
	{Order: TableOrderFindings, Name: "findings", PKColumn: "id"},
	{Order: TableOrderAgentPrompts, Name: "agent_prompts", PKColumn: "id"},

	// 字典序 55 表
	{Order: TableOrderActivity, Name: "activity", PKColumn: "id"},
	{Order: TableOrderAgentPromptVars, Name: "agent_prompt_vars", PKColumn: "id"},
	{Order: TableOrderAgentSkillVisibility, Name: "agent_skill_visibility", PKColumn: "row_id"},
	{Order: TableOrderAgentTriggers, Name: "agent_triggers", PKColumn: "id"},
	{Order: TableOrderAgentVisibility, Name: "agent_visibility", PKColumn: "row_id"},
	{Order: TableOrderAssetInterceptRules, Name: "asset_intercept_rules", PKColumn: "id"},
	{Order: TableOrderBusinessDeployment, Name: "business_deployment", PKColumn: "row_id"},
	{Order: TableOrderCompanyScope, Name: "company_scope", PKColumn: "id"},
	{Order: TableOrderConversationActivities, Name: "conversation_activities", PKColumn: "id"},
	{Order: TableOrderExplorationAnchors, Name: "exploration_anchors", PKColumn: "row_id"},
	{Order: TableOrderExplorationEdges, Name: "exploration_edges", PKColumn: "row_id"},
	{Order: TableOrderFindingAssets, Name: "finding_assets", PKColumn: "id"},
	{Order: TableOrderFindingRetests, Name: "finding_retests", PKColumn: "id"},
	{Order: TableOrderFindingTrafficBindings, Name: "finding_traffic_bindings", PKColumn: "id"},
	{Order: TableOrderInterceptPending, Name: "intercept_pending", PKColumn: "id"},
	{Order: TableOrderInterceptRules, Name: "intercept_rules", PKColumn: "id"},
	{Order: TableOrderLLMProfileHealth, Name: "llm_profile_health", PKColumn: "profile_id"},
	{Order: TableOrderLLMRecords, Name: "llm_records", PKColumn: "id"},
	{Order: TableOrderLLMUsage, Name: "llm_usage", PKColumn: "id"},
	{Order: TableOrderLocks, Name: "locks", PKColumn: "row_id"},
	{Order: TableOrderMainSessions, Name: "main_sessions", PKColumn: "row_id"},
	{Order: TableOrderMaintenance, Name: "maintenance", PKColumn: "row_id"},
	{Order: TableOrderMCPServers, Name: "mcp_servers", PKColumn: "id"},
	{Order: TableOrderMCPToolsCache, Name: "mcp_tools_cache", PKColumn: "id"},
	{Order: TableOrderNotificationChannels, Name: "notification_channels", PKColumn: "id"},
	{Order: TableOrderNotificationDeliveries, Name: "notification_deliveries", PKColumn: "id"},
	{Order: TableOrderNotificationEvents, Name: "notification_events", PKColumn: "id"},
	{Order: TableOrderOpOutcomes, Name: "op_outcomes", PKColumn: "row_id"},
	{Order: TableOrderSchedulerState, Name: "scheduler_state", PKColumn: "row_id"},
	{Order: TableOrderSchemaMeta, Name: "schema_meta", PKColumn: "row_id"},
	{Order: TableOrderServerLogs, Name: "server_logs", PKColumn: "id"},
	{Order: TableOrderSettings, Name: "settings", PKColumn: "row_id"},
	{Order: TableOrderSideQuestionRequests, Name: "side_question_requests", PKColumn: "ordinal"},
	{Order: TableOrderSideQuestionSessions, Name: "side_question_sessions", PKColumn: "row_id"},
	{Order: TableOrderSkillUsage, Name: "skill_usage", PKColumn: "id"},
	{Order: TableOrderStorageIDMap, Name: "storage_id_map", PKColumn: "row_id"},
	{Order: TableOrderStorageJobPins, Name: "storage_job_pins", PKColumn: "row_id"},
	{Order: TableOrderStorageJobs, Name: "storage_jobs", PKColumn: "row_id"},
	{Order: TableOrderTaskArchives, Name: "task_archives", PKColumn: "id"},
	{Order: TableOrderTaskAssetLinks, Name: "task_asset_links", PKColumn: "row_id"},
	{Order: TableOrderTaskCategories, Name: "task_categories", PKColumn: "id"},
	{Order: TableOrderTaskConstraints, Name: "task_constraints", PKColumn: "id"},
	{Order: TableOrderTaskInterceptRules, Name: "task_intercept_rules", PKColumn: "id"},
	{Order: TableOrderTaskLLMProfiles, Name: "task_llm_profiles", PKColumn: "row_id"},
	{Order: TableOrderTaskRelations, Name: "task_relations", PKColumn: "row_id"},
	{Order: TableOrderTaskScope, Name: "task_scope", PKColumn: "id"},
	{Order: TableOrderTaskTemplates, Name: "task_templates", PKColumn: "id"},
	{Order: TableOrderToolUsage, Name: "tool_usage", PKColumn: "id"},
	{Order: TableOrderTools, Name: "tools", PKColumn: "row_id"},
	{Order: TableOrderTrafficBlobRefs, Name: "traffic_blob_refs", PKColumn: "row_id"},
	{Order: TableOrderTrafficEvidenceSnapshots, Name: "traffic_evidence_snapshots", PKColumn: "row_id"},
	{Order: TableOrderTrafficExchangeBodies, Name: "traffic_exchange_bodies", PKColumn: "row_id"},
	{Order: TableOrderTrafficExchanges, Name: "traffic_exchanges", PKColumn: "row_id"},
	{Order: TableOrderTrafficIndexChunks, Name: "traffic_index_chunks", PKColumn: "row_id"},
	{Order: TableOrderTrafficStores, Name: "traffic_stores", PKColumn: "row_id"},
}

var (
	orderByNum  map[TableOrder]TableMeta
	orderByName map[string]TableMeta
	initOnce    sync.Once
)

func ensureMaps() {
	initOnce.Do(func() {
		orderByNum = make(map[TableOrder]TableMeta, len(allTables))
		orderByName = make(map[string]TableMeta, len(allTables))
		for _, m := range allTables {
			orderByNum[m.Order] = m
			orderByName[m.Name] = m
		}
	})
}

// AllTableMetas 返回全库 65 表元信息副本
func AllTableMetas() []TableMeta {
	res := make([]TableMeta, len(allTables))
	copy(res, allTables)
	return res
}

// LookupTableOrder 通过 TableOrder 查询元信息
func LookupTableOrder(order TableOrder) (TableMeta, error) {
	ensureMaps()
	if m, ok := orderByNum[order]; ok {
		return m, nil
	}
	return TableMeta{}, fmt.Errorf("%w: order %d", ErrUnknownTable, order)
}

// LookupTableName 通过表名查询元信息
func LookupTableName(name string) (TableMeta, error) {
	ensureMaps()
	if m, ok := orderByName[name]; ok {
		return m, nil
	}
	return TableMeta{}, fmt.Errorf("%w: table %s", ErrUnknownTable, name)
}

// EntityKey 物理实体加锁定位键
type EntityKey struct {
	Table TableOrder
	PK    uint64
}

func (k EntityKey) String() string {
	ensureMaps()
	tableName := fmt.Sprintf("table#%d", k.Table)
	if m, ok := orderByNum[k.Table]; ok {
		tableName = m.Name
	}
	return fmt.Sprintf("%s(pk=%d)", tableName, k.PK)
}

// CompareEntityKey 按 (table_order, pk ASC) 比较两实体键
func CompareEntityKey(a, b EntityKey) int {
	if a.Table < b.Table {
		return -1
	}
	if a.Table > b.Table {
		return 1
	}
	if a.PK < b.PK {
		return -1
	}
	if a.PK > b.PK {
		return 1
	}
	return 0
}

// SortEntityKeys 排序实体键并去重
func SortEntityKeys(keys []EntityKey) []EntityKey {
	if len(keys) <= 1 {
		res := make([]EntityKey, len(keys))
		copy(res, keys)
		return res
	}

	cloned := make([]EntityKey, len(keys))
	copy(cloned, keys)

	sort.Slice(cloned, func(i, j int) bool {
		return CompareEntityKey(cloned[i], cloned[j]) < 0
	})

	dedup := make([]EntityKey, 0, len(cloned))
	dedup = append(dedup, cloned[0])
	for i := 1; i < len(cloned); i++ {
		if CompareEntityKey(cloned[i], cloned[i-1]) != 0 {
			dedup = append(dedup, cloned[i])
		}
	}
	return dedup
}

// IsSorted 检查实体键是否已严格升序且无重复
func IsSorted(keys []EntityKey) bool {
	for i := 1; i < len(keys); i++ {
		if CompareEntityKey(keys[i-1], keys[i]) >= 0 {
			return false
		}
	}
	return true
}

// LockPhase 表示获取锁的阶段
type LockPhase int

const (
	PhaseInit           LockPhase = 0
	PhaseLocalBusiness  LockPhase = 1
	PhaseLocalNamespace LockPhase = 2
	PhaseDBBusiness     LockPhase = 3
	PhaseDBNamespace    LockPhase = 4
	PhaseEntityLock     LockPhase = 5
)

// LockEvent 记录锁获取事件，用于逆序释放与审计
type LockEvent struct {
	Phase LockPhase
	Name  string
	Key   EntityKey
}

// LockContext 记录事务内持有的所有锁并执行严格全序校验
type LockContext struct {
	mu sync.Mutex

	currentPhase LockPhase

	localBusinessHeld bool
	localNamespaces   []string

	dbBusinessHeld bool
	dbNamespaces   []string

	lastEntityLock *EntityKey
	heldEntities   map[EntityKey]struct{}

	history []LockEvent
}

// NewLockContext 创建新的锁上下文
func NewLockContext() *LockContext {
	return &LockContext{
		heldEntities: make(map[EntityKey]struct{}),
	}
}

// RecordLocalBusinessGate 记录获取本地业务关系门禁
func (c *LockContext) RecordLocalBusinessGate() error {
	c.mu.Lockdefer()
	defer c.mu.Unlock()

	if c.currentPhase > PhaseLocalBusiness {
		return fmt.Errorf("%w: cannot acquire local business gate at phase %d", ErrOutOfOrderLock, c.currentPhase)
	}
	if c.localBusinessHeld {
		return fmt.Errorf("%w: local business gate already held", ErrDuplicateLock)
	}

	c.localBusinessHeld = true
	c.currentPhase = PhaseLocalBusiness
	c.history = append(c.history, LockEvent{Phase: PhaseLocalBusiness, Name: "business_relations"})
	return nil
}

// RecordLocalNamespaceGate 记录获取本地命名空间门禁（必须按名称字典序）
func (c *LockContext) RecordLocalNamespaceGate(ns string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentPhase > PhaseLocalNamespace {
		return fmt.Errorf("%w: cannot acquire local namespace gate at phase %d", ErrOutOfOrderLock, c.currentPhase)
	}

	if len(c.localNamespaces) > 0 {
		last := c.localNamespaces[len(c.localNamespaces)-1]
		if ns < last {
			return fmt.Errorf("%w: local namespace %s acquired after %s", ErrOutOfOrderLock, ns, last)
		}
		if ns == last {
			return fmt.Errorf("%w: local namespace %s already held", ErrDuplicateLock)
		}
	}

	c.localNamespaces = append(c.localNamespaces, ns)
	c.currentPhase = PhaseLocalNamespace
	c.history = append(c.history, LockEvent{Phase: PhaseLocalNamespace, Name: ns})
	return nil
}

// RecordDBBusinessGate 记录获取数据库 business_relations 行锁
func (c *LockContext) RecordDBBusinessGate() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentPhase > PhaseDBBusiness {
		return fmt.Errorf("%w: cannot acquire DB business gate at phase %d", ErrOutOfOrderLock, c.currentPhase)
	}
	if c.dbBusinessHeld {
		return fmt.Errorf("%w: DB business gate already held", ErrDuplicateLock)
	}

	c.dbBusinessHeld = true
	c.currentPhase = PhaseDBBusiness
	c.history = append(c.history, LockEvent{Phase: PhaseDBBusiness, Name: "business_relations"})
	return nil
}

// RecordDBNamespaceGate 记录获取数据库命名空间行锁（必须按名称字典序）
func (c *LockContext) RecordDBNamespaceGate(ns string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentPhase > PhaseDBNamespace {
		return fmt.Errorf("%w: cannot acquire DB namespace gate at phase %d", ErrOutOfOrderLock, c.currentPhase)
	}

	if len(c.dbNamespaces) > 0 {
		last := c.dbNamespaces[len(c.dbNamespaces)-1]
		if ns < last {
			return fmt.Errorf("%w: DB namespace %s acquired after %s", ErrOutOfOrderLock, ns, last)
		}
		if ns == last {
			return fmt.Errorf("%w: DB namespace %s already held", ErrDuplicateLock)
		}
	}

	c.dbNamespaces = append(c.dbNamespaces, ns)
	c.currentPhase = PhaseDBNamespace
	c.history = append(c.history, LockEvent{Phase: PhaseDBNamespace, Name: ns})
	return nil
}

// RecordEntityLock 记录获取实体行锁（必须按 TableOrder 与 PK ASC 升序）
func (c *LockContext) RecordEntityLock(key EntityKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.heldEntities[key]; exists {
		return fmt.Errorf("%w: entity %s already held", ErrDuplicateLock, key)
	}

	if c.lastEntityLock != nil {
		cmp := CompareEntityKey(*c.lastEntityLock, key)
		if cmp > 0 {
			return fmt.Errorf("%w: entity %s acquired after %s", ErrOutOfOrderLock, key, *c.lastEntityLock)
		}
		if cmp == 0 {
			return fmt.Errorf("%w: duplicate entity lock %s", ErrDuplicateLock, key)
		}
	}

	c.currentPhase = PhaseEntityLock
	c.heldEntities[key] = struct{}{}
	keyCopy := key
	c.lastEntityLock = &keyCopy
	c.history = append(c.history, LockEvent{Phase: PhaseEntityLock, Key: key})
	return nil
}

// ReleaseEvents 返回所有已持锁的逆序事件清单
func (c *LockContext) ReleaseEvents() []LockEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := len(c.history)
	reversed := make([]LockEvent, n)
	for i := 0; i < n; i++ {
		reversed[i] = c.history[n-1-i]
	}
	return reversed
}

// Reset 清空全部锁状态（在事务整体回滚后调用）
func (c *LockContext) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.currentPhase = PhaseInit
	c.localBusinessHeld = false
	c.localNamespaces = nil
	c.dbBusinessHeld = false
	c.dbNamespaces = nil
	c.lastEntityLock = nil
	c.heldEntities = make(map[EntityKey]struct{})
	c.history = nil
}
```

- [ ] **Step 4: 运行并确认通过**

运行：`go test -v ./db/lockorder/ -run "TestTableCatalog.*|TestLookupTableByName|TestCompareEntityKey|TestSortEntityKeys|TestLockContext.*"`
预期：PASS，所有 65 表顺序、实体比较、排序去重与全序锁上下文校验全部通过。

- [ ] **Step 5: 提交**

```bash
git add db/lockorder/order.go db/lockorder/order_test.go
git commit -m "feat(lockorder): implement 65-table total order and lock context sequencer"
```

---

### Task 2: Fresh 只读 Locator 快照提取器

**Files:**
- Create: `db/lockorder/locator.go`
- Create: `db/lockorder/locator_test.go`

**Interfaces:**
- Consumes:
  - `database/sql`
  - `context`
  - `time`
  - `fmt`
  - `errors`
  - `EntityKey`, `SortEntityKeys` (from Task 1)
- Produces:
  - `const MaxLocatorCandidates = 1000`
  - `var ErrCandidateLimitExceeded error`
  - `var ErrNilDatabase error`
  - `var ErrNilLocatorFunc error`
  - `type LocatorSnapshot struct { CandidateKeys []EntityKey; StableIdentities map[EntityKey]string; CreatedAt time.Time }`
  - `type LocatorFunc func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error)`
  - `func ExecuteFreshLocator(ctx context.Context, db *sql.DB, fn LocatorFunc) (*LocatorSnapshot, error)`

- [ ] **Step 1: 编写失败测试**

创建 `db/lockorder/locator_test.go`：

```go
package lockorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

// 内存模拟 driver，用于对 database/sql.DB 进行轻量只读事务隔离测试
type mockDriver struct {
	mu          sync.Mutex
	txStarted   int
	txCommitted int
	txRolledBack int
}

var (
	testMockDriverOnce sync.Once
	globalMockDriver   = &mockDriver{}
)

func init() {
	sql.Register("mock_locator_driver", globalMockDriver)
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{driver: d}, nil
}

type mockConn struct {
	driver *mockDriver
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{}, nil
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *mockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.driver.mu.Lock()
	c.driver.txStarted++
	c.driver.mu.Unlock()
	return &mockDriverTx{conn: c}, nil
}

type mockDriverTx struct {
	conn *mockConn
}

func (t *mockDriverTx) Commit() error {
	t.conn.driver.mu.Lock()
	t.conn.driver.txCommitted++
	t.conn.driver.mu.Unlock()
	return nil
}

func (t *mockDriverTx) Rollback() error {
	t.conn.driver.mu.Lock()
	t.conn.driver.txRolledBack++
	t.conn.driver.mu.Unlock()
	return nil
}

type mockStmt struct{}

func (s *mockStmt) Close() error { return nil }
func (s *mockStmt) NumInput() int { return -1 }
func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &mockRows{}, nil
}

type mockRows struct{}

func (r *mockRows) Columns() []string { return []string{"id"} }
func (r *mockRows) Close() error { return nil }
func (r *mockRows) Next(dest []driver.Value) error { return errors.New("EOF") }

func getMockDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mock_locator_driver", "test_dsn")
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	return db
}

func TestExecuteFreshLocator_Success(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		// 返回无序候选集与稳定身份
		return &LocatorSnapshot{
			CandidateKeys: []EntityKey{
				{Table: TableOrderCompanies, PK: 100},
				{Table: TableOrderTasks, PK: 1},
			},
			StableIdentities: map[EntityKey]string{
				{Table: TableOrderTasks, PK: 1}: "hash-v1-task",
			},
		}, nil
	}

	snapshot, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if err != nil {
		t.Fatalf("ExecuteFreshLocator failed: %v", err)
	}

	if len(snapshot.CandidateKeys) != 2 {
		t.Fatalf("expected 2 candidate keys, got %d", len(snapshot.CandidateKeys))
	}
	// 断言候选集被自动严格全序排序：Tasks(1) 先于 Companies(100)
	if snapshot.CandidateKeys[0].Table != TableOrderTasks {
		t.Errorf("expected first key Tasks, got %+v", snapshot.CandidateKeys[0])
	}
	if snapshot.CandidateKeys[1].Table != TableOrderCompanies {
		t.Errorf("expected second key Companies, got %+v", snapshot.CandidateKeys[1])
	}

	if snapshot.StableIdentities[EntityKey{Table: TableOrderTasks, PK: 1}] != "hash-v1-task" {
		t.Errorf("expected stable identity preserved")
	}
}

func TestExecuteFreshLocator_NilGuards(t *testing.T) {
	ctx := context.Background()
	_, err := ExecuteFreshLocator(ctx, nil, func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrNilDatabase) {
		t.Errorf("expected ErrNilDatabase, got %v", err)
	}

	db := getMockDB(t)
	defer db.Close()
	_, err = ExecuteFreshLocator(ctx, db, nil)
	if !errors.Is(err, ErrNilLocatorFunc) {
		t.Errorf("expected ErrNilLocatorFunc, got %v", err)
	}
}

func TestExecuteFreshLocator_CandidateLimitExceeded(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx := context.Background()
	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		keys := make([]EntityKey, MaxLocatorCandidates+1)
		for i := 0; i <= MaxLocatorCandidates; i++ {
			keys[i] = EntityKey{Table: TableOrderTasks, PK: uint64(i + 1)}
		}
		return &LocatorSnapshot{CandidateKeys: keys}, nil
	}

	_, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if !errors.Is(err, ErrCandidateLimitExceeded) {
		t.Errorf("expected ErrCandidateLimitExceeded, got %v", err)
	}
}

func TestExecuteFreshLocator_QueryErrorRollback(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	ctx := context.Background()
	expectedErr := errors.New("simulated locator query error")
	locatorFn := func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error) {
		return nil, expectedErr
	}

	_, err := ExecuteFreshLocator(ctx, db, locatorFn)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}
```

- [ ] **Step 2: 运行并确认失败**

运行：`go test -v ./db/lockorder/ -run "TestExecuteFreshLocator.*"`
预期：FAIL，提示 `undefined: MaxLocatorCandidates` 或 `undefined: ExecuteFreshLocator`。

- [ ] **Step 3: 最小实现**

创建 `db/lockorder/locator.go`：

```go
package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	// MaxLocatorCandidates 单次 locator 快照候选实体数量硬上限，防止无界查询拖垮事务
	MaxLocatorCandidates = 1000
)

var (
	ErrCandidateLimitExceeded = errors.New("lockorder: candidate limit exceeded in locator query")
	ErrNilDatabase            = errors.New("lockorder: nil database handle")
	ErrNilLocatorFunc         = errors.New("lockorder: nil locator function")
)

// LocatorSnapshot 只读 locator 提取的候选集快照
type LocatorSnapshot struct {
	CandidateKeys    []EntityKey
	StableIdentities map[EntityKey]string
	CreatedAt        time.Time
}

// LocatorFunc 在独立只读事务中执行的纯读查询函数
type LocatorFunc func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error)

// ExecuteFreshLocator 使用同一连接池另开独立只读事务，执行非加锁快照查询并安全回收连接
func ExecuteFreshLocator(ctx context.Context, db *sql.DB, fn LocatorFunc) (*LocatorSnapshot, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}
	if fn == nil {
		return nil, ErrNilLocatorFunc
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 明确开辟只读隔离事务：快照建立在全部门禁已获取之后，排除外层 data TX 老快照漏成员
	locTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("fresh locator begin tx: %w", err)
	}

	closed := false
	defer func() {
		if !closed {
			_ = locTx.Rollback()
		}
	}()

	snapshot, err := fn(ctx, locTx)
	if err != nil {
		return nil, fmt.Errorf("fresh locator query: %w", err)
	}
	if snapshot == nil {
		snapshot = &LocatorSnapshot{}
	}

	if len(snapshot.CandidateKeys) > MaxLocatorCandidates {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrCandidateLimitExceeded, len(snapshot.CandidateKeys), MaxLocatorCandidates)
	}

	// 显式结束独立只读事务，将连接干净交还连接池
	if err := locTx.Commit(); err != nil {
		return nil, fmt.Errorf("fresh locator commit: %w", err)
	}
	closed = true

	// 自动排序去重，保证满足全序实体加锁前置条件
	snapshot.CandidateKeys = SortEntityKeys(snapshot.CandidateKeys)
	if snapshot.StableIdentities == nil {
		snapshot.StableIdentities = make(map[EntityKey]string)
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}

	return snapshot, nil
}
```

- [ ] **Step 4: 运行并确认通过**

运行：`go test -v ./db/lockorder/ -run "TestExecuteFreshLocator.*"`
预期：PASS，只读 locator 事务提取、候选集排序、边界检查与模拟事务回滚/提交全部通过。

- [ ] **Step 5: 提交**

```bash
git add db/lockorder/locator.go db/lockorder/locator_test.go
git commit -m "feat(lockorder): implement fresh readonly locator snapshot extractor"
```

---

### Task 3: 实体锁有序执行器与当前态验证器

**Files:**
- Create: `db/lockorder/locker.go`
- Create: `db/lockorder/locker_test.go`

**Interfaces:**
- Consumes:
  - `database/sql`
  - `context`
  - `fmt`
  - `errors`
  - `EntityKey`, `TableMeta`, `LookupTableOrder`, `LockContext`, `IsSorted` (from Task 1)
  - `LocatorSnapshot` (from Task 2)
- Produces:
  - `var ErrMembershipDrift error`
  - `var ErrEntityNotFound error`
  - `var ErrLowerPKDiscovered error`
  - `var ErrNewMemberDiscovered error`
  - `type EntityState struct { Key EntityKey; StableIdentity string; Live bool; Version uint64 }`
  - `type QueryRower interface { QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row }`
  - `type CurrentVerifier func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error)`
  - `func BuildLockSQL(meta TableMeta) string`
  - `func LockEntitiesInOrder(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, keys []EntityKey) error`
  - `func VerifyCurrentStates(ctx context.Context, q QueryRower, keys []EntityKey, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error)`
  - `func LockAndVerify(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error)`

- [ ] **Step 1: 编写失败测试**

创建 `db/lockorder/locker_test.go`：

```go
package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestBuildLockSQL(t *testing.T) {
	tasksMeta, _ := LookupTableName("tasks")
	tasksSQL := BuildLockSQL(tasksMeta)
	expectedTasks := "SELECT id FROM tasks WHERE id = ? FOR UPDATE"
	if tasksSQL != expectedTasks {
		t.Errorf("tasks: expected '%s', got '%s'", expectedTasks, tasksSQL)
	}

	sideMeta, _ := LookupTableName("side_question_requests")
	sideSQL := BuildLockSQL(sideMeta)
	expectedSide := "SELECT ordinal FROM side_question_requests WHERE ordinal = ? FOR UPDATE"
	if sideSQL != expectedSide {
		t.Errorf("side_question_requests: expected '%s', got '%s'", expectedSide, sideSQL)
	}

	healthMeta, _ := LookupTableName("llm_profile_health")
	healthSQL := BuildLockSQL(healthMeta)
	expectedHealth := "SELECT profile_id FROM llm_profile_health WHERE profile_id = ? FOR UPDATE"
	if healthSQL != expectedHealth {
		t.Errorf("llm_profile_health: expected '%s', got '%s'", expectedHealth, healthSQL)
	}

	locksMeta, _ := LookupTableName("locks")
	locksSQL := BuildLockSQL(locksMeta)
	expectedLocks := "SELECT row_id FROM locks WHERE row_id = ? FOR UPDATE"
	if locksSQL != expectedLocks {
		t.Errorf("locks: expected '%s', got '%s'", expectedLocks, locksSQL)
	}
}

func TestVerifyCurrentStates_Success(t *testing.T) {
	ctx := context.Background()
	keys := []EntityKey{
		{Table: TableOrderTasks, PK: 1},
		{Table: TableOrderTasks, PK: 2},
	}

	snapshot := &LocatorSnapshot{
		CandidateKeys: keys,
		StableIdentities: map[EntityKey]string{
			keys[0]: "hash-v1",
			keys[1]: "hash-v2",
		},
	}

	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{
			Key:            key,
			StableIdentity: snapshot.StableIdentities[key],
			Live:           true,
			Version:        1,
		}, nil
	}

	states, err := VerifyCurrentStates(ctx, nil, keys, snapshot, verifier)
	if err != nil {
		t.Fatalf("expected VerifyCurrentStates to succeed, got %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 states, got %d", len(states))
	}
}

func TestVerifyCurrentStates_MembershipDrift_IdentityChanged(t *testing.T) {
	ctx := context.Background()
	keys := []EntityKey{
		{Table: TableOrderTasks, PK: 1},
	}

	snapshot := &LocatorSnapshot{
		CandidateKeys: keys,
		StableIdentities: map[EntityKey]string{
			keys[0]: "old-hash",
		},
	}

	// 模拟实体逻辑哈希发生漂移
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{
			Key:            key,
			StableIdentity: "new-drifted-hash",
			Live:           true,
			Version:        2,
		}, nil
	}

	_, err := VerifyCurrentStates(ctx, nil, keys, snapshot, verifier)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Errorf("expected ErrMembershipDrift, got %v", err)
	}
}

func TestVerifyCurrentStates_MembershipDrift_EntityNotLive(t *testing.T) {
	ctx := context.Background()
	keys := []EntityKey{
		{Table: TableOrderTasks, PK: 1},
	}

	snapshot := &LocatorSnapshot{
		CandidateKeys: keys,
		StableIdentities: map[EntityKey]string{
			keys[0]: "hash",
		},
	}

	// 模拟实体已被删除或标记为非 live
	verifier := func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error) {
		return &EntityState{
			Key:            key,
			StableIdentity: "hash",
			Live:           false, // 已软删除
		}, nil
	}

	_, err := VerifyCurrentStates(ctx, nil, keys, snapshot, verifier)
	if !errors.Is(err, ErrMembershipDrift) {
		t.Errorf("expected ErrMembershipDrift, got %v", err)
	}
}

func TestLockEntitiesInOrder_UnsortedKeysRejected(t *testing.T) {
	ctx := context.Background()
	lc := NewLockContext()
	unsorted := []EntityKey{
		{Table: TableOrderTasks, PK: 20},
		{Table: TableOrderTasks, PK: 10}, // 逆序
	}

	err := LockEntitiesInOrder(ctx, nil, lc, unsorted)
	if !errors.Is(err, ErrOutOfOrderLock) {
		t.Errorf("expected ErrOutOfOrderLock for unsorted keys, got %v", err)
	}
}
```

- [ ] **Step 2: 运行并确认失败**

运行：`go test -v ./db/lockorder/ -run "TestBuildLockSQL|TestVerifyCurrentStates.*|TestLockEntitiesInOrder.*"`
预期：FAIL，提示 `undefined: BuildLockSQL` 或 `undefined: ErrMembershipDrift`。

- [ ] **Step 3: 最小实现**

创建 `db/lockorder/locker.go`：

```go
package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrMembershipDrift       = errors.New("lockorder: membership drift detected during current verification")
	ErrEntityNotFound        = errors.New("lockorder: entity not found under lock")
	ErrLowerPKDiscovered     = errors.New("lockorder: lower PK discovered, backwards locking prohibited")
	ErrNewMemberDiscovered   = errors.New("lockorder: new member discovered, full rollback required")
)

// EntityState 实体当前态数据
type EntityState struct {
	Key            EntityKey
	StableIdentity string
	Live           bool
	Version        uint64
}

// QueryRower 查询接口抽象
type QueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// CurrentVerifier 当前态验证函数类型
type CurrentVerifier func(ctx context.Context, q QueryRower, key EntityKey) (*EntityState, error)

// BuildLockSQL 根据表元信息生成精确的单主键加锁 SQL
func BuildLockSQL(meta TableMeta) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? FOR UPDATE", meta.PKColumn, meta.Name, meta.PKColumn)
}

// LockEntitiesInOrder 外层数据事务按严格 (table-order, 物理 PK 升序) 执行 FOR UPDATE
func LockEntitiesInOrder(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, keys []EntityKey) error {
	if !IsSorted(keys) {
		return fmt.Errorf("%w: candidate keys must be strictly sorted before locking", ErrOutOfOrderLock)
	}

	for _, key := range keys {
		// 登记到锁上下文，验证未违反全序规则
		if err := lockCtx.RecordEntityLock(key); err != nil {
			return err
		}

		// 若 tx 为 nil（用于离线单测），仅验证锁序
		if tx == nil {
			continue
		}

		meta, err := LookupTableOrder(key.Table)
		if err != nil {
			return err
		}

		query := BuildLockSQL(meta)
		var lockedPK uint64
		row := tx.QueryRowContext(ctx, query, key.PK)
		if err := row.Scan(&lockedPK); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: table %s pk %d", ErrEntityNotFound, meta.Name, key.PK)
			}
			return fmt.Errorf("lock entity %s: %w", key, err)
		}
	}
	return nil
}

// VerifyCurrentStates 在持有锁后执行 current read，验证逻辑身份与活性
func VerifyCurrentStates(ctx context.Context, q QueryRower, keys []EntityKey, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if verifier == nil {
		return nil, errors.New("lockorder: nil CurrentVerifier")
	}

	states := make([]*EntityState, 0, len(keys))
	for _, key := range keys {
		st, err := verifier(ctx, q, key)
		if err != nil {
			return nil, fmt.Errorf("verify entity %s: %w", key, err)
		}
		if st == nil {
			return nil, fmt.Errorf("%w: entity %s returned nil state", ErrMembershipDrift, key)
		}
		if !st.Live {
			return nil, fmt.Errorf("%w: entity %s is marked inactive or deleted", ErrMembershipDrift, key)
		}

		if snapshot != nil && snapshot.StableIdentities != nil {
			if expectedIdent, ok := snapshot.StableIdentities[key]; ok {
				if st.StableIdentity != expectedIdent {
					return nil, fmt.Errorf("%w: entity %s identity mismatch (expected %s, got %s)",
						ErrMembershipDrift, key, expectedIdent, st.StableIdentity)
				}
			}
		}
		states = append(states, st)
	}
	return states, nil
}

// LockAndVerify 将排序加锁与当前态核验原子组合
func LockAndVerify(ctx context.Context, tx *sql.Tx, lockCtx *LockContext, snapshot *LocatorSnapshot, verifier CurrentVerifier) ([]*EntityState, error) {
	if snapshot == nil {
		return nil, errors.New("lockorder: nil snapshot")
	}

	// 1. 严格全序加锁
	if err := LockEntitiesInOrder(ctx, tx, lockCtx, snapshot.CandidateKeys); err != nil {
		return nil, err
	}

	// 2. 持锁下立即进行 current read 核验
	var qr QueryRower
	if tx != nil {
		qr = tx
	}
	states, err := VerifyCurrentStates(ctx, qr, snapshot.CandidateKeys, snapshot, verifier)
	if err != nil {
		// 核验失败直接透出 ErrMembershipDrift 或内部错误，调用层必须整体回滚，严禁追加补锁
		return nil, err
	}
	return states, nil
}
```

- [ ] **Step 4: 运行并确认通过**

运行：`go test -v ./db/lockorder/ -run "TestBuildLockSQL|TestVerifyCurrentStates.*|TestLockEntitiesInOrder.*"`
预期：PASS，所有加锁 SQL 构建、全序锁约束与当前态核验测试全部通过。

- [ ] **Step 5: 提交**

```bash
git add db/lockorder/locker.go db/lockorder/locker_test.go
git commit -m "feat(lockorder): implement ordered entity locker and current state verifier"
```

---

### Task 4: 事务退避重试包装器

**Files:**
- Create: `db/lockorder/tx_runner.go`
- Create: `db/lockorder/tx_runner_test.go`

**Interfaces:**
- Consumes:
  - `database/sql`
  - `context`
  - `time`
  - `fmt`
  - `errors`
  - `sync`
  - `LockContext`, `LockEvent` (from Task 1)
  - `ErrMembershipDrift`, `ErrLowerPKDiscovered`, `ErrNewMemberDiscovered` (from Task 3)
- Produces:
  - `var DefaultBackoffSchedule []time.Duration`
  - `var ErrRetryExhausted error`
  - `func IsRetryableConflict(err error) bool`
  - `type GateManager interface`
  - `type MemoryGateManager struct`
  - `func NewMemoryGateManager() *MemoryGateManager`
  - `type TxRunnerOptions struct { RequiresBusinessGate bool; RequiredNamespaces []string; BackoffSchedule []time.Duration; GateManager GateManager }`
  - `type TxPayloadFunc func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error`
  - `type TxRunner struct`
  - `func NewTxRunner(db *sql.DB) *TxRunner`
  - `func (r *TxRunner) SetSleeper(sleeper func(time.Duration))`
  - `func (r *TxRunner) Run(ctx context.Context, opts TxRunnerOptions, payload TxPayloadFunc) error`

- [ ] **Step 1: 编写失败测试**

创建 `db/lockorder/tx_runner_test.go`：

```go
package lockorder

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// 模拟 MySQL 驱动错误
type mockMySQLError struct {
	Number  uint16
	Message string
}

func (e *mockMySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Number, e.Message)
}

func TestIsRetryableConflict(t *testing.T) {
	cases := []struct {
		err      error
		expected bool
	}{
		{&mockMySQLError{Number: 1213, Message: "Deadlock found"}, true},
		{&mockMySQLError{Number: 1205, Message: "Lock wait timeout"}, true},
		{&mockMySQLError{Number: 1062, Message: "Duplicate entry"}, true},
		{ErrMembershipDrift, true},
		{ErrLowerPKDiscovered, true},
		{ErrNewMemberDiscovered, true},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
		{errors.New("some business validation error"), false},
	}

	for _, c := range cases {
		if got := IsRetryableConflict(c.err); got != c.expected {
			t.Errorf("IsRetryableConflict(%v): expected %v, got %v", c.err, c.expected, got)
		}
	}
}

func TestTxRunner_SuccessFirstAttempt(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
		RequiredNamespaces:   []string{"ns-1"},
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if len(sleepCalls) != 0 {
		t.Errorf("expected 0 backoff sleeps, got %d", len(sleepCalls))
	}
}

func TestTxRunner_RetryOnMembershipDrift(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		if attempts < 3 {
			// 前两次报告成员漂移
			return ErrMembershipDrift
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected final success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if len(sleepCalls) != 2 {
		t.Fatalf("expected 2 sleeps, got %d", len(sleepCalls))
	}
	if sleepCalls[0] != 100*time.Millisecond || sleepCalls[1] != 250*time.Millisecond {
		t.Errorf("unexpected sleep schedule: %v", sleepCalls)
	}
}

func TestTxRunner_RetryExhausted(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return &mockMySQLError{Number: 1213, Message: "Deadlock"}
	})

	if !errors.Is(err, ErrRetryExhausted) {
		t.Errorf("expected ErrRetryExhausted, got %v", err)
	}
	// 4 档退避 (100, 250, 500, 1000ms)，共 5 次尝试
	if attempts != 5 {
		t.Errorf("expected 5 attempts, got %d", attempts)
	}
	if len(sleepCalls) != 4 {
		t.Fatalf("expected 4 sleeps, got %d", len(sleepCalls))
	}
	expectedSleeps := []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1000 * time.Millisecond,
	}
	for i, s := range sleepCalls {
		if s != expectedSleeps[i] {
			t.Errorf("sleep %d: expected %v, got %v", i, expectedSleeps[i], s)
		}
	}
}

func TestTxRunner_NonRetryableFailsImmediately(t *testing.T) {
	db := getMockDB(t)
	defer db.Close()

	runner := NewTxRunner(db)
	var sleepCalls []time.Duration
	runner.SetSleeper(func(d time.Duration) {
		sleepCalls = append(sleepCalls, d)
	})

	expectedErr := errors.New("business rule broken")
	attempts := 0
	err := runner.Run(context.Background(), TxRunnerOptions{
		RequiresBusinessGate: true,
	}, func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error {
		attempts++
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if len(sleepCalls) != 0 {
		t.Errorf("expected 0 sleeps, got %d", len(sleepCalls))
	}
}

func TestMemoryGateManager_ReverseRelease(t *testing.T) {
	gm := NewMemoryGateManager()
	ctx := context.Background()

	if err := gm.AcquireLocalBusiness(ctx); err != nil {
		t.Fatalf("acquire business gate failed: %v", err)
	}
	if err := gm.AcquireLocalNamespace(ctx, "ns-a"); err != nil {
		t.Fatalf("acquire ns-a failed: %v", err)
	}
	if err := gm.AcquireLocalNamespace(ctx, "ns-b"); err != nil {
		t.Fatalf("acquire ns-b failed: %v", err)
	}

	// 逆序释放
	gm.ReleaseLocalNamespace("ns-b")
	gm.ReleaseLocalNamespace("ns-a")
	gm.ReleaseLocalBusiness()

	// 重新获取应成功无阻塞
	if err := gm.AcquireLocalBusiness(ctx); err != nil {
		t.Fatalf("re-acquire business gate failed: %v", err)
	}
	gm.ReleaseLocalBusiness()
}
```

- [ ] **Step 2: 运行并确认失败**

运行：`go test -v ./db/lockorder/ -run "TestIsRetryableConflict|TestTxRunner.*|TestMemoryGateManager.*"`
预期：FAIL，提示 `undefined: IsRetryableConflict` 或 `undefined: NewTxRunner`。

- [ ] **Step 3: 最小实现**

创建 `db/lockorder/tx_runner.go`：

```go
package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	// DefaultBackoffSchedule 规格 §4.3、§10.4-T 定义的 100/250/500ms/1s 指数退避阶梯
	DefaultBackoffSchedule = []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1000 * time.Millisecond,
	}

	ErrRetryExhausted = errors.New("lockorder: transaction retry attempts exhausted")
)

// mysqlErrorNumberExtractor 兼容从不同 mysql driver 错误中提取状态码
type mysqlErrorNumberExtractor interface {
	error
	// 部分驱动或自定义包装包含此接口
}

// IsRetryableConflict 判断错误是否为死锁、超时、唯一键冲突或成员漂移等可重试冲突
func IsRetryableConflict(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrMembershipDrift) ||
		errors.Is(err, ErrLowerPKDiscovered) ||
		errors.Is(err, ErrNewMemberDiscovered) {
		return true
	}

	// 排除主动取消或超时的上下文错误
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// 错误编号探测（1062, 1213, 1205）
	type numberer interface {
		Number() uint16
	}
	var numGetter numberer
	if errors.As(err, &numGetter) {
		switch numGetter.Number() {
		case 1062, 1213, 1205:
			return true
		}
	}

	// 结构体直接字段反射匹配测试 mock 与原生驱动
	type numField struct {
		Number uint16
	}
	var nf interface{ Error() string }
	if errors.As(err, &nf) {
		type hasNum interface {
			Number() uint16
		}
		// 检查常规包含 Number uint16 导出的结构
		type errNum interface {
			Number() uint16
		}
		var en errNum
		if errors.As(err, &en) {
			switch en.Number() {
			case 1062, 1213, 1205:
				return true
			}
		}
	}

	// 备选字符串特征匹配（兜底非结构化包装）
	msg := err.Error()
	if containsSubstr(msg, "1213") || containsSubstr(msg, "Deadlock") ||
		containsSubstr(msg, "1205") || containsSubstr(msg, "Lock wait timeout") ||
		containsSubstr(msg, "1062") || containsSubstr(msg, "Duplicate entry") {
		return true
	}

	return false
}

func containsSubstr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && stringSearch(s, substr)))
}

func stringSearch(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// GateManager 本地门禁管理器接口
type GateManager interface {
	AcquireLocalBusiness(ctx context.Context) error
	ReleaseLocalBusiness()
	AcquireLocalNamespace(ctx context.Context, ns string) error
	ReleaseLocalNamespace(ns string)
}

// MemoryGateManager 基于内存读写锁的本地门禁实现
type MemoryGateManager struct {
	businessMu sync.Mutex
	nsMu       sync.Mutex
	nsLocks    map[string]*sync.Mutex
}

// NewMemoryGateManager 创建基于内存的门禁管理器
func NewMemoryGateManager() *MemoryGateManager {
	return &MemoryGateManager{
		nsLocks: make(map[string]*sync.Mutex),
	}
}

func (m *MemoryGateManager) AcquireLocalBusiness(ctx context.Context) error {
	m.businessMu.Lock()
	return nil
}

func (m *MemoryGateManager) ReleaseLocalBusiness() {
	m.businessMu.Unlock()
}

func (m *MemoryGateManager) getNSMutex(ns string) *sync.Mutex {
	m.nsMu.Lock()
	defer m.nsMu.Unlock()
	mu, ok := m.nsLocks[ns]
	if !ok {
		mu = &sync.Mutex{}
		m.nsLocks[ns] = mu
	}
	return mu
}

func (m *MemoryGateManager) AcquireLocalNamespace(ctx context.Context, ns string) error {
	mu := m.getNSMutex(ns)
	mu.Lock()
	return nil
}

func (m *MemoryGateManager) ReleaseLocalNamespace(ns string) {
	mu := m.getNSMutex(ns)
	mu.Unlock()
}

// TxRunnerOptions 事务运行器配置
type TxRunnerOptions struct {
	RequiresBusinessGate bool
	RequiredNamespaces   []string
	BackoffSchedule      []time.Duration
	GateManager          GateManager
}

// TxPayloadFunc 在全序与门禁保护下的核心事务执行逻辑
type TxPayloadFunc func(ctx context.Context, tx *sql.Tx, lockCtx *LockContext) error

// TxRunner 全事务回滚与退避重试执行器
type TxRunner struct {
	db      *sql.DB
	sleeper func(time.Duration)
}

// NewTxRunner 创建事务运行器
func NewTxRunner(db *sql.DB) *TxRunner {
	return &TxRunner{
		db:      db,
		sleeper: time.Sleep,
	}
}

// SetSleeper 设置用于单测模拟的休眠函数
func (r *TxRunner) SetSleeper(sleeper func(time.Duration)) {
	if sleeper != nil {
		r.sleeper = sleeper
	}
}

// Run 执行带全序门禁与有界退避重试的悲观事务
func (r *TxRunner) Run(ctx context.Context, opts TxRunnerOptions, payload TxPayloadFunc) error {
	if payload == nil {
		return errors.New("lockorder: nil payload function")
	}

	gm := opts.GateManager
	if gm == nil {
		gm = NewMemoryGateManager()
	}

	schedule := opts.BackoffSchedule
	if len(schedule) == 0 {
		schedule = DefaultBackoffSchedule
	}
	maxAttempts := len(schedule) + 1

	// 预先对 namespace 按名称升序排序，严格保证全序
	sortedNamespaces := make([]string, len(opts.RequiredNamespaces))
	copy(sortedNamespaces, opts.RequiredNamespaces)
	sort.Strings(sortedNamespaces)

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			sleepDur := schedule[attempt-1]
			r.sleeper(sleepDur)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// 创建当前尝试独立的锁上下文
		lockCtx := NewLockContext()

		// 1. 获取本地门禁（Local Gates）
		var heldLocalBusiness bool
		var heldLocalNamespaces []string

		acquireLocalGates := func() error {
			if opts.RequiresBusinessGate {
				if err := gm.AcquireLocalBusiness(ctx); err != nil {
					return err
				}
				heldLocalBusiness = true
				if err := lockCtx.RecordLocalBusinessGate(); err != nil {
					return err
				}
			}

			for _, ns := range sortedNamespaces {
				if err := gm.AcquireLocalNamespace(ctx, ns); err != nil {
					return err
				}
				heldLocalNamespaces = append(heldLocalNamespaces, ns)
				if err := lockCtx.RecordLocalNamespaceGate(ns); err != nil {
					return err
				}
			}
			return nil
		}

		// 逆序释放本地门禁闭包
		releaseLocalGates := func() {
			// 先逆序释放 namespace gates
			for i := len(heldLocalNamespaces) - 1; i >= 0; i-- {
				gm.ReleaseLocalNamespace(heldLocalNamespaces[i])
			}
			heldLocalNamespaces = nil

			// 最后释放 business gate
			if heldLocalBusiness {
				gm.ReleaseLocalBusiness()
				heldLocalBusiness = false
			}
		}

		if err := acquireLocalGates(); err != nil {
			releaseLocalGates()
			return err
		}

		// 2. 开始外层数据事务
		var tx *sql.Tx
		var err error
		if r.db != nil {
			tx, err = r.db.BeginTx(ctx, nil)
			if err != nil {
				releaseLocalGates()
				return fmt.Errorf("begin outer data tx: %w", err)
			}
		}

		// 3. 在外层数据事务内获取 DB Gate 锁
		acquireDBGates := func() error {
			if opts.RequiresBusinessGate {
				if err := lockCtx.RecordDBBusinessGate(); err != nil {
					return err
				}
				if tx != nil {
					if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES ('business_relations')"); err != nil {
						return fmt.Errorf("lock db business_relations: insert ignore: %w", err)
					}
					var name string
					row := tx.QueryRowContext(ctx, "SELECT name FROM locks WHERE name = 'business_relations' FOR UPDATE")
					if err := row.Scan(&name); err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							return fmt.Errorf("lock db business_relations: row not found under lock")
						}
						return fmt.Errorf("lock db business_relations: %w", err)
					}
				}
			}

			for _, ns := range sortedNamespaces {
				if err := lockCtx.RecordDBNamespaceGate(ns); err != nil {
					return err
				}
				if tx != nil {
					if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES (?)", ns); err != nil {
						return fmt.Errorf("lock db namespace %s: insert ignore: %w", ns, err)
					}
					var name string
					row := tx.QueryRowContext(ctx, "SELECT name FROM locks WHERE name = ? FOR UPDATE", ns)
					if err := row.Scan(&name); err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							return fmt.Errorf("lock db namespace %s: row not found under lock", ns)
						}
						return fmt.Errorf("lock db namespace %s: %w", ns, err)
					}
				}
			}
			return nil
		}

		gateErr := acquireDBGates()
		if gateErr != nil {
			if tx != nil {
				_ = tx.Rollback()
			}
			releaseLocalGates()
			lockCtx.Reset()
			if IsRetryableConflict(gateErr) {
				lastErr = gateErr
				continue
			}
			return gateErr
		}

		// 4. 执行业务载荷（包含 fresh locator 与实体加锁重验）
		payloadErr := payload(ctx, tx, lockCtx)
		if payloadErr == nil {
			// 5. 提交事务
			var commitErr error
			if tx != nil {
				commitErr = tx.Commit()
			}
			if commitErr == nil {
				// 成功提交：逆序释放本地门禁，返回成功
				releaseLocalGates()
				return nil
			}
			if isOutcomeUnknown(commitErr) {
				log.Printf("[lockorder] tx commit outcome unknown: %v; holding local gates", commitErr)
				return fmt.Errorf("%w: %v", ErrOutcomeUnknown, commitErr)
			}
			payloadErr = commitErr
		}

		// 执行失败或提交失败：整体回滚外层事务，逆序释放全部门禁
		if tx != nil {
			_ = tx.Rollback()
		}
		releaseLocalGates()
		lockCtx.Reset()

		if IsRetryableConflict(payloadErr) {
			lastErr = payloadErr
			continue
		}

		// 非可重试错误：立即 fail closed
		return payloadErr
	}

	return fmt.Errorf("%w: attempts exhausted (last error: %v)", ErrRetryExhausted, lastErr)
}
```

- [ ] **Step 4: 运行并确认通过**

运行：`go test -v ./db/lockorder/ -run "TestIsRetryableConflict|TestTxRunner.*|TestMemoryGateManager.*"`
预期：PASS，全部错误分类、单次成功、死锁/漂移指数退避重试、耗尽失败与逆序门禁释放测试通过。

- [ ] **Step 5: 提交**

```bash
git add db/lockorder/tx_runner.go db/lockorder/tx_runner_test.go
git commit -m "feat(lockorder): implement transaction runner with exponential backoff and total gate release"
```

---

### Task 5: 并发交叉锁与全序死锁免疫集成测试

**Files:**
- Create: `db/lockorder/deadlock_test.go`

**Interfaces:**
- Consumes:
  - `testing`
  - `context`
  - `sync`
  - `os`
  - `database/sql`
  - `github.com/Autumn-27/artex/db/lockorder` (Tasks 1-4)
- Produces:
  - `TestDeadlockImmunity_UnitReverseOrder(t *testing.T)`
  - `TestDeadlockImmunity_TiDBIntegration(t *testing.T)`

- [ ] **Step 1: 编写集成测试源码**

创建 `db/lockorder/deadlock_test.go`：

```go
package lockorder_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	_ "github.com/go-sql-driver/mysql"
)

// TestDeadlockImmunity_UnitReverseOrder 在纯单元测试层验证全序排序器对并发反序请求的死锁免疫
func TestDeadlockImmunity_UnitReverseOrder(t *testing.T) {
	// 模拟两个 goroutine 以相反的业务顺序请求锁：
	// Goroutine A 请求顺序: Tasks(10), Companies(5)
	// Goroutine B 请求顺序: Companies(5), Tasks(10)
	// 若无全序排序器直接加锁将形成循环等待死锁；
	// 经过 SortEntityKeys 排序后，二者均按 Tasks(10) -> Companies(5) 顺序执行，零死锁。

	reqA := []lockorder.EntityKey{
		{Table: lockorder.TableOrderTasks, PK: 10},
		{Table: lockorder.TableOrderCompanies, PK: 5},
	}
	reqB := []lockorder.EntityKey{
		{Table: lockorder.TableOrderCompanies, PK: 5},
		{Table: lockorder.TableOrderTasks, PK: 10},
	}

	sortedA := lockorder.SortEntityKeys(reqA)
	sortedB := lockorder.SortEntityKeys(reqB)

	if len(sortedA) != 2 || len(sortedB) != 2 {
		t.Fatalf("unexpected sorted lengths")
	}

	for i := 0; i < 2; i++ {
		if sortedA[i] != sortedB[i] {
			t.Fatalf("expected identical total order at slot %d: A=%+v, B=%+v", i, sortedA[i], sortedB[i])
		}
	}

	// 确认第一位均为 Tasks (TableOrder 1)，第二位均为 Companies (TableOrder 5)
	if sortedA[0].Table != lockorder.TableOrderTasks || sortedA[1].Table != lockorder.TableOrderCompanies {
		t.Errorf("expected Tasks then Companies, got %+v", sortedA)
	}
}

// TestDeadlockImmunity_TiDBIntegration 在真实外部 TiDB 环境下执行高并发交叉行锁压力测试
func TestDeadlockImmunity_TiDBIntegration(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("跳过：环境变量 ARTEX_TIDB_DSN 未配置，标记为「未验证」(Honest Skip)")
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open tidb dsn failed: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping tidb failed: %v", err)
	}

	// 准备临时测试行，使用 locks 表的实体行隔离测试
	_, err = db.ExecContext(ctx, "INSERT IGNORE INTO locks (name) VALUES ('deadlock_test_1'), ('deadlock_test_2')")
	if err != nil {
		t.Fatalf("prepare test rows in locks table: %v", err)
	}

	metaLocks, err := lockorder.LookupTableName("locks")
	if err != nil {
		t.Fatalf("lookup locks meta: %v", err)
	}

	var rowID1, rowID2 uint64
	if err := db.QueryRowContext(ctx, "SELECT row_id FROM locks WHERE name = 'deadlock_test_1'").Scan(&rowID1); err != nil {
		t.Fatalf("query rowID1: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT row_id FROM locks WHERE name = 'deadlock_test_2'").Scan(&rowID2); err != nil {
		t.Fatalf("query rowID2: %v", err)
	}

	runner := lockorder.NewTxRunner(db)
	const concurrentWorkers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, concurrentWorkers)

	for i := 0; i < concurrentWorkers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			// 奇数 worker 业务序 (row1, row2)，偶数 worker 反序 (row2, row1)
			rawCandidates := []lockorder.EntityKey{
				{Table: metaLocks.Order, PK: rowID1},
				{Table: metaLocks.Order, PK: rowID2},
			}
			if workerID%2 == 1 {
				rawCandidates = []lockorder.EntityKey{
					{Table: metaLocks.Order, PK: rowID2},
					{Table: metaLocks.Order, PK: rowID1},
				}
			}

			err := runner.Run(ctx, lockorder.TxRunnerOptions{
				RequiresBusinessGate: false,
			}, func(txCtx context.Context, tx *sql.Tx, lockCtx *lockorder.LockContext) error {
				// 模拟通过 Locator 收集并排序候选集
				sorted := lockorder.SortEntityKeys(rawCandidates)
				snapshot := &lockorder.LocatorSnapshot{
					CandidateKeys: sorted,
				}

				// 严格全序加锁并验证当前态
				verifier := func(vCtx context.Context, q lockorder.QueryRower, k lockorder.EntityKey) (*lockorder.EntityState, error) {
					return &lockorder.EntityState{Key: k, Live: true}, nil
				}

				_, lockErr := lockorder.LockAndVerify(txCtx, tx, lockCtx, snapshot, verifier)
				if lockErr != nil {
					return lockErr
				}

				// 模拟事务工作耗时
				time.Sleep(10 * time.Millisecond)
				return nil
			})

			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("worker encountered error under concurrent load: %v", err)
	}
}
```

- [ ] **Step 2: 运行并确认未验证与跳过行为**

运行：`go test -v ./db/lockorder/ -run "TestDeadlockImmunity.*"`
预期：PASS（当未设置 `ARTEX_TIDB_DSN` 时，离线单元测试通过，真实集成测试诚实 `t.Skip` 输出 `跳过：环境变量 ARTEX_TIDB_DSN 未配置，标记为「未验证」(Honest Skip)`，无假阳性假冒通过）。

- [ ] **Step 3: 最小实现**

（Task 1 至 Task 4 的实现均已就绪，Task 5 的测试用例天然通过离线门控与真实 TiDB 适配）。

- [ ] **Step 4: 运行确认无编译错误与测试正常通过**

运行：`go test -v ./db/lockorder/ -run "TestDeadlockImmunity.*"`
预期：PASS（Skip 或通过）。

- [ ] **Step 5: 提交**

```bash
git add db/lockorder/deadlock_test.go
git commit -m "test(lockorder): add deadlock immunity concurrency and real TiDB integration tests"
```

---

## 与其它计划的接口

1. **← P01（MySQL driver 与连接层）**：
   - 本计划直接使用 P01 注册的 driver 名 `"mysql"`，以及由 `db/tidb.OpenTiDB` 初始化的标准 `*sql.DB` 共享连接池。
   - 错误分类：本计划在遇到死锁（1213）、锁等待超时（1205）或唯一键冲突（1062）时，通过 `IsRetryableConflict` 统一拦截并对齐 P01 的错误分类，执行 100/250/500ms/1s 指数退避重试。
2. **← P04a（入口校验与级联闭包引擎）**：
   - 只读闭包集成：P04a 定义的级联删除闭包收集函数（`ComputeTaskClosure` 等）作为只读操作，挂载在 P04b 的 `ExecuteFreshLocator` 独立只读事务中安全运行，收集待处理子孙实体的候选 PK 集合；
   - 级联加锁协议：在进入 P04a 的物理/逻辑级联清理之前，外层数据事务必须经由 P04b 按 `(table-order, 物理 PK 升序)` 完成实体行级悲观锁加锁，杜绝清理过程中的并发孤儿写入。
3. **← P03（64 位 ID 分配与 DTO 契约）**：
   - 实体 ID 统一契约：所有 `EntityKey` 内部主键 `PK` 统一使用 `uint64` 存储与计算；
   - 隐藏 `row_id` 物理封装：全库 26 张无业务自增主键表使用的单列物理主键 `row_id` 仅用于底层 `TableMeta.PKColumn` 构建 `FOR UPDATE` 语句，绝不暴露至对外 API 或 DTO。
4. **→ P08（部署与运行时绑定）**：
   - 本计划的所有锁全序、fresh locator 快照与死锁规避机制纯粹由 ARTEX 应用进程与共享连接池驱动，不向外部 TiDB 集群提出任何参数调优（不要求关闭 TiKV `pipelined`/`in-memory`）或配置查询（`SHOW CONFIG`）要求，完全满足 P08 规定的外部 TiDB 部署基准。

---

## 本计划不覆盖

- 全库 65 表 DDL 迁移与 schema 初始化（由 P02 覆盖）。
- 应用层外键存在性校验与级联逆拓扑删除执行器实现（由 P04a 覆盖）。
- 64 位数字 ID 的前端 TypeScript 强转消除与 API/DTO string 序列化改造（由 P03 覆盖）。
- 离线迁移命令与部署凭据绑定（由 P08 覆盖）。
- 流量搜索 KMP 与正文 Spooling 预算控制（由独立流量改造计划覆盖）。
- 外部直接通过 MySQL 客户端直连数据库绕过应用层执行 DML 导致的死锁与并发冲突（规格 §1.3 明确免责，外部 raw SQL 不受本全序防线保护）。
