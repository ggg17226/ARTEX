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
	c.mu.Lock()
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
			return fmt.Errorf("%w: local namespace %s already held", ErrDuplicateLock, ns)
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
			return fmt.Errorf("%w: DB namespace %s already held", ErrDuplicateLock, ns)
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
