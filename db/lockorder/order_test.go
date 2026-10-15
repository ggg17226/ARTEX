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
