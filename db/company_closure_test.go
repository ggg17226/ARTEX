package db

import (
	"testing"
)

func TestCompanyCascadeClosure_Totals(t *testing.T) {
	closure := &CompanyCascadeClosure{
		CompanyID:       1,
		ScopeIDs:        []uint64{10, 11},
		TaskScopeIDs:    []uint64{20},
		NullifyAssetIDs: []uint64{30, 31, 32},
		NullifyTaskIDs:  []uint64{40},
	}
	if closure.TotalCascadeDeleteCount() != 4 { // 1(company) + 2(scope) + 1(task_scope)
		t.Fatalf("expected 4 deletes, got %d", closure.TotalCascadeDeleteCount())
	}
	if closure.TotalNullifyCount() != 4 { // 3(assets) + 1(tasks)
		t.Fatalf("expected 4 nullifies, got %d", closure.TotalNullifyCount())
	}
}
