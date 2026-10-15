package db

import (
	"context"
	"database/sql"
	"testing"
)

// fakeTaskClosureDB 模拟闭包查询返回的一组数据
type fakeTaskClosureDB struct {
	taskID        uint64
	explorationID uint64
	nodeIDs       []uint64
	sessionKeys   []string
}

func (f *fakeTaskClosureDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return nil
}

func (f *fakeTaskClosureDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}

func (f *fakeTaskClosureDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, nil
}

func TestTaskCascadeClosure_TotalEntitiesCount(t *testing.T) {
	closure := &TaskCascadeClosure{
		TaskID:              1,
		ExplorationID:       2,
		ArchiveIDs:          []uint64{10},
		RelationIDs:         []uint64{20, 21},
		AssetLinkIDs:        []uint64{30},
		LLMProfileLinkIDs:   []uint64{40},
		ScopeIDs:            []uint64{50, 51},
		InterceptRuleIDs:    []uint64{60},
		SideSessionKeys:     []string{"sess-1"},
		SideRequestOrdinals: []uint64{100, 101},
		EdgeIDs:             []uint64{200},
		AnchorIDs:           []uint64{300},
		ConstraintIDs:       []uint64{400},
		ActivityIDs:         []uint64{500, 501},
		MainSessionIDs:      []uint64{600},
		NodeIDs:             []uint64{700, 701},
	}

	count := closure.TotalCascadeDeleteCount()
	// 期望实体总数：1(task) + 1(exp) + 1 + 2 + 1 + 1 + 2 + 1 + 1(sess) + 2(req) + 1 + 1 + 1 + 2 + 1 + 2 = 21
	if count != 21 {
		t.Fatalf("expected total cascade delete count 21, got %d", count)
	}
}

func TestTaskCascadeClosure_ValidateSelfConsistency(t *testing.T) {
	closure := &TaskCascadeClosure{
		TaskID:        1,
		ExplorationID: 0, // 无效关联
	}
	err := closure.Validate()
	if err == nil {
		t.Fatal("expected error when exploration_id is missing, got nil")
	}
}
