package lockorder

import (
	"context"
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
