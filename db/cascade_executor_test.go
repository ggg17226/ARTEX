package db

import (
	"testing"
	"time"
)

func TestCascadeExecutionAudit_Summary(t *testing.T) {
	audit := &CascadeExecutionAudit{
		RootKind:        "task",
		RootID:          42,
		EntitiesDeleted: map[string]int64{"exploration_nodes": 5, "tasks": 1},
		EntitiesUpdated: map[string]int64{"findings": 2},
		ExecutedAt:      time.Now(),
	}

	if audit.TotalDeleted() != 6 {
		t.Fatalf("expected total deleted 6, got %d", audit.TotalDeleted())
	}
	if audit.TotalUpdated() != 2 {
		t.Fatalf("expected total updated 2, got %d", audit.TotalUpdated())
	}
}

func TestAssertNoOrphans_ZeroCountCheck(t *testing.T) {
	// 测试纯内存结构/辅助函数
	if ErrDanglingReferenceRemaining == nil {
		t.Fatal("expected ErrDanglingReferenceRemaining to be defined")
	}
}
