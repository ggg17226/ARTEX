package db

import (
	"testing"
)

func TestConversationCascadeClosure_Totals(t *testing.T) {
	closure := &ConversationCascadeClosure{
		ConversationID:        100,
		ActivityIDs:           []uint64{1, 2, 3},
		InterceptPendingIDs:   []uint64{4},
		SideSessionKeys:       []string{"s1"},
		SideRequestOrdinals:   []uint64{10, 11},
		NullifyRetestIDs:      []uint64{20},
		ActiveRetestIDsToStop: []uint64{20},
	}
	// 1(conv) + 3(act) + 1(pending) + 1(sess) + 2(req) = 8
	if closure.TotalCascadeDeleteCount() != 8 {
		t.Fatalf("expected 8 deletes, got %d", closure.TotalCascadeDeleteCount())
	}
	if len(closure.ActiveRetestIDsToStop) != 1 {
		t.Fatalf("expected 1 retest to stop, got %d", len(closure.ActiveRetestIDsToStop))
	}
}
