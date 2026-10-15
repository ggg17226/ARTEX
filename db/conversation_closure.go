package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ConversationCascadeClosure 记录删除 Conversation 时的级联清理与置空集合
type ConversationCascadeClosure struct {
	ConversationID        uint64
	ActivityIDs           []uint64 // conversation_activities.id
	InterceptPendingIDs   []uint64 // intercept_pending.id
	SideSessionKeys       []string // side_question_sessions.session_key
	SideRequestOrdinals   []uint64 // side_question_requests.ordinal
	NullifyRetestIDs      []uint64 // finding_retests.id (conversation_id 置空)
	ActiveRetestIDsToStop []uint64 // finding_retests.id (处于 pending/running 的 retest，需先行停止)
}

func (c *ConversationCascadeClosure) TotalCascadeDeleteCount() int {
	return 1 + len(c.ActivityIDs) + len(c.InterceptPendingIDs) + len(c.SideSessionKeys) + len(c.SideRequestOrdinals)
}

func (c *ConversationCascadeClosure) Validate() error {
	if c.ConversationID == 0 {
		return errors.New("conversation closure: ConversationID is 0")
	}
	return nil
}

// ComputeConversationClosure 收集删除 Conversation 时的完整级联闭包
func ComputeConversationClosure(ctx context.Context, tx DBTX, convID uint64) (*ConversationCascadeClosure, error) {
	closure := &ConversationCascadeClosure{
		ConversationID: convID,
	}

	// 1. 验证会话存在
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM conversations WHERE id = ? LIMIT 1`, convID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: conversation id=%d not found", ErrForeignKeyViolation, convID)
		}
		return nil, err
	}

	// 2. 收集级联删除：conversation_activities
	if err := queryUint64s(ctx, tx, &closure.ActivityIDs, `SELECT id FROM conversation_activities WHERE conversation_id = ?`, convID); err != nil {
		return nil, err
	}

	// 3. 收集级联删除：intercept_pending
	if err := queryUint64s(ctx, tx, &closure.InterceptPendingIDs, `SELECT id FROM intercept_pending WHERE conversation_id = ?`, convID); err != nil {
		return nil, err
	}

	// 4. 收集级联删除：side_question_sessions 与 requests
	if err := queryStrings(ctx, tx, &closure.SideSessionKeys, `SELECT session_key FROM side_question_sessions WHERE conversation_id = ?`, convID); err != nil {
		return nil, err
	}
	if len(closure.SideSessionKeys) > 0 {
		reqQuery := `SELECT ordinal FROM side_question_requests WHERE session_key IN (` + closurePlaceholders(len(closure.SideSessionKeys)) + `)`
		if err := queryUint64s(ctx, tx, &closure.SideRequestOrdinals, reqQuery, stringArgs(closure.SideSessionKeys)...); err != nil {
			return nil, err
		}
	}

	// 5. 收集待置空的 finding_retests 及需提前停止的活跃 retest
	if err := queryUint64s(ctx, tx, &closure.NullifyRetestIDs, `SELECT id FROM finding_retests WHERE conversation_id = ?`, convID); err != nil {
		return nil, err
	}
	if err := queryUint64s(ctx, tx, &closure.ActiveRetestIDsToStop, `SELECT id FROM finding_retests WHERE conversation_id = ? AND status IN ('pending', 'running')`, convID); err != nil {
		return nil, err
	}

	return closure, closure.Validate()
}
