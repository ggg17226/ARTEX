package db

import "fmt"

// DiscardOpenIntent compensates a follow-up creation when task admission fails.
// The task execution gate must still be held by the caller, so the intent cannot
// be claimed between this check and deletion.
//
// 新库**没有外键**，也就没有任何 ON DELETE CASCADE / ON DELETE SET NULL：旧实现里
// 「Edges and anchors cascade with the node」这句话在 TiDB 上不成立，不显式写就
// 永久残留孤儿（与 Task 3.1 的 expDropNodes 同一口径、同一顺序「先清关联 → 再置空
// 弱引用 → 最后删节点」）：
//   - activity：本意图的活动属于这次被撤销的跟进创建，整行删除（旧实现同样显式删，
//     而不是留一行 node_id 为 NULL 的悬空轨迹）；
//   - exploration_edges / exploration_anchors：旧库是 ON DELETE CASCADE，显式删除；
//   - findings.node_id / task_asset_links.source_node_id：旧库是 ON DELETE SET NULL，
//     显式置空（行本身是事实，不随节点消失）；
//   - side_question_sessions（旧库 intent_id 是 CASCADE）：连同其 session_key 名下的
//     请求历史一并删除，复用 expDropSideQuestionSessions。
//
// 节点的 DELETE 保留 kind='intent' AND state='open' 守卫：RowsAffected != 1 就说明
// 意图在门禁之外被动过，补偿必须显式失败而不是静默跳过。DELETE 的影响行数在这里
// 依然可信（删掉就是删掉，不存在 MySQL changed-rows 与 matched-rows 的那类歧义）。
func (s *ExplorationStore) DiscardOpenIntent(id int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(`DELETE FROM activity WHERE exploration_id = ? AND node_id = ?`, s.expID, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM exploration_edges WHERE exploration_id = ? AND (src_id = ? OR dst_id = ?)`,
		s.expID, id, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM exploration_anchors WHERE node_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE findings SET node_id = NULL WHERE node_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE task_asset_links SET source_node_id = NULL WHERE source_node_id = ?`, id); err != nil {
		return err
	}
	if err := expDropSideQuestionSessions(tx, id); err != nil {
		return err
	}
	res, err := tx.Exec(`DELETE FROM exploration_nodes
		WHERE id = ? AND exploration_id = ? AND kind = 'intent' AND state = 'open'`, id, s.expID)
	if err != nil {
		return err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if removed != 1 {
		return fmt.Errorf("open intent %d was not available for admission rollback", id)
	}
	return tx.Commit()
}
