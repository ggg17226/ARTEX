package db

// cold-digest §1/§2.3/§5: persistence for cold-node compression — the round
// counter, cold_since_round stamps, content versions, digest nodes and the
// covers edges that are the source of truth for "which digest folds node X".
//
// P08-03 Task 3.3（M21）：本文件全部语句已是原生 TiDB 8.5 方言 —— 占位符一律 `?`
// 并按文本顺序绑定；没有 `INSERT … RETURNING`（自增主键走 LastInsertId + 范围校验，
// 复用 Task 3.1 的 expInsertID）、没有 `ON CONFLICT`（幂等插入用 uq_exploration_edges
// 上的 `ON DUPLICATE KEY UPDATE 列 = 列` 空转换）。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Autumn-27/artex/db/lockorder"
)

// BumpRound advances this exploration's planner-round counter and returns the
// new value (§2.3). Called once per planner wake-up.
//
// TiDB 没有 `UPDATE … RETURNING`。这里把它拆成一个事务里的两步：先按主键 UPDATE
// （该语句在行上持有排他锁，别的写者进不来），再在同一事务里回读。事务内的读取
// 必定包含自己的写入，因此回读到的就是刚刚自增后的值 —— 语义与 RETURNING 一致，
// 且不会像"两条独立语句"那样被并发写者插队读到中间值。
// 目标 exploration 不存在时，回读返回 sql.ErrNoRows（旧 RETURNING 同样是零行）。
func (s *ExplorationStore) BumpRound() (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`UPDATE explorations SET round_no = round_no + 1 WHERE id = ?`, s.expID); err != nil {
		return 0, err
	}
	var r int64
	if err := tx.QueryRow(`SELECT round_no FROM explorations WHERE id = ?`, s.expID).Scan(&r); err != nil {
		return 0, err
	}
	return r, tx.Commit()
}

// RoundNo returns the current planner-round counter.
func (s *ExplorationStore) RoundNo() (int64, error) {
	var r int64
	err := s.db.QueryRow(`SELECT round_no FROM explorations WHERE id = ?`, s.expID).Scan(&r)
	return r, err
}

// ColdStamps returns cold_since_round for every foldable node (intent/fact):
// id → *round (nil when the node is hot / unstamped). Used to compute the ≥R
// debounce and to know which stamps to set/clear this round.
func (s *ExplorationStore) ColdStamps() (map[int64]*int64, error) {
	rows, err := s.db.Query(`SELECT id, cold_since_round FROM exploration_nodes
		WHERE exploration_id = ? AND kind IN ('intent','fact')`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]*int64{}
	for rows.Next() {
		var id int64
		var cs sql.NullInt64
		if err := rows.Scan(&id, &cs); err != nil {
			return nil, err
		}
		if cs.Valid {
			v := cs.Int64
			out[id] = &v
		} else {
			out[id] = nil
		}
	}
	return out, rows.Err()
}

// StampOp is one cold_since_round change: Set stamps Round; !Set clears it.
type StampOp struct {
	ID    int64
	Set   bool
	Round int64
}

// ApplyStampOps writes a batch of cold_since_round changes in one transaction.
func (s *ExplorationStore) ApplyStampOps(ops []StampOp) error {
	if len(ops) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, o := range ops {
		if o.Set {
			if _, err := tx.Exec(`UPDATE exploration_nodes SET cold_since_round = ? WHERE id = ? AND exploration_id = ?`,
				o.Round, o.ID, s.expID); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(`UPDATE exploration_nodes SET cold_since_round = NULL WHERE id = ? AND exploration_id = ?`,
				o.ID, s.expID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// ContentVersions returns id → content_version for all nodes (§5.3 signature).
func (s *ExplorationStore) ContentVersions() (map[int64]int, error) {
	rows, err := s.db.Query(`SELECT id, content_version FROM exploration_nodes WHERE exploration_id = ?`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int{}
	for rows.Next() {
		var id int64
		var v int
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

// ActiveDigests returns the exploration's live digest nodes (state='active'),
// oldest first.
func (s *ExplorationStore) ActiveDigests() ([]*Node, error) {
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
		WHERE exploration_id = ? AND kind = ? AND state = ? ORDER BY id`, s.expID, KindDigest, StateDigestActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// AddDigest writes one digest node and its covers edges (digest→member) in a
// single transaction. payload is the digest body + member_ids + generation +
// signature (see cold-digest §1). Returns the new digest id.
//
// 自增主键经 LastInsertId 取回并做 [1, MaxInt64] 校验（expInsertID）；covers 边的
// "已存在即幂等"落在 uq_exploration_edges 上，用 `src_id = src_id` 的空转换表达
// DO NOTHING —— 不用 INSERT IGNORE，免得把非空/截断这类真错误一起吞掉。
// AddDigest writes one digest node and its covers edges (digest→member) in a
// single transaction under business gate protection (Spec §3.2.2, 闭环 I1).
func (s *ExplorationStore) AddDigest(payload map[string]any, memberIDs []int64) (int64, error) {
	raw, _ := json.Marshal(payload)
	var id int64
	err := s.db.taskTxRunner().Run(context.Background(), lockorder.TxRunnerOptions{RequiresBusinessGate: true}, func(ctx context.Context, tx *sql.Tx, _ *lockorder.LockContext) error {
		// 1. 若探索关联了任务，按全局锁序（tasks=1 -> explorations=6）核验父任务存活且未被软删除（闭环 I1 残留 1）
		var taskDeletedAt sql.NullTime
		err := tx.QueryRowContext(ctx, "SELECT deleted_at FROM tasks WHERE exploration_id = ? FOR UPDATE", s.expID).Scan(&taskDeletedAt)
		if err == nil {
			if taskDeletedAt.Valid {
				return fmt.Errorf("add digest: parent task for exploration %d is deleted", s.expID)
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 2. 核验所属 exploration 仍真实存活，杜绝向正在被删除的任务/探索写入孤儿节点和边
		var expExists int
		if err := tx.QueryRowContext(ctx, "SELECT 1 FROM explorations WHERE id = ? FOR UPDATE", s.expID).Scan(&expExists); err != nil {
			return fmt.Errorf("add digest: exploration %d not found or deleting: %w", s.expID, err)
		}

		// 3. 过滤并锁定真实属于本 exploration 且当前存活的 memberIDs（闭环 I1 残留 2，杜绝悬空 covers）
		var validMemberIDs []int64
		if len(memberIDs) > 0 {
			sort.Slice(memberIDs, func(i, j int) bool { return memberIDs[i] < memberIDs[j] })
			ph := make([]string, len(memberIDs))
			args := make([]any, 0, len(memberIDs)+1)
			args = append(args, s.expID)
			for i, m := range memberIDs {
				ph[i] = "?"
				args = append(args, m)
			}
			rows, err := tx.QueryContext(ctx, fmt.Sprintf(
				"SELECT id FROM exploration_nodes WHERE exploration_id = ? AND id IN (%s) FOR UPDATE",
				strings.Join(ph, ",")), args...)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var mid int64
				if err := rows.Scan(&mid); err != nil {
					return err
				}
				validMemberIDs = append(validMemberIDs, mid)
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}

		res, err := tx.ExecContext(ctx, `
INSERT INTO exploration_nodes(exploration_id, kind, payload, priority, state, origin)
VALUES (?, ?, ?, 0, ?, 'compactor')`,
			s.expID, KindDigest, string(raw), StateDigestActive)
		if err != nil {
			return err
		}
		insertedID, err := expInsertID(res, "exploration_nodes")
		if err != nil {
			return err
		}
		id = insertedID
		for _, m := range validMemberIDs {
			if m == id {
				continue
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO exploration_edges(exploration_id, src_id, rel, dst_id) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE src_id = src_id`, s.expID, id, RelCovers, m); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// CoveredMembers maps member id → covering digest id, for ACTIVE digests only
// (§6.1 point 1). A node with no entry is not currently folded. Should a member
// carry covers edges from two digests (a torn major write), the lowest digest id
// wins deterministically — callers dedupe on this.
func (s *ExplorationStore) CoveredMembers() (map[int64]int64, error) {
	rows, err := s.db.Query(`
SELECT e.dst_id, e.src_id
FROM exploration_edges e
JOIN exploration_nodes d ON d.id=e.src_id AND d.exploration_id=e.exploration_id
WHERE e.exploration_id = ? AND e.rel = ? AND d.kind = ? AND d.state = ?
ORDER BY e.src_id`, s.expID, RelCovers, KindDigest, StateDigestActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var member, digest int64
		if err := rows.Scan(&member, &digest); err != nil {
			return nil, err
		}
		if _, seen := out[member]; !seen { // first (lowest digest id) wins
			out[member] = digest
		}
	}
	return out, rows.Err()
}

// DigestMembers returns the member ids a digest covers (covers edges), sorted.
func (s *ExplorationStore) DigestMembers(digestID int64) ([]int64, error) {
	rows, err := s.db.Query(`SELECT dst_id FROM exploration_edges
		WHERE exploration_id = ? AND src_id = ? AND rel = ?`, s.expID, digestID, RelCovers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, rows.Err()
}

// SupersedeDigests retires digest nodes (state→superseded) AND removes their
// covers edges, atomically, so the active-coverage set (CoveredMembers) never
// double-counts a member during a major recompaction (§5.1). The digest node
// itself is kept (node_detail can still resolve it).
func (s *ExplorationStore) SupersedeDigests(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, id := range ids {
		if _, err := tx.Exec(`DELETE FROM exploration_edges
			WHERE exploration_id = ? AND src_id = ? AND rel = ?`, s.expID, id, RelCovers); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE exploration_nodes SET state = ? WHERE id = ? AND exploration_id = ? AND kind = ?`,
			StateDigestSuperseded, id, s.expID, KindDigest); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// NodeAssets maps each of the given node ids → the asset ids it is anchored to
// (exploration_anchors). Used to group cold digests by asset (§6.2 index).
//
// IN (…) 的占位符由 expInPlaceholders 逐个产出 `?` 并把值按同一顺序追加进 args；
// 这里绝不能像 PG 那样按 $n 下标自举，否则参数顺序会与占位符错位。
func (s *ExplorationStore) NodeAssets(ids []int64) (map[int64][]int64, error) {
	out := map[int64][]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{s.expID}
	list := expInPlaceholders(ids, &args)
	rows, err := s.db.Query(`SELECT a.node_id, a.asset_id
		FROM exploration_anchors a
		JOIN exploration_nodes n ON n.id=a.node_id
		WHERE n.exploration_id = ? AND a.node_id IN (`+list+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var node, asset int64
		if err := rows.Scan(&node, &asset); err != nil {
			return nil, err
		}
		out[node] = append(out[node], asset)
	}
	return out, rows.Err()
}
