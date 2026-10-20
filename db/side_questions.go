package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
	"github.com/Autumn-27/artex/sidequestion"
	"github.com/google/uuid"
)

var ErrSideBusy = errors.New("当前会话已有旁路问题正在回答")
var ErrSideParentGone = errors.New("旁路父会话已删除或归档")

// Lock the real parent before the side session, also covering soft task/intent
// deletion. A delayed checkpoint cannot recreate data after archive cleanup.
//
// P08-04 Task 4.3（M28）：PostgreSQL 的 `FOR SHARE` 在这里换成 **FOR UPDATE**，不是
// 等价直译，而是有意的收紧，理由三条：
//  1. 本文件的并发契约是"旁路快照写入必须与父会话的删除/归档互斥"。PG 时代靠
//     FOR SHARE 与删除侧的 FOR UPDATE 冲突；TiDB 的 READ COMMITTED / 悲观模型没有
//     可依赖的"共享锁挡住 INSERT 子查询可见性"契约，写者之间必须用写锁串行化。
//  2. 会话闭包删除（DeleteConversation）对 conversations 取的就是 FOR UPDATE；
//     这里也用 FOR UPDATE 才与之真正互斥，否则"父会话已删 yet 快照复活"的窗口回来。
//  3. 项目里已明确禁止 LOCK IN SHARE MODE / FOR SHARE 写法（db/config.go 的迁移口径
//     与 company_lock_test.go 的源码守卫），不得用共享锁冒充行锁语义。
func lockSideParent(ctx context.Context, tx *sql.Tx, p sidequestion.Parent) error {
	var id int64
	var err error
	if p.ConversationID > 0 {
		err = tx.QueryRowContext(ctx, `SELECT id FROM conversations WHERE id = ? FOR UPDATE`, p.ConversationID).Scan(&id)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT id FROM tasks WHERE id = ? AND exploration_id = ? AND deleted_at IS NULL AND archived_at IS NULL FOR UPDATE`, p.TaskID, p.ExplorationID).Scan(&id)
		if err == nil && p.IntentID > 0 {
			err = tx.QueryRowContext(ctx, `SELECT id FROM exploration_nodes WHERE id = ? AND exploration_id = ? AND kind='intent' AND state<>'stopped' FOR UPDATE`, p.IntentID, p.ExplorationID).Scan(&id)
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrSideParentGone
	}
	return err
}

func nullableSideID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

// SaveSideSnapshot 落一次父会话快照（M28：锁内元组比较防版本倒退）。
//
// PG 原文是一条 `INSERT … ON CONFLICT(session_key) DO UPDATE … WHERE
// (run_id,version) < (EXCLUDED.run_id, EXCLUDED.version)`：upsert 的 WHERE 让"迟到的
// 旧快照"被静默丢弃。TiDB 没有 ON CONFLICT，而直接换成 `ON DUPLICATE KEY UPDATE`
// 会**丢掉这个防倒退守卫**（后写覆盖先写），因此拆成"锁行 → 条件更新"两步：
//
//  1. 先锁父行（与父会话删除/归档互斥），再对 side_question_sessions 的行取
//     FOR UPDATE —— 这一步把"读当前 (run_id, version)"和"写新值"关进同一个互斥区间，
//     StartSideRequest 读 generation 用的也是这一行的写锁，两者不会交叉；
//  2. 行已存在：UPDATE 带字典序前进守卫 `(run_id < ? OR (run_id = ? AND version < ?))`，
//     等值或倒退的快照 changed rows=0，静默返回（与迁移期那个带 WHERE 的 upsert 同义）；
//  3. 行不存在：裸 INSERT。memory 是 JSON NOT NULL 且 TiDB 不给 JSON 列声明 DDL 默认值，
//     省略它会得到 Error 1364，因此显式写 '{}'。并发首写撞 uq_side_sessions_session_key
//     时**整笔回滚并返回错误**（fail closed），下一次快照自然走第 2 步的守卫更新，
//     绝不靠 INSERT IGNORE 之类的写法把冲突吞成"什么都没发生"。
func (d *DB) SaveSideSnapshot(ctx context.Context, s sidequestion.Snapshot) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	snapshot := string(jsonbClean(b))
	key := s.Parent.Key()

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockSideParent(ctx, tx, s.Parent); err != nil {
		return err
	}

	var rowID int64
	err = tx.QueryRowContext(ctx, `SELECT row_id FROM side_question_sessions WHERE session_key = ? FOR UPDATE`, key).Scan(&rowID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO side_question_sessions(session_key,conversation_id,task_id,exploration_id,intent_id,run_id,version,snapshot,memory)
VALUES(?,?,?,?,?,?,?,?,?)`,
			key, nullableSideID(s.Parent.ConversationID), nullableSideID(s.Parent.TaskID),
			nullableSideID(s.Parent.ExplorationID), nullableSideID(s.Parent.IntentID),
			s.RunID, s.Version, snapshot, "{}"); err != nil {
			if tidb.IsDuplicateKey(err) {
				return fmt.Errorf("%w（并发首写，下一次快照将走守卫更新）", err)
			}
			return err
		}
	case err != nil:
		return err
	default:
		if _, err := tx.ExecContext(ctx, `UPDATE side_question_sessions SET run_id=?, version=?, snapshot=?
WHERE row_id = ? AND (run_id < ? OR (run_id = ? AND version < ?))`,
			s.RunID, s.Version, snapshot, rowID, s.RunID, s.RunID, s.Version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) SideSnapshot(ctx context.Context, key string) (*sidequestion.Snapshot, error) {
	var b []byte
	err := d.QueryRowContext(ctx, `SELECT snapshot FROM side_question_sessions WHERE session_key = ?`, key).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s sidequestion.Snapshot
	err = json.Unmarshal(b, &s)
	return &s, err
}

const sideCols = `id,ordinal,session_key,generation,client_id,question,answer,status,error,model,snapshot_at,created_at,sequence,usage,context_info`

// ExistingSideRequest 取"同一会话当前代次 + 同一客户端请求 ID"的既有请求（幂等重试）。
// 代次仍然用子查询现读：MySQL/TiDB 允许在 SELECT 的 WHERE 里对**另一张表**做子查询
// （被禁掉的是在 UPDATE/DELETE 的子查询里引用被修改的表）。
func (d *DB) ExistingSideRequest(ctx context.Context, key, client string) (*sidequestion.Exchange, error) {
	e, err := scanSide(d.QueryRowContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND client_id = ? AND generation = (SELECT generation FROM side_question_sessions WHERE session_key = ?)`, key, client, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &e, err
}

func scanSide(row interface{ Scan(...any) error }) (sidequestion.Exchange, error) {
	var e sidequestion.Exchange
	var model, usage, info []byte
	err := row.Scan(&e.ID, &e.Ordinal, &e.SessionKey, &e.Generation, &e.ClientID, &e.Question, &e.Answer, &e.Status, &e.Error, &model, &e.SnapshotAt, &e.CreatedAt, &e.Sequence, &usage, &info)
	if err != nil {
		return e, err
	}
	if err = json.Unmarshal(model, &e.Model); err != nil {
		return e, err
	}
	if err = json.Unmarshal(info, &e.Context); err != nil {
		return e, err
	}
	err = json.Unmarshal(usage, &e.Usage)
	return e, err
}

func (d *DB) SideRequest(ctx context.Context, id string) (*sidequestion.Exchange, error) {
	e, err := scanSide(d.QueryRowContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &e, err
}

func (d *DB) CurrentSideRequest(ctx context.Context, key string) (*sidequestion.Exchange, error) {
	e, err := scanSide(d.QueryRowContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND status='running'`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &e, err
}

// SideHistory returns the newest requests before an ordinal cursor (0 = latest page).
// PG 的 `$2::bigint=0` 只是"游标为 0 时不加条件"的写法，参数本来就是 bigint，
// 直接 `? = 0 OR ordinal < ?` 即可（同一参数绑两次，顺序照抄 SQL 文本）。
func (d *DB) SideHistory(ctx context.Context, key string, before int64, limit int) ([]sidequestion.Exchange, error) {
	rows, err := d.QueryContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND (? = 0 OR ordinal < ?) ORDER BY ordinal DESC LIMIT ?`, key, before, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []sidequestion.Exchange{}
	for rows.Next() {
		e, err := scanSide(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (d *DB) SideReplay(ctx context.Context, key string) ([]sidequestion.Exchange, error) {
	rows, err := d.QueryContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND status='completed' ORDER BY ordinal DESC LIMIT 20`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []sidequestion.Exchange{}
	for rows.Next() {
		e, err := scanSide(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, rows.Err()
}

// StartSideRequest admits one side question for the session's current generation.
//
// 与迁移前一致的四道闸门：父会话存活 → 锁会话行读当前代次 → 同 (代次, 客户端 ID)
// 幂等复用 → 同会话只允许一个 running。
//
// M17（identity 补齐）：side_question_requests 在 TiDB 基线里是 hash 唯一键表，
// uq_side_requests_identity(identity_hash) 才是"同一会话代次 + 同一客户端请求 ID
// 只能有一个请求"的真正守卫（自然键 id 是 UUID，唯一键 uq_side_requests_id 只挡
// 同 UUID 重复）。因此每条 INSERT 必须同时写 identity（canonical 载荷）与
// identity_hash（其 SHA-256）：漏写等于把唯一键退化成 NULL，同一客户端重试可以
// 无限并发插入。载荷用 schema.SideQuestionRequestsIdentity(session_key, generation,
// client_id)，与 VerifyHashArbitration 的仲裁口径一致。
//
// 另外 TiDB 不给 TEXT / JSON 列声明 DDL 默认值，answer / error / model / usage /
// context_info 这些 NOT NULL 列必须在 INSERT 里显式给值，否则 strict 模式 1364；
// 自增 ordinal 由 LastInsertId 取回后再按主键回读整行（无 RETURNING）。
func (d *DB) StartSideRequest(ctx context.Context, s sidequestion.Snapshot, clientID, question string) (*sidequestion.Exchange, bool, error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	if err = lockSideParent(ctx, tx, s.Parent); err != nil {
		return nil, false, err
	}
	var generation int64
	if err = tx.QueryRowContext(ctx, `SELECT generation FROM side_question_sessions WHERE session_key = ? FOR UPDATE`, s.Parent.Key()).Scan(&generation); err != nil {
		return nil, false, err
	}
	e, err := scanSide(tx.QueryRowContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND generation = ? AND client_id = ?`, s.Parent.Key(), generation, clientID))
	if err == nil {
		if e.Question != question {
			return nil, false, fmt.Errorf("同一请求 ID 不能用于不同问题")
		}
		return &e, false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	var busy bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM side_question_requests WHERE session_key = ? AND status = 'running')`, s.Parent.Key()).Scan(&busy); err != nil {
		return nil, false, err
	}
	if busy {
		return nil, false, ErrSideBusy
	}
	model, err := json.Marshal(s.Model)
	if err != nil {
		return nil, false, err
	}
	id := uuid.NewString()
	identity, identityHash := canonicalIdentityPair(schema.SideQuestionRequestsIdentity(s.Parent.Key(), uint64(generation), clientID))
	res, err := tx.ExecContext(ctx, `INSERT INTO side_question_requests(id,session_key,generation,client_id,identity,identity_hash,question,answer,status,error,model,usage,context_info,snapshot_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, s.Parent.Key(), generation, clientID, identity, identityHash,
		question, "", "running", "", string(model), "{}", "{}", s.CapturedAt)
	if err != nil {
		// 唯一键冲突（并发重试撞上同一 identity_hash）原样上抛：调用方按"请求已存在"处理，
		// 绝不在本事务里改写别人的行。
		return nil, false, err
	}
	ordinal, err := res.LastInsertId()
	if err != nil {
		return nil, false, err
	}
	e, err = scanSide(tx.QueryRowContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE ordinal = ?`, ordinal))
	if err != nil {
		return nil, false, err
	}
	return &e, true, tx.Commit()
}

// UpdateSideRequest writes an answer back, but only while the request is still the
// live one. Conditional updates cannot resurrect deleted history or overwrite a
// terminal cancellation with a late provider callback.
//
// M31（changed rows）契约在这里天然成立：守卫 `r.sequence < ?` 保证了凡命中的行
// sequence 一定真的变小→变大，因此 ROW_COUNT() 的"改动行数"与"命中行数"恒等，
// n==1 就是"这次回调被采纳"，n==0 就是"终态/迟到/代次已切，丢弃"。
func (d *DB) UpdateSideRequest(ctx context.Context, e sidequestion.Exchange) (bool, error) {
	usage, err := json.Marshal(e.Usage)
	if err != nil {
		return false, err
	}
	info, err := json.Marshal(e.Context)
	if err != nil {
		return false, err
	}
	r, err := d.ExecContext(ctx, `UPDATE side_question_requests r SET answer=?,status=?,error=?,sequence=?,usage=?,context_info=?
WHERE r.id = ? AND r.status='running' AND r.sequence < ? AND EXISTS(SELECT 1 FROM side_question_sessions s WHERE s.session_key=r.session_key AND s.generation=r.generation)`,
		e.Answer, e.Status, e.Error, e.Sequence, string(usage), string(info), e.ID, e.Sequence)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// ClearSideHistory 推进代次水位并清掉本代次的请求：新代的请求必须重新 admitted，
// 迟到的旧代回调被 UpdateSideRequest 的 EXISTS 守卫挡掉。
func (d *DB) ClearSideHistory(ctx context.Context, key string) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE side_question_sessions SET generation=generation+1,memory='{}' WHERE session_key = ?`, key); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM side_question_requests WHERE session_key = ?`, key); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) SideMemory(ctx context.Context, e sidequestion.Exchange) (sidequestion.Memory, error) {
	var memory sidequestion.Memory
	var raw []byte
	err := d.QueryRowContext(ctx, `SELECT s.memory FROM side_question_sessions s JOIN side_question_requests r ON r.session_key=s.session_key
WHERE r.id = ? AND r.generation=s.generation AND s.generation = ? AND r.status='running'`, e.ID, e.Generation).Scan(&raw)
	if err != nil {
		return memory, err
	}
	err = json.Unmarshal(raw, &memory)
	return memory, err
}

// Unlike SideReplay's UI-era 20-row window, this cursor visits all unsummarized
// successful exchanges, in bounded pages and only before the admitted request.
func (d *DB) SideReplayPage(ctx context.Context, e sidequestion.Exchange, after int64) ([]sidequestion.Exchange, error) {
	rows, err := d.QueryContext(ctx, `SELECT `+sideCols+` FROM side_question_requests WHERE session_key = ? AND generation = ?
AND status='completed' AND ordinal > ? AND ordinal < ? ORDER BY ordinal LIMIT 20`, e.SessionKey, e.Generation, after, e.Ordinal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sidequestion.Exchange
	for rows.Next() {
		item, err := scanSide(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// SaveSideMemory persists the rolling summary of the session's current generation.
//
// M31（changed rows）：这条 UPDATE 的 SET 只有一列 memory，**把同样的摘要再写一次是
// 完全正常的路径**（Replay 每轮都会 Save），MySQL/TiDB 此时 ROW_COUNT()=0。PG 时代
// 0 行等价于"父会话没了"，在 TiDB 上不成立，直接照搬会把正常写入误判成
// ErrSideParentGone 并中断旁路回答。因此 0 行时必须回读一次"请求仍是 running 且
// 代次仍被该会话持有"：仍在 → 成功；确实不在 → 才是 ErrSideParentGone。
func (d *DB) SaveSideMemory(ctx context.Context, e sidequestion.Exchange, memory sidequestion.Memory) error {
	raw, err := json.Marshal(memory)
	if err != nil {
		return err
	}
	result, err := d.ExecContext(ctx, `UPDATE side_question_sessions s SET memory=? WHERE s.session_key = ? AND s.generation = ?
AND EXISTS(SELECT 1 FROM side_question_requests r WHERE r.id = ? AND r.session_key=s.session_key AND r.generation=s.generation AND r.status='running')`,
		string(raw), e.SessionKey, e.Generation, e.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var alive int64
		err := d.QueryRowContext(ctx, `SELECT 1 FROM side_question_requests r JOIN side_question_sessions s ON s.session_key=r.session_key AND s.generation=r.generation
WHERE r.id = ? AND r.generation = ? AND r.status='running' LIMIT 1`, e.ID, e.Generation).Scan(&alive)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSideParentGone
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// InterruptSideRequests 在服务启动时把上一次进程遗留的 running 请求改成 interrupted，
// sequence+1 让任何迟到的旧回调无法覆盖这个终态。
func (d *DB) InterruptSideRequests(ctx context.Context) error {
	_, err := d.ExecContext(ctx, `UPDATE side_question_requests SET status='interrupted',error='服务重启，回答已中断',sequence=sequence+1 WHERE status='running'`)
	return err
}
