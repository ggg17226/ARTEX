package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const FindingRetestAgentKey = "retester"

var ErrRetestNotRunning = errors.New("本次复测已结束或尚未开始，请从漏洞详情发起新的复测")

// FindingRetest is an immutable historical test once its conversation turn ends.
// Snapshot is only loaded for the agent, never sent with the history list.
//
// P03 R17：id / finding_id / conversation_id 是 64 位实体 ID，对外 string。
// ConversationID 无 omitempty，nil 仍是 null。Snapshot 是 RawMessage（DB 列原样），未动。
type FindingRetest struct {
	ID             int64           `json:"id,string"`
	FindingID      int64           `json:"finding_id,string"`
	ConversationID *int64          `json:"conversation_id,string"`
	Status         string          `json:"status"`
	Verdict        string          `json:"verdict"`
	Notes          string          `json:"notes"`
	Snapshot       json.RawMessage `json:"snapshot,omitempty"`
	Summary        string          `json:"summary"`
	Evidence       string          `json:"evidence"`
	Error          string          `json:"error"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
}

const retestCols = `id, finding_id, conversation_id, status, verdict, notes, summary, evidence, error, created_at, started_at, finished_at`

// ActiveFindingRetest is the small status payload polled by the findings list.
// Finding IDs use the same string representation as the findings API.
//
// P03 R18：id / conversation_id 同样对外 string（FindingID 早已合规）。
type ActiveFindingRetest struct {
	ID             int64  `json:"id,string"`
	FindingID      int64  `json:"finding_id,string"`
	ConversationID int64  `json:"conversation_id,string"`
	Status         string `json:"status"`
}

func (d *DB) ListActiveFindingRetests(ctx context.Context) ([]ActiveFindingRetest, error) {
	rows, err := d.QueryContext(ctx, `SELECT id, finding_id, conversation_id, status FROM finding_retests
	WHERE status IN ('pending','running') AND conversation_id IS NOT NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ActiveFindingRetest{}
	for rows.Next() {
		var item ActiveFindingRetest
		if err := rows.Scan(&item.ID, &item.FindingID, &item.ConversationID, &item.Status); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanRetest(row interface{ Scan(...any) error }) (*FindingRetest, error) {
	r := &FindingRetest{}
	err := row.Scan(&r.ID, &r.FindingID, &r.ConversationID, &r.Status, &r.Verdict, &r.Notes,
		&r.Summary, &r.Evidence, &r.Error, &r.CreatedAt, &r.StartedAt, &r.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// retestSnapshot 是复测上下文快照的形状：漏洞整行 + 关联资产行 + 任务约束行。
//
// 迁移前由 PG 的 jsonb_build_object / to_jsonb / jsonb_agg 在服务端一次拼出；
// TiDB 没有这组函数，改为在 Go 端读三份行、用 encoding/json 组装，键名与历史
// 归档里的快照逐字一致（finding / assets / constraints，空集恒为 []）。
type retestSnapshot struct {
	Finding     map[string]any   `json:"finding"`
	Assets      []map[string]any `json:"assets"`
	Constraints []map[string]any `json:"constraints"`
}

// buildRetestSnapshot 读出复测所需的全部上下文并编成 JSON 文档。
func buildRetestSnapshot(ctx context.Context, tx *sql.Tx, findingID int64) ([]byte, error) {
	finding, err := queryRowMap(ctx, tx, `SELECT f.* FROM findings f WHERE f.id = ?`, findingID)
	if err != nil {
		return nil, err
	}
	// 原条件 f.asset_ids @> to_jsonb(ARRAY[a.id]) 等价于「JSON 数组里含该资产 id」。
	assets, err := queryRowMaps(ctx, tx, `SELECT a.* FROM assets a
JOIN findings f ON f.id = ? AND JSON_CONTAINS(COALESCE(f.asset_ids, '[]'), CAST(a.id AS JSON))`, findingID)
	if err != nil {
		return nil, err
	}
	var taskID any
	err = tx.QueryRowContext(ctx, `SELECT task_id FROM findings WHERE id = ?`, findingID).Scan(&taskID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	constraints := []map[string]any{}
	if taskID != nil {
		constraints, err = queryRowMaps(ctx, tx, `SELECT c.* FROM task_constraints c
JOIN tasks t ON t.exploration_id = c.exploration_id WHERE t.id = ?`, taskID)
		if err != nil {
			return nil, err
		}
	}
	if assets == nil {
		assets = []map[string]any{}
	}
	raw, err := json.Marshal(retestSnapshot{Finding: finding, Assets: assets, Constraints: constraints})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// queryRowMaps 把任意 SELECT 的每一行读成「列名 → 值」映射：快照要的是整行原样，
// 不列列清单，schema 演进时不会漏列。
func queryRowMaps(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, 8)
	for rows.Next() {
		raw := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range raw {
			targets[i] = &raw[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = snapshotValue(raw[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// queryRowMap 取首行；无行返回 nil（调用方按缺失处理）。
func queryRowMap(ctx context.Context, tx *sql.Tx, query string, args ...any) (map[string]any, error) {
	rows, err := queryRowMaps(ctx, tx, query, args...)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// snapshotValue 归一 driver 返回值：[]byte 里是 JSON 对象/数组时解回结构（否则
// 快照里会多一层转义字符串），其余文本按原样进 JSON。
func snapshotValue(value any) any {
	typed, ok := value.([]byte)
	if !ok {
		return value
	}
	var doc any
	if err := json.Unmarshal(typed, &doc); err == nil {
		switch doc.(type) {
		case map[string]any, []any:
			return doc
		}
	}
	return string(typed)
}

// CreateFindingRetest atomically snapshots the source, creates its conversation
// and persists the first message. A finding row lock deduplicates simultaneous
// clicks across clients; an existing active run is returned without dispatching.
func (d *DB) CreateFindingRetest(ctx context.Context, findingID int64, notes string) (*FindingRetest, *Conversation, bool, error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, false, err
	}
	defer tx.Rollback()
	// 锁住来源漏洞：单表点查加锁（TiDB 没有 PG 的 FOR UPDATE OF 行锁定子句）。
	var title string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(f.name,''), NULLIF(f.vulnclass,''), '未分类')
FROM findings f WHERE f.id = ? FOR UPDATE`, findingID).Scan(&title)
	if err != nil {
		return nil, nil, false, err
	}
	r, err := scanRetest(tx.QueryRowContext(ctx, `SELECT `+retestCols+` FROM finding_retests WHERE finding_id = ? AND status IN ('pending','running')`, findingID))
	if err != nil {
		return nil, nil, false, err
	}
	if r != nil {
		return r, nil, false, nil
	}
	// Keep the title within the same limit as ordinary conversations.
	if runes := []rune(title); len(runes) > 100 {
		title = string(runes[:100])
	}
	snapshot, err := buildRetestSnapshot(ctx, tx, findingID)
	if err != nil {
		return nil, nil, false, err
	}
	convRes, err := tx.ExecContext(ctx, `INSERT INTO conversations(agent_key,title) VALUES (?, ?)`,
		FindingRetestAgentKey, fmt.Sprintf("复测 #%d · %s", findingID, title))
	if err != nil {
		return nil, nil, false, err
	}
	convID, err := convRes.LastInsertId()
	if err != nil {
		return nil, nil, false, fmt.Errorf("conversations: LastInsertId after INSERT: %w", err)
	}
	if convID < 1 {
		return nil, nil, false, fmt.Errorf("conversations: auto-increment id %d out of range", convID)
	}
	c, err := scanConv(tx.QueryRowContext(ctx, `SELECT `+convCols+` FROM conversations WHERE id = ?`, convID))
	if err != nil {
		return nil, nil, false, err
	}
	retestRes, err := tx.ExecContext(ctx, `INSERT INTO finding_retests(finding_id,conversation_id,notes,snapshot) VALUES (?, ?, ?, ?)`,
		findingID, c.ID, strings.TrimSpace(notes), string(snapshot))
	if err != nil {
		return nil, nil, false, err
	}
	retestID, err := retestRes.LastInsertId()
	if err != nil {
		return nil, nil, false, fmt.Errorf("finding_retests: LastInsertId after INSERT: %w", err)
	}
	if retestID < 1 {
		return nil, nil, false, fmt.Errorf("finding_retests: auto-increment id %d out of range", retestID)
	}
	r, err = scanRetest(tx.QueryRowContext(ctx, `SELECT `+retestCols+` FROM finding_retests WHERE id = ?`, retestID))
	if err != nil {
		return nil, nil, false, err
	}
	msg := r.InitialMessage()
	_, err = tx.ExecContext(ctx, `INSERT INTO conversation_activities(conversation_id,worker,kind,summary,detail) VALUES (?, ?, 'user', ?, ?)`,
		c.ID, FindingRetestAgentKey, fmt.Sprintf("请复测漏洞 #%d", findingID), msg)
	if err != nil {
		return nil, nil, false, err
	}
	if err = tx.Commit(); err != nil {
		return nil, nil, false, err
	}
	return r, &c, true, nil
}

func (r *FindingRetest) InitialMessage() string {
	msg := fmt.Sprintf("请复测漏洞 #%d。先调用 get_finding_retest_context 读取本会话关联的原始证据与约束，再执行针对性验证，最后调用 record_finding_retest_result 保存结论。", r.FindingID)
	if r.Notes != "" {
		msg += "\n\n本次复测补充说明：\n" + r.Notes
	}
	return msg
}

func (d *DB) ListFindingRetests(findingID int64) ([]*FindingRetest, error) {
	rows, err := d.Query(`SELECT `+retestCols+` FROM finding_retests WHERE finding_id = ? ORDER BY id DESC`, findingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*FindingRetest{}
	for rows.Next() {
		r, err := scanRetest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *DB) FindingRetestForConversation(ctx context.Context, conversationID int64) (*FindingRetest, error) {
	r, err := scanRetest(d.QueryRowContext(ctx, `SELECT `+retestCols+` FROM finding_retests WHERE conversation_id = ?`, conversationID))
	if err != nil || r == nil {
		return r, err
	}
	err = d.QueryRowContext(ctx, `SELECT snapshot FROM finding_retests WHERE id = ?`, r.ID).Scan(&r.Snapshot)
	return r, err
}

// FailPendingRetestForConversation seals a conversation's unfinished retest when
// the runner could not even load it — the retest ID is unknown on that path, so
// the conversation ID is the only handle. Without it a transient read error
// leaves the row 'pending' forever: the findings list keeps showing 复测中 and
// every later 发起复测 is deduped against a run that is not happening, with only
// a process restart (RecoverFindingRetests) able to clear it.
func (d *DB) FailPendingRetestForConversation(conversationID int64, reason string) error {
	_, err := d.Exec(`UPDATE finding_retests SET status='failed', error=?, finished_at=CURRENT_TIMESTAMP(3)
		WHERE conversation_id = ? AND status IN ('pending','running')`, reason, conversationID)
	return err
}

func (d *DB) StartFindingRetest(ctx context.Context, id int64) (bool, error) {
	// pending → running 一定改变列值，这里的 RowsAffected 可信（不存在"值未变更
	// 返回 0 行"的歧义），n==1 即抢占成功。
	res, err := d.ExecContext(ctx, `UPDATE finding_retests SET status='running', started_at=CURRENT_TIMESTAMP(3) WHERE id = ? AND status='pending'`, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// RecordFindingRetestResult never accepts a finding ID: ownership comes from the
// runtime conversation. Identical retries are safe; a second verdict is refused.
func (d *DB) RecordFindingRetestResult(ctx context.Context, conversationID int64, verdict, summary, evidence string) error {
	if verdict != "reproduced" && verdict != "fixed" && verdict != "inconclusive" {
		return errors.New("verdict 必须为 reproduced / fixed / inconclusive")
	}
	summary, evidence = strings.TrimSpace(summary), strings.TrimSpace(evidence)
	if summary == "" || evidence == "" {
		return errors.New("summary 与 evidence 不能为空；无法确认时说明实际检查及阻塞原因")
	}
	if len(summary) > 16000 || len(evidence) > 128000 {
		return errors.New("复测结论过长（summary ≤ 16KB，evidence ≤ 128KB）")
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 先锁后回查，绝不拿 RowsAffected 当存在性判据（M29）：MySQL/TiDB 的 affected
	// 只统计「值真的变了」的行，把同一条结论重复提交一遍会命中 0 行，原实现据此
	// 抛 ErrRetestNotRunning，Agent 的幂等重试被误判成「复测已结束」。
	var retestID int64
	var current, currentSummary, currentEvidence string
	err = tx.QueryRowContext(ctx, `SELECT id, verdict, summary, evidence FROM finding_retests
WHERE conversation_id = ? AND status = 'running' FOR UPDATE`, conversationID).Scan(&retestID, &current, &currentSummary, &currentEvidence)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRetestNotRunning
	}
	if err != nil {
		return err
	}
	if current != "" {
		// 已有结论：逐字相同才是安全重放（幂等成功），换了文案/判词都算二次改判。
		if current == verdict && currentSummary == summary && currentEvidence == evidence {
			return tx.Commit()
		}
		return ErrRetestNotRunning
	}
	if _, err := tx.ExecContext(ctx, `UPDATE finding_retests SET verdict = ?, summary = ?, evidence = ? WHERE id = ?`,
		verdict, summary, evidence, retestID); err != nil {
		return err
	}
	return tx.Commit()
}

// FinishFindingRetest seals the result. Cancellation/failure takes precedence
// over a staged verdict so an interrupted test cannot appear successfully fixed.
// Only a newly completed fixed verdict updates triage, in the same transaction.
func (d *DB) FinishFindingRetest(id int64, status, reason string) error {
	if status != "completed" && status != "failed" && status != "stopped" {
		return errors.New("invalid terminal retest status")
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Lock the finding before the retest, matching creation and cascading deletion.
	// TiDB 的 SELECT … FOR UPDATE 只吃单表点查，PG 的 `WHERE id=(SELECT …) FOR UPDATE OF f`
	// 拆成「普通读定位 → 主键加锁」两步，锁序仍是 findings → finding_retests。
	var linkedFinding sql.NullInt64
	if err := tx.QueryRow(`SELECT finding_id FROM finding_retests WHERE id = ?`, id).Scan(&linkedFinding); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // Retest already deleted.
		}
		return err
	}
	if !linkedFinding.Valid {
		return nil
	}
	var findingID int64
	if err := tx.QueryRow(`SELECT id FROM findings WHERE id = ? FOR UPDATE`, linkedFinding.Int64).Scan(&findingID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // Finding already deleted.
		}
		return err
	}

	// UPDATE … RETURNING 不存在：先锁住这条复测，回查它当前是否还在收尾窗口内，
	// 再由 Go 端算出终态并写入。回查（而不是 affected 行数）才是判定依据。
	var currentStatus, verdict string
	err = tx.QueryRow(`SELECT status, verdict FROM finding_retests WHERE id = ? FOR UPDATE`, id).Scan(&currentStatus, &verdict)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // Retest already deleted.
	}
	if err != nil {
		return err
	}
	if currentStatus != "pending" && currentStatus != "running" {
		return nil // A replay must not overwrite a later manual triage decision.
	}
	finalStatus, finalError := status, reason
	if status == "completed" && verdict == "" {
		finalStatus, finalError = "failed", "Agent 未保存复测结论，请查看会话后重新复测"
	}
	if _, err := tx.Exec(`UPDATE finding_retests SET status = ?, error = ?, finished_at = CURRENT_TIMESTAMP(3) WHERE id = ?`,
		finalStatus, finalError, id); err != nil {
		return err
	}
	if finalStatus == "completed" && verdict == "fixed" {
		// 走带通知的版本，与人工在详情页改状态共用同一套语义。
		//
		// 此前这里是裸的 UPDATE：复测判「已修复」时状态确实变了，但配了
		// on_status_change 的渠道完全收不到推送——状态在界面上悄悄变了，
		// 运维要打开平台才知道。状态更新与推送事件必须一起落库，
		// SetFindingStatusTx 内部处理了「状态没变就不登记」等细节。
		// 用 context.Background()：本函数整条都是无 ctx 的旧风格（d.Begin()/
		// tx.QueryRow/tx.Exec），没有可传递的取消信号，硬加一个 ctx 参数会
		// 牵动 server 侧调用点与多处测试，超出本次改动的范围。
		if _, _, _, _, err := SetFindingStatusTx(context.Background(), tx, findingID, FindingFixed); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) RecoverFindingRetests() error {
	_, err := d.Exec(`UPDATE finding_retests SET status='stopped', error='服务重启，复测已中断，请重新发起', finished_at=CURRENT_TIMESTAMP(3) WHERE status IN ('pending','running')`)
	return err
}
