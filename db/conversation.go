package db

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

// ConvTokenSummary is one conversation's token total (sum of its kind='result'
// rows) with its profile + created_at, used to merge conversation usage into the
// dashboard's per-profile / daily token stats (which otherwise cover only tasks).
//
// P03 R20：llm_profile_id 是 64 位配置 ID，对外 string；无 omitempty，nil 仍是 null。
// token 计数保持 number。
type ConvTokenSummary struct {
	LLMProfileID     *int64 `json:"llm_profile_id,string"`
	CreatedAt        string `json:"created_at"`
	InputTokens      int    `json:"input_tokens"`
	OutputTokens     int    `json:"output_tokens"`
	CacheReadTokens  int    `json:"cache_read_tokens"`
	CacheWriteTokens int    `json:"cache_write_tokens"`
}

// ConversationTokenSummaries returns one row per conversation with its summed
// result-row token usage (0 for conversations with no completed run yet).
//
// TiDB（P08-04 Task 4.3）：created_at 的文本化由 `::text` 改为 DATE_FORMAT，格式与
// assets/exploration 侧同一套口径（前端只取前 10 位当日桶，见 dashboard 的
// `created_at.slice(0, 10)`）。ONLY_FULL_GROUP_BY 下分组键必须显式列出所有非聚合列，
// 不再依赖 PG "GROUP BY 主键即可裸选函数依赖列" 的宽松规则。
func (d *DB) ConversationTokenSummaries() ([]ConvTokenSummary, error) {
	rows, err := d.Query(`
SELECT c.llm_profile_id, DATE_FORMAT(c.created_at, '%Y-%m-%dT%H:%i:%s.%f'),
       COALESCE(SUM(ca.input_tokens),0), COALESCE(SUM(ca.output_tokens),0),
       COALESCE(SUM(ca.cache_read_tokens),0), COALESCE(SUM(ca.cache_write_tokens),0)
FROM conversations c
LEFT JOIN conversation_activities ca ON ca.conversation_id = c.id AND ca.kind = 'result'
GROUP BY c.id, c.llm_profile_id, c.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ConvTokenSummary{}
	for rows.Next() {
		var s ConvTokenSummary
		var pid sql.NullInt64
		if err := rows.Scan(&pid, &s.CreatedAt, &s.InputTokens, &s.OutputTokens, &s.CacheReadTokens, &s.CacheWriteTokens); err != nil {
			return nil, err
		}
		if pid.Valid {
			v := pid.Int64
			s.LLMProfileID = &v
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Conversation is one ChatGPT-style chat thread bound to an agent key. It lives
// independent of the pentest exploration graph — see schema.sql §I.
type Conversation struct {
	ID           int64      `json:"id,string"`
	AgentKey     string     `json:"agent_key"`
	Title        string     `json:"title"`
	LLMProfileID *int64     `json:"llm_profile_id,string,omitempty"`
	Pinned       bool       `json:"pinned"`
	PinnedAt     *time.Time `json:"pinned_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// ConversationPatch updates only the fields whose pointers are non-nil.
type ConversationPatch struct {
	Title  *string
	Pinned *bool
}

const convCols = `id, agent_key, title, llm_profile_id, pinned_at, created_at, updated_at`

func scanConv(row interface{ Scan(...any) error }) (Conversation, error) {
	var c Conversation
	var pinnedAt sql.NullTime
	err := row.Scan(&c.ID, &c.AgentKey, &c.Title, &c.LLMProfileID, &pinnedAt, &c.CreatedAt, &c.UpdatedAt)
	if pinnedAt.Valid {
		c.Pinned = true
		c.PinnedAt = &pinnedAt.Time
	}
	return c, err
}

// CreateConversation opens a new chat thread for agentKey with an initial title.
// llmProfileID may be nil to use the globally active profile.
//
// TiDB：PostgreSQL 的 INSERT … RETURNING 没有等价物，改成 Exec + LastInsertId 定位自增
// 主键，再在事务内回读整行（本事务刚插入的行必然可见）。"先插子行、后锁 profile 行"
// 的顺序保持不变，与 DeleteProfile 的 子行 → profile 行 加锁协议一致。
func (d *DB) CreateConversation(agentKey, title string, llmProfileID *int64) (*Conversation, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`INSERT INTO conversations(agent_key, title, llm_profile_id) VALUES (?, ?, NULL)`, agentKey, title)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	if err := lockLLMProfileForReference(tx, llmProfileID); err != nil {
		return nil, err
	}
	if llmProfileID != nil {
		if _, err := tx.Exec(`UPDATE conversations SET llm_profile_id = ? WHERE id = ?`, *llmProfileID, id); err != nil {
			return nil, err
		}
	}
	c, err := scanConv(tx.QueryRow(`SELECT `+convCols+` FROM conversations WHERE id = ?`, id))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &c, nil
}

// UpdateConversationProfile sets (or clears) the LLM profile override for a conversation.
func (d *DB) UpdateConversationProfile(id int64, llmProfileID *int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var lockedID int64
	if err := tx.QueryRow(`SELECT id FROM conversations WHERE id = ? FOR UPDATE`, id).Scan(&lockedID); err != nil {
		if err == sql.ErrNoRows {
			// Preserve the previous UPDATE semantics: an unknown id is a no-op.
			return tx.Commit()
		}
		return err
	}
	if err := lockLLMProfileForReference(tx, llmProfileID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE conversations SET llm_profile_id = ? WHERE id = ?`, llmProfileID, id); err != nil {
		return err
	}
	return tx.Commit()
}

// ListConversations returns all threads, most-recently-updated first.
//
// TiDB：DESC 排序天然把 NULL 排到最后，`NULLS LAST` 是 PG 专有语法，直接删除即可
// （MySQL 只在 ASC 时需要手写 IS NULL 前置）。
func (d *DB) ListConversations() ([]*Conversation, error) {
	rows, err := d.Query(`SELECT ` + convCols + ` FROM conversations
ORDER BY (pinned_at IS NOT NULL) DESC, pinned_at DESC, updated_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Conversation{}
	for rows.Next() {
		c, err := scanConv(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

// GetConversation returns one thread (nil, nil if absent).
func (d *DB) GetConversation(id int64) (*Conversation, error) {
	c, err := scanConv(d.QueryRow(`SELECT `+convCols+` FROM conversations WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// UpdateConversation applies a partial title/pin mutation and returns the updated
// row. Pinning an already-pinned conversation preserves its original pin order.
//
// TiDB：PG 用 `$n::boolean IS NULL` 在 SQL 里区分"未提供"与"提供 false"，MySQL 没有
// 可空的 boolean 转型，改为 Go 侧把三态拆成两个 int 门（hasPin / pinVal），与
// tasks.go 的 UpdateTask 同一套写法。
//
// 另外 MySQL 的 ROW_COUNT() 是 **changed rows**：标题/置顶值原样重写会返回 0，不能据此
// 判定行不存在，因此统一改为"执行 UPDATE → 回读整行"，回读为空才是"会话不存在"。
func (d *DB) UpdateConversation(id int64, patch ConversationPatch) (*Conversation, error) {
	titleGate := boolToInt(patch.Title != nil)
	titleArg := ""
	if patch.Title != nil {
		titleArg = *patch.Title
	}
	hasPin := boolToInt(patch.Pinned != nil)
	pinVal := 0
	if patch.Pinned != nil && *patch.Pinned {
		pinVal = 1
	}
	if _, err := d.Exec(`UPDATE conversations SET
	title = CASE WHEN ? = 1 THEN ? ELSE title END,
	pinned_at = CASE
		WHEN ? = 0 THEN pinned_at
		WHEN ? = 1 THEN COALESCE(pinned_at, NOW(3))
		ELSE NULL
	END,
	updated_at = NOW(3)
WHERE id = ?`, titleGate, titleArg, hasPin, pinVal, id); err != nil {
		return nil, err
	}
	c, err := scanConv(d.QueryRow(`SELECT `+convCols+` FROM conversations WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// RenameConversation sets a thread's title. Kept for automatic first-message
// titles and compatibility with existing callers.
func (d *DB) RenameConversation(id int64, title string) error {
	_, err := d.UpdateConversation(id, ConversationPatch{Title: &title})
	return err
}

// TouchConversation bumps updated_at so the thread floats to the top of the list.
func (d *DB) TouchConversation(id int64) error {
	_, err := d.Exec(`UPDATE conversations SET updated_at=NOW(3) WHERE id = ?`, id)
	return err
}

// DeleteConversation removes a thread together with its **complete** conversation
// closure (M20 / 闭环 R51-F07-01)。
//
// TiDB 不启用外键约束（见 db/schema 的设计说明），PG 时代的 `ON DELETE CASCADE`
// 在迁移后不会自动发生，级联必须由本函数在同一事务里显式完成：
//
//  1. SELECT … FOR UPDATE 锁住会话根行（与旁路快照写者互斥，杜绝闭包收集期间被改写）；
//  2. ComputeConversationClosure 收集 activities / intercept_pending / 旁路会话与其
//     下挂请求 / 待置空的 retest；
//  3. ExecuteConversationCascade 依次：停止本会话活跃 retest → 置空 retest 的弱关联
//     conversation_id → 删 side_question_requests → side_question_sessions →
//     conversation_activities → intercept_pending → 最后删会话根行，并断言无孤儿。
//
// Retest 语义契约（务必保持）：finding_retests 的行**永不物删**（它是 finding 的重测
// 历史，不是会话的从属数据），只把弱引用置空；处于 pending/running 的 retest 转成
// 停止态 stopped（否则 active_finding_id 生成列会一直占住该 finding 的唯一键），
// completed 等终态一律不动。
func (d *DB) DeleteConversation(id int64) error {
	if id <= 0 {
		return nil
	}
	ctx := context.Background()
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	present, err := deleteConversationLocked(ctx, tx, id)
	if err != nil {
		return err
	}
	if !present {
		// 会话本就不存在：空事务没有可提交的内容，直接由 defer Rollback 收尾。
		return nil
	}
	return tx.Commit()
}

// DeleteConversations removes existing threads in one transaction and returns the
// ids that were actually present. Each id goes through the same full-closure path
// as DeleteConversation（同一个事务里按 id 升序逐个加锁删除，避免与并发批量删除交叉
// 成反向锁序）。
func (d *DB) DeleteConversations(ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return []int64{}, nil
	}
	seen := make(map[int64]bool, len(ids))
	unique := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return []int64{}, nil
	}
	// 统一加锁顺序：升序，重复 id 只删一次。
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })

	ctx := context.Background()
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	deleted := make([]int64, 0, len(unique))
	for _, id := range unique {
		present, err := deleteConversationLocked(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if present {
			deleted = append(deleted, id)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return deleted, nil
}

// deleteConversationLocked deletes one conversation closure inside an existing
// transaction and reports whether the root row was there at all. A missing root is
// a no-op (matching the old `DELETE FROM conversations WHERE id = ?` semantics:
// deleting an absent thread is not an error).
func deleteConversationLocked(ctx context.Context, tx *sql.Tx, id int64) (bool, error) {
	var locked int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM conversations WHERE id = ? FOR UPDATE`, id).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closure, err := ComputeConversationClosure(ctx, tx, uint64(id))
	if err != nil {
		// 已持锁仍报"会话不存在"只可能是并发删除窗口，同样按 no-op 处理。
		if errors.Is(err, ErrForeignKeyViolation) {
			return false, nil
		}
		return false, err
	}
	if _, err := ExecuteConversationCascade(ctx, tx, closure); err != nil {
		return false, err
	}
	return true, nil
}

// AppendConvActivity records one step of a conversation (human message or an agent
// execution step) and returns its id. Mirrors ExplorationStore.AppendActivity but
// keyed by conversation_id. Reuses the Activity struct (NodeID is ignored here).
func (d *DB) AppendConvActivity(convID int64, a Activity) (int64, error) {
	res, err := d.Exec(`
INSERT INTO conversation_activities(conversation_id, worker, kind, tool, tool_use_id, is_error, summary, detail, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)
VALUES (?,NULLIF(?,''),NULLIF(?,''),NULLIF(?,''),NULLIF(?,''),?,NULLIF(?,''),NULLIF(?,''),?,?,?,?)`,
		convID, utf8Clean(a.Worker), utf8Clean(a.Kind), utf8Clean(a.Tool), utf8Clean(a.ToolUseID), a.IsError,
		utf8Clean(a.Summary), utf8Clean(a.Detail), a.InputTokens, a.OutputTokens, a.CacheReadTokens, a.CacheWriteTokens)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ConvActivityList returns a conversation's steps after sinceID (exclusive) with
// the summary-only column set (detail is lazy-loaded via ConvActivityDetail).
func (d *DB) ConvActivityList(convID, sinceID int64, limit int) ([]Activity, int64, error) {
	if limit <= 0 {
		limit = 500
	}
	const cols = `id, COALESCE(worker,''), COALESCE(kind,''), COALESCE(tool,''), COALESCE(tool_use_id,''), is_error, COALESCE(summary,''), created_at, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens`
	rows, err := d.Query(`SELECT `+cols+`
FROM conversation_activities WHERE conversation_id = ? AND id > ? ORDER BY id LIMIT ?`, convID, sinceID, limit)
	if err != nil {
		return nil, sinceID, err
	}
	defer rows.Close()
	out := []Activity{}
	cursor := sinceID
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.CreatedAt,
			&a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheWriteTokens); err != nil {
			return nil, sinceID, err
		}
		if a.ID > cursor {
			cursor = a.ID
		}
		out = append(out, a)
	}
	return out, cursor, rows.Err()
}

// ConvActivityPage returns one page for reverse (newest-first) pagination: up to
// `limit` steps ending before id `before` (exclusive; before<=0 = the latest
// page), returned in ASCENDING id order. hasMore reports whether still-older steps
// exist before the returned window, so the client can stop loading earlier history
// on scroll-up. Summary-only columns (detail is lazy-loaded via ConvActivityDetail).
func (d *DB) ConvActivityPage(convID, before int64, limit int) ([]Activity, bool, error) {
	if limit <= 0 {
		limit = 200
	}
	const cols = `id, COALESCE(worker,''), COALESCE(kind,''), COALESCE(tool,''), COALESCE(tool_use_id,''), is_error, COALESCE(summary,''), created_at, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens`
	// fetch one extra row to detect whether older history remains before this window.
	rows, err := d.Query(`SELECT `+cols+`
FROM conversation_activities
WHERE conversation_id = ? AND (? <= 0 OR id < ?)
ORDER BY id DESC LIMIT ?`, convID, before, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	desc := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.CreatedAt,
			&a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheWriteTokens); err != nil {
			return nil, false, err
		}
		desc = append(desc, a)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(desc) > limit
	if hasMore {
		desc = desc[:limit]
	}
	// reverse the newest-first window into ascending id order for display.
	out := make([]Activity, len(desc))
	for i, a := range desc {
		out[len(desc)-1-i] = a
	}
	return out, hasMore, nil
}

// ConvActivityDetail lazily returns the full detail blob for one step.
func (d *DB) ConvActivityDetail(convID, id int64) (string, error) {
	var s sql.NullString
	err := d.QueryRow(`SELECT detail FROM conversation_activities WHERE id = ? AND conversation_id = ?`, id, convID).Scan(&s)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return s.String, err
}
