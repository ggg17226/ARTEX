package db

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/tidb"
)

// =====================================================================
// 探索图谱层（P08-03 Task 3.1：原生 TiDB 8.5 方言，M21）
//
// 五条硬性约定：
//  1. 占位符一律 `?`，且**按实际出现顺序**逐个绑定 —— `?` 没有编号复用能力，同一个
//     值出现在两个谓词里就必须各绑一次（`id IN (list) OR dst_id IN (list)` 这类
//     片段尤其容易只绑一遍，让第二个谓词静默拿到空集合）；
//  2. 自增主键一律 `res.LastInsertId()` 并校验落在 [1, MaxInt64]：TiDB 没有
//     `INSERT … RETURNING`；
//  3. 没有 `pg_advisory_xact_lock`：并发正确性由行级 `SELECT … FOR UPDATE` 悲观锁 +
//     唯一索引（uq_exploration_edges / uq_exploration_anchors / uq_main_sessions）裁决；
//  4. 时序列（completed_at / created_at / 统计窗口起点）在加锁事务里由 **Go 端内存
//     计算**后作为参数显式写入，不再依赖 SQL 端的 `CASE WHEN $n THEN now()` 与
//     `INTERVAL '1 day'` 这类 PG 时间算术（会话时区已由 db/tidb 固定为 +00:00）；
//  5. 新库**没有外键**，旧库的 `ON DELETE CASCADE / SET NULL` 全部不存在：删除节点
//     必须显式清理 exploration_edges / exploration_anchors，并把 activity.node_id、
//     findings.node_id、task_asset_links.source_node_id 这些弱引用显式置空。
// =====================================================================

// ErrIntentStateConflict marks a failed compare-and-set transition. Callers use
// it to distinguish an expected control race (HTTP 409) from a storage failure.
var ErrIntentStateConflict = errors.New("intent state changed concurrently")

// expInsertID 把一次 INSERT 的自增主键收敛成一次校验。TiDB 没有 RETURNING，
// LastInsertId 是唯一路径；0 与负值只可能是驱动/中间件异常，绝不允许当作合法主键
// 交回调用方（上界由 int64 与下面的显式判据共同守住，防止将来改成 uint 语义时静默
// 溢出成负数）。
func expInsertID(res sql.Result, table string) (int64, error) {
	if res == nil {
		return 0, fmt.Errorf("exploration: nil result after INSERT into %s", table)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("exploration: LastInsertId after INSERT into %s: %w", table, err)
	}
	if id <= 0 || uint64(id) > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("exploration: INSERT into %s returned out-of-range id %d", table, id)
	}
	return id, nil
}

// expNow 是写入时序列的唯一入口。会话时区被 db/tidb 强制为 +00:00，Go 侧统一用 UTC
// 计算再显式绑定，杜绝"同一行的时间由 SQL now() 与 Go time.Now() 两个时钟分别决定"。
func expNow() time.Time { return time.Now().UTC() }

// expInPlaceholders 生成 `?, ?, ?` 形式的 IN 列表并把值按顺序追加进 args
// （TiDB 不能像 PG 那样用 `= ANY($1)` 传数组）。
func expInPlaceholders[T int64 | string](values []T, args *[]any) string {
	if len(values) == 0 {
		return "?"
	}
	marks := make([]string, 0, len(values))
	for _, v := range values {
		*args = append(*args, v)
		marks = append(marks, "?")
	}
	return strings.Join(marks, ", ")
}

// expLowerLikeCond 是大小写不敏感包含匹配的 TiDB 写法。列是 utf8mb4 的 MEDIUMTEXT
// （默认 utf8mb4_bin，比较区分大小写），PG 的 `ILIKE` 没有对应操作符，只能两侧显式
// LOWER；模式在 Go 端预先小写，避免每行再算一次 LOWER(?)。
func expLowerLikeCond(column string) string {
	return "LOWER(" + column + ") LIKE ?"
}

// expJSONTextCond 把 MEDIUMTEXT 里的 JSON 载荷按路径取值。JSON_VALID 守卫不可省：
// JSON_EXTRACT 遇到历史脏行里的非法 JSON 会直接让整条查询报错（Error 3141），
// 而 PG 的 jsonb 列里根本不可能出现非法 JSON，旧实现没有这一层防御。
func expJSONTextCond(column, path string) string {
	return "COALESCE(CASE WHEN JSON_VALID(" + column + ") THEN JSON_UNQUOTE(JSON_EXTRACT(" +
		column + ", '" + path + "')) ELSE NULL END, '')"
}

// utf8Clean makes a string safe for a PostgreSQL text column. It (1) replaces
// invalid UTF-8 byte sequences with U+FFFD and (2) strips NUL (0x00) bytes.
// Tool output (nmap/curl/… stdout) can carry raw/truncated bytes that PostgreSQL's
// UTF8 encoding rejects ("invalid byte sequence for encoding UTF8"); without this
// the INSERT fails and the activity record is silently lost. NUL is *valid* UTF-8
// (U+0000) so ToValidUTF8 leaves it in place, yet PostgreSQL text still rejects it
// (SQLSTATE 22021) — so it must be removed separately. JSONB columns need the
// separate jsonbClean below: json.Marshal encodes a NUL as the escape backslash-u-0000,
// which the text-type json accepts but jsonb rejects (SQLSTATE 22P05).
func utf8Clean(s string) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	return strings.ToValidUTF8(s, "�")
}

// jsonbClean makes marshaled JSON safe for a PostgreSQL jsonb column. json.Marshal
// faithfully encodes a NUL byte (U+0000) as the 6-byte escape sequence \u0000; the
// json type stores it, but jsonb rejects it with "unsupported Unicode escape
// sequence" (SQLSTATE 22P05). Captured HTTP/tool bytes can carry NULs, so drop the
// escape — this also covers NULs nested inside json.RawMessage fields, which are
// copied verbatim into the output. Only a *real* escape is stripped: a \u0000 is
// genuine when preceded by an even number of backslashes, so an escaped-backslash
// run such as \\u0000 (the literal text u0000) is left intact.
func jsonbClean(b []byte) []byte {
	if !bytes.Contains(b, []byte("\\u0000")) {
		return b
	}
	out := make([]byte, 0, len(b))
	bs := 0 // consecutive backslashes already emitted before position i
	for i := 0; i < len(b); i++ {
		if b[i] == '\\' && bs%2 == 0 && i+5 < len(b) &&
			b[i+1] == 'u' && b[i+2] == '0' && b[i+3] == '0' && b[i+4] == '0' && b[i+5] == '0' {
			i += 5 // skip the whole \u0000
			bs = 0
			continue
		}
		if b[i] == '\\' {
			bs++
		} else {
			bs = 0
		}
		out = append(out, b[i])
	}
	return out
}

// Node is a typed reasoning node (= old task_nodes). kind ∈ goal|intent|finding|hint.
type Node struct {
	FindingID     int64           `json:"finding_id,omitempty"` // populated by finding-aware reads; never inferred from ID
	FindingNodeID int64           `json:"finding_node_id,omitempty"`
	TrafficCount  int             `json:"traffic_count,omitempty"`
	ID            int64           `json:"id"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
	Priority      int             `json:"priority"`
	State         string          `json:"state"`
	Origin        string          `json:"origin,omitempty"`
	Owner         string          `json:"owner,omitempty"`
	BlockedReason string          `json:"blocked_reason,omitempty"`
	DeleteReason  string          `json:"delete_reason,omitempty"` // 仅意图假删除(state='deleted')时非空
	Anchors       []int64         `json:"anchors,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	SourceTaskID  int64           `json:"source_task_id,omitempty"`
	Inherited     bool            `json:"inherited,omitempty"`
}

// Activity is one worker execution step (= old task_activity).
type Activity struct {
	ID        int64           `json:"id"`
	NodeID    *int64          `json:"node_id,omitempty"`
	Worker    string          `json:"worker,omitempty"`
	Kind      string          `json:"kind,omitempty"`
	Tool      string          `json:"tool,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	IsError   bool            `json:"is_error"`
	Summary   string          `json:"summary,omitempty"`
	Detail    string          `json:"-"` // full blob; lazy-loaded, omitted from list payloads
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	// Token usage — set only on the terminal kind='result' record; nil elsewhere.
	InputTokens      *int  `json:"input_tokens,omitempty"`
	OutputTokens     *int  `json:"output_tokens,omitempty"`
	CacheReadTokens  *int  `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens *int  `json:"cache_write_tokens,omitempty"`
	SourceTaskID     int64 `json:"source_task_id,omitempty"`
	Inherited        bool  `json:"inherited,omitempty"`
	// MainSeg is the main-agent conversation segment (nil for non-mainagent rows;
	// nil/0 == the original session). Lets the UI route a mainagent row to its segment.
	MainSeg *int `json:"main_seg,omitempty"`
}

// TokenUsage is a per-worker token aggregate (TokenStatsByWorker).
type TokenUsage struct {
	Worker           string `json:"worker"`
	InputTokens      int    `json:"input_tokens"`
	OutputTokens     int    `json:"output_tokens"`
	CacheReadTokens  int    `json:"cache_read_tokens"`
	CacheWriteTokens int    `json:"cache_write_tokens"`
}

// SessionTokenUsage is the authoritative completed-run total for one task UI
// session. Worker sessions are keyed by intent rather than the reusable work#N
// executor name, so retries and reassignment remain attached to the same row.
type SessionTokenUsage struct {
	Session          string `json:"session"`
	InputTokens      int    `json:"input_tokens"`
	OutputTokens     int    `json:"output_tokens"`
	CacheReadTokens  int    `json:"cache_read_tokens"`
	CacheWriteTokens int    `json:"cache_write_tokens"`
}

// DailyTokenBucket is one day's global token aggregate across all tasks.
type DailyTokenBucket struct {
	Day             string `json:"day"` // "YYYY-MM-DD"
	InputTokens     int    `json:"input_tokens"`
	OutputTokens    int    `json:"output_tokens"`
	CacheReadTokens int    `json:"cache_read_tokens"`
}

// TokenDailyAll aggregates token consumption by calendar day (UTC) for the
// past `days` days across all explorations. Only kind='result' rows carry token
// counts (the terminal per-run summary), so there is no double-counting.
func (d *DB) TokenDailyAll(days int) ([]DailyTokenBucket, error) {
	if days <= 0 {
		days = 30
	}
	// 窗口起点在 Go 端算好再绑定：PG 的 `NOW() - ($1 * INTERVAL '1 day')` 既没有
	// TiDB 对应写法，也把"减多少天"这件事交给了 SQL 端的会话时区。
	since := expNow().AddDate(0, 0, -days)
	rows, err := d.Query(`
		SELECT DATE_FORMAT(created_at, '%Y-%m-%d') AS day,
		       COALESCE(SUM(input_tokens), 0),
		       COALESCE(SUM(output_tokens), 0),
		       COALESCE(SUM(cache_read_tokens), 0)
		FROM activity
		WHERE kind = 'result'
		  AND created_at >= ?
		GROUP BY day
		ORDER BY day`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DailyTokenBucket
	for rows.Next() {
		var b DailyTokenBucket
		if err := rows.Scan(&b.Day, &b.InputTokens, &b.OutputTokens, &b.CacheReadTokens); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ExplorationStore is a handle onto one exploration's reasoning graph + activity.
type ExplorationStore struct {
	db    *DB
	expID int64
}

// CreateExploration creates a new exploration root and returns its id.
func (d *DB) CreateExploration(description, goal string) (int64, error) {
	res, err := d.Exec(`INSERT INTO explorations(description, goal) VALUES (?, ?)`, description, goal)
	if err != nil {
		return 0, err
	}
	return expInsertID(res, "explorations")
}

// Exploration returns a handle bound to an exploration id.
func (d *DB) Exploration(id int64) *ExplorationStore { return &ExplorationStore{db: d, expID: id} }

func (s *ExplorationStore) ID() int64 { return s.expID }

// Root returns the exploration's description and goal.
func (s *ExplorationStore) Root() (description, goal string, err error) {
	var d sql.NullString
	err = s.db.QueryRow(`SELECT description, goal FROM explorations WHERE id=?`, s.expID).Scan(&d, &goal)
	return d.String, goal, err
}

// AddNode writes a typed reasoning node with optional anchors to asset ids.
// The node id comes from LastInsertId (TiDB has no RETURNING); the anchor rows are
// idempotent on uq_exploration_anchors, so re-anchoring the same asset never fails.
func (s *ExplorationStore) AddNode(kind string, payload map[string]any, priority int, state, origin string, anchors []int64) (int64, error) {
	raw, _ := json.Marshal(payload)
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`
INSERT INTO exploration_nodes(exploration_id, kind, payload, priority, state, origin)
VALUES (?, ?, ?, ?, ?, ?)`,
		s.expID, kind, string(raw), priority, state, origin)
	if err != nil {
		return 0, err
	}
	id, err := expInsertID(res, "exploration_nodes")
	if err != nil {
		return 0, err
	}
	for _, a := range anchors {
		if _, err := tx.Exec(`INSERT INTO exploration_anchors(node_id, asset_id) VALUES (?, ?)
ON DUPLICATE KEY UPDATE node_id = node_id`, id, a); err != nil {
			return 0, err
		}
	}
	return id, tx.Commit()
}

// AddIntent is a convenience: an open intent.
func (s *ExplorationStore) AddIntent(payload map[string]any, priority int, anchors []int64, origin string) (int64, error) {
	if origin == "" {
		origin = "planner"
	}
	return s.AddNode("intent", payload, priority, "open", origin, anchors)
}

// AddGoal writes a goal node (state open).
func (s *ExplorationStore) AddGoal(payload map[string]any, origin string) (int64, error) {
	return s.AddNode("goal", payload, 0, "open", origin, nil)
}

// OriginFactID returns this exploration's root fact id — the KindFact node with
// state='origin' seeded at task creation (the task root). Every intent traces
// back to it. Returns 0 (no error) when none exists (legacy explorations created
// under the old 'begin' root, or none yet).
func (s *ExplorationStore) OriginFactID() (int64, error) {
	var id int64
	err := s.db.QueryRow(`SELECT id FROM exploration_nodes WHERE exploration_id=? AND kind='fact' AND state='origin' ORDER BY id LIMIT 1`, s.expID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// Anchor records a node→asset reference edge (many-to-many): it captures which
// global assets an exploration node touched, as lineage/provenance. The asset
// graph itself is global and shared — anchoring no longer gates visibility (any
// task may read any asset via search); it only preserves the reasoning trail.
// Idempotent: the duplicate check lands on uq_exploration_anchors (TiDB has no
// ON CONFLICT), and the self-assignment keeps the row untouched on a repeat.
func (s *ExplorationStore) Anchor(nodeID, assetID int64) error {
	if nodeID <= 0 || assetID <= 0 {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO exploration_anchors(node_id, asset_id) VALUES (?, ?)
ON DUPLICATE KEY UPDATE node_id = node_id`, nodeID, assetID)
	return err
}

// Link adds a typed exploration edge (idempotent on uq_exploration_edges).
func (s *ExplorationStore) Link(from int64, rel string, to int64) error {
	_, err := s.db.Exec(`
INSERT INTO exploration_edges(exploration_id, src_id, rel, dst_id) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel = rel`, s.expID, from, rel, to)
	return err
}

// expExecer 让同一段清理逻辑既能跑在 *sql.DB 上也能跑在 *sql.Tx 里。
type expExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// expQueryExecer 同上，但这段清理还要先**枚举**子行的物理主键（新库没有 FK，删除
// 父行前必须自己把子行找出来），因此除 Exec 还需要 Query。
type expQueryExecer interface {
	expExecer
	Query(query string, args ...any) (*sql.Rows, error)
}

// expCollect 跑一条单列 SELECT 并把结果收成切片；Rows 在返回前必定关闭。
// 泛型只放 int64/string 两种主键形态（row_id/task 主键 vs session_key 自然键）。
func expCollect[T int64 | string](ex expQueryExecer, query string, args ...any) ([]T, error) {
	rows, err := ex.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var v T
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// expDropFindingRows 删掉一批 finding **节点**名下的 findings 行及其子表。
//
// findings 的子表在 DDL 里都没有指向 findings 的外键（历史设计），删了漏洞行它们谁也
// 不会跟着动，只能按「锁父 → 清子 → 删父」在同一事务里显式收尾，并与 Task 3.2 已入库的
// DeleteFinding / DeleteFindingsByTask 保持**同一删除口径**：
//   - finding_traffic_bindings：finding_id NOT NULL，没有置空这条路，整行删除；证据
//     快照文件本身由证据存储的 GC 负责，这里只断开绑定；
//   - finding_retests：整行物理删除。只把 pending/running 封成 stopped 终态，等于让
//     completed/failed/stopped 的历史连同指向**已删漏洞**的 finding_id 永久留在库里，
//     那是悬挂引用而不是「解除关联」（Task 3.2 I2 的结论）；承载复测的 conversation 独立
//     存在，不随漏洞删除；
//   - findings：只按锁读回来的主键删，绝不写 `WHERE node_id=?` 这种按未索引列的裸范围
//     删除。
//
// 并发协议（Important 2）：第一步按 node_id 的普通读**只用于定位**（findings.node_id 没有
// 索引，对它做锁定读会在 TiDB 上升级成近乎全表加锁），拿到的候选主键必须立刻换成按物理
// 主键升序的 `SELECT … FOR UPDATE` 锁定当前读，**在任何子表清理之前**把待删的 finding 行
// 全部锁住；随后以锁定读的结果为准清子表、再删父行。
//
// 为什么意图行锁不够：证据写者根本不遵守意图锁 —— Store.Bind 走 WithEvidenceTx →
// LockFindingEvidenceTx（锁 task / finding 行）→ AddFindingTrafficTx 追加绑定，发起复测同样
// 按 finding 行锁串行化。少了这把前置锁，「本事务清空 bindings（当时为空）→ 并发的 Bind
// 校验父行仍存在并插入新绑定 → 本事务删掉父行」就能依次提交，留下一条指向已删漏洞的绑定；
// 事后那条父行 DELETE 的行锁无法追溯保护已经做完的子表清理，TiDB 也不会靠空匹配集上的
// 间隙锁替我们挡住新子行。全局锁序不变（tasks=1 → exploration_nodes=7 → findings=9）：
// 调用方先锁意图节点，本函数只在之后才按主键锁 findings。
func expDropFindingRows(ex expQueryExecer, nodeIDs []int64) error {
	if len(nodeIDs) == 0 {
		return nil
	}
	// 1) 定位读（非锁定）：只为拿到候选主键，锁一律留到下一步按主键取。
	locateArgs := make([]any, 0, len(nodeIDs))
	nodeList := expInPlaceholders(nodeIDs, &locateArgs)
	candidates, err := expCollect[int64](ex, `SELECT id FROM findings WHERE node_id IN (`+nodeList+`)`, locateArgs...)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}

	// 2) 前置锁：按物理主键升序（稳定顺序，避免不同事务以相反顺序抢同一批锁而互相死锁）
	//    锁定这些 finding 行。这一步先于任何子表语句，是整段清理的串行化前提。
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	lockArgs := make([]any, 0, len(candidates))
	lockList := expInPlaceholders(candidates, &lockArgs)
	findingRowIDs, err := expCollect[int64](ex, `SELECT id FROM findings WHERE id IN (`+lockList+`) FOR UPDATE`, lockArgs...)
	if err != nil {
		return err
	}
	if len(findingRowIDs) == 0 {
		// 定位与加锁之间这批漏洞已被别的事务删净：本路径无事可做，父节点由调用方继续收尾。
		return nil
	}
	// 持锁后的当前读才是本次要删的全集；顺序仍按主键升序，保证后续语句的 IN 列表可重放。
	sort.Slice(findingRowIDs, func(i, j int) bool { return findingRowIDs[i] < findingRowIDs[j] })

	// 3) 子表 → 父表：任何一步失败都由调用方的 deferred rollback 整笔回滚。
	bindingArgs := make([]any, 0, len(findingRowIDs))
	bindingList := expInPlaceholders(findingRowIDs, &bindingArgs)
	if _, err := ex.Exec(`DELETE FROM finding_traffic_bindings WHERE finding_id IN (`+bindingList+`)`, bindingArgs...); err != nil {
		return err
	}

	retestArgs := make([]any, 0, len(findingRowIDs))
	retestList := expInPlaceholders(findingRowIDs, &retestArgs)
	if _, err := ex.Exec(`DELETE FROM finding_retests WHERE finding_id IN (`+retestList+`)`, retestArgs...); err != nil {
		return err
	}

	deleteArgs := make([]any, 0, len(findingRowIDs))
	deleteList := expInPlaceholders(findingRowIDs, &deleteArgs)
	_, err = ex.Exec(`DELETE FROM findings WHERE id IN (`+deleteList+`)`, deleteArgs...)
	return err
}

// expDropSideQuestionSessions 在同一事务内收尾一个意图名下的副问答会话及其请求。
//
// side_question_requests 只靠 session_key 关联会话，两张表之间**没有外键**：只删会话
// 会把请求历史永久留成孤儿，而 running 请求更糟——它的 running_session_key 生成列带
// 唯一键，父会话消失后这个唯一键继续占位，迟到的回调也再也找不到父会话把请求改成终态。
// 因此顺序固定为「锁定会话 → 按 session_key 删请求 → 删会话」：先删会话就丢掉了从
// intent 定位 request 的唯一映射，事后无法补救。
// 会话行用 FOR UPDATE 锁定：intent_id 命中 idx_side_sessions_intent，锁范围就是这几行。
func expDropSideQuestionSessions(ex expQueryExecer, intentID int64) error {
	keys, err := expCollect[string](ex, `SELECT session_key FROM side_question_sessions WHERE intent_id=? FOR UPDATE`, intentID)
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		args := make([]any, 0, len(keys))
		list := expInPlaceholders(keys, &args)
		if _, err := ex.Exec(`DELETE FROM side_question_requests WHERE session_key IN (`+list+`)`, args...); err != nil {
			return err
		}
	}
	_, err = ex.Exec(`DELETE FROM side_question_sessions WHERE intent_id=?`, intentID)
	return err
}

// expDropNodes 删除一批节点，并把它名下的关联行与弱引用一次收尾。
// 0) 前置锁定待删除全部节点（FOR UPDATE）：杜绝并发写者（如 AddDigest）在清边后、
//    删节点前向本节点建立新的 covers 边留下悬挂引用（闭环 I1）；
// 1) 边：两端任一在删除集内即断开（含 covers 这类非产出边）；
// 2) 锚点：node_id 是锚点的归属键，节点没了锚点就没有意义；
// 3) 弱引用置空：活动、漏洞、资产绑定均改设为 NULL；
// 4) 节点：逐行删并核对行数。
func expDropNodes(ex expExecer, expID int64, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// 0) 前置锁：若支持 Query（如在事务内），按升序显式锁定全部待删除节点行，
	//    确保与并发写者（AddDigest 等）形成全局写锁互斥。
	if qe, ok := ex.(expQueryExecer); ok {
		sortedIDs := make([]int64, len(ids))
		copy(sortedIDs, ids)
		sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
		lockArgs := []any{expID}
		lockList := expInPlaceholders(sortedIDs, &lockArgs)
		if _, err := expCollect[int64](qe, `SELECT id FROM exploration_nodes WHERE exploration_id = ? AND id IN (`+lockList+`) FOR UPDATE`, lockArgs...); err != nil {
			return 0, err
		}
	}

	// 1) 边：两端任一再删除集内即断开（含 covers 这类非产出边）。
	//    同一个 id 列表出现在 src/dst 两个谓词里，参数必须整段绑两遍——`?` 没有
	//    PG `$n` 的编号复用能力，只绑一遍会让 dst 侧静默拿到空集合。
	edgeArgs := []any{expID}
	list := expInPlaceholders(ids, &edgeArgs)
	expInPlaceholders(ids, &edgeArgs)
	if _, err := ex.Exec(`DELETE FROM exploration_edges WHERE exploration_id = ?
	AND (src_id IN (`+list+`) OR dst_id IN (`+list+`))`, edgeArgs...); err != nil {
		return 0, err
	}

	// 2) 锚点：node_id 是锚点的归属键，节点没了锚点就没有意义。
	anchorArgs := []any{}
	if _, err := ex.Exec(`DELETE FROM exploration_anchors WHERE node_id IN (`+expInPlaceholders(ids, &anchorArgs)+`)`, anchorArgs...); err != nil {
		return 0, err
	}

	// 3) 弱引用置空：活动与漏洞行本身是审计事实，不随节点消失，只断开指向。
	activityArgs := []any{expID}
	if _, err := ex.Exec(`UPDATE activity SET node_id=NULL WHERE exploration_id = ?
	AND node_id IN (`+expInPlaceholders(ids, &activityArgs)+`)`, activityArgs...); err != nil {
		return 0, err
	}
	findingsArgs := []any{}
	if _, err := ex.Exec(`UPDATE findings SET node_id=NULL WHERE node_id IN (`+expInPlaceholders(ids, &findingsArgs)+`)`, findingsArgs...); err != nil {
		return 0, err
	}
	linkArgs := []any{}
	if _, err := ex.Exec(`UPDATE task_asset_links SET source_node_id=NULL WHERE source_node_id IN (`+expInPlaceholders(ids, &linkArgs)+`)`, linkArgs...); err != nil {
		return 0, err
	}

	// 4) 节点：逐行删并核对行数，让"别人在我锁之外动了图"这件事当场暴露。
	var deleted int64
	for _, id := range ids {
		res, err := ex.Exec(`DELETE FROM exploration_nodes WHERE id = ? AND exploration_id = ?`, id, expID)
		if err != nil {
			return deleted, err
		}
		if n, err := res.RowsAffected(); err == nil {
			deleted += n
		}
	}
	return deleted, nil
}

// SetNodeState updates any node's state (never deletes). content_version bumps
// so a folded member's state flip invalidates its digest's cached body (§5.3).
func (s *ExplorationStore) SetNodeState(id int64, state string) error {
	_, err := s.db.Exec(`UPDATE exploration_nodes SET state=?, blocked_reason=NULL, content_version=content_version+1 WHERE id=? AND exploration_id=?`, state, id, s.expID)
	return err
}

// UpdateGoalPayload rewrites a goal node's payload text (and optional vulnclass);
// scoped to kind='goal' so it can never mutate an intent/fact by id. Returns an
// error if no such goal exists in this exploration.
//
// 存在性与 changed rows 必须分开判定。MySQL/TiDB 协议的 ROW_COUNT() 返回的是 **changed
// rows**，不是 matched rows：把同一份文本原样再保存一次（或客户端重试同一次保存）时
// UPDATE 命中的是真实存在的行，changed rows 却是 0。旧实现直接 `RowsAffected()==0 →
// 目标不存在`，于是「重复保存」被误报成 404/500（server/goals_api.go 直接把错误抛给
// 调用方）。db/tidb 的强制 DSN 参数并没有打开 clientFoundRows，不能指望配置兜底。
// 现在改成：先在同一事务里按完整 scope（id + exploration_id + kind='goal'）行锁确认
// 目标存在，再执行 UPDATE。目标存在而 UPDATE 一行未改 = 内容无变更的合法幂等 no-op；
// 目标不存在则由锁定读直接判定，且此时一条 UPDATE 都不会发出。
func (s *ExplorationStore) UpdateGoalPayload(id int64, text, vulnclass string) error {
	payload := map[string]any{"text": text}
	if vulnclass != "" {
		payload["vulnclass"] = vulnclass
	}
	raw, _ := json.Marshal(payload)
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var goalID int64
	if err := tx.QueryRow(`SELECT id FROM exploration_nodes
		WHERE id=? AND exploration_id=? AND kind='goal' FOR UPDATE`, id, s.expID).Scan(&goalID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("目标不存在")
		}
		return err
	}
	if _, err := tx.Exec(`UPDATE exploration_nodes SET payload=? WHERE id=? AND exploration_id=? AND kind='goal'`, string(raw), goalID, s.expID); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteGoal hard-deletes a goal node; scoped to kind='goal' so it can never drop
// an intent/fact by id. The新库没有外键，edges / anchors 的连带清理与 activity /
// findings / task_asset_links 的弱引用置空由 expDropNodes 在同一事务里显式发完
// （旧库靠 ON DELETE CASCADE / SET NULL）。Returns an error if no such goal exists
// in this exploration.
func (s *ExplorationStore) DeleteGoal(id int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 行锁确认这个 goal 在本探索内存在，避免删到并发的同 id 其它节点。
	var goalID int64
	if err := tx.QueryRow(`SELECT id FROM exploration_nodes
		WHERE id=? AND exploration_id=? AND kind='goal' FOR UPDATE`, id, s.expID).Scan(&goalID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("目标不存在")
		}
		return err
	}
	if _, err := expDropNodes(tx, s.expID, []int64{goalID}); err != nil {
		return err
	}
	return tx.Commit()
}

// ResetRunningIntents returns any intent left in 'running' back to 'open' so it
// is re-claimed. Called on startup: a 'running' intent with no live worker (a
// backend restart or crashed worker goroutine left it stuck) would otherwise spin
// forever in the UI. Workers resume from their transcript, so reopening is safe.
func (s *ExplorationStore) ResetRunningIntents() (int64, error) {
	res, err := s.db.Exec(`UPDATE exploration_nodes SET state='open', completed_at=NULL, blocked_reason=NULL
WHERE exploration_id=? AND kind='intent' AND state='running'`, s.expID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ReopenIntent flips ONE not-successfully-finished intent (blocked/exhausted/stopped)
// back to 'open' so a worker re-claims it (graph writes it already produced stay).
// The worker resumes from its prior LLM transcript instead of restarting from scratch.
// done/open/running are left untouched. Returns whether a row changed. Used by "重跑".
func (s *ExplorationStore) ReopenIntent(id int64) (bool, error) {
	res, err := s.db.Exec(`UPDATE exploration_nodes
SET state='open', completed_at=NULL, blocked_reason=NULL
WHERE id=? AND exploration_id=? AND kind='intent'
  AND state IN ('blocked','exhausted','stopped')`, id, s.expID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReopenBlockedIntents flips EVERY 'blocked' intent in this exploration back to 'open'
// (batch rerun after e.g. an LLM/network outage that blocked many at once). Returns the
// number reopened. Workers resume from their transcripts; kept graph writes remain.
func (s *ExplorationStore) ReopenBlockedIntents() (int64, error) {
	res, err := s.db.Exec(`UPDATE exploration_nodes SET state='open', completed_at=NULL, blocked_reason=NULL
WHERE exploration_id=? AND kind='intent' AND state='blocked'`, s.expID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// SetIntentState updates an intent node's state. completed_at is decided here, in Go,
// and written as an explicit value: the PG variant let SQL pick the clock with a
// `CASE WHEN <bool> THEN now() ELSE NULL END`, and TiDB cannot bind a boolean into
// that predicate at all.
func (s *ExplorationStore) SetIntentState(id int64, state string) error {
	_, err := s.db.Exec(`UPDATE exploration_nodes
SET state=?, blocked_reason=NULL, content_version=content_version+1, completed_at=?
WHERE id=? AND exploration_id=? AND kind='intent'`,
		state, expCompletedAt(state), id, s.expID)
	return err
}

// expCompletedAt 是意图状态机唯一的时间口径：终态盖一个 Go 端的 UTC 时刻，
// 回到 open/running/paused 一律显式写 NULL（TiDB 无 RETURNING/CASE 兜底，写什么就是什么）。
func expCompletedAt(state string) any {
	switch state {
	case "done", "blocked", "exhausted", "stopped":
		return expNow()
	default:
		return nil
	}
}

// CompareAndSetIntentState transitions one local intent only when its current
// state still matches expected. It is the state boundary used by pause/resume and
// worker settlement, so a stale API read cannot overwrite a concurrent finish.
func (s *ExplorationStore) CompareAndSetIntentState(id int64, expected, state string) (bool, error) {
	res, err := s.db.Exec(`UPDATE exploration_nodes
SET state=?, blocked_reason=NULL, content_version=content_version+1, completed_at=?
WHERE id=? AND exploration_id=? AND kind='intent' AND state = ?`,
		state, expCompletedAt(state), id, s.expID, expected)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// IntentCleanup summarizes the blackboard records removed when a user cancels
// one worker intent. Global assets and traffic are deliberately not part of this
// operation; exploration anchors disappear only because their owning nodes do.
type IntentCleanup struct {
	Intents    int64 `json:"intents"`
	Facts      int64 `json:"facts"`
	Findings   int64 `json:"findings"`
	Activities int64 `json:"activities"`
}

// SoftDeleteIntent 假删除一个待领/运行中/已暂停的意图:置 state='deleted' 并把用户填写的
// 删除原因记入 delete_reason 字段,保留意图节点及其全部产出/血缘(不再像旧实现那样在图上
// 另挂一条 fact)。返回删除前意图的 summary,供 planner 通知使用。副会话随删除态一并清理。
// 调用方须先停掉运行中的 worker,避免其后续写入。
func (s *ExplorationStore) SoftDeleteIntent(id int64, reason string) (string, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var state string
	var rawPayload []byte
	if err := tx.QueryRow(`SELECT state, payload FROM exploration_nodes
		WHERE id=? AND exploration_id=? AND kind='intent' FOR UPDATE`, id, s.expID).Scan(&state, &rawPayload); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("intent not found")
		}
		return "", err
	}
	if state != "running" && state != "paused" && state != "open" {
		return "", fmt.Errorf("%w: intent state %s cannot be deleted", ErrIntentStateConflict, state)
	}
	if _, err := tx.Exec(`UPDATE exploration_nodes
		SET state='deleted', delete_reason=?, blocked_reason=NULL,
		    content_version=content_version+1, completed_at=?
		WHERE id=? AND exploration_id=?`, reason, expNow(), id, s.expID); err != nil {
		return "", err
	}
	// 主意图审计轨迹保留,但副问答会话随删除态原子清理(延迟快照会拒绝它)。会话的
	// 请求历史按 session_key 一并删除,不留孤儿(见 expDropSideQuestionSessions)。
	if err := expDropSideQuestionSessions(tx, id); err != nil {
		return "", err
	}
	var summary string
	var p map[string]any
	if json.Unmarshal(rawPayload, &p) == nil {
		if sm, ok := p["summary"].(string); ok {
			summary = sm
		}
	}
	return summary, tx.Commit()
}

// CancelIntent 物理删除一个意图,以及"仅由该意图支撑"的全部独占子孙节点——从该意图沿
// yields/derived_from 向下可达、且所有父节点(所有指向它的边的源)都落在删除集内的节点。
// goal 与 origin fact 永不删除;还被删除集之外的意图/digest 引用的共享节点也保留,以免破坏
// 其它分支、产生断链。被删的每个 intent 先做 token rollup(保留不可逆计量)并清理其 activity
// 与副会话(会话连同 session_key 名下的请求历史一并删除);被删的 finding 先按主键锁定
// findings 行,再清掉无外键的 finding_traffic_bindings 与 finding_retests(与 DeleteFinding
// 同口径整行删除,不做只改状态的收尾,否则历史复测会永久悬挂在已删漏洞上),最后才删漏洞行
// (新库没有 FK,也没有 ON DELETE CASCADE/SET NULL,不写就永久残留孤儿)。
// 边与锚点显式删除、activity/findings/task_asset_links 弱引用显式置空,整个清理保持在一个事务内。调用方须先停掉运行中的 worker 防止其后续写入。
func (s *ExplorationStore) CancelIntent(id int64) (IntentCleanup, error) {
	var out IntentCleanup
	tx, err := s.db.Begin()
	if err != nil {
		return out, err
	}
	defer tx.Rollback()

	// 锁定意图行确认存在(幂等:已删则 not found)。状态不校验——真删除对任何状态成立,
	// running 的 worker 由调用方先停。
	if err := tx.QueryRow(`SELECT 1 FROM exploration_nodes
		WHERE id=? AND exploration_id=? AND kind='intent' FOR UPDATE`, id, s.expID).Scan(new(int)); err != nil {
		if err == sql.ErrNoRows {
			return out, fmt.Errorf("intent not found")
		}
		return out, err
	}

	// 载入全图节点(判 protected)与边(算向下可达 + 父集)。图规模对真实任务很小。
	kind := map[int64]string{}
	protected := map[int64]bool{}
	nrows, err := tx.Query(`SELECT id, kind, state FROM exploration_nodes WHERE exploration_id=?`, s.expID)
	if err != nil {
		return out, err
	}
	for nrows.Next() {
		var nid int64
		var k, st string
		if err := nrows.Scan(&nid, &k, &st); err != nil {
			nrows.Close()
			return out, err
		}
		kind[nid] = k
		if k == KindGoal || (k == KindFact && st == StateOrigin) {
			protected[nid] = true // 目标与任务根事实永不随意图删除
		}
	}
	nrows.Close()
	if err := nrows.Err(); err != nil {
		return out, err
	}

	parentsOf := map[int64][]int64{} // dst -> 所有指向它的边的 src(任意 rel,含 covers:被 digest 覆盖的成员因此有 digest 父而被保留)
	downOf := map[int64][]int64{}    // src -> 沿 yields/derived_from 的向下邻居
	erows, err := tx.Query(`SELECT src_id, rel, dst_id FROM exploration_edges WHERE exploration_id=?`, s.expID)
	if err != nil {
		return out, err
	}
	for erows.Next() {
		var src, dst int64
		var rel string
		if err := erows.Scan(&src, &rel, &dst); err != nil {
			erows.Close()
			return out, err
		}
		parentsOf[dst] = append(parentsOf[dst], src)
		if rel == RelYields || rel == RelDerivedFrom {
			downOf[src] = append(downOf[src], dst)
		}
	}
	erows.Close()
	if err := erows.Err(); err != nil {
		return out, err
	}

	// 独占级联:从意图向下扩展,一个节点入删除集当且仅当它未被保护、且它的每个父都已在集内
	// (即除了经过被删节点外再无来路)。迭代到不动点。
	del := map[int64]bool{id: true}
	for changed := true; changed; {
		changed = false
		for src := range del {
			for _, dst := range downOf[src] {
				if del[dst] || protected[dst] {
					continue
				}
				exclusive := true
				for _, p := range parentsOf[dst] {
					if !del[p] {
						exclusive = false
						break
					}
				}
				if exclusive {
					del[dst] = true
					changed = true
				}
			}
		}
	}

	// 按类型分桶。
	var ids, intentIDs, findingIDs []int64
	for nid := range del {
		ids = append(ids, nid)
		switch kind[nid] {
		case KindIntent:
			intentIDs = append(intentIDs, nid)
			out.Intents++
		case KindFact:
			out.Facts++
		case KindFinding:
			findingIDs = append(findingIDs, nid)
			out.Findings++
		}
	}

	// 每个被删意图:token rollup(保留不可逆计量)后清 activity。rollup 行 node_id 为 NULL,
	// 不会被后面按 node_id 的置空命中。
	for _, iid := range intentIDs {
		tokenBuckets, err := intentTokenRollup(tx, s.expID, iid)
		if err != nil {
			return out, err
		}
		res, err := tx.Exec(`DELETE FROM activity WHERE exploration_id=? AND node_id=?`, s.expID, iid)
		if err != nil {
			return out, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			out.Activities += n
		}
		for _, bucket := range tokenBuckets {
			metadata, _ := json.Marshal(map[string]any{
				"cancelled_intent_id": iid,
				"token_day":           bucket.Day.Format(time.DateOnly),
				"token_rollup":        true,
			})
			if _, err := tx.Exec(`INSERT INTO activity(
				exploration_id, worker, kind, summary, metadata,
				input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, created_at)
				VALUES (?,'token-ledger','result',?,?,?,?,?,?,?)`,
				s.expID, fmt.Sprintf("已取消意图 #%d 的 Token 计量", iid), metadata,
				bucket.Usage.InputTokens, bucket.Usage.OutputTokens,
				bucket.Usage.CacheReadTokens, bucket.Usage.CacheWriteTokens, bucket.Day); err != nil {
				return out, err
			}
		}
		// 副会话与它名下的请求历史一并清理(无 FK,只删会话会把请求永久留成孤儿)。
		if err := expDropSideQuestionSessions(tx, iid); err != nil {
			return out, err
		}
	}

	// finding 节点删除前先收尾 findings 表(否则留下 node_id=NULL 的孤儿漏洞),并在同一
	// 事务内按「绑定 → 复测 → 漏洞行」的逆拓扑显式删净无 FK 的子表(见 expDropFindingRows)。
	if err := expDropFindingRows(tx, findingIDs); err != nil {
		return out, err
	}

	// 新库没有外键:边、锚点必须显式删除,activity / findings / task_asset_links 的弱引用
	// 必须显式置空,最后才删节点。整个清理与上面的计量共享同一事务。
	removed, err := expDropNodes(tx, s.expID, ids)
	if err != nil {
		return out, err
	}
	if removed != int64(len(ids)) {
		return out, fmt.Errorf("intent cleanup changed concurrently")
	}
	return out, tx.Commit()
}

type tokenUsageBucket struct {
	Day   time.Time
	Usage TokenUsage
}

// intentTokenRollup returns exactly-once usage grouped by its original UTC day.
// Each result terminates one run. A result carrying usage is authoritative; an
// older error/cancel result without usage falls back to the latest cumulative
// usage frame since the previous result. A trailing frame represents a run that
// was interrupted before its terminal result could be persisted.
func intentTokenRollup(tx *sql.Tx, explorationID, intentID int64) ([]tokenUsageBucket, error) {
	rows, err := tx.Query(`SELECT kind, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, created_at
		FROM activity
		WHERE exploration_id=? AND node_id=? AND kind IN ('usage','result')
		ORDER BY id`, explorationID, intentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byDay := make(map[time.Time]TokenUsage)
	add := func(at time.Time, usage TokenUsage) {
		if usage.InputTokens == 0 && usage.OutputTokens == 0 &&
			usage.CacheReadTokens == 0 && usage.CacheWriteTokens == 0 {
			return
		}
		utc := at.UTC()
		day := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		total := byDay[day]
		total.add(usage)
		byDay[day] = total
	}

	var pending TokenUsage
	var pendingAt time.Time
	hasPending := false
	for rows.Next() {
		var kind string
		var input, output, read, write sql.NullInt64
		var createdAt time.Time
		if err := rows.Scan(&kind, &input, &output, &read, &write, &createdAt); err != nil {
			return nil, err
		}
		current := TokenUsage{
			InputTokens:      int(input.Int64),
			OutputTokens:     int(output.Int64),
			CacheReadTokens:  int(read.Int64),
			CacheWriteTokens: int(write.Int64),
		}
		hasCurrent := input.Valid || output.Valid || read.Valid || write.Valid
		if kind == "usage" {
			if hasCurrent {
				pending, pendingAt, hasPending = current, createdAt, true
			}
			continue
		}

		if hasCurrent {
			// Preserve a partially populated legacy terminal row by filling only its
			// missing dimensions from the latest cumulative usage frame.
			if hasPending {
				if !input.Valid {
					current.InputTokens = pending.InputTokens
				}
				if !output.Valid {
					current.OutputTokens = pending.OutputTokens
				}
				if !read.Valid {
					current.CacheReadTokens = pending.CacheReadTokens
				}
				if !write.Valid {
					current.CacheWriteTokens = pending.CacheWriteTokens
				}
			}
			add(createdAt, current)
		} else if hasPending {
			add(createdAt, pending)
		}
		pending, pendingAt, hasPending = TokenUsage{}, time.Time{}, false
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if hasPending {
		add(pendingAt, pending)
	}

	buckets := make([]tokenUsageBucket, 0, len(byDay))
	for day, usage := range byDay {
		buckets = append(buckets, tokenUsageBucket{Day: day, Usage: usage})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Day.Before(buckets[j].Day) })
	return buckets, nil
}

func (u *TokenUsage) add(other TokenUsage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.CacheReadTokens += other.CacheReadTokens
	u.CacheWriteTokens += other.CacheWriteTokens
}

const nodeCols = `id, kind, payload, priority, state, COALESCE(origin,''), COALESCE(owner,''), COALESCE(blocked_reason,''), COALESCE(delete_reason,''), created_at`

func scanNode(sc interface{ Scan(...any) error }) (*Node, error) {
	var n Node
	var payload []byte
	if err := sc.Scan(&n.ID, &n.Kind, &payload, &n.Priority, &n.State, &n.Origin, &n.Owner, &n.BlockedReason, &n.DeleteReason, &n.CreatedAt); err != nil {
		return nil, err
	}
	n.Payload = json.RawMessage(payload)
	return &n, nil
}

func scanNodes(rows *sql.Rows) ([]*Node, error) {
	var out []*Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListByKind lists nodes of a kind (newest first).
func (s *ExplorationStore) ListByKind(kind string, limit int) ([]*Node, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
WHERE exploration_id=? AND kind=? ORDER BY id DESC LIMIT ?`, s.expID, kind, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// ListByKindPage returns one newest-first page of nodes of a kind for reverse
// pagination: up to `limit` nodes with id < `before` (before<=0 = the newest page).
// hasMore reports whether still-older nodes exist, so a client (e.g. the worker
// session list) can page past the old fixed cap instead of losing older intents.
func (s *ExplorationStore) ListByKindPage(kind string, before int64, limit int) ([]*Node, bool, error) {
	if limit <= 0 {
		limit = 300
	}
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
WHERE exploration_id=? AND kind=? AND (? <= 0 OR id < ?)
ORDER BY id DESC LIMIT ?`, s.expID, kind, before, before, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	nodes, err := scanNodes(rows)
	if err != nil {
		return nil, false, err
	}
	hasMore := len(nodes) > limit
	if hasMore {
		nodes = nodes[:limit]
	}
	return nodes, hasMore, nil
}

// listByKindPageFiltered is ListByKindPage with an optional summary keyword
// filter — a case-insensitive substring over payload's $.summary text. Fetches one
// extra row so the caller can probe hasMore. Empty q = no filter. Newest-first by id.
func (s *ExplorationStore) listByKindPageFiltered(kind string, before int64, limit int, q string) ([]*Node, error) {
	where := `exploration_id=? AND kind=? AND (? <= 0 OR id < ?)`
	args := []any{s.expID, kind, before, before}
	if q != "" {
		args = append(args, "%"+strings.ToLower(q)+"%")
		where += ` AND ` + expLowerLikeCond(expJSONTextCond("payload", "$.summary"))
	}
	args = append(args, limit+1)
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
WHERE `+where+` ORDER BY id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// countByKindFiltered counts nodes of a kind under the same summary filter, for a
// page's total. Empty q = count all of that kind.
func (s *ExplorationStore) countByKindFiltered(kind, q string) (int, error) {
	where := `exploration_id=? AND kind=?`
	args := []any{s.expID, kind}
	if q != "" {
		args = append(args, "%"+strings.ToLower(q)+"%")
		where += ` AND ` + expLowerLikeCond(expJSONTextCond("payload", "$.summary"))
	}
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE `+where, args...).Scan(&n)
	return n, err
}

// CountFinishedIntents counts this exploration's intents in a terminal explored
// state (done/blocked/exhausted) — graph_overview's done_intents_total, so the planner
// knows recent_done_intents (capped at a recent window) is a truncated view and
// stays cautious about "already tried" dedup. Excludes 'stopped' (killed/deleted),
// matching exactly what recent_done_intents surfaces.
func (s *ExplorationStore) CountFinishedIntents() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM exploration_nodes
WHERE exploration_id=? AND kind='intent' AND state IN ('done','blocked','exhausted')`, s.expID).Scan(&n)
	return n, err
}

// CountOpenIntents counts this exploration's open intents — graph_overview's
// frontier_open, so the planner knows open_intents (capped at the top-N by
// priority) is a truncated view and there may be more claimable work.
func (s *ExplorationStore) CountOpenIntents() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM exploration_nodes
WHERE exploration_id=? AND kind='intent' AND state='open'`, s.expID).Scan(&n)
	return n, err
}

// GetNode returns one node of this exploration by id (nil, nil if not found).
func (s *ExplorationStore) GetNode(id int64) (*Node, error) {
	n, err := scanNode(s.db.QueryRow(`SELECT `+nodeCols+` FROM exploration_nodes WHERE id=? AND exploration_id=?`, id, s.expID))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return n, err
}

// Nodes lists all nodes (for graph viz).
func (s *ExplorationStore) Nodes(limit int) ([]*Node, error) {
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes WHERE exploration_id=? ORDER BY id LIMIT ?`, s.expID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// NodeFilter narrows a NodesPage query. Empty fields include every value.
type NodeFilter struct {
	Kinds  []string // exploration_nodes.kind
	States []string // exploration_nodes.state
	Query  string   // case-insensitive substring over payload text / origin
	Asc    bool     // true = oldest first (replay); default newest first (broadcast)
}

// NodesPage returns one 1-based page of this exploration's nodes plus the total
// matching count. The 播报板 reads the graph as a time series, so it pages in SQL
// rather than pulling the whole graph like Nodes does. Ordering is by id, which
// is BIGSERIAL and therefore creation order — stable when several nodes share a
// created_at second.
func (s *ExplorationStore) NodesPage(f NodeFilter, page, size int) ([]*Node, int, error) {
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	if size > 200 {
		size = 200
	}
	offset := (page - 1) * size

	conds := []string{"exploration_id=?"}
	args := []any{s.expID}
	addIn := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		marks := make([]string, 0, len(values))
		for _, v := range values {
			args = append(args, v)
			marks = append(marks, "?")
		}
		conds = append(conds, column+" IN ("+strings.Join(marks, ",")+")")
	}
	addIn("kind", f.Kinds)
	addIn("state", f.States)
	if q := strings.TrimSpace(f.Query); q != "" {
		// `?` 没有 PG `$n` 的编号复用能力：payload 与 origin 两个谓词各绑一份同样的
		// 模式，只绑一次会让第二个谓词拿到空参数（TiDB 里 NULL LIKE 永远不匹配）。
		like := "%" + strings.ToLower(q) + "%"
		args = append(args, like, like)
		ors := []string{expLowerLikeCond("payload"), expLowerLikeCond("COALESCE(origin,'')")}
		// 纯数字(或 UI 里带 # 前缀的形式,如「#41」)当作节点 id 精确匹配,方便直接定位某个节点。
		if idStr := strings.TrimPrefix(q, "#"); idStr != "" {
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				args = append(args, id)
				ors = append(ors, "id = ?")
			}
		}
		conds = append(conds, "("+strings.Join(ors, " OR ")+")")
	}
	where := " WHERE " + strings.Join(conds, " AND ")

	var total int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM exploration_nodes`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	order := "DESC"
	if f.Asc {
		order = "ASC"
	}
	args = append(args, size, offset)
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes`+where+
		` ORDER BY id `+order+
		` LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	nodes, err := scanNodes(rows)
	if err != nil {
		return nil, 0, err
	}
	return nodes, total, nil
}

// NodesByIDs loads the given nodes of this exploration in id order. Used to
// resolve the neighbours of a 播报板 page without fetching the whole graph.
func (s *ExplorationStore) NodesByIDs(ids []int64) ([]*Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, s.expID)
	marks := expInPlaceholders(ids, &args)
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
WHERE exploration_id=? AND id IN (`+marks+`) ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// EdgesTouching returns every edge with one end among ids — the 播报板 uses it to
// spell out where a node came from and what it produced.
func (s *ExplorationStore) EdgesTouching(ids []int64) ([]Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 1+2*len(ids))
	args = append(args, s.expID)
	list := expInPlaceholders(ids, &args)
	// 同一个 id 列表出现在 src / dst 两个谓词里，必须整段再绑一遍。
	expInPlaceholders(ids, &args)
	rows, err := s.db.Query(`SELECT src_id, rel, dst_id FROM exploration_edges
WHERE exploration_id=? AND (src_id IN (`+list+`) OR dst_id IN (`+list+`))`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.From, &e.Rel, &e.To); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Edge is one row from exploration_edges.
type Edge struct {
	From int64
	Rel  string
	To   int64
}

// Edges lists exploration edges (for graph viz).
func (s *ExplorationStore) Edges(limit int) ([]Edge, error) {
	if limit <= 0 {
		limit = 5000
	}
	rows, err := s.db.Query(`SELECT src_id, rel, dst_id FROM exploration_edges WHERE exploration_id=? LIMIT ?`, s.expID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.From, &e.Rel, &e.To); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// FactsYielded returns the ids of fact nodes an intent produced (intent --yields-->
// fact), oldest first. Used to spell out the round's incremental facts to the planner
// alongside the finished worker's output. Best-effort: returns nil for an intent with
// no facts (or a missing one).
func (s *ExplorationStore) FactsYielded(intentID int64) ([]int64, error) {
	rows, err := s.db.Query(`SELECT n.id
		FROM exploration_edges e
		JOIN exploration_nodes n ON n.id=e.dst_id AND n.exploration_id=e.exploration_id
		WHERE e.exploration_id=? AND e.src_id=? AND e.rel=? AND n.kind=?
		ORDER BY n.id`, s.expID, intentID, RelYields, KindFact)
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
	return ids, rows.Err()
}

// FindingLineage returns the sub-DAG that leads FROM the exploration root TO the
// given node: the node itself plus all its ancestors (nodes reverse-reachable by
// following edges backward), and every edge whose both endpoints are in that set.
// Used to render "how this finding was reached" — origin → … → finding. Returns
// empty (not error) for a nonexistent node.
func (s *ExplorationStore) FindingLineage(nodeID int64) ([]*Node, []Edge, error) {
	edges, err := s.allEdges()
	if err != nil {
		return nil, nil, err
	}

	// 反向 BFS：anc = 该节点 + 一切能沿入边到达它的节点。只沿入边上行，天然把
	// finding --proves--> goal 这类下游节点排除在外。
	parents := make(map[int64][]int64, len(edges))
	for _, e := range edges {
		parents[e.To] = append(parents[e.To], e.From)
	}
	anc := map[int64]bool{nodeID: true}
	queue := []int64{nodeID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, p := range parents[cur] {
			if !anc[p] {
				anc[p] = true
				queue = append(queue, p)
			}
		}
	}

	ids := make([]int64, 0, len(anc))
	for id := range anc {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	nodes, err := s.NodesByIDs(ids)
	if err != nil {
		return nil, nil, err
	}
	if len(nodes) == 0 {
		return nil, nil, nil
	}
	// edges among the ancestor set only (the lineage's internal relations).
	var out []Edge
	for _, e := range edges {
		if anc[e.From] && anc[e.To] {
			out = append(out, e)
		}
	}
	return nodes, out, nil
}

// allEdges 取回本探索的全部边。血缘遍历必须看到全图，因此不能走 Edges 的默认
// 5000 条上限；除血缘与意图取消清理外没有调用方，图规模对真实任务很小。
//
// 排序键是 **row_id**，不是 id：原生 Catalog 把这张表原来的复合主键改成了隐藏自增
// 主键 row_id（db/schema/catalog_baseline.go 的 T("exploration_edges","row_id",…)），
// 表里根本没有 id 列，`ORDER BY id` 在真实 TiDB 上是 1054 `Unknown column 'id' in
// 'order clause'`，血缘查询连一行都回不来。row_id 是 AUTO_INCREMENT 的物理主键，
// 既保证遍历顺序确定（同一批边的邻接表构造可重放），也不会命中不存在的列。
func (s *ExplorationStore) allEdges() ([]Edge, error) {
	rows, err := s.db.Query(`SELECT src_id, rel, dst_id FROM exploration_edges WHERE exploration_id=? ORDER BY row_id`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.From, &e.Rel, &e.To); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// FindingIntents maps each finding id to the intent that produced it (the
// intent --yields--> finding edge; report_finding links it). Findings with no
// producing intent are absent. Precise (JOIN, no edge-scan limit).
func (s *ExplorationStore) FindingIntents() (map[int64]int64, error) {
	rows, err := s.db.Query(`SELECT e.dst_id, e.src_id
FROM exploration_edges e
JOIN exploration_nodes n ON n.id = e.dst_id AND n.exploration_id = e.exploration_id
WHERE e.exploration_id=? AND e.rel=? AND n.kind='finding'`, s.expID, RelYields)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var findingID, intentID int64
		if err := rows.Scan(&findingID, &intentID); err != nil {
			return nil, err
		}
		out[findingID] = intentID
	}
	return out, rows.Err()
}

// FindingIntentsTerminal is the inherited-history variant: it returns finding
// lineage only when the producing intent has reached an immutable terminal
// state. This keeps a source task's live worker topology private.
func (s *ExplorationStore) FindingIntentsTerminal() (map[int64]int64, error) {
	rows, err := s.db.Query(`SELECT e.dst_id, e.src_id
	FROM exploration_edges e
	JOIN exploration_nodes finding ON finding.id=e.dst_id AND finding.exploration_id=e.exploration_id
	JOIN exploration_nodes intent ON intent.id=e.src_id AND intent.exploration_id=e.exploration_id
	WHERE e.exploration_id=? AND e.rel=? AND finding.kind='finding'
	  AND intent.kind='intent' AND intent.state IN ('done','blocked','exhausted','stopped')`, s.expID, RelYields)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var findingID, intentID int64
		if err := rows.Scan(&findingID, &intentID); err != nil {
			return nil, err
		}
		out[findingID] = intentID
	}
	return out, rows.Err()
}

// Frontier returns open intents ordered by priority desc then id (FIFO tiebreak).
func (s *ExplorationStore) Frontier(limit int) ([]*Node, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`SELECT `+nodeCols+` FROM exploration_nodes
WHERE exploration_id=? AND kind='intent' AND state='open'
ORDER BY priority DESC, id ASC LIMIT ?`, s.expID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// HasActiveIntent reports whether this exploration has any intent still in play
// (state open OR running). Used to decide whether to kick the FIRST planner round:
// a seeded task's intent may already have been claimed (open→running) by a worker
// before the check, so counting only 'open' (Frontier) races with worker claim and
// wrongly kicks a redundant first round. Counting open+running is race-proof.
func (s *ExplorationStore) HasActiveIntent() (bool, error) {
	// 存在性用"有没有命中行"表达：TiDB 的 `SELECT EXISTS(...)` 返回数值列，直接
	// Scan 进 bool 在 MySQL 协议下是类型不符，命中与否本来就是同一条判定。
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM exploration_nodes
WHERE exploration_id=? AND kind='intent' AND state IN ('open','running') LIMIT 1`, s.expID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// HasOpenGoal reports whether this exploration still has any goal in state 'open'.
// false ⇒ 所有目标已 met/abandoned（或本任务无目标）⇒ 进入 goalless（人工直投）分支：
// planner 停跑，任务是否结束改由 frontier 是否抽干决定。met 与 abandoned 都算"已了结"。
func (s *ExplorationStore) HasOpenGoal() (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM exploration_nodes
WHERE exploration_id=? AND kind='goal' AND state='open' LIMIT 1`, s.expID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ClaimIntent atomically moves an open intent to running. Returns true if claimed.
func (s *ExplorationStore) ClaimIntent(id int64, owner string) (bool, error) {
	res, err := s.db.Exec(`UPDATE exploration_nodes SET state='running', owner=?
WHERE id=? AND exploration_id=? AND kind='intent' AND state='open'`, owner, id, s.expID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// Stats returns node counts grouped by kind (for dashboard).
func (s *ExplorationStore) Stats() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT kind, count(*) FROM exploration_nodes WHERE exploration_id=? GROUP BY kind`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var c int
		if err := rows.Scan(&k, &c); err != nil {
			return nil, err
		}
		out[k] = c
	}
	return out, rows.Err()
}

// --- activity (poll by global id cursor) ---

// AppendActivity records one worker step and returns its id.
func (s *ExplorationStore) AppendActivity(a Activity) (int64, error) {
	metadata := a.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	res, err := s.db.Exec(`
INSERT INTO activity(exploration_id, node_id, worker, kind, tool, tool_use_id, is_error, summary, detail, metadata, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, main_seg)
VALUES (?, ?, NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, ?, ?)`,
		s.expID, a.NodeID, utf8Clean(a.Worker), utf8Clean(a.Kind), utf8Clean(a.Tool), utf8Clean(a.ToolUseID), a.IsError, utf8Clean(a.Summary), utf8Clean(a.Detail),
		// metadata 以 string 下发：json.RawMessage 是命名 slice，database/sql 的默认
		// 参数转换器会直接拒绝它。
		string(metadata), a.InputTokens, a.OutputTokens, a.CacheReadTokens, a.CacheWriteTokens, a.MainSeg)
	if err != nil {
		return 0, err
	}
	return expInsertID(res, "activity")
}

// ExplorationDiag probes, at the moment of an activity FK violation (23503), why the
// parent row is unreachable. It reports whether the exploration row still exists, how
// many task rows reference it (a live task should keep exactly 1 — and RESTRICT on
// tasks.exploration_id means the exploration CANNOT be deleted while that row lives),
// and the current MAX(explorations.id). Together these tell apart the failure modes:
//   - expExists=false, taskRefs=0 → the whole row set is gone (DB reset / wrong DB).
//   - expExists=false, taskRefs=1 → impossible under RESTRICT; would mean a broken FK.
//   - expExists=true              → the write's expID is NOT this exploration (stale/
//     wrong id in the in-memory store); compare against Store.ID().
func (d *DB) ExplorationDiag(expID int64) (expExists bool, taskRefs int, maxExpID int64, err error) {
	// 存在性以计数表达（同上：EXISTS() 的数值结果不落在 bool 上），同一个 expID
	// 出现在两个子查询里就绑两次。
	var expRows int
	err = d.QueryRow(`SELECT
		(SELECT COUNT(*) FROM explorations WHERE id=?),
		(SELECT COUNT(*) FROM tasks WHERE exploration_id=?),
		COALESCE((SELECT MAX(id) FROM explorations),0)`, expID, expID).Scan(&expRows, &taskRefs, &maxExpID)
	expExists = expRows > 0
	return
}

// TokenTotal sums token usage across ALL workers for this exploration (whole-task
// total). Same source as TokenStatsByWorker (kind='result' rows), just ungrouped.
func (s *ExplorationStore) TokenTotal() (TokenUsage, error) {
	var u TokenUsage
	err := s.db.QueryRow(`SELECT
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0)
	FROM activity WHERE exploration_id=? AND kind='result'`, s.expID).
		Scan(&u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens)
	return u, err
}

// TokenTotalsAll returns the whole-task token total for every exploration in one
// query (exploration_id → total), so the task list can show per-task consumption
// without a query per row.
func (d *DB) TokenTotalsAll() (map[int64]TokenUsage, error) {
	rows, err := d.Query(`SELECT exploration_id,
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0)
	FROM activity WHERE kind='result' GROUP BY exploration_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]TokenUsage{}
	for rows.Next() {
		var eid int64
		var u TokenUsage
		if err := rows.Scan(&eid, &u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens); err != nil {
			return nil, err
		}
		out[eid] = u
	}
	return out, rows.Err()
}

// LastActivityAll returns the unix time of the most recent activity per
// exploration (exploration_id → max created_at epoch), one query for all tasks.
// Persisted (unlike Engine.LastActivity's in-memory map), so it survives restarts
// and gives终态任务 a stable "ran until" time for computing run duration.
func (d *DB) LastActivityAll() (map[int64]int64, error) {
	// CAST 不可省：UNIX_TIMESTAMP(DATETIME(3)) 返回带小数秒的 DECIMAL，直接 Scan
	// 进 int64 会因小数位报错。
	rows, err := d.Query(`SELECT exploration_id, CAST(UNIX_TIMESTAMP(MAX(created_at)) AS SIGNED) FROM activity GROUP BY exploration_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var eid, ts int64
		if err := rows.Scan(&eid, &ts); err != nil {
			return nil, err
		}
		out[eid] = ts
	}
	return out, rows.Err()
}

// GoalCounts is the goal summary for one exploration.
type GoalCounts struct{ Total, Met int }

// FindingSeverityCounts breaks a task's findings down by severity for the task
// list (severity 白名单外/为空的记录不计入任一档)。
type FindingSeverityCounts struct{ Critical, High, Medium, Low int }

// TaskListMetrics contains the aggregates rendered in task lists.
type TaskListMetrics struct {
	Tokens         TokenUsage
	LastActivity   int64
	Goals          GoalCounts
	RunningIntents int                   // kind='intent' 且 state='running' 的条数，即运行中 Worker 数
	Findings       FindingSeverityCounts // findings 表里该任务的漏洞数（按严重度分档）
}

// TaskListMetricsAll returns list aggregates for every live task in one query.
// The lateral lookups use the per-exploration indexes instead of grouping the
// complete activity history on every poll.
func (d *DB) TaskListMetricsAll() (map[int64]TaskListMetrics, error) {
	// 原实现是 5 个 `LEFT JOIN LATERAL (…) ON true`：TiDB 没有 LATERAL 派生表，
	// 且 `COUNT(*) FILTER (WHERE …)` 也是 PG 专属。这里拆成"每条聚合各自 GROUP BY
	// exploration_id + Go 端按主键合并"，结果集与原来逐列一致。
	out := map[int64]TaskListMetrics{}

	// 先铺满所有活任务的 key：没有任何产出的任务同样要有一行全零 metrics
	// （对应原查询里的 COALESCE(...,0)）。
	if err := expEachRow(d, `SELECT exploration_id FROM tasks WHERE deleted_at IS NULL`, func(rows *sql.Rows) error {
		var explorationID int64
		if err := rows.Scan(&explorationID); err != nil {
			return err
		}
		out[explorationID] = TaskListMetrics{}
		return nil
	}); err != nil {
		return nil, err
	}

	// 分组结果里可能出现已删除任务的残留活动，原查询以 tasks 为主表不会输出它们；
	// map 的值不可寻址，回填一律"取值 → 改 → 写回"。
	put := func(explorationID int64, fn func(*TaskListMetrics)) {
		metrics, ok := out[explorationID]
		if !ok {
			return
		}
		fn(&metrics)
		out[explorationID] = metrics
	}

	// token 总量（kind='result' 行，与 TokenTotalsAll 同源）
	if err := expEachRow(d, `SELECT exploration_id,
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0)
	FROM activity WHERE kind='result' GROUP BY exploration_id`, func(rows *sql.Rows) error {
		var explorationID int64
		var u TokenUsage
		if err := rows.Scan(&explorationID, &u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens); err != nil {
			return err
		}
		put(explorationID, func(m *TaskListMetrics) { m.Tokens = u })
		return nil
	}); err != nil {
		return nil, err
	}

	// 最近一次活动（原 LATERAL 是 ORDER BY created_at DESC LIMIT 1，按组 MAX 等价）
	if err := expEachRow(d, `SELECT exploration_id, CAST(UNIX_TIMESTAMP(MAX(created_at)) AS SIGNED)
	FROM activity GROUP BY exploration_id`, func(rows *sql.Rows) error {
		var explorationID, ts int64
		if err := rows.Scan(&explorationID, &ts); err != nil {
			return err
		}
		put(explorationID, func(m *TaskListMetrics) { m.LastActivity = ts })
		return nil
	}); err != nil {
		return nil, err
	}

	// 目标计数
	if err := expEachRow(d, `SELECT exploration_id, COUNT(*), COUNT(CASE WHEN state='met' THEN 1 END)
	FROM exploration_nodes WHERE kind='goal' GROUP BY exploration_id`, func(rows *sql.Rows) error {
		var explorationID int64
		var total, met int
		if err := rows.Scan(&explorationID, &total, &met); err != nil {
			return err
		}
		put(explorationID, func(m *TaskListMetrics) { m.Goals = GoalCounts{Total: total, Met: met} })
		return nil
	}); err != nil {
		return nil, err
	}

	// 运行中意图
	if err := expEachRow(d, `SELECT exploration_id, COUNT(*)
	FROM exploration_nodes WHERE kind='intent' AND state='running' GROUP BY exploration_id`, func(rows *sql.Rows) error {
		var explorationID int64
		var running int
		if err := rows.Scan(&explorationID, &running); err != nil {
			return err
		}
		put(explorationID, func(m *TaskListMetrics) { m.RunningIntents = running })
		return nil
	}); err != nil {
		return nil, err
	}

	// 漏洞分档（severity 白名单外/为空的行不计入任一档，与 FILTER 版一致）
	if err := expEachRow(d, `SELECT t.exploration_id,
		COUNT(CASE WHEN f.severity='critical' THEN 1 END),
		COUNT(CASE WHEN f.severity='high' THEN 1 END),
		COUNT(CASE WHEN f.severity='medium' THEN 1 END),
		COUNT(CASE WHEN f.severity='low' THEN 1 END)
	FROM findings f
	JOIN tasks t ON t.id=f.task_id
	WHERE t.deleted_at IS NULL
	GROUP BY t.exploration_id`, func(rows *sql.Rows) error {
		var explorationID int64
		var counts FindingSeverityCounts
		if err := rows.Scan(&explorationID, &counts.Critical, &counts.High, &counts.Medium, &counts.Low); err != nil {
			return err
		}
		put(explorationID, func(m *TaskListMetrics) { m.Findings = counts })
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// expEachRow 跑一条只读查询并把每一行交给 fn；fn 的错误原样上抛。
func expEachRow(d *DB, query string, fn func(rows *sql.Rows) error) error {
	rows, err := d.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// GoalCountsAll returns goal totals (total / met) per exploration id, one query
// for all tasks — used by listTasks so the task list shows progress for every task.
func (d *DB) GoalCountsAll() (map[int64]GoalCounts, error) {
	rows, err := d.Query(`
		SELECT exploration_id,
		       COUNT(*) AS total,
		       COUNT(CASE WHEN state = 'met' THEN 1 END) AS met
		FROM exploration_nodes
		WHERE kind = 'goal'
		GROUP BY exploration_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]GoalCounts{}
	for rows.Next() {
		var eid int64
		var gc GoalCounts
		if err := rows.Scan(&eid, &gc.Total, &gc.Met); err != nil {
			return nil, err
		}
		out[eid] = gc
	}
	return out, rows.Err()
}

// TokenStatsByWorker sums token usage per worker for this exploration (from the
// kind='result' records that carry usage). Used by the per-agent token display.
func (s *ExplorationStore) TokenStatsByWorker() ([]TokenUsage, error) {
	rows, err := s.db.Query(`SELECT COALESCE(worker,''),
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0)
	FROM activity WHERE exploration_id=? AND kind='result'
	GROUP BY worker ORDER BY worker`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TokenUsage{}
	for rows.Next() {
		var u TokenUsage
		if err := rows.Scan(&u.Worker, &u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// TokenStatsBySession aggregates every persisted completed run, independent of
// activity-history pagination. Main/planner use fixed keys; Worker executions use
// their intent node id, even when a different work#N process handled a retry.
func (s *ExplorationStore) TokenStatsBySession() ([]SessionTokenUsage, error) {
	rows, err := s.db.Query(`SELECT
		CASE
			WHEN COALESCE(a.worker,'')='mainagent' THEN CONCAT('main:', CAST(COALESCE(a.main_seg,0) AS CHAR))
			WHEN COALESCE(a.worker,'')='planner' THEN 'plan'
			WHEN n.id IS NOT NULL THEN CONCAT('intent:', CAST(n.id AS CHAR))
			ELSE ''
		END AS session_key,
		COALESCE(SUM(a.input_tokens),0), COALESCE(SUM(a.output_tokens),0),
		COALESCE(SUM(a.cache_read_tokens),0), COALESCE(SUM(a.cache_write_tokens),0)
	FROM activity a
	LEFT JOIN exploration_nodes n
	  ON n.id=a.node_id AND n.exploration_id=a.exploration_id AND n.kind='intent'
	WHERE a.exploration_id=? AND a.kind='result'
	  AND (COALESCE(a.worker,'') IN ('mainagent','planner') OR n.id IS NOT NULL)
	GROUP BY session_key
	ORDER BY session_key`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SessionTokenUsage{}
	for rows.Next() {
		var u SessionTokenUsage
		if err := rows.Scan(&u.Session, &u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ActivityList returns steps after sinceID (exclusive), optionally filtered by node.
// Returns items and the new cursor (max id seen).
func (s *ExplorationStore) ActivityList(nodeID *int64, sinceID int64, limit int) ([]Activity, int64, error) {
	if limit <= 0 {
		limit = 300
	}
	var rows *sql.Rows
	var err error
	const cols = `id, node_id, COALESCE(worker,''), COALESCE(kind,''), COALESCE(tool,''), COALESCE(tool_use_id,''), is_error, COALESCE(summary,''), metadata, created_at, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, main_seg`
	if nodeID != nil {
		rows, err = s.db.Query(`SELECT `+cols+`
FROM activity WHERE exploration_id=? AND node_id=? AND id>? ORDER BY id LIMIT ?`, s.expID, *nodeID, sinceID, limit)
	} else {
		rows, err = s.db.Query(`SELECT `+cols+`
FROM activity WHERE exploration_id=? AND id>? ORDER BY id LIMIT ?`, s.expID, sinceID, limit)
	}
	if err != nil {
		return nil, sinceID, err
	}
	defer rows.Close()
	out := []Activity{}
	cursor := sinceID
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.Metadata, &a.CreatedAt,
			&a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheWriteTokens, &a.MainSeg); err != nil {
			return nil, sinceID, err
		}
		if a.ID > cursor {
			cursor = a.ID
		}
		out = append(out, a)
	}
	return out, cursor, rows.Err()
}

// ActivityListForTerminalIntent is the inherited, incremental-read boundary.
// The intent state check and activity read happen in one statement so a source
// intent reopened between API-level checks cannot expose its new in-flight trace.
// Model reasoning and accounting rows are never inherited.
func (s *ExplorationStore) ActivityListForTerminalIntent(nodeID, sinceID int64, limit int) ([]Activity, int64, error) {
	if limit <= 0 {
		limit = 300
	}
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.worker,''), COALESCE(a.kind,''),
		COALESCE(a.tool,''), COALESCE(a.tool_use_id,''), a.is_error, COALESCE(a.summary,''),
		a.created_at, a.input_tokens, a.output_tokens, a.cache_read_tokens, a.cache_write_tokens
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
	WHERE a.exploration_id=? AND a.node_id=? AND a.id>?
	  AND n.kind='intent' AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.kind NOT IN ('thinking','usage')
	ORDER BY a.id LIMIT ?`, s.expID, nodeID, sinceID, limit)
	if err != nil {
		return nil, sinceID, err
	}
	defer rows.Close()
	out := []Activity{}
	cursor := sinceID
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.CreatedAt,
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

// ActivitySessionFilter selects one UI "session" within a task's activity stream.
// Exactly one field is meaningful:
//   - Main == true  → the main-agent session (worker="mainagent") for one segment
//     (MainSeg; nil/0 = the original segment, which also matches legacy NULL rows).
//   - Worker != ""  → filter by worker name (Plan = "planner").
//     Goal Agent 的第 0 轮拆解也以 worker="planner" 落库，故 Plan 会话完整覆盖 Goal+Planner。
//   - NodeID != nil → a Worker session, filtered by node_id (= intent id).
//
// A zero value (all empty) matches the whole task (no session filter).
type ActivitySessionFilter struct {
	Worker  string
	NodeID  *int64
	Main    bool // main-agent session
	MainSeg *int // segment for the main session (nil == current, resolved by caller; 0 == original)
}

// cond 返回会话过滤片段与其绑定参数。TiDB 的 `?` 纯按出现顺序绑定，片段里的占位符
// 天然排在调用方已绑参数之后，因此不再需要 PG 的编号游标 argStart。
func (f ActivitySessionFilter) cond() (string, []any) {
	switch {
	case f.NodeID != nil:
		return " AND node_id=?", []any{*f.NodeID}
	case f.Main:
		seg := 0
		if f.MainSeg != nil {
			seg = *f.MainSeg
		}
		if seg == 0 { // original segment also owns legacy rows with NULL main_seg
			return " AND worker='mainagent' AND COALESCE(main_seg,0)=0", nil
		}
		return " AND worker='mainagent' AND main_seg=?", []any{seg}
	case f.Worker != "":
		return " AND worker=?", []any{f.Worker}
	default:
		return "", nil
	}
}

// ActivityPage returns one page for reverse (newest-first) pagination of a session:
// up to `limit` steps ending before id `before` (exclusive; before<=0 = the latest
// page), returned in ASCENDING id order for direct display. hasMore reports whether
// still-older steps exist before the returned window (so the client can stop loading
// on scroll-up). Summary-only columns — detail is lazy-loaded via ActivityDetail.
func (s *ExplorationStore) ActivityPage(f ActivitySessionFilter, before int64, limit int) ([]Activity, bool, error) {
	if limit <= 0 {
		limit = 200
	}
	const cols = `id, node_id, COALESCE(worker,''), COALESCE(kind,''), COALESCE(tool,''), COALESCE(tool_use_id,''), is_error, COALESCE(summary,''), metadata, created_at, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, main_seg`
	args := []any{s.expID}
	cond, cargs := f.cond()
	args = append(args, cargs...)
	// `?` 没有 PG 的 $n 复用能力：游标条件出现两次就绑两次。
	args = append(args, before, before)
	args = append(args, limit+1) // one extra row to detect older history
	q := `SELECT ` + cols + `
FROM activity WHERE exploration_id=?` + cond + ` AND (? <= 0 OR id < ?)
ORDER BY id DESC LIMIT ?`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	desc := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.Metadata, &a.CreatedAt,
			&a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheWriteTokens, &a.MainSeg); err != nil {
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

// ActivityPageForTerminalIntent is the inherited session-history boundary. It
// combines ownership, terminal-state and row-kind checks in one query, closing
// the race where a previously completed source intent is reopened before paging.
func (s *ExplorationStore) ActivityPageForTerminalIntent(nodeID, before int64, limit int) ([]Activity, bool, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.worker,''), COALESCE(a.kind,''),
		COALESCE(a.tool,''), COALESCE(a.tool_use_id,''), a.is_error, COALESCE(a.summary,''),
		a.created_at, a.input_tokens, a.output_tokens, a.cache_read_tokens, a.cache_write_tokens
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
	WHERE a.exploration_id=? AND a.node_id=?
	  AND n.kind='intent' AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.kind NOT IN ('thinking','usage')
	  AND (? <= 0 OR a.id < ?)
	ORDER BY a.id DESC LIMIT ?`, s.expID, nodeID, before, before, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	desc := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Worker, &a.Kind, &a.Tool, &a.ToolUseID, &a.IsError, &a.Summary, &a.CreatedAt,
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
	out := make([]Activity, len(desc))
	for i, a := range desc {
		out[len(desc)-1-i] = a
	}
	return out, hasMore, nil
}

// ActivityMaxID returns the task's current maximum activity id (0 when empty). It is
// the task-level snapshot cursor handed to the SSE stream so history (id<=cursor) and
// the live tail (id>cursor) meet with no gap — deliberately task-level, not per
// session, since one SSE covers the whole task.
func (s *ExplorationStore) ActivityMaxID() (int64, error) {
	var max sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(id) FROM activity WHERE exploration_id=?`, s.expID).Scan(&max)
	if err != nil {
		return 0, err
	}
	return max.Int64, nil
}

// MainSession is one resettable main-agent conversation segment of a task. Segment 0
// is the original session (implicit, never stored); further segments are created by
// "新建会话" to start the main agent on a clean transcript while the task's graph,
// assets and goal stay shared.
type MainSession struct {
	Seq       int       `json:"seq"`
	CreatedAt time.Time `json:"created_at"`
}

// CurrentMainSeg returns the highest main-session segment (0 when none created yet).
func (s *ExplorationStore) CurrentMainSeg() (int, error) {
	var seg int
	err := s.db.QueryRow(`SELECT COALESCE(MAX(seq),0) FROM main_sessions WHERE exploration_id=?`, s.expID).Scan(&seg)
	return seg, err
}

// ListMainSessions returns every main-session segment newest-first, always including
// the implicit original segment 0. Segment 0 carries no stored timestamp.
func (s *ExplorationStore) ListMainSessions() ([]MainSession, error) {
	rows, err := s.db.Query(`SELECT seq, created_at FROM main_sessions WHERE exploration_id=? ORDER BY seq DESC`, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MainSession{}
	for rows.Next() {
		var m MainSession
		if err := rows.Scan(&m.Seq, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out = append(out, MainSession{Seq: 0}) // implicit original session (oldest)
	return out, nil
}

// NewMainSession creates the next main-session segment (seq = current+1) and returns
// it. It touches only main_sessions — the task's exploration graph/assets/goal are
// untouched, so the new session starts with a clean transcript over the same task.
func (s *ExplorationStore) NewMainSession() (MainSession, error) {
	// 读最大 seq → Go 端 +1 → 显式 INSERT → 同事务回读 created_at。
	// 两个并发"新建会话"可能算出同一个 seq，裁决权在 uq_main_sessions：撞键的那次
	// 整笔回滚，并在全新事务里重放（绝不在已失败的事务里原地重试）。
	var lastErr error
	for attempt := 0; attempt < mainSessionMaxAttempts; attempt++ {
		m, err := s.newMainSessionOnce()
		if err == nil {
			return m, nil
		}
		lastErr = err
		if !tidb.IsDuplicateKey(err) {
			return MainSession{}, err
		}
	}
	return MainSession{}, fmt.Errorf("main session sequence exhausted after %d attempts: %w", mainSessionMaxAttempts, lastErr)
}

// mainSessionMaxAttempts 是 uq_main_sessions 撞键后的总尝试次数（1 次首试 + 2 次重放）。
const mainSessionMaxAttempts = 3

func (s *ExplorationStore) newMainSessionOnce() (MainSession, error) {
	var m MainSession
	err := s.db.execInTx(func(tx *sql.Tx) error {
		// 锁住当前最大段，把"读旧值 → 算新值 → 写入"关进同一把行锁；首次建会话没有行
		// 可锁，此时唯一键负责裁决。
		var prev sql.NullInt64
		if err := tx.QueryRow(`SELECT seq FROM main_sessions
			WHERE exploration_id=? ORDER BY seq DESC LIMIT 1 FOR UPDATE`, s.expID).Scan(&prev); err != nil && err != sql.ErrNoRows {
			return err
		}
		m.Seq = int(prev.Int64) + 1
		if _, err := tx.Exec(`INSERT INTO main_sessions(exploration_id, seq) VALUES (?, ?)`, s.expID, m.Seq); err != nil {
			return err
		}
		// created_at 由列默认 CURRENT_TIMESTAMP(3) 生成，插入后按 (exploration_id, seq) 回读。
		return tx.QueryRow(`SELECT created_at FROM main_sessions WHERE exploration_id=? AND seq=?`, s.expID, m.Seq).Scan(&m.CreatedAt)
	})
	return m, err
}

// ActivityDetail lazily returns the full detail blob for one step.
func (s *ExplorationStore) ActivityDetail(id int64) (string, error) {
	var d sql.NullString
	err := s.db.QueryRow(`SELECT detail FROM activity WHERE id=? AND exploration_id=?`, id, s.expID).Scan(&d)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return d.String, err
}

// scanTrace reads the summary-only column set shared by the worker-trace tools
// (ActivityTrace / ActivityTraceSearch). Detail is never selected here — it is
// pulled separately, on demand, via ActivityByIDs.
func scanTrace(rows *sql.Rows) ([]Activity, error) {
	out := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Worker, &a.Kind, &a.Tool, &a.IsError, &a.Summary); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

const traceCols = `id, node_id, COALESCE(worker,''), COALESCE(kind,''), COALESCE(tool,''), is_error, COALESCE(summary,'')`

// ActivityTrace lists one work's steps (by intent/node id) for the trace tools:
// summaries only (detail is lazy — fetch via ActivityByIDs). thinking/usage rows
// are dropped so the caller sees the work's actions + observations, not the
// model's internal reasoning or token-accounting noise.
func (s *ExplorationStore) ActivityTrace(nodeID int64, limit int) ([]Activity, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT `+traceCols+`
FROM activity WHERE exploration_id=? AND node_id=? AND kind NOT IN ('thinking','usage')
ORDER BY id LIMIT ?`, s.expID, nodeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// ActivityTraceForTerminalIntent returns one inherited work trace only while its
// source intent is still terminal at query time.
func (s *ExplorationStore) ActivityTraceForTerminalIntent(nodeID int64, limit int) ([]Activity, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.worker,''), COALESCE(a.kind,''), COALESCE(a.tool,''), a.is_error, COALESCE(a.summary,'')
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
	WHERE a.exploration_id=? AND a.node_id=? AND n.kind='intent'
	  AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.kind NOT IN ('thinking','usage')
	ORDER BY a.id LIMIT ?`, s.expID, nodeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// ActivityTraceSearch finds steps whose summary OR detail matches q
// (case-insensitive), returning summaries only. nodeID != nil scopes to one work;
// nil searches across ALL works (node_id IS NOT NULL — planner/main steps have no
// node_id, so this cleanly means "worker traces"). thinking/usage rows dropped.
func (s *ExplorationStore) ActivityTraceSearch(nodeID *int64, q string, limit int) ([]Activity, error) {
	if limit <= 0 {
		limit = 100
	}
	// 模式在 Go 端预先小写、列侧显式 LOWER：列是 utf8mb4_bin，不 LOWER 就退化成
	// 区分大小写的匹配；同一个模式要在 summary/detail 两个谓词里各绑一次。
	like := "%" + strings.ToLower(q) + "%"
	summaryLike, detailLike := expLowerLikeCond("summary"), expLowerLikeCond("detail")
	var rows *sql.Rows
	var err error
	if nodeID != nil {
		rows, err = s.db.Query(`SELECT `+traceCols+`
FROM activity WHERE exploration_id=? AND node_id=? AND kind NOT IN ('thinking','usage')
AND (`+summaryLike+` OR `+detailLike+`) ORDER BY id LIMIT ?`, s.expID, *nodeID, like, like, limit)
	} else {
		rows, err = s.db.Query(`SELECT `+traceCols+`
FROM activity WHERE exploration_id=? AND node_id IS NOT NULL AND kind NOT IN ('thinking','usage')
AND (`+summaryLike+` OR `+detailLike+`) ORDER BY id LIMIT ?`, s.expID, like, like, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// ActivityTraceSearchForTerminalIntent is the scoped inherited search variant;
// terminal eligibility is evaluated by the same statement that reads the rows.
func (s *ExplorationStore) ActivityTraceSearchForTerminalIntent(nodeID int64, q string, limit int) ([]Activity, error) {
	if limit <= 0 {
		limit = 100
	}
	like := "%" + q + "%"
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.worker,''), COALESCE(a.kind,''), COALESCE(a.tool,''), a.is_error, COALESCE(a.summary,'')
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
	WHERE a.exploration_id=? AND a.node_id=? AND n.kind='intent'
	  AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.kind NOT IN ('thinking','usage')
	  AND (`+expLowerLikeCond("a.summary")+` OR `+expLowerLikeCond("a.detail")+`)
	ORDER BY a.id LIMIT ?`, s.expID, nodeID, strings.ToLower(like), strings.ToLower(like), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// ActivityTraceSearchTerminalIntents searches inherited worker history without
// exposing planner/main rows or work that is still open/running in the source.
func (s *ExplorationStore) ActivityTraceSearchTerminalIntents(q string, limit int) ([]Activity, error) {
	if limit <= 0 {
		limit = 100
	}
	like := "%" + q + "%"
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.worker,''), COALESCE(a.kind,''), COALESCE(a.tool,''), a.is_error, COALESCE(a.summary,'')
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
	WHERE a.exploration_id=? AND n.kind='intent'
	  AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.kind NOT IN ('thinking','usage')
	  AND (`+expLowerLikeCond("a.summary")+` OR `+expLowerLikeCond("a.detail")+`)
	ORDER BY a.id LIMIT ?`, s.expID, strings.ToLower(like), strings.ToLower(like), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// AssetRef is a compact exploration node (intent/fact/finding) that references an
// asset via an anchor — for the coverage graph's node drawer.
type AssetRef struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	State        string `json:"state"`
	Summary      string `json:"summary"`
	SourceTaskID int64  `json:"source_task_id,omitempty"`
	Inherited    bool   `json:"inherited,omitempty"`
}

// AssetRefs returns the intents / facts / findings in THIS exploration anchored to
// the given asset id (newest first) — what this task tested / concluded about it.
func (s *ExplorationStore) AssetRefs(assetID int64) ([]AssetRef, error) {
	if assetID <= 0 {
		return []AssetRef{}, nil
	}
	rows, err := s.db.Query(`
SELECT en.id, en.kind, en.state, en.payload
FROM exploration_anchors ea
JOIN exploration_nodes en ON en.id = ea.node_id
WHERE ea.asset_id = ? AND en.exploration_id = ? AND en.kind IN ('intent','fact','finding')
ORDER BY en.id DESC`, assetID, s.expID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AssetRef{}
	for rows.Next() {
		var r AssetRef
		var payload []byte
		if err := rows.Scan(&r.ID, &r.Kind, &r.State, &payload); err != nil {
			return nil, err
		}
		var p map[string]any
		_ = json.Unmarshal(payload, &p)
		for _, k := range []string{"summary", "text"} {
			if v, ok := p[k].(string); ok && strings.TrimSpace(v) != "" {
				r.Summary = v
				break
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActivityTraceSearchExcluding keyword-searches every node's steps in this
// exploration EXCEPT one node's own (excludeNodeID) — so a worker's
// search_all_worker_traces doesn't return its own in-progress trace (which is
// already in its context). excludeNodeID<=0 → no exclusion (searches all nodes).
func (s *ExplorationStore) ActivityTraceSearchExcluding(excludeNodeID int64, q string, limit int) ([]Activity, error) {
	if excludeNodeID <= 0 {
		return s.ActivityTraceSearch(nil, q, limit)
	}
	if limit <= 0 {
		limit = 100
	}
	like := "%" + q + "%"
	rows, err := s.db.Query(`SELECT `+traceCols+`
FROM activity WHERE exploration_id=? AND node_id IS NOT NULL AND node_id <> ?
AND kind NOT IN ('thinking','usage')
AND (`+expLowerLikeCond("summary")+` OR `+expLowerLikeCond("detail")+`) ORDER BY id LIMIT ?`, s.expID, excludeNodeID, strings.ToLower(like), strings.ToLower(like), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTrace(rows)
}

// ActivityByIDs loads full detail for specific step ids (the trace drill-down),
// scoped to this exploration. thinking rows are skipped (never returned, even if
// their id is asked for). Order is ascending id, not the input order.
func (s *ExplorationStore) ActivityByIDs(ids []int64) ([]Activity, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, s.expID)
	list := expInPlaceholders(ids, &args)
	rows, err := s.db.Query(`SELECT id, node_id, COALESCE(kind,''), COALESCE(tool,''), is_error, COALESCE(detail,'')
FROM activity WHERE exploration_id=? AND kind<>'thinking' AND id IN (`+list+`) ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Kind, &a.Tool, &a.IsError, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ActivityByIDsForTerminalIntents is the inherited-detail read boundary. It
// deliberately excludes node-less planner/main rows, non-intent activities, live
// source work, and thinking. Local callers that need legacy behavior use
// ActivityByIDs instead.
func (s *ExplorationStore) ActivityByIDsForTerminalIntents(ids []int64) ([]Activity, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, s.expID)
	list := expInPlaceholders(ids, &args)
	rows, err := s.db.Query(`SELECT a.id, a.node_id, COALESCE(a.kind,''), COALESCE(a.tool,''), a.is_error, COALESCE(a.detail,'')
	FROM activity a
	JOIN exploration_nodes n ON n.id=a.node_id AND n.exploration_id=a.exploration_id
		WHERE a.exploration_id=? AND a.kind NOT IN ('thinking','usage') AND n.kind='intent'
	  AND n.state IN ('done','blocked','exhausted','stopped')
	  AND a.id IN (`+list+`) ORDER BY a.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Activity{}
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.NodeID, &a.Kind, &a.Tool, &a.IsError, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AddStandaloneFinding writes a finding to the standalone findings table, which
// persists across task deletion (task_id / node_id become NULL when the task or
// exploration node is deleted). taskID and nodeID may be 0 (stored as NULL).
func (s *ExplorationStore) AddStandaloneFinding(taskID, nodeID int64, vulnclass, name, severity, summary, evidence, worker string, assetIDs []int64) (int64, error) {
	return s.db.AddFinding(taskID, nodeID, vulnclass, name, severity, summary, evidence, worker, assetIDs)
}
