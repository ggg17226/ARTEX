package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/Autumn-27/artex/notify"
)

var (
	ErrEvidenceConflict = errors.New("流量证据已变更，请刷新后重试")
	ErrFindingNotFound  = errors.New("漏洞不存在")
	ErrEvidenceNotFound = errors.New("流量证据不存在")
)

// findingEvidenceLockKey 是 PG 时代的 advisory lock key（pg_advisory_xact_lock）。
// TiDB 上没有 advisory lock，证据互斥已改为 maintenance 锚点行（见下），这里只把
// 原 key 保留成命名空间记录：它与迁移/测试/企业锁的取值互不冲突（既有门禁用例）。
const findingEvidenceLockKey int64 = 7337741004

// evidenceLockSingletonKey 是证据子系统的互斥锚点键。
//
// 这把锁同时覆盖证据文件系统与其 SQL 引用，共享同一库的所有进程都必须排队，
// 读者、导出与 GC 也不例外。PG 时代靠 `SELECT pg_advisory_xact_lock(7337741004)`；
// TiDB 没有 advisory lock（也没有可靠的会话级 GET_LOCK），等价原语是对 maintenance
// 锁表的这一条单例行加**事务级悲观锁** —— 与 db.go 里 coordinateWithSchemaMigration
// 抢 'main' 行完全同一口径：单行 O(1) 加锁（保住 advisory lock 的成本特征），行锁
// 随 commit/rollback 原子释放，绝不把滞留在池化连接上的锁泄漏给后来的请求。
const evidenceLockSingletonKey = "evidence"

// WithEvidenceTx 在"已持有证据互斥锁"的事务里执行 fn。
func (d *DB) WithEvidenceTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockEvidenceTx(ctx, tx); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// lockEvidenceTx 抢证据互斥锚点行。锚点行由本函数首次使用时自愈式 seed
// （EnsureLockInfrastructure 只 seed main/maintenance 两条单例行），并发 seed
// 被唯一键 uq_maintenance_singleton 挡下，之后仍然只有一行可锁。
func lockEvidenceTx(ctx context.Context, tx *sql.Tx) error {
	seek := `SELECT row_id FROM maintenance WHERE singleton_key = ? FOR UPDATE`
	var rowID int64
	err := tx.QueryRowContext(ctx, seek, evidenceLockSingletonKey).Scan(&rowID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO maintenance (singleton_key, op_id, mode, revision, updated_at)
VALUES(?, NULL, 0, 0, CURRENT_TIMESTAMP(3))`, evidenceLockSingletonKey); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, seek, evidenceLockSingletonKey).Scan(&rowID)
}

type TrafficRef struct {
	TrafficID string `json:"traffic_id"`
	Role      string `json:"role"`
	Note      string `json:"note"`
}

func NormalizeTrafficRefs(refs []TrafficRef) ([]TrafficRef, error) {
	out := make([]TrafficRef, 0, len(refs))
	seen := map[string]bool{}
	for _, ref := range refs {
		ref.TrafficID = strings.TrimSpace(ref.TrafficID)
		if ref.TrafficID == "" {
			return nil, errors.New("traffic_id 不能为空")
		}
		if ref.Role == "" {
			ref.Role = "supporting"
		}
		if !ValidTrafficRole(ref.Role) {
			return nil, fmt.Errorf("无效的流量用途 %q", ref.Role)
		}
		if !seen[ref.TrafficID] {
			out = append(out, ref)
			seen[ref.TrafficID] = true
		}
	}
	return out, nil
}

func ValidTrafficRole(role string) bool {
	return role == "baseline" || role == "proof" || role == "verification" || role == "supporting"
}

type TrafficEvidenceSnapshot struct {
	ID              string    `json:"id"`
	SourceTrafficID string    `json:"source_traffic_id"`
	CapturedAt      int64     `json:"captured_at"`
	URL             string    `json:"url"`
	Method          string    `json:"method"`
	Status          int       `json:"status"`
	ContentType     string    `json:"content_type"`
	ReqHead         string    `json:"req_head,omitempty"`
	RespHead        string    `json:"resp_head,omitempty"`
	ReqHash         string    `json:"req_hash"`
	RespHash        string    `json:"resp_hash"`
	ReqLen          int64     `json:"req_len"`
	RespLen         int64     `json:"resp_len"`
	CreatedAt       time.Time `json:"created_at"`
}

type FindingTrafficBinding struct {
	ID         int64                   `json:"id,string"`
	FindingID  int64                   `json:"finding_id,string"`
	SnapshotID string                  `json:"snapshot_id"`
	Role       string                  `json:"role"`
	Note       string                  `json:"note"`
	Position   int                     `json:"position"`
	CreatedAt  time.Time               `json:"created_at"`
	Snapshot   TrafficEvidenceSnapshot `json:"snapshot"`
}

type FindingTraffic struct {
	FindingID     int64                   `json:"finding_id,string"`
	Version       int64                   `json:"version"`
	ReportVersion int64                   `json:"report_version"`
	Bindings      []FindingTrafficBinding `json:"bindings"`
}

type PreparedTrafficEvidence struct {
	Ref      TrafficRef
	Snapshot TrafficEvidenceSnapshot
}

// Normalize makes the snapshot's text columns safe for PostgreSQL. URL and the
// head blocks come straight off the wire, so a target answering with a non-UTF-8
// header (a GBK `Content-Disposition: filename=…`, a NUL byte) would otherwise
// abort the INSERT and roll back the whole finding — losing a confirmed finding
// over a malformed response header. Applied before hashing so the ID always
// matches the bytes that actually land in the table.
func (s TrafficEvidenceSnapshot) Normalize() TrafficEvidenceSnapshot {
	s.SourceTrafficID = utf8Clean(s.SourceTrafficID)
	s.URL = utf8Clean(s.URL)
	s.Method = utf8Clean(s.Method)
	s.ContentType = utf8Clean(s.ContentType)
	s.ReqHead = utf8Clean(s.ReqHead)
	s.RespHead = utf8Clean(s.RespHead)
	return s
}

func TrafficSnapshotID(snapshot TrafficEvidenceSnapshot) string {
	snapshot = snapshot.Normalize()
	snapshot.ID = ""
	snapshot.CreatedAt = time.Time{}
	raw, _ := json.Marshal(snapshot)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// LockTaskEvidenceTx serializes writes with archive queueing (which locks the
// same task row). Once queued, its snapshot must not acquire new evidence.
func LockTaskEvidenceTx(tx *sql.Tx, taskID int64) error {
	if taskID == 0 {
		return nil
	}
	var deleted sql.NullTime
	if err := tx.QueryRow(`SELECT deleted_at FROM tasks WHERE id = ? FOR UPDATE`, taskID).Scan(&deleted); err != nil {
		return err
	}
	if deleted.Valid {
		return ErrTaskArchiveState
	}
	var state string
	err := tx.QueryRow(`SELECT state FROM task_archives WHERE task_id = ?`, taskID).Scan(&state)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && state != ArchiveFailed {
		return ErrTaskArchiveState
	}
	return nil
}

func LockFindingEvidenceTx(tx *sql.Tx, findingID int64, version *int64) error {
	var taskID sql.NullInt64
	if err := tx.QueryRow(`SELECT task_id FROM findings WHERE id = ?`, findingID).Scan(&taskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFindingNotFound
		}
		return err
	}
	if err := LockTaskEvidenceTx(tx, taskID.Int64); err != nil {
		return err
	}
	var current int64
	if err := tx.QueryRow(`SELECT evidence_version FROM findings WHERE id = ? FOR UPDATE`, findingID).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFindingNotFound
		}
		return err
	}
	if version != nil && *version != current {
		return ErrEvidenceConflict
	}
	return nil
}

func InsertEvidenceSnapshotTx(tx *sql.Tx, s TrafficEvidenceSnapshot) error {
	if s.ID != TrafficSnapshotID(s) {
		return errors.New("证据快照元数据哈希不匹配")
	}
	// The ID was computed over the normalized form; store those same bytes.
	id := s.ID
	s = s.Normalize()
	s.ID = id
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	// 快照是内容寻址的（id 就是正文的 SHA-256），重复写入必然等价，所以 PG 的
	// ON CONFLICT(id) DO NOTHING 在这里落成「普通 INSERT + 只吞 1062」：被忽略的
	// 只有 uq_traffic_evidence_snapshots_id 的重复键，其余违例照常上抛，绝不像
	// INSERT IGNORE 那样把截断/非空错误一起咽掉。
	_, err := tx.Exec(`INSERT INTO traffic_evidence_snapshots
(id,source_traffic_id,captured_at,url,method,status,content_type,req_head,resp_head,req_hash,resp_hash,req_len,resp_len,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		s.ID, s.SourceTrafficID, s.CapturedAt, s.URL, s.Method, s.Status, s.ContentType, s.ReqHead, s.RespHead, s.ReqHash, s.RespHash, s.ReqLen, s.RespLen, s.CreatedAt)
	if err != nil && !tidb.IsDuplicateKey(err) {
		return err
	}
	return nil
}

func AddFindingTrafficTx(tx *sql.Tx, findingID int64, prepared []PreparedTrafficEvidence) error {
	var position int
	if err := tx.QueryRow(`SELECT COALESCE(MAX(position)+1,0) FROM finding_traffic_bindings WHERE finding_id = ?`, findingID).Scan(&position); err != nil {
		return err
	}
	changed := false
	for _, item := range prepared {
		if err := InsertEvidenceSnapshotTx(tx, item.Snapshot); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE traffic_evidence_snapshots SET unreferenced_at=NULL WHERE id = ?`, item.Snapshot.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO finding_traffic_bindings(finding_id,snapshot_id,role,note,position)
VALUES(?, ?, ?, ?, ?)`, findingID, item.Snapshot.ID, item.Ref.Role, item.Ref.Note, position); err != nil {
			// 同一漏洞重复绑定同一快照：唯一键 uq_finding_traffic_bindings 冲突即
			// 「早已绑过」，等价原 ON CONFLICT(finding_id,snapshot_id) DO NOTHING ——
			// 不加行、不推进 position、也不算 changed。
			if !tidb.IsDuplicateKey(err) {
				return err
			}
			continue
		}
		changed = true
		position++
	}
	if changed {
		return bumpEvidenceVersionTx(tx, findingID)
	}
	return nil
}

func bumpEvidenceVersionTx(tx *sql.Tx, findingID int64) error {
	_, err := tx.Exec(`UPDATE findings SET evidence_version=evidence_version+1 WHERE id = ?`, findingID)
	return err
}

func FindingTrafficTx(tx *sql.Tx, findingID int64) (*FindingTraffic, error) {
	out := &FindingTraffic{FindingID: findingID, Bindings: []FindingTrafficBinding{}}
	if err := tx.QueryRow(`SELECT evidence_version,report_evidence_version FROM findings WHERE id = ?`, findingID).Scan(&out.Version, &out.ReportVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFindingNotFound
		}
		return nil, err
	}
	// PG 用 to_jsonb(s) 把整条快照行拼成 JSON 再 Unmarshal；TiDB 没有 to_jsonb，
	// 直接按列展开扫进结构体（字段与历史 JSON 逐一对应，省一次编解码）。
	rows, err := tx.Query(`SELECT b.id,b.finding_id,b.snapshot_id,b.role,b.note,b.position,b.created_at,
s.id,s.source_traffic_id,s.captured_at,s.url,s.method,s.status,s.content_type,s.req_head,s.resp_head,s.req_hash,s.resp_hash,s.req_len,s.resp_len,s.created_at
FROM finding_traffic_bindings b JOIN traffic_evidence_snapshots s ON s.id=b.snapshot_id WHERE b.finding_id = ? ORDER BY b.position,b.id`, findingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b FindingTrafficBinding
		if err := rows.Scan(&b.ID, &b.FindingID, &b.SnapshotID, &b.Role, &b.Note, &b.Position, &b.CreatedAt,
			&b.Snapshot.ID, &b.Snapshot.SourceTrafficID, &b.Snapshot.CapturedAt, &b.Snapshot.URL, &b.Snapshot.Method,
			&b.Snapshot.Status, &b.Snapshot.ContentType, &b.Snapshot.ReqHead, &b.Snapshot.RespHead,
			&b.Snapshot.ReqHash, &b.Snapshot.RespHash, &b.Snapshot.ReqLen, &b.Snapshot.RespLen, &b.Snapshot.CreatedAt); err != nil {
			return nil, err
		}
		out.Bindings = append(out.Bindings, b)
	}
	return out, rows.Err()
}

func (d *DB) GetFindingTraffic(ctx context.Context, findingID int64) (out *FindingTraffic, err error) {
	err = d.WithEvidenceTx(ctx, func(tx *sql.Tx) error { var e error; out, e = FindingTrafficTx(tx, findingID); return e })
	return
}

func (d *DB) EditFindingTraffic(ctx context.Context, findingID, bindingID, version int64, role, note *string, remove bool, order []int64) error {
	if role != nil && !ValidTrafficRole(*role) {
		return errors.New("无效的流量用途")
	}
	return d.WithEvidenceTx(ctx, func(tx *sql.Tx) error {
		if err := LockFindingEvidenceTx(tx, findingID, &version); err != nil {
			return err
		}
		if order != nil {
			current, err := FindingTrafficTx(tx, findingID)
			if err != nil {
				return err
			}
			if len(order) != len(current.Bindings) {
				return ErrEvidenceConflict
			}
			ids := map[int64]bool{}
			for _, b := range current.Bindings {
				ids[b.ID] = true
			}
			for position, id := range order {
				if !ids[id] {
					return ErrEvidenceConflict
				}
				delete(ids, id)
				if _, err := tx.Exec(`UPDATE finding_traffic_bindings SET position=? WHERE finding_id=? AND id=?`, position, findingID, id); err != nil {
					return err
				}
			}
		} else {
			// 存在性先按主键点查判定：TiDB 的 UPDATE 只把"值真的变了"的行计入
			// affected，把绑定改成它本来的 role/note 会返回 0 行，绝不能报成
			// 「证据不存在」。DELETE 的 affected 仍然可信（0 行确实没有这条绑定）。
			var owned int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM finding_traffic_bindings WHERE finding_id = ? AND id = ?`, findingID, bindingID).Scan(&owned); err != nil {
				return err
			}
			if owned == 0 {
				return ErrEvidenceNotFound
			}
			if remove {
				res, err := tx.Exec(`DELETE FROM finding_traffic_bindings WHERE finding_id = ? AND id = ?`, findingID, bindingID)
				if err != nil {
					return err
				}
				if n, err := res.RowsAffected(); err != nil {
					return err
				} else if n == 0 {
					return ErrEvidenceNotFound
				}
			} else if _, err := tx.Exec(`UPDATE finding_traffic_bindings SET role=COALESCE(?,role),note=COALESCE(?,note) WHERE finding_id = ? AND id = ?`,
				role, note, findingID, bindingID); err != nil {
				return err
			}
		}
		return bumpEvidenceVersionTx(tx, findingID)
	})
}

type RecordFindingInput struct {
	TaskID, ExplorationID, IntentID                      int64
	VulnClass, Name, Severity, Summary, Evidence, Worker string
	AssetIDs                                             []int64
}

type RecordedFinding struct {
	FindingID int64           `json:"finding_id,string"`
	NodeID    int64           `json:"finding_node_id,string"`
	Traffic   *FindingTraffic `json:"traffic"`
}

// validateAnchorAssetsTx 在写任何一行之前确认 asset_ids 指向真实存在的资产。
//
// PG 时代 exploration_anchors.asset_id 上挂着外键：指向不存在的资产会被数据库直接
// 拒绝，整笔 Record 原子回滚。TiDB 基线**不建外键**，同一个入参会落成一条悬空锚点，
// 而任务删除级联要把锚点引用的资产行逐个 FOR UPDATE 锁住，锁不到就判定"成员漂移"
// 整笔回滚并放弃删除 —— 一条幻觉 asset id 就能让这条任务**永远删不掉**（启用
// evidence 真实库用例时正是这样把 DeleteTask 卡死的）。迁移后的引用完整性必须由
// 应用层补齐，这里就是 RecordFinding 这条写路径上的补位，错误口径与
// relation_validator 一致（ErrForeignKeyViolation）。
//
// 参数类型取 DBTX 而不是 *sql.Tx：同一条校验要服务两类执行体 ——
//   - RecordFindingTx / ExplorationStore.AddNode 这类已经开着事务的写路径（传 *sql.Tx）；
//   - ExplorationStore.Anchor 这种单条幂等 INSERT（传 *DB，见 db/exploration.go，
//     它自己把"校验 + 插入"包进一个事务，语义与这里完全一致）。
//
// id 列表按 assetRefPurgeBatchSize 分批，避免超长 IN；非正数与重复项由
// normalizeAssetIDs 丢弃（与旧实现里 `id <= 0 || seen[id]` 的跳过口径一致）。
func validateAnchorAssetsTx(ctx context.Context, exec DBTX, assetIDs []int64) error {
	for _, chunk := range chunkInt64(normalizeAssetIDs(assetIDs), assetRefPurgeBatchSize) {
		rows, err := exec.QueryContext(ctx,
			`SELECT id FROM assets WHERE id IN (`+inPlaceholders(len(chunk))+`)`, int64Args(chunk)...)
		if err != nil {
			return err
		}
		found := make(map[int64]bool, len(chunk))
		var scanErr error
		for rows.Next() {
			var id int64
			if scanErr = rows.Scan(&id); scanErr != nil {
				break
			}
			found[id] = true
		}
		if scanErr == nil {
			scanErr = rows.Err()
		}
		rows.Close()
		if scanErr != nil {
			return scanErr
		}
		for _, id := range chunk {
			if !found[id] {
				return fmt.Errorf("%w: assets id=%d 不存在，漏洞只能锚定本库已登记的资产", ErrForeignKeyViolation, id)
			}
		}
	}
	return nil
}

// ctx 由调用方传入本次事务所用的上下文（而非在内部取 context.Background）：
// 事务内新加的推送事件写入同样应受调用方的取消与超时约束。
func RecordFindingTx(ctx context.Context, tx *sql.Tx, in RecordFindingInput, prepared []PreparedTrafficEvidence) (*RecordedFinding, error) {
	if err := LockTaskEvidenceTx(tx, in.TaskID); err != nil {
		return nil, err
	}
	// 锚点引用先校验再落库（详见函数注释）：此刻事务里一行都还没写。
	if err := validateAnchorAssetsTx(ctx, tx, in.AssetIDs); err != nil {
		return nil, err
	}
	if in.TaskID > 0 {
		var expID int64
		if err := tx.QueryRow(`SELECT exploration_id FROM tasks WHERE id = ?`, in.TaskID).Scan(&expID); err != nil {
			return nil, err
		}
		if expID != in.ExplorationID {
			return nil, errors.New("漏洞所属任务与探索记录不匹配")
		}
	}
	if in.IntentID > 0 {
		var intents int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM exploration_nodes WHERE id = ? AND exploration_id = ? AND kind='intent'`,
			in.IntentID, in.ExplorationID).Scan(&intents); err != nil {
			return nil, err
		}
		if intents == 0 {
			return nil, errors.New("intent_id 必须是本任务的意图（关联任务意图只读）")
		}
	}
	payload, _ := json.Marshal(map[string]any{"vulnclass": in.VulnClass, "name": in.Name, "severity": in.Severity, "summary": in.Summary, "evidence": map[string]any{"by": in.Worker, "poc": in.Evidence}})
	out := &RecordedFinding{}
	// TiDB 没有 INSERT … RETURNING：两张表的自增主键都经 LastInsertId() 取回。
	nodeRes, err := tx.Exec(`INSERT INTO exploration_nodes(exploration_id,kind,payload,priority,state,origin)
VALUES(?, 'finding', ?, 9, 'confirmed', ?)`, in.ExplorationID, string(payload), in.Worker)
	if err != nil {
		return nil, err
	}
	if out.NodeID, err = nodeRes.LastInsertId(); err != nil {
		return nil, fmt.Errorf("exploration_nodes: LastInsertId after INSERT: %w", err)
	}
	if out.NodeID < 1 {
		return nil, fmt.Errorf("exploration_nodes: auto-increment id %d out of range", out.NodeID)
	}
	for _, asset := range in.AssetIDs {
		// 唯一键 uq_exploration_anchors(node_id,asset_id)：重复锚点是幂等重放的常态，
		// PG 的 ON CONFLICT DO NOTHING 在这里对应 INSERT IGNORE。
		if _, err := tx.Exec(`INSERT IGNORE INTO exploration_anchors(node_id,asset_id) VALUES(?, ?)`, out.NodeID, asset); err != nil {
			return nil, err
		}
	}
	if in.IntentID > 0 {
		if _, err := tx.Exec(`INSERT INTO exploration_edges(exploration_id,src_id,rel,dst_id) VALUES(?, ?, ?, ?)`, in.ExplorationID, in.IntentID, RelYields, out.NodeID); err != nil {
			return nil, err
		}
	}
	assets := in.AssetIDs
	if assets == nil {
		assets = []int64{}
	}
	raw, _ := json.Marshal(assets)
	// status / report 与 AddFinding 同口径：MEDIUMTEXT NOT NULL 在 TiDB 严格模式
	// 下没有 DEFAULT 可用（声明期默认值不进 DDL），省略列名只会让整条 INSERT 报
	// 1364，应用层默认值必须写进语句里。
	findingRes, err := tx.Exec(`INSERT INTO findings(task_id,node_id,vulnclass,name,severity,summary,evidence,worker,asset_ids,status,report)
VALUES(NULLIF(?, 0),?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, in.TaskID, out.NodeID, in.VulnClass, in.Name, in.Severity, in.Summary, in.Evidence, in.Worker, string(raw), FindingPending, "")
	if err != nil {
		return nil, err
	}
	if out.FindingID, err = findingRes.LastInsertId(); err != nil {
		return nil, fmt.Errorf("findings: LastInsertId after INSERT: %w", err)
	}
	if out.FindingID < 1 {
		return nil, fmt.Errorf("findings: auto-increment id %d out of range", out.FindingID)
	}
	// 在**同一事务**里登记一条推送事件：提交即保证「漏洞落库」与「推送任务存在」
	// 原子一致，不存在提交成功却没入队、消息永久丢失的窗口。
	// 这里的失败被隔离在保存点上、不影响漏洞写入（见函数注释），因此忽略返回值。
	RecordNotificationEventTx(ctx, tx, notify.EventFindingCreated, out.FindingID, notify.Snapshot{
		Kind:      notify.EventFindingCreated,
		FindingID: out.FindingID,
		TaskID:    in.TaskID,
		VulnClass: in.VulnClass,
		Name:      in.Name,
		Severity:  in.Severity,
		Summary:   in.Summary,
		AssetIDs:  assets,
	})
	if err := AddFindingTrafficTx(tx, out.FindingID, prepared); err != nil {
		return nil, err
	}
	out.Traffic, err = FindingTrafficTx(tx, out.FindingID)
	return out, err
}

// RecordFinding is the atomic legacy/no-recorder path used by tools and tests.
func (s *ExplorationStore) RecordFinding(ctx context.Context, in RecordFindingInput) (out *RecordedFinding, err error) {
	in.ExplorationID = s.expID
	err = s.db.WithEvidenceTx(ctx, func(tx *sql.Tx) error { var e error; out, e = RecordFindingTx(ctx, tx, in, nil); return e })
	return
}

func (d *DB) FindingIDByNodeID(nodeID int64) (id int64, err error) {
	err = d.QueryRow(`SELECT id FROM findings WHERE node_id = ?`, nodeID).Scan(&id)
	return
}

// PopulateFindingTrafficIDs only enriches already-visible nodes. It performs no
// discovery or ID guessing, and leaves the legacy node ID unchanged.
func (s *ExplorationStore) PopulateFindingTrafficIDs(nodes []*Node) error {
	byID := map[int64]*Node{}
	var args []any
	for _, n := range nodes {
		if n == nil || n.Kind != KindFinding || byID[n.ID] != nil {
			continue
		}
		byID[n.ID] = n
		args = append(args, n.ID)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := s.db.Query(`SELECT f.id,f.node_id,(SELECT count(*) FROM finding_traffic_bindings b WHERE b.finding_id=f.id) FROM findings f WHERE f.node_id IN (`+questionPlaceholders(len(args))+`)`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var findingID, nodeID int64
		var count int
		if err := rows.Scan(&findingID, &nodeID, &count); err != nil {
			return err
		}
		n := byID[nodeID]
		n.FindingID, n.FindingNodeID, n.TrafficCount = findingID, nodeID, count
	}
	return rows.Err()
}

func (d *DB) SetFindingReportVersionByNodeID(ctx context.Context, nodeID int64, report string, version *int64) (n int64, err error) {
	err = d.WithEvidenceTx(ctx, func(tx *sql.Tx) error {
		var id int64
		if err := tx.QueryRow(`SELECT id FROM findings WHERE node_id = ?`, nodeID).Scan(&id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if err := LockFindingEvidenceTx(tx, id, version); err != nil {
			return err
		}
		// report/evidence 版本与现值完全相同时 affected=0，但这一行确实存在，
		// 所以 n 按"命中一行"回报，绝不让调用方把幂等重写读成「漏洞不存在」。
		if _, err := tx.Exec(`UPDATE findings SET report=?,report_evidence_version=COALESCE(?, CASE WHEN evidence_version=0 THEN 0 ELSE -1 END) WHERE id = ?`,
			report, version, id); err != nil {
			return err
		}
		n = 1
		return nil
	})
	return
}
