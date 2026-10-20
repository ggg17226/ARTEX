package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

const (
	TaskArchiveFormatVersion       = 3
	TaskArchiveLegacyFormatVersion = 1
	TaskArchiveLLMRecordsPath      = "database/llm_records.ndjson"
)

func IsTaskArchiveFormatSupported(version int) bool {
	return version >= TaskArchiveLegacyFormatVersion && version <= TaskArchiveFormatVersion
}

const (
	ArchiveQueued = "archive_queued"
	Archiving     = "archiving"
	ArchiveFailed = "archive_failed"
	ArchiveReady  = "ready"
	RestoreQueued = "restore_queued"
	Restoring     = "restoring"
	RestoreFailed = "restore_failed"
	DeleteQueued  = "delete_queued"
	Deleting      = "deleting"
	DeleteFailed  = "delete_failed"
)

var (
	ErrTaskArchiveNotFound       = errors.New("task archive not found")
	ErrTaskArchiveIneligible     = errors.New("task must be paused or terminal before archiving")
	ErrTaskArchiveQueued         = errors.New("queued task must be paused before archiving")
	ErrTaskArchiveDependent      = errors.New("task is inherited by a live task")
	ErrTaskArchiveState          = errors.New("task archive state does not allow this operation")
	ErrTaskArchiveDeleteBlocked  = errors.New("task archive is required by another archive")
	ErrTaskArchiveFormatMismatch = errors.New("task archive format is not supported")
)

// TaskArchive is the compact TiDB record retained while a task is cold.
// Sensitive profile configuration and API keys are intentionally absent.
type TaskArchive struct {
	ID                      int64           `json:"id,string"`
	TaskID                  int64           `json:"task_id,string"`
	State                   string          `json:"state"`
	Phase                   string          `json:"phase"`
	Progress                int             `json:"progress"`
	Error                   string          `json:"error,omitempty"`
	Warnings                json.RawMessage `json:"warnings"`
	FormatVersion           int             `json:"format_version"`
	ArchivePath             string          `json:"-"`
	SHA256                  string          `json:"sha256,omitempty"`
	OriginalSize            int64           `json:"original_size"`
	CompressedSize          int64           `json:"compressed_size"`
	TaskName                string          `json:"task_name"`
	TaskDescription         string          `json:"task_description"`
	TaskGoal                string          `json:"task_goal"`
	OriginalStatus          string          `json:"original_status"`
	CategoryIDSnapshot      *int64          `json:"category_id,string,omitempty"`
	CategoryNameSnapshot    string          `json:"category_name,omitempty"`
	SourceTaskIDs           []int64         `json:"source_task_ids"`
	RemainingTimeoutSeconds int64           `json:"remaining_timeout_seconds"`
	DataCounts              json.RawMessage `json:"data_counts"`
	AggregateStats          json.RawMessage `json:"aggregate_stats"`
	ArchivedAt              *time.Time      `json:"archived_at,omitempty"`
	RequestedAt             time.Time       `json:"requested_at"`
	CreatedAt               time.Time       `json:"created_at"`
	UpdatedAt               time.Time       `json:"updated_at"`
}

// TaskArchiveBlockers returns one live direct dependent for every source task
// that cannot currently be archived. Dependents already queued for archiving do
// not block their source because the FIFO worker will compact them first.
func (d *DB) TaskArchiveBlockers() (map[int64]int64, error) {
	rows, err := d.Query(`SELECT relation.source_task_id, MIN(child.id)
FROM task_relations relation
JOIN tasks child ON child.id=relation.task_id AND child.deleted_at IS NULL
LEFT JOIN task_archives pending ON pending.task_id=child.id
WHERE pending.id IS NULL OR pending.state NOT IN ('archive_queued','archiving')
GROUP BY relation.source_task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	blockers := map[int64]int64{}
	for rows.Next() {
		var sourceID, dependentID int64
		if err := rows.Scan(&sourceID, &dependentID); err != nil {
			return nil, err
		}
		blockers[sourceID] = dependentID
	}
	return blockers, rows.Err()
}

type TaskArchivePage struct {
	Items []TaskArchive `json:"items"`
	Total int           `json:"total"`
	Page  int           `json:"page"`
	Size  int           `json:"size"`
}

// TaskArchiveSnapshot is serialized into manifest.json inside the cold package.
// Small tables remain JSON arrays in Tables. Large v2 tables are streamed to
// package files listed in StreamedTables so their size is not bounded by memory.
type TaskArchiveSnapshot struct {
	FormatVersion     int                        `json:"format_version"`
	CreatedAt         time.Time                  `json:"created_at"`
	TaskID            int64                      `json:"task_id"`
	ExplorationID     int64                      `json:"exploration_id"`
	SourceTaskIDs     []int64                    `json:"source_task_ids"`
	Hosts             []string                   `json:"hosts"`
	ExclusiveHosts    []string                   `json:"exclusive_hosts"`
	ExclusiveAssetIDs []int64                    `json:"exclusive_asset_ids"`
	Tables            map[string]json.RawMessage `json:"tables"`
	StreamedTables    map[string]string          `json:"streamed_tables,omitempty"`
	DataCounts        map[string]int64           `json:"data_counts"`
	AggregateStats    map[string]any             `json:"aggregate_stats"`
}

func scanTaskArchive(sc interface{ Scan(...any) error }) (*TaskArchive, error) {
	var item TaskArchive
	var sources string
	err := sc.Scan(
		&item.ID, &item.TaskID, &item.State, &item.Phase, &item.Progress, &item.Error,
		&item.Warnings, &item.FormatVersion, &item.ArchivePath, &item.SHA256,
		&item.OriginalSize, &item.CompressedSize, &item.TaskName, &item.TaskDescription,
		&item.TaskGoal, &item.OriginalStatus, &item.CategoryIDSnapshot,
		&item.CategoryNameSnapshot, &sources, &item.RemainingTimeoutSeconds,
		&item.DataCounts, &item.AggregateStats, &item.ArchivedAt, &item.RequestedAt,
		&item.CreatedAt, &item.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(sources), &item.SourceTaskIDs); err != nil {
		return nil, err
	}
	if len(item.Warnings) == 0 {
		item.Warnings = json.RawMessage("[]")
	}
	if len(item.DataCounts) == 0 {
		item.DataCounts = json.RawMessage("{}")
	}
	if len(item.AggregateStats) == 0 {
		item.AggregateStats = json.RawMessage("{}")
	}
	return &item, nil
}

// taskArchiveCols 是 task_archives 的读取列清单。source_task_ids 在新库是
// MEDIUMTEXT 的 JSON 数组（原 PG bigint[] + array_to_json(...)::text），直接读原文。
const taskArchiveCols = `id, task_id, state, phase, progress, COALESCE(error,''), warnings,
format_version, COALESCE(archive_path,''), COALESCE(sha256,''), original_size,
compressed_size, COALESCE(task_name,''), COALESCE(task_description,''),
COALESCE(task_goal,''), COALESCE(original_status,''), category_id_snapshot,
COALESCE(category_name_snapshot,''), COALESCE(source_task_ids,'[]'),
remaining_timeout_seconds, data_counts, aggregate_stats, archived_at,
requested_at, created_at, updated_at`

func (d *DB) GetTaskArchive(id int64) (*TaskArchive, error) {
	item, err := scanTaskArchive(d.QueryRow(`SELECT `+taskArchiveCols+` FROM task_archives WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return item, err
}

func (d *DB) GetTaskArchiveByTask(taskID int64) (*TaskArchive, error) {
	item, err := scanTaskArchive(d.QueryRow(`SELECT `+taskArchiveCols+` FROM task_archives WHERE task_id=?`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return item, err
}

func (d *DB) ListTaskArchives(search, state string, page, size int) (TaskArchivePage, error) {
	if page < 1 {
		page = 1
	}
	if size <= 0 || size > 100 {
		size = 20
	}
	search = strings.TrimSpace(search)
	state = strings.TrimSpace(state)
	// 表默认排序规则是 utf8mb4_bin（大小写敏感），原来的 ILIKE 在 TiDB 上必须
	// 两侧同时 LOWER，否则搜 "Cold" 就再也找不到 "cold task"。
	where := `WHERE (?='' OR CAST(task_id AS CHAR) LIKE CONCAT('%',?,'%') OR LOWER(task_name) LIKE LOWER(CONCAT('%',?,'%')) OR LOWER(task_description) LIKE LOWER(CONCAT('%',?,'%')))
AND (?='' OR state=?)`
	var out TaskArchivePage
	out.Page, out.Size = page, size
	if err := d.QueryRow(`SELECT count(*) FROM task_archives `+where,
		search, search, search, search, state, state).Scan(&out.Total); err != nil {
		return out, err
	}
	rows, err := d.Query(`SELECT `+taskArchiveCols+` FROM task_archives `+where+`
ORDER BY COALESCE(archived_at, requested_at) DESC, id DESC LIMIT ? OFFSET ?`,
		search, search, search, search, state, state, size, (page-1)*size)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		item, err := scanTaskArchive(rows)
		if err != nil {
			return out, err
		}
		out.Items = append(out.Items, *item)
	}
	return out, rows.Err()
}

// QueueTaskArchive validates lifecycle and direct inheritance while holding the
// task row. A failed archive can be explicitly retried through the same API.
//
// 原实现是一条 INSERT ... ON CONFLICT (task_id) DO UPDATE SET ...=CASE WHEN
// task_archives.state='archive_failed' ...；TiDB 的 ON DUPLICATE KEY UPDATE 赋值
// 从左到右求值，后面的 CASE 会读到刚被改写的 state，语义会漂移。这里改成
// 「先锁归档行、再在 Go 里判定是否重开」，重开条件与原 CASE 完全一致。
func (d *DB) QueueTaskArchive(taskID int64) (*TaskArchive, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	var name, description, goal, status, categoryName string
	var categoryID *int64
	var paused, queued bool
	var deadline *time.Time
	err = tx.QueryRow(`SELECT COALESCE(t.name,''), t.description, t.goal, t.status,
 t.category_id, COALESCE(c.name,''), t.paused, t.queued, t.deadline_at
FROM tasks t LEFT JOIN task_categories c ON c.id=t.category_id
WHERE t.id=? AND t.deleted_at IS NULL FOR UPDATE`, taskID).Scan(
		&name, &description, &goal, &status, &categoryID, &categoryName, &paused, &queued, &deadline,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskArchiveNotFound
	}
	if err != nil {
		return nil, err
	}
	if queued {
		return nil, ErrTaskArchiveQueued
	}
	if !paused && !IsTerminal(status) {
		return nil, ErrTaskArchiveIneligible
	}
	var dependent int64
	err = tx.QueryRow(`SELECT child.id FROM task_relations relation
JOIN tasks child ON child.id=relation.task_id AND child.deleted_at IS NULL
LEFT JOIN task_archives pending ON pending.task_id=child.id
WHERE relation.source_task_id=?
  AND (pending.id IS NULL OR pending.state NOT IN ('archive_queued','archiving'))
LIMIT 1 FOR UPDATE`, taskID).Scan(&dependent)
	if err == nil {
		return nil, fmt.Errorf("%w: task %d", ErrTaskArchiveDependent, dependent)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var sources []int64
	rows, err := tx.Query(`SELECT source_task_id FROM task_relations WHERE task_id=? ORDER BY created_at, source_task_id`, taskID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		sources = append(sources, id)
	}
	// 迭代中途的驱动错误只会让 Next() 提前结束；不查 Rows.Err() 就会把残缺的
	// source_task_ids 当成完整结果写进归档元数据。
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if sources == nil {
		sources = []int64{}
	}
	remaining := int64(0)
	if paused && deadline != nil {
		remaining = int64(time.Until(*deadline).Seconds())
		if remaining < 0 {
			remaining = 0
		}
	}
	sourcesRaw, err := json.Marshal(sources)
	if err != nil {
		return nil, err
	}
	// 归档行上的行锁：判定与写入之间不允许别的 worker 抢先改状态。
	var existingId int64
	var existingState string
	err = tx.QueryRow(`SELECT id, state FROM task_archives WHERE task_id=? FOR UPDATE`, taskID).Scan(&existingId, &existingState)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(`INSERT INTO task_archives(
 task_id,state,phase,progress,error,warnings,format_version,task_name,
 task_description,task_goal,original_status,category_id_snapshot,
 category_name_snapshot,source_task_ids,remaining_timeout_seconds,requested_at)
VALUES(?,?,'queued',0,'','[]',?,?,?,?,?,?,?,?,?,NOW(3))`,
			taskID, ArchiveQueued, TaskArchiveFormatVersion, name, description, goal, status,
			categoryID, categoryName, string(sourcesRaw), remaining); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		if existingState != ArchiveFailed {
			break
		}
		if _, err := tx.Exec(`UPDATE task_archives SET
 state=?,phase='queued',progress=0,error='',format_version=?,
 task_name=?,task_description=?,task_goal=?,original_status=?,
 category_id_snapshot=?,category_name_snapshot=?,source_task_ids=?,
 remaining_timeout_seconds=?,requested_at=NOW(3)
WHERE id=?`, ArchiveQueued, TaskArchiveFormatVersion, name, description, goal, status,
			categoryID, categoryName, string(sourcesRaw), remaining, existingId); err != nil {
			return nil, err
		}
	}
	item, err := scanTaskArchive(tx.QueryRow(`SELECT `+taskArchiveCols+` FROM task_archives WHERE task_id=?`, taskID))
	if err != nil {
		return nil, err
	}
	if item.State != ArchiveQueued && item.State != ArchiveFailed {
		return nil, fmt.Errorf("%w: current state %s", ErrTaskArchiveState, item.State)
	}
	return item, tx.Commit()
}

func (d *DB) QueueTaskArchiveRestore(id int64) (*TaskArchive, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	// 原 UPDATE ... RETURNING 在 TiDB 上不存在：改成先按状态谓词更新，再回读整行。
	res, err := tx.Exec(`UPDATE task_archives
SET state=?, phase='queued', progress=0, error='', requested_at=NOW(3)
WHERE id=? AND state IN ('ready','restore_failed')`, RestoreQueued, id)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, ErrTaskArchiveState
	}
	item, err := scanTaskArchive(tx.QueryRow(`SELECT `+taskArchiveCols+` FROM task_archives WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return item, tx.Commit()
}

func (d *DB) QueueTaskArchiveDelete(id int64) (*TaskArchive, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	var taskID int64
	if err := tx.QueryRow(`SELECT task_id FROM task_archives WHERE id=? AND state IN ('ready','delete_failed') FOR UPDATE`, id).Scan(&taskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskArchiveState
		}
		return nil, err
	}
	var dependent int64
	// source_task_ids 是 JSON 数组文本，原 $2=ANY(source_task_ids) 换成 JSON_CONTAINS。
	err = tx.QueryRow(`SELECT task_id FROM task_archives
WHERE id<>? AND JSON_CONTAINS(COALESCE(source_task_ids,'[]'), CAST(? AS JSON)) AND state NOT IN ('delete_queued','deleting') LIMIT 1`, id, taskID).Scan(&dependent)
	if err == nil {
		return nil, fmt.Errorf("%w: task %d", ErrTaskArchiveDeleteBlocked, dependent)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	res, err := tx.Exec(`UPDATE task_archives
SET state=?, phase='queued', progress=0, error='', requested_at=NOW(3) WHERE id=?`, DeleteQueued, id)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, ErrTaskArchiveState
	}
	item, err := scanTaskArchive(tx.QueryRow(`SELECT `+taskArchiveCols+` FROM task_archives WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return item, tx.Commit()
}

// RecoverTaskArchiveJobs keeps restore/delete resumable after an unclean shutdown.
// An interrupted archive requires an explicit retry: automatic startup retries can
// otherwise form a crash loop when the prior process was killed by resource limits.
func (d *DB) RecoverTaskArchiveJobs() error {
	// SET 赋值顺序即求值顺序：error 必须排在 state 之前，否则它读到的是刚刚
	// 被改写成的新状态（'archive_failed'），归档中断原因就永久丢失了。
	_, err := d.Exec(`UPDATE task_archives SET
 error=CASE WHEN state='archiving' THEN '上次归档进程异常退出，请手动重试' ELSE '' END,
 phase='interrupted',
 state=CASE state WHEN 'archiving' THEN 'archive_failed'
                  WHEN 'restoring' THEN 'restore_queued'
                  WHEN 'deleting' THEN 'delete_queued' ELSE state END
WHERE state IN ('archiving','restoring','deleting')`)
	return err
}

// ClaimTaskArchiveJob claims one persistent FIFO item for the single archive
// worker. It returns nil when the queue is empty.
//
// 原 SELECT ... FOR UPDATE SKIP LOCKED 依赖 PG 的跳过锁；TiDB 没有 SKIP LOCKED，
// 归档本来就是单 worker FIFO 队列，改成「锁定队首 + 同行推进状态」的两步 CAS。
func (d *DB) ClaimTaskArchiveJob(ctx context.Context) (*TaskArchive, error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	var id int64
	var queuedState string
	err = tx.QueryRowContext(ctx, `SELECT id,state FROM task_archives
WHERE state IN ('archive_queued','restore_queued','delete_queued')
ORDER BY requested_at,id LIMIT 1 FOR UPDATE`).Scan(&id, &queuedState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	active := map[string]string{ArchiveQueued: Archiving, RestoreQueued: Restoring, DeleteQueued: Deleting}[queuedState]
	if _, err := tx.ExecContext(ctx, `UPDATE task_archives
SET state=?,phase='starting',progress=1,error='' WHERE id=?`, active, id); err != nil {
		return nil, err
	}
	item, err := scanTaskArchive(tx.QueryRowContext(ctx, `SELECT `+taskArchiveCols+` FROM task_archives WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return item, tx.Commit()
}

func (d *DB) UpdateTaskArchiveProgress(id int64, phase string, progress int) error {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	_, err := d.Exec(`UPDATE task_archives SET phase=?,progress=? WHERE id=?`, phase, progress, id)
	return err
}

func (d *DB) AppendTaskArchiveWarning(id int64, warning string) error {
	if strings.TrimSpace(warning) == "" {
		return nil
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	// warnings 是 MEDIUMTEXT 的 JSON 数组；PG 的 jsonb 拼接运算符在 TiDB 上只能
	// 读改写（加行锁，保证并发追加不丢条目）。
	var raw string
	if err := tx.QueryRow(`SELECT warnings FROM task_archives WHERE id=? FOR UPDATE`, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	items := []json.RawMessage{}
	if text := strings.TrimSpace(raw); text != "" && text != "null" {
		if err := json.Unmarshal([]byte(text), &items); err != nil {
			items = items[:0]
		}
	}
	entry, err := json.Marshal(warning)
	if err != nil {
		return err
	}
	items = append(items, entry)
	encoded, err := json.Marshal(items)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE task_archives SET warnings=? WHERE id=?`, string(encoded), id); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) IsTaskArchiveRestored(id int64) (bool, error) {
	// TiDB 的 `a IS NULL AND b IS NULL` 返回 0/1 而不是 boolean，直接 COUNT 计数。
	var joined, restored int
	err := d.QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN task.deleted_at IS NULL AND task.archived_at IS NULL THEN 1 ELSE 0 END),0)
FROM task_archives archive JOIN tasks task ON task.id=archive.task_id WHERE archive.id=?`, id).Scan(&joined, &restored)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrTaskArchiveNotFound
		}
		return false, err
	}
	if joined == 0 {
		return false, ErrTaskArchiveNotFound
	}
	return restored > 0, nil
}

func (d *DB) FailTaskArchiveJob(id int64, activeState string, cause error) error {
	failed := map[string]string{Archiving: ArchiveFailed, Restoring: RestoreFailed, Deleting: DeleteFailed}[activeState]
	if failed == "" {
		return fmt.Errorf("unknown active archive state %q", activeState)
	}
	message := "unknown archive failure"
	if cause != nil {
		message = cause.Error()
	}
	_, err := d.Exec(`UPDATE task_archives SET state=?,phase='failed',error=? WHERE id=?`, failed, message, id)
	return err
}

// =====================================================================
// 归档导出：真实 Catalog 双白名单（M06，闭环 F06）
// =====================================================================

// archiveCatalogOnce/Maps 缓存真实 Catalog（db/schema）的表列元数据。导出投影与
// 导入写入都以它为准，绝不再靠 SELECT *：TiDB 上隐藏主键 row_id 与 STORED 生成列
// 都会出现在 * 里，一旦进入 manifest 就会把物理键写回线上库。
var (
	archiveCatalogOnce sync.Once
	archiveCatalogMap  map[string]*schema.Table
)

func archiveCatalogTable(table string) (*schema.Table, bool) {
	archiveCatalogOnce.Do(func() {
		archiveCatalogMap = make(map[string]*schema.Table, 64)
		for _, item := range schema.AllTables() {
			archiveCatalogMap[item.Name] = item
		}
	})
	found, ok := archiveCatalogMap[table]
	return found, ok
}

// archiveDerivedColumns 声明由 canonical identity 派生的 identity/hash 列：它们
// 不进入 manifest（导出白名单），导入侧一律按当前行的语义列重新计算（M25）。
var archiveDerivedColumns = map[string][]string{
	"assets": {
		"identity_root_domain", "hash_root_domain",
		"identity_ip", "hash_ip",
		"identity_subdomain", "hash_subdomain",
		"identity_app_bundle", "hash_app_bundle",
		"identity_app_name", "hash_app_name",
		"identity_service_http", "hash_service_http",
		"identity_service_other", "hash_service_other",
		"identity_endpoint", "hash_endpoint",
	},
	"task_scope":             {"identity", "identity_hash"},
	"side_question_requests": {"identity", "identity_hash"},
}

// archiveIsDerivedColumn 判定某列是否为该表的 canonical identity 派生列。
func archiveIsDerivedColumn(table, column string) bool {
	for _, name := range archiveDerivedColumns[table] {
		if name == column {
			return true
		}
	}
	return false
}

// archiveHiddenPhysicalKey 返回该表的隐藏物理主键列名（原复合主键表在 TiDB 上
// 统一落到自增 row_id）。该列既不能出现在 manifest，也不能被写回线上库。
func archiveHiddenPhysicalKey(table string) string {
	item, ok := archiveCatalogTable(table)
	if !ok || item.PKColumn != "row_id" {
		return ""
	}
	return "row_id"
}

// ArchiveTableWhitelist 返回某张表在归档 manifest 里的严格列白名单，供导出投影、
// 测试断言与恢复写入共用（0 个隐藏 row_id、0 个生成列、0 个派生 hash 列）。
func ArchiveTableWhitelist(table string) ([]string, error) {
	item, ok := archiveCatalogTable(table)
	if !ok {
		return nil, fmt.Errorf("archive whitelist: table %q is absent from the real catalog", table)
	}
	hidden := archiveHiddenPhysicalKey(table)
	columns := make([]string, 0, len(item.Columns))
	for _, column := range item.Columns {
		if column.Generated != "" {
			continue
		}
		if hidden != "" && column.Name == hidden {
			continue
		}
		if archiveIsDerivedColumn(table, column.Name) {
			continue
		}
		columns = append(columns, column.Name)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("archive whitelist: table %q has no exportable columns", table)
	}
	return columns, nil
}

// archiveColumnOf 取某列的真实 Catalog 定义（类型驱动 JSON 编解码）。
func archiveColumnOf(table, column string) (schema.Column, error) {
	item, ok := archiveCatalogTable(table)
	if !ok {
		return schema.Column{}, fmt.Errorf("archive codec: table %q is absent from the real catalog", table)
	}
	for _, candidate := range item.Columns {
		if candidate.Name == column {
			return candidate, nil
		}
	}
	return schema.Column{}, fmt.Errorf("archive codec: column %q.%q is absent from the real catalog", table, column)
}

// archiveQuoteColumns 把列名列表渲染成反引号投影。
func archiveQuoteColumns(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, "`"+column+"`")
	}
	return strings.Join(quoted, ", ")
}

// archiveEncodeColumnValue 按 Catalog 列类型把一个 database/sql 读出的值编码成
// manifest 里的 JSON 片段：
//   - JSON 列（MEDIUMTEXT 存 JSON 文本）还原成嵌套 JSON，保持既有 manifest 形状；
//   - 布尔列统一输出 true/false（TiDB 驱动在文本协议下回传 0/1 文本）；
//   - 时间列固定 UTC RFC3339，导入侧 archiveTimestamp 可逆解析；
//   - 二进制列走 base64（encoding/json 默认），导入侧同口径解回。
func archiveEncodeColumnValue(column schema.Column, value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("null"), nil
	}
	switch column.Type {
	case schema.TJSON:
		switch typed := value.(type) {
		case []byte:
			if json.Valid(typed) {
				return json.RawMessage(append([]byte(nil), typed...)), nil
			}
			return json.Marshal(string(typed))
		case string:
			if json.Valid([]byte(typed)) {
				return json.RawMessage(typed), nil
			}
			return json.Marshal(typed)
		}
		return json.Marshal(value)
	case schema.TBool:
		var truthy bool
		switch typed := value.(type) {
		case bool:
			truthy = typed
		case int64:
			truthy = typed != 0
		case uint64:
			truthy = typed != 0
		case []byte:
			truthy = archiveBoolText(string(typed))
		case string:
			truthy = archiveBoolText(typed)
		default:
			return nil, fmt.Errorf("cannot encode %T as boolean", value)
		}
		return json.Marshal(truthy)
	case schema.TDate3:
		switch typed := value.(type) {
		case time.Time:
			return json.Marshal(typed.UTC().Format(time.RFC3339Nano))
		case []byte:
			return json.Marshal(string(typed))
		case string:
			return json.Marshal(typed)
		}
		return nil, fmt.Errorf("cannot encode %T as datetime", value)
	case schema.TBinary, schema.TVarbinary, schema.TMediumBlob:
		switch typed := value.(type) {
		case []byte:
			return json.Marshal(typed)
		case string:
			return json.Marshal([]byte(typed))
		}
		return nil, fmt.Errorf("cannot encode %T as binary", value)
	case schema.TText, schema.TVarchar, schema.TChar:
		switch typed := value.(type) {
		case string:
			return json.Marshal(typed)
		case []byte:
			return json.Marshal(string(typed))
		}
		return nil, fmt.Errorf("cannot encode %T as text", value)
	default:
		switch typed := value.(type) {
		case int64, uint64, float64, int, int32:
			return json.Marshal(typed)
		case []byte:
			if parsed, err := strconv.ParseInt(string(typed), 10, 64); err == nil {
				return json.Marshal(parsed)
			}
			return json.Marshal(string(typed))
		case string:
			if parsed, err := strconv.ParseInt(typed, 10, 64); err == nil {
				return json.Marshal(parsed)
			}
			return json.Marshal(typed)
		}
		return json.Marshal(value)
	}
}

func archiveBoolText(text string) bool {
	text = strings.TrimSpace(text)
	return text != "" && text != "0"
}

// archiveProjectedRows 把一次显式白名单投影查询折叠成「每行一段 JSON 对象文本」
// 的行游标，复用既有 encodeArchiveRows / writeArchiveRows 的 JSON 数组与 NDJSON
// 框架（两者都只要求 Next/Scan(*[]byte)/Err）。
type archiveProjectedRows struct {
	rows       *sql.Rows
	table      string
	columns    []string
	definition []schema.Column
	current    json.RawMessage
	failure    error
	started    bool
}

func newArchiveProjectedRows(rows *sql.Rows, table string, columns []string) (*archiveProjectedRows, error) {
	definition := make([]schema.Column, 0, len(columns))
	for _, column := range columns {
		item, err := archiveColumnOf(table, column)
		if err != nil {
			return nil, err
		}
		definition = append(definition, item)
	}
	return &archiveProjectedRows{rows: rows, table: table, columns: columns, definition: definition}, nil
}

func (a *archiveProjectedRows) Next() bool {
	if a.failure != nil || !a.rows.Next() {
		return false
	}
	values := make([]any, len(a.columns))
	targets := make([]any, len(a.columns))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := a.rows.Scan(targets...); err != nil {
		a.failure = fmt.Errorf("archive scan %s: %w", a.table, err)
		return false
	}
	row := make(map[string]json.RawMessage, len(a.columns))
	for i, name := range a.columns {
		encoded, err := archiveEncodeColumnValue(a.definition[i], values[i])
		if err != nil {
			a.failure = fmt.Errorf("archive encode %s.%s: %w", a.table, name, err)
			return false
		}
		row[name] = encoded
	}
	// json.Marshal(map) 按列名排序，manifest 字节级稳定，可直接参与冷包校验。
	encoded, err := json.Marshal(row)
	if err != nil {
		a.failure = fmt.Errorf("archive marshal %s: %w", a.table, err)
		return false
	}
	a.current = encoded
	a.started = true
	return true
}

func (a *archiveProjectedRows) Scan(dest ...any) error {
	if len(dest) != 1 || !a.started {
		return fmt.Errorf("invalid archive projection row scan on %s", a.table)
	}
	target, ok := dest[0].(*[]byte)
	if !ok {
		return fmt.Errorf("archive projection destination is %T", dest[0])
	}
	*target = append((*target)[:0], a.current...)
	return nil
}

func (a *archiveProjectedRows) Err() error {
	if a.failure != nil {
		return a.failure
	}
	return a.rows.Err()
}

// archiveSelectProjection 按白名单投影读取一张表，产出 manifest 用的 JSON 数组。
func archiveSelectProjection(q rowsQuerier, table, where string, args []any, orderBy string) (json.RawMessage, int64, error) {
	return archiveSelectStream(q, table, where, args, orderBy, nil)
}

// archiveSelectStream 与 archiveSelectProjection 同一条投影查询；writer 非空时把
// 每行以 NDJSON 流式写出（manifest 里只留空数组），用于 llm_records 这类大表。
func archiveSelectStream(q rowsQuerier, table, where string, args []any, orderBy string, writer io.Writer) (json.RawMessage, int64, error) {
	columns, err := ArchiveTableWhitelist(table)
	if err != nil {
		return nil, 0, err
	}
	query := "SELECT " + archiveQuoteColumns(columns) + " FROM `" + table + "`"
	if strings.TrimSpace(where) != "" {
		query += " WHERE " + where
	}
	if strings.TrimSpace(orderBy) != "" {
		query += " ORDER BY " + orderBy
	}
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("archive query %s: %w", table, err)
	}
	defer rows.Close()
	projected, err := newArchiveProjectedRows(rows, table, columns)
	if err != nil {
		return nil, 0, err
	}
	if writer != nil {
		count, err := writeArchiveRows(projected, writer)
		if err != nil {
			return nil, 0, err
		}
		if err := projected.Err(); err != nil {
			return nil, 0, err
		}
		return json.RawMessage("[]"), count, nil
	}
	raw, count, err := encodeArchiveRows(projected)
	if err != nil {
		return nil, 0, err
	}
	if err := projected.Err(); err != nil {
		return nil, 0, err
	}
	return raw, count, nil
}

func encodeArchiveRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) (json.RawMessage, int64, error) {
	var output bytes.Buffer
	output.WriteByte('[')
	var count int64
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, 0, err
		}
		if count > 0 {
			output.WriteByte(',')
		}
		output.Write(raw)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	output.WriteByte(']')
	return json.RawMessage(output.Bytes()), count, nil
}

func writeArchiveRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}, writer io.Writer) (int64, error) {
	var count int64
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		if written, err := writer.Write(raw); err != nil {
			return 0, err
		} else if written != len(raw) {
			return 0, io.ErrShortWrite
		}
		if written, err := io.WriteString(writer, "\n"); err != nil {
			return 0, err
		} else if written != 1 {
			return 0, io.ErrShortWrite
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func rawRowCount(raw json.RawMessage) int64 {
	var rows []json.RawMessage
	if json.Unmarshal(raw, &rows) != nil {
		return 0
	}
	return int64(len(rows))
}

// archiveAssetCandidates 是任务归属资产的候选集子查询（4 个占位符：
// task, task, exploration, task）。原实现依赖 =ANY(bigint[]) 与
// jsonb_array_elements_text(...) CROSS JOIN LATERAL；新库 task_ids/asset_ids 都是
// JSON 数组文本，统一用 JSON_CONTAINS 表达，不再需要 JSON_TABLE/LATERAL。
func archiveAssetCandidates() string {
	return `SELECT owned.id FROM assets owned WHERE ` + assetJSONContains("owned.task_ids", "?") + `
UNION SELECT link.asset_id FROM task_asset_links link WHERE link.task_id = ?
UNION SELECT anchor.asset_id FROM exploration_anchors anchor
       JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id = ?
UNION SELECT finding_asset.id FROM assets finding_asset
       WHERE EXISTS (SELECT 1 FROM findings finding WHERE finding.task_id = ? AND
         JSON_CONTAINS(COALESCE(finding.asset_ids, '[]'), CAST(finding_asset.id AS JSON)))`
}

// archiveAssetCandidatesArgs 是候选集子查询的固定参数顺序。
func archiveAssetCandidatesArgs(taskID, expID int64) []any {
	return []any{taskID, taskID, expID, taskID}
}

// SnapshotTaskArchive reads one consistent TiDB snapshot. Task-owned Agent
// writes are already quiescent at the server barrier; TiDB 的快照隔离保证资产视图
// 与账目视图彼此一致。
func (d *DB) SnapshotTaskArchive(taskID int64) (*TaskArchiveSnapshot, error) {
	return d.snapshotTaskArchive(taskID, nil)
}

// SnapshotTaskArchiveWithLLMRecords streams the heavyweight record history to
// llmRecords while all other task-owned data is read from the same snapshot.
func (d *DB) SnapshotTaskArchiveWithLLMRecords(taskID int64, llmRecords io.Writer) (*TaskArchiveSnapshot, error) {
	if llmRecords == nil {
		return nil, errors.New("nil LLM record archive writer")
	}
	return d.snapshotTaskArchive(taskID, llmRecords)
}

// archiveTableExport 描述一张表在 manifest 里的导出投影。
type archiveTableExport struct {
	name    string
	where   string
	args    func(taskID, expID int64) []any
	orderBy string
}

func archiveExportPlan(taskID, expID int64) []archiveTableExport {
	single := func(args ...any) func(int64, int64) []any {
		return func(int64, int64) []any { return args }
	}
	return []archiveTableExport{
		{"tasks", `id = ? AND deleted_at IS NULL`, single(taskID), "id"},
		{"explorations", `id = ?`, single(expID), "id"},
		{"exploration_nodes", `exploration_id = ?`, single(expID), "id"},
		{"exploration_edges", `exploration_id = ?`, single(expID), "src_id, dst_id, rel"},
		{"exploration_anchors", `node_id IN (SELECT id FROM exploration_nodes WHERE exploration_id = ?)`, single(expID), "node_id, asset_id"},
		{"task_constraints", `exploration_id = ?`, single(expID), "id"},
		{"activity", `exploration_id = ?`, single(expID), "id"},
		{"task_relations", `task_id = ?`, single(taskID), "created_at, source_task_id"},
		{"task_asset_links", `task_id = ?`, single(taskID), "asset_id"},
		{"task_llm_profiles", `task_id = ?`, single(taskID), "position"},
		{"task_scope", `task_id = ?`, single(taskID), "id"},
		{"findings", `task_id = ?`, single(taskID), "id"},
		{"finding_traffic_bindings", `finding_id IN (SELECT id FROM findings WHERE task_id = ?)`, single(taskID), "finding_id, position, id"},
		{"traffic_evidence_snapshots", `id IN (SELECT snapshot_id FROM finding_traffic_bindings WHERE finding_id IN (SELECT id FROM findings WHERE task_id = ?))`, single(taskID), "id"},
		{"llm_records", `COALESCE(task_id,'') = ?`, single(strconv.FormatInt(taskID, 10)), "id"},
		{"llm_usage", `COALESCE(task_id,'') = ? OR exploration_id = ?`, single(strconv.FormatInt(taskID, 10), expID), "id"},
		{"skill_usage", `task_id = ? OR exploration_id = ?`, single(taskID, expID), "id"},
		{"tool_usage", `task_id = ? OR exploration_id = ?`, single(taskID, expID), "id"},
		{"intercept_pending", `COALESCE(task_id,'') = ?`, single(strconv.FormatInt(taskID, 10)), "id"},
		{"side_question_sessions", `task_id = ?`, single(taskID), "session_key"},
		{"side_question_requests", `session_key IN (SELECT session_key FROM side_question_sessions WHERE task_id = ?)`, single(taskID), "ordinal"},
	}
}

func (d *DB) snapshotTaskArchive(taskID int64, llmRecords io.Writer) (*TaskArchiveSnapshot, error) {
	tx, err := d.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.coordinateWithSchemaMigration(tx); err != nil {
		return nil, err
	}
	var expID int64
	if err := tx.QueryRow(`SELECT exploration_id FROM tasks WHERE id=? AND deleted_at IS NULL`, taskID).Scan(&expID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskArchiveNotFound
		}
		return nil, err
	}
	tables := make(map[string]json.RawMessage)
	counts := make(map[string]int64)
	streamedTables := map[string]string{}
	for _, export := range archiveExportPlan(taskID, expID) {
		args := export.args(taskID, expID)
		// llm_records 是唯一的流式大表：写出冷包 NDJSON，manifest 里只留空数组。
		var writer io.Writer
		if export.name == "llm_records" && llmRecords != nil {
			writer = llmRecords
		}
		raw, count, err := archiveSelectStream(tx, export.name, export.where, args, export.orderBy, writer)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", export.name, err)
		}
		if writer != nil {
			streamedTables[export.name] = TaskArchiveLLMRecordsPath
		}
		tables[export.name] = raw
		counts[export.name] = count
	}
	// 资产走候选集子查询（原 SELECT * + =ANY(task_ids) 的 TiDB 等价式）。
	assetRows, assetCount, err := archiveSelectProjection(tx, "assets",
		"id IN ("+archiveAssetCandidates()+")", archiveAssetCandidatesArgs(taskID, expID), "id")
	if err != nil {
		return nil, fmt.Errorf("snapshot assets: %w", err)
	}
	tables["assets"] = assetRows
	counts["assets"] = assetCount

	assetIDs, exclusiveAssetIDs, hosts, exclusiveHosts, err := archiveAssetMetadata(tx, taskID, expID, tables["assets"])
	if err != nil {
		return nil, err
	}
	_ = assetIDs // retained in the assets table payload; only exclusive ids need a side channel.
	var sources []int64
	rows, err := tx.Query(`SELECT source_task_id FROM task_relations WHERE task_id=? ORDER BY created_at,source_task_id`, taskID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		sources = append(sources, id)
	}
	// 同上：冷包里的 source_task_ids 必须来自完整迭代，中途错误一律上抛。
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	stats, err := taskArchiveAggregates(tx, taskID, expID)
	if err != nil {
		return nil, err
	}
	snapshot := &TaskArchiveSnapshot{
		FormatVersion: TaskArchiveFormatVersion, CreatedAt: time.Now().UTC(), TaskID: taskID,
		ExplorationID: expID, SourceTaskIDs: sources, Hosts: hosts, ExclusiveHosts: exclusiveHosts,
		ExclusiveAssetIDs: exclusiveAssetIDs, Tables: tables, StreamedTables: streamedTables,
		DataCounts: counts, AggregateStats: stats,
	}
	return snapshot, tx.Commit()
}

// archiveExclusiveAssetIDs 返回只被本任务独占、可以随冷包一起删除的资产 id。
// 原查询用 =ANY(asset.task_ids) 表达归属，新库换成 JSON_CONTAINS。
func archiveExclusiveAssetIDs(q rowsQuerier, taskID, expID int64) ([]int64, error) {
	query := `SELECT asset.id FROM assets asset JOIN (` + archiveAssetCandidates() + `) candidate ON candidate.id=asset.id
WHERE asset.company_id IS NULL
AND NOT EXISTS (SELECT 1 FROM tasks task WHERE task.id<>? AND task.deleted_at IS NULL AND
  JSON_CONTAINS(COALESCE(asset.task_ids, '[]'), CAST(task.id AS JSON)))
AND NOT EXISTS (
 SELECT 1 FROM exploration_anchors anchor JOIN exploration_nodes node ON node.id=anchor.node_id
 JOIN tasks task ON task.exploration_id=node.exploration_id
 WHERE anchor.asset_id=asset.id AND task.id<>? AND task.deleted_at IS NULL
) ORDER BY asset.id`
	args := append(archiveAssetCandidatesArgs(taskID, expID), taskID, taskID)
	rows, err := q.Query(query, args...)
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

func archiveAssetMetadata(tx *sql.Tx, taskID, expID int64, assetRows json.RawMessage) ([]int64, []int64, []string, []string, error) {
	var rows []map[string]any
	if err := json.Unmarshal(assetRows, &rows); err != nil {
		return nil, nil, nil, nil, err
	}
	allHosts := map[string]struct{}{}
	assetIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		if id, ok := jsonInt64(row["id"]); ok {
			assetIDs = append(assetIDs, id)
		}
		for _, key := range []string{"domain", "ip"} {
			if value, _ := row[key].(string); strings.TrimSpace(value) != "" {
				allHosts[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
			}
		}
		if rawURL, _ := row["url"].(string); rawURL != "" {
			if parsed, err := url.Parse(rawURL); err == nil && parsed.Hostname() != "" {
				allHosts[strings.ToLower(parsed.Hostname())] = struct{}{}
			}
		}
	}
	exclusiveHosts, err := hostsForTaskDeletion(tx, taskID, expID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	exclusiveIDs, err := archiveExclusiveAssetIDs(tx, taskID, expID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	hosts := make([]string, 0, len(allHosts))
	for host := range allHosts {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return assetIDs, exclusiveIDs, hosts, exclusiveHosts, nil
}

// taskArchiveAggregates 汇总冷包的展示型统计。原实现把聚合形状交给 PG 的
// jsonb_agg/jsonb_object_agg/to_jsonb/FILTER/AT TIME ZONE 拼装；TiDB 没有这些
// 聚合 JSON 构造函数，改成普通 GROUP BY 取数 + Go 侧按既有 JSON 形状组装
// （键名与 db/task_archive_aggregate_stats.go 的反序列化结构严格一致）。
func taskArchiveAggregates(tx *sql.Tx, taskID, expID int64) (map[string]any, error) {
	stats := map[string]any{}
	taskText := strconv.FormatInt(taskID, 10)
	var calls, input, output, cacheRead, cacheWrite int64
	if err := tx.QueryRow(`SELECT COUNT(*), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
COALESCE(SUM(cache_read),0), COALESCE(SUM(cache_write),0)
FROM llm_usage WHERE COALESCE(task_id,'')=? OR exploration_id=?`, taskText, expID).
		Scan(&calls, &input, &output, &cacheRead, &cacheWrite); err != nil {
		return nil, err
	}
	stats["tokens"] = map[string]int64{"calls": calls, "input_tokens": input, "output_tokens": output, "cache_read_tokens": cacheRead, "cache_write_tokens": cacheWrite}

	profiles := []ProfileUsage{}
	profileRows, err := tx.Query(`SELECT COALESCE(profile_name,''), COUNT(*), COUNT(DISTINCT COALESCE(task_id,'')),
COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), COALESCE(SUM(cache_read),0), COALESCE(SUM(cache_write),0)
FROM llm_usage WHERE COALESCE(task_id,'')=? OR exploration_id=?
GROUP BY COALESCE(profile_name,'')
ORDER BY COALESCE(SUM(input_tokens),0)+COALESCE(SUM(output_tokens),0) DESC`, taskText, expID)
	if err != nil {
		return nil, err
	}
	for profileRows.Next() {
		var item ProfileUsage
		if err := profileRows.Scan(&item.ProfileName, &item.Calls, &item.Tasks, &item.InputTokens,
			&item.OutputTokens, &item.CacheReadTokens, &item.CacheWriteTokens); err != nil {
			profileRows.Close()
			return nil, err
		}
		profiles = append(profiles, item)
	}
	if err := profileRows.Err(); err != nil {
		profileRows.Close()
		return nil, err
	}
	if err := profileRows.Close(); err != nil {
		return nil, err
	}
	stats["token_profiles"] = profiles

	daily := []ProfileDayUsage{}
	dailyRows, err := tx.Query(`SELECT COALESCE(profile_name,''), DATE_FORMAT(ts,'%Y-%m-%d'),
COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), COALESCE(SUM(cache_read),0)
FROM llm_usage WHERE COALESCE(task_id,'')=? OR exploration_id=?
GROUP BY COALESCE(profile_name,''), DATE_FORMAT(ts,'%Y-%m-%d')
ORDER BY DATE_FORMAT(ts,'%Y-%m-%d') ASC`, taskText, expID)
	if err != nil {
		return nil, err
	}
	for dailyRows.Next() {
		var item ProfileDayUsage
		if err := dailyRows.Scan(&item.ProfileName, &item.Date, &item.InputTokens, &item.OutputTokens, &item.CacheReadTokens); err != nil {
			dailyRows.Close()
			return nil, err
		}
		daily = append(daily, item)
	}
	if err := dailyRows.Err(); err != nil {
		dailyRows.Close()
		return nil, err
	}
	if err := dailyRows.Close(); err != nil {
		return nil, err
	}
	stats["token_daily"] = daily

	skills, skillStats, missingSkillStats, err := taskArchiveSkillAggregates(tx, taskID, expID)
	if err != nil {
		return nil, err
	}
	stats["skills"] = skills
	stats["skill_stats"] = skillStats
	stats["missing_skill_stats"] = missingSkillStats

	tools := map[string]int{}
	toolRows, err := tx.Query(`SELECT tool_key, COUNT(*) FROM tool_usage WHERE task_id=? OR exploration_id=? GROUP BY tool_key`, taskID, expID)
	if err != nil {
		return nil, err
	}
	for toolRows.Next() {
		var name string
		var count int
		if err := toolRows.Scan(&name, &count); err != nil {
			toolRows.Close()
			return nil, err
		}
		tools[name] = count
	}
	if err := toolRows.Err(); err != nil {
		toolRows.Close()
		return nil, err
	}
	if err := toolRows.Close(); err != nil {
		return nil, err
	}
	stats["tools"] = tools

	severity := map[string]int{}
	severityRows, err := tx.Query(`SELECT COALESCE(NULLIF(severity,''),'unknown'), COUNT(*) FROM findings WHERE task_id=? GROUP BY COALESCE(NULLIF(severity,''),'unknown')`, taskID)
	if err != nil {
		return nil, err
	}
	for severityRows.Next() {
		var name string
		var count int
		if err := severityRows.Scan(&name, &count); err != nil {
			severityRows.Close()
			return nil, err
		}
		severity[name] = count
	}
	if err := severityRows.Err(); err != nil {
		severityRows.Close()
		return nil, err
	}
	if err := severityRows.Close(); err != nil {
		return nil, err
	}
	stats["findings"] = severity

	findingStats := map[string]any{"vulnclasses": []string{}}
	var total, pending, critical, high, medium, low int
	if err := tx.QueryRow(`SELECT COUNT(*),
 COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN severity='high' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN severity='medium' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN severity='low' THEN 1 ELSE 0 END),0)
FROM findings WHERE task_id=?`, taskID).
		Scan(&total, &pending, &critical, &high, &medium, &low); err != nil {
		return nil, err
	}
	findingStats["total"], findingStats["pending"] = total, pending
	findingStats["critical"], findingStats["high"] = critical, high
	findingStats["medium"], findingStats["low"] = medium, low
	classRows, err := tx.Query(`SELECT DISTINCT vulnclass FROM findings WHERE task_id=? AND vulnclass<>'' ORDER BY vulnclass`, taskID)
	if err != nil {
		return nil, err
	}
	classes := []string{}
	for classRows.Next() {
		var item string
		if err := classRows.Scan(&item); err != nil {
			classRows.Close()
			return nil, err
		}
		classes = append(classes, item)
	}
	if err := classRows.Err(); err != nil {
		classRows.Close()
		return nil, err
	}
	if err := classRows.Close(); err != nil {
		return nil, err
	}
	findingStats["vulnclasses"] = classes
	stats["finding_stats"] = findingStats
	return stats, nil
}

// taskArchiveSkillAggregates 汇总技能命中/未命中。agents 维度原样保留为去重后的
// agent_key 列表（原 array_agg(DISTINCT ...) FILTER），TiDB 用 GROUP_CONCAT 取回。
func taskArchiveSkillAggregates(tx *sql.Tx, taskID, expID int64) (map[string]int, []SkillStat, []SkillStat, error) {
	counts := map[string]int{}
	resolved := []SkillStat{}
	missing := []SkillStat{}
	for _, item := range []struct {
		found  bool
		target *[]SkillStat
	}{
		{true, &resolved},
		{false, &missing},
	} {
		predicate := "NOT found"
		if item.found {
			predicate = "found"
		}
		rows, err := tx.Query(`SELECT skill, COUNT(*), COALESCE(GROUP_CONCAT(DISTINCT agent_key),''), MAX(ts)
FROM skill_usage WHERE (task_id=? OR exploration_id=?) AND `+predicate+` GROUP BY skill`, taskID, expID)
		if err != nil {
			return nil, nil, nil, err
		}
		for rows.Next() {
			var stat SkillStat
			var agents string
			var lastUsed sql.NullTime
			if err := rows.Scan(&stat.Skill, &stat.Calls, &agents, &lastUsed); err != nil {
				rows.Close()
				return nil, nil, nil, err
			}
			stat.Tasks = 0
			if item.found {
				stat.Tasks = 1
				counts[stat.Skill] = stat.Calls
			}
			if agents != "" {
				stat.Agents = strings.Split(agents, ",")
			}
			if lastUsed.Valid {
				copied := lastUsed.Time
				stat.LastUsed = &copied
			}
			*item.target = append(*item.target, stat)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, nil, nil, err
		}
	}
	return counts, resolved, missing, nil
}

func jsonInt64(value any) (int64, bool) {
	switch value := value.(type) {
	case float64:
		return int64(value), value == float64(int64(value))
	case json.Number:
		id, err := value.Int64()
		return id, err == nil
	case string:
		id, err := strconv.ParseInt(value, 10, 64)
		return id, err == nil
	default:
		return 0, false
	}
}
