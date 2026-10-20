package db

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// =====================================================================
// 归档恢复：真实 Catalog 写入白名单 + 全图冲突裁决（M25，闭环 F06）
// =====================================================================

// archiveImportSurrogates 声明"导入侧必须交给 TiDB 重新分配"的物理自增列。
// 隐藏主键 row_id 由 Catalog（PKColumn == "row_id"）自动识别，这里补上两类：
//   - side_question_requests.ordinal：逻辑身份是 id + session_key，ordinal 只是
//     顺序号，冷包里的旧值绝不能写回线上库；
//   - 叶子账目表的 id：归档之后线上自增早已越过这些号段，照抄回去只会在恢复时撞上
//     别人的新行。核心图实体（exploration_nodes、findings、assets）不在其列——它们
//     的 id 就是被引用方，必须原样保留。
var archiveImportSurrogates = map[string]string{
	"side_question_requests": "ordinal",
	// 叶子账目行没有任何入向引用，冷包里的旧 id 一律不复用：归档之后线上自增早已
	// 越过这些号段，照抄回去只会在恢复时撞上别人的行。核心图实体（exploration_nodes、
	// findings、assets）不在其列——它们的 id 就是被引用方，必须原样保留。
	"activity":         "id",
	"task_constraints": "id",
	"task_scope":       "id",
	"llm_records":      "id",
	"llm_usage":        "id",
	"skill_usage":      "id",
	"tool_usage":       "id",
}

// archiveImportReusePKWhenFree 是"旧物理键仍空闲就复用、被占用才重新分配"的表集合。
// intercept_pending.id 会被审计查询与前端按号引用，因此优先保住原号；一旦撞上别人
// 的新行就摘掉旧号交给 TiDB，绝不用 nextval 造号。
var archiveImportReusePKWhenFree = map[string]bool{"intercept_pending": true}

// archiveImportAllowList 是允许被恢复流程写回的表集合；不在集合内的表名一律拒绝，
// 防止 manifest 被篡改后把任意表灌回线上库。
var archiveImportAllowList = map[string]bool{
	"assets":            true,
	"exploration_nodes": true, "exploration_edges": true, "exploration_anchors": true,
	"task_constraints": true, "activity": true, "task_asset_links": true, "findings": true,
	"llm_records": true, "llm_usage": true, "skill_usage": true, "tool_usage": true,
	"side_question_sessions": true, "side_question_requests": true,
	"task_scope": true, "task_llm_profiles": true, "intercept_pending": true,
}

// archiveImportColumns 返回一张表的写入列白名单：真实 Catalog 列，去掉隐藏主键
// row_id、导入侧代理键、STORED 生成列与 canonical identity 派生列。
func archiveImportColumns(table string) ([]string, error) {
	if !archiveImportAllowList[table] {
		return nil, fmt.Errorf("archive restore table %q is not allowed", table)
	}
	item, ok := archiveCatalogTable(table)
	if !ok {
		return nil, fmt.Errorf("archive restore: table %q is absent from the real catalog", table)
	}
	hidden := archiveHiddenPhysicalKey(table)
	surrogate := archiveImportSurrogates[table]
	columns := make([]string, 0, len(item.Columns))
	for _, column := range item.Columns {
		switch {
		case column.Generated != "":
			continue
		case hidden != "" && column.Name == hidden:
			continue
		case surrogate != "" && column.Name == surrogate:
			continue
		case archiveIsDerivedColumn(table, column.Name):
			continue
		}
		columns = append(columns, column.Name)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("archive restore: table %q has no writable columns", table)
	}
	return columns, nil
}

// archiveDecodeValue 把 manifest 里的一个 JSON 值还原成该 Catalog 列的绑定参数。
// 冷包既可能来自新 TiDB 导出（JSON 列是嵌套对象、时间是 RFC3339），也可能来自
// 历史 PG 冷包（bytea 的 \x 十六进制、jsonb 的嵌套对象），两侧都要能吃下。
func archiveDecodeValue(column schema.Column, raw json.RawMessage) (any, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	switch column.Type {
	case schema.TJSON:
		if strings.HasPrefix(trimmed, `"`) {
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return nil, err
			}
			return text, nil
		}
		// 嵌套对象/数组：TiDB 的 JSON 列其实是 MEDIUMTEXT，直接落 JSON 文本。
		return trimmed, nil
	case schema.TBool:
		var boolean bool
		if err := json.Unmarshal(raw, &boolean); err == nil {
			if boolean {
				return 1, nil
			}
			return 0, nil
		}
		var number json.Number
		if err := json.Unmarshal(raw, &number); err == nil {
			parsed, err := strconv.ParseFloat(number.String(), 64)
			if err != nil {
				return nil, err
			}
			if parsed != 0 {
				return 1, nil
			}
			return 0, nil
		}
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			if archiveBoolText(text) {
				return 1, nil
			}
			return 0, nil
		}
		return nil, fmt.Errorf("cannot decode boolean from %s", trimmed)
	case schema.TDate3:
		at, ok := archiveJSONTimestamp(raw)
		if ok {
			return at.UTC(), nil
		}
		return nil, fmt.Errorf("cannot decode datetime from %s", trimmed)
	case schema.TBinary, schema.TVarbinary, schema.TMediumBlob:
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, err
		}
		if text == "" {
			return []byte(nil), nil
		}
		// 历史 PG 冷包的 bytea 是 "\x<hex>"；新导出走 encoding/json 的 base64。
		if strings.HasPrefix(text, `\x`) || strings.HasPrefix(text, `\X`) {
			decoded, err := hex.DecodeString(text[2:])
			if err != nil {
				return nil, err
			}
			return decoded, nil
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	case schema.TText, schema.TVarchar, schema.TChar:
		if strings.HasPrefix(trimmed, `"`) {
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return nil, err
			}
			return text, nil
		}
		// 数字/嵌套结构落进文本列：原样保留 JSON 文本（task_ids 一类数组列就是这个形状）。
		return trimmed, nil
	default:
		if strings.HasPrefix(trimmed, `"`) {
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return nil, err
			}
			if text == "" {
				return nil, nil
			}
			return text, nil
		}
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return parsed, nil
		}
		var number json.Number
		if err := json.Unmarshal(raw, &number); err == nil {
			if parsed, err := number.Int64(); err == nil {
				return parsed, nil
			}
			parsed, err := number.Float64()
			if err != nil {
				return nil, err
			}
			return parsed, nil
		}
		return trimmed, nil
	}
}

// archiveRowProjection 把一行 manifest 折成 (列名, 绑定值)，顺序即写入白名单顺序。
// manifest 里没有的列一律缺席，交给列默认值（v3 老冷包的 memory/context_info 兜底
// 就依赖这一点，不再需要 json_populate_recordset 时代的空对象补齐）。
func archiveRowProjection(table string, row map[string]json.RawMessage) ([]string, []any, error) {
	columns, err := archiveImportColumns(table)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(columns))
	values := make([]any, 0, len(columns))
	for _, name := range columns {
		raw, ok := row[name]
		if !ok {
			continue
		}
		column, err := archiveColumnOf(table, name)
		if err != nil {
			return nil, nil, err
		}
		value, err := archiveDecodeValue(column, raw)
		if err != nil {
			return nil, nil, fmt.Errorf("archive restore %s.%s: %w", table, name, err)
		}
		names = append(names, name)
		values = append(values, value)
	}
	return names, values, nil
}

// archiveIdentityWrite 是一次 hash 表写入：canonical identity 载荷、摘要与落库列对。
type archiveIdentityWrite struct {
	pkCol    string
	identCol string
	hashCol  string
	identity []byte
	hash     [32]byte
}

// columns 返回追加到写入白名单之后的两列（identity 载荷 + BINARY(32) 摘要）。
func (w archiveIdentityWrite) columns() ([]string, []any) {
	return []string{w.identCol, w.hashCol}, []any{w.identity, w.hash[:]}
}

func (w archiveIdentityWrite) validate(table string) error {
	if w.identCol == "" || w.hashCol == "" || len(w.identity) == 0 {
		return fmt.Errorf("archive restore %s: missing canonical identity", table)
	}
	return nil
}

// archiveInsertRowTx 按写入白名单落回一行。
//   - identity 非空：追加重算后的 canonical identity/hash，并在**同一事务内**用
//     schema.VerifyHashArbitration 做写后自洽仲裁（命中的物理 PK 必须是本行）；
//   - explicitPK 非空：按调用方指定的物理 PK 插入（核心图实体必须保持原 PK）；
//     为空则交给 TiDB 自增重新分配（隐藏 row_id 与 ordinal 永远走这条分支）。
func archiveInsertRowTx(
	ctx context.Context,
	tx *sql.Tx,
	table string,
	row map[string]json.RawMessage,
	identity *archiveIdentityWrite,
	explicitPK any,
) (int64, error) {
	columns, values, err := archiveRowProjection(table, row)
	if err != nil {
		return 0, err
	}
	if identity != nil {
		if err := identity.validate(table); err != nil {
			return 0, err
		}
		extraColumns, extraValues := identity.columns()
		columns = append(columns, extraColumns...)
		values = append(values, extraValues...)
	}
	if explicitPK != nil {
		pkColumn, err := archivePrimaryKeyColumn(table)
		if err != nil {
			return 0, err
		}
		columns = append([]string{pkColumn}, columns...)
		values = append([]any{explicitPK}, values...)
	}
	query := "INSERT INTO `" + table + "` (" + archiveQuoteColumns(columns) + ") VALUES (" +
		questionPlaceholders(len(values)) + ")"
	res, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, fmt.Errorf("archive restore %s: %w", table, err)
	}
	declared, _ := jsonInt64Value(explicitPK)
	pk := declared
	if pk <= 0 {
		// 没有显式物理键（隐藏 row_id、ordinal 代理键、被占用的旧 id）时才向 TiDB 取号。
		if pk, err = res.LastInsertId(); err != nil {
			return 0, fmt.Errorf("archive restore %s last insert id: %w", table, err)
		}
	}
	if identity != nil {
		// 本行的物理写锁由刚完成的 INSERT 持有，满足 VerifyHashArbitration 的锁序前置；
		// 命中到别的物理 PK 说明 hash 撞车或身份漂移，必须 fail-closed。
		if err := archiveVerifyIdentityArbitration(ctx, tx, table, *identity, pk); err != nil {
			return 0, err
		}
	}
	return pk, nil
}

// archiveVerifyIdentityArbitration 在同一事务内回读 identity 并做精确字节比对。
func archiveVerifyIdentityArbitration(ctx context.Context, tx *sql.Tx, table string, identity archiveIdentityWrite, pk int64) error {
	found, err := schema.VerifyHashArbitration(ctx, tx, table, identity.pkCol, identity.identCol, identity.hashCol, identity.hash, identity.identity)
	if err != nil {
		return fmt.Errorf("archive restore %s hash arbitration: %w", table, err)
	}
	if pk > 0 && int64(found) != pk {
		return fmt.Errorf("archive restore %s hash arbitration collided with row %d", table, found)
	}
	return nil
}

// archivePrimaryKeyColumn 返回该表在导入时可以显式指定的物理主键列（隐藏 row_id
// 与代理键绝不允许显式指定）。
func archivePrimaryKeyColumn(table string) (string, error) {
	item, ok := archiveCatalogTable(table)
	if !ok {
		return "", fmt.Errorf("archive restore: table %q is absent from the real catalog", table)
	}
	if archiveHiddenPhysicalKey(table) != "" || archiveImportSurrogates[table] != "" {
		return "", fmt.Errorf("archive restore %s: surrogate primary key must be reassigned by TiDB", table)
	}
	if item.PKColumn == "" {
		return "", fmt.Errorf("archive restore: table %q has no primary key", table)
	}
	return item.PKColumn, nil
}

func jsonInt64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, typed > 0
	case int:
		return int64(typed), typed > 0
	default:
		return 0, false
	}
}

// archiveDecodeRawRows 把 manifest 里的一张表解成逐行 JSON（保留数字原文，避免
// float64 把 BIGINT 的大 id 打回科学计数法）。
func archiveDecodeRawRows(raw json.RawMessage) ([]map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// insertArchiveRows 按写入白名单逐行落回一张归档表（老入口，签名保留给既有测试）。
func insertArchiveRows(tx *sql.Tx, table string, raw json.RawMessage) error {
	return insertArchiveTableRows(context.Background(), tx, table, raw)
}

// insertArchiveTableRows 落回一张表的全部行。重复键一律 fail-closed：核心图实体
// （节点/边/锚点/漏洞）与纯关联行（task_asset_links）冲突都必须上抛，由调用方
// 事务整体回滚，绝不"跳过一行继续"。
func insertArchiveTableRows(ctx context.Context, tx *sql.Tx, table string, raw json.RawMessage) error {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return err
	}
	if _, err := archiveImportColumns(table); err != nil {
		return err
	}
	for _, row := range rows {
		identity, err := archiveRowIdentity(table, row)
		if err != nil {
			return err
		}
		explicitPK, err := archivePrepareLeafPrimaryKey(ctx, tx, table, row)
		if err != nil {
			return err
		}
		if _, err := archiveInsertRowTx(ctx, tx, table, row, identity, explicitPK); err != nil {
			return err
		}
	}
	return nil
}

// archivePrepareLeafPrimaryKey 处理叶子行的旧物理键：仍空闲就复用（保住历史按号引用），
// 已被占用就把它从行里摘掉，交给 TiDB 自增重新分配。
func archivePrepareLeafPrimaryKey(ctx context.Context, tx *sql.Tx, table string, row map[string]json.RawMessage) (any, error) {
	if !archiveImportReusePKWhenFree[table] {
		return nil, nil
	}
	column, err := archivePrimaryKeyColumn(table)
	if err != nil {
		return nil, err
	}
	raw, present := row[column]
	if !present {
		return nil, nil
	}
	delete(row, column)
	id, ok := jsonInt64(rawAny(raw))
	if !ok || id <= 0 {
		return nil, nil
	}
	var taken int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"` WHERE `"+column+"`=?", id).Scan(&taken); err != nil {
		return nil, fmt.Errorf("archive restore %s primary key probe: %w", table, err)
	}
	if taken > 0 {
		return nil, nil
	}
	return id, nil
}

// archiveRowIdentity 按表重算 canonical identity/hash。冷包里即便带着旧的
// identity 派生列也一律不采信（导出白名单里已经没有它们），缺失语义列直接 fail-closed。
func archiveRowIdentity(table string, row map[string]json.RawMessage) (*archiveIdentityWrite, error) {
	switch table {
	case "task_scope":
		identity, err := archiveTaskScopeIdentity(row)
		if err != nil {
			return nil, err
		}
		return &identity, nil
	case "side_question_requests":
		identity, err := archiveSideRequestIdentity(row)
		if err != nil {
			return nil, err
		}
		return &identity, nil
	case "assets":
		// 资产有 8 个身份分支，必须由 restoreArchiveAssets 逐分支激活后写入。
		return nil, errors.New("archive restore assets: assets must be restored through the asset branch engine")
	}
	return nil, nil
}

// =====================================================================
// 冷包落库前的热库压缩
// =====================================================================

// CompleteTaskArchive performs the hot-store compaction only after the external
// package has been fully written and checksummed. The task/exploration rows remain
// as minimal ID stubs; all heavyweight task-owned rows move into the package.
func (d *DB) CompleteTaskArchive(
	archiveID int64,
	snapshot *TaskArchiveSnapshot,
	archivePath, sha256 string,
	originalSize, compressedSize int64,
) error {
	if snapshot == nil || snapshot.FormatVersion != TaskArchiveFormatVersion {
		return ErrTaskArchiveFormatMismatch
	}
	countsRaw, err := json.Marshal(snapshot.DataCounts)
	if err != nil {
		return err
	}
	statsRaw, err := json.Marshal(snapshot.AggregateStats)
	if err != nil {
		return err
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.coordinateWithSchemaMigration(tx); err != nil {
		return err
	}
	var taskID, expID int64
	var state string
	if err := tx.QueryRow(`SELECT archive.task_id,task.exploration_id,archive.state
FROM task_archives archive JOIN tasks task ON task.id=archive.task_id
WHERE archive.id=? FOR UPDATE`, archiveID).Scan(&taskID, &expID, &state); err != nil {
		return err
	}
	if state != Archiving || taskID != snapshot.TaskID || expID != snapshot.ExplorationID {
		return fmt.Errorf("%w: archive snapshot identity/state mismatch", ErrTaskArchiveState)
	}
	var dependent int64
	err = tx.QueryRow(`SELECT child.id FROM task_relations relation
JOIN tasks child ON child.id=relation.task_id AND child.deleted_at IS NULL
WHERE relation.source_task_id=? LIMIT 1 FOR UPDATE`, taskID).Scan(&dependent)
	if err == nil {
		return fmt.Errorf("%w: task %d", ErrTaskArchiveDependent, dependent)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// 防止独占性判定期间资产归属被改写：原实现是一条 PG 表级
	// `LOCK TABLE assets, exploration_anchors IN SHARE ROW EXCLUSIVE MODE`，
	// 会把整库归档之外的资产写入全部堵死。TiDB 上换成"按物理 PK 升序对本任务
	// 真正涉及的资产行 FOR UPDATE"，锁粒度降到行级且加锁顺序全局一致。
	if err := lockArchiveAssets(tx, taskID, expID, snapshot); err != nil {
		return err
	}
	// 逆拓扑闭包（I01）：新 Catalog 明确没有 FK/CHECK，旧库"删父行靠级联清子行"
	// 在这里根本不会发生。压缩必须自己按 证据绑定 → 探索锚点 → 探索边 的顺序删到底，
	// 否则这些关系会以本次归档自己的残留留在热库，恢复预检把它们误判成冲突
	// （归档成功、恢复必失败）。锚点/绑定都靠 nodes/findings 定位，因此共享的
	// 证据快照 id 必须在删除父行之前取走。
	evidenceIDs, err := archivedEvidenceSnapshotIDs(tx, taskID)
	if err != nil {
		return err
	}
	if err := deleteArchiveGraphClosure(tx, taskID, expID); err != nil {
		return err
	}
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`DELETE FROM intercept_pending WHERE COALESCE(task_id,'')=?`, []any{strconv.FormatInt(taskID, 10)}},
		{`DELETE FROM llm_records WHERE COALESCE(task_id,'')=?`, []any{strconv.FormatInt(taskID, 10)}},
		{`DELETE FROM llm_usage WHERE COALESCE(task_id,'')=? OR exploration_id=?`, []any{strconv.FormatInt(taskID, 10), expID}},
		{`DELETE FROM skill_usage WHERE task_id=? OR exploration_id=?`, []any{taskID, expID}},
		{`DELETE FROM tool_usage WHERE task_id=? OR exploration_id=?`, []any{taskID, expID}},
		{`DELETE FROM side_question_requests WHERE session_key IN (SELECT session_key FROM side_question_sessions WHERE task_id=?)`, []any{taskID}},
		{`DELETE FROM side_question_sessions WHERE task_id=?`, []any{taskID}},
		{`DELETE FROM findings WHERE task_id=?`, []any{taskID}},
		{`DELETE FROM task_relations WHERE task_id=? OR source_task_id=?`, []any{taskID, taskID}},
		{`DELETE FROM task_asset_links WHERE task_id=?`, []any{taskID}},
		{`DELETE FROM task_llm_profiles WHERE task_id=?`, []any{taskID}},
		{`DELETE FROM task_scope WHERE task_id=?`, []any{taskID}},
		{`DELETE FROM task_constraints WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM activity WHERE exploration_id=?`, []any{expID}},
		{`DELETE FROM exploration_nodes WHERE exploration_id=?`, []any{expID}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			return err
		}
	}
	if err := stripTaskFromAssetOwnership(tx, taskID); err != nil {
		return err
	}
	if err := deleteExclusiveArchiveAssets(tx, taskID, snapshot.ExclusiveAssetIDs); err != nil {
		return err
	}
	// 证据快照是跨任务共享的物理行：压缩只解绑、不删除。解绑后已经没有任何绑定的
	// 快照补上"孤儿"标记，与恢复侧 restoreFindingTrafficTx 的
	// `SET s.unreferenced_at = NULL` 严格对称（共享给别的 finding 的快照不受影响）。
	if err := markEvidenceSnapshotsUnreferenced(tx, evidenceIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE explorations SET description='',goal='',status='open' WHERE id=?`, expID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE tasks SET
 name='',category_id=NULL,description='',goal='',paused=true,queued=false,queued_at=NULL,queue_mode='',
 llm_profile_id=NULL,active_llm_profile_id=NULL,llm_chain_revision=llm_chain_revision+1,
 company_id=NULL,parent_ref=NULL,timeout_seconds=0,coverage_enabled=true,pinned_at=NULL,
 first_run_at=NULL,deadline_at=NULL,archived_at=NOW(3),deleted_at=NOW(3)
WHERE id=?`, taskID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE task_archives SET
 state=?,phase='ready',progress=100,error='',archive_path=?,sha256=?,
 original_size=?,compressed_size=?,data_counts=?,aggregate_stats=?,
 format_version=?,archived_at=NOW(3),warnings='[]'
WHERE id=?`, ArchiveReady, archivePath, sha256, originalSize, compressedSize,
		string(countsRaw), string(statsRaw), snapshot.FormatVersion, archiveID); err != nil {
		return err
	}
	return tx.Commit()
}

// lockArchiveAssets 给本任务涉及的资产行加悲观行锁，替代原 PG 的
// 全表 SHARE ROW EXCLUSIVE 锁。
//
// 锁协议（I07）：候选资产来自三个彼此独立的来源（task_ids 归属 / 节点锚定 /
// 冷包独占），"每组各自 ORDER BY id 各锁一遍"**不等于**全局升序——任务自有资产
// 20 与仅被节点锚定的资产 10 是合法数据（锚定不要求资产归属本任务），按
// 20→10 取锁就会与另一个 10→20 的写者构成反向锁序。因此这里先把**全部**候选
// 发现齐、去重、按物理 PK 全局升序，再统一按有界批次 FOR UPDATE。
//
// 发现阶段是普通 current read，只负责确定候选集合；真正的悲观锁只在最后一阶段
// 按物理 PK 获取，与全仓"先按物理 PK 全局全序加锁"的统一协议一致。
func lockArchiveAssets(tx *sql.Tx, taskID, expID int64, snapshot *TaskArchiveSnapshot) error {
	candidates := map[int64]bool{}
	if err := collectArchiveAssetIDs(tx, candidates, `SELECT id FROM assets WHERE `+
		assetJSONContains("COALESCE(task_ids, '[]')", "?")+` ORDER BY id`, taskID); err != nil {
		return err
	}
	if err := collectArchiveAssetIDs(tx, candidates, `SELECT anchor.asset_id FROM exploration_anchors anchor
JOIN exploration_nodes node ON node.id = anchor.node_id
WHERE node.exploration_id = ? GROUP BY anchor.asset_id`, expID); err != nil {
		return err
	}
	if snapshot != nil {
		for _, id := range snapshot.ExclusiveAssetIDs {
			if id > 0 {
				candidates[id] = true
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	// 单一来源、全局升序、有界批次：既消除分组逆序，也让 IN (...) 的占位符数量有界。
	for _, batch := range archiveIDBatches(ids) {
		args := make([]any, 0, len(batch))
		for _, id := range batch {
			args = append(args, id)
		}
		if err := drainLockedRows(tx, `SELECT id FROM assets WHERE id IN (`+
			questionPlaceholders(len(batch))+`) ORDER BY id FOR UPDATE`, args...); err != nil {
			return err
		}
	}
	return nil
}

// collectArchiveAssetIDs 执行一条只读的资产 id 发现查询，把结果并入候选集合。
// 这里刻意不加 FOR UPDATE：加锁必须等到完整候选集合确定之后一次性按全局升序进行。
func collectArchiveAssetIDs(tx *sql.Tx, candidates map[int64]bool, query string, args ...any) error {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if id > 0 {
			candidates[id] = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}

// archivedEvidenceSnapshotIDs 取出本任务 findings 当前引用的证据快照 id（去重升序）。
// 必须在删除 finding_traffic_bindings / findings **之前**调用：逆拓扑一旦失去父行集合，
// 这些跨任务共享的快照就再也定位不到了。
func archivedEvidenceSnapshotIDs(tx *sql.Tx, taskID int64) ([]string, error) {
	rows, err := tx.Query(`SELECT DISTINCT b.snapshot_id
FROM finding_traffic_bindings b JOIN findings f ON f.id = b.finding_id
WHERE f.task_id=? ORDER BY b.snapshot_id`, taskID)
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return out, rows.Close()
}

// deleteArchiveGraphClosure 按逆拓扑显式删除热库里的图叶子关系与证据绑定。
// 顺序固定为 证据绑定 → 探索锚点 → 探索边：前两者都要 JOIN 父行定位，必须早于
// findings / exploration_nodes 的删除执行。
func deleteArchiveGraphClosure(tx *sql.Tx, taskID, expID int64) error {
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`DELETE binding FROM finding_traffic_bindings binding
JOIN findings finding ON finding.id = binding.finding_id WHERE finding.task_id=?`, []any{taskID}},
		{`DELETE anchor FROM exploration_anchors anchor
JOIN exploration_nodes node ON node.id = anchor.node_id WHERE node.exploration_id=?`, []any{expID}},
		{`DELETE FROM exploration_edges WHERE exploration_id=?`, []any{expID}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			return err
		}
	}
	return nil
}

// markEvidenceSnapshotsUnreferenced 给解绑后不再被任何绑定引用的快照补上孤儿标记。
// 快照行本身不删（可能仍被其它任务的 finding 共享），且已有标记的行不重复覆盖。
func markEvidenceSnapshotsUnreferenced(tx *sql.Tx, snapshotIDs []string) error {
	const batchSize = 200
	for start := 0; start < len(snapshotIDs); start += batchSize {
		end := start + batchSize
		if end > len(snapshotIDs) {
			end = len(snapshotIDs)
		}
		batch := snapshotIDs[start:end]
		args := make([]any, 0, len(batch))
		for _, id := range batch {
			args = append(args, id)
		}
		if _, err := tx.Exec(`UPDATE traffic_evidence_snapshots SET unreferenced_at=NOW(3)
WHERE unreferenced_at IS NULL AND id IN (`+questionPlaceholders(len(batch))+`)
AND NOT EXISTS (SELECT 1 FROM finding_traffic_bindings binding
  WHERE binding.snapshot_id = traffic_evidence_snapshots.id)`, args...); err != nil {
			return err
		}
	}
	return nil
}

// drainLockedRows 执行一条 FOR UPDATE 查询并把结果读干净——行锁只有在结果集被完整
// 消费后才真正落到这些行上。
func drainLockedRows(tx *sql.Tx, query string, args ...any) error {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return err
	}
	var ignored int64
	for rows.Next() {
		if err := rows.Scan(&ignored); err != nil {
			rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}

// stripTaskFromAssetOwnership 把任务 id 从 assets.task_ids（JSON 数组文本）里摘掉。
// 原 array_remove(task_ids,$1) 是标量函数，新库的 JSON 数组只能在 Go 侧改写。
func stripTaskFromAssetOwnership(tx *sql.Tx, taskID int64) error {
	rows, err := tx.Query(`SELECT id, COALESCE(task_ids,'[]') FROM assets WHERE `+
		assetJSONContains("COALESCE(task_ids, '[]')", "?")+` ORDER BY id FOR UPDATE`, taskID)
	if err != nil {
		return err
	}
	type ownership struct {
		id      int64
		taskIDs string
	}
	var pending []ownership
	for rows.Next() {
		var item ownership
		if err := rows.Scan(&item.id, &item.taskIDs); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range pending {
		var ids []int64
		if err := json.Unmarshal([]byte(item.taskIDs), &ids); err != nil {
			ids = nil
		}
		kept := make([]int64, 0, len(ids))
		for _, id := range ids {
			if id != taskID {
				kept = append(kept, id)
			}
		}
		encoded, err := json.Marshal(kept)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE assets SET task_ids=? WHERE id=?`, string(encoded), item.id); err != nil {
			return err
		}
	}
	return nil
}

// deleteExclusiveArchiveAssets 删除只属于本任务的资产。归属判定在 DELETE 里重算
// 一遍（与冷包导出时的独占判据一致），避免导出与压缩之间的窗口里被别的任务认领后
// 仍然被误删。
func deleteExclusiveArchiveAssets(tx *sql.Tx, taskID int64, exclusiveIDs []int64) error {
	for _, batch := range archiveIDBatches(exclusiveIDs) {
		// 绑定顺序必须与 SQL 里占位符出现的顺序一致：先 IN 列表，再两个 task.id<>?。
		args := make([]any, 0, len(batch)+2)
		for _, id := range batch {
			args = append(args, id)
		}
		args = append(args, taskID, taskID)
		query := `DELETE FROM assets WHERE id IN (` + questionPlaceholders(len(batch)) + `) AND company_id IS NULL
AND NOT EXISTS (SELECT 1 FROM tasks task WHERE task.id<>? AND task.deleted_at IS NULL AND
  JSON_CONTAINS(COALESCE(assets.task_ids, '[]'), CAST(task.id AS JSON)))
AND NOT EXISTS (
 SELECT 1 FROM exploration_anchors anchor JOIN exploration_nodes node ON node.id=anchor.node_id
 JOIN tasks task ON task.exploration_id=node.exploration_id
 WHERE anchor.asset_id=assets.id AND task.id<>? AND task.deleted_at IS NULL
)`
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}
	}
	return nil
}

// archiveIDBatches 把 id 列表切成有界批次，避免 IN (...) 的占位符无限膨胀。
func archiveIDBatches(ids []int64) [][]int64 {
	const size = 200
	batches := [][]int64{}
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		batches = append(batches, ids[start:end])
	}
	return batches
}

// RestoreTaskArchive restores TiDB rows from a verified manifest. It returns
// non-fatal warnings for global objects that intentionally are not recreated.
//
// 全图冲突裁决（M25）：
//   - 核心图实体（task / exploration / exploration_nodes / exploration_edges /
//     exploration_anchors / findings）冲突时**报错拒绝**，整个事务回滚；
//   - 纯关联表与叶子行不重取冷包里的物理 row_id，由 TiDB 重新分配；
//   - 带 hash 的实体（assets / task_scope / side_question_requests）在事务内重算
//     canonical identity 并执行 VerifyHashArbitration 验证。
func (d *DB) RestoreTaskArchive(archiveID int64, snapshot *TaskArchiveSnapshot, remainingTimeoutSeconds int64) ([]string, error) {
	return d.restoreTaskArchive(archiveID, snapshot, remainingTimeoutSeconds, nil)
}

// RestoreTaskArchiveWithLLMRecords restores a v2 package whose heavyweight LLM
// record history is stored as a sequence of JSON objects outside manifest.json.
func (d *DB) RestoreTaskArchiveWithLLMRecords(
	archiveID int64,
	snapshot *TaskArchiveSnapshot,
	remainingTimeoutSeconds int64,
	llmRecords io.Reader,
) ([]string, error) {
	if llmRecords == nil {
		return nil, errors.New("nil streamed LLM record reader")
	}
	return d.restoreTaskArchive(archiveID, snapshot, remainingTimeoutSeconds, llmRecords)
}

func (d *DB) restoreTaskArchive(
	archiveID int64,
	snapshot *TaskArchiveSnapshot,
	remainingTimeoutSeconds int64,
	llmRecords io.Reader,
) ([]string, error) {
	if snapshot == nil || !IsTaskArchiveFormatSupported(snapshot.FormatVersion) {
		return nil, ErrTaskArchiveFormatMismatch
	}
	streamedLLMRecords := snapshot.StreamedTables["llm_records"]
	if (len(snapshot.StreamedTables) > 0 && snapshot.FormatVersion < 2) ||
		len(snapshot.StreamedTables) > 1 ||
		(len(snapshot.StreamedTables) == 1 && streamedLLMRecords != TaskArchiveLLMRecordsPath) {
		return nil, fmt.Errorf("%w: unsupported streamed table metadata", ErrTaskArchiveFormatMismatch)
	}
	if streamedLLMRecords != "" && llmRecords == nil {
		return nil, fmt.Errorf("%w: streamed LLM records are missing", ErrTaskArchiveFormatMismatch)
	}
	ctx := context.Background()
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.coordinateWithSchemaMigration(tx); err != nil {
		return nil, err
	}
	var taskID, expID int64
	var state string
	if err := tx.QueryRow(`SELECT archive.task_id,task.exploration_id,archive.state
FROM task_archives archive JOIN tasks task ON task.id=archive.task_id
WHERE archive.id=? FOR UPDATE`, archiveID).Scan(&taskID, &expID, &state); err != nil {
		return nil, err
	}
	if state != Restoring || taskID != snapshot.TaskID || expID != snapshot.ExplorationID {
		return nil, fmt.Errorf("%w: restore snapshot identity/state mismatch", ErrTaskArchiveState)
	}
	// 合法 stub 例外（I05）：归档恢复只允许覆盖"当前 archive 自己的压缩残留"。
	// 冷包子 ID 恰好不冲突，并不意味着目标根实体可以安全覆写——活跃 task/exploration
	// 必须在这里 fail-closed，否则底层 API 就把整个恢复退化成"覆盖任意根实体"。
	if err := assertRestorableArchiveStub(ctx, tx, taskID, expID, snapshot); err != nil {
		return nil, err
	}
	warnings := []string{}

	// 全图冲突裁决：核心图实体一旦被别的行占用，立即 fail-closed，整个事务回滚，
	// 绝不出现"恢复了半张图"的中间态。
	if err := assertCoreGraphConflictFree(ctx, tx, expID, snapshot); err != nil {
		return nil, err
	}

	assetMap, assetWarnings, err := restoreArchiveAssets(ctx, tx, taskID, snapshot.Tables["assets"])
	if err != nil {
		return nil, err
	}
	warnings = append(warnings, assetWarnings...)

	// The task row exists as an archived stub. Restore its global references only
	// when the current instance still owns them; never recreate categories/profiles.
	taskRows, err := decodeArchiveRows(snapshot.Tables["tasks"])
	if err != nil || len(taskRows) != 1 {
		return nil, fmt.Errorf("restore task row: expected one row: %w", err)
	}
	taskRow := taskRows[0]
	categoryID, _ := jsonInt64(taskRow["category_id"])
	if categoryID > 0 && !rowExists(tx, "task_categories", categoryID) {
		taskRow["category_id"] = nil
		warnings = append(warnings, fmt.Sprintf("任务分类 %d 已删除，已恢复为未分类", categoryID))
	}
	companyID, _ := jsonInt64(taskRow["company_id"])
	if companyID > 0 && !rowExists(tx, "companies", companyID) {
		taskRow["company_id"] = nil
		warnings = append(warnings, fmt.Sprintf("任务企业 %d 已删除，企业关联已跳过", companyID))
	}
	for _, key := range []string{"llm_profile_id", "active_llm_profile_id"} {
		profileID, _ := jsonInt64(taskRow[key])
		if profileID > 0 && !rowExists(tx, "llm_profiles", profileID) {
			taskRow[key] = nil
			warnings = append(warnings, fmt.Sprintf("LLM 配置 %d 已删除，已从任务配置中移除", profileID))
		}
	}
	if err := restoreExplorationStub(tx, snapshot.Tables["explorations"], expID); err != nil {
		return nil, err
	}
	if err := restoreTaskStub(tx, taskRow, taskID, remainingTimeoutSeconds); err != nil {
		return nil, err
	}

	remappedTables, err := remapArchiveAssetReferences(snapshot.Tables, assetMap)
	if err != nil {
		return nil, err
	}
	remappedTables["findings"], err = normalizeArchivedFindingVersions(remappedTables["findings"])
	if err != nil {
		return nil, err
	}
	// Insert graph rows in foreign-key order. The archived stub has no graph rows,
	// so an ID conflict signals external corruption and must stop the restore.
	for _, table := range []string{"exploration_nodes", "exploration_edges", "exploration_anchors", "task_constraints", "activity"} {
		if err := insertArchiveTableRows(ctx, tx, table, remappedTables[table]); err != nil {
			return nil, fmt.Errorf("restore %s: %w", table, err)
		}
	}
	if warning, err := restoreTaskRelations(ctx, tx, taskID, remappedTables["task_relations"]); err != nil {
		return nil, err
	} else {
		warnings = append(warnings, warning...)
	}
	if warning, err := restoreSideQuestionSessions(ctx, tx, remappedTables["side_question_sessions"]); err != nil {
		return nil, err
	} else {
		warnings = append(warnings, warning...)
	}
	if err := restoreSideQuestionRequests(ctx, tx, remappedTables["side_question_requests"]); err != nil {
		return nil, fmt.Errorf("restore side_question_requests: %w", err)
	}
	if warning, err := restoreTaskScopes(ctx, tx, remappedTables["task_scope"]); err != nil {
		return nil, err
	} else {
		warnings = append(warnings, warning...)
	}
	if warning, err := restoreTaskLLMProfiles(ctx, tx, taskID, remappedTables["task_llm_profiles"]); err != nil {
		return nil, err
	} else {
		warnings = append(warnings, warning...)
	}
	for _, table := range []string{"task_asset_links", "findings"} {
		if err := insertArchiveTableRows(ctx, tx, table, remappedTables[table]); err != nil {
			return nil, fmt.Errorf("restore %s: %w", table, err)
		}
	}
	// 证据绑定是纯关联行（I04）：入向逻辑引用是 (finding_id, snapshot_id)，冷包里的
	// 自增 id 没有任何引用方，照抄回去只会撞上归档期间别人的新行。恢复前把旧物理
	// 主键摘掉，交给 TiDB 重新分配；snapshot 属于调用方，这里只改副本。
	if err := restoreFindingTrafficTx(tx, archiveSnapshotWithoutBindingPKs(snapshot)); err != nil {
		return nil, fmt.Errorf("restore finding traffic: %w", err)
	}
	if streamedLLMRecords != "" {
		count, err := insertArchiveJSONSequenceRows(ctx, tx, "llm_records", llmRecords)
		if err != nil {
			return nil, fmt.Errorf("restore llm_records: %w", err)
		}
		if expected := snapshot.DataCounts["llm_records"]; count != expected {
			return nil, fmt.Errorf("restore llm_records: row count %d does not match manifest %d", count, expected)
		}
	} else if err := insertArchiveTableRows(ctx, tx, "llm_records", remappedTables["llm_records"]); err != nil {
		return nil, fmt.Errorf("restore llm_records: %w", err)
	}
	for _, table := range []string{"llm_usage", "skill_usage", "tool_usage"} {
		if err := insertArchiveTableRows(ctx, tx, table, remappedTables[table]); err != nil {
			return nil, fmt.Errorf("restore %s: %w", table, err)
		}
	}
	if err := restoreInterceptRowsCtx(ctx, tx, remappedTables["intercept_pending"]); err != nil {
		return nil, err
	}
	warningsRaw, _ := json.Marshal(warnings)
	if _, err := tx.Exec(`UPDATE task_archives SET warnings=?,phase='database_restored',progress=85,error='' WHERE id=?`, string(warningsRaw), archiveID); err != nil {
		return nil, err
	}
	return warnings, tx.Commit()
}

// archiveSnapshotWithoutBindingPKs 返回一份仅剥离 finding_traffic_bindings 旧物理
// 主键的快照副本（I04）。证据绑定行没有任何入向物理引用，逻辑身份是
// (finding_id, snapshot_id)，冷包里的旧 id 一律由 TiDB 重新分配；原快照归调用方
// 所有，绝不就地改写。
func archiveSnapshotWithoutBindingPKs(snapshot *TaskArchiveSnapshot) *TaskArchiveSnapshot {
	raw, present := snapshot.Tables["finding_traffic_bindings"]
	if !present || len(raw) == 0 {
		return snapshot
	}
	rows, err := archiveDecodeRawRows(raw)
	if err != nil || len(rows) == 0 {
		return snapshot
	}
	stripped := false
	for _, row := range rows {
		if _, found := row["id"]; found {
			delete(row, "id")
			stripped = true
		}
	}
	if !stripped {
		return snapshot
	}
	trimmed, err := json.Marshal(rows)
	if err != nil {
		return snapshot
	}
	tables := make(map[string]json.RawMessage, len(snapshot.Tables))
	for name, value := range snapshot.Tables {
		tables[name] = value
	}
	tables["finding_traffic_bindings"] = trimmed
	out := *snapshot
	out.Tables = tables
	return &out
}

// archiveEdgeKey / archiveAnchorKey 是核心图关联的逻辑身份，用于冷包与热库的元组比对。
type archiveEdgeKey struct {
	src int64
	dst int64
	rel string
}

type archiveAnchorKey struct {
	nodeID  int64
	assetID int64
}

// assertRestorableArchiveStub 持锁核验恢复目标确属当前 archive 的合法压缩残留（I05）。
//
// 归档恢复的"根实体冲突例外"只有一种情形：task/exploration 还在，但已经被压缩成
// 只剩身份的空壳。若目标是活跃根实体（archived_at/deleted_at 未同时置位，或
// exploration 仍带着压缩前的内容），说明 restoring 元数据与热库已经不一致，必须
// fail-closed，绝不能把冷包内容覆盖到无关的活跃任务上。
func assertRestorableArchiveStub(ctx context.Context, tx *sql.Tx, taskID, expID int64, snapshot *TaskArchiveSnapshot) error {
	// 快照根行身份：冷包里的 tasks/explorations 行必须就是这张 archive 的根实体本身，
	// 否则紧随其后的根实体 UPDATE 会写到无关行上。
	if err := assertArchivedRootRowIdentity(snapshot, "tasks", taskID); err != nil {
		return err
	}
	if err := assertArchivedRootRowIdentity(snapshot, "explorations", expID); err != nil {
		return err
	}
	var taskExpID int64
	var archived, deleted int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(exploration_id,0),
 COALESCE(archived_at IS NOT NULL,0), COALESCE(deleted_at IS NOT NULL,0)
FROM tasks WHERE id=? FOR UPDATE`, taskID).Scan(&taskExpID, &archived, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("restore tasks: archived task %d is missing, restore refused", taskID)
		}
		return fmt.Errorf("restore probe tasks: %w", err)
	}
	if taskExpID != expID {
		return fmt.Errorf("restore tasks: task %d points at exploration %d, snapshot expects %d", taskID, taskExpID, expID)
	}
	if archived == 0 || deleted == 0 {
		return fmt.Errorf("restore tasks: task %d is not an archived stub (archived=%d deleted=%d), restore refused",
			taskID, archived, deleted)
	}
	var description, goal, status string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(description,''),COALESCE(goal,''),COALESCE(status,'')
FROM explorations WHERE id=? FOR UPDATE`, expID).Scan(&description, &goal, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("restore explorations: archived exploration %d is missing, restore refused", expID)
		}
		return fmt.Errorf("restore probe explorations: %w", err)
	}
	// 压缩把 exploration 清成 description=''、goal=''、status='open'；任一不符就说明
	// 它不是本次归档留下的 stub（可能已被别的任务写回内容）。
	if description != "" || goal != "" || status != "open" {
		return fmt.Errorf("restore explorations: exploration %d is not a compacted stub (description %d chars, goal %d chars, status %q), restore refused",
			expID, len(description), len(goal), status)
	}
	return nil
}

// assertArchivedRootRowIdentity 校验冷包根表里那一行的 id 与 archive 声称的根实体一致。
func assertArchivedRootRowIdentity(snapshot *TaskArchiveSnapshot, table string, want int64) error {
	rows, err := archiveDecodeRawRows(snapshot.Tables[table])
	if err != nil {
		return fmt.Errorf("restore %s identity: %w", table, err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("restore %s: expected exactly one root row, manifest has %d", table, len(rows))
	}
	id, ok := jsonInt64(rawAny(rows[0]["id"]))
	if !ok || id != want {
		return fmt.Errorf("restore %s: snapshot root id does not match archive root %d", table, want)
	}
	return nil
}

// assertCoreGraphConflictFree 在写入之前对冷包里的核心图实体做一次 current read
// 行锁探测（按物理 PK 升序）：
//   - findings / exploration_nodes：主键即逻辑身份，命中即说明身份已被占用；
//   - exploration_edges / exploration_anchors：隐藏 row_id 由 TiDB 重新分配，冲突
//     只会落在唯一键上，因此锁住同探索/同节点集合的既有边与锚点做元组比对。
//
// 核心图实体的身份一旦被占用就无法靠重新分配物理键绕开，因此任一命中都直接返回
// 错误，由调用方的事务整体回滚（fail-closed，M25）。
func assertCoreGraphConflictFree(ctx context.Context, tx *sql.Tx, expID int64, snapshot *TaskArchiveSnapshot) error {
	probes := []struct {
		table string
		rows  json.RawMessage
	}{
		{"exploration_nodes", snapshot.Tables["exploration_nodes"]},
		{"findings", snapshot.Tables["findings"]},
	}
	for _, probe := range probes {
		ids, err := archivedRowPrimaryKeys(probe.rows)
		if err != nil {
			return fmt.Errorf("restore %s identity: %w", probe.table, err)
		}
		for _, batch := range archiveIDBatches(ids) {
			args := make([]any, 0, len(batch))
			for _, id := range batch {
				args = append(args, id)
			}
			// 按物理 PK 升序加行锁：与并发写入者保持同一锁序，探测本身不会死锁。
			var occupied int64
			err := tx.QueryRowContext(ctx, `SELECT id FROM `+probe.table+` WHERE id IN (`+
				questionPlaceholders(len(batch))+`) ORDER BY id LIMIT 1 FOR UPDATE`, args...).Scan(&occupied)
			if err == nil {
				return fmt.Errorf("restore %s: core graph entity %d already exists, restore refused", probe.table, occupied)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("restore probe %s: %w", probe.table, err)
			}
		}
	}
	if err := assertArchiveEdgesFree(ctx, tx, expID, snapshot.Tables["exploration_edges"]); err != nil {
		return err
	}
	return assertArchiveAnchorsFree(ctx, tx, snapshot.Tables["exploration_anchors"])
}

// assertArchiveEdgesFree 锁住同探索的既有边（隐藏 row_id 会被 TiDB 重新分配，冲突
// 只会落在 uq_exploration_edges 唯一键上），再与冷包逐条比对逻辑身份。
func assertArchiveEdgesFree(ctx context.Context, tx *sql.Tx, expID int64, raw json.RawMessage) error {
	archived, err := archiveDecodeRawRows(raw)
	if err != nil {
		return fmt.Errorf("restore exploration_edges identity: %w", err)
	}
	if len(archived) == 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT src_id,dst_id,rel FROM exploration_edges
WHERE exploration_id=? ORDER BY src_id,dst_id,rel FOR UPDATE`, expID)
	if err != nil {
		return fmt.Errorf("restore probe exploration_edges: %w", err)
	}
	occupied := map[archiveEdgeKey]bool{}
	for rows.Next() {
		var key archiveEdgeKey
		if err := rows.Scan(&key.src, &key.dst, &key.rel); err != nil {
			rows.Close()
			return err
		}
		occupied[key] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, row := range archived {
		key := archiveEdgeKey{}
		key.src, _ = jsonInt64(rawAny(row["src_id"]))
		key.dst, _ = jsonInt64(rawAny(row["dst_id"]))
		key.rel, _ = rawText(row["rel"])
		if occupied[key] {
			return fmt.Errorf("restore exploration_edges: edge %d-%s->%d already exists, restore refused", key.src, key.rel, key.dst)
		}
	}
	return nil
}

// assertArchiveAnchorsFree 与边同理，比对 uq_exploration_anchors(node_id,asset_id)。
func assertArchiveAnchorsFree(ctx context.Context, tx *sql.Tx, raw json.RawMessage) error {
	archived, err := archiveDecodeRawRows(raw)
	if err != nil {
		return fmt.Errorf("restore exploration_anchors identity: %w", err)
	}
	if len(archived) == 0 {
		return nil
	}
	nodes := make([]int64, 0, len(archived))
	for _, row := range archived {
		if nodeID, ok := jsonInt64(rawAny(row["node_id"])); ok {
			nodes = append(nodes, nodeID)
		}
	}
	for _, batch := range archiveIDBatches(nodes) {
		args := make([]any, 0, len(batch))
		for _, id := range batch {
			args = append(args, id)
		}
		rows, err := tx.QueryContext(ctx, `SELECT node_id,asset_id FROM exploration_anchors
WHERE node_id IN (`+questionPlaceholders(len(batch))+`) ORDER BY node_id,asset_id FOR UPDATE`, args...)
		if err != nil {
			return fmt.Errorf("restore probe exploration_anchors: %w", err)
		}
		occupied := map[archiveAnchorKey]bool{}
		for rows.Next() {
			var key archiveAnchorKey
			if err := rows.Scan(&key.nodeID, &key.assetID); err != nil {
				rows.Close()
				return err
			}
			occupied[key] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, row := range archived {
			key := archiveAnchorKey{}
			key.nodeID, _ = jsonInt64(rawAny(row["node_id"]))
			key.assetID, _ = jsonInt64(rawAny(row["asset_id"]))
			if occupied[key] {
				return fmt.Errorf("restore exploration_anchors: anchor (%d,%d) already exists, restore refused", key.nodeID, key.assetID)
			}
		}
	}
	return nil
}

// archivedRowPrimaryKeys 取出冷包某张表里显式声明的物理主键（升序、去重）。
func archivedRowPrimaryKeys(raw json.RawMessage) ([]int64, error) {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return nil, err
	}
	seen := map[int64]bool{}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		id, ok := jsonInt64(rawAny(row["id"]))
		if !ok || id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	// 升序：探测语句按物理 PK 顺序加行锁，与并发写入者共用同一把锁序。
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func restoreExplorationStub(tx *sql.Tx, raw json.RawMessage, expID int64) error {
	// 原实现把整行 JSON 灌进 json_populate_record 做 UPDATE ... FROM；TiDB 没有
	// 这个集合函数，改成显式列绑定。
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("restore explorations: expected one row")
	}
	row := rows[0]
	description, err := archiveStubValue("explorations", "description", row)
	if err != nil {
		return err
	}
	goal, err := archiveStubValue("explorations", "goal", row)
	if err != nil {
		return err
	}
	status, err := archiveStubValue("explorations", "status", row)
	if err != nil {
		return err
	}
	createdAt, err := archiveStubValue("explorations", "created_at", row)
	if err != nil {
		return err
	}
	updatedAt, err := archiveStubValue("explorations", "updated_at", row)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE explorations SET description=?, goal=?, status=?, created_at=?, updated_at=? WHERE id=?`,
		description, goal, status, createdAt, updatedAt, expID)
	return err
}

// archiveStubValue 从 manifest 行里取某一列的绑定值（列不存在时给 NULL）。
func archiveStubValue(table, column string, row map[string]json.RawMessage) (any, error) {
	raw, ok := row[column]
	if !ok {
		return nil, nil
	}
	definition, err := archiveColumnOf(table, column)
	if err != nil {
		return nil, err
	}
	return archiveDecodeValue(definition, raw)
}

// restoreTaskStub 把冷包里的任务行写回归档 stub。
// 原实现是 `UPDATE ... FROM json_populate_record(NULL::tasks, $2::json)`，TiDB 没有
// 集合值函数，这里按列显式绑定；paused 且仍有剩余时长时用
// `DATE_ADD(NOW(3), INTERVAL ? SECOND)` 续期（原 make_interval(secs=>)）。
func restoreTaskStub(tx *sql.Tx, row map[string]any, taskID, remaining int64) error {
	raw, err := archiveEncodeTaskRow(row)
	if err != nil {
		return err
	}
	set := []string{"queued=false", "queued_at=NULL", "queue_mode=''", "deleted_at=NULL", "archived_at=NULL"}
	args := []any{}
	for _, column := range []string{
		"name", "category_id", "description", "goal", "status", "paused",
		"llm_profile_id", "active_llm_profile_id", "llm_chain_revision", "company_id",
		"parent_ref", "timeout_seconds", "plan_heartbeat_seconds", "coverage_enabled",
		"pinned_at", "first_run_at", "completed_at", "created_at", "updated_at",
	} {
		value, err := archiveStubValue("tasks", column, raw)
		if err != nil {
			return err
		}
		set = append(set, column+"=?")
		args = append(args, value)
	}
	paused, _ := archiveStubValue("tasks", "paused", raw)
	if remaining > 0 && archiveIsTrue(paused) {
		set = append(set, "deadline_at=DATE_ADD(NOW(3), INTERVAL ? SECOND)")
		args = append(args, remaining)
	} else {
		deadline, err := archiveStubValue("tasks", "deadline_at", raw)
		if err != nil {
			return err
		}
		set = append(set, "deadline_at=?")
		args = append(args, deadline)
	}
	_, err = tx.Exec("UPDATE tasks SET "+strings.Join(set, ",")+" WHERE id=?", append(args, taskID)...)
	return err
}

// archiveIsTrue 判定已绑定值的真值（archiveDecodeValue 把布尔写成 0/1）。
func archiveIsTrue(value any) bool {
	switch typed := value.(type) {
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case bool:
		return typed
	case []byte:
		return len(typed) > 0 && typed[0] != '0'
	case string:
		return archiveBoolText(typed)
	default:
		return false
	}
}

// archiveEncodeTaskRow 把 decodeArchiveRows 的 map[string]any 重新编成逐列 JSON，
// 让 stub 更新与普通行写入共用同一套 Catalog 驱动的类型还原。
func archiveEncodeTaskRow(row map[string]any) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(row))
	for key, value := range row {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		out[key] = encoded
	}
	return out, nil
}

func decodeArchiveRows(raw json.RawMessage) ([]map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	// UseNumber：BIGINT 的 id 一旦被解成 float64，超过 2^53 的资产/节点 id 就会被
	// 悄悄改写，重映射出来的关联会指向不存在的行。
	decoder.UseNumber()
	var rows []map[string]any
	if err := decoder.Decode(&rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func rowExists(tx *sql.Tx, table string, id int64) bool {
	if table != "task_categories" && table != "llm_profiles" && table != "companies" && table != "tasks" && table != "assets" {
		return false
	}
	var exists int
	_ = tx.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE id=?`, id).Scan(&exists)
	return exists > 0
}

func insertArchiveJSONSequenceRows(ctx context.Context, tx *sql.Tx, table string, reader io.Reader) (int64, error) {
	if table != "llm_records" {
		return 0, fmt.Errorf("archive streamed restore table %q is not allowed", table)
	}
	decoder := json.NewDecoder(reader)
	var count int64
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, err
		}
		trimmed := []byte(strings.TrimSpace(string(raw)))
		if len(trimmed) == 0 {
			continue
		}
		if trimmed[0] != '{' {
			return 0, errors.New("streamed archive row must be a JSON object")
		}
		var row map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &row); err != nil {
			return 0, err
		}
		if _, err := archiveInsertRowTx(ctx, tx, table, row, nil, nil); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// restoreArchiveAssets 逐行还原资产：自然键命中就复用现有行，否则重算 canonical
// identity/hash 后插入，物理 id 由 TiDB 重新分配（冷包里的旧 id 绝不复用）。
func restoreArchiveAssets(ctx context.Context, tx *sql.Tx, taskID int64, raw json.RawMessage) (map[int64]int64, []string, error) {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return nil, nil, err
	}
	// 按冷包里的旧 id 升序处理：所有资产行的加锁顺序全局一致，杜绝交叉死锁。
	sortArchiveRowsByLegacyID(rows)
	mapping := make(map[int64]int64, len(rows))
	warnings := []string{}
	for _, row := range rows {
		oldID, ok := jsonInt64(rawAny(row["id"]))
		if !ok || oldID <= 0 {
			return nil, nil, errors.New("archived asset has invalid id")
		}
		if companyID, ok := jsonInt64(rawAny(row["company_id"])); ok && companyID > 0 && !rowExists(tx, "companies", companyID) {
			row["company_id"] = json.RawMessage("null")
			row["company_source"] = json.RawMessage(`"explicit"`)
			warnings = append(warnings, fmt.Sprintf("资产 %d 的企业 %d 已删除，已恢复为未归属", oldID, companyID))
		}
		identity, err := archiveAssetIdentity(row)
		if err != nil {
			return nil, nil, err
		}
		existing, found, err := findArchiveAssetNaturalID(tx, row)
		if err != nil {
			return nil, nil, err
		}
		if found {
			// 自然键命中：先锁住这一行的物理 PK，再在同一事务内做 hash 仲裁，
			// 确认冷包里的语义列与线上 canonical identity 字节级一致（M25）。
			if _, err := tx.Exec(`SELECT id FROM assets WHERE id=? FOR UPDATE`, existing); err != nil {
				return nil, nil, err
			}
			if err := archiveVerifyIdentityArbitration(ctx, tx, "assets", identity, existing); err != nil {
				return nil, nil, err
			}
			mapping[oldID] = existing
			if err := appendTaskToAssetOwnership(tx, existing, taskID); err != nil {
				return nil, nil, err
			}
			continue
		}
		// 冷包里的物理 id 只有在无人占用时才复用（历史 finding/task 引用会指向它），
		// 一旦被占用就彻底交给 TiDB 自增重新分配，绝不 nextval 造号。
		var explicitPK any
		if rowExists(tx, "assets", oldID) {
			warnings = append(warnings, fmt.Sprintf("资产 ID %d 已被占用，恢复为 TiDB 重新分配的自增 ID", oldID))
		} else {
			explicitPK = oldID
		}
		row["task_ids"] = mergeJSONTaskIDRaw(row["task_ids"], taskID)
		// id 一律从行里摘掉：显式复用走 explicitPK，否则由 TiDB 自增重新分配。
		delete(row, "id")
		pk, err := archiveInsertRowTx(ctx, tx, "assets", row, &identity, explicitPK)
		if err != nil {
			if !tidb.IsDuplicateKey(err) {
				return nil, nil, err
			}
			// 唯一 hash 冲突：并发窗口里已经有人插入了同一自然键，回读并复用。
			existing, found, findErr := findArchiveAssetNaturalID(tx, row)
			if findErr != nil || !found {
				return nil, nil, errors.Join(findErr, fmt.Errorf("could not restore asset %d", oldID))
			}
			if _, err := tx.Exec(`SELECT id FROM assets WHERE id=? FOR UPDATE`, existing); err != nil {
				return nil, nil, err
			}
			if err := archiveVerifyIdentityArbitration(ctx, tx, "assets", identity, existing); err != nil {
				return nil, nil, err
			}
			if err := appendTaskToAssetOwnership(tx, existing, taskID); err != nil {
				return nil, nil, err
			}
			pk = existing
		}
		mapping[oldID] = pk
	}
	return mapping, warnings, nil
}

// archiveAssetIdentity 按资产类型激活对应的 canonical identity 分支。
func archiveAssetIdentity(row map[string]json.RawMessage) (archiveIdentityWrite, error) {
	typeName, _ := rawText(row["type"])
	text := func(key string) string {
		value, _ := rawText(row[key])
		return value
	}
	var branch assetBranch
	switch typeName {
	case "root_domain":
		branch = newAssetBranch("assets_root_domain", schema.AssetsRootDomainIdentity(text("domain")))
	case "ip":
		branch = newAssetBranch("assets_ip", schema.AssetsIPIdentity(text("ip")))
	case "subdomain":
		branch = newAssetBranch("assets_subdomain", schema.AssetsSubdomainIdentity(text("domain"), text("record_type")))
	case "app":
		if bundle := text("bundle_id"); bundle != "" {
			branch = newAssetBranch("assets_app_bundle", schema.AssetsAppBundleIdentity(bundle))
		} else {
			branch = newAssetBranch("assets_app_name", schema.AssetsAppNameIdentity(text("app_name")))
		}
	case "service":
		if text("service_type") == "http" {
			branch = newAssetBranch("assets_service_http", schema.AssetsServiceHTTPIdentity(text("url")))
		} else {
			portValue, portNull := archiveUint64OrNull(row["port"])
			nameValue, nameNull := archiveStringOrNil(row["service_name"])
			branch = newAssetBranch("assets_service_other", schema.AssetsServiceOtherIdentity(
				text("domain"), text("ip"), portValue, portNull, nameValue, nameNull))
		}
	case "endpoint":
		branch = newAssetBranch("assets_endpoint", schema.AssetsEndpointIdentity(text("url"), text("method")))
	default:
		return archiveIdentityWrite{}, fmt.Errorf("unsupported archived asset type %q", typeName)
	}
	return archiveIdentityWrite{
		pkCol:    "id",
		identCol: branch.identCol,
		hashCol:  branch.hashCol,
		identity: branch.identity,
		hash:     branch.hash,
	}, nil
}

func archiveUint64OrNull(raw json.RawMessage) (uint64, bool) {
	value, ok := jsonInt64(rawAny(raw))
	if !ok || string(raw) == "null" || len(raw) == 0 {
		return 0, true
	}
	if value < 0 {
		return 0, true
	}
	return uint64(value), false
}

func archiveStringOrNil(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", true
	}
	text, ok := rawText(raw)
	if !ok {
		return "", true
	}
	return text, false
}

// appendTaskToAssetOwnership 把任务补进 assets.task_ids（JSON 数组文本），并同步
// 依赖该归属的读取视图。原实现是 array_append + $1=ANY 的 SQL 端并集。
func appendTaskToAssetOwnership(tx *sql.Tx, assetID, taskID int64) error {
	var raw string
	if err := tx.QueryRow(`SELECT COALESCE(task_ids,'[]') FROM assets WHERE id=? FOR UPDATE`, assetID).Scan(&raw); err != nil {
		return err
	}
	var ids []int64
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		ids = nil
	}
	for _, id := range ids {
		if id == taskID {
			return nil
		}
	}
	ids = append(ids, taskID)
	encoded, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE assets SET task_ids=? WHERE id=?`, string(encoded), assetID)
	return err
}

// mergeJSONTaskIDRaw 把任务 id 合入 manifest 行里的 task_ids（保持 JSON 原文形状）。
func mergeJSONTaskIDRaw(raw json.RawMessage, taskID int64) json.RawMessage {
	value := rawAny(raw)
	ids := mergeJSONTaskID(value, taskID)
	encoded, err := json.Marshal(ids)
	if err != nil {
		return json.RawMessage(`[]`)
	}
	return encoded
}

func mergeJSONTaskID(value any, taskID int64) []int64 {
	out := []int64{}
	seen := map[int64]bool{}
	if values, ok := value.([]any); ok {
		for _, item := range values {
			if id, ok := jsonInt64(item); ok && id > 0 && !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	if !seen[taskID] {
		out = append(out, taskID)
	}
	return out
}

func findArchiveAssetNaturalID(tx *sql.Tx, row map[string]json.RawMessage) (int64, bool, error) {
	typeName, _ := rawText(row["type"])
	stringValue := func(key string) string {
		value, _ := rawText(row[key])
		return value
	}
	var query string
	var args []any
	switch typeName {
	case "root_domain":
		query, args = `SELECT id FROM assets WHERE type='root_domain' AND domain=?`, []any{stringValue("domain")}
	case "ip":
		query, args = `SELECT id FROM assets WHERE type='ip' AND ip=?`, []any{stringValue("ip")}
	case "subdomain":
		query, args = `SELECT id FROM assets WHERE type='subdomain' AND domain=? AND COALESCE(record_type,'')=?`, []any{stringValue("domain"), stringValue("record_type")}
	case "app":
		if bundle := stringValue("bundle_id"); bundle != "" {
			query, args = `SELECT id FROM assets WHERE type='app' AND bundle_id=?`, []any{bundle}
		} else {
			query, args = `SELECT id FROM assets WHERE type='app' AND bundle_id IS NULL AND app_name=?`, []any{stringValue("app_name")}
		}
	case "service":
		if stringValue("service_type") == "http" {
			query, args = `SELECT id FROM assets WHERE type='service' AND service_type='http' AND url=?`, []any{stringValue("url")}
		} else {
			port, _ := jsonInt64(rawAny(row["port"]))
			query, args = `SELECT id FROM assets WHERE type='service' AND service_type='other' AND COALESCE(domain,'')=? AND COALESCE(ip,'')=? AND port=? AND service_name=?`, []any{stringValue("domain"), stringValue("ip"), port, stringValue("service_name")}
		}
	case "endpoint":
		query, args = `SELECT id FROM assets WHERE type='endpoint' AND url=? AND method=?`, []any{stringValue("url"), stringValue("method")}
	default:
		return 0, false, fmt.Errorf("unsupported archived asset type %q", typeName)
	}
	var id int64
	err := tx.QueryRow(query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

func remapArchiveAssetReferences(tables map[string]json.RawMessage, mapping map[int64]int64) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(tables))
	for name, raw := range tables {
		out[name] = raw
	}
	for _, table := range []string{"exploration_anchors", "task_asset_links"} {
		rows, err := decodeArchiveRows(tables[table])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if old, ok := jsonInt64(row["asset_id"]); ok {
				if replacement, exists := mapping[old]; exists {
					row["asset_id"] = replacement
				}
			}
		}
		out[table], _ = json.Marshal(rows)
	}
	for _, table := range []string{"exploration_nodes", "findings"} {
		rows, err := decodeArchiveRows(tables[table])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			key := "asset_ids"
			container := row
			if table == "exploration_nodes" {
				payload, ok := row["payload"].(map[string]any)
				if !ok {
					continue
				}
				container = payload
			}
			if values, ok := container[key].([]any); ok {
				for i, value := range values {
					if old, ok := jsonInt64(value); ok {
						if replacement, exists := mapping[old]; exists {
							values[i] = replacement
						}
					}
				}
				container[key] = values
			}
		}
		out[table], _ = json.Marshal(rows)
	}
	return out, nil
}

// restoreTaskRelations 还原继承关系：来源任务必须仍然存活（合法归档 stub 之外一律
// 跳过并给出告警），已存在的关系不重复插入。原 ON CONFLICT DO NOTHING 改成显式判重，
// 避免 INSERT IGNORE 顺手把非空/截断错误一起咽掉。
func restoreTaskRelations(ctx context.Context, tx *sql.Tx, taskID int64, raw json.RawMessage) ([]string, error) {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return nil, err
	}
	warnings := []string{}
	for _, row := range rows {
		sourceID, ok := jsonInt64(rawAny(row["source_task_id"]))
		if !ok || !liveTaskExists(tx, sourceID) {
			warnings = append(warnings, fmt.Sprintf("来源任务 %d 不可用，继承关系已跳过", sourceID))
			continue
		}
		var existing int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM task_relations WHERE task_id=? AND source_task_id=?`, taskID, sourceID).Scan(&existing); err != nil {
			return nil, err
		}
		if existing > 0 {
			continue
		}
		created, _ := archiveJSONTimestamp(row["created_at"])
		if _, err := tx.ExecContext(ctx, `INSERT INTO task_relations(task_id,source_task_id,created_at) VALUES(?,?,COALESCE(?,NOW(3)))`,
			taskID, sourceID, archiveTimeArg(created)); err != nil {
			return nil, fmt.Errorf("restore task_relations: %w", err)
		}
	}
	return warnings, nil
}

// restoreSideQuestionSessions 还原旁路会话。物理 row_id 交给 TiDB 重新分配；
// memory/context_info 一类缺列由列默认值兜底，不再手工塞 {}。
func restoreSideQuestionSessions(ctx context.Context, tx *sql.Tx, raw json.RawMessage) ([]string, error) {
	if err := insertArchiveTableRows(ctx, tx, "side_question_sessions", raw); err != nil {
		return nil, err
	}
	return nil, nil
}

// restoreSideQuestionRequests 还原旁路请求：生成列 running_session_key 绝不写入，
// ordinal 由 TiDB 重新分配，identity/identity_hash 按 canonical 口径重算并在事务内仲裁。
func restoreSideQuestionRequests(ctx context.Context, tx *sql.Tx, raw json.RawMessage) error {
	return insertArchiveTableRows(ctx, tx, "side_question_requests", raw)
}

func archiveSideRequestIdentity(row map[string]json.RawMessage) (archiveIdentityWrite, error) {
	sessionKey, ok := rawText(row["session_key"])
	if !ok {
		return archiveIdentityWrite{}, errors.New("archived side question request has no session_key")
	}
	clientID, ok := rawText(row["client_id"])
	if !ok {
		return archiveIdentityWrite{}, errors.New("archived side question request has no client_id")
	}
	generation, _ := jsonInt64(rawAny(row["generation"]))
	payload := schema.SideQuestionRequestsIdentity(sessionKey, uint64(generation), clientID)
	sum := schema.CanonicalHash(payload)
	return archiveIdentityWrite{
		pkCol:    "ordinal",
		identCol: "identity",
		hashCol:  "identity_hash",
		identity: payload,
		hash:     sum,
	}, nil
}

// restoreTaskScopes 还原任务范围。identity/identity_hash 是 NOT NULL 的 canonical
// 派生列，必须按当前行的语义列重算（冷包里的旧值不采信）。
//
// hash 命中本身不构成"已经恢复过"的证据（I03）：摘要相同、完整 identity 不同是合法
// 可能。因此命中时也要先按物理 PK 取行锁，再在同一事务内用 VerifyHashArbitration
// 核验完整 identity；碰撞一律上抛，由外层整笔回滚。1062 同样不得盲目吞掉。
func restoreTaskScopes(ctx context.Context, tx *sql.Tx, raw json.RawMessage) ([]string, error) {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return nil, err
	}
	warnings := []string{}
	for _, row := range rows {
		if companyID, ok := jsonInt64(rawAny(row["company_id"])); ok && companyID > 0 && !rowExists(tx, "companies", companyID) {
			warnings = append(warnings, fmt.Sprintf("企业 %d 已删除，关联范围已跳过", companyID))
			continue
		}
		identity, err := archiveTaskScopeIdentity(row)
		if err != nil {
			return nil, err
		}
		duplicate, err := resolveArchivedHashIdentity(ctx, tx, "task_scope", &identity)
		if err != nil {
			return nil, err
		}
		if duplicate {
			continue
		}
		if _, err := archiveInsertRowTx(ctx, tx, "task_scope", row, &identity, nil); err != nil {
			if !tidb.IsDuplicateKey(err) {
				return nil, err
			}
			// 唯一键撞车：并发窗口里已经有人写下了同一摘要。走与命中分支完全相同的
			// 协议——锁候选 PK、仲裁完整 identity；只有字节级一致才允许跳过。
			duplicate, arbErr := resolveArchivedHashIdentity(ctx, tx, "task_scope", &identity)
			if arbErr != nil {
				return nil, arbErr
			}
			if !duplicate {
				return nil, fmt.Errorf("archive restore task_scope: duplicate key on a hash that no longer resolves: %w", err)
			}
			continue
		}
	}
	return warnings, nil
}

// resolveArchivedHashIdentity 处理"按 hash 已经命中一行"的分支：
//  1. 先用一次非锁定读定位候选物理 PK（VerifyHashArbitration 不能作为预定位器）；
//  2. 按物理 PK 拿 FOR UPDATE 行锁，满足 helper 的锁序前置条件；
//  3. 在同一事务内比对完整 identity：字节相等返回 true（同一身份，不必重复插入）；
//     不等时把 ErrIdentityHashCollision 原样上抛，由调用方整笔回滚。
func resolveArchivedHashIdentity(ctx context.Context, tx *sql.Tx, table string, identity *archiveIdentityWrite) (bool, error) {
	var candidate int64
	err := tx.QueryRowContext(ctx, "SELECT `"+identity.pkCol+"` FROM `"+table+"` WHERE `"+identity.hashCol+"`=?", identity.hash[:]).Scan(&candidate)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("archive restore %s hash lookup: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT `"+identity.pkCol+"` FROM `"+table+"` WHERE `"+identity.pkCol+"`=? FOR UPDATE", candidate); err != nil {
		return false, fmt.Errorf("archive restore %s lock candidate row %d: %w", table, candidate, err)
	}
	found, err := schema.VerifyHashArbitration(ctx, tx, table, identity.pkCol, identity.identCol, identity.hashCol, identity.hash, identity.identity)
	if err != nil {
		if errors.Is(err, schema.ErrIdentityHashCollision) {
			return false, fmt.Errorf("archive restore %s: identity hash collision on candidate row %d, restore refused: %w", table, candidate, err)
		}
		return false, fmt.Errorf("archive restore %s hash arbitration: %w", table, err)
	}
	if int64(found) != candidate {
		return false, fmt.Errorf("archive restore %s: hash arbitration resolved to row %d but lookup found %d", table, found, candidate)
	}
	return true, nil
}

func archiveTaskScopeIdentity(row map[string]json.RawMessage) (archiveIdentityWrite, error) {
	taskID, ok := jsonInt64(rawAny(row["task_id"]))
	if !ok {
		return archiveIdentityWrite{}, errors.New("archived task scope has no task_id")
	}
	kind, ok := rawText(row["kind"])
	if !ok {
		return archiveIdentityWrite{}, errors.New("archived task scope has no kind")
	}
	domain, _ := rawText(row["domain"])
	network, _ := rawText(row["net"])
	value, _ := rawText(row["value"])
	companyID, _ := jsonInt64(rawAny(row["company_id"]))
	if companyID < 0 {
		companyID = 0
	}
	payload := schema.TaskScopeIdentity(uint64(taskID), kind, domain, network, uint64(companyID), value)
	sum := schema.CanonicalHash(payload)
	return archiveIdentityWrite{
		pkCol:    "id",
		identCol: "identity",
		hashCol:  "identity_hash",
		identity: payload,
		hash:     sum,
	}, nil
}

// restoreTaskLLMProfiles 还原配置链。profile 已被删除时跳过并告警；(task_id,
// profile_id) 已存在时不重复插入；position 按链内顺序重新编号。
func restoreTaskLLMProfiles(ctx context.Context, tx *sql.Tx, taskID int64, raw json.RawMessage) ([]string, error) {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return nil, err
	}
	warnings := []string{}
	position := 0
	for _, row := range rows {
		profileID, ok := jsonInt64(rawAny(row["profile_id"]))
		if !ok || !rowExists(tx, "llm_profiles", profileID) {
			warnings = append(warnings, fmt.Sprintf("LLM 配置 %d 已删除，配置链项已跳过", profileID))
			continue
		}
		var existing int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM task_llm_profiles WHERE task_id=? AND profile_id=?`, taskID, profileID).Scan(&existing); err != nil {
			return nil, err
		}
		if existing > 0 {
			continue
		}
		row["task_id"] = mustJSON(taskID)
		row["position"] = mustJSON(position)
		if status, _ := rawText(row["status"]); status == "" {
			row["status"] = json.RawMessage(`"ready"`)
		}
		if last, ok := rawText(row["last_error"]); ok && last == "" {
			row["last_error"] = json.RawMessage("null")
		}
		if _, err := archiveInsertRowTx(ctx, tx, "task_llm_profiles", row, nil, nil); err != nil {
			return nil, err
		}
		position++
	}
	return warnings, nil
}

// restoreInterceptRows 还原审批记录（老入口，签名保留给既有测试）。
func restoreInterceptRows(tx *sql.Tx, raw json.RawMessage) error {
	return restoreInterceptRowsCtx(context.Background(), tx, raw)
}

func restoreInterceptRowsCtx(ctx context.Context, tx *sql.Tx, raw json.RawMessage) error {
	rows, err := archiveDecodeRawRows(raw)
	if err != nil {
		return err
	}
	for _, row := range rows {
		// Archives predating approval snapshots lack this NOT NULL column.
		if source, _ := rawText(row["decision_source"]); source == "" {
			source = "unknown"
			if _, present := row["rule_id"]; present && string(row["rule_id"]) != "null" {
				source = "rule"
			} else if reason, _ := rawText(row["reason"]); strings.HasPrefix(reason, "[模型]") {
				source = "model"
			}
			row["decision_source"] = mustJSON(source)
		}
		status, _ := rawText(row["status"])
		if status == "pending" {
			row["status"] = mustJSON("timeout")
			row["reason"] = mustJSON("任务归档期间审批已超时")
			row["decided_at"] = mustJSON(time.Now().UTC().Format(time.RFC3339Nano))
			if audit, ok := rawObject(row["audit"]); ok {
				audit["effective_action"] = mustJSON("deny")
				audit["decision_reason"] = mustJSON("任务归档期间审批已超时")
				audit["execution_status"] = mustJSON("not_executed")
				row["audit"] = mustJSON(audit)
			}
		} else if audit, ok := rawObject(row["audit"]); ok {
			if exec, _ := rawText(audit["execution_status"]); exec == "awaiting_result" {
				// An archived run cannot resume its former result callback.
				audit["execution_status"] = mustJSON("unknown")
				row["audit"] = mustJSON(audit)
			}
		}
		// 旧自增主键只有在仍空闲时才复用（I04）：archivePrepareLeafPrimaryKey 会先把
		// 行里的旧 id 摘掉再探测占用，被别的任务占走时返回 nil，交给 TiDB 重新分配。
		// 此前这里直接传 nil 显式键，白名单仍带着旧 id 落库，撞上无关任务的主键。
		explicitPK, err := archivePrepareLeafPrimaryKey(ctx, tx, "intercept_pending", row)
		if err != nil {
			return err
		}
		if _, err := archiveInsertRowTx(ctx, tx, "intercept_pending", row, nil, explicitPK); err != nil {
			return err
		}
	}
	return nil
}

func liveTaskExists(tx *sql.Tx, id int64) bool {
	var exists int
	_ = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE id=? AND deleted_at IS NULL`, id).Scan(&exists)
	return exists > 0
}

// CompleteTaskArchiveRestore removes compact metadata after every external
// component has been verified and the package has been consumed.
func (d *DB) CompleteTaskArchiveRestore(archiveID int64) error {
	res, err := d.Exec(`DELETE archive FROM task_archives archive JOIN tasks task ON archive.task_id=task.id
WHERE archive.id=? AND task.deleted_at IS NULL AND task.archived_at IS NULL`, archiveID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrTaskArchiveState
	}
	return nil
}

// DeleteTaskArchiveStub permanently removes the cold task after its package has
// been staged for deletion. Dependency protection is rechecked transactionally.
func (d *DB) DeleteTaskArchiveStub(archiveID int64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := d.coordinateWithSchemaMigration(tx); err != nil {
		return err
	}
	var taskID, expID int64
	var state string
	if err := tx.QueryRow(`SELECT archive.task_id,task.exploration_id,archive.state
FROM task_archives archive JOIN tasks task ON task.id=archive.task_id
WHERE archive.id=? FOR UPDATE`, archiveID).Scan(&taskID, &expID, &state); err != nil {
		return err
	}
	if state != Deleting {
		return ErrTaskArchiveState
	}
	var dependent int64
	err = tx.QueryRow(`SELECT task_id FROM task_archives WHERE id<>? AND JSON_CONTAINS(COALESCE(source_task_ids,'[]'), CAST(? AS JSON)) LIMIT 1`, archiveID, taskID).Scan(&dependent)
	if err == nil {
		return fmt.Errorf("%w: task %d", ErrTaskArchiveDeleteBlocked, dependent)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// 影响行数逐条核对：目标不是归档 stub（或已经被别处删走）时，绝不能"删 0 行也算成功"。
	if n, err := execArchiveStubDelete(tx, `DELETE FROM tasks WHERE id=? AND archived_at IS NOT NULL`, taskID); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("%w: task %d is not a deletable archive stub", ErrTaskArchiveState, taskID)
	}
	if n, err := execArchiveStubDelete(tx, `DELETE FROM explorations WHERE id=?`, expID); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("%w: exploration %d of archive %d is missing", ErrTaskArchiveState, expID, archiveID)
	}
	// 新 Catalog 没有 FK 级联（I06）：task_archives 元数据必须在这个事务里自己删干净。
	// 只删 task/exploration 会留下一条 state='deleting' 的孤儿归档——它污染归档列表、
	// 任务 claim 与依赖检查，重启重入时又 JOIN 不到 task，永远无法收敛。
	if n, err := execArchiveStubDelete(tx, `DELETE FROM task_archives WHERE id=?`, archiveID); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("%w: archive %d metadata is missing", ErrTaskArchiveState, archiveID)
	}
	return tx.Commit()
}

// execArchiveStubDelete 执行永久删除的一条 DELETE，并把真实影响行数带回来。
func execArchiveStubDelete(tx *sql.Tx, query string, args ...any) (int64, error) {
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

// ArchivedAggregateStats returns compact summaries used by global dashboards so
// cold data does not disappear from historical totals.
func (d *DB) ArchivedAggregateStats() ([]json.RawMessage, error) {
	rows, err := d.Query(`SELECT archive.aggregate_stats
FROM task_archives archive
JOIN tasks task ON task.id=archive.task_id
WHERE task.archived_at IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []json.RawMessage
	for rows.Next() {
		var raw json.RawMessage
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, rows.Err()
}

// =====================================================================
// manifest 行的通用小工具
// =====================================================================

// rawAny 把 json.RawMessage 还原成 any（供 jsonInt64 一类既有取值器复用）。
func rawAny(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	return value
}

// archiveJSONTimestamp 解出按列存放的 manifest 时间值（先取 JSON 文本再交给
// 既有的 archiveTimestamp 解析器，兼容 RFC3339 与 SQL 字面量两种冷包形态）。
func archiveJSONTimestamp(raw json.RawMessage) (time.Time, bool) {
	text, ok := rawText(raw)
	if !ok {
		return time.Time{}, false
	}
	return archiveTimestamp(text)
}

// rawText 解出一个 JSON 字符串列；非字符串（历史冷包的数字）转成其原文。
func rawText(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, `"`) {
		var text string
		if err := json.Unmarshal([]byte(trimmed), &text); err != nil {
			return "", false
		}
		return text, true
	}
	return trimmed, true
}

// rawObject 解出一个 JSON 对象列。
func rawObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, false
	}
	return out, true
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("null")
	}
	return encoded
}

// archiveTimeArg 把零值时间转成 NULL，交给列默认值。
func archiveTimeArg(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

// sortArchiveRowsByLegacyID 按冷包里的旧 id 升序稳定处理资产行。
func sortArchiveRowsByLegacyID(rows []map[string]json.RawMessage) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			left, _ := jsonInt64(rawAny(rows[j-1]["id"]))
			right, _ := jsonInt64(rawAny(rows[j]["id"]))
			if left <= right {
				break
			}
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
}
