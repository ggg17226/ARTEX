package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func ArchiveEvidenceSnapshots(snapshot *TaskArchiveSnapshot) ([]TrafficEvidenceSnapshot, error) {
	var out []TrafficEvidenceSnapshot
	if rawRowCount(snapshot.Tables["traffic_evidence_snapshots"]) == 0 {
		return out, nil
	}
	if snapshot.FormatVersion < 3 {
		return nil, ErrTaskArchiveFormatMismatch
	}
	err := json.Unmarshal(snapshot.Tables["traffic_evidence_snapshots"], &out)
	return out, err
}

func restoreFindingTrafficTx(tx *sql.Tx, snapshot *TaskArchiveSnapshot) error {
	snapshots, err := ArchiveEvidenceSnapshots(snapshot)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, v := range snapshots {
		if allowed[v.ID] {
			return errors.New("duplicate archived evidence snapshot")
		}
		allowed[v.ID] = true
		if err = InsertEvidenceSnapshotTx(tx, v); err != nil {
			return err
		}
	}
	rows, err := decodeArchiveRows(snapshot.Tables["finding_traffic_bindings"])
	if err != nil {
		return err
	}
	if len(rows) > 0 && snapshot.FormatVersion < 3 {
		return ErrTaskArchiveFormatMismatch
	}
	for _, row := range rows {
		fid, ok := jsonInt64(row["finding_id"])
		if !ok {
			return errors.New("invalid archived evidence finding id")
		}
		sid, _ := row["snapshot_id"].(string)
		role, _ := row["role"].(string)
		if !allowed[sid] || !ValidTrafficRole(role) {
			return errors.New("invalid archived evidence binding")
		}
		// SELECT EXISTS(…) 返回的是 PG 的 boolean；TiDB 侧统一用 COUNT(*) 判定，
		// 结果恒为数值，不需要再和 NULL 玩三值逻辑。
		var owned int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM findings WHERE id = ? AND task_id = ?`, fid, snapshot.TaskID).Scan(&owned); err != nil {
			return err
		}
		if owned == 0 {
			return fmt.Errorf("evidence references finding outside archived task: %d", fid)
		}
	}
	// PG 用 json_populate_recordset 一次吞下整个 JSON 记录集；TiDB 没有这个
	// 集合返回函数，改成显式列清单 + 绑定参数逐行落回（见 insertArchivedBindingTx）。
	for _, row := range rows {
		if err = insertArchivedBindingTx(tx, row); err != nil {
			return err
		}
	}
	if len(rows) > 0 {
		// 归档回来的快照重新被引用，必须撤销"孤儿"标记。原 WHERE EXISTS(子查询)
		// 在 TiDB 上用多表 UPDATE 表达：能被 JOIN 到的行恰好就是仍有绑定的那些。
		_, err = tx.Exec(`UPDATE traffic_evidence_snapshots s
JOIN finding_traffic_bindings b ON b.snapshot_id = s.id
SET s.unreferenced_at = NULL`)
	}
	return err
}

// insertArchivedBindingTx 落回一条归档的证据绑定。
//
// 归档里带 id 就原样保留（还原必须维持跨表引用不变），没有才退回自增；created_at
// 同理，解不出时间就让列的 CURRENT_TIMESTAMP(3) 默认值接手。
func insertArchivedBindingTx(tx *sql.Tx, row map[string]any) error {
	findingID, _ := jsonInt64(row["finding_id"])
	snapshotID, _ := row["snapshot_id"].(string)
	role, _ := row["role"].(string)
	note, _ := row["note"].(string)
	position, _ := jsonInt64(row["position"])

	columns := []string{"finding_id", "snapshot_id", "role", "note", "position"}
	values := []any{findingID, snapshotID, role, note, position}
	if id, ok := jsonInt64(row["id"]); ok && id > 0 {
		columns = append([]string{"id"}, columns...)
		values = append([]any{id}, values...)
	}
	if at, ok := archiveTimestamp(row["created_at"]); ok {
		columns = append(columns, "created_at")
		values = append(values, at.UTC())
	}
	_, err := tx.Exec(`INSERT INTO finding_traffic_bindings(`+strings.Join(columns, ",")+`)
VALUES(`+questionPlaceholders(len(values))+`)`, values...)
	return err
}

// archiveTimestamp 解析归档里的时间字段：归档由 encoding/json 写出，时间列是
// RFC3339 文本，老归档里也可能已是 SQL 字面量形态。
func archiveTimestamp(value any) (time.Time, bool) {
	text, ok := value.(string)
	if !ok {
		return time.Time{}, false
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999", "2006-01-02 15:04:05"} {
		if at, err := time.Parse(layout, text); err == nil {
			return at, true
		}
	}
	return time.Time{}, false
}

func normalizeArchivedFindingVersions(raw json.RawMessage) (json.RawMessage, error) {
	rows, err := decodeArchiveRows(raw)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		for _, key := range []string{"evidence_version", "report_evidence_version"} {
			if row[key] == nil {
				row[key] = 0
			}
		}
	}
	return json.Marshal(rows)
}
