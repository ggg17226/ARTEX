package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// DBLog is one persisted backend log row from the server_logs table.
type DBLog struct {
	ID        int64
	CreatedAt time.Time
	Level     string
	Tag       string
	Text      string
}

// InsertLog appends one log line and returns its auto-assigned id.
//
// TiDB 没有 `INSERT … RETURNING`：自增主键只能经 LastInsertId() 取回，并且必须
// 校验落在 [1, MaxInt64]（0/负值只可能是驱动或中间件异常，绝不能当成合法主键
// 交回调用方）；created_at 由列默认 CURRENT_TIMESTAMP(3) 盖章，Go 端不自造时间。
func (d *DB) InsertLog(level, tag, text string) (int64, error) {
	res, err := d.ExecContext(context.Background(),
		"INSERT INTO server_logs(level,tag,text) VALUES(?,?,?)",
		level, tag, text,
	)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, errors.New("server_logs: nil result after INSERT")
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("server_logs: LastInsertId after INSERT: %w", err)
	}
	if id <= 0 || uint64(id) > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("server_logs: INSERT returned out-of-range id %d", id)
	}
	return id, nil
}

// RecentLogs returns the most recent limit rows, oldest-first.
func (d *DB) RecentLogs(limit int) ([]*DBLog, error) {
	rows, err := d.QueryContext(context.Background(),
		`SELECT id, created_at, level, tag, text
		   FROM server_logs
		  ORDER BY id DESC
		  LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DBLog
	for rows.Next() {
		l := &DBLog{}
		if err := rows.Scan(&l.ID, &l.CreatedAt, &l.Level, &l.Tag, &l.Text); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	// Reverse to oldest-first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// ListLogsBefore returns up to limit rows with id < beforeID, oldest-first.
func (d *DB) ListLogsBefore(beforeID int64, limit int) ([]*DBLog, error) {
	rows, err := d.QueryContext(context.Background(),
		`SELECT id, created_at, level, tag, text
		   FROM server_logs
		  WHERE id < ?
		  ORDER BY id DESC
		  LIMIT ?`, beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DBLog
	for rows.Next() {
		l := &DBLog{}
		if err := rows.Scan(&l.ID, &l.CreatedAt, &l.Level, &l.Tag, &l.Text); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	// Reverse to oldest-first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}
