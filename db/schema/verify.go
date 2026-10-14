package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Rows 是 database/sql.Rows 用到的方法集（便于 fake 与离线测试）。
// Err() 是 Task 7A 强制要求的必要修订，*sql.Rows 天然满足。
type Rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// Querier 是结构验收的数据库抽象（*sql.DB 经 DBQuerier 适配）。
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DBQuerier adapts *sql.DB to Querier.
type DBQuerier struct{ DB *sql.DB }

type sqlRowsWrapper struct {
	*sql.Rows
}

// Query implements Querier.
func (d DBQuerier) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	if d.DB == nil {
		return nil, errors.New("DBQuerier: underlying DB is nil")
	}
	r, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return sqlRowsWrapper{Rows: r}, nil
}

// Exec implements Querier.
func (d DBQuerier) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if d.DB == nil {
		return nil, errors.New("DBQuerier: underlying DB is nil")
	}
	return d.DB.ExecContext(ctx, query, args...)
}

// IsRealTiDB 是 VERSION() 守卫（spec §10.2：必须证明真实 TiDB，MySQL/SQLite 不能替代）。
// Task 7 扩展本文件时不得重命名或改签名。
func IsRealTiDB(version string) bool {
	return strings.Contains(version, "TiDB")
}

// qPKShape 查询单表物理 PK 的列名/类型（information_schema 口径）。
const qPKShape = `SELECT k.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE
FROM information_schema.KEY_COLUMN_USAGE k
JOIN information_schema.COLUMNS c
  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA
 AND c.TABLE_NAME = k.TABLE_NAME
 AND c.COLUMN_NAME = k.COLUMN_NAME
WHERE k.TABLE_SCHEMA = DATABASE()
  AND k.TABLE_NAME = ?
  AND k.CONSTRAINT_NAME = 'PRIMARY'`

// qFKCheckCount 统计全库 FOREIGN KEY/CHECK 约束数（C1/I3：必须为 0）。
const qFKCheckCount = `SELECT COUNT(*)
FROM information_schema.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = DATABASE()
  AND CONSTRAINT_TYPE IN ('FOREIGN KEY', 'CHECK')`

// CheckPrimaryKeys 断言（C1）：每表物理 PK 恰好 1 列，DATA_TYPE='bigint' 且
// COLUMN_TYPE 含 'unsigned'，列名与目录 PKColumn 一致。
// 逐表处理时，每次 Query 成功的 Rows 任何路径都 Close（不 defer 到外层大循环留到末尾）。
// Next 迭代结束后先检查 rows.Err()，任何已发生的错误与 Close 错误使用 errors.Join 合并，
// 完整保留原始错误的 errors.Is 关系。
func CheckPrimaryKeys(ctx context.Context, q Querier, tables []*Table) error {
	if q == nil {
		return errors.New("CheckPrimaryKeys: querier is nil")
	}
	for _, tb := range tables {
		if tb == nil {
			continue
		}
		rows, err := q.Query(ctx, qPKShape, tb.Name)
		if err != nil {
			return fmt.Errorf("table %s: %w", tb.Name, err)
		}

		pkCols, scanErr := func() ([]string, error) {
			var cols []string
			var loopErr error
			for rows.Next() {
				var col, dataType, colType string
				if err := rows.Scan(&col, &dataType, &colType); err != nil {
					loopErr = err
					break
				}
				cols = append(cols, col)
				if dataType != "bigint" {
					loopErr = fmt.Errorf("table %s: PK column %s DATA_TYPE=%s, want bigint", tb.Name, col, dataType)
					break
				}
				if !strings.Contains(colType, "unsigned") {
					loopErr = fmt.Errorf("table %s: PK column %s COLUMN_TYPE=%s, want unsigned", tb.Name, col, colType)
					break
				}
			}

			// 检查驱动迭代错误（Scan/loop 错误后也读取 Rows.Err 并 Join，保主要/迭代/Close 全部 errors.Is）
			if err := rows.Err(); err != nil {
				if loopErr != nil {
					loopErr = errors.Join(loopErr, err)
				} else {
					loopErr = err
				}
			}

			closeErr := rows.Close()
			if loopErr != nil && closeErr != nil {
				return nil, errors.Join(loopErr, closeErr)
			}
			if loopErr != nil {
				return nil, loopErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			return cols, nil
		}()

		if scanErr != nil {
			return scanErr
		}
		if len(pkCols) != 1 {
			return fmt.Errorf("table %s: %d PK columns %v, want exactly 1", tb.Name, len(pkCols), pkCols)
		}
		if pkCols[0] != tb.PKColumn {
			return fmt.Errorf("table %s: PK column %s, want %s", tb.Name, pkCols[0], tb.PKColumn)
		}
	}
	return nil
}

// CheckNoFKNoCheck 断言（C1/I3）：全库 FK/CHECK 零计数（dev 实测 TiDB 8.5.8 会真正
// 强制执行已声明外键，§11.1）。
// 任何路径都必须 Close Rows，多行、0 行、非零约束或 Scan/驱动错误均不可假绿。
func CheckNoFKNoCheck(ctx context.Context, q Querier) error {
	if q == nil {
		return errors.New("CheckNoFKNoCheck: querier is nil")
	}
	rows, err := q.Query(ctx, qFKCheckCount)
	if err != nil {
		return err
	}

	var rowCount int
	var n int
	var loopErr error

	for rows.Next() {
		rowCount++
		if rowCount == 1 {
			if err := rows.Scan(&n); err != nil {
				loopErr = err
				break
			}
		}
	}

	// Scan/loop 错误后也读取 Rows.Err 并 Join，保主要/迭代/Close 全部 errors.Is
	if err := rows.Err(); err != nil {
		if loopErr != nil {
			loopErr = errors.Join(loopErr, err)
		} else {
			loopErr = err
		}
	}

	closeErr := rows.Close()
	if loopErr != nil && closeErr != nil {
		return errors.Join(loopErr, closeErr)
	}
	if loopErr != nil {
		return loopErr
	}
	if closeErr != nil {
		return closeErr
	}

	if rowCount == 0 {
		return fmt.Errorf("FK/CHECK count: no row")
	}
	if rowCount > 1 {
		return fmt.Errorf("FK/CHECK count: multiple rows (%d)", rowCount)
	}
	if n != 0 {
		return fmt.Errorf("found %d FOREIGN KEY/CHECK constraints, want 0", n)
	}
	return nil
}
