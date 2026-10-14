package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

// ---- Fake 基础设施（仅在 _test.go，不修改现有 verify_test.go 中的 fake） ----

// probeFakeRows 模拟 database/sql Rows，支持动态列扫描与任意类型的 Scan 适配。
// 特别支持 *any 扫描以承载 SHOW TABLE 等未知/动态结果集的单行扫描。
type probeFakeRows struct {
	mu         sync.Mutex
	cols       []string
	data       [][]any
	pos        int
	closed     bool
	errVal     error
	closeErr   error
	scanErr    error
	columnsErr error
}

func newProbeFakeRows(cols []string, data [][]any) *probeFakeRows {
	return &probeFakeRows{
		cols: cols,
		data: data,
		pos:  0,
	}
}

func (r *probeFakeRows) Columns() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.columnsErr != nil {
		return nil, r.columnsErr
	}
	return r.cols, nil
}

func (r *probeFakeRows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.pos < len(r.data) {
		r.pos++
		return true
	}
	return false
}

func (r *probeFakeRows) Scan(dest ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("scan on closed rows")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos == 0 || r.pos > len(r.data) {
		return fmt.Errorf("scan called at invalid cursor position: %d (data len: %d)", r.pos, len(r.data))
	}
	row := r.data[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan dest count mismatch: got %d, row has %d", len(dest), len(row))
	}
	for i, val := range row {
		if err := assignProbeFakeValue(dest[i], val); err != nil {
			return fmt.Errorf("scan col %d: %w", i, err)
		}
	}
	return nil
}

func (r *probeFakeRows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.closeErr
}

func (r *probeFakeRows) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errVal
}

func assignProbeFakeValue(dest any, src any) error {
	switch d := dest.(type) {
	case *any:
		*d = src
		return nil
	case *string:
		switch s := src.(type) {
		case string:
			*d = s
			return nil
		case []byte:
			*d = string(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *string", src)
		}
	case *[]byte:
		switch s := src.(type) {
		case []byte:
			*d = s
			return nil
		case string:
			*d = []byte(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *[]byte", src)
		}
	case *int:
		switch s := src.(type) {
		case int:
			*d = s
			return nil
		case int64:
			*d = int(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *int", src)
		}
	case *int64:
		switch s := src.(type) {
		case int:
			*d = int64(s)
			return nil
		case int64:
			*d = s
			return nil
		case uint64:
			*d = int64(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
	case *uint64:
		switch s := src.(type) {
		case uint64:
			*d = s
			return nil
		case int:
			if s < 0 {
				return fmt.Errorf("negative int %d into *uint64", s)
			}
			*d = uint64(s)
			return nil
		case int64:
			if s < 0 {
				return fmt.Errorf("negative int64 %d into *uint64", s)
			}
			*d = uint64(s)
			return nil
		default:
			return fmt.Errorf("cannot scan %T into *uint64", src)
		}
	default:
		return fmt.Errorf("unsupported dest type %T", dest)
	}
}

type probeQueryCall struct {
	query string
	args  []any
}

type probeExecCall struct {
	query string
	args  []any
}

type probeFakeResult struct {
	affected int64
	lastID   int64
}

func (r probeFakeResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r probeFakeResult) RowsAffected() (int64, error) { return r.affected, nil }

type probeFakeQuerier struct {
	mu           sync.Mutex
	queries      []probeQueryCall
	execs        []probeExecCall
	queryHandler func(ctx context.Context, query string, args ...any) (Rows, error)
	execHandler  func(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (q *probeFakeQuerier) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	q.mu.Lock()
	q.queries = append(q.queries, probeQueryCall{query: query, args: args})
	handler := q.queryHandler
	q.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args...)
	}
	return nil, fmt.Errorf("probeFakeQuerier: unexpected query %q", query)
}

func (q *probeFakeQuerier) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	q.mu.Lock()
	q.execs = append(q.execs, probeExecCall{query: query, args: args})
	handler := q.execHandler
	q.mu.Unlock()

	if handler != nil {
		return handler(ctx, query, args...)
	}
	return nil, fmt.Errorf("probeFakeQuerier: unexpected exec %q", query)
}

// -----------------------------------------------------------------------------
// 1. CheckAutoIncStep 测试用例
// -----------------------------------------------------------------------------

func TestCheckAutoIncStep_Success_Baseline17To19(t *testing.T) {
	ctx := context.Background()
	showCount := 0
	var insertedNames []any
	var deletedNames []any

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
				showCount++
				var val uint64
				if showCount == 1 {
					val = 17
				} else {
					val = 19
				}
				return newProbeFakeRows(
					[]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"},
					[][]any{
						{"test_db", "locks", "row_id", val, "AUTO_INCREMENT"},
					},
				), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `locks` (`name`) VALUES (?), (?)") {
				if len(args) != 2 {
					return nil, fmt.Errorf("expected 2 insert args, got %d", len(args))
				}
				nameA, okA := args[0].(string)
				nameB, okB := args[1].(string)
				if !okA || !okB {
					return nil, fmt.Errorf("insert args not string: %T, %T", args[0], args[1])
				}
				if len(nameA) > 64 || len(nameB) > 64 {
					return nil, fmt.Errorf("marker length > 64: len(A)=%d, len(B)=%d", len(nameA), len(nameB))
				}
				if nameA == nameB {
					return nil, fmt.Errorf("marker A and B collision: %q", nameA)
				}
				insertedNames = append(insertedNames, nameA, nameB)
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "DELETE FROM `locks` WHERE `name` IN (?, ?)") {
				if len(args) != 2 {
					return nil, fmt.Errorf("expected 2 delete args, got %d", len(args))
				}
				deletedNames = append(deletedNames, args...)
				return probeFakeResult{affected: 2}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if showCount != 2 {
		t.Fatalf("expected 2 SHOW queries, got %d", showCount)
	}
	if len(insertedNames) != 2 {
		t.Fatalf("expected 2 inserted names, got %d", len(insertedNames))
	}
	if len(deletedNames) != 2 {
		t.Fatalf("expected 2 deleted names, got %d", len(deletedNames))
	}
	if deletedNames[0] != insertedNames[0] || deletedNames[1] != insertedNames[1] {
		t.Errorf("cleanup names mismatch: inserted %v, deleted %v", insertedNames, deletedNames)
	}
}

func TestCheckAutoIncStep_ColumnPermutationsAndFilters(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		baseVal  uint64
		cols     []string
		rowsData [][]any
	}{
		{
			name:    "next_row_id_lowercase_columns",
			baseVal: 100,
			cols:    []string{"db_name", "table_name", "column_name", "next_row_id", "id_type"},
			rowsData: [][]any{
				{"test_db", "locks", "row_id", "100", "AUTO_INCREMENT"},
			},
		},
		{
			name:    "column_order_swapped_id_type_first",
			baseVal: 100,
			cols:    []string{"ID_TYPE", "NEXT_GLOBAL_ROW_ID", "TABLE_NAME", "COLUMN_NAME"},
			rowsData: [][]any{
				{"AUTO_INCREMENT", []byte("100"), "locks", "row_id"},
			},
		},
		{
			name:    "f14_highbyte_max_uint64_minus_2_success",
			baseVal: math.MaxUint64 - 2, // 18446744073709551613 -> +2 -> MaxUint64 (18446744073709551615) 合法成功
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{"locks", "row_id", []byte("18446744073709551613")},
			},
		},
		{
			name:    "f14_highbyte_max_uint64_minus_2_rawbytes_success",
			baseVal: math.MaxUint64 - 2,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{"locks", "row_id", sql.RawBytes("18446744073709551613")},
			},
		},
		{
			name:    "filter_out_rowid_keep_auto_increment",
			baseVal: 100,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_GLOBAL_ROW_ID"},
			rowsData: [][]any{
				{"locks", "_tidb_rowid", "_TIDB_ROWID", uint64(50)},
				{"locks", "row_id", "AUTO_INCREMENT", uint64(100)},
			},
		},
		{
			name:    "filter_table_locks_when_multiple_tables_in_result",
			baseVal: 100,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{"other_table", "row_id", "AUTO_INCREMENT", uint64(999)},
				{"locks", "row_id", "AUTO_INCREMENT", uint64(100)},
			},
		},
		{
			name:    "all_filter_and_value_columns_bytes_17_to_19",
			baseVal: 17,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"},
			rowsData: [][]any{
				{[]byte("other_table"), []byte("row_id"), []byte("99"), []byte("AUTO_INCREMENT")},
				{[]byte("locks"), []byte("sub_id"), []byte("88"), []byte("AUTO_INCREMENT")},
				{[]byte("locks"), []byte("_tidb_rowid"), []byte("77"), []byte("AUTO_INCREMENT")},
				{[]byte("locks"), []byte("row_id"), []byte("17"), []byte("AUTO_INCREMENT")},
			},
		},
		{
			name:    "sql_raw_bytes_filters_and_value",
			baseVal: 17,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{sql.RawBytes("other_table"), sql.RawBytes("row_id"), sql.RawBytes("AUTO_INCREMENT"), sql.RawBytes("999")},
				{sql.RawBytes("locks"), sql.RawBytes("sub_id"), sql.RawBytes("AUTO_INCREMENT"), sql.RawBytes("888")},
				{sql.RawBytes("locks"), sql.RawBytes("row_id"), sql.RawBytes("AUTO_INCREMENT"), sql.RawBytes("17")},
			},
		},
		{
			name:    "nil_in_other_rows_does_not_crash",
			baseVal: 17,
			cols:    []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{nil, nil, nil, nil},
				{"locks", "row_id", "AUTO_INCREMENT", uint64(17)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			showCount := 0
			var insertedMarkers []string
			var deletedMarkers []any

			base := tc.baseVal
			if base == 0 {
				base = 100
			}

			fq := &probeFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
						showCount++
						val := base
						if showCount == 2 {
							val = base + 2
						}
						// 根据 showCount 动态生成第二轮数据
						data := make([][]any, len(tc.rowsData))
						for i, r := range tc.rowsData {
							rowCopy := make([]any, len(r))
							copy(rowCopy, r)
							// 如果某列是 target 列且值为 base，在第二轮置为 base + 2
							for j, colName := range tc.cols {
								upperCol := strings.ToUpper(colName)
								if (upperCol == "NEXT_GLOBAL_ROW_ID" || upperCol == "NEXT_ROW_ID") && i == len(tc.rowsData)-1 {
									switch rowCopy[j].(type) {
									case string:
										rowCopy[j] = fmt.Sprintf("%d", val)
									case []byte:
										rowCopy[j] = []byte(fmt.Sprintf("%d", val))
									case sql.RawBytes:
										rowCopy[j] = sql.RawBytes(fmt.Sprintf("%d", val))
									default:
										rowCopy[j] = val
									}
								}
							}
							data[i] = rowCopy
						}
						return newProbeFakeRows(tc.cols, data), nil
					}
					return nil, fmt.Errorf("unexpected query: %s", query)
				},
				execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
					if strings.HasPrefix(query, "INSERT INTO `locks` (`name`) VALUES (?), (?)") {
						if len(args) != 2 {
							return nil, fmt.Errorf("expected 2 insert args, got %d", len(args))
						}
						nameA, okA := args[0].(string)
						nameB, okB := args[1].(string)
						if !okA || !okB {
							return nil, fmt.Errorf("insert args not string: %T, %T", args[0], args[1])
						}
						if len(nameA) > 64 || len(nameB) > 64 {
							return nil, fmt.Errorf("marker length > 64: %d, %d", len(nameA), len(nameB))
						}
						if nameA == nameB {
							return nil, fmt.Errorf("marker collision: %s", nameA)
						}
						insertedMarkers = append(insertedMarkers, nameA, nameB)
						return probeFakeResult{affected: 2}, nil
					}
					if strings.HasPrefix(query, "DELETE FROM `locks` WHERE `name` IN (?, ?)") {
						if len(args) != 2 {
							return nil, fmt.Errorf("expected 2 delete args, got %d", len(args))
						}
						deletedMarkers = append(deletedMarkers, args...)
						return probeFakeResult{affected: 2}, nil
					}
					return nil, fmt.Errorf("unexpected exec: %s", query)
				},
			}

			err := CheckAutoIncStep(ctx, fq)
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
			if showCount != 2 {
				t.Fatalf("expected 2 SHOW queries, got %d", showCount)
			}
			if len(insertedMarkers) != 2 {
				t.Fatalf("expected 2 inserted markers, got %d", len(insertedMarkers))
			}
			if len(deletedMarkers) != 2 {
				t.Fatalf("expected 2 deleted markers, got %d", len(deletedMarkers))
			}
			if deletedMarkers[0] != insertedMarkers[0] || deletedMarkers[1] != insertedMarkers[1] {
				t.Errorf("cleanup markers mismatch: inserted %v, deleted %v", insertedMarkers, deletedMarkers)
			}
		})
	}
}

func TestCheckAutoIncStep_SHOWPhaseErrorInjection(t *testing.T) {
	ctx := context.Background()

	scanSentinel := errors.New("sentinel scan error in SHOW")
	rowsErrSentinel := errors.New("sentinel rows.Err() in SHOW")
	closeErrSentinel := errors.New("sentinel rows.Close() in SHOW")
	columnsSentinel := errors.New("sentinel rows.Columns() error in SHOW")

	validCols := []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_ROW_ID"}
	validDataPhase1 := [][]any{
		{"locks", "row_id", "AUTO_INCREMENT", uint64(17)},
	}
	validDataPhase2 := [][]any{
		{"locks", "row_id", "AUTO_INCREMENT", uint64(19)},
	}

	tests := []struct {
		name          string
		phase         int // 1 = first SHOW, 2 = second SHOW
		columnsErr    error
		scanErr       error
		rowsErr       error
		closeErr      error
		ambiguousCols bool
		expectedErr   error
		expectJoin    []error
		expectClosed  bool
		expectInsert  bool
		expectCleanup bool
	}{
		{
			name:          "phase1_columns_error_with_close_error",
			phase:         1,
			columnsErr:    columnsSentinel,
			closeErr:      closeErrSentinel,
			expectJoin:    []error{columnsSentinel, closeErrSentinel},
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase1_ambiguous_value_columns_with_close_error",
			phase:         1,
			closeErr:      closeErrSentinel,
			ambiguousCols: true,
			expectJoin:    []error{closeErrSentinel},
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase1_scan_error",
			phase:         1,
			scanErr:       scanSentinel,
			expectedErr:   scanSentinel,
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase1_rows_err",
			phase:         1,
			rowsErr:       rowsErrSentinel,
			expectedErr:   rowsErrSentinel,
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase1_close_error",
			phase:         1,
			closeErr:      closeErrSentinel,
			expectedErr:   closeErrSentinel,
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase1_dual_error_rows_err_and_close_err",
			phase:         1,
			rowsErr:       rowsErrSentinel,
			closeErr:      closeErrSentinel,
			expectJoin:    []error{rowsErrSentinel, closeErrSentinel},
			expectClosed:  true,
			expectInsert:  false,
			expectCleanup: false,
		},
		{
			name:          "phase2_columns_error_with_close_error_cleans_up_2_owned_markers",
			phase:         2,
			columnsErr:    columnsSentinel,
			closeErr:      closeErrSentinel,
			expectJoin:    []error{columnsSentinel, closeErrSentinel},
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_ambiguous_value_columns_with_close_error_cleans_up_2_owned_markers",
			phase:         2,
			closeErr:      closeErrSentinel,
			ambiguousCols: true,
			expectJoin:    []error{closeErrSentinel},
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_columns_error_cleans_up_2_owned_markers",
			phase:         2,
			columnsErr:    columnsSentinel,
			expectedErr:   columnsSentinel,
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_scan_error_cleans_up_2_owned_markers",
			phase:         2,
			scanErr:       scanSentinel,
			expectedErr:   scanSentinel,
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_rows_err_cleans_up_2_owned_markers",
			phase:         2,
			rowsErr:       rowsErrSentinel,
			expectedErr:   rowsErrSentinel,
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_close_error_cleans_up_2_owned_markers",
			phase:         2,
			closeErr:      closeErrSentinel,
			expectedErr:   closeErrSentinel,
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
		{
			name:          "phase2_dual_error_scan_and_close_cleans_up_2_owned_markers",
			phase:         2,
			scanErr:       scanSentinel,
			closeErr:      closeErrSentinel,
			expectJoin:    []error{scanSentinel, closeErrSentinel},
			expectClosed:  true,
			expectInsert:  true,
			expectCleanup: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			showCount := 0
			var insertedMarkers []string
			var deletedMarkers []any
			var targetRows *probeFakeRows

			fq := &probeFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
						showCount++
						var r *probeFakeRows
						if showCount == 1 {
							cols := validCols
							if tc.phase == 1 && tc.ambiguousCols {
								cols = []string{"NEXT_GLOBAL_ROW_ID", "NEXT_ROW_ID", "ID_TYPE"}
							}
							if tc.phase == 1 {
								r = newProbeFakeRows(cols, validDataPhase1)
								r.columnsErr = tc.columnsErr
								r.scanErr = tc.scanErr
								r.errVal = tc.rowsErr
								r.closeErr = tc.closeErr
								targetRows = r
							} else {
								r = newProbeFakeRows(cols, validDataPhase1)
							}
						} else if showCount == 2 {
							cols := validCols
							if tc.phase == 2 && tc.ambiguousCols {
								cols = []string{"NEXT_GLOBAL_ROW_ID", "NEXT_ROW_ID", "ID_TYPE"}
							}
							if tc.phase == 2 {
								r = newProbeFakeRows(cols, validDataPhase2)
								r.columnsErr = tc.columnsErr
								r.scanErr = tc.scanErr
								r.errVal = tc.rowsErr
								r.closeErr = tc.closeErr
								targetRows = r
							} else {
								r = newProbeFakeRows(cols, validDataPhase2)
							}
						}
						return r, nil
					}
					return nil, fmt.Errorf("unexpected query: %s", query)
				},
				execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
					if strings.HasPrefix(query, "INSERT INTO `locks` (`name`) VALUES (?), (?)") {
						if len(args) != 2 {
							return nil, fmt.Errorf("expected 2 insert args, got %d", len(args))
						}
						nameA, okA := args[0].(string)
						nameB, okB := args[1].(string)
						if !okA || !okB {
							return nil, fmt.Errorf("insert args not string: %T, %T", args[0], args[1])
						}
						insertedMarkers = append(insertedMarkers, nameA, nameB)
						return probeFakeResult{affected: 2}, nil
					}
					if strings.HasPrefix(query, "DELETE FROM `locks` WHERE `name` IN (?, ?)") {
						deletedMarkers = append(deletedMarkers, args...)
						return probeFakeResult{affected: 2}, nil
					}
					return nil, fmt.Errorf("unexpected exec: %s", query)
				},
			}

			err := CheckAutoIncStep(ctx, fq)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tc.expectedErr != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("expected errors.Is(err, %v), got: %v", tc.expectedErr, err)
				}
			}
			for _, expected := range tc.expectJoin {
				if !errors.Is(err, expected) {
					t.Errorf("expected errors.Is(err, %v) in combined error, got: %v", expected, err)
				}
			}

			if targetRows != nil && tc.expectClosed {
				targetRows.mu.Lock()
				closed := targetRows.closed
				targetRows.mu.Unlock()
				if !closed {
					t.Errorf("expected targetRows to be closed")
				}
			}

			if tc.expectInsert {
				if len(insertedMarkers) != 2 {
					t.Fatalf("expected 2 inserted markers, got %d", len(insertedMarkers))
				}
			} else {
				if len(insertedMarkers) != 0 {
					t.Fatalf("expected 0 inserted markers in phase 1 error, got %d", len(insertedMarkers))
				}
			}

			if tc.expectCleanup {
				if len(deletedMarkers) != 2 {
					t.Fatalf("expected 2 deleted markers in cleanup, got %d", len(deletedMarkers))
				}
				if deletedMarkers[0] != insertedMarkers[0] || deletedMarkers[1] != insertedMarkers[1] {
					t.Errorf("deleted markers %v did not match inserted markers %v", deletedMarkers, insertedMarkers)
				}
			} else {
				if len(deletedMarkers) != 0 {
					t.Fatalf("expected 0 deleted markers, got %d", len(deletedMarkers))
				}
			}
		})
	}
}

func TestCheckAutoIncStep_NegativeCases_SHOWParsing(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		cols     []string
		rowsData [][]any
		closeErr error
		wantErr  string
	}{
		{
			name:     "corrupted_non_target_column_name_not_row_id",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE", "NEXT_ROW_ID"},
			rowsData: [][]any{{"locks", "extra_id", "AUTO_INCREMENT", uint64(10)}},
			wantErr:  "no target row",
		},
		{
			name:     "missing_value_column_with_close_error",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE"},
			rowsData: [][]any{{"locks", "row_id", "AUTO_INCREMENT"}},
			closeErr: errors.New("close socket error sentinel"),
			wantErr:  "no value column",
		},
		{
			name:     "missing_value_column",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "ID_TYPE"},
			rowsData: [][]any{{"locks", "row_id", "AUTO_INCREMENT"}},
			wantErr:  "no value column",
		},
		{
			name:     "ambiguous_value_columns",
			cols:     []string{"NEXT_GLOBAL_ROW_ID", "NEXT_ROW_ID", "ID_TYPE"},
			rowsData: [][]any{{uint64(1), uint64(2), "AUTO_INCREMENT"}},
			wantErr:  "ambiguous value columns",
		},
		{
			name:     "no_matching_target_row",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{{"other_table", "id", uint64(10)}},
			wantErr:  "no target row",
		},
		{
			name: "multiple_target_rows_ambiguity",
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{
				{"locks", "row_id", uint64(10)},
				{"locks", "row_id", uint64(20)},
			},
			wantErr: "multiple target rows",
		},
		{
			name:     "negative_value",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{{"locks", "row_id", int64(-5)}},
			wantErr:  "negative",
		},
		{
			name:     "corrupted_non_numeric_string",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{{"locks", "row_id", "not_a_number"}},
			wantErr:  "invalid syntax",
		},
		{
			name:     "f14_21digit_overflow_string_fails_before_insert",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{{"locks", "row_id", "999999999999999999999"}}, // 21位溢出文本
			wantErr:  "value out of range",
		},
		{
			name:     "f14_21digit_overflow_bytes_fails_before_insert",
			cols:     []string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID"},
			rowsData: [][]any{{"locks", "row_id", []byte("999999999999999999999")}}, // 21位溢出字节
			wantErr:  "value out of range",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fq := &probeFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
						r := newProbeFakeRows(tc.cols, tc.rowsData)
						r.closeErr = tc.closeErr
						return r, nil
					}
					return nil, fmt.Errorf("unexpected query: %s", query)
				},
			}

			err := CheckAutoIncStep(ctx, fq)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
			if tc.closeErr != nil {
				if !errors.Is(err, tc.closeErr) {
					t.Errorf("expected errors.Is(err, tc.closeErr), got: %v", err)
				}
			}
		})
	}
}

func TestCheckAutoIncStep_MaxUint64Overflow_FailsBeforeInsert(t *testing.T) {
	ctx := context.Background()
	var insertAttempted bool

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
				// baseline 为 ^uint64(0) 即 18446744073709551615
				return newProbeFakeRows(
					[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
					[][]any{
						{"locks", "row_id", ^uint64(0), "AUTO_INCREMENT"},
					},
				), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT") {
				insertAttempted = true
			}
			return probeFakeResult{affected: 1}, nil
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Errorf("expected overflow error, got: %v", err)
	}
	if insertAttempted {
		t.Fatal("INSERT must not be executed when baseline overflows uint64+2")
	}
}

func TestCheckAutoIncStep_StepMismatch_CleansUpOwned(t *testing.T) {
	ctx := context.Background()
	showCount := 0
	var deletedNames []any

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
				showCount++
				val := uint64(10)
				if showCount == 2 {
					val = 11 // 应当为 12，但返回 11
				}
				return newProbeFakeRows(
					[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
					[][]any{
						{"locks", "row_id", val, "AUTO_INCREMENT"},
					},
				), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT") {
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "DELETE") {
				deletedNames = append(deletedNames, args...)
				return probeFakeResult{affected: 2}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err == nil {
		t.Fatal("expected step mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "step mismatch") && !strings.Contains(err.Error(), "want 12") {
		t.Errorf("expected step mismatch error, got: %v", err)
	}
	if len(deletedNames) != 2 {
		t.Fatalf("expected 2 deleted names in cleanup after step mismatch, got %d", len(deletedNames))
	}
}

func TestCheckAutoIncStep_InsertFailed_NoOwnedCleanup(t *testing.T) {
	ctx := context.Background()
	var deleteAttempted bool
	insertSentinel := errors.New("insert disk full sentinel")

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			return newProbeFakeRows(
				[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
				[][]any{{"locks", "row_id", uint64(5), "AUTO_INCREMENT"}},
			), nil
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT") {
				return nil, insertSentinel
			}
			if strings.HasPrefix(query, "DELETE") {
				deleteAttempted = true
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err == nil {
		t.Fatal("expected insert error, got nil")
	}
	if !errors.Is(err, insertSentinel) {
		t.Errorf("expected errors.Is(err, insertSentinel), got: %v", err)
	}
	if deleteAttempted {
		t.Fatal("no DELETE should be attempted when INSERT fails (no owned markers)")
	}
}

func TestCheckAutoIncStep_CancelledContext_IndependentCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	showCount := 0
	var cleanupCtx context.Context

	fq := &probeFakeQuerier{
		queryHandler: func(c context.Context, query string, args ...any) (Rows, error) {
			showCount++
			if showCount == 1 {
				return newProbeFakeRows(
					[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
					[][]any{{"locks", "row_id", uint64(10), "AUTO_INCREMENT"}},
				), nil
			}
			// 第二次 SHOW 前主 ctx 取消
			cancel()
			return nil, c.Err()
		},
		execHandler: func(c context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT") {
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "DELETE") {
				cleanupCtx = c
				// 在 Exec 执行时，c 必须是独立 context，绝不能是已经被 cancel 的主 ctx
				if c == ctx {
					t.Errorf("cleanup was called with canceled main context instead of independent context")
				}
				if c.Err() != nil {
					t.Errorf("cleanup context was canceled before execution: %v", c.Err())
				}
				return probeFakeResult{affected: 2}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
	if cleanupCtx == nil {
		t.Fatal("cleanup DELETE was not executed")
	}
}

func TestCheckAutoIncStep_CleanupError_PreservesMainError(t *testing.T) {
	ctx := context.Background()
	showCount := 0
	secondShowErr := errors.New("second show timeout sentinel")
	cleanupErr := errors.New("delete network failure sentinel")

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			showCount++
			if showCount == 1 {
				return newProbeFakeRows(
					[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
					[][]any{{"locks", "row_id", uint64(10), "AUTO_INCREMENT"}},
				), nil
			}
			return nil, secondShowErr
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT") {
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "DELETE") {
				return nil, cleanupErr
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAutoIncStep(ctx, fq)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, secondShowErr) {
		t.Errorf("expected errors.Is(err, secondShowErr), got: %v", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Errorf("expected errors.Is(err, cleanupErr), got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 2. CheckAsciiCollision 测试用例
// -----------------------------------------------------------------------------

func TestCheckAsciiCollision_Success(t *testing.T) {
	ctx := context.Background()
	var insertedKeys []string
	var deletedKeys []any

	fq := &probeFakeQuerier{
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `schema_meta` (`key`, `value`, `updated_at`) VALUES (?, ?, NOW(3))") {
				if len(args) != 2 {
					return nil, fmt.Errorf("expected 2 insert args (key, value), got %d", len(args))
				}
				key, ok := args[0].(string)
				if !ok {
					return nil, fmt.Errorf("key not string: %T", args[0])
				}
				if len(key) > 64 {
					return nil, fmt.Errorf("key length > 64: %d", len(key))
				}
				insertedKeys = append(insertedKeys, key)
				return probeFakeResult{affected: 1}, nil
			}
			if strings.HasPrefix(query, "DELETE FROM `schema_meta` WHERE `key` IN (?, ?)") {
				deletedKeys = append(deletedKeys, args...)
				return probeFakeResult{affected: 2}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAsciiCollision(ctx, fq)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if len(insertedKeys) != 2 {
		t.Fatalf("expected 2 inserted keys, got %d", len(insertedKeys))
	}
	// 两 key 唯一区别大小写
	if strings.ToLower(insertedKeys[0]) != strings.ToLower(insertedKeys[1]) {
		t.Errorf("keys differ beyond case: %q vs %q", insertedKeys[0], insertedKeys[1])
	}
	if insertedKeys[0] == insertedKeys[1] {
		t.Errorf("keys are identical, want case difference: %q", insertedKeys[0])
	}

	if len(deletedKeys) != 2 {
		t.Fatalf("expected 2 deleted keys, got %d", len(deletedKeys))
	}
	if deletedKeys[0] != insertedKeys[0] || deletedKeys[1] != insertedKeys[1] {
		t.Errorf("deleted keys mismatch: %v vs %v", deletedKeys, insertedKeys)
	}
}

func TestCheckAsciiCollision_SecondKeyCollision1062_CleansUpFirstKey(t *testing.T) {
	ctx := context.Background()
	var insertCount int
	var deletedKeys []any
	err1062 := errors.New("Error 1062 (23000): Duplicate entry 'abc_probe' for key 'uq_schema_meta_key'")

	fq := &probeFakeQuerier{
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `schema_meta`") {
				insertCount++
				if insertCount == 1 {
					return probeFakeResult{affected: 1}, nil
				}
				return nil, err1062
			}
			if strings.HasPrefix(query, "DELETE FROM `schema_meta` WHERE `key` IN (?)") {
				deletedKeys = append(deletedKeys, args...)
				return probeFakeResult{affected: 1}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAsciiCollision(ctx, fq)
	if err == nil {
		t.Fatal("expected 1062 error, got nil")
	}
	if !errors.Is(err, err1062) {
		t.Errorf("expected errors.Is(err, err1062), got: %v", err)
	}
	// 必须清理第一条成功插入的 key，且不删未成功的第二条 key
	if len(deletedKeys) != 1 {
		t.Fatalf("expected 1 deleted key in cleanup, got %d (%v)", len(deletedKeys), deletedKeys)
	}
}

func TestCheckAsciiCollision_FirstKeyFailed_NoCleanup(t *testing.T) {
	ctx := context.Background()
	var deleteAttempted bool
	firstErr := errors.New("first insert error")

	fq := &probeFakeQuerier{
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `schema_meta`") {
				return nil, firstErr
			}
			if strings.HasPrefix(query, "DELETE") {
				deleteAttempted = true
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	err := CheckAsciiCollision(ctx, fq)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, firstErr) {
		t.Errorf("expected errors.Is(err, firstErr), got: %v", err)
	}
	if deleteAttempted {
		t.Fatal("no cleanup should be attempted when first insert fails")
	}
}

func TestCheckAsciiCollision_CancelledContext_IndependentCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	insertCount := 0
	var insertedKeys []string
	var deletedKeys []any
	var cleanupCtx context.Context

	fq := &probeFakeQuerier{
		execHandler: func(c context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `schema_meta`") {
				insertCount++
				if insertCount == 1 {
					if len(args) > 0 {
						if k, ok := args[0].(string); ok {
							insertedKeys = append(insertedKeys, k)
						}
					}
					return probeFakeResult{affected: 1}, nil
				}
				cancel()
				return nil, c.Err()
			}
			if strings.HasPrefix(query, "DELETE FROM `schema_meta`") {
				cleanupCtx = c
				deletedKeys = append(deletedKeys, args...)
				if c == ctx {
					t.Errorf("cleanup was called with canceled main context")
				}
				if c.Err() != nil {
					t.Errorf("cleanup context was canceled before execution: %v", c.Err())
				}
				return probeFakeResult{affected: 1}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	if err := CheckAsciiCollision(ctx, fq); err == nil {
		t.Fatal("expected context canceled error, got nil")
	} else {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
		}
	}
	if cleanupCtx == nil {
		t.Fatal("cleanup DELETE was not executed")
	}
	if len(deletedKeys) != 1 {
		t.Fatalf("expected exactly 1 deleted key in cleanup, got %d (%v)", len(deletedKeys), deletedKeys)
	}
	if len(insertedKeys) != 1 || deletedKeys[0] != insertedKeys[0] {
		t.Errorf("expected deleted key %v to match first inserted key %v", deletedKeys, insertedKeys)
	}
}

// -----------------------------------------------------------------------------
// 3. VerifyStructure 组合测试（4 层按顺序执行，首错停止）
// -----------------------------------------------------------------------------

func TestVerifyStructure_Success_All4LayersSequential(t *testing.T) {
	ctx := context.Background()
	var executedOps []string

	fq := &probeFakeQuerier{
		queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
			if query == qPKShape {
				executedOps = append(executedOps, "PK")
				return newProbeFakeRows(
					[]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
					[][]any{{"id", "bigint", "bigint unsigned"}},
				), nil
			}
			if query == qFKCheckCount {
				executedOps = append(executedOps, "FKCHECK")
				return newProbeFakeRows([]string{"COUNT(*)"}, [][]any{{0}}), nil
			}
			if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
				executedOps = append(executedOps, "SHOW_AUTO_INC")
				return newProbeFakeRows(
					[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
					[][]any{{"locks", "row_id", uint64(10), "AUTO_INCREMENT"}},
				), nil
			}
			return nil, fmt.Errorf("unexpected query: %s", query)
		},
		execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if strings.HasPrefix(query, "INSERT INTO `locks`") {
				executedOps = append(executedOps, "INSERT_LOCKS")
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "DELETE FROM `locks`") {
				executedOps = append(executedOps, "DELETE_LOCKS")
				return probeFakeResult{affected: 2}, nil
			}
			if strings.HasPrefix(query, "INSERT INTO `schema_meta`") {
				executedOps = append(executedOps, "INSERT_ASCII")
				return probeFakeResult{affected: 1}, nil
			}
			if strings.HasPrefix(query, "DELETE FROM `schema_meta`") {
				executedOps = append(executedOps, "DELETE_ASCII")
				return probeFakeResult{affected: 2}, nil
			}
			return nil, fmt.Errorf("unexpected exec: %s", query)
		},
	}

	// 假定 2nd show auto inc 会递增 2
	showCall := 0
	origHandler := fq.queryHandler
	fq.queryHandler = func(ctx context.Context, query string, args ...any) (Rows, error) {
		if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
			showCall++
			val := uint64(10)
			if showCall == 2 {
				val = 12
			}
			executedOps = append(executedOps, "SHOW_AUTO_INC")
			return newProbeFakeRows(
				[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
				[][]any{{"locks", "row_id", val, "AUTO_INCREMENT"}},
			), nil
		}
		return origHandler(ctx, query, args...)
	}

	tables := []*Table{{Name: "t1", PKColumn: "id"}}
	err := VerifyStructure(ctx, fq, tables)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	// 验证层级执行顺序：PK (1) -> FKCHECK (2) -> AUTO_INC (3) -> ASCII (4)
	firstPK := -1
	firstFK := -1
	firstAutoInc := -1
	firstAscii := -1
	for i, op := range executedOps {
		if op == "PK" && firstPK == -1 {
			firstPK = i
		}
		if op == "FKCHECK" && firstFK == -1 {
			firstFK = i
		}
		if op == "SHOW_AUTO_INC" && firstAutoInc == -1 {
			firstAutoInc = i
		}
		if op == "INSERT_ASCII" && firstAscii == -1 {
			firstAscii = i
		}
	}

	if firstPK == -1 || firstFK == -1 || firstAutoInc == -1 || firstAscii == -1 {
		t.Fatalf("missing layer execution: PK=%d, FK=%d, AutoInc=%d, ASCII=%d (all ops: %v)", firstPK, firstFK, firstAutoInc, firstAscii, executedOps)
	}
	if !(firstPK < firstFK && firstFK < firstAutoInc && firstAutoInc < firstAscii) {
		t.Errorf("layers executed out of order: PK=%d, FK=%d, AutoInc=%d, ASCII=%d", firstPK, firstFK, firstAutoInc, firstAscii)
	}
}

func TestVerifyStructure_StopsAtFirstError(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		failAt     string // "PK", "FKCHECK", "AUTO_INC", "ASCII"
		wantSubstr string
	}{
		{
			name:       "fail_at_PK_stops_early",
			failAt:     "PK",
			wantSubstr: "DATA_TYPE=int",
		},
		{
			name:       "fail_at_FKCHECK_stops_early",
			failAt:     "FKCHECK",
			wantSubstr: "found 1 FOREIGN KEY",
		},
		{
			name:       "fail_at_AUTO_INC_stops_early",
			failAt:     "AUTO_INC",
			wantSubstr: "step mismatch",
		},
		{
			name:       "fail_at_ASCII_stops_early",
			failAt:     "ASCII",
			wantSubstr: "Duplicate entry",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var recordedOps []string
			showCount := 0

			fq := &probeFakeQuerier{
				queryHandler: func(ctx context.Context, query string, args ...any) (Rows, error) {
					if query == qPKShape {
						recordedOps = append(recordedOps, "PK")
						dataType := "bigint"
						if tc.failAt == "PK" {
							dataType = "int"
						}
						return newProbeFakeRows(
							[]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE"},
							[][]any{{"id", dataType, "bigint unsigned"}},
						), nil
					}
					if query == qFKCheckCount {
						recordedOps = append(recordedOps, "FKCHECK")
						count := 0
						if tc.failAt == "FKCHECK" {
							count = 1
						}
						return newProbeFakeRows([]string{"COUNT(*)"}, [][]any{{count}}), nil
					}
					if strings.HasPrefix(query, "SHOW TABLE `locks` NEXT_ROW_ID") {
						recordedOps = append(recordedOps, "SHOW_AUTO_INC")
						showCount++
						val := uint64(10)
						if showCount == 2 {
							if tc.failAt == "AUTO_INC" {
								val = 11 // 错误步长
							} else {
								val = 12
							}
						}
						return newProbeFakeRows(
							[]string{"TABLE_NAME", "COLUMN_NAME", "NEXT_ROW_ID", "ID_TYPE"},
							[][]any{{"locks", "row_id", val, "AUTO_INCREMENT"}},
						), nil
					}
					return nil, fmt.Errorf("unexpected query: %s", query)
				},
				execHandler: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
					if strings.HasPrefix(query, "INSERT INTO `locks`") {
						recordedOps = append(recordedOps, "INSERT_LOCKS")
						return probeFakeResult{affected: 2}, nil
					}
					if strings.HasPrefix(query, "DELETE FROM `locks`") {
						recordedOps = append(recordedOps, "DELETE_LOCKS")
						return probeFakeResult{affected: 2}, nil
					}
					if strings.HasPrefix(query, "INSERT INTO `schema_meta`") {
						recordedOps = append(recordedOps, "INSERT_ASCII")
						if tc.failAt == "ASCII" && strings.Contains(fmt.Sprint(args), "abc_") {
							return nil, errors.New("Duplicate entry 'abc_' for key")
						}
						return probeFakeResult{affected: 1}, nil
					}
					if strings.HasPrefix(query, "DELETE FROM `schema_meta`") {
						recordedOps = append(recordedOps, "DELETE_ASCII")
						return probeFakeResult{affected: 1}, nil
					}
					return nil, fmt.Errorf("unexpected exec: %s", query)
				},
			}

			tables := []*Table{{Name: "t1", PKColumn: "id"}}
			err := VerifyStructure(ctx, fq, tables)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
			}

			// 验证后续层级没有被执行
			switch tc.failAt {
			case "PK":
				for _, op := range recordedOps {
					if op == "FKCHECK" || op == "SHOW_AUTO_INC" || op == "INSERT_ASCII" {
						t.Errorf("operation %s should not have executed when PK failed", op)
					}
				}
			case "FKCHECK":
				for _, op := range recordedOps {
					if op == "SHOW_AUTO_INC" || op == "INSERT_ASCII" {
						t.Errorf("operation %s should not have executed when FKCHECK failed", op)
					}
				}
			case "AUTO_INC":
				for _, op := range recordedOps {
					if op == "INSERT_ASCII" {
						t.Errorf("operation %s should not have executed when AUTO_INC failed", op)
					}
				}
			}
		})
	}
}
