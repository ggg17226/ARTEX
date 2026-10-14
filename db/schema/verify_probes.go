package schema

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// SQL 模板常量
const (
	qShowLocksNextRowID = "SHOW TABLE `locks` NEXT_ROW_ID"
	qInsertLocksTwoRows = "INSERT INTO `locks` (`name`) VALUES (?), (?)"
	qDeleteLocksTwoRows = "DELETE FROM `locks` WHERE `name` IN (?, ?)"

	qInsertSchemaMeta = "INSERT INTO `schema_meta` (`key`, `value`, `updated_at`) VALUES (?, ?, NOW(3))"
	qDeleteSchemaMeta = "DELETE FROM `schema_meta` WHERE `key` IN (%s)"
)

// makeRandomSuffix 生成固定 8 字节（16 十六进制字符）的随机后缀。
func makeRandomSuffix() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// boundedCleanupCtx 构造独立有界的清理 context（默认 5 秒超时），保证在主 ctx 取消时仍能安全清理资源。
func boundedCleanupCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// normalizeDriverValue 将数据库驱动返回的原始列值归一化为字符串，用于大小写/前后缀过滤。
// 规则：
//   - nil -> ""
//   - string -> 原字符串
//   - []byte -> 原字节切片转为字符串
//   - sql.RawBytes -> 原字节切片转为字符串
//   - 其他类型 -> fmt.Sprint(v)
func normalizeDriverValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case sql.RawBytes:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}

// parseShowAutoIncValue 从 SHOW TABLE NEXT_ROW_ID 结果中解析 AUTO_INCREMENT 目标值。
func parseShowAutoIncValue(ctx context.Context, q Querier) (val uint64, retErr error) {
	rows, err := q.Query(ctx, qShowLocksNextRowID)
	if err != nil {
		return 0, fmt.Errorf("query %s: %w", qShowLocksNextRowID, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			if retErr != nil {
				retErr = errors.Join(retErr, closeErr)
			} else {
				retErr = closeErr
			}
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		retErr = fmt.Errorf("columns: %w", err)
		return 0, retErr
	}

	// 查找列索引（大小写不敏感）
	valColIdx := -1
	idTypeIdx := -1
	tblNameIdx := -1
	colNameIdx := -1

	for i, c := range cols {
		upper := strings.ToUpper(c)
		switch upper {
		case "NEXT_GLOBAL_ROW_ID", "NEXT_ROW_ID":
			if valColIdx != -1 {
				retErr = fmt.Errorf("ambiguous value columns in SHOW result: %s and %s", cols[valColIdx], c)
				return 0, retErr
			}
			valColIdx = i
		case "ID_TYPE":
			idTypeIdx = i
		case "TABLE_NAME":
			tblNameIdx = i
		case "COLUMN_NAME":
			colNameIdx = i
		}
	}

	if valColIdx == -1 {
		retErr = errors.New("no value column (NEXT_GLOBAL_ROW_ID or NEXT_ROW_ID) found in SHOW result")
		return 0, retErr
	}

	type candidateRow struct {
		rawVal any
	}

	var candidates []candidateRow
	var loopErr error

	for rows.Next() {
		dest := make([]any, len(cols))
		for i := range dest {
			var v any
			dest[i] = &v
		}
		if err := rows.Scan(dest...); err != nil {
			loopErr = fmt.Errorf("scan: %w", err)
			break
		}

		// 检查 TABLE_NAME 是否为 locks（若存在该列）
		if tblNameIdx != -1 {
			valPtr := dest[tblNameIdx].(*any)
			valStr := strings.TrimSpace(normalizeDriverValue(*valPtr))
			if !strings.EqualFold(valStr, "locks") {
				continue
			}
		}

		// 检查 ID_TYPE 是否为 AUTO_INCREMENT（若存在该列）
		if idTypeIdx != -1 {
			valPtr := dest[idTypeIdx].(*any)
			valStr := strings.TrimSpace(normalizeDriverValue(*valPtr))
			if !strings.EqualFold(valStr, "AUTO_INCREMENT") {
				continue
			}
		}

		// 检查 COLUMN_NAME 是否为 row_id（若存在该列）
		if colNameIdx != -1 {
			valPtr := dest[colNameIdx].(*any)
			valStr := strings.TrimSpace(normalizeDriverValue(*valPtr))
			if !strings.EqualFold(valStr, "row_id") {
				continue
			}
		}

		valPtr := dest[valColIdx].(*any)
		candidates = append(candidates, candidateRow{rawVal: *valPtr})
	}

	if loopErr != nil {
		retErr = loopErr
		return 0, retErr
	}
	if err := rows.Err(); err != nil {
		retErr = fmt.Errorf("rows err: %w", err)
		return 0, retErr
	}

	if len(candidates) == 0 {
		retErr = errors.New("no target row found in SHOW result for locks AUTO_INCREMENT")
		return 0, retErr
	}
	if len(candidates) > 1 {
		retErr = fmt.Errorf("multiple target rows (%d) found in SHOW result for locks AUTO_INCREMENT", len(candidates))
		return 0, retErr
	}

	// 解析 uint64 计数器
	v, err := parseUint64Exact(candidates[0].rawVal)
	if err != nil {
		retErr = err
		return 0, retErr
	}
	return v, nil
}

func parseUint64Exact(v any) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("negative counter value: %d", val)
		}
		return uint64(val), nil
	case int:
		if val < 0 {
			return 0, fmt.Errorf("negative counter value: %d", val)
		}
		return uint64(val), nil
	case []byte:
		s := strings.TrimSpace(string(val))
		if strings.HasPrefix(s, "-") {
			return 0, fmt.Errorf("negative counter value string: %s", s)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse counter []byte %q as uint64: %w", s, err)
		}
		return u, nil
	case sql.RawBytes:
		s := strings.TrimSpace(string(val))
		if strings.HasPrefix(s, "-") {
			return 0, fmt.Errorf("negative counter value string: %s", s)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse counter sql.RawBytes %q as uint64: %w", s, err)
		}
		return u, nil
	case string:
		s := strings.TrimSpace(val)
		if strings.HasPrefix(s, "-") {
			return 0, fmt.Errorf("negative counter value string: %s", s)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse counter string %q as uint64: %w", s, err)
		}
		return u, nil
	default:
		s := strings.TrimSpace(fmt.Sprint(val))
		if strings.HasPrefix(s, "-") {
			return 0, fmt.Errorf("negative counter value string: %s", s)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse counter %T (%v) as uint64: %w", val, val, err)
		}
		return u, nil
	}
}

// CheckAutoIncStep 探测 TiDB locks 表自增步长（§7.5）：
//
//	前提：管理员预建独占测试schema/无外部并发写入，基线+2不是并发集群证明。
//	1. 生成 crypto 随机 markers（前缀+suffix+_a/_b <= 64）；
//	2. SHOW TABLE `locks` NEXT_ROW_ID 取得基线 uint64（检查 +2 是否溢出 MaxUint64）；
//	3. INSERT INTO `locks` (`name`) VALUES (?), (?) 单列插入两行；
//	4. 只有 INSERT 成功后才登记 owned markers；
//	5. 再次 SHOW TABLE `locks` NEXT_ROW_ID 验证 target == baseline + 2；
//	6. 所有后续失败路径均精确清理本次 owned markers，使用独立 bounded cleanup ctx，并通过 errors.Join 合并错误。
func CheckAutoIncStep(ctx context.Context, q Querier) (retErr error) {
	if q == nil {
		return errors.New("CheckAutoIncStep: querier is nil")
	}

	suffix, err := makeRandomSuffix()
	if err != nil {
		return err
	}
	markerA := fmt.Sprintf("artex_probe_%s_a", suffix)
	markerB := fmt.Sprintf("artex_probe_%s_b", suffix)
	if len(markerA) > 64 || len(markerB) > 64 {
		return fmt.Errorf("marker length exceeds 64: %d, %d", len(markerA), len(markerB))
	}

	// 1. 基线读取
	beforeVal, err := parseShowAutoIncValue(ctx, q)
	if err != nil {
		return fmt.Errorf("get baseline NEXT_ROW_ID: %w", err)
	}

	// 2. 检查 uint64 溢出
	if beforeVal > math.MaxUint64-2 {
		return fmt.Errorf("baseline NEXT_ROW_ID %d would overflow uint64 after +2", beforeVal)
	}
	expectedAfter := beforeVal + 2

	// 3. 插入两行
	var ownedMarkers []string
	defer func() {
		if len(ownedMarkers) == 0 {
			return
		}
		cCtx, cancel := boundedCleanupCtx()
		defer cancel()
		_, delErr := q.Exec(cCtx, qDeleteLocksTwoRows, ownedMarkers[0], ownedMarkers[1])
		if delErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("cleanup locks markers %v: %w", ownedMarkers, delErr))
		}
	}()

	_, insertErr := q.Exec(ctx, qInsertLocksTwoRows, markerA, markerB)
	if insertErr != nil {
		return fmt.Errorf("insert locks markers: %w", insertErr)
	}
	// 仅在成功后登记归属
	ownedMarkers = []string{markerA, markerB}

	// 4. 读取后值
	afterVal, err := parseShowAutoIncValue(ctx, q)
	if err != nil {
		retErr = fmt.Errorf("get after NEXT_ROW_ID: %w", err)
		return retErr
	}

	if afterVal != expectedAfter {
		retErr = fmt.Errorf("auto-increment step mismatch: before=%d, after=%d, want %d", beforeVal, afterVal, expectedAfter)
		return retErr
	}

	return nil
}

// CheckAsciiCollision 探测 schema_meta 的 key 列 ascii_bin 大小写敏感隔离性（§7.5）：
//
//	前提：独占测试 schema / 无并发写入环境。
//	1. 生成同随机 suffix 的两个 key（如 ABC_<suffix> 和 abc_<suffix>），长度 <= 64，唯一区别大小写；
//	2. 分别插入 schema_meta (key, value, updated_at)，每条插入成功后即刻登记归属；
//	3. 第二条若由于非大小写敏感报 1062 或驱动错误，精确清理第一条成功插入的 key；
//	4. 资源清理使用独立 bounded ctx，清理错误与主错误 errors.Join。
func CheckAsciiCollision(ctx context.Context, q Querier) (retErr error) {
	if q == nil {
		return errors.New("CheckAsciiCollision: querier is nil")
	}

	suffix, err := makeRandomSuffix()
	if err != nil {
		return err
	}
	keyUpper := fmt.Sprintf("ABC_%s", suffix)
	keyLower := fmt.Sprintf("abc_%s", suffix)
	if len(keyUpper) > 64 || len(keyLower) > 64 {
		return fmt.Errorf("key length exceeds 64: %d, %d", len(keyUpper), len(keyLower))
	}

	var ownedKeys []string
	defer func() {
		if len(ownedKeys) == 0 {
			return
		}
		cCtx, cancel := boundedCleanupCtx()
		defer cancel()

		placeholders := make([]string, len(ownedKeys))
		args := make([]any, len(ownedKeys))
		for i, k := range ownedKeys {
			placeholders[i] = "?"
			args[i] = k
		}
		delSQL := fmt.Sprintf(qDeleteSchemaMeta, strings.Join(placeholders, ", "))
		_, delErr := q.Exec(cCtx, delSQL, args...)
		if delErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("cleanup schema_meta keys %v: %w", ownedKeys, delErr))
		}
	}()

	// 插入第一条 (keyUpper)
	_, err = q.Exec(ctx, qInsertSchemaMeta, keyUpper, 0)
	if err != nil {
		retErr = fmt.Errorf("insert ascii uppercase key %s: %w", keyUpper, err)
		return retErr
	}
	ownedKeys = append(ownedKeys, keyUpper)

	// 插入第二条 (keyLower)
	_, err = q.Exec(ctx, qInsertSchemaMeta, keyLower, 1)
	if err != nil {
		retErr = fmt.Errorf("insert ascii lowercase key %s: %w", keyLower, err)
		return retErr
	}
	ownedKeys = append(ownedKeys, keyLower)

	return nil
}

// VerifyStructure 是完整的结构验收组合入口（Task 7B 核心规格）：
// 严格按以下 4 层顺序执行，首错停止，不跳层、不吞错：
//  1. CheckPrimaryKeys(ctx, q, tables)
//  2. CheckNoFKNoCheck(ctx, q)
//  3. CheckAutoIncStep(ctx, q)
//  4. CheckAsciiCollision(ctx, q)
func VerifyStructure(ctx context.Context, q Querier, tables []*Table) error {
	if q == nil {
		return errors.New("VerifyStructure: querier is nil")
	}

	// Layer 1: PK contracts
	if err := CheckPrimaryKeys(ctx, q, tables); err != nil {
		return fmt.Errorf("layer 1 PK verification failed: %w", err)
	}

	// Layer 2: No FK and No CHECK constraints
	if err := CheckNoFKNoCheck(ctx, q); err != nil {
		return fmt.Errorf("layer 2 FK/CHECK constraint verification failed: %w", err)
	}

	// Layer 3: Auto-increment allocator step probe
	if err := CheckAutoIncStep(ctx, q); err != nil {
		return fmt.Errorf("layer 3 auto-increment probe failed: %w", err)
	}

	// Layer 4: ASCII case-sensitive collision probe
	if err := CheckAsciiCollision(ctx, q); err != nil {
		return fmt.Errorf("layer 4 ASCII collision probe failed: %w", err)
	}

	return nil
}
