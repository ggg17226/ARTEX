// Package idalloc 提供 64 位 ID 的兼容域守卫与安全回读（spec §2.4.1）。
//
// 规则：
//   - 兼容域 1..math.MaxInt64（用户确认的 §10.4-S 规则，不是 JS-safe 上限）。
//   - 分配/回读后、显式 tx commit 前必须校验；越域/回读 0 → 显式错误，
//     调用方负责整 tx rollback。本包不静默 fallback、不盲推连续 ID、不盲 cast。
//   - 回读只使用 database/sql 标准 API（*sql.Tx），不引用具体 driver 类型。
package idalloc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

var (
	// ErrOutOfSafeRange：ID 为 0 或 > math.MaxInt64。
	ErrOutOfSafeRange = errors.New("idalloc: id 超出 1..MaxInt64 兼容域")
	// ErrReadbackZero：LAST_INSERT_ID()/identity 回读为 0（TiDB 对 >MaxInt64
	// 高值、INSERT IGNORE miss、upsert 均返回 0，见 spec §11.1）。
	ErrReadbackZero = errors.New("idalloc: LAST_INSERT_ID() 回读为 0，分配失败")
)

// ValidateUint64 校验 id 落在兼容域 1..math.MaxInt64。
func ValidateUint64(id uint64) error {
	if id == 0 || id > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrOutOfSafeRange, id)
	}
	return nil
}

// AllocID 在调用方已完成单行 INSERT（且 RowsAffected==1）之后，于同一事务
// （同一物理连接）执行 SELECT LAST_INSERT_ID() 并扫 uint64，随后做兼容域校验。
//
// 约束：INSERT 与本函数之间不得插入其他 INSERT（否则 session last_insert_id
// 被覆盖）。LAST_INSERT_ID()==0（高值/IGNORE miss/upsert）或越域 → 显式错误；
// 调用方收到错误后必须 rollback。
func AllocID(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var id uint64
	if err := tx.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		return 0, fmt.Errorf("idalloc: 回读 LAST_INSERT_ID 失败: %w", err)
	}
	if id == 0 {
		return 0, ErrReadbackZero
	}
	if err := ValidateUint64(id); err != nil {
		return 0, err
	}
	return id, nil
}

// AllocIDByIdentity 用于面向公开业务实体的 exact logical identity 回读（例如
// 高值 >MaxInt64 实测 LAST_INSERT_ID()=0，需由业务根据唯一键查回）。
// query 必须恰好返回 1 行、首列为逻辑身份 ID（BIGINT UNSIGNED），
// 例如 "SELECT id FROM t WHERE logical_key = ?"。
//
// 约束：回读到 0 返回 ErrReadbackZero；且回读后必须执行 ValidateUint64(id)，
// 超出 1..MaxInt64 兼容域返回 ErrOutOfSafeRange，杜绝公开 ID 越界漏网（spec §2.4.1）。
func AllocIDByIdentity(ctx context.Context, tx *sql.Tx, query string, args ...any) (uint64, error) {
	var id uint64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: identity 查询未命中", ErrReadbackZero)
	}
	if err != nil {
		return 0, fmt.Errorf("idalloc: identity 回读失败: %w", err)
	}
	if id == 0 {
		return 0, ErrReadbackZero
	}
	if err := ValidateUint64(id); err != nil {
		return 0, err
	}
	return id, nil
}

// AllocPhysicalRowIDByIdentity 专供底层隐藏 row_id 的 exact identity 回读（spec §2.4.1）。
// 仅供内部并发定位与事务锁续期，不暴露给前端或业务 DTO。
//
// 约束：回读到 0 返回 ErrReadbackZero；但允许全部有效的 uint64 高值（包含 math.MaxUint64），
// 不执行 1..MaxInt64 兼容域截断守卫。
func AllocPhysicalRowIDByIdentity(ctx context.Context, tx *sql.Tx, query string, args ...any) (uint64, error) {
	var id uint64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: identity 查询未命中", ErrReadbackZero)
	}
	if err != nil {
		return 0, fmt.Errorf("idalloc: identity 回读失败: %w", err)
	}
	if id == 0 {
		return 0, ErrReadbackZero
	}
	return id, nil
}

