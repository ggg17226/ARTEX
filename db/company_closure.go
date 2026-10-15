package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CompanyCascadeClosure 记录删除 Company 时的级联清理与置空集合
type CompanyCascadeClosure struct {
	CompanyID       uint64
	ScopeIDs        []uint64 // company_scope.id
	TaskScopeIDs    []uint64 // task_scope.id (WHERE kind='company' AND company_id=?)
	NullifyAssetIDs []uint64 // assets.id (WHERE company_id=?)
	NullifyTaskIDs  []uint64 // tasks.id (WHERE company_id=?)
}

func (c *CompanyCascadeClosure) TotalCascadeDeleteCount() int {
	return 1 + len(c.ScopeIDs) + len(c.TaskScopeIDs)
}

func (c *CompanyCascadeClosure) TotalNullifyCount() int {
	return len(c.NullifyAssetIDs) + len(c.NullifyTaskIDs)
}

func (c *CompanyCascadeClosure) Validate() error {
	if c.CompanyID == 0 {
		return errors.New("company closure: CompanyID is 0")
	}
	return nil
}

// ComputeCompanyClosure 收集删除 Company 时的完整级联闭包
func ComputeCompanyClosure(ctx context.Context, tx DBTX, companyID uint64) (*CompanyCascadeClosure, error) {
	closure := &CompanyCascadeClosure{
		CompanyID: companyID,
	}

	// 1. 验证公司存在
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM companies WHERE id = ? LIMIT 1`, companyID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: company id=%d not found", ErrForeignKeyViolation, companyID)
		}
		return nil, err
	}

	// 2. 收集级联删除子表：company_scope
	if err := queryUint64s(ctx, tx, &closure.ScopeIDs, `SELECT id FROM company_scope WHERE company_id = ?`, companyID); err != nil {
		return nil, err
	}

	// 3. 收集级联删除子表：task_scope (kind='company' 规则)
	if err := queryUint64s(ctx, tx, &closure.TaskScopeIDs, `SELECT id FROM task_scope WHERE company_id = ?`, companyID); err != nil {
		return nil, err
	}

	// 4. 收集待置空子表：assets
	if err := queryUint64s(ctx, tx, &closure.NullifyAssetIDs, `SELECT id FROM assets WHERE company_id = ?`, companyID); err != nil {
		return nil, err
	}

	// 5. 收集待置空子表：tasks
	if err := queryUint64s(ctx, tx, &closure.NullifyTaskIDs, `SELECT id FROM tasks WHERE company_id = ?`, companyID); err != nil {
		return nil, err
	}

	return closure, closure.Validate()
}
