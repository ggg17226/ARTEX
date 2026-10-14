package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ApplyDDL applies DDL statements one by one (每条自动提交；DDL 不可包在
// 回滚事务中，spec §11.1)。
// 首个失败立即停止并以 %w 保留原错误链与首行上下文。
func ApplyDDL(ctx context.Context, db *sql.DB, stmts []string) error {
	if db == nil {
		return errors.New("apply DDL: nil db")
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("apply DDL %q: %w", firstLine(s), err)
		}
	}
	return nil
}

// PrepareInfraSQL returns the idempotent non-business seed DML for
// db-migrate --prepare (P08 调用)：business_relations 锁命名行 + schema_version 版本戳。
// 每次调用均返回新切片。
func PrepareInfraSQL() []string {
	return []string{
		"INSERT IGNORE INTO `locks` (`name`) VALUES ('business_relations')",
		"INSERT IGNORE INTO `schema_meta` (`key`, `value`, `updated_at`) VALUES ('schema_version', 0, NOW(3))",
	}
}

// firstLine returns the first line of a statement (错误信息用).
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
