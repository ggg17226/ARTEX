package schema

import "strings"

// IsRealTiDB 是 VERSION() 守卫（spec §10.2：必须证明真实 TiDB，MySQL/SQLite 不能替代）。
// Task 7 扩展本文件时不得重命名或改签名。
func IsRealTiDB(version string) bool {
	return strings.Contains(version, "TiDB")
}
