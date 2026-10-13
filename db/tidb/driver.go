// Package tidb is the ARTEX native-TiDB connection layer (P01).
//
// It owns the MySQL driver registration, DSN normalization, the shared
// database/sql pool, read-only capability probing, MySQL error
// classification and the maintenance-row lock primitives that replace
// PostgreSQL advisory locks. It is deliberately decoupled from the
// existing PG-backed db/ package, which stays untouched and
// compilable in this plan.
package tidb

import (
	_ "github.com/go-sql-driver/mysql" // 注册 database/sql driver 名 "mysql"
)

// DriverName 是 P01 落定的 TiDB driver 注册名（go-sql-driver/mysql 自注册名）。
// 跨计划（P02/P03/P08）引用 driver 名时一律走此函数，不得硬编码字符串。
func DriverName() string { return "mysql" }
