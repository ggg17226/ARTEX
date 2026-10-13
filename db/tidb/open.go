package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Capabilities 是只读能力探测结果（spec §6.5：只读探测适配，能力不足显式失败）。
type Capabilities struct {
	Version          string // @@version（dev 实测 8.0.11-TiDB-v8.5.8）
	MaxAllowedPacket int64  // @@max_allowed_packet（dev 实测 64MiB）
	Timezone         string // @@time_zone（DSN 强制 +00:00 后的会话值）
	Collation        string // @@collation_database（dev 实测 utf8mb4_general_ci）
	Isolation        string // @@transaction_isolation（dev 实测 REPEATABLE-READ）
	ForeignKeyChecks int    // @@foreign_key_checks（dev 实测 FK 会被真正强制）
	SQLMode          string // @@sql_mode（dev 实测含 STRICT_TRANS_TABLES）
}

const (
	maxOpenConns    = 32 // 沿用既有 PG 池数值（spec §2.1）
	maxIdleConns    = 32
	connMaxLifetime = 30 * time.Minute
	connMaxIdleTime = 5 * time.Minute
	openPingTimeout = 10 * time.Second
	probeTimeout    = 10 * time.Second
)

// OpenTiDB 打开共享 TiDB 连接池：规范化 DSN → sql.Open("mysql") → 池参数 →
// PingContext 真实建连 → 只读能力探测。任何环节失败都关闭池并返回错误。
// 本函数不执行任何 DDL、不创建任何表（strict-only 生命周期由 P08 负责）。
func OpenTiDB(dsn string) (db *sql.DB, caps Capabilities, err error) {
	norm, err := NormalizeDSN(dsn)
	if err != nil {
		return nil, Capabilities{}, err
	}
	sqlDB, err := sql.Open(DriverName(), norm)
	if err != nil {
		return nil, Capabilities{}, fmt.Errorf("open mysql pool: %w", err)
	}
	applyPoolConfig(sqlDB)
	pingCtx, cancel := context.WithTimeout(context.Background(), openPingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, Capabilities{}, fmt.Errorf("ping %s: %w", RedactDSN(norm), err)
	}
	probeCtx, pCancel := context.WithTimeout(context.Background(), probeTimeout)
	defer pCancel()
	caps, err = ProbeCapabilities(probeCtx, sqlDB)
	if err != nil {
		_ = sqlDB.Close()
		return nil, Capabilities{}, err
	}
	return sqlDB, caps, nil
}

// applyPoolConfig 固定池参数（spec §2.1）。
func applyPoolConfig(db *sql.DB) {
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)
}

// ProbeCapabilities 执行只读能力探测。全部为 SELECT @@var，无副作用；
// 任一查询失败（无权限/驱动错误）→ 显式错误，不做降级假设。
func ProbeCapabilities(ctx context.Context, db *sql.DB) (Capabilities, error) {
	var c Capabilities
	targets := []struct {
		name string
		dst  any
	}{
		{"version", &c.Version},
		{"max_allowed_packet", &c.MaxAllowedPacket},
		{"time_zone", &c.Timezone},
		{"collation_database", &c.Collation},
		{"transaction_isolation", &c.Isolation},
		{"foreign_key_checks", &c.ForeignKeyChecks},
		{"sql_mode", &c.SQLMode},
	}
	for _, tv := range targets {
		if err := db.QueryRowContext(ctx, "SELECT @@"+tv.name).Scan(tv.dst); err != nil {
			return Capabilities{}, fmt.Errorf("probe %s: %w", tv.name, err)
		}
	}
	// spec L83：DSN 每物理连接必须 UTC；探测到非 +00:00 说明 DSN 参数未生效，显式失败。
	if c.Timezone != "+00:00" {
		return Capabilities{}, fmt.Errorf("probe time_zone: session is %q, required +00:00 — DSN 参数未生效", c.Timezone)
	}
	return c, nil
}
