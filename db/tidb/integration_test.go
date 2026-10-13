package tidb_test

import (
	"context"
	"os"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
)

// 真实 TiDB 门控 helper：缺 DSN 时打印「未验证」并 skip。
func requireTiDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skipf("未验证：ARTEX_TIDB_DSN 未设置，跳过真实 TiDB 集成测试")
	}
	return dsn
}

func TestOpenTiDB_EmptyDSNFailsClosed(t *testing.T) {
	t.Setenv("ARTEX_TIDB_DSN", "")
	db, _, err := tidb.OpenTiDB("")
	if err == nil {
		db.Close()
		t.Fatal("expected error for empty DSN, got nil")
	}
}

func TestOpenTiDB_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, caps, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if caps.Version == "" {
		t.Fatal("capabilities.Version empty — 探测未执行")
	}
	if caps.MaxAllowedPacket <= 0 {
		t.Fatalf("capabilities.MaxAllowedPacket = %d, want > 0（dev 实测 64MiB）", caps.MaxAllowedPacket)
	}
	if caps.Timezone != "+00:00" {
		t.Fatalf("session time_zone = %q, want +00:00（DSN 强制参数未生效）", caps.Timezone)
	}
	// 池参数抽查：MaxOpenConns 应为 32
	if db.Stats().MaxOpenConnections != 32 {
		t.Fatalf("MaxOpenConnections = %d, want 32", db.Stats().MaxOpenConnections)
	}
}

func TestProbeCapabilities_Real(t *testing.T) {
	dsn := requireTiDB(t)
	db, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		t.Fatalf("OpenTiDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	caps, err := tidb.ProbeCapabilities(context.Background(), db)
	if err != nil {
		t.Fatalf("ProbeCapabilities: %v", err)
	}
	if caps.Isolation == "" || caps.Collation == "" {
		t.Fatalf("incomplete capabilities: %+v", caps)
	}
}
