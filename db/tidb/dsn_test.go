package tidb_test

import (
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
)

func hasParam(t *testing.T, dsn, key, val string) {
	t.Helper()
	if !strings.Contains(dsn, key+"="+val) {
		t.Fatalf("DSN %q missing %s=%s", dsn, key, val)
	}
}

func TestNormalizeDSN_EnsuresRequiredParams(t *testing.T) {
	const raw = "app:secret@tcp(127.0.0.1:4000)/artex_db"
	got, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, kv := range []struct{ k, v string }{
		{"parseTime", "true"},
		{"loc", "UTC"},
		{"time_zone", "%2B00%3A00"}, // NormalizeDSN 对 params 百分号编码，保证 ParseDSN 往返稳定
		{"charset", "utf8mb4"},
	} {
		hasParam(t, got, kv.k, kv.v)
	}
	if !strings.Contains(got, "/artex_db") {
		t.Fatalf("database lost: %q", got)
	}
}

func TestNormalizeDSN_Idempotent(t *testing.T) {
	const raw = "app:secret@tcp(127.0.0.1:4000)/artex_db"
	once, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	twice, err := tidb.NormalizeDSN(once)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if once != twice {
		t.Fatalf("not idempotent:\nfirst  %s\nsecond %s", once, twice)
	}
}

func TestNormalizeDSN_PreservesUserParams(t *testing.T) {
	got, err := tidb.NormalizeDSN("app:secret@tcp(127.0.0.1:4000)/artex_db?timeout=5s&readTimeout=9s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, p := range []string{"timeout=5s", "readTimeout=9s"} {
		if !strings.Contains(got, p) {
			t.Fatalf("user param %q lost: %q", p, got)
		}
	}
}

func TestNormalizeDSN_ConflictingRequiredParamFails(t *testing.T) {
	_, err := tidb.NormalizeDSN("app:secret@tcp(127.0.0.1:4000)/artex_db?parseTime=false")
	if err == nil {
		t.Fatal("expected conflict error for parseTime=false, got nil")
	}
	if !strings.Contains(err.Error(), "parseTime") {
		t.Fatalf("error should name the conflicting param: %v", err)
	}
}

func TestNormalizeDSN_MissingDatabaseFails(t *testing.T) {
	if _, err := tidb.NormalizeDSN("app:secret@tcp(127.0.0.1:4000)/"); err == nil {
		t.Fatal("expected error for empty database name, got nil")
	}
}

func TestNormalizeDSN_MissingUserFails(t *testing.T) {
	if _, err := tidb.NormalizeDSN(":secret@tcp(127.0.0.1:4000)/artex_db"); err == nil {
		t.Fatal("expected error for empty user, got nil")
	}
}
