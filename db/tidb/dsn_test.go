package tidb_test

import (
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
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
		{"time_zone", "%27%2B00%3A00%27"}, // NormalizeDSN 对 params 百分号编码，且 time_zone 带 SQL 单引号
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

// 审查反馈 1：UTC 会话参数单引号匹配与注入
func TestNormalizeDSN_TimeZoneQuotes(t *testing.T) {
	// 用户提供带单引号的原生值或 urlencoded 值均应被判定为匹配而不报错
	for _, dsn := range []string{
		"app:secret@tcp(127.0.0.1:4000)/artex_db?time_zone='%2B00:00'",
		"app:secret@tcp(127.0.0.1:4000)/artex_db?time_zone=%27%2B00%3A00%27",
	} {
		got, err := tidb.NormalizeDSN(dsn)
		if err != nil {
			t.Fatalf("expected accept for %q, got error: %v", dsn, err)
		}
		hasParam(t, got, "time_zone", "%27%2B00%3A00%27")
	}

	// 若用户提供不带单引号的值，与强制值冲突，必须报错拒绝
	if _, err := tidb.NormalizeDSN("app:secret@tcp(127.0.0.1:4000)/artex_db?time_zone=+00:00"); err == nil {
		t.Fatal("expected conflict error for unquoted time_zone=+00:00, got nil")
	}
}

// 审查反馈 2：Fail Closed 检查 query 解析非法字符及重复冲突参数
func TestNormalizeDSN_FailClosedMalformedAndDuplicates(t *testing.T) {
	// 包含无法解析的 query 语法（如 semicolon 错误）必须 Fail Closed 报错
	if _, err := tidb.NormalizeDSN("app:secret@tcp(127.0.0.1:4000)/artex_db?x=a;b"); err == nil {
		t.Fatal("expected error for malformed query x=a;b, got nil")
	}

	// 重复参数中包含与强制要求冲突的任一项，必须报错拒绝
	for _, dsn := range []string{
		"app:secret@tcp(127.0.0.1:4000)/artex_db?parseTime=false&parseTime=true",
		"app:secret@tcp(127.0.0.1:4000)/artex_db?parseTime=true&parseTime=false",
	} {
		_, err := tidb.NormalizeDSN(dsn)
		if err == nil {
			t.Fatalf("expected conflict error for duplicate param in %q, got nil", dsn)
		}
		if !strings.Contains(err.Error(), "parseTime") {
			t.Fatalf("error should mention parseTime: %v", err)
		}
	}
}

// 审查反馈 3：库名保留 PathEscape 防止被截断破坏幂等
func TestNormalizeDSN_DBNameEscaping(t *testing.T) {
	// 库名包含 '?' 或 '/' 等字符
	raw := "app:secret@tcp(127.0.0.1:4000)/artex%3Fdb"
	norm, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(norm, "/artex%3Fdb") {
		t.Fatalf("escaped dbname missing in %q", norm)
	}
	norm2, err := tidb.NormalizeDSN(norm)
	if err != nil {
		t.Fatalf("second pass error: %v", err)
	}
	if norm != norm2 {
		t.Fatalf("not idempotent for escaped dbname: %q vs %q", norm, norm2)
	}
	cfg, err := mysql.ParseDSN(norm)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	if cfg.DBName != "artex?db" {
		t.Fatalf("DBName = %q, want artex?db", cfg.DBName)
	}
}

// 审查反馈 4：合法附加参数（如 duration 1µs）驱动回读兼容与幂等
func TestNormalizeDSN_DurationRoundTrip(t *testing.T) {
	raw := "app:secret@tcp(127.0.0.1:4000)/artex_db?timeout=1µs&readTimeout=100ms"
	norm, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("NormalizeDSN error: %v", err)
	}
	// 再次 NormalizeDSN 保持完全幂等
	norm2, err := tidb.NormalizeDSN(norm)
	if err != nil {
		t.Fatalf("NormalizeDSN idempotent pass error: %v", err)
	}
	if norm != norm2 {
		t.Fatalf("NormalizeDSN not idempotent:\n1st: %s\n2nd: %s", norm, norm2)
	}

	// 驱动可以正常 ParseDSN 回读
	cfg, err := mysql.ParseDSN(norm)
	if err != nil {
		t.Fatalf("mysql.ParseDSN failed on normalized DSN: %v", err)
	}
	if cfg.Timeout != 1000 { // 1µs = 1000ns
		t.Fatalf("cfg.Timeout = %v, want 1µs", cfg.Timeout)
	}
	if cfg.ReadTimeout != 100*1000*1000 {
		t.Fatalf("cfg.ReadTimeout = %v, want 100ms", cfg.ReadTimeout)
	}
}

// 审查反馈 5：RedactDSN 单元测试
func TestRedactDSN(t *testing.T) {
	raw := "app:super_secret@tcp(127.0.0.1:4000)/artex_db?timeout=5s"
	redacted := tidb.RedactDSN(raw)
	if strings.Contains(redacted, "super_secret") {
		t.Fatalf("password leaked in redacted DSN: %q", redacted)
	}
	if !strings.Contains(redacted, ":****@") {
		t.Fatalf("expected password masked with ****: %q", redacted)
	}

	// 非法 DSN 测试
	bad := "invalid-dsn"
	redactedBad := tidb.RedactDSN(bad)
	if redactedBad != "***unparseable-dsn***" {
		t.Fatalf("expected ***unparseable-dsn***, got %q", redactedBad)
	}
}
