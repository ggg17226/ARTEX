package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
)

// isolateEnv 把本包所有会读取的 ARTEX_* 环境变量显式置空。
// Viper v1.19 把「存在但为空串」的环境变量视为未设置，因此置空等价于 unset，
// 使测试与调用方（shell / CI）残留的环境变量彻底隔离。
func isolateEnv(t *testing.T) {
	t.Helper()
	// 切进空目录：隐式寻址（./config.yaml 与 BaseDir()/config.yaml）永远命中不了
	// 仓库里真实存在的配置文件，测试与项目目录彻底解耦。
	t.Chdir(t.TempDir())
	for _, k := range []string{
		"ARTEX_TIDB_DSN",
		"ARTEX_TEST_TIDB_DSN",
		"ARTEX_PG_DSN",
		"ARTEX_CONFIG",
		"ARTEX_DATABASE_DSN",
		"ARTEX_DATABASE_HOST",
		"ARTEX_DATABASE_PORT",
		"ARTEX_DATABASE_USER",
		"ARTEX_DATABASE_PASSWORD",
		"ARTEX_DATABASE_NAME",
		"ARTEX_DATABASE_DBNAME",
		"ARTEX_DATABASE_SSLMODE",
		"ARTEX_SKILL_DIR",
	} {
		t.Setenv(k, "")
	}
}

// writeConfig 写入一份临时 config.yaml 并把 ARTEX_CONFIG 指向它。
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config.yaml: %v", err)
	}
	t.Setenv("ARTEX_CONFIG", path)
	return path
}

func TestTiDBDSN_EnvPrecedence(t *testing.T) {
	rawWant := "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev?charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'"
	wantNormalized, err := tidb.NormalizeDSN(rawWant)
	if err != nil {
		t.Fatalf("NormalizeDSN failed: %v", err)
	}
	t.Setenv("ARTEX_TIDB_DSN", rawWant)
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN returned error: %v", err)
	}
	if got != wantNormalized {
		t.Fatalf("expected normalized DSN %s, got %s", wantNormalized, got)
	}
	if !strings.Contains(src, "ARTEX_TIDB_DSN") {
		t.Fatalf("expected env source, got %s", src)
	}
}

func TestTiDBDSN_RejectPostgres(t *testing.T) {
	t.Setenv("ARTEX_TIDB_DSN", "postgres://user:pass@127.0.0.1:5432/artex")
	_, _, err := TiDBDSN()
	if err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Fatalf("expected rejection of postgres DSN, got %v", err)
	}
}

// ARTEX_TIDB_DSN 是最高优先级：即使配置文件同样提供了 database.dsn，也必须以它为准。
func TestTiDBDSN_TiDBDSNWinsOverFile(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"file:pw@tcp(10.0.0.9:4000)/file_db\"\n")
	raw := "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev"
	want, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	t.Setenv("ARTEX_TIDB_DSN", raw)
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("env should win over file: got %s want %s", got, want)
	}
	if !strings.Contains(src, "ARTEX_TIDB_DSN") {
		t.Fatalf("expected ARTEX_TIDB_DSN source, got %s", src)
	}
}

// 配置文件里的 database.dsn 必须被规范化（补齐 UTC/charset 四要素）。
func TestTiDBDSN_FileDSNNormalized(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev\"\n")
	want, err := tidb.NormalizeDSN("artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev")
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if !strings.Contains(got, "charset=utf8mb4") || !strings.Contains(got, "parseTime=true") {
		t.Fatalf("required params missing: %s", got)
	}
	if !strings.Contains(src, "database.dsn") {
		t.Fatalf("expected database.dsn source, got %s", src)
	}
}

// 环境变量 database.dsn（ARTEX_DATABASE_DSN）优先于配置文件，且同样经过 NormalizeDSN。
func TestTiDBDSN_DatabaseDSNEnvOverridesFile(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"file:pw@tcp(10.0.0.9:4000)/file_db\"\n")
	raw := "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev"
	want, err := tidb.NormalizeDSN(raw)
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	t.Setenv("ARTEX_DATABASE_DSN", raw)
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("ARTEX_DATABASE_DSN should win: got %s want %s", got, want)
	}
	if !strings.Contains(src, "ARTEX_DATABASE_DSN") {
		t.Fatalf("expected ARTEX_DATABASE_DSN source, got %s", src)
	}
}

// 散列字段（host/port/user/password/dbname）组装出的 DSN 默认端口 4000，同样被规范化。
func TestTiDBDSN_AssembledFromFields(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  password: \"PASSWORD\"\n  dbname: \"artex_dev\"\n")
	want, err := tidb.NormalizeDSN("artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev")
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("assembled DSN got %s want %s", got, want)
	}
	if !strings.Contains(src, "database") {
		t.Fatalf("expected database field source, got %s", src)
	}
}

// 散列字段的环境变量映射：database.host → ARTEX_DATABASE_HOST 等。
func TestTiDBDSN_FieldEnvMapping(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "") // 无配置文件：纯环境变量启动
	t.Setenv("ARTEX_DATABASE_HOST", "127.0.0.1")
	t.Setenv("ARTEX_DATABASE_PORT", "4001")
	t.Setenv("ARTEX_DATABASE_USER", "artex")
	t.Setenv("ARTEX_DATABASE_PASSWORD", "PASSWORD")
	t.Setenv("ARTEX_DATABASE_DBNAME", "artex_dev")
	want, err := tidb.NormalizeDSN("artex:PASSWORD@tcp(127.0.0.1:4001)/artex_dev")
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	got, _, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("env mapped DSN got %s want %s", got, want)
	}
}

// 纯环境变量启动（无配置文件）时，来源描述必须指向环境变量而不是某个不存在的文件。
func TestTiDBDSN_EnvOnlySourceLabel(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "")
	t.Setenv("ARTEX_DATABASE_HOST", "127.0.0.1")
	t.Setenv("ARTEX_DATABASE_USER", "artex")
	t.Setenv("ARTEX_DATABASE_PASSWORD", "PASSWORD")
	t.Setenv("ARTEX_DATABASE_DBNAME", "artex_dev")
	_, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if !strings.Contains(src, "ARTEX_DATABASE_*") {
		t.Fatalf("expected env source label, got %s", src)
	}
	if strings.Contains(src, "配置文件") {
		t.Fatalf("no config file was loaded, source must not mention one: %s", src)
	}
}

// postgres:// 也可能藏在配置文件或 database.dsn 环境变量里：一律显式拒绝。
func TestTiDBDSN_RejectPostgresEverywhere(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"postgres://user:pass@127.0.0.1:5432/artex\"\n")
	if _, _, err := TiDBDSN(); err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Fatalf("config file postgres dsn must be rejected, got %v", err)
	}

	t.Setenv("ARTEX_DATABASE_DSN", "postgresql://user:pass@127.0.0.1:5432/artex")
	if _, _, err := TiDBDSN(); err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Fatalf("env postgres dsn must be rejected, got %v", err)
	}
}

// ARTEX_CONFIG 显式指定却不存在的文件：fail-closed，严禁静默回退。
func TestTiDBDSN_ExplicitConfigMissingFailsClosed(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if _, _, err := TiDBDSN(); err == nil {
		t.Fatalf("missing explicit ARTEX_CONFIG must fail closed")
	}
}

// 解析失败同样 fail-closed，且错误里带上文件名。
func TestTiDBDSN_ExplicitConfigBrokenFailsClosed(t *testing.T) {
	isolateEnv(t)
	path := writeConfig(t, "database: [oops\n  bad: : y\n")
	_, _, err := TiDBDSN()
	if err == nil {
		t.Fatalf("broken YAML must fail closed")
	}
	if !strings.Contains(err.Error(), filepath.Base(path)) {
		t.Fatalf("error should name the config file, got %v", err)
	}
}

// 完全没有任何来源时报错，且不给出静默默认连接（fail-closed）。
func TestTiDBDSN_NoSourceFails(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "/dev/null") // 空配置
	_, _, err := TiDBDSN()
	if err == nil {
		t.Fatalf("expected error when no DSN source is configured")
	}
	if !strings.Contains(err.Error(), "ARTEX_TIDB_DSN") {
		t.Fatalf("error should tell the user how to configure, got %v", err)
	}
}

// 缺 user / 缺库名等非法 DSN 由 tidb.NormalizeDSN 拒绝，config 层不得吞掉。
func TestTiDBDSN_InvalidDSNFailsClosed(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_TIDB_DSN", "artex:PASSWORD@tcp(127.0.0.1:4000)/")
	if _, _, err := TiDBDSN(); err == nil {
		t.Fatalf("DSN without database name must fail")
	}
}

func TestLoad_FileAndEnv(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev\"\nskill_dir: \"/tmp/skills-from-file\"\n")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Database.DSN != "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev" {
		t.Fatalf("unexpected dsn %q", c.Database.DSN)
	}
	if c.SkillDir != "/tmp/skills-from-file" {
		t.Fatalf("unexpected skill_dir %q", c.SkillDir)
	}

	// 环境变量优先于配置文件
	t.Setenv("ARTEX_SKILL_DIR", "/tmp/skills-from-env")
	t.Setenv("ARTEX_DATABASE_DSN", "env:pw@tcp(127.0.0.1:4000)/env_db")
	c, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.SkillDir != "/tmp/skills-from-env" {
		t.Fatalf("env must win skill_dir, got %q", c.SkillDir)
	}
	if c.Database.DSN != "env:pw@tcp(127.0.0.1:4000)/env_db" {
		t.Fatalf("env must win database.dsn, got %q", c.Database.DSN)
	}
}

// 无配置文件时 Load 仍可用（纯环境变量启动是被支持的路径）。
func TestLoad_NoConfigFileIsNotAnError(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "")
	t.Setenv("ARTEX_DATABASE_PORT", "4000")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load without config file: %v", err)
	}
	if c.Database.Port != 4000 {
		t.Fatalf("unexpected port %d", c.Database.Port)
	}
}

// 显式 ARTEX_CONFIG 解析失败时 Load 也必须报错（fail-closed）。
func TestLoad_ExplicitConfigBroken(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database: [oops\n  bad: : y\n")
	if _, err := Load(); err == nil {
		t.Fatalf("broken YAML must fail Load")
	}
}

// 兼容层：PostgreSQL 连接串已彻底废除，旧入口只返回错误，绝不静默连错库。
func TestPostgresDSN_Abolished(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "/dev/null")
	_, _, err := PostgresDSN()
	if err == nil {
		t.Fatalf("PostgresDSN must always fail after the TiDB migration")
	}
}

func TestSkillDir(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "/dev/null")
	t.Setenv("ARTEX_SKILL_DIR", t.TempDir())
	if got := SkillDir(); got == "" {
		t.Fatalf("SkillDir empty")
	}
}
