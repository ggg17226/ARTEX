package config

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/go-sql-driver/mysql"
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

func TestTiDBDSN_FullDSN_IgnoresUnusedInvalidPort(t *testing.T) {
	isolateEnv(t)
	// YAML 格式合法，但包含非数字非法 port；因为提供了 ARTEX_TIDB_DSN，不应被未使用的非法 port 阻断
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configFile, []byte("database:\n  port: not-a-port\n"), 0o600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}
	t.Setenv(EnvConfigFile, configFile)

	rawWant := "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev"
	t.Setenv(EnvTiDBDSN, rawWant)

	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("expected successful DSN retrieval despite unused invalid port, got: %v", err)
	}
	want, err := tidb.NormalizeDSN(rawWant)
	if err != nil {
		t.Fatalf("NormalizeDSN failed: %v", err)
	}
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
	if !strings.Contains(src, EnvTiDBDSN) {
		t.Fatalf("expected env source, got: %s", src)
	}
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

func TestSkillDir(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "/dev/null")
	t.Setenv("ARTEX_SKILL_DIR", t.TempDir())
	if got := SkillDir(); got == "" {
		t.Fatalf("SkillDir empty")
	}
}

// ---------------------------------------------------------------------------
// 审查修复轮 1（R1）：ARTEX_TIDB_DSN 只决定 DSN 取值来源，绝不豁免显式
// 配置文件（ARTEX_CONFIG）的存在性与可解析性。
// ---------------------------------------------------------------------------

// validTopDSN 是一条本身完全合法的顶层覆盖连接串：它成功与否都不能掩盖
// 显式配置文件的损坏。
const validTopDSN = "artex:PASSWORD@tcp(127.0.0.1:4000)/artex_dev"

// 显式 ARTEX_CONFIG 指向不存在的文件 + 合法 ARTEX_TIDB_DSN：必须 fail-closed。
func TestTiDBDSN_ExplicitConfigMissingFailsClosedEvenWithTopDSN(t *testing.T) {
	isolateEnv(t)
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	t.Setenv("ARTEX_CONFIG", missing)
	t.Setenv("ARTEX_TIDB_DSN", validTopDSN)

	got, src, err := TiDBDSN()
	if err == nil {
		t.Fatalf("显式配置文件缺失时必须报错，即使 ARTEX_TIDB_DSN 合法；实际返回 %q (source=%s)", got, src)
	}
	if got != "" || src != "" {
		t.Fatalf("fail-closed 时不得返回任何 DSN/来源，got %q src=%q", got, src)
	}
	if !strings.Contains(err.Error(), filepath.Base(missing)) {
		t.Fatalf("错误必须点名缺失的配置文件，got %v", err)
	}
}

// 显式 ARTEX_CONFIG 是坏 YAML + 合法 ARTEX_TIDB_DSN：同样必须 fail-closed。
func TestTiDBDSN_ExplicitConfigBrokenFailsClosedEvenWithTopDSN(t *testing.T) {
	isolateEnv(t)
	path := writeConfig(t, "database: [oops\n  bad: : y\n")
	t.Setenv("ARTEX_TIDB_DSN", validTopDSN)

	got, src, err := TiDBDSN()
	if err == nil {
		t.Fatalf("显式配置文件损坏时必须报错，即使 ARTEX_TIDB_DSN 合法；实际返回 %q (source=%s)", got, src)
	}
	if got != "" || src != "" {
		t.Fatalf("fail-closed 时不得返回任何 DSN/来源，got %q src=%q", got, src)
	}
	if !strings.Contains(err.Error(), filepath.Base(path)) {
		t.Fatalf("错误必须点名损坏的配置文件，got %v", err)
	}
}

// 配置文件合法时，ARTEX_TIDB_DSN 仍是最高优先级取值（R1 的修复不得把
// 文件里的 DSN 覆盖到顶层环境变量之上）。
func TestTiDBDSN_TopDSNStillWinsWhenConfigIsValid(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database:\n  dsn: \"file:pw@tcp(10.0.0.9:4000)/file_db\"\n  port: 4000\n")
	t.Setenv("ARTEX_TIDB_DSN", validTopDSN)

	want, err := tidb.NormalizeDSN(validTopDSN)
	if err != nil {
		t.Fatalf("NormalizeDSN: %v", err)
	}
	got, src, err := TiDBDSN()
	if err != nil {
		t.Fatalf("TiDBDSN: %v", err)
	}
	if got != want {
		t.Fatalf("ARTEX_TIDB_DSN 仍应是最高取值：got %s want %s", got, want)
	}
	if !strings.Contains(src, "ARTEX_TIDB_DSN") {
		t.Fatalf("source 应指向 ARTEX_TIDB_DSN，got %s", src)
	}
}

// Load 也必须先校验显式配置载体：损坏文件 + 顶层 DSN 一样报错。
func TestLoad_ExplicitConfigBrokenFailsClosedEvenWithTopDSN(t *testing.T) {
	isolateEnv(t)
	writeConfig(t, "database: [oops\n  bad: : y\n")
	t.Setenv("ARTEX_TIDB_DSN", validTopDSN)
	if _, err := Load(); err == nil {
		t.Fatalf("Load 必须对损坏的显式配置文件 fail-closed")
	}
}

// ---------------------------------------------------------------------------
// 审查修复轮 1（R2/R3/R5）：散列字段组装必须与 go-sql-driver 的真实字段语法
// 对齐。断言一律通过 mysql.ParseDSN 回读「实际连接字段」，而不是比较字符串形状。
// ---------------------------------------------------------------------------

// mustParseDSN 用驱动自己的解析器回读 DSN，失败即终止测试（错误里只带脱敏串）。
func mustParseDSN(t *testing.T, dsn string) *mysql.Config {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("驱动回读 DSN 失败：%v（dsn=%s）", err, tidb.RedactDSN(dsn))
	}
	return cfg
}

// assertCanonical 断言返回的 DSN 已处于规范化不动点（再规范化一次不变），
// 且没有意外开启任何驱动侧参数。
func assertCanonical(t *testing.T, dsn string, cfg *mysql.Config) {
	t.Helper()
	again, err := tidb.NormalizeDSN(dsn)
	if err != nil {
		t.Fatalf("返回的 DSN 必须可被再次规范化：%v（dsn=%s）", err, tidb.RedactDSN(dsn))
	}
	if again != dsn {
		t.Fatalf("返回的 DSN 必须是规范化不动点：\n got=%s\nagain=%s", tidb.RedactDSN(dsn), tidb.RedactDSN(again))
	}
	if cfg.MultiStatements {
		t.Fatalf("配置字段不得意外启用 multiStatements（dsn=%s）", tidb.RedactDSN(dsn))
	}
	if cfg.AllowAllFiles {
		t.Fatalf("配置字段不得意外启用 allowAllFiles（dsn=%s）", tidb.RedactDSN(dsn))
	}
	if cfg.ParseTime != true || cfg.Loc == nil || cfg.Loc.String() != "UTC" {
		t.Fatalf("强制参数 parseTime/loc 必须生效（dsn=%s）", tidb.RedactDSN(dsn))
	}
}

// setHashFields 用纯环境变量的散列字段路径配置一组最小可用字段。
func setHashFields(t *testing.T, host, user, password, dbname string) {
	t.Helper()
	isolateEnv(t)
	t.Setenv("ARTEX_CONFIG", "") // 无配置文件：纯环境变量启动
	if host != "" {
		t.Setenv("ARTEX_DATABASE_HOST", host)
	}
	if user != "" {
		t.Setenv("ARTEX_DATABASE_USER", user)
	}
	if password != "" {
		t.Setenv("ARTEX_DATABASE_PASSWORD", password)
	}
	if dbname != "" {
		t.Setenv("ARTEX_DATABASE_DBNAME", dbname)
	}
}

// (R2) 库名里带 '/' 或 '?' 时必须拒绝：绝不能"成功"连到 artex_dev 之类的
// 另一个库，也绝不能顺手开启 multiStatements 等驱动参数。
func TestTiDBDSN_DBNameCannotCrossFieldBoundary(t *testing.T) {
	for _, name := range []string{
		"artex_dev?multiStatements=true",
		"artex_dev?charset=utf8",
		"artex_dev?tls=skip-verify&parseTime=false",
		"artex_dev/../other_db",
		"/other_db",
		"?",
		"/",
	} {
		t.Run(name, func(t *testing.T) {
			setHashFields(t, "127.0.0.1", "artex", "PASSWORD", name)

			got, src, err := TiDBDSN()
			if err == nil {
				t.Fatalf("跨越库名/参数边界的 dbname 必须被拒绝；实际得到 dsn=%s source=%s",
					tidb.RedactDSN(got), src)
			}
			if got != "" || src != "" {
				t.Fatalf("报错时不得返回 DSN/来源：dsn=%s src=%s", tidb.RedactDSN(got), src)
			}
			if !strings.Contains(err.Error(), "dbname") {
				t.Fatalf("错误必须指向 database.dbname，got %v", err)
			}
		})
	}
}

// (R2) 常规以及「特殊但安全」的库名必须原样往返，且编码/解码成对稳定。
func TestTiDBDSN_DBNameRoundTrip(t *testing.T) {
	for _, name := range []string{
		"artex_dev",
		"artex-dev.01",
		"ARTEX_DEV2",
		"artex 2",
		"pct%s",
		"hash#1",
		"q&v=1",
		"db:name",
		"库名中文",
	} {
		t.Run(name, func(t *testing.T) {
			setHashFields(t, "127.0.0.1", "artex", "PASSWORD", name)

			got, _, err := TiDBDSN()
			if err != nil {
				t.Fatalf("TiDBDSN: %v", err)
			}
			cfg := mustParseDSN(t, got)
			if cfg.DBName != name {
				t.Fatalf("库名必须原样往返：got %q want %q（dsn=%s）", cfg.DBName, name, tidb.RedactDSN(got))
			}
			if cfg.User != "artex" || cfg.Passwd != "PASSWORD" {
				t.Fatalf("凭据被改写：user=%q passwd=%q", cfg.User, tidb.RedactDSN(got))
			}
			host, port, err := net.SplitHostPort(cfg.Addr)
			if err != nil || host != "127.0.0.1" || port != "4000" {
				t.Fatalf("连接目标被改写：addr=%q err=%v", cfg.Addr, err)
			}
			assertCanonical(t, got, cfg)
		})
	}
}

// (R3) IPv6 host 必须按 net.JoinHostPort 规则组装，端口保持 TiDB 默认 4000。
func TestTiDBDSN_IPv6HostRoundTrip(t *testing.T) {
	for _, tc := range []struct{ envHost, wantHost string }{
		{"::1", "::1"},
		{"[::1]", "::1"},
		{"::", "::"},
		{"2001:db8::10", "2001:db8::10"},
		{"fe80::1%eth0", "fe80::1%eth0"},
	} {
		t.Run(tc.envHost, func(t *testing.T) {
			setHashFields(t, tc.envHost, "artex", "PASSWORD", "artex_dev")

			got, _, err := TiDBDSN()
			if err != nil {
				t.Fatalf("TiDBDSN: %v", err)
			}
			cfg := mustParseDSN(t, got)
			if cfg.Net != "tcp" {
				t.Fatalf("net 应为 tcp，got %q", cfg.Net)
			}
			if wantAddr := "[" + tc.wantHost + "]:4000"; cfg.Addr != wantAddr {
				t.Fatalf("驱动回读 Addr 必须是 %q，got %q（dsn=%s）", wantAddr, cfg.Addr, tidb.RedactDSN(got))
			}
			host, port, err := net.SplitHostPort(cfg.Addr)
			if err != nil {
				t.Fatalf("驱动回读的 addr %q 不是合法 host:port：%v", cfg.Addr, err)
			}
			if host != tc.wantHost {
				t.Fatalf("IPv6 连接目标被改写：host=%q want %q（dsn=%s）", host, tc.wantHost, tidb.RedactDSN(got))
			}
			if port != "4000" {
				t.Fatalf("端口必须保持 TiDB 默认 4000，got %q（dsn=%s）", port, tidb.RedactDSN(got))
			}
			if !strings.Contains(got, "tcp(["+tc.wantHost+"]:4000)") {
				t.Fatalf("DSN 地址段应为 tcp([%s]:4000)，got %s", tc.wantHost, tidb.RedactDSN(got))
			}
			assertCanonical(t, got, cfg)
		})
	}
}

// (R3) 把端口写进 host、或 host 含跨字段字符：直接报错，绝不静默改写目标。
func TestTiDBDSN_RejectMalformedHost(t *testing.T) {
	for _, h := range []string{
		"127.0.0.1:4000",
		"db.internal:4000",
		"not:an:ipv6",
		"local host",
		"a/b",
		"a?b",
		"a(b)",
		"[]",
	} {
		t.Run(h, func(t *testing.T) {
			setHashFields(t, h, "artex", "PASSWORD", "artex_dev")
			got, _, err := TiDBDSN()
			if err == nil {
				t.Fatalf("非法 host %q 必须报错，实际 dsn=%s", h, tidb.RedactDSN(got))
			}
			if got != "" {
				t.Fatalf("报错时不得返回 DSN：%s", tidb.RedactDSN(got))
			}
		})
	}
}

// (R4) 端口：显式非法（非数字/越界）必须报错；只有「未设置」才回退默认 4000。
func TestTiDBDSN_InvalidPortFailsClosed(t *testing.T) {
	for _, bad := range []string{
		"not-a-port",
		"0",
		"-1",
		"65536",
		"9999999999999999999999",
		"4000.5",
		"1e3",
		"0x10",
		"  ",
		"4000abc",
	} {
		t.Run(bad, func(t *testing.T) {
			setHashFields(t, "127.0.0.1", "artex", "PASSWORD", "artex_dev")
			t.Setenv("ARTEX_DATABASE_PORT", bad)

			got, src, err := TiDBDSN()
			if err == nil {
				t.Fatalf("非法端口 %q 必须报错，实际 dsn=%s source=%s", bad, tidb.RedactDSN(got), src)
			}
			if !strings.Contains(err.Error(), "database.port") {
				t.Fatalf("错误必须指向 database.port，got %v", err)
			}
			// 关键：绝不允许静默回退到默认端口。
			if strings.Contains(tidb.RedactDSN(got), ":4000") || got != "" {
				t.Fatalf("非法端口不得返回任何 DSN（更不得回退 4000）：%s", tidb.RedactDSN(got))
			}
		})
	}
}

// (R4) 合法端口往返，以及「未设置端口 → 默认 4000」。
func TestTiDBDSN_PortRoundTrip(t *testing.T) {
	for _, tc := range []struct{ env, want string }{
		{"", "4000"},
		{"4000", "4000"},
		{"4001", "4001"},
		{"1", "1"},
		{"65535", "65535"},
		{" 4001 ", "4001"},
		{"+4001", "4001"},
	} {
		t.Run("port="+tc.env, func(t *testing.T) {
			setHashFields(t, "127.0.0.1", "artex", "PASSWORD", "artex_dev")
			if tc.env != "" {
				t.Setenv("ARTEX_DATABASE_PORT", tc.env)
			}
			got, _, err := TiDBDSN()
			if err != nil {
				t.Fatalf("TiDBDSN: %v", err)
			}
			cfg := mustParseDSN(t, got)
			_, port, err := net.SplitHostPort(cfg.Addr)
			if err != nil {
				t.Fatalf("addr %q 不合法：%v", cfg.Addr, err)
			}
			if port != tc.want {
				t.Fatalf("端口 got %q want %q（dsn=%s）", port, tc.want, tidb.RedactDSN(got))
			}
			assertCanonical(t, got, cfg)
		})
	}
}

// (R4) 配置文件里的端口同样严格：YAML 非整数类型必须报错，不静默回退。
func TestTiDBDSN_PortTypeFromYAML(t *testing.T) {
	for _, tc := range []struct{ name, body, wantPort string }{
		{"int", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  password: \"PASSWORD\"\n  dbname: \"artex_dev\"\n  port: 4001\n", "4001"},
		{"string", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  password: \"PASSWORD\"\n  dbname: \"artex_dev\"\n  port: \"4001\"\n", "4001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateEnv(t)
			writeConfig(t, tc.body)
			got, _, err := TiDBDSN()
			if err != nil {
				t.Fatalf("TiDBDSN: %v", err)
			}
			cfg := mustParseDSN(t, got)
			_, port, _ := net.SplitHostPort(cfg.Addr)
			if port != tc.wantPort {
				t.Fatalf("端口 got %q want %q（dsn=%s）", port, tc.wantPort, tidb.RedactDSN(got))
			}
			assertCanonical(t, got, cfg)
		})
	}

	for _, tc := range []struct{ name, body string }{
		{"bool", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  dbname: \"artex_dev\"\n  port: true\n"},
		{"float", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  dbname: \"artex_dev\"\n  port: 4000.5\n"},
		{"text", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  dbname: \"artex_dev\"\n  port: not-a-port\n"},
		{"list", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  dbname: \"artex_dev\"\n  port: [4000]\n"},
		{"out_of_range", "database:\n  host: \"127.0.0.1\"\n  user: \"artex\"\n  dbname: \"artex_dev\"\n  port: 70000\n"},
	} {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			isolateEnv(t)
			writeConfig(t, tc.body)
			got, src, err := TiDBDSN()
			if err == nil {
				t.Fatalf("非法 YAML 端口必须报错，实际 dsn=%s source=%s", tidb.RedactDSN(got), src)
			}
			if !strings.Contains(err.Error(), "database.port") {
				t.Fatalf("错误必须指向 database.port，got %v", err)
			}
			if _, err := Load(); err == nil {
				t.Fatalf("Load 也必须拒绝非法 YAML 端口")
			}
		})
	}
}

// (R5) 密码按驱动真实语法保留：'/'、'?'、'@'、':'、空格等非必要限制全部移除，
// 组装结果必须能被驱动逐字符回读。
func TestTiDBDSN_SpecialCharacterPasswordsRoundTrip(t *testing.T) {
	for _, pw := range []string{
		"PASS/WORD?",
		"p@ss:word",
		"a?b/c@d:e",
		"@leading",
		"trailing@",
		"  spaced  password  ",
		"hunter2",
		"%P%ss word?#/@",
		"ünicode密码?/@",
		"!#%&()=",
	} {
		t.Run(pw, func(t *testing.T) {
			setHashFields(t, "127.0.0.1", "artex", pw, "artex_dev")

			got, _, err := TiDBDSN()
			if err != nil {
				t.Fatalf("TiDBDSN: %v", err)
			}
			cfg := mustParseDSN(t, got)
			if cfg.Passwd != pw {
				t.Fatalf("密码必须原样往返：got %q want %q（dsn=%s）", cfg.Passwd, pw, tidb.RedactDSN(got))
			}
			if cfg.User != "artex" {
				t.Fatalf("user 必须保持 artex，got %q（dsn=%s）", cfg.User, tidb.RedactDSN(got))
			}
			if cfg.DBName != "artex_dev" {
				t.Fatalf("库名必须保持 artex_dev，got %q（dsn=%s）", cfg.DBName, tidb.RedactDSN(got))
			}
			host, port, _ := net.SplitHostPort(cfg.Addr)
			if host != "127.0.0.1" || port != "4000" {
				t.Fatalf("连接目标被改写：addr=%q", cfg.Addr)
			}
			assertCanonical(t, got, cfg)
		})
	}
}

// (R5) user 里唯一真正会破坏字段切分的是 ':'：必须报错而不是把 user 截断。
func TestTiDBDSN_RejectUserWithColon(t *testing.T) {
	setHashFields(t, "127.0.0.1", "art:ex", "PASSWORD", "artex_dev")
	got, _, err := TiDBDSN()
	if err == nil {
		t.Fatalf("user 含 ':' 必须报错，实际 dsn=%s", tidb.RedactDSN(got))
	}
	if !strings.Contains(err.Error(), "user") {
		t.Fatalf("错误必须指向 database.user，got %v", err)
	}
}

// (R5) 缺少 user 时同样 fail-closed，绝不组装出无凭据连接串。
func TestTiDBDSN_RejectMissingUser(t *testing.T) {
	setHashFields(t, "127.0.0.1", "", "PASSWORD", "artex_dev")
	if _, _, err := TiDBDSN(); err == nil {
		t.Fatalf("缺 database.user 必须报错")
	}
}
