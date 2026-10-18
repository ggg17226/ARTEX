// Package config 使用 Viper 加载 config.yaml，并解析 TiDB 连接串。
//
// 配置文件寻址优先级（P08 Spec §3.1.1）：
//  1. 环境变量 ARTEX_CONFIG —— 一旦显式设置，文件必须存在且可解析，
//     否则 fail-closed 直接报错，严禁静默回退到其他文件或静默忽略；
//  2. ./config.yaml（当前工作目录）；
//  3. config.BaseDir()/config.yaml（运行中的二进制所在目录）。
//     2/3 属于隐式寻址：都不存在时允许「纯环境变量启动」。
//
// 环境变量绑定：Viper 前缀 ARTEX + 键分隔符 "." → "_"，因此
// database.dsn → ARTEX_DATABASE_DSN，database.host → ARTEX_DATABASE_HOST，
// database.port → ARTEX_DATABASE_PORT，skill_dir → ARTEX_SKILL_DIR …
// 环境变量优先于配置文件同名字段。
//
// 最高优先级：ARTEX_TIDB_DSN（整体覆盖，非空即直接采用）。
//
// PostgreSQL（postgres:// 协议与 ARTEX_PG_DSN）已彻底废除：任何来源出现
// postgres:// / postgresql:// 一律显式报错，严禁静默连错库。所有 DSN 无论
// 来自整体连接串还是散列字段组装，最终都经过 db/tidb.NormalizeDSN，强制
// 补齐 charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'。
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Autumn-27/artex/db/tidb"
	"github.com/spf13/viper"
)

const (
	// EnvConfigFile 显式指定配置文件路径（设置后必须可用，否则 fail-closed）。
	EnvConfigFile = "ARTEX_CONFIG"
	// EnvTiDBDSN 是最高优先级的整体连接串覆盖。
	EnvTiDBDSN = "ARTEX_TIDB_DSN"
	// EnvSkillDir 覆盖 skill 根目录。
	EnvSkillDir = "ARTEX_SKILL_DIR"
	// EnvDatabaseDSN 是 database.dsn 的环境变量映射（Viper 前缀 + 键替换生成）。
	EnvDatabaseDSN = "ARTEX_DATABASE_DSN"

	// envPrefix 是 Viper 的环境变量前缀；database.dsn → ARTEX_DATABASE_DSN。
	envPrefix = "ARTEX"
	// configFileName 是隐式寻址使用的文件名。
	configFileName = "config.yaml"
	// defaultTiDBPort 是 TiDB 标准端口（Spec §3.1.1：默认端口 4000）。
	defaultTiDBPort = 4000
	// defaultTiDBHost 是散列字段未提供 host 时使用的回环地址。
	defaultTiDBHost = "127.0.0.1"
)

// Database 是 TiDB 连接配置：可直接给出 DSN（go-sql-driver 语法
// user:password@tcp(host:port)/dbname?params），也可用散列字段拼装。
// 注意：PostgreSQL 专用字段已随 pgx 一并废除，SSLMode 仅用于识别并拒绝旧配置。
type Database struct {
	DSN      string `mapstructure:"dsn" yaml:"dsn" json:"dsn"`
	Host     string `mapstructure:"host" yaml:"host" json:"host"`
	Port     int    `mapstructure:"port" yaml:"port" json:"port"`
	User     string `mapstructure:"user" yaml:"user" json:"user"`
	Password string `mapstructure:"password" yaml:"password" json:"password"`
	DBName   string `mapstructure:"dbname" yaml:"dbname" json:"dbname"`
	// SSLMode 是已废除的 PostgreSQL 参数；非空时报错而不是静默忽略。
	SSLMode string `mapstructure:"sslmode" yaml:"sslmode" json:"sslmode"`
}

// Config 是配置文件（config.yaml）的形状。
type Config struct {
	Database Database `mapstructure:"database" yaml:"database" json:"database"`
	SkillDir string   `mapstructure:"skill_dir" yaml:"skill_dir" json:"skill_dir"`
}

// BaseDir is the directory that anchors all runtime artifacts (config.yaml and
// the data/ store). It is the directory the running binary lives in, so a
// distributed executable keeps its files next to itself on any OS (Windows,
// Linux, …) regardless of the working directory it is launched from.
//
// When launched via `go run`, the binary is throwaway: it sits either in a temp
// build dir (cache miss → fresh link) OR straight inside the Go build cache
// (cache hit → run from $GOCACHE/.../...-d). We detect both and fall back to the
// current working directory so dev artifacts (data/, transcripts) and config
// resolve against the project dir, not the throwaway binary's location.
func BaseDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	dir := filepath.Dir(exe)
	if isGoRunDir(dir) {
		return "." // throwaway `go run` binary → use CWD
	}
	return dir
}

// isGoRunDir reports whether dir is where `go run` parked its executable: under
// the system temp dir (cache miss), or anywhere inside a Go build cache
// (…/go-build/…, the cache-hit case — NOT under os.TempDir(), which is why the
// old temp-only check failed intermittently). In both cases the binary is
// throwaway, so config/data must resolve against the CWD.
func isGoRunDir(dir string) bool {
	if tmp := os.TempDir(); tmp != "" {
		if rel, err := filepath.Rel(tmp, dir); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	for _, seg := range strings.Split(filepath.ToSlash(dir), "/") {
		if seg == "go-build" {
			return true
		}
	}
	return false
}

// Path returns the config file path this process resolves (or would resolve):
// env ARTEX_CONFIG first, then ./config.yaml, then BaseDir()/config.yaml. When
// no candidate exists it returns the CWD path so startup logs and "not found"
// messages point at the project dir the user most likely expected.
//
// 仅用于日志与错误提示；真正读取配置请用 Load / TiDBDSN（它们走 loadViper
// 并带有 fail-closed 语义）。
func Path() string {
	p, _ := resolveConfigPath()
	return p
}

// resolveConfigPath 返回寻址后的路径与 explicit（是否由 ARTEX_CONFIG 显式指定）。
func resolveConfigPath() (path string, explicit bool) {
	if v := strings.TrimSpace(os.Getenv(EnvConfigFile)); v != "" {
		return v, true
	}
	var candidates []string
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, configFileName))
	}
	candidates = append(candidates, filepath.Join(BaseDir(), configFileName))
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c, false
		}
	}
	return candidates[0], false
}

// loadViper 按寻址优先级构造绑定了环境变量的 Viper 实例。
//
// fail-closed：ARTEX_CONFIG 显式指定但文件不存在 / 不可读 / 解析失败时直接返回
// 错误，绝不静默回退；隐式寻址命中的文件解析失败同样报错（只有「文件不存在」
// 才允许跳过读取，走纯环境变量启动）。
func loadViper() (v *viper.Viper, path string, err error) {
	path, explicit := resolveConfigPath()

	v = viper.New()
	v.SetEnvPrefix(envPrefix)                          // database.dsn → ARTEX_DATABASE_DSN
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // 嵌套键的 "." 换成 "_"
	v.AutomaticEnv()                                   // 环境变量优先于配置文件
	v.SetConfigType("yaml")                            // 与扩展名解耦：/dev/null、.conf 管道都能按 YAML 解析

	if fi, statErr := os.Stat(path); statErr != nil {
		if explicit {
			return nil, path, fmt.Errorf("配置文件 %s 无法访问（%s 已显式指定，拒绝静默回退）：%w", path, EnvConfigFile, statErr)
		}
		// 隐式寻址未命中：允许纯环境变量启动（Spec §3.1.1）。
		return v, "", nil
	} else if fi.IsDir() {
		return nil, path, fmt.Errorf("配置文件 %s 是目录，不是 %s", path, configFileName)
	}

	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, path, fmt.Errorf("解析配置文件 %s 失败：%w", path, err)
	}
	return v, path, nil
}

// readConfig 从 Viper 实例里取出显式键位。环境变量（ARTEX_ 前缀）优先于文件。
func readConfig(v *viper.Viper) Config {
	var c Config
	c.Database.DSN = strings.TrimSpace(v.GetString("database.dsn"))
	c.Database.Host = strings.TrimSpace(v.GetString("database.host"))
	c.Database.Port = v.GetInt("database.port")
	c.Database.User = strings.TrimSpace(v.GetString("database.user"))
	// 密码保留原始字符：绝不 TrimSpace（Spec §3.1.1 第 3 条）。
	c.Database.Password = v.GetString("database.password")
	c.Database.DBName = strings.TrimSpace(v.GetString("database.dbname"))
	c.Database.SSLMode = strings.TrimSpace(v.GetString("database.sslmode"))
	c.SkillDir = strings.TrimSpace(v.GetString("skill_dir"))
	return c
}

// Load 读取配置文件与环境变量（后者优先），返回运行时配置。
//
// 没有配置文件不是错误（允许纯环境变量启动）；但 ARTEX_CONFIG 显式指定的文件
// 缺失或解析失败会返回错误（fail-closed）。
func Load() (Config, error) {
	v, _, err := loadViper()
	if err != nil {
		return Config{}, err
	}
	return readConfig(v), nil
}

// TiDBDSN 解析 TiDB 连接串（go-sql-driver 语法）并报告来源（供启动日志）。
//
// 优先级（P08 Spec §3.1.1）：
//  1. 环境变量 ARTEX_TIDB_DSN（最高，整体覆盖）；
//  2. database.dsn（环境变量 ARTEX_DATABASE_DSN 或配置文件 database.dsn）；
//  3. database.host / port / user / password / dbname 散列字段组装（默认端口 4000）。
//
// 所有分支统一交给 tidb.NormalizeDSN：强制补齐 charset=utf8mb4、parseTime=true、
// loc=UTC、time_zone='%2B00%3A00'，并拒绝缺 user / 缺库名 / 强制参数冲突。
// 检测到 postgres:// / postgresql:// 一律显式报错。没有任何来源时报错，
// 不提供内置默认连接（严禁静默连到错误的库）。
func TiDBDSN() (dsn, source string, err error) {
	// 1. 最高优先级：显式整体覆盖。
	if raw := strings.TrimSpace(os.Getenv(EnvTiDBDSN)); raw != "" {
		return normalizeDSN(raw, "环境变量 "+EnvTiDBDSN)
	}

	v, path, err := loadViper()
	if err != nil {
		return "", "", err
	}
	d := readConfig(v).Database
	// baseLabel 描述本次生效的配置载体：命中配置文件则指向文件，否则是纯环境变量启动。
	baseLabel := "环境变量 ARTEX_DATABASE_*"
	if path != "" {
		baseLabel = "配置文件 " + path
	}

	// PostgreSQL 残留参数：显式拒绝而不是静默忽略用户意图。
	if d.SSLMode != "" {
		return "", "", fmt.Errorf("已废除 PostgreSQL 连接参数 sslmode（来源：%s）。TiDB 请在 database.dsn 中按需指定 tls 参数", describeSource(baseLabel, "database.sslmode"))
	}

	// 2. 整体连接串（环境变量映射优先于配置文件同名字段）。
	if d.DSN != "" {
		label := baseLabel
		if strings.TrimSpace(os.Getenv(EnvDatabaseDSN)) != "" {
			label = "环境变量 " + EnvDatabaseDSN
		}
		return normalizeDSN(d.DSN, describeSource(label, "database.dsn"))
	}

	// 3. 散列字段组装。
	if d.Host != "" || d.User != "" || d.DBName != "" || d.Port != 0 {
		label := baseLabel
		if path != "" && anyDatabaseEnvSet() {
			label = baseLabel + " + 环境变量 ARTEX_DATABASE_*"
		}
		raw, err := d.buildDSN()
		if err != nil {
			return "", "", fmt.Errorf("组装 TiDB 连接串失败（来源：%s）：%w", describeSource(label, "database 字段"), err)
		}
		return normalizeDSN(raw, describeSource(label, "database 字段"))
	}

	return "", "", fmt.Errorf(
		"未找到数据库配置：环境变量 %s 未设置，环境变量 ARTEX_DATABASE_*（database.dsn/host/user/dbname）未设置，"+
			"配置文件 %s 也未提供 database 段。请创建 config.yaml 或设置上述环境变量后重试",
		EnvTiDBDSN, path,
	)
}

// describeSource 拼出面向运维的来源描述，例如
// "配置文件 /etc/artex/config.yaml (database.dsn)"。
func describeSource(label, key string) string {
	return label + " (" + key + ")"
}

// anyDatabaseEnvSet 报告是否有 ARTEX_DATABASE_* 散列环境变量参与覆盖。
func anyDatabaseEnvSet() bool {
	for _, k := range []string{
		"ARTEX_DATABASE_HOST", "ARTEX_DATABASE_PORT", "ARTEX_DATABASE_USER",
		"ARTEX_DATABASE_PASSWORD", "ARTEX_DATABASE_DBNAME",
	} {
		if strings.TrimSpace(os.Getenv(k)) != "" {
			return true
		}
	}
	return false
}

// normalizeDSN 校验并规范化单条连接串：postgres:// 显式拒绝，其余交给
// tidb.NormalizeDSN（补齐 UTC 会话时区与 utf8mb4 等强制参数）。
// 错误信息里只带来源，不回显原始连接串，避免凭据进日志。
func normalizeDSN(raw, source string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if isPostgresDSN(raw) {
		return "", "", fmt.Errorf(
			"已彻底废除 PostgreSQL：来源 %s 提供了 postgres:// 连接串。"+
				"TiDB 请使用 go-sql-driver 语法 user:password@tcp(host:%d)/dbname（可直接放入 %s 或 config.yaml 的 database.dsn）",
			source, defaultTiDBPort, EnvTiDBDSN)
	}
	normalized, err := tidb.NormalizeDSN(raw)
	if err != nil {
		return "", "", fmt.Errorf("无效 TiDB 连接串（来源：%s）。"+
			"请使用 go-sql-driver 语法 user:password@tcp(host:%d)/dbname（散列字段含 / ? 等保留字符时请改用完整 database.dsn 或 %s）：%w",
			source, defaultTiDBPort, EnvTiDBDSN, err)
	}
	return normalized, source, nil
}

// isPostgresDSN 报告连接串是否使用了被废除的 PostgreSQL 协议前缀。
func isPostgresDSN(dsn string) bool {
	s := strings.ToLower(strings.TrimSpace(dsn))
	return strings.HasPrefix(s, "postgres:") || strings.HasPrefix(s, "postgresql:")
}

// buildDSN 用散列字段组装 go-sql-driver DSN（host 默认 127.0.0.1，端口默认 4000）。
// 用户名/口令/库名里出现 DSN 语法的保留字符时直接报错——这类凭据请改用完整
// database.dsn，绝不做会被二次解码的“聪明”转义。
func (d Database) buildDSN() (string, error) {
	host := d.Host
	if host == "" {
		host = defaultTiDBHost
	}
	port := d.Port
	if port == 0 {
		port = defaultTiDBPort
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("非法端口 %d（应为 1..65535）", d.Port)
	}
	if d.DBName == "" {
		return "", errors.New("缺少 database.dbname：ARTEX 只连接管理员预建库，不创建数据库")
	}
	if strings.ContainsAny(d.User, "/?@ \t\n") {
		return "", errors.New("database.user 不能包含 / ? @ 或空白字符，请改用完整 database.dsn")
	}
	if strings.ContainsAny(d.Password, "/?") {
		return "", errors.New("database.password 不能包含 / ? 保留字符，请改用完整 database.dsn")
	}
	if d.Password == "" {
		return fmt.Sprintf("%s@tcp(%s:%d)/%s", d.User, host, port, d.DBName), nil
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", d.User, d.Password, host, port, d.DBName), nil
}

// SkillDir returns the skill root directory with precedence:
//
//	env ARTEX_SKILL_DIR  >  config file (skill_dir)  >  BaseDir()/skills
//
// The directory is created if it does not exist. 配置文件读取失败时退回默认
// 目录（启动闭环会在 TiDBDSN 那一步以明确错误终止，不依赖此处报错）。
func SkillDir() string {
	var d string
	if v := strings.TrimSpace(os.Getenv(EnvSkillDir)); v != "" {
		d = v
	} else if c, err := Load(); err == nil && c.SkillDir != "" {
		d = c.SkillDir
	} else {
		d = filepath.Join(BaseDir(), "skills")
	}
	_ = os.MkdirAll(d, 0o755)
	return d
}

// Redact 返回脱敏后的连接串（密码替换为 ****），供启动日志使用。
// 解析失败时返回安全占位符而不是原串。
//
// Deprecated: 请直接使用 db/tidb.RedactDSN；此别名仅为 P08 迁移期间的旧调用方
// （db/db.go，Task 1.4 收尾）保留。
func Redact(dsn string) string {
	return tidb.RedactDSN(dsn)
}

// PostgresDSN 已随 PostgreSQL 依赖一并废除：它永远返回错误，绝不返回连接串，
// 从而杜绝“静默连错库”。
//
// Deprecated: 请改用 TiDBDSN。本符号仅为 P08 Task 1.4 改造 db/db.go 之前的
// 调用方保留编译兼容。
func PostgresDSN() (dsn, source string, err error) {
	return "", "", fmt.Errorf(
		"PostgreSQL 连接配置已彻底废除（ARTEX 已迁移至 TiDB）：请改用 %s 或 config.yaml 的 database 段（TiDB go-sql-driver 语法）",
		EnvTiDBDSN)
}
