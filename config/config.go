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
// 最高优先级：ARTEX_TIDB_DSN（整体覆盖，非空即直接采用其取值）。它只决定
// 「DSN 的值来自哪里」，不豁免第 1 条：显式指定的配置文件依旧必须存在且可
// 解析，否则 fail-closed 报错。
//
// PostgreSQL（postgres:// 协议与 ARTEX_PG_DSN）已彻底废除：任何来源出现
// postgres:// / postgresql:// 一律显式报错，严禁静默连错库。所有 DSN 无论
// 来自整体连接串还是散列字段组装，最终都经过 db/tidb.NormalizeDSN，强制
// 补齐 charset=utf8mb4&parseTime=true&loc=UTC&time_zone='%2B00%3A00'。
package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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

	// envDatabasePort 是 database.port 的环境变量映射名，仅用于错误提示。
	envDatabasePort = "ARTEX_DATABASE_PORT"

	// portKey 是端口在 Viper 里的键名。
	portKey = "database.port"

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
//
// 端口是唯一非字符串字段，必须走 readPort 的严格解析：任何「显式提供但非法」
// 的值（非数字、小数、bool、超范围）都直接报错，绝不静默当成未配置。
func readConfig(v *viper.Viper) (Config, error) {
	var c Config
	c.Database.DSN = strings.TrimSpace(v.GetString("database.dsn"))
	c.Database.Host = strings.TrimSpace(v.GetString("database.host"))
	port, err := readPort(v)
	if err != nil {
		return Config{}, err
	}
	c.Database.Port = port
	c.Database.User = strings.TrimSpace(v.GetString("database.user"))
	// 密码保留原始字符：绝不 TrimSpace（Spec §3.1.1 第 3 条）。
	c.Database.Password = v.GetString("database.password")
	c.Database.DBName = strings.TrimSpace(v.GetString("database.dbname"))
	c.Database.SSLMode = strings.TrimSpace(v.GetString("database.sslmode"))
	c.SkillDir = strings.TrimSpace(v.GetString("skill_dir"))
	return c, nil
}

// readPort 严格读取 database.port，区分「未设置」与「显式提供但非法」：
//   - 未设置（配置文件无该键且环境变量不存在/为空）→ 返回 0，由 buildDSN
//     回退默认端口 4000；
//   - 显式提供但不是 1..65535 的整数 → 返回错误。
//
// 严禁改用 v.GetInt / cast.ToInt：它们在转换失败时静默返回 0，运维把
// ARTEX_DATABASE_PORT 误写成 "not-a-port" 就会被当成「未配置」而悄悄连到
// 默认端口 4000（审查 R4）。环境变量始终是字符串，YAML 可能是 int/float64/
// 字符串，因此这里按原始类型逐项判定，其余类型（bool/map/slice/nil 指针等）
// 一律拒绝。
func readPort(v *viper.Viper) (int, error) {
	raw := v.Get(portKey)
	if raw == nil {
		return 0, nil // 未设置：允许（纯环境变量或纯默认端口启动）
	}
	switch t := raw.(type) {
	case int:
		return checkPort(int64(t), raw)
	case int64:
		return checkPort(t, raw)
	case uint64:
		if t > math.MaxInt64 {
			return 0, portError(raw)
		}
		return checkPort(int64(t), raw)
	case float64:
		// 先在 float 域做范围检查，避免溢出到 int64 的未定义行为。
		if math.IsNaN(t) || t < 1 || t > math.MaxInt64 {
			return 0, portError(raw)
		}
		if t != math.Trunc(t) {
			return 0, portError(raw)
		}
		return checkPort(int64(t), raw)
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		if err != nil {
			return 0, portError(raw)
		}
		return checkPort(n, raw)
	default:
		return 0, portError(raw)
	}
}

// checkPort 校验端口范围（1..65535），越界即报错，绝不回退默认端口。
func checkPort(n int64, raw interface{}) (int, error) {
	if n < 1 || n > 65535 {
		return 0, portError(raw)
	}
	return int(n), nil
}

// portError 统一端口错误文案：只回显值与键名，指明正确写法。
func portError(raw interface{}) error {
	return fmt.Errorf("非法 %s=%v：端口必须是 1..65535 的整数（也可在配置文件中写 database.port）。"+
		"拒绝静默回退默认端口 %d，请先修正配置", portKey, raw, defaultTiDBPort)
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
	return readConfig(v)
}

// TiDBDSN 解析 TiDB 连接串（go-sql-driver 语法）并报告来源（供启动日志）。
//
// 优先级（P08 Spec §3.1.1）：
//  1. 环境变量 ARTEX_TIDB_DSN（最高，整体覆盖）；
//  2. database.dsn（环境变量 ARTEX_DATABASE_DSN 或配置文件 database.dsn）；
//  3. database.host / port / user / password / dbname 散列字段组装（默认端口 4000）。
//
// 最高优先级只决定「DSN 的取值来自哪里」，不豁免配置载体的有效性：无论
// ARTEX_TIDB_DSN 是否设置，都先完成配置文件寻址与解析（ARTEX_CONFIG 显式指定
// 时必须存在且可解析）以及字段类型校验（非法端口等），任一失败一律
// fail-closed 报错，严禁用高优先级 DSN 掩盖损坏配置（审查 R1）。
//
// 所有分支统一交给 tidb.NormalizeDSN：强制补齐 charset=utf8mb4、parseTime=true、
// loc=UTC、time_zone='%2B00%3A00'，并拒绝缺 user / 缺库名 / 强制参数冲突。
// 检测到 postgres:// / postgresql:// 一律显式报错。没有任何来源时报错，
// 不提供内置默认连接（严禁静默连到错误的库）。
func TiDBDSN() (dsn, source string, err error) {
	// 0. 先加载并校验配置载体：ARTEX_CONFIG 显式指定却不存在的文件、损坏的
	//    YAML、非法端口等，都必须在这里 fail-closed，先于任何优先级判定。
	v, path, err := loadViper()
	if err != nil {
		return "", "", err
	}
	d, err := readConfig(v)
	if err != nil {
		return "", "", err
	}

	// 1. 最高优先级：显式整体覆盖（只覆盖取值，不豁免上面已完成的载体校验）。
	if raw := strings.TrimSpace(os.Getenv(EnvTiDBDSN)); raw != "" {
		return normalizeDSN(raw, "环境变量 "+EnvTiDBDSN)
	}

	db := d.Database
	// baseLabel 描述本次生效的配置载体：命中配置文件则指向文件，否则是纯环境变量启动。
	baseLabel := "环境变量 ARTEX_DATABASE_*"
	if path != "" {
		baseLabel = "配置文件 " + path
	}

	// PostgreSQL 残留参数：显式拒绝而不是静默忽略用户意图。
	if db.SSLMode != "" {
		return "", "", fmt.Errorf("已废除 PostgreSQL 连接参数 sslmode（来源：%s）。TiDB 请在 database.dsn 中按需指定 tls 参数", describeSource(baseLabel, "database.sslmode"))
	}

	// 2. 整体连接串（环境变量映射优先于配置文件同名字段）。
	if db.DSN != "" {
		label := baseLabel
		if strings.TrimSpace(os.Getenv(EnvDatabaseDSN)) != "" {
			label = "环境变量 " + EnvDatabaseDSN
		}
		return normalizeDSN(db.DSN, describeSource(label, "database.dsn"))
	}

	// 3. 散列字段组装。
	if db.Host != "" || db.User != "" || db.DBName != "" || db.Port != 0 {
		label := baseLabel
		if path != "" && anyDatabaseEnvSet() {
			label = baseLabel + " + 环境变量 ARTEX_DATABASE_*"
		}
		raw, err := db.buildDSN()
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
			"请使用 go-sql-driver 语法 user:password@tcp(host:%d)/dbname（库名含 / ? 或 user 含 : 等无法散列组装的字符时，请改用完整 database.dsn 或 %s）：%w",
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
//
// 字段边界严格对齐 go-sql-driver v1.9.3 的 parseDSN：
//   - 先取整串最后一个 '/' 作为库名起点，库名段再取第一个 '?' 作为参数起点，
//     库名段最后经 url.PathUnescape 解码；
//   - 再在该 '/' 之前取最后一个 '@' 切分凭据段，凭据段取第一个 ':' 分成
//     user / password，凭据不做任何百分号解码。
//
// 由此推出本函数唯一的两条字符限制（其余字符都由驱动原样保留）：
//   - database.dbname 不能含 '/' 或 '?'：那会直接跨越库名/参数边界，把后面的
//     文本读成驱动选项（例如 "artex_dev?multiStatements=true"）。这类库名也无法
//     在 tidb.formatDSN 的 PathEscape 序列化里保留，因此显式拒绝而不是静默连到
//     另一个库或意外开启驱动选项（审查 R2）；其他特殊字符（空格、%、# 等）由
//     url.PathEscape 与驱动的 PathUnescape 成对往返，无需禁止；
//   - database.user 不能含 ':'：驱动按凭据段第一个 ':' 切分，会把 user 截断并
//     把剩余部分读成密码。
//
// database.password 不做任何字符禁令也不 TrimSpace：驱动的字段切分保证
// '/', '?', '@', ':', 空格等字符都能原样往返（审查 R5）。地址段用
// net.JoinHostPort 组装，IPv6 字面量自动补方括号（审查 R3）。
func (d Database) buildDSN() (string, error) {
	port := d.Port
	if port == 0 {
		port = defaultTiDBPort // 只有「未设置」才会走到这里：非法端口已在 readPort 报错
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("非法端口 %d（应为 1..65535）", d.Port)
	}
	hostPort, err := joinHostPort(d.Host, port)
	if err != nil {
		return "", err
	}
	dbName, err := encodeDBName(d.DBName)
	if err != nil {
		return "", err
	}
	if strings.Contains(d.User, ":") {
		return "", errors.New("database.user 不能包含 ':'（DSN 用它切分 user/password），请改用完整 database.dsn")
	}
	if d.User == "" {
		return "", errors.New("缺少 database.user：TiDB 连接串必须是 user:password@tcp(host:port)/dbname 语法")
	}
	var sb strings.Builder
	sb.WriteString(d.User)
	if d.Password != "" {
		sb.WriteByte(':')
		sb.WriteString(d.Password)
	}
	sb.WriteString("@tcp(")
	sb.WriteString(hostPort)
	sb.WriteString(")/")
	sb.WriteString(dbName)
	return sb.String(), nil
}

// joinHostPort 按 net.JoinHostPort 规则组装地址段的 host:port：IPv6 字面量
// （含已写成 "[::1]" 的形式）统一补方括号，避免 "::1" 被驱动读成
// "[::1:4000]:3306" 这类连接目标与端口双双被改写的结果（审查 R3）。
// 含 '/'、'?'、'@'、'('、')'、空白的 host 会跨越 DSN 字段边界，直接拒绝；
// 含 ':' 但整体不是合法 IPv6 字面量（例如把端口误写进 host）同样拒绝，
// 绝不静默改写运维声明的连接目标。
func joinHostPort(rawHost string, port int) (string, error) {
	host := rawHost
	if host == "" {
		host = defaultTiDBHost
	}
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1] // 允许 [::1] / [fe80::1%eth0] 这类已带括号的写法
	}
	if host == "" || strings.ContainsAny(host, " \t\n\r/?@()") {
		return "", fmt.Errorf("非法 database.host %q：不能为空，也不能包含 / ? @ ( ) 或空白字符", rawHost)
	}
	if strings.Contains(host, ":") {
		literal := host
		if i := strings.IndexByte(literal, '%'); i > 0 {
			literal = literal[:i] // 去掉 IPv6 zone id 再判定字面量
		}
		// 合法 IPv6 字面量至少含两个 ':'（"::1" 即两个），因此
		// "127.0.0.1:4000" 这种「host 里塞端口」的写法会被拒绝。
		if net.ParseIP(literal) == nil || strings.Count(literal, ":") < 2 {
			return "", fmt.Errorf("非法 database.host %q：含 ':' 但不是合法 IPv6 地址；端口请用 database.port 或 %s", rawHost, envDatabasePort)
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// encodeDBName 校验并编码逻辑库名，使其安全落入 DSN 的库名段。驱动对该段做
// url.PathUnescape，因此这里成对使用 url.PathEscape（%、空格、# 等均可原样
// 往返）。含 '/' 或 '?' 的库名会跨越字段边界、无法保留，必须显式拒绝，
// 严禁静默连到另一个库或意外启用驱动参数（审查 R2）。
func encodeDBName(name string) (string, error) {
	if name == "" {
		return "", errors.New("缺少 database.dbname：ARTEX 只连接管理员预建库，不创建数据库")
	}
	if strings.ContainsAny(name, "/?") {
		return "", fmt.Errorf("非法 database.dbname %q：'/' 与 '?' 是 DSN 的库名/参数边界字符，无法安全组装，请改用完整 database.dsn", name)
	}
	return url.PathEscape(name), nil
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
