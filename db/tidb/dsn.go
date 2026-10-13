package tidb

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// requiredDSNParams 是 spec L83「DSN 每物理连接 UTC」的强制参数集合。
var requiredDSNParams = map[string]string{
	"parseTime": "true",
	"loc":       "UTC",
	"time_zone": "+00:00",
	"charset":   "utf8mb4",
}

// NormalizeDSN 解析并规范化 TiDB DSN（go-sql-driver 语法
// user:password@tcp(host:port)/dbname?params）：
//   - 缺 user / 缺库名 → 错误（ARTEX 只连接预建库，绝不隐式建库）；
//   - 必需参数（parseTime/loc/time_zone/charset）缺失 → 补齐；
//     已存在但值不同 → 错误，不静默覆盖（用户显式意图冲突必须暴露）；
//   - 其余用户参数原样保留；
//   - 幂等：对已规范化的 DSN 再次调用返回原串。
//
// 纯函数，无 I/O，可被 P01 OpenTiDB 与 P08 config.TiDBDSN 共用。
//
// 序列化说明：go-sql-driver 的 Config.FormatDSN 不 URL 编码 params，
// 输出 time_zone=+00:00 再经 ParseDSN（内部 url.ParseQuery）回读时 '+'
// 会被解码成空格。本包因此自实现 formatDSN：params 一律用
// url.Values.Encode 百分号编码（time_zone → time_zone=%2B00%3A00），
// 保证 ParseDSN 往返稳定、幂等成立。
func NormalizeDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("normalize DSN: %w", err)
	}
	// go-sql-driver v1.9.3 的 ParseDSN 将已识别参数（parseTime/loc/charset/
	// timeout/readTimeout 等）消费到 Config 专属字段，未识别参数才写入
	// cfg.Params，且无 ?params 段时 Params 保持 nil。从原始 DSN 的 query
	// 段还原用户显式参数，冲突检测与用户参数保留均依赖 cfg.Params。
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	if qs := explicitParams(dsn); qs != "" {
		if extra, err := url.ParseQuery(qs); err == nil {
			for k, vs := range extra {
				if len(vs) > 0 {
					cfg.Params[k] = vs[len(vs)-1]
				}
			}
		}
	}
	if cfg.User == "" {
		return "", fmt.Errorf("normalize DSN: missing user (go-sql-driver 语法 user:password@tcp(host:port)/dbname)")
	}
	if cfg.DBName == "" {
		return "", fmt.Errorf("normalize DSN: missing database name — ARTEX 只连接管理员预建库，不创建数据库")
	}
	for key, want := range requiredDSNParams {
		got, present := cfg.Params[key]
		if !present {
			cfg.Params[key] = want
			continue
		}
		if got != want {
			return "", fmt.Errorf("normalize DSN: param %q=%q conflicts with required value %q", key, got, want)
		}
	}
	return formatDSN(cfg), nil
}

// formatDSN 把 Config 序列化为 go-sql-driver DSN。params 段用
// url.Values.Encode（键排序、百分号编码）统一编码，保证 ParseDSN 往返稳定。
func formatDSN(cfg *mysql.Config) string {
	var sb strings.Builder
	sb.WriteString(cfg.User)
	if cfg.Passwd != "" {
		sb.WriteString(":" + cfg.Passwd)
	}
	sb.WriteString("@")
	if cfg.Net != "" {
		sb.WriteString(cfg.Net + "(" + cfg.Addr + ")")
	}
	sb.WriteString("/")
	sb.WriteString(cfg.DBName)
	if len(cfg.Params) > 0 {
		vals := url.Values{}
		for k, v := range cfg.Params {
			vals.Set(k, v)
		}
		sb.WriteString("?" + vals.Encode())
	}
	return sb.String()
}

// explicitParams 返回 DSN 中 '?' 之后的原始 query 段（不含 '?'）。
// 与 go-sql-driver 一致：先定位最后一个 '/'，再取其后第一个 '?'。
func explicitParams(dsn string) string {
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 {
		return ""
	}
	rest := dsn[slash+1:]
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		return rest[i+1:]
	}
	return ""
}

// RedactDSN 返回脱敏 DSN（密码替换为 ****），供启动日志使用。
// 解析失败时返回 "***unparseable-dsn***" 而不是原串，避免凭据泄漏。
func RedactDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "***unparseable-dsn***"
	}
	cfg.Passwd = "****"
	return formatDSN(cfg)
}
