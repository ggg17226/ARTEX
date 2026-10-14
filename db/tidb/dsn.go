package tidb

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// requiredDSNParams 是 spec L83「DSN 每物理连接 UTC」的强制参数集合。
// 注意：go-sql-driver 执行连接初始化时，对于 params["time_zone"] 会直接
// 拼入 "SET time_zone = " + val。为保证符合 SQL 语法，值必须用单引号包裹：
// 即 `'+00:00'`。
var requiredDSNParams = map[string]string{
	"parseTime": "true",
	"loc":       "UTC",
	"time_zone": "'+00:00'",
	"charset":   "utf8mb4",
}

// safeParseDSN 包装 mysql.ParseDSN 并使用 recover 捕获 panic。
// go-sql-driver/mysql v1.9.3 在遇到已废弃参数（如 strict=true）时会直接 panic，
// 本函数确保将 panic 优雅转换为 error，防止进程崩溃（Fail Closed）。
func safeParseDSN(dsn string) (cfg *mysql.Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("tidb: invalid DSN: %v", r)
		}
	}()
	return mysql.ParseDSN(dsn)
}

// NormalizeDSN 解析并规范化 TiDB DSN（go-sql-driver 语法
// user:password@tcp(host:port)/dbname?params）：
//   - 缺 user / 缺库名 → 错误（ARTEX 只连接预建库，绝不隐式建库）；
//   - 必需参数（parseTime/loc/time_zone/charset）缺失 → 补齐；
//     已存在但值不同 → 错误，不静默覆盖（用户显式意图冲突必须暴露）；
//   - 其余用户参数原样保留；重复键的多值（如 x=first&x=second）按
//     url.Values 语义全部保留，不静默丢弃；
//   - 幂等：对已规范化的 DSN 再次调用返回原串。
//
// 纯函数，无 I/O，可被 P01 OpenTiDB 与 P08 config.TiDBDSN 共用。
//
// 序列化说明：go-sql-driver 的 Config.FormatDSN 不 URL 编码 params，
// 输出 time_zone='+00:00' 再经 ParseDSN（内部 url.ParseQuery）回读时 '+'
// 会被解码成空格。本包因此自实现 formatDSN：params 一律用
// url.Values.Encode 百分号编码（time_zone → time_zone=%27%2B00%3A00%27），
// 且对 duration 类型参数（如 1µs / 1μs 均格式化为 1us）做 ASCII 兼容处理，
// 库名保留 url.PathEscape，保证 ParseDSN 往返稳定、幂等成立。
func NormalizeDSN(dsn string) (string, error) {
	cfg, err := safeParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("normalize DSN: %w", err)
	}
	// go-sql-driver v1.9.3 的 ParseDSN 将已识别参数（parseTime/loc/charset/
	// timeout/readTimeout 等）消费到 Config 专属字段，未识别参数才写入
	// cfg.Params，且无 ?params 段时 Params 保持 nil。因此从原始 DSN 的
	// query 段用 url.Values 还原用户显式参数：冲突检测、用户参数保留
	// （含重复键多值）均依赖 params。
	params, err := explicitParamsValues(dsn)
	if err != nil {
		return "", err
	}
	if params == nil {
		params = url.Values{}
	}
	// 强制参数逐项冲突检测：重复键的每一个值都必须与强制值一致。
	for k, vs := range params {
		want, isReq := requiredDSNParams[k]
		if !isReq {
			continue
		}
		for _, v := range vs {
			if v != want {
				return "", fmt.Errorf("normalize DSN: param %q=%q conflicts with required value %q", k, v, want)
			}
		}
	}
	if cfg.User == "" {
		return "", fmt.Errorf("normalize DSN: missing user (go-sql-driver 语法 user:password@tcp(host:port)/dbname)")
	}
	if cfg.DBName == "" {
		return "", fmt.Errorf("normalize DSN: missing database name — ARTEX 只连接管理员预建库，不创建数据库")
	}
	// 缺失的强制参数补齐；已存在的强制参数已通过冲突检测，原样保留。
	for key, want := range requiredDSNParams {
		if _, present := params[key]; !present {
			params[key] = []string{want}
		}
	}
	return formatDSN(cfg, params), nil
}

// formatDSN 把 Config 与显式参数序列化为 go-sql-driver DSN。params 段用
// url.Values.Encode（键排序、百分号编码）统一编码，重复键按输入顺序逐一
// 输出，保证 ParseDSN 往返稳定且不丢弃重复参数。
// 库名必须进行 url.PathEscape 以防止包含特殊字符截断 query。
func formatDSN(cfg *mysql.Config, params url.Values) string {
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
	sb.WriteString(url.PathEscape(cfg.DBName))
	if len(params) > 0 {
		enc := url.Values{}
		for k, vs := range params {
			for _, v := range vs {
				enc.Add(k, normalizeDurationParam(k, v))
			}
		}
		sb.WriteString("?" + enc.Encode())
	}
	return sb.String()
}

// normalizeDurationParam 将 duration 参数值中的微秒单位规范化为 ASCII "us"。
// go-sql-driver v1.9.3 的 parseDSNParams 解析 timeout/readTimeout/
// writeTimeout/timeTruncate 时直接对未解码的原始 value 调 time.ParseDuration
// （这几个 case 不做 QueryUnescape）；而 url.Values.Encode 会把非 ASCII
// 微秒符号转义（U+00B5 → %C2%B5、U+03BC → %CE%BC），驱动二次 ParseDSN 时
// time.ParseDuration 看到未解码的百分号序列即报 unknown unit。
// time.ParseDuration 原生同时接受 U+00B5 MICRO SIGN 与 U+03BC GREEK SMALL
// LETTER MU 两种微秒写法，但 Encode 转义后二者均失效，故一并替换为
// ASCII "us"（stdlib 同样识别）。
func normalizeDurationParam(key, val string) string {
	switch key {
	case "timeout", "readTimeout", "writeTimeout", "timeTruncate":
		val = strings.ReplaceAll(val, "\u00b5s", "us") // µs (U+00B5 MICRO SIGN)
		val = strings.ReplaceAll(val, "\u03bcs", "us") // μs (U+03BC GREEK SMALL LETTER MU)
	}
	return val
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

// explicitParamsValues 从原始 DSN 的 query 段还原用户显式参数，以
// url.Values 保留重复键的多值（如 x=first&x=second）。无 ?params 段时
// 返回 (nil, nil)；query 语法非法（如 Go 1.24+ ParseQuery 拒绝的 ';'
// 分隔符）时 Fail Closed 返回错误。
func explicitParamsValues(dsn string) (url.Values, error) {
	qs := explicitParams(dsn)
	if qs == "" {
		return nil, nil
	}
	params, err := url.ParseQuery(qs)
	if err != nil {
		return nil, fmt.Errorf("parse DSN query %q: %w", qs, err)
	}
	return params, nil
}

// RedactDSN 返回脱敏 DSN（密码替换为 ****），供启动日志使用。
// 解析失败时返回安全占位符 "[redacted-invalid-dsn]" 而不是原串，避免凭据泄漏或 panic。
// 用户显式参数（含重复多值）经 explicitParamsValues 还原后原样保留。
func RedactDSN(dsn string) string {
	cfg, err := safeParseDSN(dsn)
	if err != nil {
		return "[redacted-invalid-dsn]"
	}
	cfg.Passwd = "****"
	params, err := explicitParamsValues(dsn)
	if err != nil {
		params = url.Values{}
	}
	return formatDSN(cfg, params)
}
