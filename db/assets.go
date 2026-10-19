package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db/lockorder"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/go-sql-driver/mysql"
)

// =====================================================================
// 统一资产表
// =====================================================================

// Asset is a row in the assets table.
//
// P03（R08）：ID / CompanyID / TaskSourceNodeID 是 BIGINT，对外带引号 string。
// TaskIDs 保持 []int64 且**不得**加 json:",string"：该字段由 assets.task_ids 列
// json.Unmarshal 回读（存的是数字数组）；encoding/json 会忽略切片上的 ,string
// （既不生效也不报错），所以该字段不能靠 tag，必须由 wire DTO 在输出侧覆盖。
// HTTP 输出侧由 server 的 assetWire 用外层同名字段把 task_ids 覆盖成 []string。
type Asset struct {
	ID         int64   `json:"id,string"`
	Type       string  `json:"type"`
	CompanyID  *int64  `json:"company_id,string,omitempty"`
	TaskIDs    []int64 `json:"task_ids"`
	Domain     string  `json:"domain,omitempty"`
	RootDomain string  `json:"root_domain,omitempty"`
	IP         string  `json:"ip,omitempty"`
	CSegment   string  `json:"c_segment,omitempty"`
	Port       *int    `json:"port,omitempty"`
	ICP        string  `json:"icp,omitempty"`
	// ip fields
	BoundDomains []string         `json:"bound_domains,omitempty"`
	OpenPorts    []map[string]any `json:"open_ports,omitempty"`
	// subdomain fields
	RecordType  string   `json:"record_type,omitempty"`
	RecordValue []string `json:"record_value,omitempty"`
	// app fields
	BundleID       string `json:"bundle_id,omitempty"`
	AppName        string `json:"app_name,omitempty"`
	Category       string `json:"category,omitempty"`
	AppDescription string `json:"app_description,omitempty"`
	AppICP         string `json:"app_icp,omitempty"`
	// service fields
	URL           string           `json:"url,omitempty"`
	ServiceType   string           `json:"service_type,omitempty"`
	ServiceName   string           `json:"service_name,omitempty"`
	FaviconMMH3   string           `json:"favicon_mmh3,omitempty"`
	StatusCode    *int             `json:"status_code,omitempty"`
	ContentLength *int64           `json:"content_length,omitempty"`
	PageTitle     string           `json:"page_title,omitempty"`
	Technologies  []string         `json:"technologies,omitempty"`
	Auth          []map[string]any `json:"auth,omitempty"`
	// endpoint fields
	Method string           `json:"method,omitempty"`
	Params []map[string]any `json:"params,omitempty"`
	// meta
	Extra             map[string]any `json:"extra,omitempty"`
	LastSeen          string         `json:"last_seen"`
	TaskSource        string         `json:"task_source,omitempty"`
	TaskSourceSummary string         `json:"task_source_summary,omitempty"`
	TaskSourceNodeID  *int64         `json:"task_source_node_id,string,omitempty"`
}

// AuthItem is one entry in the auth array.
type AuthItem struct {
	Type        string `json:"type,omitempty"`
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	Token       string `json:"token,omitempty"`
	Description string `json:"description,omitempty"`
}

// ParamItem is one entry in the params array.
type ParamItem struct {
	Location string `json:"location"`
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Type     string `json:"type,omitempty"`
}

// PortService is one entry in open_ports: {"port":22,"service":"ssh"}.
type PortService struct {
	Port    int    `json:"port"`
	Service string `json:"service,omitempty"`
}

// AssetStore operates on the assets table.
type AssetStore struct {
	db      *DB
	company *CompanyStore
	tx      *sql.Tx
}

// execInTx 在单条事务里执行 fn：TiDB 没有 DELETE … RETURNING，
// "先读回副作用再删除"这类读改写必须整体包在事务里。
func (d *DB) execInTx(fn func(tx *sql.Tx) error) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 Rollback 只返回 ErrTxDone
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Assets returns the asset store.
func (d *DB) Assets() *AssetStore {
	return &AssetStore{db: d, company: d.Companies()}
}

// Companies returns the company store associated with this asset store.
func (s *AssetStore) Companies() *CompanyStore { return s.company }

// ── 事务边界与外层重试（spec §3.2.1 第 2 点）─────────────────────────────────
//
// 原实现是 `SELECT pg_advisory_xact_lock($1)` 的进程级串行化：TiDB 没有 advisory
// lock，也不需要它 —— 资产唯一性现在由 8 条 hash 唯一索引 + 写前仲裁协议保证，
// 并发赢家与输家的裁决权交给数据库的 1062，而不是应用层互斥锁。
//
// 外层重试循环负责 spec §3.2.1 路径 B-3：1062 / 1213 / 1205 一律**整笔回滚**后在
// **全新事务**里重放（此时并发赢家已可见，重放自然落入 Path A）。

// assetWriteMaxAttempts 是一次逻辑写入的尝试上限：**1 次首试 + (assetWriteMaxAttempts-1)
// 次重试**。首试不消耗任何退避档位（它前面没有失败）。
const assetWriteMaxAttempts = 4

// assetWriteRetryDelays 是第 1/2/3 **次重试之前**的退避等待，长度必须严格等于
// assetWriteMaxAttempts-1：一次重试当且仅当对应一个档位，任何档位都可达。
// 首档 0 —— 1062 输家是确定性冲突，整笔回滚后立刻在全新事务里重放即可。
// （R6 整改：原先"4 次尝试 + 4 档阶梯"的下标口径不一致，末档永远够不到。）
var assetWriteRetryDelays = []time.Duration{
	0,
	50 * time.Millisecond,
	200 * time.Millisecond,
}

// errAssetWriteRetry 标记"本轮尝试已完整回滚，必须在新事务里重放"的内部信号。
// 它只在外层重试循环内部出现，绝不会返回给调用方（耗尽后会被包装成显式错误）。
//
// **必须包装 lockorder.ErrApplicationRetry（R2）**：带 TaskID 的资产写者跑在
// lockorder.TxRunner 上，没有 withAssetMutation 这层私有循环，重试判定只能从
// 错误链做结构化分类；私有哨兵若对本包之外的 Runner 不可见，"候选在加锁前消失"
// 这类信号就会变成终局错误，永远等不到新事务里的 Path A 重放。
var errAssetWriteRetry = fmt.Errorf("assets: retryable write conflict: %w", lockorder.ErrApplicationRetry)

// ErrAssetWriteRetriesExhausted 表示外层事务重试循环已耗尽。
var ErrAssetWriteRetriesExhausted = errors.New("assets: write retry attempts exhausted")

// withAssetMutation 在一次事务里执行 fn，并在可重试冲突时以全新事务重放。
// 嵌套的资产副作用（例如 subdomain 写入顺带 upsert root_domain/IP）通过
// 同一个 scoped store 复用同一事务，不会各开一笔。
//
// **同一事务的子写入绝不允许吞错（R1 / F02）**：子调用复用 s.tx，它自己无法回滚，
// 唯一的回滚点在 runAssetAttempt 依据 fn 的最终返回值决定 Commit。因此子写入的
// ErrIdentityHashCollision / 1062 / 1205 / 1213 必须原样上抛到这一层，否则外层会
// 带着"子写入已失败"的事实提交其他资产的写入。
func (s *AssetStore) withAssetMutation(fn func(*AssetStore) (int64, error)) (int64, error) {
	if s.tx != nil {
		return fn(s)
	}
	var lastErr error
	for attempt := 0; attempt < assetWriteMaxAttempts; attempt++ {
		// attempt == 0 是首试（无退避）；attempt == k 就是第 k 次重试，
		// 直接以 k 取退避档位 —— 下标口径与 assetWriteRetryDelays 一一对应。
		if wait := assetBackoff(attempt); wait > 0 {
			time.Sleep(wait)
		}
		id, err := s.runAssetAttempt(fn)
		if err == nil {
			return id, nil
		}
		lastErr = err
		if !assetShouldRetry(err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("%w after %d attempts: %w", ErrAssetWriteRetriesExhausted, assetWriteMaxAttempts, lastErr)
}

// withCompanyScopeMutation 保留旧名字（tasks.go / task_assets.go 的同名锁由
// Task 2.3 统一接入 lockorder.TxRunner），语义等价于 withAssetMutation。
func (s *AssetStore) withCompanyScopeMutation(fn func(*AssetStore) (int64, error)) (int64, error) {
	return s.withAssetMutation(fn)
}

// runAssetAttempt 是单次尝试：Begin → fn → Commit；任何错误路径都由 defer 完整
// Rollback（严禁"原地重试"：重试必须发生在 runAssetAttempt 之外的事务里）。
func (s *AssetStore) runAssetAttempt(fn func(*AssetStore) (int64, error)) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	scoped := &AssetStore{db: s.db, company: s.company, tx: tx}
	id, err := fn(scoped)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// assetShouldRetry 只按**结构化**信号判定：本包的重试哨兵，或 lockorder 按 MySQL
// 错误编号（1062/1205/1213）分类的可重试冲突。碰撞哨兵 ErrIdentityHashCollision
// 不在其中 —— 身份冲突是业务终局，绝不重放。
func assetShouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, schema.ErrIdentityHashCollision) {
		return false
	}
	return errors.Is(err, errAssetWriteRetry) || lockorder.IsRetryableConflict(err)
}

// assetBackoff 返回第 retry 次**重试之前**应等待的时长（retry 从 1 开始计数）。
// retry <= 0 表示首试，永不等待；retry 超出阶梯长度时钳到最后一档。
func assetBackoff(retry int) time.Duration {
	if retry <= 0 {
		return 0
	}
	if retry > len(assetWriteRetryDelays) {
		return assetWriteRetryDelays[len(assetWriteRetryDelays)-1]
	}
	return assetWriteRetryDelays[retry-1]
}

// assetIsDuplicateKey 结构化识别 MySQL 1062（唯一索引/主键冲突）。
func assetIsDuplicateKey(err error) bool {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1062
	}
	return false
}

// ── 企业归属解析（TiDB 方言）─────────────────────────────────────────────────
//
// 原实现依赖 PG 的 `net >>= $1::inet` 与 masklen()。TiDB 的 company_scope.net 是
// CIDR 文本，没有 inet 类型，所以网络归属按 spec §3.3.1 的口径改成
// "SQL 取候选 + Go 内存前缀匹配"，匹配语义（最长前缀优先、同长比 company_id）不变。

type assetScopeQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

func (s *AssetStore) resolveCompanyWithICP(rootDomain, ipStr, icp string) (*int64, error) {
	if s.tx != nil {
		return resolveAssetCompany(s.tx, rootDomain, ipStr, icp)
	}
	return resolveAssetCompany(s.db, rootDomain, ipStr, icp)
}

func resolveAssetCompany(q assetScopeQueryer, rootDomain, ipStr, icp string) (*int64, error) {
	// 在解析到有效 company_id 时，验证该目标企业在数据库中真实存活且有效（M19 并发防悬垂保护）：
	// 避免在企业删除提交之后，由于旧快照读解析出了已被删除的 company_id，导致新写入的资产带有悬空的 company_id。
	verifyCompanyAlive := func(cid int64) (*int64, error) {
		var exists int
		err := q.QueryRow(`SELECT 1 FROM companies WHERE id = ? LIMIT 1`, cid).Scan(&exists)
		if err == nil {
			return &cid, nil
		}
		if errors.Is(err, sql.ErrNoRows) {
			// 该企业已经被并发删除，忽略陈旧归属
			return nil, nil
		}
		return nil, err
	}

	if rootDomain != "" {
		var cid int64
		err := q.QueryRow(`
SELECT company_id FROM company_scope
WHERE kind = 'domain' AND domain = ?
ORDER BY CHAR_LENGTH(domain) DESC, company_id ASC
LIMIT 1`, rootDomain).Scan(&cid)
		if err == nil {
			return verifyCompanyAlive(cid)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	if ip := net.ParseIP(strings.TrimSpace(ipStr)); ip != nil {
		cid, err := resolveCompanyByIP(q, ip)
		if err != nil {
			return nil, err
		}
		if cid != nil {
			return verifyCompanyAlive(*cid)
		}
	}
	if normalized := NormalizeICP(icp); normalized != "" {
		var cid int64
		err := q.QueryRow(`
SELECT company_id FROM company_scope
WHERE kind = 'icp' AND value = ?
ORDER BY company_id ASC
LIMIT 1`, normalized).Scan(&cid)
		if err == nil {
			return verifyCompanyAlive(cid)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	return nil, nil
}

// resolveCompanyByIP 取回全部 ip/cidr 归属规则，在内存里做最长前缀匹配
// （原 PG 语义：masklen 降序、company_id 升序）。
func resolveCompanyByIP(q assetScopeQueryer, ip net.IP) (*int64, error) {
	rows, err := q.Query(`
SELECT company_id, net FROM company_scope
WHERE kind IN ('ip', 'cidr') AND net IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type candidate struct {
		companyID int64
		maskBits  int
	}
	var best *candidate
	for rows.Next() {
		var cid int64
		var raw []byte
		if err := rows.Scan(&cid, &raw); err != nil {
			return nil, err
		}
		ones, ok := assetNetPrefixLen(string(raw), ip)
		if !ok {
			continue
		}
		if best == nil || ones > best.maskBits || (ones == best.maskBits && cid < best.companyID) {
			best = &candidate{companyID: cid, maskBits: ones}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if best == nil {
		return nil, nil
	}
	id := best.companyID
	return &id, nil
}

// assetNetPrefixLen 判断 ip 是否落在规则 net（CIDR 或裸地址）内，并返回前缀长度。
func assetNetPrefixLen(rawNet string, ip net.IP) (int, bool) {
	raw := strings.TrimSpace(rawNet)
	if raw == "" {
		return 0, false
	}
	if strings.ContainsRune(raw, '/') {
		_, network, err := net.ParseCIDR(raw)
		if err != nil || !network.Contains(ip) {
			return 0, false
		}
		ones, bits := network.Mask.Size()
		if ones == 0 && bits == 0 {
			return 0, false
		}
		return ones, true
	}
	rule := net.ParseIP(raw)
	if rule == nil || !rule.Equal(ip) {
		return 0, false
	}
	if ip.To4() != nil {
		return 32, true
	}
	return 128, true
}

// =====================================================================
// Helpers
// =====================================================================

// calcCSegment computes the /24 (IPv4) or /48 (IPv6) network for an IP string.
func calcCSegment(ipStr string) string {
	if ipStr == "" {
		return ""
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}
	if ip.To4() != nil {
		// IPv4 /24
		parts := strings.Split(ipStr, ".")
		if len(parts) == 4 {
			return parts[0] + "." + parts[1] + "." + parts[2] + ".0/24"
		}
		return ""
	}
	// IPv6 /48
	_, ipnet, err := net.ParseCIDR(ip.String() + "/48")
	if err != nil {
		return ""
	}
	return ipnet.String()
}

// canonicalIP 把地址规范成 Canonical 文本（IPv4 点分十进制、IPv6 压缩小写），
// 使 "::ffff:127.0.0.1" 与 "127.0.0.1" 这类同址写法命中同一 canonical 身份。
func canonicalIP(raw string) (string, bool) {
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

// normalizeURL lowercases scheme and host, strips the scheme's default port and
// trims a trailing slash (bare root or a single path segment), so that
// "HTTP://API.Example.COM:80/v1/" and "http://api.example.com/v1" share one URL.
func normalizeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port != "" && port == strconv.Itoa(defaultPort(strings.ToLower(u.Scheme), "")) {
		port = "" // 默认端口不进入 canonical URL
	}
	if host != "" {
		if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
			host = "[" + host + "]" // IPv6 needs brackets again
		}
	}
	if port != "" {
		host = host + ":" + port
	}
	u.Host = host
	if strings.HasSuffix(u.Path, "/") && u.Path != "/" {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}
	s := u.String()
	if strings.HasSuffix(s, "/") && u.Path == "/" && u.RawQuery == "" {
		s = strings.TrimSuffix(s, "/")
	}
	return s
}

// parseURL extracts domain, port, service_name from a URL.
func parseURL(raw string) (domain string, port int, serviceName string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, ""
	}
	domain = strings.ToLower(u.Hostname())
	port = defaultPort(strings.ToLower(u.Scheme), u.Port())
	switch strings.ToLower(u.Scheme) {
	case "https":
		serviceName = "HTTPS"
	case "http":
		serviceName = "HTTP"
	default:
		serviceName = strings.ToUpper(u.Scheme)
	}
	return
}

// nullableInt64 returns sql.NullInt64.
func nullableInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

// ── JSON 编解码（原 PG text[]/jsonb[] 列在 TiDB 里是 JSON 文本 MEDIUMTEXT）────

// marshalJSONArray 把对象数组编成合法 JSON 文本（空 → "[]"）。
func marshalJSONArray(items []map[string]any) (string, error) {
	if len(items) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// marshalStringArray 把字符串数组编成合法 JSON 文本（空 → "[]"）。
func marshalStringArray(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func marshalPortServices(ps []PortService) (string, error) {
	items := make([]map[string]any, 0, len(ps))
	for _, p := range ps {
		items = append(items, map[string]any{"port": p.Port, "service": p.Service})
	}
	return marshalJSONArray(items)
}

// marshalTaskIDs 把 task_id 集合编成 JSON 数字数组（assets.task_ids 列）。
func marshalTaskIDs(ids []int64) string {
	if len(ids) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, itoa(id))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// decodeJSONArray 读回一个 JSON 数组列；空列 / 非法 JSON 一律按空数组处理。
// 注意：该函数经由 []any 解码，数字元素是 float64，只适用于**元素数值不会超过
// 2^53** 的列（open_ports 的 port、字符串数组等）。task_ids 这类 BIGINT 列必须走
// parseTaskIDArray，否则会静默丢精度（R3）。
func decodeJSONArray(raw string) []any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

// decodeStringArray 读回一个字符串 JSON 数组列。
func decodeStringArray(raw string) []string {
	var out []string
	for _, item := range decodeJSONArray(raw) {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseTaskIDArray 严格解析 assets.task_ids 这类"数字 JSON 数组"。
//
// R3 关键：全程走 json.Number（UseNumber），**绝不经过 float64**。float64 只有
// 53 位有效尾数，> 2^53 的合法 BIGINT（如 9007199254740993）会在解码时被舍入成
// 9007199254740992，而这个函数产出的是要**写回数据库**的值 —— 一旦舍入就等于
// 静默改写任务归属外键，且再也回不来。
//
// 兼容与严格性：
//   - 空列 / "null" → 空切片，无错误（列本来就没值）；
//   - 数字元素必须能精确落进 int64，越界 / 小数 / null / 对象 → 显式错误；
//   - 历史遗留的"数字字符串"元素（"7"）继续接受，同样要求精确落进 int64。
func parseTaskIDArray(raw string) ([]int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var items []any
	if err := dec.Decode(&items); err != nil {
		return nil, fmt.Errorf("assets: task_ids is not a JSON array (%q): %w", raw, err)
	}
	// 严格校验完整 JSON 输入：确保首个合法 JSON 值之后不存在任何多余的非空白内容或连续第二个值（RR1）
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("assets: task_ids contains trailing garbage or multiple JSON values (%q)", raw)
	}
	out := make([]int64, 0, len(items))
	for i, item := range items {
		switch v := item.(type) {
		case json.Number:
			n, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("assets: task_ids[%d]=%s is not an exact int64", i, v.String())
			}
			out = append(out, n)
		case string:
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("assets: task_ids[%d]=%q is not an exact int64", i, v)
			}
			out = append(out, n)
		default:
			return nil, fmt.Errorf("assets: task_ids[%d] has unexpected JSON type %T", i, item)
		}
	}
	return out, nil
}

// assetUnionStrings 合并字符串 JSON 数组：旧元素保持原顺序在前，新元素按值去重后追加。
func assetUnionStrings(oldJSON string, added []string) string {
	if len(added) == 0 {
		if strings.TrimSpace(oldJSON) == "" {
			return "[]"
		}
		return marshalStringArray(decodeStringArray(oldJSON))
	}
	seen := map[string]struct{}{}
	merged := make([]string, 0, len(added))
	for _, s := range decodeStringArray(oldJSON) {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		merged = append(merged, s)
	}
	for _, s := range added {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		merged = append(merged, s)
	}
	return marshalStringArray(merged)
}

// assetUnionTaskIDs 合并 assets.task_ids（数字数组，按值去重，保持稳定顺序）。
// 旧列解析失败必须显式报错：这里的产出是要写回数据库的值，静默按空数组处理会
// 把既有归属整体抹掉（R3）。
func assetUnionTaskIDs(oldJSON string, added []int64) (string, error) {
	persisted, err := parseTaskIDArray(oldJSON)
	if err != nil {
		return "", err
	}
	seen := map[int64]struct{}{}
	merged := make([]int64, 0, len(persisted)+len(added))
	for _, id := range persisted {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		merged = append(merged, id)
	}
	for _, id := range added {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		merged = append(merged, id)
	}
	return marshalTaskIDs(merged), nil
}

// assetMergeObjects 合并对象 JSON 数组：同 key 由 pick 决定保留哪一个，
// 不同 key 按"旧在前、新在后"排列。pick 返回 true 表示用 next 替换 cur。
// open_ports（port 分组 + service 非空优先）与 params（location+name 分组 +
// 元素文本 DESC 优先）都走这里，对应 spec §3.3.1 的两条真实去重语义。
func assetMergeObjects(
	oldJSON string,
	added []map[string]any,
	key func(map[string]any) string,
	pick func(cur, next map[string]any) bool,
) (string, error) {
	var current []map[string]any
	for _, item := range decodeJSONArray(oldJSON) {
		if m, ok := item.(map[string]any); ok {
			current = append(current, m)
		}
	}
	index := map[string]int{}
	for i, m := range current {
		index[key(m)] = i
	}
	for _, next := range added {
		k := key(next)
		if at, ok := index[k]; ok {
			if pick != nil && pick(current[at], next) {
				current[at] = next
			}
			continue
		}
		index[k] = len(current)
		current = append(current, next)
	}
	if len(current) == 0 {
		return "[]", nil
	}
	return marshalJSONArray(current)
}

// assetPortKey 是 open_ports 元素的去重键（同端口只保留一条）。
func assetPortKey(m map[string]any) string {
	return fmt.Sprintf("%v", m["port"])
}

// assetPreferService 是同端口元素的保留策略：service 非空优先（spec §3.3.1）。
func assetPreferService(cur, next map[string]any) bool {
	return assetTextField(cur, "service") == "" && assetTextField(next, "service") != ""
}

// assetTextField 取对象字段的文本值（缺失 / null → ""）。
func assetTextField(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprintf("%v", v))
}

// assetParamKey 是 endpoint.params 元素的去重键（location + name 视为同一引用）。
func assetParamKey(m map[string]any) string {
	return fmt.Sprintf("%v\x00%v", m["location"], m["name"])
}

// assetJSONText 把元素稳定编码成 JSON 文本，用于"元素整体"比较与 DESC 排序。
func assetJSONText(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

// =====================================================================
// 8 分支规范化写入引擎（spec §3.2.1）
// =====================================================================

// assetBranchPair 是 8 个身份分支的 (identity 列, hash 列) 对，顺序固定，
// 用于"仅激活当前分支、其余 7 对显式绑定 NULL"的全量列绑定。
var assetBranchPair = [8][2]string{
	{"identity_root_domain", "hash_root_domain"},
	{"identity_ip", "hash_ip"},
	{"identity_subdomain", "hash_subdomain"},
	{"identity_app_bundle", "hash_app_bundle"},
	{"identity_app_name", "hash_app_name"},
	{"identity_service_http", "hash_service_http"},
	{"identity_service_other", "hash_service_other"},
	{"identity_endpoint", "hash_endpoint"},
}

// assetNotNullDefaults 是 assets 表上 NOT NULL 且无 DEFAULT 的列兜底值
// （TiDB strict 模式下漏写即报错；JSON 列一律写成合法 JSON 文本）。
var assetNotNullDefaults = []assetCol{
	{"task_ids", "[]"},
	{"bound_domains", "[]"},
	{"open_ports", "[]"},
	{"record_value", "[]"},
	{"technologies", "[]"},
	{"auth", "[]"},
	{"params", "[]"},
	{"extra", "{}"},
	{"company_source", "scope"},
}

// assetBranch 是一个分支的 canonical 身份 + 该身份的 identity/hash 列对。
type assetBranch struct {
	name     string // 诊断用分支名
	identCol string
	hashCol  string
	identity []byte
	hash     [32]byte
}

func newAssetBranch(name string, identity []byte) assetBranch {
	sum := schema.CanonicalHash(identity)
	return assetBranch{
		name:     name,
		identCol: "identity_" + strings.TrimPrefix(name, "assets_"),
		hashCol:  "hash_" + strings.TrimPrefix(name, "assets_"),
		identity: identity,
		hash:     sum,
	}
}

// assetCol 是写入时的一列（列名 + 绑定值）。
type assetCol struct {
	Col string
	Val any
}

// assetRawExpr 标记"直接嵌入 SQL 的表达式"（例如 last_seen = NOW(3)），
// 它不参与参数绑定，因此不会被当成字符串字面量写进 DATETIME 列。
type assetRawExpr string

// assetWrite 是一次分支写入：全量插入列 + 身份不可变更新所需的回读列与合并器。
type assetWrite struct {
	branch assetBranch
	// taskID > 0 表示本次写入同时把资产归属到该任务：writeAsset 必须在**同一事务**
	// 里补一条 task_asset_links（PG 触发器 trg_assets_task_links 的 TiDB 替身，
	// M26 / M30），否则 assets.task_ids 与 link 表就会分叉。
	taskID int64
	// insert 是业务列（不含 identity_*/hash_*，那些由引擎按严格激活规则追加）。
	insert []assetCol
	// mergeCols 是 Path A 在持锁保护下需要回读的非身份业务列。
	mergeCols []string
	// merge 依据旧行产出 UPDATE 的 SET 列表；严禁返回 identity_*/hash_*/自然键列。
	merge func(old *assetSnapshot) ([]assetCol, error)
}

// assetSnapshot 是 Path A 持锁回读到的非身份业务列视图。
type assetSnapshot struct {
	cols map[string]any
}

// str 返回文本列（NULL → ""）。
func (a *assetSnapshot) str(col string) string {
	switch v := a.cols[col].(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// raw 返回列的原始驱动值。
func (a *assetSnapshot) raw(col string) any { return a.cols[col] }

// int64Val 返回整数列（NULL / 非数值 → ok=false）。
func (a *assetSnapshot) int64Val(col string) (int64, bool) {
	switch v := a.cols[col].(type) {
	case int64:
		return v, true
	case []byte:
		var n int64
		_, err := fmt.Sscanf(string(v), "%d", &n)
		return n, err == nil
	case string:
		var n int64
		_, err := fmt.Sscanf(v, "%d", &n)
		return n, err == nil
	default:
		return 0, false
	}
}

// assetForbiddenUpdateCols 是 Path A UPDATE 里**绝对禁止**出现的身份/自然键列。
var assetForbiddenUpdateCols = map[string]struct{}{
	"identity_root_domain": {}, "hash_root_domain": {},
	"identity_ip": {}, "hash_ip": {},
	"identity_subdomain": {}, "hash_subdomain": {},
	"identity_app_bundle": {}, "hash_app_bundle": {},
	"identity_app_name": {}, "hash_app_name": {},
	"identity_service_http": {}, "hash_service_http": {},
	"identity_service_other": {}, "hash_service_other": {},
	"identity_endpoint": {}, "hash_endpoint": {},
	"type": {}, "domain": {}, "root_domain": {}, "record_type": {},
	"bundle_id": {}, "app_name": {}, "url": {}, "method": {},
	"port": {}, "service_name": {}, "service_type": {},
}

// writeAsset 执行 spec §3.2.1 的两阶段写前仲裁协议：
//
//	候选定位（非锁定读 hash 唯一索引）
//	  ├─ Path A（candidatePK > 0）：SELECT id … WHERE id = ? FOR UPDATE 行级悲观锁
//	  │                              → schema.VerifyHashArbitration 写前仲裁
//	  │                              → 身份不可变 UPDATE → 身份漂移复核
//	  └─ Path B（无候选）        ：严格 INSERT（无 ODKU / 无 RETURNING）
//	                                 → LastInsertId → 同事务 VerifyHashArbitration 自洽
//	                                 → 1062 时整笔回滚，由外层在新事务里转 Path A
//
// 任何 ErrIdentityHashCollision 都直接上抛，由 runAssetAttempt 的 defer 完整回滚，
// 绝不修改该行（"先覆盖后仲裁"在本协议里被结构性排除）。
func (s *AssetStore) writeAsset(w assetWrite) (int64, error) {
	if s.tx == nil {
		return 0, fmt.Errorf("assets: writeAsset requires an active transaction")
	}
	ctx := context.Background()

	candidate, err := s.assetCandidatePK(w.branch)
	if err != nil {
		return 0, err
	}
	id := int64(0)
	if candidate > 0 {
		id, err = s.assetPathA(ctx, w, candidate)
	} else {
		id, err = s.assetPathB(ctx, w)
	}
	if err != nil {
		return 0, err
	}
	// **双写闭环（R3）**：写资产与写归属在同一个事务、同一个出口里完成。
	// 任何一条带 TaskID 的资产 upsert（含 subdomain/service 的嵌套副作用）都从这里
	// 长出 link 行，绝不允许出现"task_ids 记了、link 没有"的半态。
	if w.taskID > 0 {
		if err := ensureTaskAssetLinkTx(ctx, s.tx, w.taskID, id); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// assetCandidatePK 按 hash 唯一索引定位候选物理 PK（非锁定读；真正的悲观锁
// 只在 Path A 拿到 PK 之后按物理主键获取，符合 spec §4.3 的锁序前置条件）。
func (s *AssetStore) assetCandidatePK(b assetBranch) (int64, error) {
	var pk int64
	err := s.tx.QueryRow("SELECT id FROM assets WHERE "+b.hashCol+" = ?", b.hash[:]).Scan(&pk)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if pk < 0 {
		return 0, fmt.Errorf("assets: negative candidate pk %d on %s", pk, b.hashCol)
	}
	return pk, nil
}

// assetPathA 是已知候选行的两阶段更新：PK 行锁 → 写前仲裁 → 身份不可变更新。
func (s *AssetStore) assetPathA(ctx context.Context, w assetWrite, candidate int64) (int64, error) {
	b := w.branch

	// 1) 行级悲观锁：严格按物理 PK 取锁（hash/自然键一律不能当预定位器）。
	var locked int64
	err := s.tx.QueryRow("SELECT id FROM assets WHERE id = ? FOR UPDATE", candidate).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		// 候选已被并发删除：整笔回滚后在新事务里重放（此时应落进 Path B）。
		return 0, fmt.Errorf("%w: candidate %d vanished before lock", errAssetWriteRetry, candidate)
	}
	if err != nil {
		return 0, err
	}
	if locked != candidate {
		return 0, fmt.Errorf("%w: pk lock returned %d, want %d", errAssetWriteRetry, locked, candidate)
	}

	// 2) 写前仲裁：持锁保护下核验"持久化 identity 与输入逐字节相等且 PK 等于候选"。
	verified, err := schema.VerifyHashArbitration(ctx, s.tx, "assets", "id", b.identCol, b.hashCol, b.hash, b.identity)
	if err != nil {
		// ErrIdentityHashCollision 走这里：调用方 defer 已保证完整回滚、零写入。
		return 0, fmt.Errorf("assets: write-ahead arbitration rejected branch %s: %w", b.name, err)
	}
	if uint64(candidate) != verified {
		return 0, fmt.Errorf("%w: arbitration pk %d != candidate %d", errAssetWriteRetry, verified, candidate)
	}

	// 3) 身份不可变更新：只读非身份业务列，只写非身份业务列。
	old, err := s.assetReadCols(candidate, w.mergeCols)
	if err != nil {
		return 0, err
	}
	set, err := w.merge(old)
	if err != nil {
		return 0, err
	}
	for _, c := range set {
		if _, forbidden := assetForbiddenUpdateCols[c.Col]; forbidden {
			return 0, fmt.Errorf("assets: identity-immutable violation, UPDATE must never write column %q", c.Col)
		}
	}
	if err := s.assetUpdate(candidate, set); err != nil {
		return 0, err
	}

	// 4) 更新后复核：身份必须仍然逐字节等于输入 canonical 身份（零漂移）。
	if err := s.assetVerifyIdentity(ctx, b, candidate); err != nil {
		return 0, err
	}
	return candidate, nil
}

// assetPathB 是无候选行的严格插入路径。
func (s *AssetStore) assetPathB(ctx context.Context, w assetWrite) (int64, error) {
	b := w.branch

	cols := make([]assetCol, 0, len(w.insert)+len(assetNotNullDefaults)+len(assetBranchPair)*2)
	cols = append(cols, w.insert...)
	// 表上这些列是 NOT NULL 且没有 DEFAULT：任何分支漏写都会在 strict 模式下
	// 直接失败，所以由引擎统一兜底成合法 JSON 文本 / 默认归属来源。
	provided := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		provided[c.Col] = struct{}{}
	}
	for _, def := range assetNotNullDefaults {
		if _, ok := provided[def.Col]; !ok {
			cols = append(cols, def)
		}
	}
	for _, pair := range assetBranchPair {
		if pair[0] == b.identCol {
			cols = append(cols,
				assetCol{pair[0], b.identity},
				assetCol{pair[1], b.hash[:]},
			)
			continue
		}
		// 其余 7 个分支必须显式绑定 NULL（hash 唯一索引对 NULL 不去重，
		// 漏绑会让分支唯一性静默失效）。
		cols = append(cols,
			assetCol{pair[0], nil},
			assetCol{pair[1], nil},
		)
	}

	names := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Col)
		args = append(args, c.Val)
	}
	stmt := "INSERT INTO assets(" + strings.Join(names, ", ") + ") VALUES (" +
		strings.Join(repeatQuestion(len(cols)), ", ") + ")"

	res, err := s.tx.Exec(stmt, args...)
	if err != nil {
		if assetIsDuplicateKey(err) {
			// 并发插入赢家：本轮尝试必须整体回滚，由外层重试循环在全新事务里转 Path A。
			//
			// **必须用 %w 保留原始 *mysql.MySQLError（R2）**：接入 lockorder.TxRunner 的
			// 任务写者（TaskID > 0）拿不到本包私有的 withAssetMutation 循环，重试判定
			// 完全依赖 `lockorder.IsRetryableConflict` 从错误链里**结构化**取到 1062。
			// 用 %v 会把编号抹掉，输家只回滚一次就直接报错，永远不会在新事务里转 Path A。
			return 0, fmt.Errorf("%w: duplicate %s on branch %s: %w", errAssetWriteRetry, b.hashCol, b.name, err)
		}
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("assets: LastInsertId after INSERT: %w", err)
	}
	// LastInsertId 必须落在 [1, MaxInt64]：int64 天然满足上界，这里当场拒绝
	// 0 与负数（BIGINT UNSIGNED 溢出成负数时必须阻断，绝不能让脏 PK 扩散到 API）。
	if id < 1 || id > int64(math.MaxInt64) {
		return 0, fmt.Errorf("assets: inserted pk %d out of range [1, %d]", id, int64(math.MaxInt64))
	}

	// 同事务自洽校验：新行的持久化 identity 必须与输入逐字节一致且 PK 等于自增值。
	verified, err := schema.VerifyHashArbitration(ctx, s.tx, "assets", "id", b.identCol, b.hashCol, b.hash, b.identity)
	if err != nil {
		return 0, fmt.Errorf("assets: self-verification failed for fresh row on branch %s: %w", b.name, err)
	}
	if uint64(id) != verified {
		return 0, fmt.Errorf("assets: self-verification pk %d != inserted %d", verified, id)
	}
	return id, nil
}

// assetVerifyIdentity 复核持久化身份未发生漂移。
//
// 这里同样是**锁定当前读**：本事务已持有该 PK 的行锁，普通快照读看不到并发写者
// 已提交的身份改动，漂移复核就会静默失效（R2 同一口径）。
func (s *AssetStore) assetVerifyIdentity(ctx context.Context, b assetBranch, pk int64) error {
	var persisted []byte
	stmt := "SELECT " + b.identCol + " FROM assets WHERE id = ? FOR UPDATE"
	if err := s.tx.QueryRowContext(ctx, stmt, pk).Scan(&persisted); err != nil {
		return fmt.Errorf("assets: identity recheck on %d: %w", pk, err)
	}
	if !bytes.Equal(persisted, b.identity) {
		return fmt.Errorf("assets: identity drifted on pk %d branch %s after update", pk, b.name)
	}
	return nil
}

// assetReadCols 在**已持有该物理 PK 行锁**的前提下，用锁定当前读回读指定的非身份业务列。
//
// R2：必须是 `SELECT … FROM assets WHERE id = ? FOR UPDATE`，不能是普通 SELECT。
// 本事务更早的候选定位 / 归属解析已经建立了 REPEATABLE-READ 快照，PK 行锁本身不会
// 把后续普通 SELECT 变成当前读；用快照读去 merge 就会把并发写者已提交的业务元素
// 覆盖掉（lost update）。PK 锁此刻由本事务持有，FOR UPDATE 不会引入新的等待对象。
func (s *AssetStore) assetReadCols(pk int64, cols []string) (*assetSnapshot, error) {
	snapshot := &assetSnapshot{cols: map[string]any{}}
	if len(cols) == 0 {
		return snapshot, nil
	}
	stmt := "SELECT " + strings.Join(cols, ", ") + " FROM assets WHERE id = ? FOR UPDATE"
	row := s.tx.QueryRow(stmt, pk)
	targets := make([]any, 0, len(cols))
	for range cols {
		targets = append(targets, new(any))
	}
	if err := row.Scan(targets...); err != nil {
		return nil, fmt.Errorf("assets: read business columns on %d: %w", pk, err)
	}
	for i, col := range cols {
		if ptr, ok := targets[i].(*any); ok {
			snapshot.cols[col] = *ptr
		}
	}
	return snapshot, nil
}

// assetUpdate 只写非身份业务列（调用前已按 assetForbiddenUpdateCols 过滤）。
func (s *AssetStore) assetUpdate(pk int64, set []assetCol) error {
	if len(set) == 0 {
		return nil
	}
	assign := make([]string, 0, len(set))
	args := make([]any, 0, len(set)+1)
	for _, c := range set {
		if expr, ok := c.Val.(assetRawExpr); ok {
			assign = append(assign, c.Col+" = "+string(expr))
			continue
		}
		assign = append(assign, c.Col+" = ?")
		args = append(args, c.Val)
	}
	args = append(args, pk)
	if _, err := s.tx.Exec("UPDATE assets SET "+strings.Join(assign, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("assets: identity-immutable update on %d: %w", pk, err)
	}
	return nil
}

func repeatQuestion(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "?"
	}
	return out
}

// assetCompanyCols 把归属解析结果编成 company_id / company_source 两列。
func assetCompanyCols(id *int64, explicit bool) []assetCol {
	source := "scope"
	if explicit {
		source = "explicit"
	}
	var val any
	if id != nil {
		val = *id
	}
	return []assetCol{{"company_id", val}, {"company_source", source}}
}

// assetMergeCompanyCols 复刻原 ODKU 的归属优先级：
// 持久化 explicit 归属不可被 scope 推导覆盖；本次是 explicit 时直接覆盖。
func assetMergeCompanyCols(old *assetSnapshot, incoming *int64, incomingExplicit bool) []assetCol {
	persistedID, hasID := old.int64Val("company_id")
	persistedSource := old.str("company_source")

	if incomingExplicit && incoming != nil {
		return []assetCol{{"company_id", *incoming}, {"company_source", "explicit"}}
	}
	if persistedSource == "explicit" && hasID {
		return []assetCol{{"company_id", persistedID}, {"company_source", "explicit"}}
	}
	if incoming != nil {
		return []assetCol{{"company_id", *incoming}, {"company_source", "scope"}}
	}
	if hasID {
		source := persistedSource
		if source == "" {
			source = "scope"
		}
		return []assetCol{{"company_id", persistedID}, {"company_source", source}}
	}
	return nil
}

// assetTaskIDsOf 把单个 TaskID 编成切片（0 表示本次不归属任何任务）。
func assetTaskIDsOf(taskID int64) []int64 {
	if taskID > 0 {
		return []int64{taskID}
	}
	return nil
}

// runTaskBoundUpsert 是**带 TaskID 的资产 upsert** 的统一事务边界（R3）：
//
//   - s.tx != nil：外层已经持有业务门禁与父任务行锁（写者协议入口唯一赋予该推定），
//     直接执行；在同一事务里重复取父锁只会自锁；
//   - s.tx == nil 且 taskID > 0：经写者协议（全局业务门禁 → 行锁父任务 → 核验存活 →
//     整体退避重放）。正在被级联删除的任务不可能再被挂上新资产（I1 屏障）；
//   - s.tx == nil 且无 TaskID：走资产引擎自己的 1062/1205/1213 重试循环。
//
// 归属 link 的双写不在这里做，而在 writeAsset 这个**唯一写入出口**里完成：嵌套副作用
// （subdomain 顺带 upsert root_domain / IP 等）复用同一个 s.tx，同样会经过出口，
// 因此"task_ids 记了、link 没跟上"的分支被结构性排除。
func (s *AssetStore) runTaskBoundUpsert(taskID int64, upsert func(*AssetStore) (int64, error)) (int64, error) {
	if s.tx != nil {
		return upsert(s)
	}
	if taskID <= 0 {
		return s.withCompanyScopeMutation(upsert)
	}
	var out int64
	err := s.withTaskChildWriter(taskID, func(_ context.Context, tx *sql.Tx) error {
		scoped := &AssetStore{db: s.db, company: s.company, tx: tx}
		id, err := upsert(scoped)
		if err != nil {
			return err
		}
		out = id
		return nil
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

// =====================================================================
// UpsertRootDomain
// =====================================================================

// UpsertRootDomainReq is the input for UpsertRootDomain.
type UpsertRootDomainReq struct {
	Domain string
	ICP    string
	TaskID int64
}

// UpsertRootDomain idempotently inserts or merges a root domain asset.
func (s *AssetStore) UpsertRootDomain(req UpsertRootDomainReq) (int64, error) {
	domain := DomainKey(req.Domain)
	if domain == "" {
		return 0, fmt.Errorf("domain is required")
	}
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertRootDomain(req)
		})
	}
	companyID, err := s.resolveCompanyWithICP(domain, "", req.ICP)
	if err != nil {
		return 0, err
	}

	var icpVal any
	if req.ICP != "" {
		icpVal = req.ICP
	}

	branch := newAssetBranch("assets_root_domain", schema.AssetsRootDomainIdentity(domain))
	return s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "root_domain"},
			{"domain", domain},
			{"root_domain", domain},
			{"icp", icpVal},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
			{"extra", "{}"},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "icp", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{{"task_ids", taskIDs}}
			if req.ICP != "" && old.str("icp") == "" {
				set = append(set, assetCol{"icp", req.ICP})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
}

// ErrAssetIPInvalid marks a non-address value in an asset's ip field. Both
// insert_assets and the asset API report it per item with the item index, so one
// bad entry never costs the rest of the batch.
var ErrAssetIPInvalid = errors.New("invalid asset ip")

// ValidateAssetIP keeps hostnames out of assets.ip. Network attribution casts
// that column to inet (see try_inet in schema.sql), so a hostname stored here is
// silently invisible to every IP/CIDR scope rule — the asset simply never gets
// attributed and nobody can tell why. The message states the fix rather than
// just the fault, so an agent can correct the item on its next turn. An empty
// value is accepted: the ip field is optional for service and endpoint assets.
func ValidateAssetIP(value string) error {
	if value == "" || net.ParseIP(value) != nil {
		return nil
	}
	return fmt.Errorf(
		"%w: ip 必须是 IPv4/IPv6 地址，收到 %q。若这是主机名，请改用 type=subdomain 并填 domain 字段；"+
			"若确实要登记地址，请先解析出 A/AAAA 记录，再用解析出的地址填 ip",
		ErrAssetIPInvalid, value)
}

// =====================================================================
// UpsertIP
// =====================================================================

// UpsertIPReq is the input for UpsertIP.
type UpsertIPReq struct {
	IP           string
	BoundDomains []string
	OpenPorts    []PortService
	TaskID       int64
}

// UpsertIP idempotently inserts or merges an IP asset.
func (s *AssetStore) UpsertIP(req UpsertIPReq) (int64, error) {
	if req.IP == "" {
		return 0, fmt.Errorf("ip is required")
	}
	if err := ValidateAssetIP(req.IP); err != nil {
		return 0, err
	}
	ipStr, ok := canonicalIP(req.IP)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrAssetIPInvalid, req.IP)
	}
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertIP(req)
		})
	}
	cseg := calcCSegment(ipStr)
	companyID, err := s.resolveCompanyWithICP("", ipStr, "")
	if err != nil {
		return 0, err
	}

	boundDomains := marshalStringArray(req.BoundDomains)
	openPorts, err := marshalPortServices(req.OpenPorts)
	if err != nil {
		return 0, err
	}

	var csegVal any
	if cseg != "" {
		csegVal = cseg
	}

	addedPorts := make([]map[string]any, 0, len(req.OpenPorts))
	for _, p := range req.OpenPorts {
		addedPorts = append(addedPorts, map[string]any{"port": p.Port, "service": p.Service})
	}

	branch := newAssetBranch("assets_ip", schema.AssetsIPIdentity(ipStr))
	return s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "ip"},
			{"ip", ipStr},
			{"c_segment", csegVal},
			{"bound_domains", boundDomains},
			{"open_ports", openPorts},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "bound_domains", "open_ports", "c_segment", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
				{"bound_domains", assetUnionStrings(old.str("bound_domains"), req.BoundDomains)},
			}
			merged, err := assetMergeObjects(old.str("open_ports"), addedPorts, assetPortKey, assetPreferService)
			if err != nil {
				return nil, err
			}
			set = append(set, assetCol{"open_ports", merged})
			if csegVal != nil && old.str("c_segment") == "" {
				set = append(set, assetCol{"c_segment", csegVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
}

// AppendIPPort appends a {port, service} entry to an existing IP asset's open_ports.
//
// 入参必须先 canonicalIP：`::ffff:127.0.0.1` 与 `127.0.0.1` 是同一个地址，
// 直接拿原文去算 identity 会得到另一个 hash，从而把既有 IP 资产误判成"不存在"，
// 追加被静默丢弃（R4）。
func (s *AssetStore) AppendIPPort(ipStr string, port int, serviceName string) error {
	if ipStr == "" || port == 0 {
		return nil
	}
	canonical, ok := canonicalIP(ipStr)
	if !ok {
		return fmt.Errorf("%w: %s", ErrAssetIPInvalid, ipStr)
	}
	entry := map[string]any{"port": port, "service": serviceName}
	return s.appendIPJSONArray(canonical, "open_ports", func(old string) (string, error) {
		return assetMergeObjects(old, []map[string]any{entry}, assetPortKey, assetPreferService)
	})
}

// AppendIPBoundDomain appends a domain to an existing IP asset's bound_domains.
func (s *AssetStore) AppendIPBoundDomain(ipStr, domain string) error {
	if ipStr == "" || domain == "" {
		return nil
	}
	canonical, ok := canonicalIP(ipStr)
	if !ok {
		return fmt.Errorf("%w: %s", ErrAssetIPInvalid, ipStr)
	}
	return s.appendIPJSONArray(canonical, "bound_domains", func(old string) (string, error) {
		return assetUnionStrings(old, []string{domain}), nil
	})
}

// appendIPJSONArray 往既有 IP 资产的一个 JSON 数组列里追加元素
// （TiDB 8.5 不支持 JSON_TABLE，数组展开/去重一律在 Go 内存里完成）。
//
// 追加路径不是"协议之外"的捷径（R4）：它必须复用与 writeAsset 完全相同的公共契约，
//
//  1. 候选定位：按 hash 唯一索引做**普通**查询（只选路，不加锁）；
//  2. 物理 PK 预锁：SELECT id FROM assets WHERE id = ? FOR UPDATE；
//  3. 写前仲裁 + PK 匹配：schema.VerifyHashArbitration 命中的 PK 必须等于候选 PK；
//  4. 当前读回读：assetReadCols（持 PK 锁的 SELECT … FOR UPDATE）；
//  5. 非身份 UPDATE + 更新后身份零漂移复核。
//
// 按 hash 直接 FOR UPDATE 抢锁被 schema.VerifyHashArbitration 的前置契约明确禁止
// （hash 不是允许的物理预定位器）。IP 资产确实不存在时保持原实现的静默 no-op。
func (s *AssetStore) appendIPJSONArray(ipStr, column string, merge func(string) (string, error)) error {
	if _, forbidden := assetForbiddenUpdateCols[column]; forbidden {
		return fmt.Errorf("assets: append target %q is an identity column", column)
	}
	branch := newAssetBranch("assets_ip", schema.AssetsIPIdentity(ipStr))

	run := func(store *AssetStore) (int64, error) {
		candidate, err := store.assetCandidatePK(branch)
		if err != nil {
			return 0, err
		}
		if candidate == 0 {
			return 0, nil // IP 资产不存在：与原实现一致，静默 no-op
		}

		// 1) 物理 PK 预锁（与 Path A 同一条语句、同一个锁序）。
		var locked int64
		err = store.tx.QueryRow("SELECT id FROM assets WHERE id = ? FOR UPDATE", candidate).Scan(&locked)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: append %s candidate %d vanished before lock", errAssetWriteRetry, column, candidate)
		}
		if err != nil {
			return 0, err
		}
		if locked != candidate {
			return 0, fmt.Errorf("%w: append %s pk lock returned %d, want %d", errAssetWriteRetry, column, locked, candidate)
		}

		// 2) 写前仲裁：持锁保护下核验 identity 逐字节相等且 PK 与候选一致。
		verified, err := schema.VerifyHashArbitration(context.Background(), store.tx, "assets", "id",
			branch.identCol, branch.hashCol, branch.hash, branch.identity)
		if err != nil {
			return 0, fmt.Errorf("assets: append %s write-ahead arbitration: %w", column, err)
		}
		if uint64(candidate) != verified {
			return 0, fmt.Errorf("%w: append %s arbitration pk %d != candidate %d",
				errAssetWriteRetry, column, verified, candidate)
		}

		// 3) 当前读回读 + 内存合并 + 非身份更新 + 身份零漂移复核。
		snapshot, err := store.assetReadCols(candidate, []string{column})
		if err != nil {
			return 0, err
		}
		merged, err := merge(snapshot.str(column))
		if err != nil {
			return 0, err
		}
		set := []assetCol{{column, merged}, {"last_seen", assetRawExpr("NOW(3)")}}
		if err := store.assetUpdate(candidate, set); err != nil {
			return 0, fmt.Errorf("assets: write %s on %d: %w", column, candidate, err)
		}
		if err := store.assetVerifyIdentity(context.Background(), branch, candidate); err != nil {
			return 0, err
		}
		return candidate, nil
	}

	if s.tx != nil {
		// 同一事务：任何错误（含碰撞 / 1062 / 锁冲突）都必须原样上抛，由最外层
		// 事务运行器统一回滚或重放，绝不能在子写入里就地吞掉（R1）。
		_, err := run(s)
		return err
	}
	_, err := s.withAssetMutation(func(scoped *AssetStore) (int64, error) { return run(scoped) })
	return err
}

// =====================================================================
// UpsertSubdomain
// =====================================================================

// UpsertSubdomainReq is the input for UpsertSubdomain.
type UpsertSubdomainReq struct {
	Domain      string
	RecordType  string
	RecordValue []string
	ICP         string
	TaskID      int64
}

// UpsertSubdomain idempotently inserts or merges a subdomain asset and triggers
// side effects: root_domain upsert + IP bound_domains update.
func (s *AssetStore) UpsertSubdomain(req UpsertSubdomainReq) (id int64, err error) {
	domain := DomainKey(req.Domain)
	if domain == "" {
		return 0, fmt.Errorf("domain is required")
	}
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertSubdomain(req)
		})
	}

	rootDomain, _ := RootDomain(domain)
	if rootDomain == "" {
		rootDomain = domain
	}

	// Resolve by root domain first, then the asset's exact normalized ICP.
	companyID, err := s.resolveCompanyWithICP(rootDomain, "", req.ICP)
	if err != nil {
		return 0, err
	}

	// side effect 1: ensure root domain exists.
	// **必须传播错误（R1）**：这是同一事务内的子写入，它复用 s.tx，自己无法回滚；
	// 吞掉 ErrIdentityHashCollision / 1062 会让外层带着半完成的写入直接 Commit。
	if _, err := s.UpsertRootDomain(UpsertRootDomainReq{
		Domain: rootDomain,
		TaskID: req.TaskID,
	}); err != nil {
		return 0, fmt.Errorf("assets: subdomain side effect root_domain %q: %w", rootDomain, err)
	}

	// side effect 2: if A/AAAA record, upsert each IP + bind domain.
	// RecordValue is []string; each element may itself be comma-separated (legacy).
	var ipStr string // first valid IP, used for the subdomain row itself
	if req.RecordType == "A" || req.RecordType == "AAAA" {
		for _, rv := range req.RecordValue {
			for _, part := range strings.Split(rv, ",") {
				candidate := strings.TrimSpace(part)
				if candidate == "" || net.ParseIP(candidate) == nil {
					continue
				}
				canonical, ok := canonicalIP(candidate)
				if !ok {
					continue
				}
				if ipStr == "" {
					ipStr = canonical
				}
				if _, err := s.UpsertIP(UpsertIPReq{
					IP:           canonical,
					BoundDomains: []string{domain},
					TaskID:       req.TaskID,
				}); err != nil {
					return 0, fmt.Errorf("assets: subdomain side effect ip %q: %w", canonical, err)
				}
			}
		}
	}

	cseg := calcCSegment(ipStr)
	if companyID == nil && ipStr != "" {
		resolved, err := s.resolveCompanyWithICP("", ipStr, "")
		if err != nil {
			return 0, err
		}
		companyID = resolved
	}

	var icpVal, csegVal, ipVal any
	if req.ICP != "" {
		icpVal = req.ICP
	}
	if cseg != "" {
		csegVal = cseg
	}
	if ipStr != "" {
		ipVal = ipStr
	}
	// record_type 参与 canonical 身份：NULL 一律按 "" 参与 COALESCE。
	recordType := strings.TrimSpace(req.RecordType)
	recordValue := marshalStringArray(req.RecordValue)

	bindRecordType := any(nil)
	if recordType != "" {
		bindRecordType = recordType
	}

	branch := newAssetBranch("assets_subdomain", schema.AssetsSubdomainIdentity(domain, recordType))
	return s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "subdomain"},
			{"domain", domain},
			{"root_domain", rootDomain},
			{"record_type", bindRecordType},
			{"record_value", recordValue},
			{"ip", ipVal},
			{"c_segment", csegVal},
			{"icp", icpVal},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
			{"extra", "{}"},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "record_value", "ip", "c_segment", "icp", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
				{"record_value", assetUnionStrings(old.str("record_value"), req.RecordValue)},
			}
			if ipVal != nil && old.str("ip") == "" {
				set = append(set, assetCol{"ip", ipVal})
			}
			if csegVal != nil && old.str("c_segment") == "" {
				set = append(set, assetCol{"c_segment", csegVal})
			}
			if icpVal != nil && old.str("icp") == "" {
				set = append(set, assetCol{"icp", icpVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
}

// =====================================================================
// UpsertApp
// =====================================================================

// UpsertAppReq is the input for UpsertApp.
type UpsertAppReq struct {
	Name        string
	BundleID    string
	Category    string
	Description string
	ICP         string
	CompanyID   *int64 // explicit override; nil = exact ICP auto-attribution when available
	TaskID      int64
}

// UpsertApp idempotently inserts or merges an app asset. bundle_id 与 app_name
// 两个分支严格互斥：bundle_id 非空时只激活 app_bundle 分支，否则回落到 app_name。
func (s *AssetStore) UpsertApp(req UpsertAppReq) (int64, error) {
	if req.Name == "" {
		return 0, fmt.Errorf("app name is required")
	}
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertApp(req)
		})
	}

	var catVal, descVal, icpVal any
	if req.Category != "" {
		catVal = req.Category
	}
	if req.Description != "" {
		descVal = req.Description
	}
	if req.ICP != "" {
		icpVal = req.ICP
	}
	companyID := req.CompanyID
	explicit := companyID != nil
	if !explicit {
		resolved, err := s.resolveCompanyWithICP("", "", req.ICP)
		if err != nil {
			return 0, err
		}
		companyID = resolved
	}

	bundleID := strings.TrimSpace(req.BundleID)
	appName := strings.TrimSpace(req.Name)

	var branch assetBranch
	bundleCol := any(nil)
	if bundleID != "" {
		bundleCol = bundleID
		branch = newAssetBranch("assets_app_bundle", schema.AssetsAppBundleIdentity(bundleID))
	} else {
		branch = newAssetBranch("assets_app_name", schema.AssetsAppNameIdentity(appName))
	}

	return s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "app"},
			{"bundle_id", bundleCol},
			{"app_name", appName},
			{"category", catVal},
			{"app_description", descVal},
			{"app_icp", icpVal},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
		}, assetCompanyCols(companyID, explicit)...),
		mergeCols: []string{"task_ids", "category", "app_description", "app_icp", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
			}
			if catVal != nil && old.str("category") == "" {
				set = append(set, assetCol{"category", catVal})
			}
			if descVal != nil && old.str("app_description") == "" {
				set = append(set, assetCol{"app_description", descVal})
			}
			if icpVal != nil && old.str("app_icp") == "" {
				set = append(set, assetCol{"app_icp", icpVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, explicit)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
}

// =====================================================================
// UpsertHTTPService
// =====================================================================

// UpsertHTTPServiceReq is the input for UpsertHTTPService.
type UpsertHTTPServiceReq struct {
	URL           string
	Technologies  []string
	StatusCode    *int
	ContentLength *int64
	PageTitle     string
	FaviconMMH3   string
	Auth          []map[string]any
	IP            string // optional, from async DNS
	TaskID        int64
}

// UpsertHTTPService inserts or merges an HTTP service asset. Domain, port,
// service_name, and root_domain are auto-extracted from URL.
func (s *AssetStore) UpsertHTTPService(req UpsertHTTPServiceReq) (int64, error) {
	if req.URL == "" {
		return 0, fmt.Errorf("url is required")
	}
	if err := ValidateAssetIP(req.IP); err != nil {
		return 0, err
	}
	normURL := normalizeURL(req.URL)
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertHTTPService(req)
		})
	}
	domain, port, serviceName := parseURL(normURL)
	rootDomain, _ := RootDomain(domain)
	if rootDomain == "" {
		rootDomain = domain
	}

	canonicalIPStr := ""
	if req.IP != "" {
		canonical, ok := canonicalIP(req.IP)
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrAssetIPInvalid, req.IP)
		}
		canonicalIPStr = canonical
	}
	cseg := calcCSegment(canonicalIPStr)
	companyID, err := s.resolveCompanyWithICP(rootDomain, canonicalIPStr, "")
	if err != nil {
		return 0, err
	}

	techs := marshalStringArray(req.Technologies)
	authJSON, err := marshalJSONArray(req.Auth)
	if err != nil {
		return 0, err
	}

	var domainVal, ipVal, csegVal, titleVal, faviconVal any
	if domain != "" {
		domainVal = domain
	}
	if canonicalIPStr != "" {
		ipVal = canonicalIPStr
	}
	if cseg != "" {
		csegVal = cseg
	}
	if req.PageTitle != "" {
		titleVal = req.PageTitle
	}
	if req.FaviconMMH3 != "" {
		faviconVal = req.FaviconMMH3
	}

	branch := newAssetBranch("assets_service_http", schema.AssetsServiceHTTPIdentity(normURL))
	id, err := s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "service"},
			{"service_type", "http"},
			{"url", normURL},
			{"service_name", serviceName},
			{"domain", domainVal},
			{"ip", ipVal},
			{"port", nullableInt(port)},
			{"root_domain", rootDomain},
			{"c_segment", csegVal},
			{"favicon_mmh3", faviconVal},
			{"technologies", techs},
			{"status_code", req.StatusCode},
			{"content_length", req.ContentLength},
			{"page_title", titleVal},
			{"auth", authJSON},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "technologies", "auth", "status_code", "content_length",
			"page_title", "favicon_mmh3", "ip", "c_segment", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
				{"technologies", assetUnionStrings(old.str("technologies"), req.Technologies)},
			}
			mergedAuth, err := assetMergeObjects(old.str("auth"), req.Auth, assetJSONText, nil)
			if err != nil {
				return nil, err
			}
			set = append(set, assetCol{"auth", mergedAuth})
			if req.StatusCode != nil {
				set = append(set, assetCol{"status_code", *req.StatusCode})
			}
			if req.ContentLength != nil {
				set = append(set, assetCol{"content_length", *req.ContentLength})
			}
			if titleVal != nil && old.str("page_title") == "" {
				set = append(set, assetCol{"page_title", titleVal})
			}
			if faviconVal != nil && old.str("favicon_mmh3") == "" {
				set = append(set, assetCol{"favicon_mmh3", faviconVal})
			}
			if ipVal != nil && old.str("ip") == "" {
				set = append(set, assetCol{"ip", ipVal}, assetCol{"c_segment", csegVal})
			} else if csegVal != nil && old.str("c_segment") == "" {
				set = append(set, assetCol{"c_segment", csegVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
	if err != nil {
		return 0, err
	}

	// side effects: register root_domain + subdomain as their own assets too.
	// 主写入已完成也不代表可以吞错（R1）：子写入失败时整笔事务必须回滚，
	// 上面那条 service_http INSERT 一起作废，绝不允许"半提交"。
	if err := s.linkHostAssets(domain, rootDomain, req.TaskID); err != nil {
		return 0, err
	}
	if canonicalIPStr != "" {
		var boundDomains []string
		if domain != "" {
			boundDomains = []string{domain}
		}
		var openPorts []PortService
		if port > 0 {
			openPorts = []PortService{{Port: port, Service: serviceName}}
		}
		if _, err := s.UpsertIP(UpsertIPReq{
			IP:           canonicalIPStr,
			BoundDomains: boundDomains,
			OpenPorts:    openPorts,
			TaskID:       req.TaskID,
		}); err != nil {
			return 0, fmt.Errorf("assets: http service side effect ip %q: %w", canonicalIPStr, err)
		}
	}
	return id, nil
}

// =====================================================================
// UpsertOtherService
// =====================================================================

// UpsertOtherServiceReq is the input for UpsertOtherService.
type UpsertOtherServiceReq struct {
	Domain      string // domain or ip required
	IP          string
	Port        int
	ServiceName string
	Auth        []map[string]any
	TaskID      int64
}

// UpsertOtherService inserts or merges a non-HTTP service asset.
func (s *AssetStore) UpsertOtherService(req UpsertOtherServiceReq) (int64, error) {
	if req.Domain == "" && req.IP == "" {
		return 0, fmt.Errorf("domain or ip is required")
	}
	if err := ValidateAssetIP(req.IP); err != nil {
		return 0, err
	}
	if req.Port == 0 {
		return 0, fmt.Errorf("port is required")
	}
	// port 必须落在合法 1..65535 区间，越界值会污染 canonical 身份。
	if req.Port < 1 || req.Port > 65535 {
		return 0, fmt.Errorf("port must be in 1..65535, got %d", req.Port)
	}
	if req.ServiceName == "" {
		return 0, fmt.Errorf("service_name is required")
	}
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertOtherService(req)
		})
	}
	// normalize service_name (lowercase) so the canonical identity doesn't split
	// "SSH" and "ssh" into separate rows.
	serviceName := strings.ToLower(strings.TrimSpace(req.ServiceName))

	domain := DomainKey(req.Domain)
	canonicalIPStr := ""
	if req.IP != "" {
		canonical, ok := canonicalIP(req.IP)
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrAssetIPInvalid, req.IP)
		}
		canonicalIPStr = canonical
	}
	var rootDomain string
	if domain != "" {
		rootDomain, _ = RootDomain(domain)
		if rootDomain == "" {
			rootDomain = domain
		}
	}

	cseg := calcCSegment(canonicalIPStr)
	companyID, err := s.resolveCompanyWithICP(rootDomain, canonicalIPStr, "")
	if err != nil {
		return 0, err
	}

	authJSON, err := marshalJSONArray(req.Auth)
	if err != nil {
		return 0, err
	}

	var domainVal, rootDomainVal, ipVal, csegVal any
	if domain != "" {
		domainVal = domain
	}
	if rootDomain != "" {
		rootDomainVal = rootDomain
	}
	if canonicalIPStr != "" {
		ipVal = canonicalIPStr
	}
	if cseg != "" {
		csegVal = cseg
	}

	// canonical 身份里的 domain/ip 先按 COALESCE 归一为 ""。
	branch := newAssetBranch("assets_service_other", schema.AssetsServiceOtherIdentity(
		domain, canonicalIPStr, uint64(req.Port), false, serviceName, false))
	id, err := s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "service"},
			{"service_type", "other"},
			{"service_name", serviceName},
			{"domain", domainVal},
			{"ip", ipVal},
			{"port", req.Port},
			{"root_domain", rootDomainVal},
			{"c_segment", csegVal},
			{"auth", authJSON},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "auth", "c_segment", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
			}
			mergedAuth, err := assetMergeObjects(old.str("auth"), req.Auth, assetJSONText, nil)
			if err != nil {
				return nil, err
			}
			set = append(set, assetCol{"auth", mergedAuth})
			if csegVal != nil && old.str("c_segment") == "" {
				set = append(set, assetCol{"c_segment", csegVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
	if err != nil {
		return 0, err
	}

	// side effects（同事务子写入：错误必须上抛，见 R1）
	if canonicalIPStr != "" && req.Port > 0 {
		var boundDomains []string
		if domain != "" {
			boundDomains = []string{domain}
		}
		if _, err := s.UpsertIP(UpsertIPReq{
			IP:           canonicalIPStr,
			BoundDomains: boundDomains,
			OpenPorts:    []PortService{{Port: req.Port, Service: serviceName}},
			TaskID:       req.TaskID,
		}); err != nil {
			return 0, fmt.Errorf("assets: service_other side effect ip %q: %w", canonicalIPStr, err)
		}
	}
	if err := s.linkHostAssets(domain, rootDomain, req.TaskID); err != nil {
		return 0, err
	}
	return id, nil
}

// linkHostAssets ensures a service/endpoint's host is also registered as its own
// root_domain and (when it's a real subdomain, not the apex or an IP) subdomain
// asset — so those asset types stay populated and can anchor task scope.
//
// 它**不是 best-effort**（R1）：这两个 upsert 与调用方共用同一事务，任何碰撞 / 1062 /
// 锁冲突都必须原样返回给最外层事务边界，由它统一回滚或在新事务里重放。
func (s *AssetStore) linkHostAssets(domain, rootDomain string, taskID int64) error {
	if rootDomain != "" {
		if _, err := s.UpsertRootDomain(UpsertRootDomainReq{Domain: rootDomain, TaskID: taskID}); err != nil {
			return fmt.Errorf("assets: link root_domain %q: %w", rootDomain, err)
		}
	}
	if domain != "" && domain != rootDomain && net.ParseIP(domain) == nil {
		if _, err := s.UpsertSubdomain(UpsertSubdomainReq{Domain: domain, TaskID: taskID}); err != nil {
			return fmt.Errorf("assets: link subdomain %q: %w", domain, err)
		}
	}
	return nil
}

// =====================================================================
// UpsertEndpoint
// =====================================================================

// UpsertEndpointReq is the input for UpsertEndpoint.
type UpsertEndpointReq struct {
	URL    string
	Method string
	Params []map[string]any
	IP     string // optional
	TaskID int64
}

// UpsertEndpoint inserts or merges an endpoint asset. Domain, port, root_domain
// are auto-extracted from the URL.
func (s *AssetStore) UpsertEndpoint(req UpsertEndpointReq) (int64, error) {
	if req.URL == "" {
		return 0, fmt.Errorf("url is required")
	}
	if req.Method == "" {
		return 0, fmt.Errorf("method is required")
	}
	if err := ValidateAssetIP(req.IP); err != nil {
		return 0, err
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	normURL := normalizeURL(req.URL)
	if s.tx == nil {
		return s.runTaskBoundUpsert(req.TaskID, func(scoped *AssetStore) (int64, error) {
			return scoped.UpsertEndpoint(req)
		})
	}
	domain, port, _ := parseURL(normURL)
	rootDomain, _ := RootDomain(domain)
	if rootDomain == "" {
		rootDomain = domain
	}

	canonicalIPStr := ""
	if req.IP != "" {
		canonical, ok := canonicalIP(req.IP)
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrAssetIPInvalid, req.IP)
		}
		canonicalIPStr = canonical
	}
	cseg := calcCSegment(canonicalIPStr)
	companyID, err := s.resolveCompanyWithICP(rootDomain, canonicalIPStr, "")
	if err != nil {
		return 0, err
	}

	paramsJSON, err := marshalJSONArray(req.Params)
	if err != nil {
		return 0, err
	}

	var domainVal, rootDomainVal, ipVal, csegVal any
	if domain != "" {
		domainVal = domain
	}
	if rootDomain != "" {
		rootDomainVal = rootDomain
	}
	if canonicalIPStr != "" {
		ipVal = canonicalIPStr
	}
	if cseg != "" {
		csegVal = cseg
	}

	branch := newAssetBranch("assets_endpoint", schema.AssetsEndpointIdentity(normURL, method))
	id, err := s.writeAsset(assetWrite{
		branch: branch,
		taskID: req.TaskID,
		insert: append([]assetCol{
			{"type", "endpoint"},
			{"url", normURL},
			{"method", method},
			{"domain", domainVal},
			{"ip", ipVal},
			{"port", nullableInt(port)},
			{"root_domain", rootDomainVal},
			{"c_segment", csegVal},
			{"params", paramsJSON},
			{"task_ids", marshalTaskIDs(assetTaskIDsOf(req.TaskID))},
		}, assetCompanyCols(companyID, false)...),
		mergeCols: []string{"task_ids", "params", "ip", "c_segment", "company_id", "company_source"},
		merge: func(old *assetSnapshot) ([]assetCol, error) {
			taskIDs, err := assetUnionTaskIDs(old.str("task_ids"), assetTaskIDsOf(req.TaskID))
			if err != nil {
				return nil, err
			}
			set := []assetCol{
				{"task_ids", taskIDs},
			}
			mergedParams, err := assetMergeObjects(old.str("params"), req.Params, assetParamKey, func(cur, next map[string]any) bool {
				// 同 (location, name)：元素 JSON 文本 DESC 优先（spec §3.3.1 params 去重）。
				return assetJSONText(next) > assetJSONText(cur)
			})
			if err != nil {
				return nil, err
			}
			set = append(set, assetCol{"params", mergedParams})
			if ipVal != nil && old.str("ip") == "" {
				set = append(set, assetCol{"ip", ipVal}, assetCol{"c_segment", csegVal})
			} else if csegVal != nil && old.str("c_segment") == "" {
				set = append(set, assetCol{"c_segment", csegVal})
			}
			set = append(set, assetMergeCompanyCols(old, companyID, false)...)
			set = append(set, assetCol{"last_seen", assetRawExpr("NOW(3)")})
			return set, nil
		},
	})
	if err != nil {
		return 0, err
	}
	// side effects: endpoint previously registered none — register its host as
	// root_domain + subdomain(+IP) so those asset types get populated too.
	// 同事务子写入：错误一律上抛（R1）。
	if err := s.linkHostAssets(domain, rootDomain, req.TaskID); err != nil {
		return 0, err
	}
	if canonicalIPStr != "" {
		if _, err := s.UpsertIP(UpsertIPReq{IP: canonicalIPStr, TaskID: req.TaskID}); err != nil {
			return 0, fmt.Errorf("assets: endpoint side effect ip %q: %w", canonicalIPStr, err)
		}
	}
	return id, nil
}

// =====================================================================
// Query helpers
// =====================================================================

// assetArrayCol 把一个 JSON 文本数组列安全读成 JSON（NULL → 空数组）。
func assetArrayCol(col string) string {
	return "COALESCE(" + col + ", '[]')"
}

// assetTextCol 把一个可空文本列读成 ” 兜底。
func assetTextCol(col string) string {
	return "COALESCE(" + col + ", '')"
}

// assetJSONExtractInt 是 JSON 数组包含判定的表达式：task_ids 里是否含某个 task_id。
func assetJSONContains(col string, placeholder string) string {
	return "JSON_CONTAINS(" + col + ", CAST(" + placeholder + " AS JSON))"
}

// QueryByType returns assets rows of a given type, newest first.
func (s *AssetStore) QueryByType(typ string, limit, offset int) ([]*Asset, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	args := []any{typ}
	q := assetSelectCols + "\nWHERE type = ?" + pageClauseArgs(&args, limit, offset)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAssets(rows)
}

// CountByType returns the total number of assets of a type (for server-side pagination).
func (s *AssetStore) CountByType(typ string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM assets WHERE type = ?`, typ).Scan(&n)
	return n, err
}

// QueryByCompany returns assets for a company, optionally filtered by type.
// limit <= 0 means no limit.
func (s *AssetStore) QueryByCompany(companyID int64, typ string, limit, offset int) ([]*Asset, error) {
	q := assetSelectCols + " WHERE company_id = ?"
	args := []any{companyID}
	if typ != "" {
		args = append(args, typ)
		q += ` AND type = ?`
	}
	q += pageClauseArgs(&args, limit, offset)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAssets(rows)
}

func (s *AssetStore) CountByCompany(companyID int64, typ string) (int, error) {
	q := `SELECT count(*) FROM assets WHERE company_id = ?`
	args := []any{companyID}
	if typ != "" {
		args = append(args, typ)
		q += ` AND type = ?`
	}
	var n int
	err := s.db.QueryRow(q, args...).Scan(&n)
	return n, err
}

// pageClauseArgs appends positional LIMIT/OFFSET args and returns the clause.
func pageClauseArgs(args *[]any, limit, offset int) string {
	q := ` ORDER BY last_seen DESC, id DESC`
	if limit > 0 {
		*args = append(*args, limit)
		q += ` LIMIT ?`
	}
	if offset > 0 {
		*args = append(*args, offset)
		q += ` OFFSET ?`
	}
	return q
}

// pageClause keeps the historical name used by sibling stores.
func pageClause(args *[]any, limit, offset int) string {
	return pageClauseArgs(args, limit, offset)
}

// QueryByTask returns assets rows that have a given task_id in task_ids.
func (s *AssetStore) QueryByTask(taskID int64, typ string, limit, offset int) ([]*Asset, error) {
	q := assetSelectCols + " WHERE " + assetJSONContains("task_ids", "?")
	args := []any{taskID}
	if typ != "" {
		args = append(args, typ)
		q += ` AND type = ?`
	}
	q += pageClauseArgs(&args, limit, offset)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assets, err := scanAssets(rows)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateTaskAssetSources(taskID, assets); err != nil {
		return nil, err
	}
	return assets, nil
}

func (s *AssetStore) CountByTask(taskID int64, typ string) (int, error) {
	q := `SELECT count(*) FROM assets WHERE ` + assetJSONContains("task_ids", "?")
	args := []any{taskID}
	if typ != "" {
		args = append(args, typ)
		q += ` AND type = ?`
	}
	var n int
	err := s.db.QueryRow(q, args...).Scan(&n)
	return n, err
}

func (s *AssetStore) CountsByTypeForTask(taskID int64) (map[string]int, error) {
	rows, err := s.db.Query(`SELECT type, COUNT(*) FROM assets WHERE `+assetJSONContains("task_ids", "?")+` GROUP BY type`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTypeCounts(rows)
}

// DeleteByTaskID removes assets owned only by taskID and detaches taskID from
// assets shared with other tasks. Full task deletion uses the coordinated
// transaction in DeleteTaskCascadePrepared; this method remains for callers
// that explicitly manage only asset associations.
//
// TiDB 8.5 不支持 JSON_TABLE，"移除单个元素" 与 "是否独占" 判定在 Go 内存里完成，
// 并且整段跑在同一事务内（避免读改写窗口里的归属漂移）。
func (s *AssetStore) DeleteByTaskID(taskID int64) (int64, error) {
	if s.tx != nil {
		return s.deleteByTaskIDTx(s.tx, taskID)
	}
	var deleted int64
	err := s.db.execInTx(func(tx *sql.Tx) error {
		var err error
		deleted, err = s.deleteByTaskIDTx(tx, taskID)
		return err
	})
	return deleted, err
}

func (s *AssetStore) deleteByTaskIDTx(tx *sql.Tx, taskID int64) (int64, error) {
	rows, err := tx.Query("SELECT id, "+assetArrayCol("task_ids")+" FROM assets WHERE "+assetJSONContains("task_ids", "?"), taskID)
	if err != nil {
		return 0, err
	}
	var owned []int64
	type sharedRow struct {
		id      int64
		taskIDs string
	}
	var shared []sharedRow
	for rows.Next() {
		var id int64
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		persisted, perr := parseTaskIDArray(raw)
		if perr != nil {
			// 归属列损坏时**绝不**按"空归属"处理：那会把整行判成 taskID 独占并删除。
			rows.Close()
			return 0, perr
		}
		kept := make([]int64, 0, 4)
		for _, ownedID := range persisted {
			if ownedID != taskID {
				kept = append(kept, ownedID)
			}
		}
		if len(kept) == 0 {
			owned = append(owned, id)
			continue
		}
		shared = append(shared, sharedRow{id: id, taskIDs: marshalTaskIDs(kept)})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	deleted := int64(0)
	if len(owned) > 0 {
		args := make([]any, 0, len(owned))
		for _, id := range owned {
			args = append(args, id)
		}
		res, err := tx.Exec("DELETE FROM assets WHERE id IN ("+strings.Join(repeatQuestion(len(owned)), ",")+")", args...)
		if err != nil {
			return 0, err
		}
		deleted, _ = res.RowsAffected()
	}
	for _, item := range shared {
		if _, err := tx.Exec("UPDATE assets SET task_ids = ? WHERE id = ?", item.taskIDs, item.id); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// HostsByTask returns the exact HTTP host candidates attached to a task's
// assets. Domain/IP columns cover root domains, subdomains and non-HTTP
// services; URL covers HTTP services and endpoints.
func (s *AssetStore) HostsByTask(taskID int64) ([]string, error) {
	rows, err := s.db.Query(`
SELECT `+assetTextCol("domain")+`, `+assetTextCol("ip")+`, `+assetTextCol("url")+`
FROM assets WHERE `+assetJSONContains("task_ids", "?"), taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hosts := make(map[string]struct{})
	add := func(host string) {
		host = strings.TrimSpace(strings.ToLower(host))
		if host != "" {
			hosts[host] = struct{}{}
		}
	}
	for rows.Next() {
		var domain, ip, rawURL string
		if err := rows.Scan(&domain, &ip, &rawURL); err != nil {
			return nil, err
		}
		add(domain)
		add(ip)
		if u, err := url.Parse(rawURL); err == nil {
			add(u.Hostname())
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(hosts))
	for host := range hosts {
		out = append(out, host)
	}
	sort.Strings(out)
	return out, nil
}

// HostsForTaskDeletion returns hosts that belong to the task being deleted and
// are not referenced by any other live task. Current-task candidates include
// both task_ids ownership and exploration anchors so legacy seeded assets (which
// were anchor-only) are covered. Protection is host-wide: if another live task
// references any asset for a candidate host, that host's global traffic remains.
func (s *AssetStore) HostsForTaskDeletion(taskID, explorationID int64) ([]string, error) {
	return hostsForTaskDeletion(s.db, taskID, explorationID)
}

type rowsQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// hostsForTaskDeletion is shared by the read-only AssetStore API and the task
// deletion transaction. Coordinated traffic deletion must call it through the
// transaction path so asset/anchor writes stay locked until the task delete is
// committed.
func hostsForTaskDeletion(q rowsQuerier, taskID, explorationID int64) ([]string, error) {
	rows, err := q.Query(`
SELECT COALESCE(a.domain,''), COALESCE(a.ip,''), COALESCE(a.url,''), true
FROM assets a
WHERE `+assetJSONContains("a.task_ids", "?")+`
   OR EXISTS (
        SELECT 1 FROM exploration_anchors ea
        JOIN exploration_nodes n ON n.id = ea.node_id
        WHERE n.exploration_id = ? AND ea.asset_id = a.id
   )
UNION ALL
SELECT COALESCE(a.domain,''), COALESCE(a.ip,''), COALESCE(a.url,''), false
FROM assets a
WHERE a.id NOT IN (
        SELECT a2.id FROM assets a2
        WHERE `+assetJSONContains("a2.task_ids", "?")+`
           OR EXISTS (
                SELECT 1 FROM exploration_anchors ea
                JOIN exploration_nodes n ON n.id = ea.node_id
                WHERE n.exploration_id = ? AND ea.asset_id = a2.id
           )
   )
   AND (
        EXISTS (
            SELECT 1
            FROM exploration_anchors ea
            JOIN exploration_nodes n ON n.id = ea.node_id
            JOIN tasks t2 ON t2.exploration_id = n.exploration_id
            WHERE ea.asset_id = a.id AND t2.id <> ? AND t2.deleted_at IS NULL
        )
        OR EXISTS (
            SELECT 1 FROM tasks t
            WHERE t.id <> ? AND t.deleted_at IS NULL
              AND (
                    (JSON_LENGTH(a.task_ids) = 1 AND `+assetJSONContains("a.task_ids", "t.id")+`)
                 OR `+assetJSONContains("a.task_ids", "?")+`
                  )
        )
   )`, taskID, explorationID, taskID, explorationID, taskID, taskID, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make(map[string]struct{})
	protected := make(map[string]struct{})
	add := func(dst map[string]struct{}, raw string) {
		raw = strings.TrimSpace(strings.ToLower(raw))
		if raw != "" {
			dst[raw] = struct{}{}
		}
	}
	for rows.Next() {
		var domain, ip, rawURL string
		var candidate bool
		if err := rows.Scan(&domain, &ip, &rawURL, &candidate); err != nil {
			return nil, err
		}
		dst := protected
		if candidate {
			dst = candidates
		}
		add(dst, domain)
		add(dst, ip)
		if u, err := url.Parse(rawURL); err == nil {
			add(dst, u.Hostname())
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(candidates))
	for host := range candidates {
		if _, shared := protected[host]; !shared {
			out = append(out, host)
		}
	}
	sort.Strings(out)
	return out, nil
}

var assetSelectCols = `SELECT id, type, company_id, ` + assetArrayCol("task_ids") + `,
       ` + assetTextCol("domain") + `, ` + assetTextCol("root_domain") + `, ` + assetTextCol("ip") + `,
       ` + assetTextCol("c_segment") + `, port,
       ` + assetTextCol("icp") + `, ` + assetArrayCol("bound_domains") + `, ` + assetArrayCol("open_ports") + `, ` + assetTextCol("record_type") + `,
       ` + assetArrayCol("record_value") + `, ` + assetTextCol("bundle_id") + `, ` + assetTextCol("app_name") + `,
       ` + assetTextCol("category") + `, ` + assetTextCol("app_description") + `, ` + assetTextCol("app_icp") + `,
       ` + assetTextCol("url") + `, ` + assetTextCol("service_type") + `, ` + assetTextCol("service_name") + `,
       ` + assetTextCol("favicon_mmh3") + `, status_code, content_length,
       ` + assetTextCol("page_title") + `, ` + assetArrayCol("technologies") + `, ` + assetArrayCol("auth") + `,
       ` + assetTextCol("method") + `, ` + assetArrayCol("params") + `, COALESCE(extra, '{}'),
       DATE_FORMAT(last_seen, '%Y-%m-%dT%H:%i:%s.%f')
FROM assets`

// GetByIDs returns assets with the given ids (order preserved by id array order).
func (s *AssetStore) GetByIDs(ids []int64) ([]*Asset, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := repeatQuestion(len(ids))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	sqlText := assetSelectCols + " WHERE id IN (" + strings.Join(placeholders, ",") + ") ORDER BY last_seen DESC"
	rows, err := s.db.Query(sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAssets(rows)
}

// DeleteByCompanyID hard-deletes all assets belonging to a company. Returns rows deleted.
func (s *AssetStore) DeleteByCompanyID(companyID int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM assets WHERE company_id = ?`, companyID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteByHost hard-deletes every asset whose host exactly matches the given value:
// root_domain / subdomain / service / endpoint (they carry the host in domain or
// root_domain) plus an ip asset and its services/endpoints (ip column). The host is
// normalized the same way it is stored (DomainKey: lowercase/trim/strip trailing dot)
// so matching is exact, not fuzzy. Passing a root domain also removes its subdomains
// and their services/endpoints (they carry root_domain = that host); passing a
// subdomain/IP removes only that host's own assets. Referencing exploration_anchors
// rows are cleaned by ON DELETE CASCADE. Returns rows deleted, grouped by type.
//
// TiDB 没有 DELETE … RETURNING：先在同一事务里按候选行加锁读回 type，再按 PK 删除，
// 分组计数与原来的 RETURNING type 完全一致。
func (s *AssetStore) DeleteByHost(host string) (map[string]int64, error) {
	h := DomainKey(host)
	if h == "" {
		return nil, fmt.Errorf("host is required")
	}
	counts := map[string]int64{}
	err := s.db.execInTx(func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT id, type FROM assets
WHERE domain = ? OR root_domain = ? OR ip = ? FOR UPDATE`, h, h, h)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			var typ string
			if err := rows.Scan(&id, &typ); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
			counts[typ]++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		placeholders := repeatQuestion(len(ids))
		args := make([]any, 0, len(ids))
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := tx.Exec("DELETE FROM assets WHERE id IN ("+strings.Join(placeholders, ",")+")", args...); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}

// DeleteByIDs hard-deletes assets by their IDs. Returns the number of rows deleted.
func (s *AssetStore) DeleteByIDs(ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := repeatQuestion(len(ids))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	sqlText := "DELETE FROM assets WHERE id IN (" + strings.Join(placeholders, ",") + ")"
	res, err := s.db.Exec(sqlText, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountsByType returns asset counts per type.
func (s *AssetStore) CountsByType() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT type, COUNT(*) FROM assets GROUP BY type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTypeCounts(rows)
}

func scanTypeCounts(rows *sql.Rows) (map[string]int, error) {
	out := map[string]int{}
	for rows.Next() {
		var typ string
		var cnt int
		if err := rows.Scan(&typ, &cnt); err != nil {
			return nil, err
		}
		out[typ] = cnt
	}
	return out, rows.Err()
}

// scanAssets scans the wide SELECT that covers all type columns.
func scanAssets(rows *sql.Rows) ([]*Asset, error) {
	var out []*Asset
	for rows.Next() {
		a := &Asset{}
		var taskIDsRaw, boundDomainsRaw, openPortsRaw, recordValueRaw, techsRaw, authRaw, paramsRaw, extraRaw []byte
		var port, statusCode sql.NullInt64
		var contentLength sql.NullInt64
		var companyID sql.NullInt64
		if err := rows.Scan(
			&a.ID, &a.Type, &companyID, &taskIDsRaw,
			&a.Domain, &a.RootDomain, &a.IP, &a.CSegment, &port,
			&a.ICP, &boundDomainsRaw, &openPortsRaw, &a.RecordType,
			&recordValueRaw, &a.BundleID, &a.AppName,
			&a.Category, &a.AppDescription, &a.AppICP,
			&a.URL, &a.ServiceType, &a.ServiceName,
			&a.FaviconMMH3, &statusCode, &contentLength,
			&a.PageTitle, &techsRaw, &authRaw,
			&a.Method, &paramsRaw, &extraRaw, &a.LastSeen,
		); err != nil {
			return nil, err
		}
		if companyID.Valid {
			cid := companyID.Int64
			a.CompanyID = &cid
		}
		if port.Valid {
			p := int(port.Int64)
			a.Port = &p
		}
		if statusCode.Valid {
			sc := int(statusCode.Int64)
			a.StatusCode = &sc
		}
		if contentLength.Valid {
			cl := contentLength.Int64
			a.ContentLength = &cl
		}
		// parse arrays
		if len(taskIDsRaw) > 0 {
			_ = json.Unmarshal(taskIDsRaw, &a.TaskIDs)
		}
		if len(boundDomainsRaw) > 0 {
			_ = json.Unmarshal(boundDomainsRaw, &a.BoundDomains)
		}
		if len(openPortsRaw) > 0 {
			_ = json.Unmarshal(openPortsRaw, &a.OpenPorts)
		}
		if len(recordValueRaw) > 0 {
			_ = json.Unmarshal(recordValueRaw, &a.RecordValue)
		}
		if len(techsRaw) > 0 {
			_ = json.Unmarshal(techsRaw, &a.Technologies)
		}
		if len(authRaw) > 0 {
			_ = json.Unmarshal(authRaw, &a.Auth)
		}
		if len(paramsRaw) > 0 {
			_ = json.Unmarshal(paramsRaw, &a.Params)
		}
		if len(extraRaw) > 0 {
			_ = json.Unmarshal(extraRaw, &a.Extra)
		}
		if a.TaskIDs == nil {
			a.TaskIDs = []int64{}
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
