package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// =====================================================================
// 公司主体层（P08-02 Task 2.2：原生 TiDB 8.5 方言，M09 / M19 / M35）
//
// 三条硬性约定：
//  1. 占位符一律 `?`，自增 PK 一律 `LastInsertId()`（TiDB 没有 RETURNING）；
//  2. hash 唯一键表的每次 INSERT 都必须写满对应的 (identity, hash) 一对，
//     否则唯一键退化成 NULL、去重彻底失效（M09 的根因）；
//  3. 新库没有 FK 与 CHECK，删除路径的级联清理/置空必须自己写清楚（M19）。
// =====================================================================

// Company is a row in the companies table.
//
// P03（R06）：ID 是 BIGINT 主键，对外一律带引号 string（>2^53 不失真）。该结构体只
// 由 SQL 扫描填充，不是请求体解码目标，也没有整包 json.Marshal 后回读的路径。
type Company struct {
	ID        int64   `json:"id,string"`
	Name      string  `json:"name"`
	NKey      string  `json:"nkey"`
	Logo      *string `json:"logo,omitempty"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

// CompanyWithScope extends Company with its scope rules and asset count.
type CompanyWithScope struct {
	Company
	Scope      []ScopeRule `json:"scope"`
	AssetCount int         `json:"asset_count"`
}

// ScopeRule is one company_scope row.
//
// P03（R07）：ID / CompanyID 是 BIGINT，对外 string；仅 SQL 扫描填充，无解码/回读路径。
type ScopeRule struct {
	ID        int64  `json:"id,string"`
	CompanyID int64  `json:"company_id,string"`
	Kind      string `json:"kind"`
	Domain    string `json:"domain,omitempty"`
	Net       string `json:"net,omitempty"`
	Value     string `json:"value,omitempty"`
	Raw       string `json:"raw"`
	Reason    string `json:"reason,omitempty"`
}

// CompanyStore operates on the companies + company_scope tables.
type CompanyStore struct{ db *DB }

var (
	ErrCompanyNameConflict = errors.New("company name already exists")
	ErrCompanyNotFound     = errors.New("company not found")
)

const (
	// 企业范围不限制规则条数:逐个 IP / 域名录入的范围动辄上千条,封顶只会逼用户
	// 拆成多个企业。请求体大小(server 侧 maxCompanyMutationBodyBytes)仍然兜底。
	//
	// Raw and normalized textual scope payloads are bounded by Unicode rune
	// count so multi-byte input is treated consistently by the API and DB layer.
	MaxCompanyScopeRawRunes   = 1024
	MaxCompanyScopeValueRunes = 1024
)

// CompanyScopeValidationError identifies a client-correctable scope error.
// Storage and transaction failures are returned as ordinary errors instead.
type CompanyScopeValidationError struct{ Message string }

func (e *CompanyScopeValidationError) Error() string { return e.Message }

// ValidateCompanyScopeInputBounds applies request-wide limits before parsing.
// Store methods call it again so non-HTTP callers cannot bypass the limits.
// 只约束单条规则的长度,不限制条数。
func ValidateCompanyScopeInputBounds(inputs []ScopeInput) error {
	for i, input := range inputs {
		if utf8.RuneCountInString(input.Value) > MaxCompanyScopeRawRunes {
			return &CompanyScopeValidationError{Message: fmt.Sprintf(
				"企业范围第 %d 条原始值过长: 最多 %d 个字符", i+1, MaxCompanyScopeRawRunes,
			)}
		}
	}
	return nil
}

// companyUpsertMaxAttempts 是一次企业 upsert 的尝试上限：1062 之后要在**新事务**里
// 重读赢家行，若赢家恰好在这一瞬间被删除（管理员并发清理），整轮重来。
const companyUpsertMaxAttempts = 3

// errCompanyRowVanished 是"1062 说行存在、按 hash 回读却已经没有了"的内部竞态信号，
// 只在 UpsertCompany 的重试循环内部出现，绝不返回给调用方。
var errCompanyRowVanished = errors.New("companies: nkey row vanished concurrently")

// Companies returns the company store.
func (d *DB) Companies() *CompanyStore { return &CompanyStore{db: d} }

// companyNKey normalises a company name: lowercase + trim + collapse whitespace.
func companyNKey(name string) string {
	return strings.Join(strings.Fields(strings.ToLower(name)), " ")
}

// companyTimeText 把 DATETIME(3) 显式格式化成 UTC 的 RFC3339Nano 文本（M35）。
//
// 旧写法是 PG 的 `created_at::text`。TiDB 驱动在 parseTime=true 下交回 time.Time，
// 这里统一按 UTC 定点格式化：API 侧仍然拿到带时区的字符串，既不会漂成驱动零值，
// 也不会因为会话时区不同而给出两个"本地时间"。
func companyTimeText(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.UTC().Format(time.RFC3339Nano)
}

// companyOptionalText 把可空文本列转成 *string（保持 logo 的 omitempty 合同）。
func companyOptionalText(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	out := v.String
	return &out
}

// UpsertCompany creates or updates a company by name. Returns the id and whether
// a new row was created.
//
// TiDB 化（M09）：nkey 的自然键换成了 uq_companies_nkey_hash 唯一索引，所以插入必须
// 同时写入 nkey_identity / nkey_hash；"存在即更新"改由 1062 判定，不再依赖
// ON CONFLICT … RETURNING id, (xmax = 0)。created 因此来自"这一次 INSERT 是否真的
// 插入了行"，不是 MySQL ODKU 那个 0/1/2 的模糊行数（M30 同一口径）。
func (s *CompanyStore) UpsertCompany(name, logo string) (id int64, created bool, err error) {
	nkey := companyNKey(name)
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	var logoVal any
	if logo != "" {
		logoVal = logo
	}
	for attempt := 0; attempt < companyUpsertMaxAttempts; attempt++ {
		id, created, err = s.upsertCompanyOnce(name, nkey, identity, hash, logoVal)
		if !errors.Is(err, errCompanyRowVanished) {
			return id, created, err
		}
	}
	return 0, false, fmt.Errorf("companies: upsert %q 在 %d 次尝试后仍在竞态中: %w", nkey, companyUpsertMaxAttempts, err)
}

// upsertCompanyOnce 是单次尝试：严格 INSERT → LastInsertId；1062 → 既有行的身份不可变更新。
func (s *CompanyStore) upsertCompanyOnce(name, nkey string, identity, hash []byte, logoVal any) (int64, bool, error) {
	res, err := s.db.Exec(`
INSERT INTO companies(name, nkey, nkey_identity, nkey_hash, logo)
VALUES (?, ?, ?, ?, ?)`, name, nkey, identity, hash, logoVal)
	if err == nil {
		id, err := res.LastInsertId()
		if err != nil {
			return 0, false, fmt.Errorf("companies: LastInsertId after INSERT: %w", err)
		}
		// 自增 PK 必须是正数：0 说明驱动没有给出自增值（协议异常），绝不能当成新行外泄。
		if id < 1 {
			return 0, false, fmt.Errorf("companies: inserted pk %d out of range", id)
		}
		return id, true, nil
	}
	if !tidb.IsDuplicateKey(err) {
		return 0, false, err
	}
	// 1062 落在 uq_companies_nkey_hash 上：企业已存在，转身份不可变更新。
	return s.mergeExistingCompany(name, logoVal, hash)
}

// mergeExistingCompany 在独立事务里按 hash 唯一定位既有行并加行锁，然后只更新展示列
// （name / logo / updated_at），绝不触碰 nkey / nkey_identity / nkey_hash。
func (s *CompanyStore) mergeExistingCompany(name string, logoVal any, hash []byte) (int64, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	var id int64
	if err := tx.QueryRow(`
SELECT id FROM companies WHERE nkey_hash = ? FOR UPDATE`, hash).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, errCompanyRowVanished
		}
		return 0, false, err
	}
	if _, err := tx.Exec(`
UPDATE companies SET name = ?, logo = COALESCE(?, logo), updated_at = NOW(3) WHERE id = ?`,
		name, logoVal, id); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return id, false, nil
}

// CreateCompanyWithScope creates a company without updating an existing row.
// The company, its valid initial scope rules, and derived asset attribution are
// committed atomically. Invalid inputs retain the legacy partial-validation
// contract and are reported without preventing valid rules from being stored.
func (s *CompanyStore) CreateCompanyWithScope(name, logo string, inputs []ScopeInput, reason string) (
	id int64, added, skipped, invalid int, validationErrors []string, err error,
) {
	if err := ValidateCompanyScopeInputBounds(inputs); err != nil {
		return 0, 0, 0, 0, nil, err
	}
	rules, invalid, validationErrors := parseScopeInputs(inputs)
	if err := validateParsedScopeBounds(rules); err != nil {
		return 0, 0, 0, invalid, validationErrors, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, 0, invalid, validationErrors, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	if err := lockCompanyScopeMutation(tx); err != nil {
		return 0, 0, 0, invalid, validationErrors, err
	}

	nkey := companyNKey(name)
	identity, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	var logoVal any
	if logo != "" {
		logoVal = logo
	}
	// 旧实现是 ON CONFLICT (nkey) DO NOTHING RETURNING id + ErrNoRows 判定重名；
	// TiDB 没有该写法，重名改由 uq_companies_nkey_hash 上的 1062 表达（M09 / M14 同一口径）。
	res, err := tx.Exec(`
INSERT INTO companies(name, nkey, nkey_identity, nkey_hash, logo)
VALUES (?, ?, ?, ?, ?)`, name, nkey, identity, hash, logoVal)
	if err != nil {
		if tidb.IsDuplicateKey(err) {
			return 0, 0, 0, invalid, validationErrors, ErrCompanyNameConflict
		}
		return 0, 0, 0, invalid, validationErrors, err
	}
	if id, err = res.LastInsertId(); err != nil {
		return 0, 0, 0, invalid, validationErrors, fmt.Errorf("companies: LastInsertId after INSERT: %w", err)
	}
	if id < 1 {
		return 0, 0, 0, invalid, validationErrors, fmt.Errorf("companies: inserted pk %d out of range", id)
	}

	added, skipped, needsAttribution, err := insertScopeRulesTx(tx, id, rules, reason)
	if err != nil {
		return 0, 0, 0, invalid, validationErrors, err
	}
	if needsAttribution {
		warning, err := recomputeAttributionTx(tx)
		if err != nil {
			return 0, 0, 0, invalid, validationErrors, err
		}
		logAttributionWarning(warning)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, 0, invalid, validationErrors, err
	}
	return id, added, skipped, invalid, validationErrors, nil
}

// GetCompany returns one company by id (nil if not found).
func (s *CompanyStore) GetCompany(id int64) (*Company, error) {
	c := &Company{}
	var logo sql.NullString
	var createdAt, updatedAt sql.NullTime
	err := s.db.QueryRow(`
SELECT id, name, nkey, logo, created_at, updated_at
FROM companies WHERE id = ?`, id).Scan(&c.ID, &c.Name, &c.NKey, &logo, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.Logo = companyOptionalText(logo)
	c.CreatedAt = companyTimeText(createdAt)
	c.UpdatedAt = companyTimeText(updatedAt)
	return c, nil
}

// GetCompanyByName returns one company by normalized name (nil if not found).
func (s *CompanyStore) GetCompanyByName(name string) (*Company, error) {
	nkey := companyNKey(name)
	_, hash := canonicalIdentityPair(schema.NKeyIdentity(nkey))
	c := &Company{}
	var logo sql.NullString
	var createdAt, updatedAt sql.NullTime
	// 按 hash 唯一索引查表：nkey 是 MEDIUMTEXT，旧库靠表达式索引，新库的唯一键在 hash 上。
	err := s.db.QueryRow(`
SELECT id, name, nkey, logo, created_at, updated_at
FROM companies WHERE nkey_hash = ?`, hash).Scan(&c.ID, &c.Name, &c.NKey, &logo, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.Logo = companyOptionalText(logo)
	c.CreatedAt = companyTimeText(createdAt)
	c.UpdatedAt = companyTimeText(updatedAt)
	return c, nil
}

// UpsertByName creates the company if it doesn't exist, then returns its id.
func (s *CompanyStore) UpsertByName(name string) (int64, error) {
	id, _, err := s.UpsertCompany(name, "")
	return id, err
}

// DeleteCompany deletes a company and re-evaluates automatic ownership against
// the remaining companies in the same transaction. Explicitly-owned assets are
// detached by the deletion cascade and may then fall back to a remaining scope
// match.
func (s *CompanyStore) DeleteCompany(id int64) error {
	_, err := s.DeleteCompanyWithAssets(id, false)
	return err
}

// DeleteCompanyWithAssets deletes a company and optionally all of its assets in
// one transaction, then re-evaluates ownership against the remaining companies.
//
// M19：新库没有 FOREIGN KEY，也就没有 ON DELETE CASCADE / SET NULL。删除路径必须
// 自己把闭包算全 —— 接线既有 company closure + 物理执行器：
//
//	Fresh Locator → ComputeCompanyClosure（顺带确认企业存在）
//	Execution     → ExecuteCompanyCascade：置空 assets.company_id / tasks.company_id，
//	                删除 company_scope / task_scope，删除 companies 根行，
//	                最后 AssertNoOrphansForCompany 断言零悬挂引用
//	Re-attribution → 其余企业的范围规则可以接管刚刚失去归属的资产
func (s *CompanyStore) DeleteCompanyWithAssets(id int64, deleteAssets bool) (assetsDeleted int64, err error) {
	if id <= 0 {
		return 0, ErrCompanyNotFound
	}
	ctx := context.Background()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	if err := lockCompanyScopeMutation(tx); err != nil {
		return 0, err
	}
	closure, err := ComputeCompanyClosure(ctx, tx, uint64(id))
	if err != nil {
		if errors.Is(err, ErrForeignKeyViolation) {
			return 0, ErrCompanyNotFound
		}
		return 0, err
	}
	if deleteAssets {
		res, err := tx.ExecContext(ctx, `DELETE FROM assets WHERE company_id = ?`, id)
		if err != nil {
			return 0, err
		}
		if assetsDeleted, err = res.RowsAffected(); err != nil {
			return 0, err
		}
	}
	audit, err := ExecuteCompanyCascade(ctx, tx, closure)
	if err != nil {
		return 0, err
	}
	if audit.EntitiesDeleted["companies"] == 0 {
		// 闭包之后、删除之前企业被并发清理：按既有契约报"不存在"，绝不返回静默成功。
		return 0, ErrCompanyNotFound
	}
	// This path has no per-request warning channel, so the log is the only place
	// the operator can learn about unparseable ip rows here.
	warning, err := recomputeAttributionTx(tx)
	if err != nil {
		return 0, err
	}
	logAttributionWarning(warning)
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return assetsDeleted, nil
}

// ListCompanies returns all companies with scope and asset count.
func (s *CompanyStore) ListCompanies() ([]*CompanyWithScope, error) {
	// TiDB 8.5 默认开着 ONLY_FULL_GROUP_BY：旧写法 `GROUP BY c.id` 再取 c.name 等
	// 非聚合列依赖函数依赖推断，直接改写成相关子查询，既严格模式安全又省掉一次
	// COUNT(DISTINCT) 的聚合去重。
	rows, err := s.db.Query(`
SELECT c.id, c.name, c.nkey, c.logo, c.created_at, c.updated_at,
       (SELECT COUNT(*) FROM assets a WHERE a.company_id = c.id) AS asset_count
FROM companies c
ORDER BY c.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*CompanyWithScope
	for rows.Next() {
		cws := &CompanyWithScope{}
		var logo sql.NullString
		var createdAt, updatedAt sql.NullTime
		if err := rows.Scan(&cws.ID, &cws.Name, &cws.NKey, &logo,
			&createdAt, &updatedAt, &cws.AssetCount); err != nil {
			return nil, err
		}
		cws.Logo = companyOptionalText(logo)
		cws.CreatedAt = companyTimeText(createdAt)
		cws.UpdatedAt = companyTimeText(updatedAt)
		out = append(out, cws)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// fetch scope rules for each company
	for _, cws := range out {
		cws.Scope, err = s.GetScope(cws.ID)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// GetScope returns all scope rules for a company.
func (s *CompanyStore) GetScope(companyID int64) ([]ScopeRule, error) {
	rows, err := s.db.Query(`
SELECT id, company_id, kind,
       COALESCE(domain,''), COALESCE(net,''), COALESCE(value,''), raw, COALESCE(reason,'')
FROM company_scope
WHERE company_id = ?
ORDER BY id`, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ScopeRule, 0)
	for rows.Next() {
		var r ScopeRule
		if err := rows.Scan(&r.ID, &r.CompanyID, &r.Kind, &r.Domain, &r.Net, &r.Value, &r.Raw, &r.Reason); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AddScope parses and inserts scope lines for a company, then reattributes assets.
// Returns counts of added, skipped, and invalid lines.
func (s *CompanyStore) AddScope(companyID int64, lines []string, reason string) (added, skipped, invalid int, errors []string) {
	inputs := make([]ScopeInput, 0, len(lines))
	for _, line := range lines {
		inputs = append(inputs, ScopeInput{Value: line})
	}
	return s.AddScopeInputs(companyID, inputs, reason)
}

// AddScopeInputs inserts structured scope rules. Empty kinds use the automatic
// CIDR/IP/ICP/domain/keyword classification used by AddScope.
func (s *CompanyStore) AddScopeInputs(companyID int64, inputs []ScopeInput, reason string) (added, skipped, invalid int, errors []string) {
	added, skipped, invalid, validationErrors, err := s.AddScopeInputsChecked(companyID, inputs, reason)
	if err != nil {
		validationErrors = append(validationErrors, err.Error())
	}
	return added, skipped, invalid, validationErrors
}

// AddScopeInputsChecked inserts structured scope rules while keeping input
// validation separate from storage and transaction errors.
func (s *CompanyStore) AddScopeInputsChecked(companyID int64, inputs []ScopeInput, reason string) (
	added, skipped, invalid int, validationErrors []string, err error,
) {
	if err := ValidateCompanyScopeInputBounds(inputs); err != nil {
		return 0, 0, 0, nil, err
	}
	rules, invalid, errors := parseScopeInputs(inputs)
	if err := validateParsedScopeBounds(rules); err != nil {
		return 0, 0, invalid, errors, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, invalid, errors, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	if err := lockCompanyScopeMutation(tx); err != nil {
		return 0, 0, invalid, errors, err
	}
	if err := ensureCompanyExistsTx(tx, companyID); err != nil {
		return 0, 0, invalid, errors, err
	}
	if len(rules) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, 0, invalid, errors, err
		}
		return 0, 0, invalid, errors, nil
	}
	added, skipped, needsAttribution, err := insertScopeRulesTx(tx, companyID, rules, reason)
	if err != nil {
		return 0, 0, invalid, errors, err
	}
	if needsAttribution {
		warning, err := recomputeAttributionTx(tx)
		if err != nil {
			return 0, 0, invalid, errors, fmt.Errorf("重新计算企业归属失败: %w", err)
		}
		logAttributionWarning(warning)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, invalid, errors, err
	}
	return added, skipped, invalid, errors, nil
}

func parseScopeInputs(inputs []ScopeInput) (rules []ParsedScope, invalid int, validationErrors []string) {
	rules = make([]ParsedScope, 0, len(inputs))
	for _, input := range inputs {
		rule, err := ParseScopeInput(input)
		if err != nil {
			invalid++
			validationErrors = append(validationErrors, fmt.Sprintf("%s: %v", input.Value, err))
			continue
		}
		rules = append(rules, rule)
	}
	return rules, invalid, validationErrors
}

func validateParsedScopeBounds(rules []ParsedScope) error {
	for i, rule := range rules {
		if utf8.RuneCountInString(rule.Raw) > MaxCompanyScopeRawRunes {
			return &CompanyScopeValidationError{Message: fmt.Sprintf(
				"企业范围第 %d 条原始值过长: 最多 %d 个字符", i+1, MaxCompanyScopeRawRunes,
			)}
		}
		if utf8.RuneCountInString(rule.Value) > MaxCompanyScopeValueRunes {
			return &CompanyScopeValidationError{Message: fmt.Sprintf(
				"企业范围第 %d 条规范化值过长: 最多 %d 个字符", i+1, MaxCompanyScopeValueRunes,
			)}
		}
	}
	return nil
}

func ensureCompanyExistsTx(tx *sql.Tx, companyID int64) error {
	if companyID <= 0 {
		return ErrCompanyNotFound
	}
	var exists int64
	err := tx.QueryRow(`SELECT id FROM companies WHERE id = ? LIMIT 1`, companyID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrCompanyNotFound
	}
	if err != nil {
		return err
	}
	return nil
}

// lockCompanyScopeMutation 把并发的企业范围变更串行化。
//
// 旧实现是 `SELECT pg_advisory_xact_lock($1)` 的进程级串行化：TiDB 没有 advisory
// lock，等价原语是**按主键升序对企业行加事务级悲观锁**（与 db/tidb.CoordinateMaintenance
// 同一口径：行锁随 commit/rollback 原子释放，不需要显式解锁，也不会像 GET_LOCK 那样
// 把锁泄漏到池化连接上）。企业规模是个位数到百级，锁范围可控；顺序恒为 id 升序，
// 因此并发变更只会排队，不会形成锁序环。归属重算因此始终对着"同一份已提交范围集合"
// 计算，不会两次重算互相踩写 assets.company_id。
func lockCompanyScopeMutation(tx *sql.Tx) error {
	_, err := tx.Exec(`SELECT id FROM companies ORDER BY id FOR UPDATE`)
	return err
}

func insertScopeRulesTx(tx *sql.Tx, companyID int64, rules []ParsedScope, reason string) (
	added, skipped int, needsAttribution bool, err error,
) {
	for _, rule := range rules {
		inserted, insertErr := insertScopeRuleTx(tx, companyID, rule, reason)
		if insertErr != nil {
			return 0, 0, false, insertErr
		}
		if !inserted {
			skipped++
			continue
		}
		added++
		needsAttribution = needsAttribution || rule.Kind != "keyword"
	}
	return added, skipped, needsAttribution, nil
}

// insertScopeRuleTx inserts a scope rule with the single hash branch selected by
// its kind (M09). inserted=false means the rule already exists — the duplicate is
// reported by MySQL 1062 on that branch's hash unique index, which is the TiDB
// equivalent of the old ON CONFLICT … DO NOTHING (and, unlike PG, a 1062 does not
// poison the surrounding transaction, so the remaining rules still get their turn).
// computeScopeInsertArgs 纯函数：根据 companyID、rule 和 reason 计算 INSERT INTO company_scope 的 13 个参数。
// 暴露该函数便于在离线单元测试中逐位精确断言激活分支列与非激活 nil 列（M1 测试闭环）。
func computeScopeInsertArgs(companyID int64, rule ParsedScope, reason string) (branch companyScopeBranch, args []any, err error) {
	branch, err = companyScopeBranchFor(companyID, rule)
	if err != nil {
		return companyScopeBranch{}, nil, err
	}

	var (
		identDomain, identNet, identVal []byte
		hashDomain, hashNet, hashVal   []byte
		valDomain, valNet, valVal       *string
	)

	// 根据 branch.ValueCol 精确判定目标载荷列与对应的 identity/hash 分支列
	switch branch.ValueCol {
	case "domain":
		identDomain = branch.Identity
		hashDomain = branch.Hash
		valDomain = &branch.Value
	case "net":
		identNet = branch.Identity
		hashNet = branch.Hash
		valNet = &branch.Value
	case "value":
		identVal = branch.Identity
		hashVal = branch.Hash
		valVal = &branch.Value
	}

	args = []any{
		companyID, rule.Kind, valDomain, valNet, valVal, rule.Raw, reason,
		identDomain, hashDomain,
		identNet, hashNet,
		identVal, hashVal,
	}
	return branch, args, nil
}

func insertScopeRuleTx(tx *sql.Tx, companyID int64, rule ParsedScope, reason string) (inserted bool, err error) {
	_, args, err := computeScopeInsertArgs(companyID, rule, reason)
	if err != nil {
		return false, err
	}

	res, err := tx.Exec(`
INSERT INTO company_scope(
    company_id, kind, domain, net, value, raw, reason,
    identity_scope_domain, hash_scope_domain,
    identity_scope_net, hash_scope_net,
    identity_scope_value, hash_scope_value
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		args...,
	)
	if err != nil {
		if tidb.IsDuplicateKey(err) {
			return false, nil
		}
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ── 归属重算（M35：消除 DISTINCT ON，改派生表窗口函数）──────────────────────
//
// 旧实现三条归属语句都建立在 PG 专有构造上：
//   - `DISTINCT ON (a.id) … ORDER BY a.id, length(cs.domain) DESC, cs.company_id`
//     配 `UPDATE … FROM matched`（TiDB 两者都没有）；
//   - `cs.net >>= try_inet(a.ip)` 与 `masklen(cs.net)`：新库 company_scope.net 是
//     CIDR 文本，没有 inet 类型与包含算子；
//   - `regexp_replace(..., '[[:space:]]+', '', 'g')`：MySQL 第 4 参数是 position。
//
// 现在按 spec §3.3.1 的口径拆分：域名匹配留在 SQL 里（派生表 + ROW_NUMBER），
// 网段与备案匹配先在 Go 内存里预筛选，再在同一事务内按 PK 原子回填。

// companyDomainAttributionSQL 是域名归属去重：一个资产可能同时被多条同域规则命中
// （不同企业登记了同一个根域），派生表里按"域名长度 DESC、company_id ASC"取第 1 行。
const companyDomainAttributionSQL = `
SELECT asset_id, company_id FROM (
    SELECT a.id AS asset_id, cs.company_id,
           ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY LENGTH(cs.domain) DESC, cs.company_id ASC) AS rn
    FROM assets a
    JOIN company_scope cs ON cs.kind = 'domain' AND a.root_domain = cs.domain
    WHERE a.company_id IS NULL
      AND a.type IN ('root_domain','subdomain','service','endpoint')
      AND a.root_domain IS NOT NULL
) t WHERE t.rn = 1`

// companyAttributionChunk 是一次批量回填的资产上限：IN 列表保持有限长度，
// 单条 DML 的写集可预期。
const companyAttributionChunk = 500

// companyAttribution 是一条 "资产 → 企业" 的归属结论。
type companyAttribution struct {
	assetID   int64
	companyID int64
}

// RecomputeAttribution rebuilds only scope-derived ownership. Explicit company
// links are immutable under scope edits. Precedence is domain, IP/CIDR, then
// normalized exact ICP; keyword rules never attribute assets.
func (s *CompanyStore) RecomputeAttribution() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	if err := lockCompanyScopeMutation(tx); err != nil {
		return err
	}
	warning, err := recomputeAttributionTx(tx)
	if err != nil {
		return err
	}
	logAttributionWarning(warning)
	return tx.Commit()
}

// recomputeAttributionTx rebuilds scope-derived ownership. It returns a warning
// for assets whose ip column cannot be parsed: they are skipped instead of
// aborting the statement, so without this they would silently never receive a
// network-based company. Callers surface the warning and it is always logged.
func recomputeAttributionTx(tx *sql.Tx) (string, error) {
	ctx := context.Background()

	// Only derived rows are cleared. Historical rows migrated without provenance
	// are marked explicit by schema.sql, which is the non-destructive default.
	if _, err := tx.ExecContext(ctx, `
UPDATE assets
SET company_id = NULL, company_source = 'scope'
WHERE company_source = 'scope'`); err != nil {
		return "", err
	}

	// 1) 域名归属：根域精确匹配 + 派生表窗口函数去重（M35）。
	domainPairs, err := queryCompanyAttribution(ctx, tx, companyDomainAttributionSQL)
	if err != nil {
		return "", err
	}
	if err := applyCompanyAttribution(ctx, tx, domainPairs); err != nil {
		return "", err
	}

	// 2) IP/CIDR 归属：TiDB 无 inet 算子，按 spec §3.3.1 在 Go 内存里做最长前缀匹配。
	netPairs, err := companyNetAttribution(ctx, tx)
	if err != nil {
		return "", err
	}
	if err := applyCompanyAttribution(ctx, tx, netPairs); err != nil {
		return "", err
	}

	// 3) Exact normalized ICP attribution after domain/network precedence.
	icpPairs, err := companyICPAttribution(ctx, tx)
	if err != nil {
		return "", err
	}
	if err := applyCompanyAttribution(ctx, tx, icpPairs); err != nil {
		return "", err
	}
	return malformedIPAssetWarning(tx)
}

// queryCompanyAttribution 执行一条返回 (asset_id, company_id) 的去重查询。
func queryCompanyAttribution(ctx context.Context, tx *sql.Tx, query string) ([]companyAttribution, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]companyAttribution, 0, 64)
	for rows.Next() {
		var pair companyAttribution
		if err := rows.Scan(&pair.assetID, &pair.companyID); err != nil {
			return nil, err
		}
		out = append(out, pair)
	}
	return out, rows.Err()
}

// companyNetAttribution 收集网段归属：先把全部 ip/cidr 规则预解析成前缀，再按 id
// 升序扫描仍未归属的资产，在内存里做最长前缀裁决。
func companyNetAttribution(ctx context.Context, tx *sql.Tx) ([]companyAttribution, error) {
	scopeRows, err := tx.QueryContext(ctx, `
SELECT company_id, net FROM company_scope
WHERE kind IN ('ip','cidr') AND net IS NOT NULL AND net <> ''`)
	if err != nil {
		return nil, err
	}
	var scopes []companyNetScope
	for scopeRows.Next() {
		var companyID int64
		var raw string
		if err := scopeRows.Scan(&companyID, &raw); err != nil {
			scopeRows.Close()
			return nil, err
		}
		if scope, ok := newCompanyNetScope(companyID, raw); ok {
			scopes = append(scopes, scope)
		}
	}
	if err := scopeRows.Err(); err != nil {
		scopeRows.Close()
		return nil, err
	}
	if err := scopeRows.Close(); err != nil {
		return nil, err
	}
	if len(scopes) == 0 {
		return nil, nil
	}

	assetRows, err := tx.QueryContext(ctx, `
SELECT id, ip FROM assets
WHERE company_id IS NULL
  AND type IN ('ip','subdomain','service','endpoint')
  AND ip IS NOT NULL AND ip <> ''
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer assetRows.Close()
	out := make([]companyAttribution, 0, 64)
	for assetRows.Next() {
		var assetID int64
		var raw string
		if err := assetRows.Scan(&assetID, &raw); err != nil {
			return nil, err
		}
		ip := net.ParseIP(strings.TrimSpace(raw))
		if ip == nil {
			// 非法 ip 文本对网段规则永远不可见，由 malformedIPAssetWarning 单独上报。
			continue
		}
		if companyID, ok := matchCompanyNetScope(scopes, ip); ok {
			out = append(out, companyAttribution{assetID: assetID, companyID: companyID})
		}
	}
	return out, assetRows.Err()
}

// companyICPAttribution 收集备案归属：规则与资产两侧都用 Go 的 NormalizeICP 归一化
// （写入端存的就是这个规范形），彻底不再依赖 PG 的 regexp_replace。
func companyICPAttribution(ctx context.Context, tx *sql.Tx) ([]companyAttribution, error) {
	scopeRows, err := tx.QueryContext(ctx, `
SELECT company_id, value FROM company_scope
WHERE kind = 'icp' AND value IS NOT NULL AND value <> ''`)
	if err != nil {
		return nil, err
	}
	var rules []companyICPScope
	for scopeRows.Next() {
		var entry companyICPScope
		if err := scopeRows.Scan(&entry.companyID, &entry.value); err != nil {
			scopeRows.Close()
			return nil, err
		}
		rules = append(rules, entry)
	}
	if err := scopeRows.Err(); err != nil {
		scopeRows.Close()
		return nil, err
	}
	if err := scopeRows.Close(); err != nil {
		return nil, err
	}
	indexed := indexCompanyICPScopes(rules)
	if len(indexed) == 0 {
		return nil, nil
	}

	assetRows, err := tx.QueryContext(ctx, `
SELECT id, COALESCE(icp,''), COALESCE(app_icp,'') FROM assets
WHERE company_id IS NULL
  AND (COALESCE(icp,'') <> '' OR COALESCE(app_icp,'') <> '')
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer assetRows.Close()
	out := make([]companyAttribution, 0, 64)
	for assetRows.Next() {
		var assetID int64
		var icp, appICP string
		if err := assetRows.Scan(&assetID, &icp, &appICP); err != nil {
			return nil, err
		}
		if companyID, ok := matchCompanyICPScope(indexed, icp, appICP); ok {
			out = append(out, companyAttribution{assetID: assetID, companyID: companyID})
		}
	}
	return out, assetRows.Err()
}

// applyCompanyAttribution 把裁决结果原子回填进 assets。
//
// 写集只包含"仍无归属"的行（WHERE company_id IS NULL），并且按 company_id 升序、
// 组内按 asset_id 升序执行 —— 与其余企业变更持锁后重算时的加锁顺序一致，
// 不会与并发的第二次重算形成锁序环。
func applyCompanyAttribution(ctx context.Context, tx *sql.Tx, pairs []companyAttribution) error {
	if len(pairs) == 0 {
		return nil
	}
	byCompany := make(map[int64][]int64, 8)
	for _, pair := range pairs {
		byCompany[pair.companyID] = append(byCompany[pair.companyID], pair.assetID)
	}
	companyIDs := make([]int, 0, len(byCompany))
	for companyID := range byCompany {
		companyIDs = append(companyIDs, int(companyID))
	}
	sort.Ints(companyIDs)
	for _, companyID := range companyIDs {
		assetIDs := byCompany[int64(companyID)]
		sort.Slice(assetIDs, func(i, j int) bool { return assetIDs[i] < assetIDs[j] })
		for start := 0; start < len(assetIDs); start += companyAttributionChunk {
			end := start + companyAttributionChunk
			if end > len(assetIDs) {
				end = len(assetIDs)
			}
			chunk := assetIDs[start:end]
			args := make([]any, 0, len(chunk)+1)
			args = append(args, int64(companyID))
			for _, id := range chunk {
				args = append(args, id)
			}
			stmt := "UPDATE assets SET company_id = ?, company_source = 'scope' WHERE company_id IS NULL AND id IN (" +
				strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",") + ")"
			if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
				return err
			}
		}
	}
	return nil
}

// malformedIPAssetsSampled bounds how many offending ids one warning names, so a
// large batch of bad rows stays readable in a toast and in the log.
const malformedIPAssetsSampled = 5

// logAttributionWarning records a recompute warning in the server log. Every
// recompute path calls it, so the warning is reported even for triggers with no
// per-request response (company deletion, scopesentry sync, agent asset writes).
func logAttributionWarning(warning string) {
	if warning != "" {
		log.Printf("[assets] %s", warning)
	}
}

// malformedIPAssetQueryer is satisfied by both *sql.Tx and *DB so the warning
// can be produced inside a recompute transaction or read standalone by the API.
type malformedIPAssetQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// MalformedIPAssetWarning reports assets with an unparseable ip outside of any
// mutation, letting the API attach the warning to a scope response without
// widening the mutation signatures — an unrelated data problem is not one of
// this request's validation errors.
func (s *CompanyStore) MalformedIPAssetWarning() (string, error) {
	return malformedIPAssetWarning(s.db)
}

// malformedIPAssetWarning describes assets whose ip column is not a valid
// address. They are invisible to network attribution, so the operator has to be
// told which rows to fix — silently skipping them would look like scope rules
// that simply do not work.
//
// 旧实现靠 PG 的 `try_inet(ip) IS NULL` 在 SQL 端筛出坏行；TiDB 没有 try_cast 之外的
// 宽松 inet 转换，判定改在 Go 里用 net.ParseIP —— 与归属匹配用的是同一个解析器，
// 因此"告警说看不见"与"匹配真的看不见"必然一致。
func malformedIPAssetWarning(q malformedIPAssetQueryer) (string, error) {
	rows, err := q.Query(`
SELECT id, ip
FROM assets
WHERE ip IS NOT NULL AND ip <> ''
  AND type IN ('ip','subdomain','service','endpoint')
ORDER BY id`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var total int
	samples := make([]string, 0, malformedIPAssetsSampled)
	for rows.Next() {
		var id int64
		var ip string
		if err := rows.Scan(&id, &ip); err != nil {
			return "", err
		}
		if net.ParseIP(strings.TrimSpace(ip)) != nil {
			continue
		}
		total++
		if len(samples) < malformedIPAssetsSampled {
			samples = append(samples, fmt.Sprintf("#%d %s", id, ip))
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if total == 0 {
		return "", nil
	}
	warning := fmt.Sprintf(
		"%d 条资产的 ip 字段不是合法 IP，已跳过 IP/CIDR 范围匹配（这些资产不会被网段规则归属到企业）：%s",
		total, strings.Join(samples, "、"),
	)
	if total > len(samples) {
		warning += fmt.Sprintf(" 等 %d 条", total)
	}
	return warning, nil
}

// UpdateScope replaces all scope rules for a company and reattributes.
func (s *CompanyStore) UpdateScope(companyID int64, lines []string, reason string) (added, invalid int, errs []string) {
	inputs := make([]ScopeInput, 0, len(lines))
	for _, line := range lines {
		inputs = append(inputs, ScopeInput{Value: line})
	}
	return s.UpdateScopeInputs(companyID, inputs, reason)
}

// UpdateScopeInputs replaces all rules with a structured set.
func (s *CompanyStore) UpdateScopeInputs(companyID int64, inputs []ScopeInput, reason string) (added, invalid int, errs []string) {
	added, invalid, validationErrors, err := s.UpdateScopeInputsChecked(companyID, inputs, reason)
	if err != nil {
		validationErrors = append(validationErrors, err.Error())
	}
	return added, invalid, validationErrors
}

// UpdateScopeInputsChecked replaces all rules while separating validation
// feedback from storage and transaction failures.
func (s *CompanyStore) UpdateScopeInputsChecked(companyID int64, inputs []ScopeInput, reason string) (
	added, invalid int, validationErrors []string, err error,
) {
	if err := ValidateCompanyScopeInputBounds(inputs); err != nil {
		return 0, 0, nil, err
	}
	rules, invalid, errs := parseScopeInputs(inputs)
	if invalid > 0 {
		return 0, invalid, errs, &CompanyScopeValidationError{Message: fmt.Sprintf(
			"企业范围包含 %d 条无效规则，未覆盖原有范围", invalid,
		)}
	}
	if err := validateParsedScopeBounds(rules); err != nil {
		return 0, invalid, errs, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, invalid, errs, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后 Rollback 只返回 ErrTxDone
	if err := lockCompanyScopeMutation(tx); err != nil {
		return 0, invalid, errs, err
	}
	if err := ensureCompanyExistsTx(tx, companyID); err != nil {
		return 0, invalid, errs, err
	}
	if _, err := tx.Exec(`DELETE FROM company_scope WHERE company_id = ?`, companyID); err != nil {
		return 0, invalid, errs, err
	}
	added, _, _, err = insertScopeRulesTx(tx, companyID, rules, reason)
	if err != nil {
		return 0, invalid, errs, err
	}
	// Rebuild even for an empty replacement because removing the old rules may
	// detach scope-derived assets or expose a lower-precedence company match.
	warning, err := recomputeAttributionTx(tx)
	if err != nil {
		return 0, invalid, errs, fmt.Errorf("重新计算企业归属失败: %w", err)
	}
	logAttributionWarning(warning)
	if err := tx.Commit(); err != nil {
		return 0, invalid, errs, err
	}
	return added, invalid, errs, nil
}

// ResolveCompany returns the company_id for a given root_domain and/or ip, or nil
// if no scope rule matches. Mirrors the attribution logic used at asset insert time.
func (s *CompanyStore) ResolveCompany(rootDomain, ipStr string) (*int64, error) {
	return s.ResolveCompanyWithICP(rootDomain, ipStr, "")
}

// ResolveCompanyWithICP mirrors RecomputeAttribution for insert-time ownership.
// ICP is consulted only after domain and IP/CIDR fail to match.
func (s *CompanyStore) ResolveCompanyWithICP(rootDomain, ipStr, icp string) (*int64, error) {
	return resolveCompanyWithICP(s.db, rootDomain, ipStr, icp)
}

// resolveCompanyWithICP 复用资产侧那份 TiDB 归属解析（assets.go 的
// resolveAssetCompany）：插入期解析与全局重算必须是同一套语义，
// 两处各写一遍就等于给"资产已存在归属、重算却给出另一个答案"留口子。
func resolveCompanyWithICP(q assetScopeQueryer, rootDomain, ipStr, icp string) (*int64, error) {
	return resolveAssetCompany(q, rootDomain, ipStr, icp)
}
