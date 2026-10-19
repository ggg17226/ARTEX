package db

import "time"

// AssetInterceptRule is one row of asset_intercept_rules — a global asset
// blocklist entry. Unlike intercept_rules (which matches tool name / input),
// these match the *target asset*: an exact/fuzzy domain·ip·url, or a CIDR range.
// This layer only stores rules; the matching/enforcement logic lives elsewhere.
type AssetInterceptRule struct {
	ID      int64  `json:"id,string"` // P03 R01：64 位实体 ID 对外 string
	Enabled bool   `json:"enabled"`
	Kind    string `json:"kind"` // exact_domain|exact_ip|exact_url|fuzzy_domain|fuzzy_ip|fuzzy_url|cidr
	Pattern string `json:"pattern"`
	Note    string `json:"note"`
	Builtin bool   `json:"builtin"`
	// Action 仅用于任务级规则：'block'=拦截 'allow'=允许(白名单)。
	// 全局规则(asset_intercept_rules)不带此列，恒为空，视为拦截。
	Action    string    `json:"action,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

const assetInterceptRuleCols = `id, enabled, kind, pattern, note, builtin, created_at, updated_at`

func scanAssetInterceptRule(row interface{ Scan(...any) error }) (AssetInterceptRule, error) {
	var r AssetInterceptRule
	err := row.Scan(&r.ID, &r.Enabled, &r.Kind, &r.Pattern, &r.Note, &r.Builtin, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

// ListAssetInterceptRules returns all rules, built-ins first then newest first.
func (d *DB) ListAssetInterceptRules() ([]AssetInterceptRule, error) {
	rows, err := d.Query(`SELECT ` + assetInterceptRuleCols + ` FROM asset_intercept_rules ORDER BY builtin DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AssetInterceptRule
	for rows.Next() {
		r, err := scanAssetInterceptRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CreateAssetInterceptRule inserts a new user rule (builtin is always false here).
//
// 原生 TiDB 没有 INSERT … RETURNING：自增 id 走 LastInsertId，再按 id 回读整行，
// 让 created_at/updated_at 这类由 DDL 默认值生成的列与库内保持一字不差。
func (d *DB) CreateAssetInterceptRule(kind, pattern, note string, enabled bool) (AssetInterceptRule, error) {
	res, err := d.Exec(`
INSERT INTO asset_intercept_rules(enabled, kind, pattern, note, builtin)
VALUES (?, ?, ?, ?, FALSE)`, enabled, kind, pattern, note)
	if err != nil {
		return AssetInterceptRule{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return AssetInterceptRule{}, err
	}
	return d.getAssetInterceptRule(id)
}

// UpdateAssetInterceptRule replaces the editable fields of an existing rule.
func (d *DB) UpdateAssetInterceptRule(id int64, kind, pattern, note string, enabled bool) (AssetInterceptRule, error) {
	if _, err := d.Exec(`
UPDATE asset_intercept_rules
   SET enabled=?, kind=?, pattern=?, note=?
 WHERE id=?`, enabled, kind, pattern, note, id); err != nil {
		return AssetInterceptRule{}, err
	}
	return d.getAssetInterceptRule(id)
}

// getAssetInterceptRule 按主键回读一条规则；行不存在时返回 sql.ErrNoRows，
// 与旧的 INSERT/UPDATE … RETURNING 行为一致。
func (d *DB) getAssetInterceptRule(id int64) (AssetInterceptRule, error) {
	return scanAssetInterceptRule(d.QueryRow(
		`SELECT `+assetInterceptRuleCols+` FROM asset_intercept_rules WHERE id=?`, id))
}

// DeleteAssetInterceptRule removes a rule (built-in rules are deletable too).
func (d *DB) DeleteAssetInterceptRule(id int64) error {
	_, err := d.Exec(`DELETE FROM asset_intercept_rules WHERE id=?`, id)
	return err
}

// ToggleAssetInterceptRule flips the enabled state of a rule.
func (d *DB) ToggleAssetInterceptRule(id int64, enabled bool) error {
	_, err := d.Exec(`UPDATE asset_intercept_rules SET enabled=? WHERE id=?`, enabled, id)
	return err
}
