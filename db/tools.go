package db

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// =====================================================================
// 工具目录层（P08-03 Task 3.3：原生 TiDB 8.5 方言，M12）
//
// 新库里的 tools 是「隐藏 row_id 物理主键 + key_hash 唯一键」的 hash DDL 表
// （schema.BaselineTables §31、schema.HashIdentityColumns）：
//
//   - 自然键 `key` 是 MEDIUMTEXT（TEXT 族不能直接建唯一索引），唯一性完全由
//     uq_tools_key_hash 承载，因此**每一条 INSERT 都必须同时写 key_identity
//     （canonical 载荷）与 key_hash（其 SHA-256）**：漏写这两列等于把唯一键
//     退化成 NULL，重复行再也挡不住；
//   - 身份载荷沿用 schema.SettingsKeyIdentity —— 该 builder 的契约注释明确写着
//     「SettingsKeyIdentity 是 settings/tools/scheduler_state 的 canonical
//     identity（分支 key）」。schema 包不存在独立的 ToolKeyIdentity，另起分支
//     会让同一张表的 identity 出现两套编码，唯一键与 VerifyHashArbitration
//     仲裁双双失配；
//   - 物理主键 row_id 只在包内定位行，绝不进入 Tool 或任何对外 API
//     （hash_arbitration 契约第 4 条）。因此本文件没有 LastInsertId 取号路径，
//     写入结果一律由 RowsAffected 与 1062 冲突表达。
//
// 四条硬性约定：
//  1. 占位符一律 `?`，并按 SQL 文本出现顺序逐个绑定；
//  2. `key` / `schema` / `exec` / `system` / `enabled` 都是 MySQL/TiDB 关键字，
//     列名一律反引号（toolKeyCol / toolCols 是唯一来源）；
//  3. 没有 `INSERT … RETURNING`，也没有 `ON CONFLICT`：首插入用 1062 判重，
//     强制覆盖用 TiDB 原生 `ON DUPLICATE KEY UPDATE`；
//  4. `agents` 是存 JSON 文本的 MEDIUMTEXT，PG 的 jsonb `||`（追加）、`-`（删元素）、
//     `?`（成员判定）三件套在 TiDB 没有等价写法，改由 Go 端「锁行读 → 解析 →
//     改写」；非法 JSON 一律 fail closed，绝不静默把用户的绑定重写成空数组。
// =====================================================================

// Tool is one row of the built-in tool catalog. key + handler live in code; this
// row carries only the page-editable surface: description, parameter schema
// (structure read-only, per-param description/default editable), agent binding,
// and the on/off switch. See schema baseline §tools and agent/toolcatalog.go.
type Tool struct {
	Key         string          `json:"key"`
	System      bool            `json:"system"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Agents      []string        `json:"agents"`
	Enabled     bool            `json:"enabled"`
	Kind        string          `json:"kind"`     // builtin | command | script | http
	Exec        json.RawMessage `json:"exec"`     // 自定义工具执行规格(kind!=builtin)
	Deferred    bool            `json:"deferred"` // schema 延迟(走 SearchExtraTools/ExecuteExtraTool)
	Calls       int             `json:"calls"`    // runtime ledger aggregate; not stored in tools
}

// toolKeyCol / toolCols 里的反引号不是装饰：`key`、`schema`、`exec`、`system`
// 都是 MySQL/TiDB 关键字，裸列名会让解析器把 key = ? 读成索引属性、把 schema 读成
// 数据库名（Error 1064）。所有读写 tools 的语句都必须从这里取列清单。
const (
	toolKeyCol = "`key`"
	toolCols   = "`key`, `system`, description, `schema`, agents, `enabled`, `kind`, `exec`, `deferred`"
)

// toolKeyIdentity 是 tools 表的 canonical 身份对：载荷进 key_identity，
// SHA-256 进 key_hash（与 settings / scheduler_state 同分支 "key"）。
func toolKeyIdentity(key string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.SettingsKeyIdentity(key))
}

// toolAgents 把 agents 列的 JSON 文本解成有序元素。非数组或非法 JSON 直接报错：
// 这是用户手配的绑定数据，按"空数组"重写等于把绑定抹掉，必须 fail closed。
func toolAgents(raw []byte, toolKey string) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return []json.RawMessage{}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(trimmed, &items); err != nil {
		return nil, fmt.Errorf("tools %s: agents 列不是 JSON 数组，拒绝改写: %w", toolKey, err)
	}
	return items, nil
}

// toolAgentsText 把元素写回 JSON 数组文本；空集合恒为 `[]`，与列默认值一致。
func toolAgentsText(items []json.RawMessage) (string, error) {
	if len(items) == 0 {
		return "[]", nil
	}
	raw, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// toolAgentLiteral 是要匹配/追加的 agent key 的紧凑 JSON 字面量（"worker"）。
func toolAgentLiteral(agentKey string) ([]byte, bool) {
	raw, err := json.Marshal(agentKey)
	if err != nil {
		return nil, false
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

// toolAgentStrValue 从 json.RawMessage 元素解码为真实 string 值（按 JSON 字符串规范解码，
// 完美支持 Unicode 转义如 "\u0077orker" 等，杜绝文本比较假阴性，闭环 I3）。
// 严格区分 JSON null 与空字符串：若为 null 则直接返回 ("", false)（闭环 Minor N1）。
func toolAgentStrValue(item json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(item)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false
	}
	// 仅当首字符和末字符为双引号时才为合法的 JSON 字符串表示
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err == nil {
		return s, true
	}
	return "", false
}

// toolAgentsContains 按 JSON 解码后的字符串值判定成员资格。
func toolAgentsContains(items []json.RawMessage, agentKey string) bool {
	for _, item := range items {
		if val, ok := toolAgentStrValue(item); ok && val == agentKey {
			return true
		}
	}
	return false
}

// toolAgentsWithout 剔除数组里的 agentKey；第二个返回值表示是否真的删掉了元素。
func toolAgentsWithout(items []json.RawMessage, agentKey string) ([]json.RawMessage, bool) {
	out := make([]json.RawMessage, 0, len(items))
	removed := false
	for _, item := range items {
		if val, ok := toolAgentStrValue(item); ok && val == agentKey {
			removed = true
			continue
		}
		out = append(out, item)
	}
	return out, removed
}

// toolLikePattern 把 agent key 编成"可能含该 key 的 JSON 文本"粗筛模式。
// 它只负责把明显无关的行挡在行锁之外，判定一律以 Go 端 JSON 解析为准，误匹配无害；
// LIKE 元字符仍须转义，否则 key 里的 % _ 会退化成通配符。
func toolLikePattern(agentKey string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(agentKey)
	return `%"` + escaped + `"%`
}

// SeedTool inserts a built-in tool's code-defined defaults ONCE. 首插入语义落在
// uq_tools_key_hash 的 1062 上（TiDB 没有 ON CONFLICT … DO NOTHING）：撞键就是
// "已有行（可能已被页面改过）"，原样保留并返回 nil —— 这正是每次重启不覆盖页面
// 编辑的关键。需要显式"恢复代码默认"时走 UpsertToolForce。
func (d *DB) SeedTool(key, desc string, schemaDoc, agents json.RawMessage) error {
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	if len(agents) == 0 {
		agents = json.RawMessage("[]")
	}
	identity, hash := toolKeyIdentity(key)
	_, err := d.Exec(`
INSERT INTO tools(`+toolKeyCol+`, key_identity, key_hash, `+"`system`"+`, description, `+"`schema`"+`, agents, `+"`enabled`"+`)
VALUES (?, ?, ?, TRUE, ?, ?, ?, TRUE)`,
		key, identity, hash, desc, schemaDoc, agents)
	if err != nil {
		if tidb.IsDuplicateKey(err) {
			return nil
		}
		return err
	}
	return nil
}

// AddAgentToToolBinding adds an agent key to the given tools' `agents` arrays if
// not already present (idempotent). Used to give the built-in Auto agent its
// default toolset on existing DBs. 未 seed 的 key 直接跳过（只给已存在的行补绑）。
func (d *DB) AddAgentToToolBinding(agentKey string, keys []string) error {
	for _, k := range keys {
		if err := d.addToolAgentBinding(k, agentKey); err != nil {
			return err
		}
	}
	return nil
}

// addToolAgentBinding 在行锁保护下往单个工具的 agents 数组追加一个元素。
// 读-改-写必须同事务加锁：PG 的 `agents || to_jsonb(x)` 是单语句原子更新，换成
// Go 端组装之后，少了 FOR UPDATE 就会让两个并发补绑互相覆盖（后提交者赢）。
func (d *DB) addToolAgentBinding(toolKey, agentKey string) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	var rowID int64
	var raw []byte
	err = tx.QueryRow("SELECT row_id, agents FROM tools WHERE "+toolKeyCol+" = ? FOR UPDATE", toolKey).Scan(&rowID, &raw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	items, err := toolAgents(raw, toolKey)
	if err != nil {
		return err
	}
	if toolAgentsContains(items, agentKey) {
		return nil
	}
	literal, ok := toolAgentLiteral(agentKey)
	if !ok {
		return fmt.Errorf("tools %s: agent key %q 无法编码为 JSON 字面量", toolKey, agentKey)
	}
	text, err := toolAgentsText(append(items, json.RawMessage(literal)))
	if err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE tools SET agents = ?, updated_at = NOW(3) WHERE row_id = ?", text, rowID); err != nil {
		return err
	}
	return tx.Commit()
}

// RemoveAgentFromToolBindings strips an agent key from every tool's `agents`
// JSON array — called when a custom agent is deleted so no tool keeps a dangling
// binding. 候选行由 LIKE 粗筛后在同一条 `SELECT … FOR UPDATE` 里锁住，剔除与写回
// 全落在这一个事务里，语义与旧的单条全表 UPDATE 一样原子。
func (d *DB) RemoveAgentFromToolBindings(agentKey string) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.Query(`SELECT row_id, ` + toolKeyCol + `, agents FROM tools
ORDER BY row_id
FOR UPDATE`)
	if err != nil {
		return err
	}
	type toolRow struct {
		rowID int64
		key   string
		raw   []byte
	}
	var candidates []toolRow
	for rows.Next() {
		var c toolRow
		if err := rows.Scan(&c.rowID, &c.key, &c.raw); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, c := range candidates {
		items, err := toolAgents(c.raw, c.key)
		if err != nil {
			return err
		}
		next, removed := toolAgentsWithout(items, agentKey)
		if !removed {
			continue
		}
		text, err := toolAgentsText(next)
		if err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE tools SET agents = ?, updated_at = NOW(3) WHERE row_id = ?", text, c.rowID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RemoveAgentFromTool strips one agent key from a SINGLE tool's `agents` array —
// used by one-time migrations that change a tool's default binding on existing DBs
// (SeedTool is first-insert-only, so a changed default never reaches a seeded row).
func (d *DB) RemoveAgentFromTool(agentKey, toolKey string) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	var rowID int64
	var raw []byte
	err = tx.QueryRow("SELECT row_id, agents FROM tools WHERE "+toolKeyCol+" = ? FOR UPDATE", toolKey).Scan(&rowID, &raw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	items, err := toolAgents(raw, toolKey)
	if err != nil {
		return err
	}
	next, removed := toolAgentsWithout(items, agentKey)
	if !removed {
		return nil
	}
	text, err := toolAgentsText(next)
	if err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE tools SET agents = ?, updated_at = NOW(3) WHERE row_id = ?", text, rowID); err != nil {
		return err
	}
	return tx.Commit()
}

// UpsertToolForce overwrites a tool row with the given code-default values (used by
// the per-tool "reset" action). It resets description/schema/agents and re-enables
// the tool, but keeps system=true.
//
// 冲突判定落在 uq_tools_key_hash 上（TiDB 原生 ON DUPLICATE KEY UPDATE）；
// key_identity / key_hash 是不可变身份列，只在插入分支写入，更新分支绝不改写它们。
func (d *DB) UpsertToolForce(key, desc string, schemaDoc, agents json.RawMessage) error {
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	if len(agents) == 0 {
		agents = json.RawMessage("[]")
	}
	identity, hash := toolKeyIdentity(key)
	_, err := d.Exec(`
INSERT INTO tools(`+toolKeyCol+`, key_identity, key_hash, `+"`system`"+`, description, `+"`schema`"+`, agents, `+"`enabled`"+`)
VALUES (?, ?, ?, TRUE, ?, ?, ?, TRUE)
ON DUPLICATE KEY UPDATE
  description   = VALUES(description),
  `+"`schema`"+`      = VALUES(`+"`schema`"+`),
  agents        = VALUES(agents),
  `+"`enabled`"+`     = TRUE,
  updated_at    = NOW(3)`,
		key, identity, hash, desc, schemaDoc, agents)
	return err
}

// RefreshToolDefaults updates a system tool's model-facing description + schema to the
// code defaults, PRESERVING the user's agent binding + enabled flag. Used by one-time
// migrations to propagate a code schema change (SeedTool is first-insert-only, so a new
// parameter added in code otherwise never reaches an already-seeded row). No-op for
// custom tools or unknown keys.
func (d *DB) RefreshToolDefaults(key, desc string, schemaDoc json.RawMessage) error {
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	_, err := d.Exec(`UPDATE tools SET description = ?, `+"`schema`"+` = ?, updated_at = NOW(3)
WHERE `+toolKeyCol+` = ? AND `+"`system`"+` = TRUE`, desc, schemaDoc, key)
	return err
}

func scanTool(rows interface{ Scan(...any) error }) (*Tool, error) {
	var t Tool
	var agents []byte
	if err := rows.Scan(&t.Key, &t.System, &t.Description, &t.Schema, &agents, &t.Enabled, &t.Kind, &t.Exec, &t.Deferred); err != nil {
		return nil, err
	}
	if len(agents) > 0 {
		_ = json.Unmarshal(agents, &t.Agents)
	}
	if t.Agents == nil {
		t.Agents = []string{}
	}
	return &t, nil
}

// ListTools returns the whole tool catalog, ordered by key.
func (d *DB) ListTools() ([]*Tool, error) {
	rows, err := d.Query("SELECT " + toolCols + " FROM tools ORDER BY " + toolKeyCol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Tool
	for rows.Next() {
		t, err := scanTool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetTool fetches one tool row (nil if absent).
func (d *DB) GetTool(key string) (*Tool, error) {
	row := d.QueryRow("SELECT "+toolCols+" FROM tools WHERE "+toolKeyCol+" = ?", key)
	t, err := scanTool(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return t, err
}

// CreateCustomTool inserts a user-defined tool (system=false) with an execution
// spec. Fails if the key already exists —— 撞键由 uq_tools_key_hash 的 1062 表达并
// 原样回给调用方（页面据此提示"标识已存在"），绝不静默覆盖既有行。
func (d *DB) CreateCustomTool(t *Tool) error {
	schemaDoc := t.Schema
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	exec := t.Exec
	if len(exec) == 0 {
		exec = json.RawMessage("{}")
	}
	agents, _ := json.Marshal(t.Agents)
	if len(agents) == 0 {
		agents = json.RawMessage("[]")
	}
	identity, hash := toolKeyIdentity(t.Key)
	_, err := d.Exec(`
INSERT INTO tools(`+toolKeyCol+`, key_identity, key_hash, `+"`system`"+`, description, `+"`schema`"+`, agents, `+"`enabled`"+`, `+"`kind`"+`, `+"`exec`"+`, deferred)
VALUES (?, ?, ?, FALSE, ?, ?, ?, ?, ?, ?, ?)`,
		t.Key, identity, hash, t.Description, schemaDoc, agents, t.Enabled, t.Kind, exec, t.Deferred)
	return err
}

// UpdateCustomTool updates a custom tool's editable fields (kind/exec/deferred +
// desc/schema/agents/enabled). Only touches system=false rows.
func (d *DB) UpdateCustomTool(t *Tool) error {
	schemaDoc := t.Schema
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	exec := t.Exec
	if len(exec) == 0 {
		exec = json.RawMessage("{}")
	}
	agents, _ := json.Marshal(t.Agents)
	if len(agents) == 0 {
		agents = json.RawMessage("[]")
	}
	_, err := d.Exec(`UPDATE tools SET description = ?, `+"`schema`"+` = ?, agents = ?, `+"`enabled`"+` = ?, `+"`kind`"+` = ?, `+"`exec`"+` = ?, deferred = ?, updated_at = NOW(3)
WHERE `+toolKeyCol+` = ? AND `+"`system`"+` = FALSE`,
		t.Description, schemaDoc, agents, t.Enabled, t.Kind, exec, t.Deferred, t.Key)
	return err
}

// DeleteCustomTool removes a custom tool (system=false only; built-ins protected).
func (d *DB) DeleteCustomTool(key string) error {
	_, err := d.Exec("DELETE FROM tools WHERE "+toolKeyCol+" = ? AND `system` = FALSE", key)
	return err
}

// ListCustomTools returns only the user-defined (system=false) tools.
func (d *DB) ListCustomTools() ([]*Tool, error) {
	rows, err := d.Query("SELECT " + toolCols + " FROM tools WHERE `system` = FALSE ORDER BY " + toolKeyCol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Tool
	for rows.Next() {
		t, err := scanTool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTool saves the page-editable fields. key is never changed (it is welded to
// the Go handler). system tools: the caller must keep the schema structure — only
// per-param description/default and the agent binding are meant to move.
func (d *DB) UpdateTool(key, desc string, schemaDoc, agents json.RawMessage, enabled bool) error {
	if len(schemaDoc) == 0 {
		schemaDoc = json.RawMessage("{}")
	}
	if len(agents) == 0 {
		agents = json.RawMessage("[]")
	}
	_, err := d.Exec(`UPDATE tools SET description = ?, `+"`schema`"+` = ?, agents = ?, `+"`enabled`"+` = ?, updated_at = NOW(3)
WHERE `+toolKeyCol+` = ?`, desc, schemaDoc, agents, enabled, key)
	return err
}
