// Package db is the TiDB data source for ARTEX (原生 TiDB 8.5，取代旧 graph 单文件
// SQLite 与过渡期的 PostgreSQL/pgx)。它打开共享连接池、执行启动期纯只读元数据基线
// 校验（schema.VerifyBaseline），并 seed 内置 agent 与变量目录。
//
// 启动生命周期（spec §3.1.2）严格只读优先：连接池与能力探测由已验收的
// db/tidb.OpenTiDB 负责（32 连接 + 强制 UTC 会话时区），本文件不再执行任何 DDL、
// 不再嵌入 schema.sql、不再使用 PG advisory lock。建表只有一条路径：
// `artex db-migrate --prepare`。
package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Autumn-27/artex/config"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// baselineVerifyTimeout 给启动期的三条 information_schema 只读查询留足余载：
// 元数据快照在 TiDB 上可能因 DDL 等待排队，但不能无限拖住启动。
const baselineVerifyTimeout = 30 * time.Second

// baselinePrepareHint 是校验失败时给运维的固定指引（spec §3.1.2 第 2 点）。
const baselinePrepareHint = "hint: run `artex db-migrate --prepare`"

// DSN resolves the TiDB connection string and reports where it came from.
// Precedence (config §3.1.1): env ARTEX_TIDB_DSN > database.dsn > 散列字段组装。
// There is no built-in default — it errors if neither source is configured.
func DSN() (dsn, source string, err error) {
	return config.TiDBDSN()
}

// DB wraps the shared *sql.DB. TiDB handles its own connection pool + concurrency
// (MVCC + 悲观事务)，所以不存在进程级写锁；池参数固定在同一份 OpenTiDB 上。
type DB struct{ *sql.DB }

// withPrepareHint 给基线校验失败补一行 --prepare 指引。
// schema.VerifyBaseline 自身已经带了同一条 hint，这里只在它没带时补，
// 避免运维日志里出现两遍同样的建议。
func withPrepareHint(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "artex db-migrate --prepare") {
		return err
	}
	return fmt.Errorf("%w\n  %s", err, baselinePrepareHint)
}

// Open connects to TiDB, verifies the 65-table baseline (read-only), and seeds
// builtin rows.
//
// 连接池不在这里重复配置：tidb.OpenTiDB 已固定 32 最大连接 / 30min 生命周期，
// 并在 Ping 通过后才做只读能力探测（会话时区必须是 +00:00，否则显式失败）。
// 基线校验失败 → 直接中止启动并指向 `artex db-migrate --prepare`（严禁带着残缺
// 结构继续 seed 出半套数据）。
func Open(dsn string) (*DB, error) {
	sqlDB, _, err := tidb.OpenTiDB(dsn)
	if err != nil {
		// OpenTiDB 内部已用 tidb.RedactDSN 脱敏连接串，这里不再重复拼接原文。
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), baselineVerifyTimeout)
	defer cancel()
	if err := schema.VerifyBaseline(ctx, sqlDB); err != nil {
		_ = sqlDB.Close()
		return nil, withPrepareHint(err)
	}
	d := &DB{sqlDB}
	if err := d.seedBuiltins(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("seed builtins: %w", err)
	}
	return d, nil
}

// coordinateWithSchemaMigration 让"长时间、跨表的归档事务"与运维侧的 DDL /
// 维护窗口互斥，同时不打扰普通运行时查询。
//
// 旧实现走 PG 的 pg_advisory_xact_lock；TiDB 没有 advisory lock，等价原语是
// db/tidb.CoordinateMaintenance：对 maintenance 单例行加 FOR UPDATE 行锁，并按
// op_id 判定维护任务是否在场（被持有 → ErrMaintenanceLocked，fail closed）。
// 调用方事务本身不变，行锁随 commit/rollback 原子释放。
func (d *DB) coordinateWithSchemaMigration(tx *sql.Tx) error {
	return tidb.CoordinateMaintenance(context.Background(), d.DB, tx)
}

// canonicalIdentityPair 把 schema 包算好的 canonical identity 编成写入用的两列：
// identity 载荷（<col>_identity BLOB）与其 SHA-256 摘要（<col>_hash BINARY(32)）。
//
// 新库把原先的 TEXT 主键/复合唯一键全部换成了 hash 唯一索引，写入方一旦漏掉这两列，
// 唯一键就会退化成 NULL 从而彻底失效——所以所有 hash 表的 INSERT 都必须经过这里。
func canonicalIdentityPair(identity []byte) (payload []byte, hash []byte) {
	sum := schema.CanonicalHash(identity)
	return identity, sum[:]
}

// builtinAgent describes one of the fixed agents and its prompt-variable catalog.
type builtinAgent struct {
	key, name, role, desc string
	vars                  []promptVar
	interactiveShell      bool // 建行时的默认交互式 shell 开关；ON DUPLICATE KEY UPDATE 不覆盖用户后续手动开关
	runSeconds            *int // 建行时的单次 run 墙钟上限(秒)；nil=用种子默认(1200)，0=不限时
}

type promptVar struct{ name, desc, example, source string }

// intp 返回 v 的指针，用于给 builtinAgent 可选字段(如 runSeconds)显式取值。
func intp(v int) *int { return &v }

// builtinAgents mirrors docs §5(a). 内置工具不入库；这里只 seed agent + 变量目录。
// 注：planner/worker/mainagent/auto 的交互式 shell 默认由下方 interactive_shell_default_v1
// 块统一置 true（尊重后续 toggle）；这里的 interactiveShell 只给需要「建行即默认开」的新 agent。
var builtinAgents = []builtinAgent{
	{"goals", "目标拆解", "goals", "把渗透任务目标拆解成若干独立、可验证的子目标。", []promptVar{
		{"EngagementDescription", "任务描述（测试对象/背景）", "测试 example.com 站点", "exploration"},
		// Now 是全局 runtime 变量(见 server.globalPromptVars),不再在各 agent 目录里
		// 重复定义,否则 withGlobalVars 追加时会与全局项撞名。
	}, false, nil},
	{"planner", "规划", "planner", "读取态势、判定目标，只在确有未覆盖的新方向时补充探索意图（每任务一个规划循环）。", []promptVar{
		{"Goal", "任务总目标", "拿下 example.com 的管理员权限", "exploration"},
		{"AssetSummary", "资产计数/类型分布摘要(可选)", "domain:3 ip:5 site:2", "distilled"},
	}, false, nil},
	{"mainagent", "主", "main", "人机接口：观察进展，把人的意图落成 hint 或高优先级意图。", []promptVar{
		{"Goal", "当前任务目标", "拿下 example.com 的管理员权限", "exploration"},
		{"AssetSummary", "开局态势摘要(可选)", "domain:3 ip:5", "distilled"},
		{"FindingsSummary", "已确认漏洞摘要(可选)", "high:1 medium:2", "distilled"},
	}, false, nil},
	{"worker", "执行", "worker", "领取一条意图执行，把发现的事实/漏洞写回知识图谱后停止。", []promptVar{
		{"ProxyAddr", "记录代理地址(驱动 if 双文案)", "127.0.0.1:8080", "runtime"},
		{"WorkerName", "worker 自我标识(可选)", "worker-1", "runtime"},
	}, false, nil},
	// Auto:内置「平台操作」agent。不参与渗透编排循环,经对话页驱动,用工具操作平台。
	{"auto", "Auto", "assistant", "平台操作助手：用工具管理任务(建/看/暂停/给提示)与资产，并可创建/修改 skill、自定义工具、MCP。", nil, false, nil},
	// 渗透测试:内置「独立渗透」agent。经对话页驱动,一人从侦察到收尾走完整条渗透链,自己规划自己执行自己验证。默认开启交互式 shell。
	{"pentest", "渗透测试", "assistant", "独立渗透 agent：一人从侦察→找攻击面→深入利用→验证→收尾走完整条链，自己规划、自己执行、自己对抗式验证。", nil, true, intp(0)},
}

// seedBuiltins inserts the fixed built-in agents and their variable catalog (idempotent).
//
// agents / agent_prompt_vars / mcp_servers / agent_skill_visibility 在基线里都是
// hash 唯一键表：ON DUPLICATE KEY UPDATE 的冲突判定就落在 *_hash 唯一索引上，
// 因此 identity/hash 只在插入分支写入，更新分支只碰展示列（身份不可变）。
func (d *DB) seedBuiltins() error {
	for _, a := range builtinAgents {
		agentID, err := d.seedAgent(a)
		if err != nil {
			return fmt.Errorf("agent %s: %w", a.key, err)
		}
		for _, v := range a.vars {
			if err := d.seedPromptVar(agentID, v); err != nil {
				return fmt.Errorf("agent %s var %s: %w", a.key, v.name, err)
			}
		}
	}
	// Drop catalog entries for variables that were renamed, so the white-list no
	// longer advertises a name templates can't resolve (EngagementTitle→Description).
	// 'Now' 从各 agent 目录提升为全局 runtime 变量后,旧库里 goals 仍残留一条 'Now'
	// 会与全局项撞名(前端变量列表 key 重复);一并清掉。
	if _, err := d.Exec(`DELETE FROM agent_prompt_vars WHERE var_name IN ('EngagementTitle', 'CoverageGaps', 'Now')`); err != nil {
		return fmt.Errorf("cleanup renamed vars: %w", err)
	}
	// Default-on interactive_shell for the runtime agents (planner/worker/mainagent/auto)
	// ONCE — respects a later user toggle-off (guarded by a settings flag). goals(one-shot
	// decomposer) stays off.
	if v, _, _ := d.GetSetting("interactive_shell_default_v1"); v != "true" {
		if _, err := d.Exec(`UPDATE agents SET interactive_shell = TRUE WHERE ` + "`key`" + ` IN ('planner','worker','mainagent','auto')`); err != nil {
			return fmt.Errorf("seed interactive_shell defaults: %w", err)
		}
		_ = d.SetSetting("interactive_shell_default_v1", "true")
	}
	// Seed the built-in browser (Playwright) MCP once — DISABLED by default (用户
	// 需要时自行启用), no proxy by default. The traffic-capture toggle injects/strips
	// the recording proxy + CA at runtime (server.Manager.syncBrowserMCPProxy).
	// Insert only if absent so we never clobber user edits (args/env/enabled/
	// visibility) on restart.
	if err := d.seedBrowserMCP(); err != nil {
		return fmt.Errorf("seed browser mcp: %w", err)
	}
	// NOTE: the placeholder ScopeSentry data-source MCP (empty URL + empty X-API-Key,
	// disabled) used to be seeded directly in schema.sql §F; that embed is gone, and
	// it now ships as a plain user-configurable MCP row (创建时同样补齐 name_identity/name_hash).
	if err := d.seedBuiltinSkillVisibility(); err != nil {
		return fmt.Errorf("seed skill visibility: %w", err)
	}
	if err := d.seedDefaultInterceptRules(); err != nil {
		return fmt.Errorf("seed intercept rules: %w", err)
	}
	if err := d.seedDefaultInterceptRulesV2(); err != nil {
		return fmt.Errorf("seed intercept rules v2: %w", err)
	}
	if err := d.seedDefaultInterceptRulesV3(); err != nil {
		return fmt.Errorf("seed intercept rules v3: %w", err)
	}
	if err := d.seedDefaultAssetInterceptRules(); err != nil {
		return fmt.Errorf("seed asset intercept rules: %w", err)
	}
	return nil
}

// seedAgent upserts one builtin agent and returns its row id.
//
// wrapup_prompt / task_timeout_wrapup_prompt / trigger_run_mode / trigger_merge_mode
// 在基线里是 MEDIUMTEXT NOT NULL，而 TiDB 拒绝给 TEXT 列声明 DDL 默认值，因此
// strict 模式下省略它们会得到 Error 1364；这里按目录声明的应用层默认值显式补齐。
//
// TiDB 的 ON DUPLICATE KEY UPDATE 不返回行 id，而 LAST_INSERT_ID() 在"走了更新
// 分支"时依赖 id=LAST_INSERT_ID(id) 这类方言技巧，可靠性不值得赌；基线里
// uq_agents_key_hash 是唯一键，按 key_hash 回查是确定性的自增 id 读取路径。
func (d *DB) seedAgent(a builtinAgent) (int64, error) {
	identity, hash := canonicalIdentityPair(schema.AgentKeyIdentity(a.key))
	if _, err := d.Exec(`
INSERT INTO agents(`+"`key`"+`, key_identity, key_hash, name, description, role, builtin, enabled,
                           interactive_shell, run_seconds, wrapup_prompt, task_timeout_wrapup_prompt,
                           trigger_run_mode, trigger_merge_mode)
VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, TRUE, TRUE, ?, COALESCE(?, 1200), '', '', 'serial', 'all')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description)`,
		a.key, identity, hash, a.name, a.desc, a.role, a.interactiveShell, a.runSeconds); err != nil {
		return 0, err
	}
	var id int64
	if err := d.QueryRow(`SELECT id FROM agents WHERE key_hash = ?`, hash).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// seedPromptVar upserts one entry of an agent's prompt-variable catalog.
func (d *DB) seedPromptVar(agentID int64, v promptVar) error {
	identity, hash := canonicalIdentityPair(schema.AgentPromptVarIdentity(uint64(agentID), v.name))
	_, err := d.Exec(`
INSERT INTO agent_prompt_vars(agent_id, var_name, identity, identity_hash, description, example, source)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  description = VALUES(description), example = VALUES(example), source = VALUES(source)`,
		agentID, v.name, identity, hash, v.desc, v.example, v.source)
	return err
}

// seedBrowserMCP inserts the built-in Playwright MCP server only if absent
// (name_hash 唯一键)，绝不覆盖用户后续改过的 args/env/enabled。
func (d *DB) seedBrowserMCP() error {
	identity, hash := canonicalIdentityPair(schema.MCPServerNameIdentity("browser"))
	_, err := d.Exec(`
INSERT INTO mcp_servers(name, name_identity, name_hash, transport, command, args, env, enabled)
SELECT ?, ?, ?, 'stdio', 'npx', CAST(? AS JSON), CAST('{}' AS JSON), FALSE
FROM (SELECT 1) AS seed
WHERE NOT EXISTS (SELECT 1 FROM mcp_servers WHERE name_hash = ?)`,
		"browser", identity, hash, `["@playwright/mcp","--headless"]`, hash)
	return err
}

// seedDefaultAssetInterceptRules inserts the built-in asset blocklist (fuzzy
// domain matches for government / education sites) once on first startup. Gated
// by a settings flag so a user's later disable/delete is never resurrected on
// restart — same policy as the intercept-rule seed.
//
// asset_intercept_rules 在新基线里没有自然键唯一索引（同一条 pattern 允许存在
// 多行），所以"只 seed 一次"由 settings 开关 + 按 (kind, pattern) 的存在性判定
// 共同保证：开关行万一丢了（比如上次启动 seed 到一半崩溃），也不会插出重复规则。
func (d *DB) seedDefaultAssetInterceptRules() error {
	if v, _, _ := d.GetSetting("asset_intercept_default_rules_v1"); v == "done" {
		return nil
	}
	rules := []struct {
		kind    string
		pattern string
		note    string
	}{
		{"fuzzy_domain", ".gov", "[内置] 政府网站 (.gov)"},
		{"fuzzy_domain", ".gov.cn", "[内置] 政府网站 (.gov.cn)"},
		{"fuzzy_domain", ".edu", "[内置] 教育网站 (.edu)"},
		{"fuzzy_domain", ".edu.cn", "[内置] 教育网站 (.edu.cn)"},
	}
	for _, r := range rules {
		if err := d.insertAssetInterceptRuleSeed(r.kind, r.pattern, r.note); err != nil {
			return fmt.Errorf("asset rule %q: %w", r.pattern, err)
		}
	}
	return d.SetSetting("asset_intercept_default_rules_v1", "done")
}

// insertAssetInterceptRuleSeed 幂等插入一条内置资产规则。
// 新表 builtin 列 NOT NULL 且 MEDIUMTEXT 列没有 DDL 默认值，必须逐列显式给值。
func (d *DB) insertAssetInterceptRuleSeed(kind, pattern, note string) error {
	var n int
	if err := d.QueryRow(`
SELECT COUNT(*) FROM asset_intercept_rules WHERE kind = ? AND pattern = ?`, kind, pattern).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := d.Exec(`
INSERT INTO asset_intercept_rules(enabled, kind, pattern, note, builtin)
VALUES (TRUE, ?, ?, ?, TRUE)`, kind, pattern, note)
	return err
}

// builtinSkillVisibility maps a shipped skill's directory name → the built-in
// agent keys that should see it by default. The skill FILES themselves live on the
// filesystem (SkillDir, loaded by norma at runtime); DB only carries this visibility
// binding. Skills omitted here (e.g. playwright-cli, scopesentry) ship invisible by
// default — the user turns them on per-agent when needed. scopesentry additionally
// declares `mcps: ScopeSentry`, which only takes effect once it's made visible and
// that MCP is enabled/configured.
var builtinSkillVisibility = map[string][]string{
	"api-recon": {"auto", "pentest", "worker"},
}

// seedBuiltinSkillVisibility binds the shipped built-in skills to their default
// agents. Insert-if-absent so a user's later toggle-off is never resurrected on
// restart — matches the browser-MCP / intercept-rule seed policy.
func (d *DB) seedBuiltinSkillVisibility() error {
	for skillName, agentKeys := range builtinSkillVisibility {
		for _, key := range agentKeys {
			var agentID int64
			err := d.QueryRow(`SELECT id FROM agents WHERE `+"`key`"+` = ?`, key).Scan(&agentID)
			if err == sql.ErrNoRows {
				continue // agent 不存在（异常库）：跳过即可，seedAgent 之后再没有理由缺它
			}
			if err != nil {
				return fmt.Errorf("skill %s → agent %s: %w", skillName, key, err)
			}
			identity, hash := canonicalIdentityPair(schema.AgentSkillVisibilityIdentity(uint64(agentID), skillName))
			if _, err := d.Exec(`
INSERT INTO agent_skill_visibility(agent_id, skill_name, identity, identity_hash, enabled)
SELECT ?, ?, ?, ?, TRUE
FROM (SELECT 1) AS seed
WHERE NOT EXISTS (SELECT 1 FROM agent_skill_visibility WHERE identity_hash = ?)`,
				agentID, skillName, identity, hash, hash); err != nil {
				return fmt.Errorf("skill %s → agent %s: %w", skillName, key, err)
			}
		}
	}
	return nil
}

// interceptRuleSeed 是一条内置拦截规则的写入载体（列顺序与 interceptRuleSeedSQL 一致）。
type interceptRuleSeed struct {
	name           string
	enabled        bool
	priority       int
	matchTarget    string
	matchType      string
	pattern        string
	action         string
	message        string
	timeoutEnabled bool
	timeoutSeconds int
	timeoutAction  string
}

// insertInterceptRuleSeed 幂等插入一条内置拦截规则。
//
// 新基线 intercept_rules 的 name 上没有唯一索引（内置规则本就允许同名多行），
// 所以 ON CONFLICT DO NOTHING 那种"靠约束去重"在 TiDB 上等于完全不去重，
// 一旦 settings 开关丢失就会每次重启插一份重复规则。这里改成显式的 name 存在性
// 判定：启动期单线程，判定与写入之间没有并发窗口。
func (d *DB) insertInterceptRuleSeed(r interceptRuleSeed) error {
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM intercept_rules WHERE name = ?`, r.name).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := d.Exec(`
INSERT INTO intercept_rules(name, enabled, priority, match_target, match_type, pattern, action, message, timeout_enabled, timeout_seconds, timeout_action)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.name, r.enabled, r.priority, r.matchTarget, r.matchType, r.pattern, r.action, r.message,
		r.timeoutEnabled, r.timeoutSeconds, r.timeoutAction)
	return err
}

// seedDefaultInterceptRules inserts built-in safety intercept rules once on
// first startup. The seed is gated by a settings flag so user edits (disable,
// delete, re-order) are never overwritten on subsequent restarts.
func (d *DB) seedDefaultInterceptRules() error {
	if v, _, _ := d.GetSetting("intercept_default_rules_v1"); v == "done" {
		return nil
	}
	rules := []interceptRuleSeed{
		// ── 系统破坏性命令 (priority 100) ──────────────────────────────────
		{
			name:           "[内置] 递归强制删除 rm -rf",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\brm\b.{0,80}(?:-[a-z]*r[a-z]*f[a-z]*|-[a-z]*f[a-z]*r[a-z]*|--recursive|--no-preserve-root)`,
			action:         "deny",
			message:        "禁止执行递归强制删除（rm -rf / rm --recursive），可能永久损坏系统或靶机环境",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 删除系统关键目录",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\brm\b[^"'\n]{0,60}["'\s](/|/etc|/bin|/usr|/boot|/var|/lib|/sys|/proc|/dev|/sbin|/root)`,
			action:         "deny",
			message:        "禁止删除系统关键路径",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 磁盘格式化 mkfs",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\bmkfs\b`,
			action:         "deny",
			message:        "禁止格式化磁盘（mkfs）",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 覆写磁盘设备 dd",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\bdd\b[^|\n]{0,100}\bof=\s*/dev/[a-zA-Z]`,
			action:         "deny",
			message:        "禁止使用 dd 覆写磁盘设备",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] Fork 炸弹",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `:\(\)\s*\{[^}]*:\|:`,
			action:         "deny",
			message:        "禁止执行 Fork 炸弹",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 关机 / 重启",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\b(?:shutdown|reboot|halt|poweroff|init\s+[06])\b`,
			action:         "deny",
			message:        "禁止执行关机或重启命令",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 杀死全部进程",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\bkill\s+-9\s+-1\b|\bkillall\s+-9\b`,
			action:         "deny",
			message:        "禁止 kill -9 -1 或 killall -9（杀死所有进程）",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 磁盘擦除 shred / wipe",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\b(?:shred|wipe)\b[^|\n]{0,80}/dev/[a-zA-Z]`,
			action:         "deny",
			message:        "禁止对磁盘设备执行 shred/wipe 擦除",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 清空防火墙规则",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `\biptables\s+(?:-F|--flush)\b|\bnft\s+flush\s+ruleset\b`,
			action:         "deny",
			message:        "禁止清空防火墙规则（iptables -F / nft flush）",
			priority:       100,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		// ── 数据库破坏性操作 (priority 90) ─────────────────────────────────
		{
			name:           "[内置] SQL DROP DATABASE / TABLE / SCHEMA",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\bDROP\s+(?:DATABASE|TABLE|SCHEMA|INDEX|VIEW|TABLESPACE|USER|ROLE)\b`,
			action:         "deny",
			message:        "禁止执行 DROP 操作，可能不可逆地销毁数据库对象",
			priority:       90,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] SQL TRUNCATE",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\bTRUNCATE\s+(?:TABLE\s+)?\w`,
			action:         "deny",
			message:        "禁止执行 TRUNCATE，可能清空数据表所有数据",
			priority:       90,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] MongoDB drop / dropDatabase",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\.(?:dropDatabase|dropCollection|drop)\s*\(`,
			action:         "deny",
			message:        "禁止执行 MongoDB drop 操作",
			priority:       90,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] Redis FLUSHALL / FLUSHDB",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\b(?:FLUSHALL|FLUSHDB)\b`,
			action:         "deny",
			message:        "禁止执行 Redis FLUSHALL / FLUSHDB，可能清空全部缓存数据",
			priority:       90,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		// ── HTTP 破坏性请求 (priority 80) ──────────────────────────────────
		// Agent 发送 DELETE 请求的三种常见方式：
		//   1. curl -X DELETE / --request DELETE（Bash 工具直接执行或写入脚本）
		//   2. Python HTTP 客户端 .delete() 方法
		//   3. JS/通用脚本里的 method: 'DELETE' / method="DELETE"
		{
			name:           "[内置] curl / wget 发送 DELETE 请求",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\bcurl\b[^|\n&;"]{0,300}(?:-X\s*DELETE|--request\s+DELETE|-XDELETE)|\bwget\b[^|\n&;"]{0,300}--method[=\s]+DELETE`,
			action:         "deny",
			message:        "禁止通过 curl/wget 发送 HTTP DELETE 请求，可能删除目标系统数据",
			priority:       80,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] Python HTTP 客户端 DELETE（requests/httpx/aiohttp）",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)\b(?:requests|httpx|aiohttp|urllib\.request)\.delete\s*\(|session\.delete\s*\(|client\.delete\s*\(`,
			action:         "deny",
			message:        "禁止使用 Python HTTP 客户端发送 DELETE 请求",
			priority:       80,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 脚本中声明 HTTP DELETE 方法（JS/通用）",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)axios\.delete\s*\(|method\s*[:=]\s*['"]DELETE['"]`,
			action:         "deny",
			message:        "禁止在脚本中声明并发送 HTTP DELETE 请求",
			priority:       80,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 批量清空 / 清除接口路径",
			enabled:        true,
			matchTarget:    "tool_input",
			matchType:      "regex",
			pattern:        `(?i)/(?:clear|wipe|flush|purge|truncate|drop|destroy|factory[-_]reset|reset[-_]all)(?:[/?#"'\s]|$)`,
			action:         "deny",
			message:        "禁止调用批量清空或销毁类接口（/clear /wipe /flush /purge 等）",
			priority:       80,
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
	}
	for _, r := range rules {
		if err := d.insertInterceptRuleSeed(r); err != nil {
			return fmt.Errorf("rule %q: %w", r.name, err)
		}
	}
	return d.SetSetting("intercept_default_rules_v1", "done")
}

// seedDefaultInterceptRulesV2 migrates the two safety patterns that used to be
// hard-coded in guard.go (destructive shell + data-exfil pipe) into ordinary
// intercept rules. Gated by its own flag so it also lands on DBs that already ran
// v1. Unlike the old guard.go floor, these are plain [内置] rules — the user can
// disable or delete them. The exfil rule ships DISABLED by default (its
// curl/wget/nc pipe pattern mis-fires on legitimate CTF/pentest reverse-shell and
// data-transfer pipes); enable it manually when exfil gating is actually wanted.
func (d *DB) seedDefaultInterceptRulesV2() error {
	if v, _, _ := d.GetSetting("intercept_default_rules_v2"); v == "done" {
		return nil
	}
	rules := []interceptRuleSeed{
		{
			name:           "[内置] 破坏性系统命令",
			pattern:        `(?i)\b(rm\s+-rf\s+/|mkfs|dd\s+if=|:\(\)\s*\{|shutdown|reboot|>\s*/dev/sd)`,
			action:         "deny",
			message:        "破坏性命令被拒绝（rm -rf / / mkfs / dd / fork bomb / 关机重启 / 覆写磁盘设备）",
			enabled:        true,
			priority:       100,
			matchTarget:    "tool_input",
			matchType:      "regex",
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
		{
			name:           "[内置] 数据外泄管道",
			pattern:        `(?i)(curl|wget|nc|ncat)\b[^|]*\b(\|\s*(curl|wget|nc))`,
			action:         "deny",
			message:        "疑似数据外泄管道被拒绝（命令输出经 curl/wget/nc 外传）",
			enabled:        false,
			priority:       80,
			matchTarget:    "tool_input",
			matchType:      "regex",
			timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
		},
	}
	for _, r := range rules {
		if err := d.insertInterceptRuleSeed(r); err != nil {
			return fmt.Errorf("rule %q: %w", r.name, err)
		}
	}
	return d.SetSetting("intercept_default_rules_v2", "done")
}

// seedDefaultInterceptRulesV3 adds the delete-endpoint path rule. The v1 HTTP rules
// only catch the DELETE *method* (curl -X DELETE, requests.delete(, method:'DELETE'),
// and v1's path rule covers only /clear /wipe /flush /purge /truncate /drop /destroy
// /factory-reset /reset-all — so a plain `curl 'http://t/api/user/delete?id=1'` (a
// delete endpoint reached with GET/POST, which is how most web apps expose deletion)
// slipped through every built-in rule. Own flag so it also lands on DBs that already
// ran v1/v2, where editing the v1 seed would have no effect.
//
// Exported as a package const only so the seeded regex is unit-testable without a DB.
const deleteEndpointPathPattern = `(?i)/(?:(?:delete|remove|unlink|erase|destroy)[-\w]*|del)(?:[/?#"'\s]|$)`

func (d *DB) seedDefaultInterceptRulesV3() error {
	if v, _, _ := d.GetSetting("intercept_default_rules_v3"); v == "done" {
		return nil
	}
	const name = "[内置] 删除类接口路径"
	err := d.insertInterceptRuleSeed(interceptRuleSeed{
		name:           name,
		enabled:        true,
		priority:       80,
		matchTarget:    "tool_input",
		matchType:      "regex",
		pattern:        deleteEndpointPathPattern,
		action:         "deny",
		message:        "禁止调用删除类接口（/delete /remove /unlink /erase 等），不论使用哪种 HTTP 方法——多数应用的删除接口用 GET/POST 就能触发，同样会真实删除目标数据",
		timeoutEnabled: false, timeoutSeconds: 60, timeoutAction: "deny",
	})
	if err != nil {
		return fmt.Errorf("rule %q: %w", name, err)
	}
	return d.SetSetting("intercept_default_rules_v3", "done")
}
