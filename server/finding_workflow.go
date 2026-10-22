package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/traffic"
	actool "github.com/Autumn-27/norma/tool"
)

// =====================================================================
// 内置工具目录的一次性升级（W5 任务四：原生 TiDB 8.5 方言）
//
// tools.`schema` / tools.agents 在新库里是「MEDIUMTEXT 存 JSON 文本」（schema
// baseline 的 JSONN 列，见 db/tools.go 的表注释）。PG 的 `::jsonb` 转换、`@>`
// 包含判定、`jsonb_array_length`、`jsonb_set`、以及裸 `?`（key 成员判定）在
// TiDB 都没有可用等价写法，MySQL 的 JSON_* 函数也不适合再拼进这一串升级逻辑。
// 统一改造机制：
//
//	BeginTx
//	  → SELECT row_id FROM tools WHERE `key` = ? AND `system` = TRUE   普通读，只定位
//	  → SELECT … FROM tools WHERE row_id = ? FOR UPDATE                 只锁这一行
//	  → Go 端解析 / 核验 / 改写（核验不通过就原样返回，绝不覆盖用户编辑）
//	  → UPDATE tools SET … WHERE row_id = ?
//	  → Commit
//
// 两个刻意的选择：
//  1. 先取 row_id 再按主键加锁，而不是一句「WHERE key = ? FOR UPDATE」：key 是
//     MEDIUMTEXT，唯一性只落在 key_hash 上，按文本谓词加行锁会在 TiDB 悲观模式下
//     把扫到的每一行都锁住（等于整张 tools 表）；按 row_id 加锁才与旧的单条 UPDATE
//     同量级。定位读与加锁读之间行消失 → 按「不可用」跳过。
//  2. 新值一律**从行锁读回的那一份字节**派生，并且经 decodeToolSchema（UseNumber）
//     解码后再 json.Marshal（见 rewriteToolSchema）。默认 map[string]any 会把数字
//     过一遍 float64，用户在页面上改过的 default、maximum 这类大整数会被静默改写
//     （9007199254740993 → 9007199254740992）。jsonCanonical 同样走 UseNumber，
//     因此它只归一键序与空白、**保留数字字面量**，不做数值归一（1 与 1.0 判不
//     相等）：误判的后果只是多写一次同样安全的字节，绝不会把用户数字改写成 float64
//     形式。**首值**非法的 JSON 一律 fail closed 返错（只用一次 Decoder.Decode，
//     「合法前缀 + 尾随垃圾」会被接受，正常 API 写不进这种数据；见离线用例
//     TestToolSchemaDecodeRejectsBadInput），绝不把用户手配的 agents 重写成空数组。
// =====================================================================

// toolRow 是行锁下读回的工具行，只带升级需要的列。
type toolRow struct {
	rowID       int64
	description string
	schema      []byte
	agents      []byte
}

// toolRowMutation 在行锁内决定这一行要不要改：返回 changed=false 表示核验未命中
// （行未 seed、用户在页面改过等），调用方保持原行不动。
type toolRowMutation func(ctx context.Context, tx *sql.Tx, row *toolRow) (bool, error)

// mutateToolRow 在单事务内对单个内置工具行做「锁读 → Go 端改写 → 写回」。
// changed 表示是否真的写了这一行；error 只在数据库或数据异常时出现，调用方据此
// 放弃本次升级（一次性 flag 不落，下次启动重试 —— 与旧实现同语义）。
func mutateToolRow(d *db.DB, key string, mutate toolRowMutation) (bool, error) {
	ctx := context.Background()
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // Commit 之后是 no-op

	// 第一步只定位、不加锁：见文件头第 1 条。
	ids, err := toolRowIDs(ctx, tx, key)
	if err != nil || len(ids) == 0 {
		return false, err
	}
	if len(ids) > 1 {
		return false, fmt.Errorf("tools %s: 同一 key 命中 %d 条内置行，拒绝改写", key, len(ids))
	}

	row := &toolRow{rowID: ids[0]}
	err = tx.QueryRowContext(ctx, "SELECT description, `schema`, agents FROM tools WHERE row_id = ? FOR UPDATE", row.rowID).
		Scan(&row.description, &row.schema, &row.agents)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil // 定位之后被并发删除：跳过
	}
	if err != nil {
		return false, err
	}

	changed, err := mutate(ctx, tx, row)
	if err != nil || !changed {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// toolRowIDs 按自然键定位内置行的 row_id（最多取 2 条，用来发现重复 key）。
func toolRowIDs(ctx context.Context, tx *sql.Tx, key string) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, "SELECT row_id FROM tools WHERE `key` = ? AND `system` = TRUE ORDER BY row_id LIMIT 2", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// writeToolColumns 把 Go 端算好的列写回锁住的那一行。列名来自本包白名单，值一律
// "?" 绑定；updated_at 走 dbNow（NOW(3)），与 db 包同一口径。
func writeToolColumns(ctx context.Context, tx *sql.Tx, rowID int64, set *setColumns) error {
	set.set("updated_at", dbNow{})
	query, args := set.query("tools", "row_id = ?", rowID)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

// decodeToolSchema 把 schema 列解成对象。UseNumber 保住数字字面量原样；非对象或
// 非法 JSON 返错 —— 覆盖式重写等于把用户的参数表清空。
func decodeToolSchema(raw []byte, key string) (map[string]any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("tools %s: schema 列为空，拒绝改写", key)
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("tools %s: schema 列不是合法 JSON，拒绝改写: %w", key, err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tools %s: schema 列不是 JSON 对象，拒绝改写", key)
	}
	return obj, nil
}

// toolAgentList 把 agents 列解成有序字符串列表；非数组（JSON null 除外）或含
// 非字符串元素一律返错。列值是 JSON null 时按 encoding/json 的语义解成 (nil, nil)：
// 调用方的 len(agents) 判定会把它当成「不匹配历史默认」，既不重写也无数据风险
// （钉在 TestWorkflowToolAgentList，复审 O-04）。
func toolAgentList(raw []byte, key string) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("tools %s: agents 列为空，拒绝改写", key)
	}
	var items []string
	if err := json.Unmarshal(trimmed, &items); err != nil {
		return nil, fmt.Errorf("tools %s: agents 列不是字符串数组，拒绝改写: %w", key, err)
	}
	return items, nil
}

// toolAgentsHasAll 复刻 jsonb 数组的 `@>`（包含全部给定元素，不要求顺序）。
func toolAgentsHasAll(items []string, want ...string) bool {
	for _, w := range want {
		found := false
		for _, item := range items {
			if item == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// jsonEqualCanonical 做 JSON **值**级比较：UseNumber 解码后紧凑序列化，把键序与空白
// 归一后再比。**注意：数字只保留字面量，不做数值归一**（canonical(1)="1"、
// canonical(1.0)="1.0"、canonical(1e2)="1e2"，两两不相等），这一点与 PG `jsonb` 的
// 数值级等值不同。在本包里这个差异是安全的：比较只用来判断「本行需不需要写回」，
// 而 next 永远是从同一份锁读字节按 UseNumber 派生的，判成「不同」最坏只是多写一次
// 精度不变的等价文档，绝不会覆盖或改写用户的数据字面量。
func jsonEqualCanonical(a, b []byte) bool {
	ca, err := jsonCanonical(a)
	if err != nil {
		return false
	}
	cb, err := jsonCanonical(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ca, cb)
}

// jsonCanonical 用 json.Number 语义解码后紧凑序列化，作为值级比较的规范形：只归一
// 键序与空白，数字字面量原样保留（不做数值归一，理由见 jsonEqualCanonical）。
func jsonCanonical(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (s *Server) seedFindingWorkflowTools() {
	const hostSearchDescriptionFlag = "finding_workflow_tools_v3_host_search_description"
	if value, _, _ := s.m.pg.GetSetting(hostSearchDescriptionFlag); value != "true" {
		// Only replace the original built-in text. A user-edited description is
		// authoritative and must survive upgrades.
		legacy := "查询记录代理已抓取的目标流量（必须指定 host，可再按 URL 子串或正文关键词过滤）。body_contains 会在已抓取的请求/响应头与正文中做全文搜索，支持任意子串和中文（至少 3 个字符），可用来找响应里的密码、密钥、报错、内网地址等。仅返回极轻量索引(id/method/url/status/resp_len)，不含任何响应内容。默认只返回 3 条、每页最多 10 条；结果多时用 page 翻页（page=0 起）；要看某条的请求/响应原文用 traffic_get(id)。回看已访问资源、找端点先用它，避免重复 curl 同一 URL。"
		if _, err := s.replaceToolDescriptionIfUnchanged("traffic_search", legacy, traffic.TrafficSearchDescription); err != nil {
			// Log and leave the flag unset so the next startup retries; do not
			// return, or a transient error here would also skip the reporter
			// migration below — the two are independent.
			log.Printf("[evidence] upgrade traffic_search description: %v", err)
		} else {
			_ = s.m.pg.SetSetting(hostSearchDescriptionFlag, "true")
		}
	}
	const flag = "finding_workflow_tools_v2_reporter"
	if value, _, _ := s.m.pg.GetSetting(flag); value == "true" {
		return
	}
	// 三个工具的 schema 补属性：读、改、写全在 mutateToolRow 的同一事务 + 同一行锁里，
	// 而且 next 派生自**锁读**到的那一份字节（见 rewriteToolSchema），因此
	//   - 不会拿锁外读到的旧文档覆盖并发编辑（旧实现是「锁外读 + 值 CAS」）；
	//   - 全程 json.Number，用户在页面上改过的 default / maximum 等大整数字面量
	//     原样保留，不会被 float64 静默改写。
	for _, key := range workflowSchemaUpgradeKeys {
		// 锁外这次 GetTool 只用来判定「行存在且是内置工具」；schema 本身**不在锁外
		// 解析**，新值一律由行锁读回的那份字节派生（见 upgradeToolSchema）。
		row, err := s.m.pg.GetTool(key)
		if err != nil {
			log.Printf("[evidence] load %s: %v", key, err)
			return
		}
		if row == nil || !row.System {
			continue
		}
		if _, err := s.upgradeToolSchema(key, upgradeFindingWorkflowSchema); err != nil {
			log.Printf("[evidence] upgrade %s: %v", key, err)
			return
		}
	}
	// Upgrade only the original default binding. Customized lists and enabled
	// flags survive; the one-time flag also preserves future user unbinding.
	//
	// 三条判定逐条对应旧的 jsonb 谓词：`@>` = 包含全部元素，`jsonb_array_length`
	// = 元素个数；命中「历史代码默认」才整体替换，用户改过的绑定原样保留。
	readers := `["worker","reporter"]`
	for _, key := range []string{"traffic_search", "traffic_get", "traffic_blob"} {
		if _, err := s.replaceToolAgentsIf(key, func(agents []string) bool {
			if len(agents) == 1 && agents[0] == "worker" {
				return true // 旧语义：绑定数组恰好只有 worker 一项
			}
			return len(agents) == 5 && toolAgentsHasAll(agents, "worker", "planner", "mainagent", "auto", "pentest")
		}, readers); err != nil {
			log.Printf("[evidence] upgrade %s agents: %v", key, err)
			return
		}
	}
	if _, err := s.replaceToolAgentsIf("get_finding_traffic", func(agents []string) bool {
		return len(agents) == 2 && toolAgentsHasAll(agents, "auto", "reporter")
	}, `["auto","reporter","worker","planner","mainagent","pentest"]`); err != nil {
		log.Printf("[evidence] upgrade get_finding_traffic agents: %v", err)
		return
	}
	// Replace the previous code default only; preserve customized binding lists.
	if _, err := s.replaceToolAgentsIf("bind_finding_traffic", func(agents []string) bool {
		return len(agents) == 5 && toolAgentsHasAll(agents, "worker", "planner", "mainagent", "auto", "pentest")
	}, `["reporter"]`); err != nil {
		log.Printf("[evidence] upgrade bind_finding_traffic agents: %v", err)
		return
	}
	if err := s.m.pg.AddAgentToToolBinding("reporter", []string{"bind_finding_traffic"}); err != nil {
		return
	}
	_ = s.m.pg.SetSetting(flag, "true")
}

// replaceToolDescriptionIfUnchanged 只在描述仍是某个历史默认时替换成新文案；用户
// 编辑过的描述是权威数据（与旧 UPDATE 的 description 等值守卫同语义）。
func (s *Server) replaceToolDescriptionIfUnchanged(key, expected, next string) (bool, error) {
	return mutateToolRow(s.m.pg, key, func(ctx context.Context, tx *sql.Tx, row *toolRow) (bool, error) {
		if row.description != expected {
			return false, nil
		}
		set := &setColumns{}
		set.set("description", next)
		return true, writeToolColumns(ctx, tx, row.rowID, set)
	})
}

// workflowSchemaUpgradeKeys 是本次一次性升级要补属性的内置工具。
var workflowSchemaUpgradeKeys = []string{"report_finding", "add_hint", "add_task_hint"}

// toolSchemaUpgrade 在 UseNumber 解出的 schema 文档上就地补齐要新增的属性。
type toolSchemaUpgrade func(key string, doc map[string]any) error

// upgradeFindingWorkflowSchema 给三个 workflow 工具补上后加的参数。
//
// 精度不变式：doc 由 decodeToolSchema（UseNumber）产出，函数只**新增**缺失的键，
// 既有节点（包括用户在页面上改过的 default / maximum 这类数字）原封不动，因此
// json.Marshal 回去后数字字面量逐字节保持原样。
//
// fail closed：中间层已存在但不是 JSON 对象时返错，绝不覆盖（见 toolSchemaLayer）
// —— 覆盖等于把用户手写的整张参数表清空。
func upgradeFindingWorkflowSchema(key string, doc map[string]any) error {
	props, err := toolSchemaLayer(doc, "properties", "schema")
	if err != nil {
		return fmt.Errorf("tools %s: %w", key, err)
	}
	if key == "report_finding" {
		toolSchemaAddIfAbsent(props, "evidence_hint_id", map[string]any{"type": "integer", "description": "可选：本任务中对应此漏洞的 hint ID；读取该提示保存的 traffic_refs 一并绑定，无提示时省略"})
		return nil
	}
	toolSchemaAddIfAbsent(props, "traffic_refs", agent.HintTrafficSchema())
	hints, err := toolSchemaLayer(props, "hints", "schema.properties")
	if err != nil {
		return fmt.Errorf("tools %s: %w", key, err)
	}
	toolSchemaAddIfAbsent(hints, "type", "array")
	items, err := toolSchemaLayer(hints, "items", "schema.properties.hints")
	if err != nil {
		return fmt.Errorf("tools %s: %w", key, err)
	}
	toolSchemaAddIfAbsent(items, "type", "object")
	itemProps, err := toolSchemaLayer(items, "properties", "schema.properties.hints.items")
	if err != nil {
		return fmt.Errorf("tools %s: %w", key, err)
	}
	for name, value := range map[string]any{"text": strParam("提示内容"), "asset_ids": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}}, "traffic_refs": agent.HintTrafficSchema()} {
		toolSchemaAddIfAbsent(itemProps, name, value)
	}
	return nil
}

// rewriteToolSchema 从「行锁读回的 schema 字节」派生要写回的新字节：UseNumber 解码
// （decodeToolSchema）→ upgrade 就地补齐 → json.Marshal。它与 SQL / 事务完全解耦，
// 因此「大整数字面量在写回 JSON 里原样保留」这条精度不变式可以完全离线单测。
//
// changed=false 表示库里已经是目标形态（要补的键早就在），调用方整行不动、连
// updated_at 都不跳。判定用 jsonEqualCanonical：两侧都走 UseNumber，数字字面量不被
// 归一，所以只要 upgrade 没真正新增内容，两侧的规范形必然一致。
func rewriteToolSchema(key string, raw []byte, upgrade toolSchemaUpgrade) (string, bool, error) {
	doc, err := decodeToolSchema(raw, key)
	if err != nil {
		return "", false, err
	}
	if err := upgrade(key, doc); err != nil {
		return "", false, err
	}
	next, err := json.Marshal(doc)
	if err != nil {
		return "", false, fmt.Errorf("tools %s: schema 序列化失败: %w", key, err)
	}
	if jsonEqualCanonical(next, raw) {
		return "", false, nil
	}
	return string(next), true, nil
}

// upgradeToolSchema 在行锁内完成「锁读 schema → UseNumber 解码 → upgrade 补齐 →
// 写回」的闭环（I-01：新值只从锁读字节派生）。next 与库里现值同源，因此既不存在
// 「拿锁外旧文档覆盖并发编辑」的窗口，也不再需要旧的 expected 值 CAS；库里已是
// 目标形态时整行不动。
func (s *Server) upgradeToolSchema(key string, upgrade toolSchemaUpgrade) (bool, error) {
	return mutateToolRow(s.m.pg, key, func(ctx context.Context, tx *sql.Tx, row *toolRow) (bool, error) {
		next, changed, err := rewriteToolSchema(key, row.schema, upgrade)
		if err != nil || !changed {
			return false, err
		}
		set := &setColumns{}
		set.set("`schema`", next)
		return true, writeToolColumns(ctx, tx, row.rowID, set)
	})
}

// replaceToolAgentsIf 在行锁内把 agents 交给 match 核验（等价旧的 jsonb `@>` +
// `jsonb_array_length` 判定），命中才整体替换为 next（一段 JSON 数组文本）。
func (s *Server) replaceToolAgentsIf(key string, match func(agents []string) bool, next string) (bool, error) {
	return mutateToolRow(s.m.pg, key, func(ctx context.Context, tx *sql.Tx, row *toolRow) (bool, error) {
		agents, err := toolAgentList(row.agents, key)
		if err != nil {
			return false, err
		}
		if !match(agents) {
			return false, nil
		}
		set := &setColumns{}
		set.set("agents", next)
		return true, writeToolColumns(ctx, tx, row.rowID, set)
	})
}

// toolSchemaLayer 返回 parent[name] 这一层 JSON 对象，取代旧的 objectProperty：
// 后者会把「存在但不是对象」的值直接替换成空对象，等于清空用户手写的整张参数表。
// 这里缺失或 JSON null 才按 create_missing 语义新建，其它类型一律 fail closed 返错。
// label 只用来把错误定位到 schema 路径（例如 "schema.properties"）。
func toolSchemaLayer(parent map[string]any, name, label string) (map[string]any, error) {
	if child, ok := parent[name].(map[string]any); ok {
		return child, nil
	}
	if existing, exists := parent[name]; exists && existing != nil {
		return nil, fmt.Errorf("%s.%s 不是 JSON 对象（%v），拒绝改写", label, name, existing)
	}
	child := map[string]any{}
	parent[name] = child
	return child, nil
}

// toolSchemaAddIfAbsent 只在同名键不存在时挂载新值，返回是否真的挂载了。
// 已存在的键（多半是用户在页面上改过的 description / default）是权威数据，原样保留。
func toolSchemaAddIfAbsent(parent map[string]any, name string, value any) bool {
	if _, exists := parent[name]; exists {
		return false
	}
	parent[name] = value
	return true
}

func (s *Server) agentFindingTrafficAccess(ctx context.Context, id int64, write bool) error {
	if id <= 0 {
		return errors.New("finding_id 必须为独立漏洞记录 ID；不是探索节点 ID")
	}
	f, err := s.m.pg.GetFinding(id)
	if err != nil {
		return err
	}
	if f == nil {
		return fmt.Errorf("%w：finding_id=%d。证据工具使用独立漏洞记录 ID，请从 list_task_findings / get_task_node_detail 的 finding_id 字段读取；不要传 id / finding_node_id", db.ErrFindingNotFound, id)
	}
	if ri := agent.RunInfoFrom(ctx); ri.TaskID > 0 {
		task := s.m.ResolveTask(strconv.FormatInt(ri.TaskID, 10))
		if task == nil {
			return errors.New("任务不存在")
		}
		_, inherited, allowed := findingProvenanceInTask(task, f.TaskID)
		if !allowed {
			return errors.New("当前任务不可读取该漏洞")
		}
		if write && inherited {
			return errors.New("继承漏洞的流量证据只读，请到来源任务修改")
		}
	}
	return nil
}

func (s *Server) toolBindFindingTraffic() actool.CoreTool {
	return wrTool("bind_finding_traffic", "为已登记漏洞补绑经核实的真实 HTTP 流量。finding_id 使用独立漏洞记录 ID；不要传探索节点 ID。同批引用全部成功或全部失败，重复引用不覆盖已有说明。补绑会使已有报告标记待更新；不要为补包重新探测或重复创建漏洞。",
		objSchema(map[string]any{"finding_id": strParam("独立漏洞记录 ID，从 list_task_findings / get_task_node_detail 的 finding_id 字段读取"), "traffic_refs": agent.HintTrafficSchema()}, "finding_id", "traffic_refs"),
		func(ctx context.Context, raw json.RawMessage) (actool.Result, error) {
			if !s.m.pg.GetBool(settingAgentTrafficBinding, false) {
				return actool.Errorf("Agent 自动绑定流量已关闭；请在系统设置开启，或使用页面人工绑定。"), nil
			}
			var args struct {
				FindingID json.RawMessage `json:"finding_id"`
				Refs      []db.TrafficRef `json:"traffic_refs"`
			}
			if err := json.Unmarshal(raw, &args); err != nil {
				return actool.Errorf(err.Error()), nil
			}
			id := parseProfileID(args.FindingID)
			if err := s.agentFindingTrafficAccess(ctx, id, true); err != nil {
				return actool.Errorf(err.Error()), nil
			}
			if len(args.Refs) == 0 {
				return actool.Errorf("补绑需要至少一条已核实的 traffic_refs；无流量无需调用此工具"), nil
			}
			list, err := s.evidenceStore().Bind(ctx, id, args.Refs)
			if err != nil {
				return actool.Errorf(err.Error()), nil
			}
			return jsonResult(trafficSummary(list))
		})
}
