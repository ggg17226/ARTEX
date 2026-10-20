package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ChatMention is a lightweight search result. Details are read again on send.
//
// P03 R25：id 是 64 位实体 ID（finding / company / asset / …），对外 string。
// 内部 chatMentionCursor 是另一类型：它被 Marshal 后 base64 成不透明的
// next_cursor 字符串，不是前端数字字段，故保持原样。
type ChatMention struct {
	Kind        string `json:"kind"`
	ID          int64  `json:"id,string"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

type ChatMentionPage struct {
	Items      []ChatMention `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

var ErrInvalidChatMentionCursor = errors.New("分页位置无效，请重新搜索")

type chatMentionCursor struct {
	ID    int64  `json:"id"`
	Kind  string `json:"kind"`
	Exact bool   `json:"exact"`
	Scope string `json:"scope"`
	Query string `json:"query"`
}

func ValidChatMentionKind(kind string) bool {
	switch kind {
	case "finding", "company", "asset", "endpoint", "ip", "app", "root_domain", "subdomain", "service":
		return true
	}
	return false
}

// SearchChatMentions searches the shared catalog, just like the asset/finding
// pages. Values remain SQL parameters; %, _ and backslash are literal search text.
func (d *DB) SearchChatMentions(ctx context.Context, kind, query string) ([]ChatMention, error) {
	page, err := d.SearchChatMentionsPage(ctx, kind, query, "")
	return page.Items, err
}

// chatMentionMarkerRe 匹配 SQL 模板里的具名参数标记 {{name}}。
var chatMentionMarkerRe = regexp.MustCompile(`\{\{([a-zA-Z]+)\}\}`)

// expandChatMentionQuery 把模板里的 {{name}} 按**文本出现顺序**逐个替换成 `?`，并同步
// 产出等长的实参数组。
//
// 这段三段 UNION 搜索里，同一批参数（kind / query / pattern / cursor id / cursor kind
// / exact 门）要重复出现几十次；PostgreSQL 用 $1..$6 的序号占位符天然可以重复引用，
// 而 MySQL/TiDB 的 `?` 是一次性的，必须逐处重复绑定。手写这两份清单正是本次迁移里
// "占位符与参数错位" 最常见的地方，因此把"绑定顺序 = 文本顺序"交给构造保证，
// 再由语句级测试（? 个数 == 实参个数、逐位对齐）兜底。
func expandChatMentionQuery(template string, params map[string]any) (string, []any, error) {
	var args []any
	var missing []string
	sqlText := chatMentionMarkerRe.ReplaceAllStringFunc(template, func(marker string) string {
		name := chatMentionMarkerRe.FindStringSubmatch(marker)[1]
		value, ok := params[name]
		if !ok {
			missing = append(missing, name)
			return "?"
		}
		args = append(args, value)
		return "?"
	})
	if len(missing) > 0 {
		return "", nil, fmt.Errorf("chatMentions: SQL 模板缺少参数 %s", strings.Join(missing, ", "))
	}
	return sqlText, args, nil
}

// chatMentionSearchSQL 是目录搜索的 TiDB 版模板（M：PostgreSQL 方言清零）。
//
// 逐条口径：
//   - `id::text = $2` → `CAST(id AS CHAR) = ?`：ID 精确命中判定必须在文本域里做，
//     与前端传来的字符串比较，避免把非数字输入硬转成数字；
//   - `expr ILIKE $3` → `LOWER(expr) LIKE ? ESCAPE '\\'`：本库表是 utf8mb4_bin，
//     LIKE 默认大小写**敏感**，所以必须显式 LOWER 两侧才等价于 ILIKE；模式在 Go 侧
//     预小写并把 \ % _ 转义成字面量（ESCAPE 子句保证用户搜 "100%" 不会变成通配）。
//     代价是 LOWER() 包列走不了索引 —— 引用补全是每标签页 20 行的小结果集搜索，
//     且 ID 精确命中那条分支不受影响，可接受；
//   - `'资产 #'||id::text` → `CONCAT('资产 #', CAST(id AS CHAR))`：`||` 在 MySQL 里是
//     逻辑或，绝不能保留；
//   - `port::text` → `CAST(port AS CHAR)`；
//   - `($4::bigint=0 OR …)` → `(? = 0 OR …)`：游标为 0 表示"取第一页"，参数本就是
//     整数，转型是 PG 侧的类型标注，直接删掉；
//   - 布尔门 `(id::text=$2) < $6` 保留原结构：MySQL 的布尔表达式取 0/1，与 PG 的
//     boolean 在数值比较下等价（实参 after.Exact 由驱动绑成 1/0）。
const chatMentionSearchSQL = `
SELECT kind, id, LEFT(label, 160), LEFT(description, 240) FROM (
 (SELECT 'finding' AS kind, id, COALESCE(NULLIF(name,''), vulnclass) AS label,
         concat_ws(' · ', severity, status, LEFT(summary, 160)) AS description
  FROM findings WHERE ({{kind}}='' OR {{kind}}='finding') AND
    ({{query}}='' OR CAST(id AS CHAR)={{query}} OR LOWER(concat_ws(' ',name,vulnclass,summary)) LIKE {{pattern}} ESCAPE '\\')
    AND ({{cursorID}}=0 OR (CAST(id AS CHAR)={{query}})<{{exact}} OR ((CAST(id AS CHAR)={{query}})={{exact}} AND (id<{{cursorID}} OR (id={{cursorID}} AND 'finding'>{{cursorKind}}))))
  ORDER BY (CAST(id AS CHAR)={{query}}) DESC, id DESC LIMIT 21)
 UNION ALL
 (SELECT 'company', id, name, nkey FROM companies
  WHERE ({{kind}}='' OR {{kind}}='company') AND ({{query}}='' OR CAST(id AS CHAR)={{query}} OR LOWER(name) LIKE {{pattern}} ESCAPE '\\' OR LOWER(nkey) LIKE {{pattern}} ESCAPE '\\')
    AND ({{cursorID}}=0 OR (CAST(id AS CHAR)={{query}})<{{exact}} OR ((CAST(id AS CHAR)={{query}})={{exact}} AND (id<{{cursorID}} OR (id={{cursorID}} AND 'company'>{{cursorKind}}))))
  ORDER BY (CAST(id AS CHAR)={{query}}) DESC, id DESC LIMIT 21)
 UNION ALL
 (SELECT type, id,
    CASE WHEN type='endpoint' THEN concat_ws(' ',NULLIF(method,''),url)
         ELSE COALESCE(NULLIF(app_name,''),NULLIF(url,''),NULLIF(domain,''),NULLIF(ip,''),NULLIF(bundle_id,''),CONCAT('资产 #',CAST(id AS CHAR))) END,
    concat_ws(' · ',type,NULLIF(page_title,''),NULLIF(service_name,''),NULLIF(bundle_id,''),NULLIF(ip,''),CAST(port AS CHAR))
  FROM assets WHERE ({{kind}}='' OR {{kind}}='asset' OR type={{kind}}) AND
    ({{query}}='' OR CAST(id AS CHAR)={{query}} OR LOWER(concat_ws(' ',domain,root_domain,ip,url,app_name,bundle_id,page_title,service_name,method)) LIKE {{pattern}} ESCAPE '\\')
    AND ({{cursorID}}=0 OR (CAST(id AS CHAR)={{query}})<{{exact}} OR ((CAST(id AS CHAR)={{query}})={{exact}} AND (id<{{cursorID}} OR (id={{cursorID}} AND type>{{cursorKind}}))))
  ORDER BY (CAST(id AS CHAR)={{query}}) DESC, id DESC LIMIT 21)
) matches ORDER BY (CAST(id AS CHAR)={{query}}) DESC, id DESC, kind LIMIT 21`

// SearchChatMentionsPage uses the last result's stable sort key rather than an
// offset, so loading later pages does not repeatedly skip all earlier results.
func (d *DB) SearchChatMentionsPage(ctx context.Context, kind, query, cursor string) (ChatMentionPage, error) {
	page := ChatMentionPage{Items: make([]ChatMention, 0)}
	if kind != "" && !ValidChatMentionKind(kind) {
		return page, fmt.Errorf("不支持的引用类型")
	}
	var after chatMentionCursor
	if cursor != "" {
		if len(cursor) > 2048 {
			return page, ErrInvalidChatMentionCursor
		}
		blob, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(blob, &after) != nil || after.ID <= 0 ||
			!ValidChatMentionKind(after.Kind) || after.Scope != kind || after.Query != query {
			return page, ErrInvalidChatMentionCursor
		}
	}
	// 转义先行，再整体小写：先小写会让 %/_ 的转义符与大写形态错配，顺序不可颠倒。
	pattern := "%" + strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(query) + "%"
	pattern = strings.ToLower(pattern)
	sqlText, args, err := expandChatMentionQuery(chatMentionSearchSQL, map[string]any{
		"kind":       kind,
		"query":      query,
		"pattern":    pattern,
		"cursorID":   after.ID,
		"cursorKind": after.Kind,
		"exact":      after.Exact,
	})
	if err != nil {
		return page, err
	}
	rows, err := d.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ChatMention
		if err := rows.Scan(&item.Kind, &item.ID, &item.Label, &item.Description); err != nil {
			return page, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if len(page.Items) > 20 {
		page.Items = page.Items[:20]
		last := page.Items[19]
		blob, _ := json.Marshal(chatMentionCursor{last.ID, last.Kind, fmt.Sprint(last.ID) == query, kind, query})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(blob)
	}
	return page, nil
}
