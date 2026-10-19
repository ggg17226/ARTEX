package db

// =====================================================================
// 任务上下文范围 / 覆盖度读取视图（P08-02 Task 2.3，TiDB 方言）
//
// 只读视图，不改数据；方言要点与 task_scope.go 一致：
//   * `$1` → `?`（本文件所有 CTE 的占位符顺序都在注释里写死，绑定必须同序）；
//   * `ts.net >>= try_inet(a.ip)` → Go 侧 netip 前缀区间 + INET_ATON/INET6_ATON
//     参数绑定范围比较（`scopeNetPredicate`）；
//   * `ctx.task_id = ANY(a.task_ids)` → 逐 task_id 的 `JSON_CONTAINS(a.task_ids, CAST(? AS JSON))`
//     OR 串（assets.task_ids 是 JSON 文本，无数组包含运算符）；
//   * `count(*) FILTER (WHERE …)` → `COALESCE(SUM(CASE WHEN … THEN 1 ELSE 0 END), 0)`；
//   * `regexp_replace(…, '[[:space:]]+', '', 'g')` → REPLACE 链（TiDB 无 REGEXP_REPLACE）。
// =====================================================================

import (
	"net/url"
	"sort"
	"strings"
)

// directTaskContextCTE is the current task plus exactly its explicitly related
// source tasks. It is deliberately non-recursive.
// 占位符：[task_id, task_id]。
const directTaskContextCTE = `
context_tasks AS (
  SELECT t.id AS task_id, t.exploration_id
  FROM tasks t
  WHERE t.id=? AND t.deleted_at IS NULL
  UNION ALL
  SELECT source.id, source.exploration_id
  FROM task_relations relation
  JOIN tasks source ON source.id=relation.source_task_id AND source.deleted_at IS NULL
  WHERE relation.task_id=?
)`

// contextScopeKindsCTE 是"SQL 可判定"的范围分支（网段分支由 Go 编译成地址区间）。
// 占位符紧跟 directTaskContextCTE：[net 区间参数…]。
func contextCoverageCTE(netPred string) string {
	return directTaskContextCTE + `,
target AS (
  SELECT DISTINCT a.id, a.type,
         COALESCE(a.url, a.domain, a.ip, a.app_name, a.root_domain, '') AS label
  FROM assets a
  JOIN task_scope ts ON (
        (ts.kind='company'     AND a.company_id = ts.company_id)
      OR (ts.kind='root_domain' AND a.root_domain = ts.domain)
      OR (ts.kind='subdomain'   AND a.domain = ts.domain)
      OR (ts.kind='icp' AND (
           ` + icpMatchSQL("a.icp") + ` = ts.value
           OR ` + icpMatchSQL("a.app_icp") + ` = ts.value
          ))
  )
  JOIN context_tasks ctx ON ctx.task_id=ts.task_id
  UNION
  SELECT DISTINCT a.id, a.type,
         COALESCE(a.url, a.domain, a.ip, a.app_name, a.root_domain, '') AS label
  FROM assets a
  WHERE ` + netPred + `
  UNION
  SELECT a.id, a.type,
         COALESCE(a.url, a.domain, a.ip, a.app_name, a.root_domain, '') AS label
  FROM assets a
  JOIN exploration_anchors ea ON ea.asset_id=a.id
  JOIN exploration_nodes en ON en.id=ea.node_id
  JOIN context_tasks ctx ON ctx.exploration_id=en.exploration_id
),
tested AS (
  SELECT DISTINCT ea.asset_id
  FROM exploration_anchors ea
  JOIN exploration_nodes en ON en.id=ea.node_id AND en.kind='fact'
  JOIN context_tasks ctx ON ctx.exploration_id=en.exploration_id
)`
}

// scopeTargetCTE selects every asset that BELONGS to the current task's (and its
// direct source tasks') declared scope — membership, not literal value: a
// root_domain scope pulls in every subdomain / service / endpoint whose own
// root_domain column equals it; an ip/cidr scope pulls in assets whose ip OR
// IP-literal host falls inside the net. Unlike contextCoverageCTE it carries
// neither the fact-anchor union nor the tested set — it is pure "in declared
// scope", independent of what has already been touched. Used by the agent's
// list_assets so a query returns the task's relevant assets, not the whole shared 库.
// 占位符：[task_id, task_id, net 区间参数…]。
func scopeTargetCTE(netPred string) string {
	return `
context_tasks AS (
  SELECT t.id AS task_id
  FROM tasks t
  WHERE t.id=? AND t.deleted_at IS NULL
  UNION ALL
  SELECT source.id
  FROM task_relations relation
  JOIN tasks source ON source.id=relation.source_task_id AND source.deleted_at IS NULL
  WHERE relation.task_id=?
),
target AS (
  SELECT DISTINCT a.id
  FROM assets a
  JOIN task_scope ts ON (
        (ts.kind='company'     AND a.company_id = ts.company_id)
      OR (ts.kind='root_domain' AND a.root_domain = ts.domain)
      OR (ts.kind='subdomain'   AND a.domain = ts.domain)
      OR (ts.kind='icp' AND (
           ` + icpMatchSQL("a.icp") + ` = ts.value
           OR ` + icpMatchSQL("a.app_icp") + ` = ts.value
          ))
  )
  JOIN context_tasks ctx ON ctx.task_id=ts.task_id
  UNION
  SELECT DISTINCT a.id
  FROM assets a
  WHERE ` + netPred + `
)`
}

// icpMatchSQL 是 ICP 备案号的归一化表达式：小写 + 去空白。
// PG 用 regexp_replace(..., '[[:space:]]+', ”, 'g')；TiDB 没有 REGEXP_REPLACE，
// 这里用 REPLACE 链覆盖实际出现的空白（空格 / 制表 / 换行 / 回车）。
func icpMatchSQL(col string) string {
	return "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(" + col +
		", ''), ' ', ''), '\\t', ''), '\\n', ''), '\\r', ''))"
}

// contextTaskIDs 解析任务及其**存活**直接源任务的 id 集合（非递归，与 CTE 同口径）。
// 网段分支的谓词要先知道这批任务有哪些范围行，才能编译成地址区间。
func (s *AssetStore) contextTaskIDs(taskID int64) ([]int64, error) {
	rows, err := s.db.Query(`
SELECT t.id FROM tasks t
WHERE (t.id=? OR t.id IN (SELECT relation.source_task_id FROM task_relations relation WHERE relation.task_id=?))
  AND t.deleted_at IS NULL`, taskID, taskID)
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

// taskIDsJSONPredicates 把"上下文任务集合"编译成 assets.task_ids 的包含谓词。
// TiDB 没有 `=ANY(array)`；JSON_CONTAINS 逐个 OR 是最直白且只用基础 JSON 函数的写法。
func taskIDsJSONPredicates(taskIDs []int64, column string) (string, []any) {
	if len(taskIDs) == 0 {
		return "0", nil
	}
	col := "COALESCE(" + column + ", '[]')"
	preds := make([]string, 0, len(taskIDs))
	args := make([]any, 0, len(taskIDs))
	for _, id := range taskIDs {
		preds = append(preds, assetJSONContains(col, "?"))
		args = append(args, id)
	}
	return "(" + strings.Join(preds, " OR ") + ")", args
}

// ListTaskScopeWithSources returns the current task's scope followed by the
// scopes of its direct source tasks. TaskScope.TaskID preserves provenance.
func (s *AssetStore) ListTaskScopeWithSources(taskID int64) ([]TaskScope, error) {
	rows, err := s.db.Query(`WITH `+directTaskContextCTE+`
SELECT ts.id, ts.task_id, ts.kind, COALESCE(ts.company_id,0), COALESCE(c.name,''), COALESCE(ts.domain,''),
       COALESCE(ts.net,''), COALESCE(ts.value,''), ts.source, COALESCE(ts.reason,'')
FROM task_scope ts
JOIN context_tasks ctx ON ctx.task_id=ts.task_id
LEFT JOIN companies c ON c.id=ts.company_id
ORDER BY CASE WHEN ts.task_id=? THEN 0 ELSE 1 END, ts.id`, taskID, taskID, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskScope{}
	for rows.Next() {
		var scope TaskScope
		var companyID int64
		if err := rows.Scan(&scope.ID, &scope.TaskID, &scope.Kind, &companyID, &scope.CompanyName, &scope.Domain, &scope.Net, &scope.Value, &scope.Source, &scope.Reason); err != nil {
			return nil, err
		}
		if companyID > 0 {
			scope.CompanyID = &companyID
		}
		out = append(out, scope)
	}
	return out, rows.Err()
}

// TaskCoverageWithSources computes one coverage view over the union of the
// current task and its direct sources: source scopes and anchored assets extend
// the denominator, while fact anchors count as tested. No row is copied.
func (s *AssetStore) TaskCoverageWithSources(taskID int64) (*Coverage, error) {
	cov := &Coverage{ByType: []CoverageByType{}}
	ctxIDs, err := s.contextTaskIDs(taskID)
	if err != nil {
		return nil, err
	}
	_ = s.db.QueryRow(`WITH `+directTaskContextCTE+`
SELECT count(*) FROM task_scope ts JOIN context_tasks ctx ON ctx.task_id=ts.task_id`, taskID, taskID).Scan(&cov.ScopeRows)

	netPred, netArgs, err := s.scopeNetPredicate(ctxIDs, []string{"a.ip"})
	if err != nil {
		return nil, err
	}
	args := append([]any{taskID, taskID}, netArgs...)
	rows, err := s.db.Query(`WITH `+contextCoverageCTE(netPred)+`
SELECT target.type, count(*) AS total,
       COALESCE(SUM(CASE WHEN target.id IN (SELECT asset_id FROM tested) THEN 1 ELSE 0 END), 0) AS tested
FROM target GROUP BY target.type ORDER BY target.type`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var byType CoverageByType
		if err := rows.Scan(&byType.Type, &byType.Total, &byType.Tested); err != nil {
			return nil, err
		}
		cov.ByType = append(cov.ByType, byType)
		cov.Denominator += byType.Total
		cov.Tested += byType.Tested
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if cov.Denominator > 0 {
		pct := float64(cov.Tested) / float64(cov.Denominator)
		cov.Pct = &pct
	}
	return cov, nil
}

// ListUntestedAssetsWithSources is the direct-source-aware backlog query used
// by inherited tasks. Source fact anchors remove assets from the backlog.
func (s *AssetStore) ListUntestedAssetsWithSources(taskID int64, typ string, limit, offset int) ([]CoverageAsset, int, error) {
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}
	ctxIDs, err := s.contextTaskIDs(taskID)
	if err != nil {
		return nil, 0, err
	}
	netPred, netArgs, err := s.scopeNetPredicate(ctxIDs, []string{"a.ip"})
	if err != nil {
		return nil, 0, err
	}
	args := append([]any{taskID, taskID}, netArgs...)
	if typ != "" {
		args = append(args, typ)
	}
	var total int
	if err := s.db.QueryRow(`WITH `+contextCoverageCTE(netPred)+`
SELECT count(*) FROM target WHERE target.id NOT IN (SELECT asset_id FROM tested)`+typFilter(typ), args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	pageArgs := append(append([]any{}, args...), limit, offset)
	rows, err := s.db.Query(`WITH `+contextCoverageCTE(netPred)+`
SELECT target.id, target.type, target.label FROM target
WHERE target.id NOT IN (SELECT asset_id FROM tested)`+typFilter(typ)+`
ORDER BY target.id LIMIT ? OFFSET ?`, pageArgs...)
	if err != nil {
		return nil, total, err
	}
	defer rows.Close()
	out := []CoverageAsset{}
	for rows.Next() {
		var asset CoverageAsset
		if err := rows.Scan(&asset.ID, &asset.Type, &asset.Label); err != nil {
			return nil, total, err
		}
		out = append(out, asset)
	}
	return out, total, rows.Err()
}

// typFilter 是可选资产类型过滤（占位符固定为 1 个 `?`，绑定顺序由调用方保证在最后）。
func typFilter(typ string) string {
	if typ == "" {
		return ""
	}
	return " AND target.type = ?"
}

// HostsByTaskWithSources resolves exact HTTP host candidates from assets that
// are attached to, anchored by, or in scope for the current task or a direct
// source. Traffic remains global and is not copied. This read helper must not be
// used for destructive task cleanup; HostsByTask intentionally retains that
// narrower, task-owned behavior.
func (s *AssetStore) HostsByTaskWithSources(taskID int64) ([]string, error) {
	ctxIDs, err := s.contextTaskIDs(taskID)
	if err != nil {
		return nil, err
	}
	netPred, netArgs, err := s.scopeNetPredicate(ctxIDs, []string{"a.ip"})
	if err != nil {
		return nil, err
	}
	jsonPred, jsonArgs := taskIDsJSONPredicates(ctxIDs, "a.task_ids")
	// 绑定顺序 = CTE 内占位符出现顺序：context_tasks(2) → task_ids 包含谓词 → 网段区间。
	args := append([]any{taskID, taskID}, jsonArgs...)
	args = append(args, netArgs...)

	rows, err := s.db.Query(`WITH `+directTaskContextCTE+`,
context_assets AS (
  SELECT DISTINCT a.id
  FROM assets a
  WHERE `+jsonPred+`
  UNION
  SELECT ea.asset_id
  FROM exploration_anchors ea
  JOIN exploration_nodes en ON en.id=ea.node_id
  JOIN context_tasks ctx ON ctx.exploration_id=en.exploration_id
  UNION
  SELECT DISTINCT a.id
  FROM assets a
  JOIN task_scope ts ON (
        (ts.kind='company'     AND a.company_id=ts.company_id)
      OR (ts.kind='root_domain' AND a.root_domain=ts.domain)
      OR (ts.kind='subdomain'   AND a.domain=ts.domain)
      OR (ts.kind='icp' AND (
           `+icpMatchSQL("a.icp")+` = ts.value
           OR `+icpMatchSQL("a.app_icp")+` = ts.value
          ))
  )
  JOIN context_tasks ctx ON ctx.task_id=ts.task_id
  UNION
  SELECT DISTINCT a.id
  FROM assets a
  WHERE `+netPred+`
)
SELECT COALESCE(a.domain,''), COALESCE(a.ip,''), COALESCE(a.url,'')
FROM assets a JOIN context_assets ctx ON ctx.id=a.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hosts := map[string]struct{}{}
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
		if parsed, err := url.Parse(rawURL); err == nil {
			add(parsed.Hostname())
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

// scopeTargetAssetIDs 是 scopeTargetCTE 的独立入口（agent 的 list_assets 走这里）。
func (s *AssetStore) scopeTargetCTEFor(taskID int64) (string, []any, error) {
	ctxIDs, err := s.contextTaskIDs(taskID)
	if err != nil {
		return "", nil, err
	}
	netPred, netArgs, err := s.scopeNetPredicate(ctxIDs, []string{"a.ip", "a.domain"})
	if err != nil {
		return "", nil, err
	}
	return scopeTargetCTE(netPred), append([]any{taskID, taskID}, netArgs...), nil
}
