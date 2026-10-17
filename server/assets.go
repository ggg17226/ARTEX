package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Autumn-27/artex/db"
)

// companyScopeInputs accepts both the new [{kind,value}] contract and the
// historical ["example.com","203.0.113.10"] contract.
type companyScopeInputs []db.ScopeInput

const maxCompanyMutationBodyBytes = 2 << 20

func decodeCompanyMutationRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxCompanyMutationBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(value); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "请求正文过大")
		} else {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return false
	}
	return true
}

func (items *companyScopeInputs) UnmarshalJSON(data []byte) error {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return err
	}
	out := make([]db.ScopeInput, 0, len(rawItems))
	for i, raw := range rawItems {
		var legacy string
		if err := json.Unmarshal(raw, &legacy); err == nil {
			out = append(out, db.ScopeInput{Value: legacy})
			continue
		}
		var structured db.ScopeInput
		if err := json.Unmarshal(raw, &structured); err != nil {
			return fmt.Errorf("scope[%d] must be a string or {kind,value}", i)
		}
		out = append(out, structured)
	}
	*items = out
	return nil
}

// assetStore returns the asset store.
func (s *Server) assetStore() *db.AssetStore {
	if s.m.pg == nil {
		return nil
	}
	return s.m.pg.Assets()
}

// companyStore returns the company store.
func (s *Server) companyStore() *db.CompanyStore {
	if s.m.pg == nil {
		return nil
	}
	return s.m.pg.Companies()
}

// =====================================================================
// GET /api/companies
// =====================================================================

func (s *Server) listCompanies(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	companies, err := cs.ListCompanies()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, companies)
}

// =====================================================================
// POST /api/companies
// =====================================================================

func (s *Server) createCompany(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	var req struct {
		Name  string             `json:"name"`
		Logo  string             `json:"logo"`
		Scope companyScopeInputs `json:"scope"`
	}
	if !decodeCompanyMutationRequest(w, r, &req) {
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeErr(w, 400, "name required")
		return
	}
	if err := db.ValidateCompanyScopeInputBounds(req.Scope); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	id, added, skipped, invalid, scopeErrs, err := cs.CreateCompanyWithScope(req.Name, req.Logo, req.Scope, "api")
	if err != nil {
		if errors.Is(err, db.ErrCompanyNameConflict) {
			writeErr(w, http.StatusConflict, "企业名称已存在")
			return
		}
		var validationErr *db.CompanyScopeValidationError
		if errors.As(err, &validationErr) {
			writeErr(w, http.StatusBadRequest, validationErr.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := map[string]any{
		"id":            i64s(id),
		"created":       true,
		"scope_added":   added,
		"scope_skipped": skipped,
		"scope_invalid": invalid,
	}
	if len(scopeErrs) > 0 {
		out["scope_errors"] = scopeErrs
	}
	writeJSON(w, 201, out)
}

// =====================================================================
// GET /api/companies/{id}
// =====================================================================

func (s *Server) getCompany(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	// P03 E09：company 是实体 ID，严格 1..MaxInt64（0 / 负数 / 小数 / 溢出均 400）。
	id, err := parseIDField(r.PathValue("id"))
	if err != nil {
		writeErr(w, 400, "invalid id")
		return
	}
	c, err := cs.GetCompany(id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if c == nil {
		writeErr(w, 404, "company not found")
		return
	}
	scope, err := cs.GetScope(id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"company": c, "scope": scope})
}

// =====================================================================
// POST /api/companies/{id}/scope
// =====================================================================

func (s *Server) addCompanyScope(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	// P03 E09：company 是实体 ID，严格 1..MaxInt64。
	id, err := parseIDField(r.PathValue("id"))
	if err != nil {
		writeErr(w, 400, "invalid company id")
		return
	}
	var req struct {
		Scope  companyScopeInputs `json:"scope"`
		Reason string             `json:"reason"`
		Reset  bool               `json:"reset"` // if true, replace existing scope
	}
	if !decodeCompanyMutationRequest(w, r, &req) {
		return
	}
	if err := db.ValidateCompanyScopeInputBounds(req.Scope); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var added, skipped, invalid int
	var errs []string
	var mutationErr error
	if req.Reset {
		added, invalid, errs, mutationErr = cs.UpdateScopeInputsChecked(id, req.Scope, req.Reason)
	} else {
		added, skipped, invalid, errs, mutationErr = cs.AddScopeInputsChecked(id, req.Scope, req.Reason)
	}
	if mutationErr != nil {
		if errors.Is(mutationErr, db.ErrCompanyNotFound) {
			writeErr(w, http.StatusNotFound, "company not found")
			return
		}
		var validationErr *db.CompanyScopeValidationError
		if errors.As(mutationErr, &validationErr) {
			writeErr(w, http.StatusBadRequest, validationErr.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, mutationErr.Error())
		return
	}
	out := map[string]any{
		"added":   added,
		"skipped": skipped,
		"invalid": invalid,
	}
	if len(errs) > 0 {
		out["errors"] = errs
	}
	// Reported separately from errors: this is pre-existing bad data, not a fault
	// in the submitted rules, but the operator still has to see it or those assets
	// look like the IP/CIDR rules simply never match. A failure to build the
	// warning must not fail the scope write that already committed.
	if warning, err := cs.MalformedIPAssetWarning(); err == nil && warning != "" {
		out["warnings"] = []string{warning}
	}
	writeJSON(w, 200, out)
}

// =====================================================================
// DELETE /api/companies/{id}
// =====================================================================

func (s *Server) deleteCompany(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	// P03 E09：company 是实体 ID，严格 1..MaxInt64。
	id, err := parseIDField(r.PathValue("id"))
	if err != nil {
		writeErr(w, 400, "invalid id")
		return
	}
	var req struct {
		DeleteAssets bool `json:"delete_assets"`
	}
	// The body is optional. Only an actual empty body is ignored; malformed or
	// trailing JSON is a client error.
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		if !errors.Is(err, io.EOF) {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
	} else {
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				writeErr(w, http.StatusBadRequest, "invalid JSON: multiple values")
			} else {
				writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			}
			return
		}
	}

	assetsDeleted, err := s.m.DeleteCompanyWithAssets(id, req.DeleteAssets)
	if err != nil {
		if errors.Is(err, db.ErrCompanyNotFound) {
			writeErr(w, http.StatusNotFound, "company not found")
			return
		}
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": 1, "assets_deleted": assetsDeleted})
}

// =====================================================================
// POST /api/companies/reattribute
// =====================================================================

func (s *Server) reattribute(w http.ResponseWriter, r *http.Request) {
	cs := s.companyStore()
	if cs == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	if err := cs.RecomputeAttribution(); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// =====================================================================
// GET /api/assets
// =====================================================================

const (
	defaultAssetPageSize = 50
	maxAssetPageSize     = 200
)

// assetWire 是 GET /api/assets 的输出结构（P03 R08）：整体沿用 db.Asset（其
// id / company_id / task_source_node_id 已带 ,string），只用外层同名字段把
// task_ids 覆盖成 []string。
//
// db.Asset.TaskIDs 必须保持 []int64 —— 该字段由 assets.task_ids 列的
// json.Unmarshal（数字数组）回读，且 encoding/json 会忽略切片上的 ,string
// （既不生效也不报错），所以只能在 server 输出侧转换。外层同名字段按 encoding/json
// 的浅层优先规则屏蔽嵌入字段，与 W4b-1 的 saveMCPRequest 同一模式。
type assetWire struct {
	db.Asset
	TaskIDs []string `json:"task_ids"`
}

// assetWires 把一批资产转成 wire 形状（nil 元素跳过，输出顺序不变）。
func assetWires(in []*db.Asset) []assetWire {
	out := make([]assetWire, 0, len(in))
	for _, a := range in {
		if a == nil {
			continue
		}
		out = append(out, assetWire{Asset: *a, TaskIDs: taskIDStrings(a.TaskIDs)})
	}
	return out
}

// taskIDStrings 是保留 nil 语义的 []int64 → []string：db 侧为 nil 时 wire 仍是
// null，是空切片时仍是 []（idStrings 会把 nil 变成 []，这里不能沿用）。
func taskIDStrings(in []int64) []string {
	if in == nil {
		return nil
	}
	return idStrings(in)
}

func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	as := s.assetStore()
	if as == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	q := r.URL.Query()
	typ := q.Get("type")
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 {
		limit = defaultAssetPageSize
	} else if limit > maxAssetPageSize {
		limit = maxAssetPageSize
	}
	if offset < 0 {
		offset = 0
	}

	assets := []*db.Asset{}
	var err error
	// total is the full match count ignoring limit/offset. Count first so an offset
	// beyond the last row can return an empty page without an expensive scan.
	total := 0

	if dsl := q.Get("dsl"); dsl != "" {
		if err := db.ValidateDSL(dsl); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		// task_id scopes the DSL search to a task's assets (the task detail
		// "测试资产" search); 0 means the global asset view.
		// P03 E10：可选 query 过滤器走 lenient —— 非法 / 溢出视为未指定（0），
		// 不再把 >MaxInt64 的输入静默变成 MaxInt64 过滤值。
		taskID := queryFilterID(q.Get("task_id"))
		total, err = as.CountDSL(dsl, typ, taskID)
		if err == nil && offset < total {
			assets, err = as.QueryDSL(dsl, typ, taskID, limit, offset)
		}
	} else {
		companyID := queryFilterID(q.Get("company_id"))
		taskID := queryFilterID(q.Get("task_id"))
		switch {
		case companyID > 0:
			total, err = as.CountByCompany(companyID, typ)
			if err == nil && offset < total {
				assets, err = as.QueryByCompany(companyID, typ, limit, offset)
			}
		case taskID > 0:
			total, err = as.CountByTask(taskID, typ)
			if err == nil && offset < total {
				assets, err = as.QueryByTask(taskID, typ, limit, offset)
			}
		default:
			if typ == "" {
				typ = "root_domain"
			}
			total, err = as.CountByType(typ)
			if err == nil && offset < total {
				assets, err = as.QueryByType(typ, limit, offset)
			}
		}
	}

	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"count":  len(assets),
		"total":  total,
		"assets": assetWires(assets), // P03 R08：task_ids 覆盖成 []string
	})
}

// =====================================================================
// GET /api/assets/counts
// =====================================================================

func (s *Server) assetCounts(w http.ResponseWriter, r *http.Request) {
	as := s.assetStore()
	if as == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	var counts map[string]int
	var err error
	// P03 E10：可选 query 过滤器 lenient（非法 / 溢出 = 未指定，不是 MaxInt64）。
	if taskID := queryFilterID(r.URL.Query().Get("task_id")); taskID > 0 {
		counts, err = as.CountsByTypeForTask(taskID)
	} else {
		counts, err = as.CountsByType()
	}
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, counts)
}

// =====================================================================
// DELETE /api/assets
// =====================================================================

// deleteAssetsRequest 是 DELETE /api/assets 的请求体。
// P03：ids 在 wire 上是 string 数组（64 位 ID 一律 string），逐项经 parseIDField
// 严格解析后才转成内部 []int64；任一项非法即 400。
type deleteAssetsRequest struct {
	IDs []string `json:"ids"`
}

// parsedIDs 把 wire 的 string ID 列表解析为内部 []int64。
func (q deleteAssetsRequest) parsedIDs() ([]int64, error) {
	return parseIDListField(q.IDs)
}

func (s *Server) deleteAssets(w http.ResponseWriter, r *http.Request) {
	as := s.assetStore()
	if as == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	var req deleteAssetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if len(req.IDs) == 0 {
		writeErr(w, 400, "ids required")
		return
	}
	ids, err := req.parsedIDs()
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	deleted, err := as.DeleteByIDs(ids)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": deleted})
}

// =====================================================================
// POST /api/assets
// =====================================================================

// insertAssetInput 是 POST /api/assets 单条资产的入参（wire 形状与前端/外部录入方一致）。
type insertAssetInput struct {
	Type string `json:"type"`
	// root_domain / subdomain
	Domain      string   `json:"domain"`
	ICP         string   `json:"icp"`
	RecordType  string   `json:"record_type"`
	RecordValue []string `json:"record_value"`
	// ip
	IP           string           `json:"ip"`
	BoundDomains []string         `json:"bound_domains"`
	OpenPorts    []db.PortService `json:"open_ports"`
	// app
	AppName     string `json:"app_name"`
	BundleID    string `json:"bundle_id"`
	Category    string `json:"category"`
	Description string `json:"description"`
	AppICP      string `json:"app_icp"`
	// service http
	URL           string           `json:"url"`
	Technologies  []string         `json:"technologies"`
	StatusCode    *int             `json:"status_code"`
	ContentLength *int64           `json:"content_length"`
	PageTitle     string           `json:"page_title"`
	FaviconMMH3   string           `json:"favicon_mmh3"`
	Auth          []map[string]any `json:"auth"`
	ServiceIP     string           `json:"service_ip"`
	// service other
	Port        int    `json:"port"`
	ServiceName string `json:"service_name"`
	// endpoint
	Method string           `json:"method"`
	Params []map[string]any `json:"params"`
}

// insertAssetsRequest 是 POST /api/assets 的请求体。
// P03：task_id 在 wire 上是 string（64 位 ID 一律 string）；未提供或 "0" 保持
// 「不绑定任务」的 0 哨兵语义，其余经 parseIDField 严格解析后才是内部 int64。
type insertAssetsRequest struct {
	TaskID string             `json:"task_id"`
	Assets []insertAssetInput `json:"assets"`
}

// parsedTaskID 把 wire 的 string task_id 解析为内部 int64（""/"0" → 0）。
func (q insertAssetsRequest) parsedTaskID() (int64, error) {
	return parseOptionalIDString(q.TaskID)
}

func (s *Server) insertAssets(w http.ResponseWriter, r *http.Request) {
	as := s.assetStore()
	if as == nil {
		writeErr(w, 503, "database unavailable")
		return
	}
	var req insertAssetsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON: "+err.Error())
		return
	}
	taskID, err := req.parsedTaskID()
	if err != nil {
		writeErr(w, 400, "task_id "+err.Error())
		return
	}

	type result struct {
		Index int    `json:"index"`
		ID    string `json:"id"`
		Type  string `json:"type"`
	}
	type errEntry struct {
		Index int    `json:"index"`
		Error string `json:"error"`
	}
	var results []result
	var errs []errEntry

	for i, a := range req.Assets {
		var id int64
		var err error
		switch a.Type {
		case "root_domain":
			id, err = as.UpsertRootDomain(db.UpsertRootDomainReq{Domain: a.Domain, ICP: a.ICP, TaskID: taskID})
		case "ip":
			id, err = as.UpsertIP(db.UpsertIPReq{IP: a.IP, BoundDomains: a.BoundDomains, OpenPorts: a.OpenPorts, TaskID: taskID})
		case "subdomain":
			id, err = as.UpsertSubdomain(db.UpsertSubdomainReq{Domain: a.Domain, RecordType: a.RecordType, RecordValue: a.RecordValue, ICP: a.ICP, TaskID: taskID})
		case "app":
			id, err = as.UpsertApp(db.UpsertAppReq{Name: a.AppName, BundleID: a.BundleID, Category: a.Category, Description: a.Description, ICP: a.AppICP, TaskID: taskID})
		case "service":
			if a.URL != "" {
				svcIP := a.ServiceIP
				if svcIP == "" {
					svcIP = a.IP
				}
				id, err = as.UpsertHTTPService(db.UpsertHTTPServiceReq{URL: a.URL, Technologies: a.Technologies, StatusCode: a.StatusCode, ContentLength: a.ContentLength, PageTitle: a.PageTitle, FaviconMMH3: a.FaviconMMH3, Auth: a.Auth, IP: svcIP, TaskID: taskID})
			} else {
				id, err = as.UpsertOtherService(db.UpsertOtherServiceReq{Domain: a.Domain, IP: a.IP, Port: a.Port, ServiceName: a.ServiceName, Auth: a.Auth, TaskID: taskID})
			}
		case "endpoint":
			svcIP := a.ServiceIP
			if svcIP == "" {
				svcIP = a.IP
			}
			id, err = as.UpsertEndpoint(db.UpsertEndpointReq{URL: a.URL, Method: a.Method, Params: a.Params, IP: svcIP, TaskID: taskID})
		default:
			errs = append(errs, errEntry{Index: i, Error: "unknown type: " + a.Type})
			continue
		}
		if err != nil {
			errs = append(errs, errEntry{Index: i, Error: err.Error()})
			continue
		}
		if taskID > 0 {
			_ = as.SetTaskAssetSource(taskID, id, "api", "通过资产 API 登记", nil)
		}
		results = append(results, result{Index: i, ID: i64s(id), Type: a.Type})
	}
	writeJSON(w, 200, map[string]any{"results": results, "errors": errs})
}
