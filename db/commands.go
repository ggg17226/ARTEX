package db

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"
)

// CommandRecord is a paired tool_use + tool_result from the activity table
// (any tool, not just Bash). Command holds the raw tool input (JSON).
//
// P03 R15：id（activity 主键）与 exploration_id（任务主键）对外是 string。
type CommandRecord struct {
	ID        int64     `json:"id,string"`
	ExpID     int64     `json:"exploration_id,string"`
	Worker    string    `json:"worker"`
	Tool      string    `json:"tool"`
	Command   string    `json:"command"`
	Output    string    `json:"output"`
	IsError   bool      `json:"is_error"`
	CreatedAt time.Time `json:"created_at"`
}

// commandsLikePattern 把用户输入编成大小写不敏感的包含匹配模式：先转义 LIKE 元字符
// （`\ % _`），再整体小写以配合列侧的 LOWER(…)，与 PG 的 `ILIKE '%q%'` 等价。
func commandsLikePattern(q string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q)
	return strings.ToLower("%" + escaped + "%")
}

// commandFilter builds the WHERE clause shared by the tool-execution list and
// its per-tool tally, so the summary always describes exactly the rows the table
// pages through. Returns the clause and its args —— TiDB 的 `?` 没有编号，占位符
// 与参数严格同序，分页的 LIMIT/OFFSET 由调用方追加在末尾（因此这里只产出前缀）。
//
// PG 的 ILIKE 没有对应操作符，基线表又是 COLLATE=utf8mb4_bin（比较区分大小写），
// 所以两侧都显式 LOWER：列侧 LOWER(…)，模式在 Go 端预先小写，语义等价原 ILIKE。
func commandFilter(expID *int64, q string) (string, []any) {
	where := `WHERE u.kind = 'tool_use'`
	args := []any{}

	if expID != nil {
		where += ` AND u.exploration_id = ?`
		args = append(args, *expID)
	}
	if q != "" {
		where += ` AND (LOWER(COALESCE(u.tool, '')) LIKE ? ESCAPE '\\' OR LOWER(COALESCE(u.detail, '')) LIKE ? ESCAPE '\\')`
		pattern := commandsLikePattern(q)
		args = append(args, pattern, pattern)
	}
	return where, args
}

// ToolStat is one tool's execution tally for the usage summary.
type ToolStat struct {
	Tool   string `json:"tool"`
	Total  int    `json:"total"`
	Errors int    `json:"errors"`
}

// ToolStats counts executions grouped by tool under the same filters
// ListCommands takes. Unpaginated on purpose: the tally describes the whole
// filtered set, not the page currently on screen.
func (d *DB) ToolStats(expID *int64, q string) ([]ToolStat, error) {
	where, args := commandFilter(expID, q)

	// PG 的 `COUNT(*) FILTER (WHERE …)` 在 TiDB 没有对应语法，改写成
	// COUNT(CASE WHEN … THEN 1 END)：CASE 在没有匹配行时恒为数值 0，
	// 不会把 SQL NULL 灌进 Scan（is_error 是 TINYINT，0 即"非错误"）。
	rows, err := d.Query(`
SELECT COALESCE(NULLIF(u.tool,''),'-') AS tool, COUNT(*) AS total,
       COUNT(CASE WHEN COALESCE(r.is_error, 0) <> 0 THEN 1 END) AS errors
FROM activity u
LEFT JOIN activity r ON r.tool_use_id = u.tool_use_id AND r.kind = 'tool_result'
`+where+`
GROUP BY 1
ORDER BY total DESC, tool ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ToolStat{}
	for rows.Next() {
		var s ToolStat
		if err := rows.Scan(&s.Tool, &s.Total, &s.Errors); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListCommands returns tool executions (tool_use + paired tool_result) across all
// explorations, with optional filtering and pagination. Covers every tool, not
// just Bash; q matches the tool name or its input.
func (d *DB) ListCommands(expID *int64, q string, page, size int) ([]CommandRecord, int, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	offset := page * size

	where, args := commandFilter(expID, q)

	// count
	var total int
	countQ := `SELECT COUNT(*) FROM activity u ` + where
	if err := d.QueryRow(countQ, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// data query: join tool_use with its tool_result
	// is_error 是可空 TINYINT：COALESCE(…, 0) 保证左连接未命中的行扫进 bool 时是 false。
	dataQ := `
SELECT u.id, u.exploration_id, COALESCE(u.worker,''), COALESCE(u.tool,''), COALESCE(u.detail,''),
       COALESCE(r.detail,''), COALESCE(r.is_error, 0), u.created_at
FROM activity u
LEFT JOIN activity r ON r.tool_use_id = u.tool_use_id AND r.kind = 'tool_result'
` + where + `
ORDER BY u.id DESC
LIMIT ? OFFSET ?`

	args = append(args, size, offset)
	rows, err := d.Query(dataQ, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []CommandRecord{}
	for rows.Next() {
		var c CommandRecord
		if err := rows.Scan(&c.ID, &c.ExpID, &c.Worker, &c.Tool, &c.Command, &c.Output, &c.IsError, &c.CreatedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
	}
	return out, total, rows.Err()
}

// LLMRecord is one recorded LLM API call (request + response).
//
// P03 R16：id（llm_records 主键）对外是 string；TaskID / SessionID 本来就是 string。
type LLMRecord struct {
	ID           int64     `json:"id,string"`
	Ts           time.Time `json:"ts"`
	Model        string    `json:"model"`
	ProfileName  string    `json:"profile_name"`
	SessionID    string    `json:"session_id"`
	TaskID       string    `json:"task_id"`
	Worker       string    `json:"worker"`
	LatencyMs    int       `json:"latency_ms"`
	InputTokens  int       `json:"input_tokens"`
	OutputTokens int       `json:"output_tokens"`
	CacheRead    int       `json:"cache_read"`
	CacheWrite   int       `json:"cache_write"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	RequestBody  string    `json:"request_body,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
	// RawRequest / RawResponse are the untouched HTTP bodies exchanged with the
	// provider — the request as buildBody() sent it (full tool schemas included)
	// and the raw SSE frames. RequestBody/ResponseBody above are the normalized
	// view, which drops tool schemas and tool_use blocks entirely. Empty for
	// records written before this was added, or when the call never reached HTTP.
	RawRequest  string `json:"raw_request,omitempty"`
	RawResponse string `json:"raw_response,omitempty"`
}

// llm_records 的建表与列演进**不再由运行期 DDL 负责**（M12）：
// 该表是基线目录里的第 50 张扩展表（schema.ExtensionTables §50，含
// idx_llm_records_ts / idx_llm_records_session 两个索引，以及旧的
// llmRecordsMigrate 逐列 ADD COLUMN 想补的 task_id/worker/profile_name/
// raw_request/raw_response），启动路径由 db.Open → schema.VerifyBaseline
// 只读校验，缺表/缺列一律 fail closed 拒绝启动。旧的 llmRecordsSchema /
// llmRecordsMigrate 两份 DDL 文本已一并删除：运行期偷偷建出一张与目录不一致的表，
// 只会让后续每一处读写都在错误的物理形状上失败。

// EnsureLLMRecordsTable 已废弃（Deprecated），恒返回 nil，且**不再执行任何 DDL**。
//
// 原有的 `CREATE TABLE IF NOT EXISTS llm_records` 与 5 条 ADD COLUMN 已按 M12
// 彻底废除（表结构由 65 表基线目录与 schema.VerifyBaseline 保障）。这里只剩下一个
// 空实现，唯一的原因是 server/manager.go 与 server/db 侧若干测试 fixture 仍在
// 启动路径上调用它，而这些文件的删改不在本次改造清单内 —— 调用点清理完成后
// （见 .superpowers/sdd/P08-03/task-3.3-report.md 的越界清单）本函数应整体删除。
func (d *DB) EnsureLLMRecordsTable() error { return nil }

// InsertLLMRecord stores one LLM call record using TiDB dialect.
// 若关联有效任务 ID，接入任务写者门禁协议核验父任务存活，严禁向正在被删除的任务并发写入记录（F1/R3）。
func (d *DB) InsertLLMRecord(r *LLMRecord) error {
	taskIDStr := strings.TrimSpace(r.TaskID)
	if taskIDStr != "" {
		if tid, parseErr := strconv.ParseInt(taskIDStr, 10, 64); parseErr == nil && tid > 0 {
			return d.runTaskChildWriter(tid, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
INSERT INTO llm_records(model, profile_name, session_id, task_id, worker, latency_ms, input_tokens, output_tokens, cache_read, cache_write, status, error, request_body, response_body, raw_request, raw_response)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
					r.Model, nullIfEmpty(r.ProfileName), r.SessionID, nullIfEmpty(r.TaskID), nullIfEmpty(r.Worker),
					r.LatencyMs, r.InputTokens, r.OutputTokens, r.CacheRead, r.CacheWrite,
					r.Status, nullIfEmpty(r.Error), nullIfEmpty(r.RequestBody), nullIfEmpty(r.ResponseBody),
					nullIfEmpty(r.RawRequest), nullIfEmpty(r.RawResponse))
				return err
			})
		}
	}

	_, err := d.Exec(`
INSERT INTO llm_records(model, profile_name, session_id, task_id, worker, latency_ms, input_tokens, output_tokens, cache_read, cache_write, status, error, request_body, response_body, raw_request, raw_response)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.Model, nullIfEmpty(r.ProfileName), r.SessionID, nullIfEmpty(r.TaskID), nullIfEmpty(r.Worker),
		r.LatencyMs, r.InputTokens, r.OutputTokens, r.CacheRead, r.CacheWrite,
		r.Status, nullIfEmpty(r.Error), nullIfEmpty(r.RequestBody), nullIfEmpty(r.ResponseBody),
		nullIfEmpty(r.RawRequest), nullIfEmpty(r.RawResponse))
	return err
}

// ListLLMRecords returns paginated LLM records with optional filters.
func (d *DB) ListLLMRecords(model, session, task string, page, size int) ([]LLMRecord, int, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	offset := page * size

	where := `WHERE 1`
	args := []any{}
	if model != "" {
		where += ` AND model = ?`
		args = append(args, model)
	}
	if session != "" {
		// ILIKE → LOWER(col) LIKE ?：列是 utf8mb4_bin 的 MEDIUMTEXT，裸 LIKE 大小写敏感，
		// 模式在 Go 端预先小写；元字符先转义再折叠，`%`/`_` 不会变成通配。
		where += ` AND LOWER(session_id) LIKE ? ESCAPE '\\'`
		args = append(args, commandsLikePattern(session))
	}
	if task != "" {
		where += ` AND COALESCE(task_id,'') = ?`
		args = append(args, task)
	}

	var total int
	if err := d.QueryRow(`SELECT COUNT(*) FROM llm_records `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	dataQ := `SELECT id, ts, COALESCE(model,''), COALESCE(profile_name,''), COALESCE(session_id,''), COALESCE(task_id,''), COALESCE(worker,''),
		COALESCE(latency_ms,0), COALESCE(input_tokens,0), COALESCE(output_tokens,0), COALESCE(cache_read,0), COALESCE(cache_write,0),
		COALESCE(status,''), COALESCE(error,'')
		FROM llm_records ` + where + ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, size, offset)

	rows, err := d.Query(dataQ, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []LLMRecord{}
	for rows.Next() {
		var r LLMRecord
		if err := rows.Scan(&r.ID, &r.Ts, &r.Model, &r.ProfileName, &r.SessionID, &r.TaskID, &r.Worker, &r.LatencyMs,
			&r.InputTokens, &r.OutputTokens, &r.CacheRead, &r.CacheWrite, &r.Status, &r.Error); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

// ModelTokenStat is one model's aggregated token usage for a task, summed from the
// llm_usage metering ledger (see db/llm_usage.go). Calls is the number of LLM calls
// that hit this model.
type ModelTokenStat struct {
	Model            string `json:"model"`
	Calls            int    `json:"calls"`
	InputTokens      int    `json:"input_tokens"`
	OutputTokens     int    `json:"output_tokens"`
	CacheReadTokens  int    `json:"cache_read_tokens"`
	CacheWriteTokens int    `json:"cache_write_tokens"`
}

// LLMTask is one distinct task with its LLM-record count.
type LLMTask struct {
	TaskID string `json:"task_id"`
	Count  int    `json:"count"`
}

// LLMTasks returns distinct non-empty task_ids with record counts, most recent
// first — powers the LLM-records page's task picker.
func (d *DB) LLMTasks() ([]LLMTask, error) {
	rows, err := d.Query(`SELECT task_id, COUNT(*) AS n FROM llm_records
WHERE COALESCE(task_id,'') <> '' GROUP BY task_id ORDER BY MAX(id) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LLMTask
	for rows.Next() {
		var t LLMTask
		if err := rows.Scan(&t.TaskID, &t.Count); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// DeleteLLMRecords removes every LLM record for one exact task_id — the same
// match the page's task picker/filter uses. Returns rows deleted.
func (d *DB) DeleteLLMRecords(task string) (int64, error) {
	res, err := d.Exec(`DELETE FROM llm_records WHERE COALESCE(task_id,'') = ?`, task)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// GetLLMRecord returns a single LLM record with full request/response bodies.
func (d *DB) GetLLMRecord(id int64) (*LLMRecord, error) {
	var r LLMRecord
	var reqBody, respBody, rawReq, rawResp sql.NullString
	err := d.QueryRow(`SELECT id, ts, COALESCE(model,''), COALESCE(profile_name,''), COALESCE(session_id,''), COALESCE(task_id,''), COALESCE(worker,''),
		COALESCE(latency_ms,0), COALESCE(input_tokens,0), COALESCE(output_tokens,0), COALESCE(cache_read,0), COALESCE(cache_write,0),
		COALESCE(status,''), COALESCE(error,''), request_body, response_body, raw_request, raw_response
		FROM llm_records WHERE id=?`, id).
		Scan(&r.ID, &r.Ts, &r.Model, &r.ProfileName, &r.SessionID, &r.TaskID, &r.Worker, &r.LatencyMs,
			&r.InputTokens, &r.OutputTokens, &r.CacheRead, &r.CacheWrite, &r.Status, &r.Error,
			&reqBody, &respBody, &rawReq, &rawResp)
	if err != nil {
		return nil, err
	}
	r.RequestBody = reqBody.String
	r.ResponseBody = respBody.String
	r.RawRequest = rawReq.String
	r.RawResponse = rawResp.String
	return &r, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
