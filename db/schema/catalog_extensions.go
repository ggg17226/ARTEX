package schema

// HashIdentityColumns maps the 13 single-key hash DDL tables to their
// canonical identity/hash column names (branch tables assets/company_scope
// and COALESCE tables task_scope/side_question_requests are covered by their
// per-branch or identity columns in the catalog; together with those 4 tables,
// there are 17 hash DDL tables in total).
var HashIdentityColumns = map[string][2]string{
	"companies":              {"nkey_identity", "nkey_hash"},
	"task_categories":        {"nkey_identity", "nkey_hash"},
	"task_templates":         {"nkey_identity", "nkey_hash"},
	"agents":                 {"key_identity", "key_hash"},
	"mcp_servers":            {"name_identity", "name_hash"},
	"settings":               {"key_identity", "key_hash"},
	"tools":                  {"key_identity", "key_hash"},
	"scheduler_state":        {"key_identity", "key_hash"},
	"agent_prompt_vars":      {"identity", "identity_hash"},
	"mcp_tools_cache":        {"identity", "identity_hash"},
	"agent_visibility":       {"identity", "identity_hash"},
	"agent_skill_visibility": {"identity", "identity_hash"},
	"llm_profiles":           {"name_identity", "name_hash"},
}

// ExtensionTables returns the 16 extension/traffic/infrastructure tables
// (3 dynamic id tables + 5 traffic tables + 8 infrastructure/arbitration tables).
//
// withExtensionIdentityCollation wraps a Column with explicit utf8mb4 / utf8mb4_bin charset/collation,
// keeping all other column properties intact. Scoped strictly to authorized identity columns.
func withExtensionIdentityCollation(c Column) Column {
	c.Charset = "utf8mb4"
	c.Collation = "utf8mb4_bin"
	return c
}

func ExtensionTables() []*Table {
	return []*Table{
		// 50. llm_records（db/commands.go:156–199 动态表）
		T("llm_records", "id", true, []Column{
			CPK("id"),
			DateN("ts"),
			Text("model"),
			Text("profile_name"),
			withExtensionIdentityCollation(Text("session_id")),
			withExtensionIdentityCollation(Text("task_id")),
			Text("worker"),
			Int("latency_ms"),
			Int("input_tokens"),
			Int("output_tokens"),
			Int("cache_read"),
			Int("cache_write"),
			Text("status"),
			Text("error"),
			Text("request_body"),
			Text("response_body"),
			Text("raw_request"),
			Text("raw_response"),
		}, []Index{
			IX("idx_llm_records_ts", false, P("ts")),
			IX("idx_llm_records_session", false, PP("session_id", 64)),
		}),

		// 51. llm_usage（db/llm_usage.go:29–48 动态表）
		T("llm_usage", "id", true, []Column{
			CPK("id"),
			DateN("ts"),
			withExtensionIdentityCollation(Text("task_id")),
			CRef("exploration_id"),
			Text("worker"),
			Text("model"),
			Text("profile_name"),
			Int("latency_ms"),
			IntN("input_tokens", "0"),
			IntN("output_tokens", "0"),
			IntN("cache_read", "0"),
			IntN("cache_write", "0"),
			Text("status"),
		}, []Index{
			IX("idx_llm_usage_task", false, PP("task_id", 64)),
			IX("idx_llm_usage_model", false, PP("task_id", 64), PP("model", 64)),
			IX("idx_llm_usage_exp", false, P("exploration_id")),
		}),

		// 52. finding_assets（规格新增资产事实表）
		T("finding_assets", "id", true, []Column{
			CPK("id"),
			CRefN("finding_id"),
			CRefN("asset_id"),
			TextN("source", ""),
			DateN("created_at"),
		}, []Index{
			IX("uq_finding_assets", true, P("finding_id"), P("asset_id")),
		}),

		// 53. traffic_exchanges（§6.1：逻辑唯一键 (namespace,id)）
		T("traffic_exchanges", "row_id", true, []Column{
			CPK("row_id"),
			CharA("namespace", 36),
			CharA("id", 36),
			BigNN("ts"),
			TextNN("host"),
			Text("url_template"),
			Text("url"),
			Text("method"),
			Text("content_type"),
			Int("status"),
			Big("req_len"),
			Big("resp_len"),
			Text("path"),
			VarBinN("search_bloom", 8192, "X'00'"),
			IntN("search_version", "0"),
		}, []Index{
			IX("uq_traffic_exchanges", true, P("namespace"), P("id")),
			IX("idx_traffic_exchanges_host", false, P("namespace"), PP("host", 191)),
			IX("idx_traffic_exchanges_tmpl", false, P("namespace"), PP("host", 191), PP("url_template", 191)),
			IX("idx_traffic_exchanges_ts", false, P("namespace"), P("ts"), P("id")),
			IX("idx_traffic_exchanges_status", false, P("namespace"), P("status"), P("id")),
			IX("idx_traffic_exchanges_resp", false, P("namespace"), P("resp_len"), P("id")),
		}),

		// 54. traffic_exchange_bodies（§6.1：逻辑唯一键 (namespace,exchange_id)）
		T("traffic_exchange_bodies", "row_id", true, []Column{
			CPK("row_id"),
			CharA("namespace", 36),
			CharA("exchange_id", 36),
			TextN("req_head", ""),
			TextN("resp_head", ""),
			Blob("req_body"),
			Blob("resp_body"),
			Column{Name: "req_blob", Type: TChar, Length: 64, Charset: "ascii", Collation: "ascii_bin"},
			Column{Name: "resp_blob", Type: TChar, Length: 64, Charset: "ascii", Collation: "ascii_bin"},
		}, []Index{
			IX("uq_traffic_exchange_bodies", true, P("namespace"), P("exchange_id")),
			IX("idx_traffic_bodies_blobs", false, P("namespace"), P("exchange_id")),
			IX("idx_traffic_blobs_req", false, P("namespace"), P("req_blob")),
			IX("idx_traffic_blobs_resp", false, P("namespace"), P("resp_blob")),
		}),

		// 55. traffic_blob_refs（§6.1：逻辑唯一键 (namespace,hash,exchange_id)）
		T("traffic_blob_refs", "row_id", true, []Column{
			CPK("row_id"),
			CharA("namespace", 36),
			CharA("hash", 64),
			CharA("exchange_id", 36),
		}, []Index{
			IX("uq_traffic_blob_refs", true, P("namespace"), P("hash"), P("exchange_id")),
			IX("idx_traffic_blob_refs_ex", false, P("namespace"), P("exchange_id")),
		}),

		// 56. traffic_index_chunks（§6.1：逻辑唯一键 (namespace,exchange_id,seq)；text 保持 utf8mb4 全文检索）
		T("traffic_index_chunks", "row_id", true, []Column{
			CPK("row_id"),
			CharA("namespace", 36),
			CharA("exchange_id", 36),
			IntNN("seq"),
			Column{Name: "text", Type: TText, NotNull: true, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
		}, []Index{
			IX("uq_traffic_index_chunks", true, P("namespace"), P("exchange_id"), P("seq")),
		}),

		// 57. traffic_stores（§6.1：namespace/store_id/role 三列各 UNIQUE）
		T("traffic_stores", "row_id", true, []Column{
			CPK("row_id"),
			CharA("namespace", 36),
			BinN("store_id", 16),
			VarcharAN("role", 8, ""),
			VarcharAN("relative_dir", 128, ""),
			withExtensionIdentityCollation(TextNN("directory_identity")),
			withExtensionIdentityCollation(TextNN("node_identity")),
		}, []Index{
			IX("uq_traffic_stores_namespace", true, P("namespace")),
			IX("uq_traffic_stores_store", true, P("store_id")),
			IX("uq_traffic_stores_role", true, P("role")),
		}),

		// 58. locks（§7.5：name VARCHAR(64) ascii_bin UNIQUE）
		T("locks", "row_id", true, []Column{
			CPK("row_id"),
			VarcharAN("name", 64, ""),
		}, []Index{
			IX("uq_locks_name", true, P("name")),
		}),

		// 59. schema_meta（§7.5：key VARCHAR(64) ascii_bin UNIQUE；schema_version 版本戳）
		T("schema_meta", "row_id", true, []Column{
			CPK("row_id"),
			VarcharAN("key", 64, ""),
			BigNN("value"),
			DateN("updated_at"),
		}, []Index{
			IX("uq_schema_meta_key", true, P("key")),
		}),

		// 60. business_deployment（§7.1：deployment_id/uuid/root 三列各 UNIQUE）
		T("business_deployment", "row_id", true, []Column{
			CPK("row_id"),
			VarcharAN("deployment_id", 16, ""),
			BinN("deployment_uuid", 16),
			BinN("root_uuid", 16),
			withExtensionIdentityCollation(TextNN("node_identity")),
			withExtensionIdentityCollation(TextNN("canonical_root")),
			withExtensionIdentityCollation(TextNN("root_identity")),
			BigNN("version"),
			TinyNN("state"),
			Bin("relocation_op_id", 16),
			DateN("bound_at"),
		}, []Index{
			IX("uq_business_deployment_id", true, P("deployment_id")),
			IX("uq_business_deployment_uuid", true, P("deployment_uuid")),
			IX("uq_business_deployment_root", true, P("root_uuid")),
		}),

		// 61. maintenance（§7.5 交接单 storage_maintenance = 规格表名 maintenance；单例行）
		T("maintenance", "row_id", true, []Column{
			CPK("row_id"),
			VarcharAN("singleton_key", 16, ""),
			Bin("op_id", 16),
			TinyNN("mode"),
			BigintUN("revision", 0),
			DateN("updated_at"),
		}, []Index{
			IX("uq_maintenance_singleton", true, P("singleton_key")),
		}),

		// 62. storage_jobs（§7.5：op_id UNIQUE；state 状态机索引）
		T("storage_jobs", "row_id", true, []Column{
			CPK("row_id"),
			BinN("op_id", 16),
			BinN("deployment_uuid", 16),
			TinyNN("kind"),
			CRef("task_id"),
			CRef("archive_id"),
			TinyNN("state"),
			BinN("claim_token", 16),
			BinN("manifest_hash", 32),
			IntUN("plan_version", "0"),
			TextNN("plan_path"),
			VarBinN("checkpoint", 1024, "X'00'"),
			BigintUN("segment_seq", 0),
			IntUN("attempt_no", "0"),
			Tiny("mutations_started", "0"),
			TinyNN("file_state"),
			BigNN("remaining_timeout_seconds"),
			DateN("updated_at"),
		}, []Index{
			IX("uq_storage_jobs_op", true, P("op_id")),
			IX("idx_storage_jobs_state", false, P("state"), P("updated_at"), P("op_id")),
		}),

		// 63. storage_id_map（§7.5：逻辑唯一键 (op_id,entity_kind,old_id) + 反向查询索引）
		T("storage_id_map", "row_id", true, []Column{
			CPK("row_id"),
			BinN("op_id", 16),
			TinyNN("entity_kind"),
			CRefN("old_id"),
			CRefN("new_id"),
		}, []Index{
			IX("uq_storage_id_map", true, P("op_id"), P("entity_kind"), P("old_id")),
			IX("idx_storage_id_map_reverse", false, P("op_id"), P("entity_kind"), P("new_id")),
		}),

		// 64. storage_job_pins（§7.5：逻辑唯一键 (op_id,file_seq) + (store_id,sha256) 反查）
		T("storage_job_pins", "row_id", true, []Column{
			CPK("row_id"),
			BinN("op_id", 16),
			BigintUN("file_seq", 0),
			BinN("store_id", 16),
			TextNN("relative_path"),
			BinN("sha256", 32),
			BigintUN("size_bytes", 0),
			TinyNN("role"),
		}, []Index{
			IX("uq_storage_job_pins", true, P("op_id"), P("file_seq")),
			IX("idx_storage_job_pins_lookup", false, P("store_id"), P("sha256"), P("op_id")),
		}),

		// 65. op_outcomes（§7.6：uq_op_outcomes_identity 4 元组 + idx_op_outcomes_lookup）
		T("op_outcomes", "row_id", true, []Column{
			CPK("row_id"),
			BinN("op_id", 16),
			TinyNN("tx_step"),
			BigintUN("segment_seq", 0),
			IntUN("attempt_no", "0"),
			TinyNN("op_kind"),
			TinyNN("result"),
			TinyNN("file_fate"),
			Bin("manifest_hash", 32),
			Bin("content_hash", 32),
			DateN("created_at"),
		}, []Index{
			IX("uq_op_outcomes_identity", true, P("op_id"), P("tx_step"), P("segment_seq"), P("attempt_no")),
			IX("idx_op_outcomes_lookup", false, P("op_id"), P("tx_step"), P("segment_seq")),
		}),
	}
}

// AllTables returns all 65 tables in the fixed order:
// BaselineTables() (49, schema.sql 出现顺序) + ExtensionTables() (16).
func AllTables() []*Table {
	all := make([]*Table, 0, 65)
	all = append(all, BaselineTables()...)
	all = append(all, ExtensionTables()...)
	return all
}
