package schema

// BaselineTables returns the 49 baseline business tables in the fixed order
// they appear in db/schema.sql (directory order = DDL order = verify order).
//
// 映射口径（spec §2.2）：BIGSERIAL→BIGINT UNSIGNED AUTO_INCREMENT；TEXT→MEDIUMTEXT；
// JSONB/数组→MEDIUMTEXT(JSON)；CIDR→TEXT；TIMESTAMPTZ→DATETIME(3)；
// BOOLEAN→TINYINT(1)；外键/CHECK 全部下推入口（无 FK/CHECK）；
// hash DDL 16 表中的基线 12 表单键表 + assets/company_scope 分支表在此列全。
func BaselineTables() []*Table {
	return []*Table{
		// 1. companies（nkey hash DDL）
		T("companies", "id", true, []Column{
			CPK("id"),
			TextN("name", ""),
			TextN("nkey", ""),
			BlobN("nkey_identity"),
			BinN("nkey_hash", 32),
			Text("logo"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_companies_nkey_hash", true, P("nkey_hash")),
		}),

		// 2. assets（8 分支 hash DDL：root_domain/ip/subdomain/app_bundle/app_name/
		// service_http/service_other/endpoint；分支列可空，分支不适用时 identity+hash 均 NULL）
		T("assets", "id", true, []Column{
			CPK("id"),
			TextNN("type"),
			CRef("company_id"),
			TextN("company_source", "explicit"),
			JSONN("task_ids", "[]"),
			Text("domain"),
			Text("root_domain"),
			Text("ip"),
			Text("c_segment"),
			Int("port"),
			Text("icp"),
			JSONN("bound_domains", "[]"),
			JSONN("open_ports", "[]"),
			Text("record_type"),
			JSONN("record_value", "[]"),
			Text("bundle_id"),
			Text("app_name"),
			Text("category"),
			Text("app_description"),
			Text("app_icp"),
			Text("url"),
			Text("service_type"),
			Text("service_name"),
			Text("favicon_mmh3"),
			Int("status_code"),
			Big("content_length"),
			Text("page_title"),
			JSONN("technologies", "[]"),
			JSONN("auth", "[]"),
			Text("method"),
			JSONN("params", "[]"),
			JSONN("extra", "{}"),
			DateN("created_at"),
			DateN("last_seen"),
			DateNOU("updated_at"),
			Blob("identity_root_domain"),
			Bin("hash_root_domain", 32),
			Blob("identity_ip"),
			Bin("hash_ip", 32),
			Blob("identity_subdomain"),
			Bin("hash_subdomain", 32),
			Blob("identity_app_bundle"),
			Bin("hash_app_bundle", 32),
			Blob("identity_app_name"),
			Bin("hash_app_name", 32),
			Blob("identity_service_http"),
			Bin("hash_service_http", 32),
			Blob("identity_service_other"),
			Bin("hash_service_other", 32),
			Blob("identity_endpoint"),
			Bin("hash_endpoint", 32),
		}, []Index{
			IX("uq_assets_hash_root_domain", true, P("hash_root_domain")),
			IX("uq_assets_hash_ip", true, P("hash_ip")),
			IX("uq_assets_hash_subdomain", true, P("hash_subdomain")),
			IX("uq_assets_hash_app_bundle", true, P("hash_app_bundle")),
			IX("uq_assets_hash_app_name", true, P("hash_app_name")),
			IX("uq_assets_hash_service_http", true, P("hash_service_http")),
			IX("uq_assets_hash_service_other", true, P("hash_service_other")),
			IX("uq_assets_hash_endpoint", true, P("hash_endpoint")),
			IX("idx_assets_company", false, P("company_id")),
			IX("idx_assets_company_type", false, P("company_id"), PP("type", 16)),
			IX("idx_assets_domain", false, PP("domain", 191)),
			IX("idx_assets_root_domain", false, PP("root_domain", 191)),
			IX("idx_assets_ip", false, PP("ip", 64)),
			IX("idx_assets_last_seen", false, PD("last_seen")),
			IX("idx_assets_type_seen", false, PP("type", 16), PD("last_seen")),
		}),

		// 3. company_scope（3 分支 hash DDL：scope_domain/scope_net/scope_value，均含 company_id）
		T("company_scope", "id", true, []Column{
			CPK("id"),
			CRefN("company_id"),
			TextNN("kind"),
			Text("domain"),
			Text("net"),
			Text("value"),
			TextNN("raw"),
			Text("reason"),
			DateN("created_at"),
			Blob("identity_scope_domain"),
			Bin("hash_scope_domain", 32),
			Blob("identity_scope_net"),
			Bin("hash_scope_net", 32),
			Blob("identity_scope_value"),
			Bin("hash_scope_value", 32),
		}, []Index{
			IX("uq_cscope_hash_domain", true, P("hash_scope_domain")),
			IX("uq_cscope_hash_net", true, P("hash_scope_net")),
			IX("uq_cscope_hash_value", true, P("hash_scope_value")),
			IX("idx_cscope_domain", false, PP("domain", 191)),
			IX("idx_cscope_value", false, PP("value", 191)),
			IX("idx_cscope_company", false, P("company_id")),
		}),

		// 4. explorations
		T("explorations", "id", true, []Column{
			CPK("id"),
			Text("description"),
			TextNN("goal"),
			TextN("status", "open"),
			BigN("round_no", "0"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, nil),

		// 5. exploration_nodes
		T("exploration_nodes", "id", true, []Column{
			CPK("id"),
			CRefN("exploration_id"),
			TextNN("kind"),
			JSONN("payload", "{}"),
			IntN("priority", "0"),
			TextN("state", "open"),
			Text("origin"),
			Text("owner"),
			Text("blocked_reason"),
			Text("delete_reason"),
			IntN("content_version", "0"),
			Big("cold_since_round"),
			DateN("created_at"),
			DateNOU("updated_at"),
			Date("completed_at"),
		}, []Index{
			IX("idx_expnodes_part", false, P("exploration_id"), PP("kind", 16)),
			IX("idx_expnodes_frontier", false, P("exploration_id"), PD("priority")),
		}),

		// 6. exploration_edges（原复合 PK → 隐藏 row_id）
		T("exploration_edges", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("exploration_id"),
			CRefN("src_id"),
			CRefN("dst_id"),
			VarcharAN("rel", 32, ""),
			DateN("created_at"),
		}, []Index{
			IX("uq_exploration_edges", true, P("exploration_id"), P("src_id"), P("rel"), P("dst_id")),
			IX("idx_expedges_src", false, P("src_id"), P("rel")),
			IX("idx_expedges_dst", false, P("dst_id"), P("rel")),
		}),

		// 7. exploration_anchors（原复合 PK → 隐藏 row_id）
		T("exploration_anchors", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("node_id"),
			CRefN("asset_id"),
		}, []Index{
			IX("uq_exploration_anchors", true, P("node_id"), P("asset_id")),
			IX("idx_anchor_asset", false, P("asset_id")),
		}),

		// 8. task_constraints
		T("task_constraints", "id", true, []Column{
			CPK("id"),
			CRefN("exploration_id"),
			TextNN("kind"),
			TextNN("text"),
			Text("origin"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("idx_task_constraints_exp", false, P("exploration_id")),
		}),

		// 9. activity
		T("activity", "id", true, []Column{
			CPK("id"),
			CRefN("exploration_id"),
			CRef("node_id"),
			Text("worker"),
			Text("kind"),
			Text("tool"),
			Text("tool_use_id"),
			BoolN("is_error", false),
			Text("summary"),
			Text("detail"),
			JSONN("metadata", "{}"),
			Int("input_tokens"),
			Int("output_tokens"),
			Int("cache_read_tokens"),
			Int("cache_write_tokens"),
			Int("main_seg"),
			DateN("created_at"),
		}, []Index{
			IX("idx_act_node", false, P("exploration_id"), P("node_id"), P("id")),
			IX("idx_act_since", false, P("exploration_id"), P("id")),
			IX("idx_act_tool_call", false, P("exploration_id"), PP("tool_use_id", 64), P("id")),
			IX("idx_act_worker", false, P("exploration_id"), PP("worker", 32), P("id")),
			IX("idx_act_main_seg", false, P("exploration_id"), P("main_seg"), P("id")),
			IX("idx_act_result_usage", false, P("exploration_id"), P("input_tokens"), P("output_tokens"), P("cache_read_tokens"), P("cache_write_tokens")),
			IX("idx_act_latest", false, P("exploration_id"), PD("created_at")),
		}),

		// 10. main_sessions（原复合 PK → 隐藏 row_id）
		T("main_sessions", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("exploration_id"),
			IntNN("seq"),
			DateN("created_at"),
		}, []Index{
			IX("uq_main_sessions", true, P("exploration_id"), P("seq")),
		}),

		// 11. settings（原 TEXT PK → 隐藏 row_id；key hash DDL）
		T("settings", "row_id", true, []Column{
			CPK("row_id"),
			TextNN("key"),
			BlobN("key_identity"),
			BinN("key_hash", 32),
			TextNN("value"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_settings_key_hash", true, P("key_hash")),
		}),

		// 12. llm_profiles（one-default 部分唯一 → 生成列 is_default_key；name hash 唯一）
		T("llm_profiles", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			BlobN("name_identity"),
			BinN("name_hash", 32),
			TextNN("format"),
			Text("base_url"),
			Text("proxy"),
			TextNN("model"),
			Text("api_key"),
			Text("api_key_hint"),
			DoubleN("rate_per_second", "0"),
			DoubleN("rate_per_minute", "0"),
			IntN("context_window_k", "0"),
			TextN("reasoning_effort", ""),
			TextN("thinking_type", ""),
			BoolN("is_default", false),
			IntN("priority", "0"),
			BoolN("pool_exclude", false),
			BoolN("streaming", true),
			IntN("max_tokens", "0"),
			TextN("max_tokens_field", ""),
			TextN("session_header_key", ""),
			IntN("retry_connect_attempts", "0"),
			IntN("retry_connect_interval_ms", "0"),
			IntN("retry_empty_attempts", "0"),
			IntN("retry_empty_interval_ms", "0"),
			IntN("retry_stream_attempts", "0"),
			IntN("retry_stream_interval_ms", "0"),
			Gen("is_default_key", TTiny, 0, "IF(is_default,1,NULL)"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_llm_profiles_name_hash", true, P("name_hash")),
			IX("uq_llm_one_default", true, P("is_default_key")),
		}),

		// 13. llm_profile_health（1:1 共享父实体整数 PK，唯一非自增表）
		T("llm_profile_health", "profile_id", false, []Column{
			CPK("profile_id"),
			IntN("fails", "0"),
			IntN("trips", "0"),
			Date("open_until"),
			TextN("last_error", ""),
			DateN("last_at"),
		}, nil),

		// 14. task_categories（nkey hash DDL）
		T("task_categories", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			TextNN("nkey"),
			BlobN("nkey_identity"),
			BinN("nkey_hash", 32),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_task_categories_nkey_hash", true, P("nkey_hash")),
			IX("idx_task_categories_name", false, PP("name", 64), P("id")),
		}),

		// 15. tasks
		T("tasks", "id", true, []Column{
			CPK("id"),
			TextN("name", ""),
			CRef("category_id"),
			TextNN("description"),
			TextNN("goal"),
			CRefN("exploration_id"),
			TextN("status", "created"),
			BoolN("paused", false),
			BoolN("queued", false),
			Date("queued_at"),
			TextN("queue_mode", ""),
			CRef("llm_profile_id"),
			CRef("active_llm_profile_id"),
			BigN("llm_chain_revision", "0"),
			CRef("company_id"),
			Text("parent_ref"),
			IntN("timeout_seconds", "0"),
			IntN("plan_heartbeat_seconds", "300"),
			BoolN("coverage_enabled", true),
			Date("pinned_at"),
			Date("first_run_at"),
			Date("deadline_at"),
			Date("archived_at"),
			Date("deleted_at"),
			Date("completed_at"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_tasks_exploration_id", true, P("exploration_id")),
			IX("idx_tasks_alive", false, PD("created_at")),
			IX("idx_tasks_status", false, PP("status", 16)),
			IX("idx_tasks_category", false, P("category_id"), PD("created_at")),
			IX("idx_tasks_pinned", false, PD("pinned_at")),
			IX("idx_tasks_archived", false, PD("archived_at")),
			IX("idx_tasks_llm_profile", false, P("llm_profile_id")),
			IX("idx_tasks_active_llm_profile", false, P("active_llm_profile_id")),
		}),

		// 16. task_archives
		T("task_archives", "id", true, []Column{
			CPK("id"),
			CRefN("task_id"),
			TextN("state", "archive_queued"),
			TextN("phase", "queued"),
			IntN("progress", "0"),
			TextN("error", ""),
			JSONN("warnings", "[]"),
			IntN("format_version", "2"),
			TextN("archive_path", ""),
			TextN("sha256", ""),
			BigN("original_size", "0"),
			BigN("compressed_size", "0"),
			TextN("task_name", ""),
			TextN("task_description", ""),
			TextN("task_goal", ""),
			TextN("original_status", ""),
			CRef("category_id_snapshot"),
			TextN("category_name_snapshot", ""),
			JSONN("source_task_ids", "[]"),
			BigN("remaining_timeout_seconds", "0"),
			JSONN("data_counts", "{}"),
			JSONN("aggregate_stats", "{}"),
			Date("archived_at"),
			DateN("requested_at"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_task_archives_task_id", true, P("task_id")),
			IX("idx_task_archives_state", false, PP("state", 32), P("requested_at"), P("id")),
			IX("idx_task_archives_archived", false, PD("archived_at"), PD("id")),
		}),

		// 17. task_templates（nkey hash DDL）
		T("task_templates", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			TextNN("nkey"),
			BlobN("nkey_identity"),
			BinN("nkey_hash", 32),
			TextNN("description"),
			TextNN("goal"),
			CRef("category_id"),
			JSONN("intercept_rules", "[]"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_task_templates_nkey_hash", true, P("nkey_hash")),
			IX("idx_task_templates_updated", false, PD("updated_at"), PD("id")),
		}),

		// 18. task_relations（原复合 PK → 隐藏 row_id）
		T("task_relations", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("task_id"),
			CRefN("source_task_id"),
			DateN("created_at"),
		}, []Index{
			IX("uq_task_relations", true, P("task_id"), P("source_task_id")),
			IX("idx_task_relations_source", false, P("source_task_id")),
		}),

		// 19. task_asset_links（原复合 PK → 隐藏 row_id）
		T("task_asset_links", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("task_id"),
			CRefN("asset_id"),
			TextN("source", "system"),
			TextN("source_summary", ""),
			CRef("source_node_id"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_task_asset_links", true, P("task_id"), P("asset_id")),
			IX("idx_task_asset_links_asset", false, P("asset_id"), P("task_id")),
			IX("idx_task_asset_links_node", false, P("source_node_id")),
		}),

		// 20. task_llm_profiles（原复合 PK → 隐藏 row_id）
		T("task_llm_profiles", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("task_id"),
			CRefN("profile_id"),
			IntNN("position"),
			TextN("status", "ready"),
			Text("last_error"),
			Date("exhausted_at"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_task_llm_profiles", true, P("task_id"), P("profile_id")),
			IX("uq_task_llm_profiles_pos", true, P("task_id"), P("position")),
			IX("idx_task_llm_profiles_profile", false, P("profile_id"), P("task_id")),
		}),

		// 21. task_scope（COALESCE 全字段 hash DDL，单一 identity/identity_hash）
		T("task_scope", "id", true, []Column{
			CPK("id"),
			CRefN("task_id"),
			TextNN("kind"),
			CRef("company_id"),
			Text("domain"),
			Text("net"),
			Text("value"),
			TextN("source", "auto"),
			Text("reason"),
			DateN("created_at"),
			BlobN("identity"),
			BinN("identity_hash", 32),
		}, []Index{
			IX("uq_task_scope_identity", true, P("identity_hash")),
			IX("idx_ts_domain", false, PP("domain", 191)),
			IX("idx_ts_company", false, P("company_id")),
		}),

		// 22. agents（key hash DDL）
		T("agents", "id", true, []Column{
			CPK("id"),
			TextNN("key"),
			BlobN("key_identity"),
			BinN("key_hash", 32),
			TextNN("name"),
			Text("description"),
			TextNN("role"),
			BoolN("builtin", true),
			BoolN("enabled", true),
			CRef("llm_profile_id"),
			CRef("current_prompt_id"),
			IntN("max_turns", "0"),
			IntN("run_seconds", "1200"),
			BoolN("web_search", false),
			BoolN("interactive_shell", false),
			TextN("wrapup_prompt", ""),
			IntN("wrapup_max_turns", "0"),
			TextN("task_timeout_wrapup_prompt", ""),
			IntN("task_timeout_wrapup_max_turns", "0"),
			TextN("trigger_run_mode", "serial"),
			TextN("trigger_merge_mode", "all"),
			IntN("trigger_max_parallel", "5"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_agents_key_hash", true, P("key_hash")),
			IX("idx_agents_llm_profile", false, P("llm_profile_id")),
		}),

		// 23. agent_prompts
		T("agent_prompts", "id", true, []Column{
			CPK("id"),
			CRefN("agent_id"),
			IntNN("version"),
			TextNN("template_text"),
			Text("note"),
			Text("updated_by"),
			DateN("created_at"),
		}, []Index{
			IX("uq_agent_prompts", true, P("agent_id"), P("version")),
		}),

		// 24. agent_prompt_vars（agent_id+var_name hash DDL）
		T("agent_prompt_vars", "id", true, []Column{
			CPK("id"),
			CRefN("agent_id"),
			TextNN("var_name"),
			BlobN("identity"),
			BinN("identity_hash", 32),
			Text("description"),
			Text("example"),
			TextNN("source"),
		}, []Index{
			IX("uq_agent_prompt_vars_identity", true, P("identity_hash")),
		}),

		// 25. mcp_servers（name hash DDL）
		T("mcp_servers", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			BlobN("name_identity"),
			BinN("name_hash", 32),
			TextNN("transport"),
			Text("command"),
			JSONN("args", "[]"),
			JSONN("env", "{}"),
			Text("url"),
			BoolN("enabled", true),
			BoolN("insecure", false),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_mcp_servers_name_hash", true, P("name_hash")),
		}),

		// 26. mcp_tools_cache（server_id+tool_name hash DDL）
		T("mcp_tools_cache", "id", true, []Column{
			CPK("id"),
			CRefN("server_id"),
			TextNN("tool_name"),
			BlobN("identity"),
			BinN("identity_hash", 32),
			Text("description"),
			JSON("schema"),
			DateN("discovered_at"),
		}, []Index{
			IX("uq_mcp_tools_cache_identity", true, P("identity_hash")),
		}),

		// 27. agent_visibility（原复合 PK → 隐藏 row_id；4 元组 hash DDL）
		T("agent_visibility", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("agent_id"),
			TextNN("resource_kind"),
			CRefN("resource_id"),
			TextN("mcp_tool_name", ""),
			BlobN("identity"),
			BinN("identity_hash", 32),
			BoolN("enabled", true),
			DateN("created_at"),
		}, []Index{
			IX("uq_agent_visibility_identity", true, P("identity_hash")),
			IX("idx_vis_resource", false, PP("resource_kind", 16), P("resource_id")),
		}),

		// 28. agent_skill_visibility（原复合 PK → 隐藏 row_id；agent_id+skill_name hash DDL）
		T("agent_skill_visibility", "row_id", true, []Column{
			CPK("row_id"),
			CRefN("agent_id"),
			TextNN("skill_name"),
			BlobN("identity"),
			BinN("identity_hash", 32),
			BoolN("enabled", true),
			DateN("created_at"),
		}, []Index{
			IX("uq_agent_skill_visibility_identity", true, P("identity_hash")),
			IX("idx_askv_skill", false, PP("skill_name", 64)),
		}),

		// 29. skill_usage
		T("skill_usage", "id", true, []Column{
			CPK("id"),
			DateN("ts"),
			TextNN("skill"),
			Text("agent_key"),
			CRef("task_id"),
			CRef("exploration_id"),
			CRef("intent_id"),
			Text("session_id"),
			IntN("args_len", "0"),
			BoolN("found", true),
		}, []Index{
			IX("idx_skill_usage_skill", false, PP("skill", 64), PD("ts")),
			IX("idx_skill_usage_task", false, P("task_id")),
		}),

		// 30. tool_usage
		T("tool_usage", "id", true, []Column{
			CPK("id"),
			DateN("ts"),
			TextNN("tool_key"),
			Text("agent_key"),
			CRef("task_id"),
			CRef("exploration_id"),
			CRef("intent_id"),
			Text("session_id"),
		}, []Index{
			IX("idx_tool_usage_tool", false, PP("tool_key", 64), PD("ts")),
			IX("idx_tool_usage_task", false, P("task_id")),
		}),

		// 31. tools（原 TEXT PK → 隐藏 row_id；key hash DDL）
		T("tools", "row_id", true, []Column{
			CPK("row_id"),
			TextNN("key"),
			BlobN("key_identity"),
			BinN("key_hash", 32),
			BoolN("system", true),
			TextN("description", ""),
			JSONN("schema", "{}"),
			JSONN("agents", "[]"),
			BoolN("enabled", true),
			TextN("kind", "builtin"),
			JSONN("exec", "{}"),
			BoolN("deferred", false),
			DateNOU("updated_at"),
		}, []Index{
			IX("uq_tools_key_hash", true, P("key_hash")),
		}),

		// 32. conversations
		T("conversations", "id", true, []Column{
			CPK("id"),
			TextNN("agent_key"),
			TextN("title", ""),
			CRef("llm_profile_id"),
			Date("pinned_at"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("idx_conversations_llm_profile", false, P("llm_profile_id")),
			IX("idx_conversations_pinned", false, PD("pinned_at")),
		}),

		// 33. conversation_activities
		T("conversation_activities", "id", true, []Column{
			CPK("id"),
			CRefN("conversation_id"),
			Text("worker"),
			Text("kind"),
			Text("tool"),
			Text("tool_use_id"),
			BoolN("is_error", false),
			Text("summary"),
			Text("detail"),
			Int("input_tokens"),
			Int("output_tokens"),
			Int("cache_read_tokens"),
			Int("cache_write_tokens"),
			DateN("created_at"),
		}, []Index{
			IX("idx_conv_act", false, P("conversation_id"), P("id")),
			IX("idx_conv_act_tool_call", false, P("conversation_id"), PP("tool_use_id", 64), P("id")),
		}),

		// 34. agent_triggers
		T("agent_triggers", "id", true, []Column{
			CPK("id"),
			TextNN("agent_key"),
			BoolN("enabled", true),
			IntN("interval_sec", "0"),
			BoolN("on_finding", false),
			BoolN("on_goal_met", false),
			BoolN("on_task_timeout", false),
			BoolN("on_tool_call", false),
			BoolN("on_task_create", false),
			TextN("interval_message", ""),
			TextN("finding_message", ""),
			TextN("goal_message", ""),
			TextN("task_timeout_message", ""),
			TextN("tool_call_message", ""),
			TextN("task_create_message", ""),
			TextN("tool_names", ""),
			Date("last_fire"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("idx_agent_triggers_agent", false, PP("agent_key", 64)),
		}),

		// 35. scheduler_state（原 TEXT PK → 隐藏 row_id；key hash DDL）
		T("scheduler_state", "row_id", true, []Column{
			CPK("row_id"),
			TextNN("key"),
			BlobN("key_identity"),
			BinN("key_hash", 32),
			TextN("value", ""),
		}, []Index{
			IX("uq_scheduler_state_key_hash", true, P("key_hash")),
		}),

		// 36. intercept_rules
		T("intercept_rules", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			BoolN("enabled", true),
			IntN("priority", "0"),
			TextNN("match_target"),
			TextNN("match_type"),
			TextNN("pattern"),
			TextNN("action"),
			TextN("message", ""),
			BoolN("timeout_enabled", true),
			IntN("timeout_seconds", "60"),
			TextN("timeout_action", "deny"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, nil),

		// 37. intercept_pending
		T("intercept_pending", "id", true, []Column{
			CPK("id"),
			CRef("rule_id"),
			CRef("conversation_id"),
			Text("task_id"),
			TextN("agent_name", ""),
			TextNN("tool_name"),
			JSONN("tool_input", "{}"),
			TextN("status", "pending"),
			TextN("reason", ""),
			Date("decided_at"),
			DateN("created_at"),
			JSON("audit"),
			TextN("decision_source", ""),
		}, []Index{
			IX("idx_intercept_pending_status", false, PP("status", 16), PD("created_at")),
			IX("idx_intercept_pending_task", false, PP("task_id", 64), PD("created_at")),
		}),

		// 38. findings
		T("findings", "id", true, []Column{
			CPK("id"),
			CRef("task_id"),
			CRef("node_id"),
			TextN("vulnclass", ""),
			TextN("name", ""),
			TextN("severity", ""),
			TextN("summary", ""),
			TextN("evidence", ""),
			TextN("worker", ""),
			JSONN("asset_ids", "[]"),
			TextN("status", "pending"),
			TextN("report", ""),
			DateN("created_at"),
			BigN("evidence_version", "0"),
			BigN("report_evidence_version", "0"),
		}, []Index{
			IX("idx_findings_task", false, P("task_id"), PD("created_at")),
			IX("idx_findings_time", false, PD("created_at")),
			IX("idx_findings_status", false, PP("status", 32), PD("created_at")),
		}),

		// 39. finding_retests（active 部分唯一 → 生成列 active_finding_id）
		T("finding_retests", "id", true, []Column{
			CPK("id"),
			CRefN("finding_id"),
			CRef("conversation_id"),
			TextN("status", "pending"),
			TextN("verdict", ""),
			TextN("notes", ""),
			JSONNN("snapshot"),
			TextN("summary", ""),
			TextN("evidence", ""),
			TextN("error", ""),
			DateN("created_at"),
			Date("started_at"),
			Date("finished_at"),
			Gen("active_finding_id", TBigUnsigned, 0, "IF(status IN ('pending','running'), finding_id, NULL)"),
		}, []Index{
			IX("uq_finding_retests_conversation", true, P("conversation_id")),
			IX("uq_finding_retests_active", true, P("active_finding_id")),
			IX("idx_finding_retests_history", false, P("finding_id"), PD("id")),
		}),

		// 40. traffic_evidence_snapshots（原 TEXT PK → 隐藏 row_id；id CHAR(64) ascii_bin UNIQUE）
		T("traffic_evidence_snapshots", "row_id", true, []Column{
			CPK("row_id"),
			CharA("id", 64),
			TextNN("source_traffic_id"),
			BigNN("captured_at"),
			TextNN("url"),
			TextNN("method"),
			IntNN("status"),
			TextN("content_type", ""),
			TextNN("req_head"),
			TextNN("resp_head"),
			CharA("req_hash", 64),
			CharA("resp_hash", 64),
			BigNN("req_len"),
			BigNN("resp_len"),
			Date("unreferenced_at"),
			DateN("created_at"),
		}, []Index{
			IX("uq_traffic_evidence_snapshots_id", true, P("id")),
		}),

		// 41. finding_traffic_bindings
		T("finding_traffic_bindings", "id", true, []Column{
			CPK("id"),
			CRefN("finding_id"),
			CharA("snapshot_id", 64),
			TextN("role", "supporting"),
			TextN("note", ""),
			IntNN("position"),
			DateN("created_at"),
		}, []Index{
			IX("uq_finding_traffic_bindings", true, P("finding_id"), P("snapshot_id")),
			IX("idx_finding_traffic_order", false, P("finding_id"), P("position"), P("id")),
			IX("idx_finding_traffic_snapshot", false, P("snapshot_id")),
		}),

		// 42. server_logs（原 PG 的 id DESC 索引与物理 PK 等价，不保留）
		T("server_logs", "id", true, []Column{
			CPK("id"),
			DateN("created_at"),
			TextN("level", "info"),
			TextN("tag", ""),
			TextN("text", ""),
		}, nil),

		// 43. side_question_sessions（原 TEXT PK → 隐藏 row_id；session_key VARCHAR(128) ascii_bin UNIQUE）
		T("side_question_sessions", "row_id", true, []Column{
			CPK("row_id"),
			VarcharAN("session_key", 128, ""),
			CRef("conversation_id"),
			CRef("task_id"),
			CRef("exploration_id"),
			CRef("intent_id"),
			BigNN("run_id"),
			BigNN("version"),
			JSONNN("snapshot"),
			BigN("generation", "0"),
			JSONN("memory", "{}"),
		}, []Index{
			IX("uq_side_sessions_session_key", true, P("session_key")),
			IX("idx_side_sessions_conv", false, P("conversation_id")),
			IX("idx_side_sessions_task", false, P("task_id")),
			IX("idx_side_sessions_exp", false, P("exploration_id")),
			IX("idx_side_sessions_intent", false, P("intent_id")),
		}),

		// 44. side_question_requests（ordinal 自增 PK；id VARCHAR(64) ascii_bin UNIQUE；
		// session_key+generation+client_id hash DDL；running 部分唯一 → 生成列 running_session_key）
		T("side_question_requests", "ordinal", true, []Column{
			CPK("ordinal"),
			VarcharAN("id", 64, ""),
			VarcharAN("session_key", 128, ""),
			BigNN("generation"),
			TextNN("client_id"),
			BlobN("identity"),
			BinN("identity_hash", 32),
			TextNN("question"),
			TextN("answer", ""),
			TextNN("status"),
			TextN("error", ""),
			JSONNN("model"),
			DateN("snapshot_at"),
			DateN("created_at"),
			BigN("sequence", "0"),
			JSONN("usage", "{}"),
			JSONN("context_info", "{}"),
			func() Column {
				c := Gen("running_session_key", TVarchar, 128, "IF(status='running', session_key, NULL)")
				c.Charset = "ascii"
				c.Collation = "ascii_bin"
				return c
			}(),
		}, []Index{
			IX("uq_side_requests_id", true, P("id")),
			IX("uq_side_requests_identity", true, P("identity_hash")),
			IX("uq_side_requests_running", true, P("running_session_key")),
			IX("idx_side_requests_history", false, P("session_key"), PD("ordinal")),
		}),

		// 45. asset_intercept_rules
		T("asset_intercept_rules", "id", true, []Column{
			CPK("id"),
			BoolN("enabled", true),
			TextNN("kind"),
			TextNN("pattern"),
			TextN("note", ""),
			BoolN("builtin", false),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("idx_asset_intercept_enabled", false, P("enabled")),
		}),

		// 46. task_intercept_rules
		T("task_intercept_rules", "id", true, []Column{
			CPK("id"),
			CRefN("task_id"),
			BoolN("enabled", true),
			TextN("action", "block"),
			TextNN("kind"),
			TextNN("pattern"),
			TextN("note", ""),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, []Index{
			IX("idx_task_intercept_task", false, P("task_id")),
		}),

		// 47. notification_channels
		T("notification_channels", "id", true, []Column{
			CPK("id"),
			TextNN("name"),
			TextNN("kind"),
			BoolN("enabled", true),
			JSONN("config", "{}"),
			TextN("mode", "realtime"),
			JSONN("filter", "{}"),
			IntN("rate_per_min", "20"),
			DateN("created_at"),
			DateNOU("updated_at"),
		}, nil),

		// 48. notification_events
		T("notification_events", "id", true, []Column{
			CPK("id"),
			TextNN("kind"),
			CRefN("finding_id"),
			JSONNN("snapshot"),
			BoolN("fanned_out", false),
			DateN("created_at"),
		}, []Index{
			IX("idx_notification_events_pending", false, P("fanned_out"), P("id")),
		}),

		// 49. notification_deliveries
		T("notification_deliveries", "id", true, []Column{
			CPK("id"),
			CRefN("event_id"),
			CRefN("channel_id"),
			TextN("state", "pending"),
			IntN("attempts", "0"),
			DateN("next_attempt_at"),
			TextN("last_error", ""),
			CRef("batch_id"),
			DateN("created_at"),
			Date("sent_at"),
		}, []Index{
			IX("idx_notification_deliveries_due", false, PP("state", 16), P("next_attempt_at")),
			IX("idx_notification_deliveries_batch", false, P("batch_id")),
			IX("idx_notification_deliveries_channel", false, P("channel_id"), PD("id")),
		}),
	}
}
