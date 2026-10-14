package schema

import (
	"strings"
	"testing"
)

// expectedColumnContract captures the static contract required by task-3-fix3-brief.md.
type expectedColumnContract struct {
	Table       string
	Column      string
	Type        ColumnType
	Length      int
	NotNull     bool
	Default     string
	Generated   string
	Charset     string
	Collation   string
	SQLContains string
}

// target33Manifest is the explicit, fixed manifest of all 33 columns defined in task-3-fix3-brief.md.
// All original types, lengths, nullability, defaults, and generated properties are hardcoded
// from the pre-modification codebase state to guarantee no regressions or drift.
var target33Manifest = []expectedColumnContract{
	// 1. companies.nkey
	{
		Table:       "companies",
		Column:      "nkey",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "''",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 2. task_categories.nkey
	{
		Table:       "task_categories",
		Column:      "nkey",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 3. task_templates.nkey
	{
		Table:       "task_templates",
		Column:      "nkey",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 4. settings.key
	{
		Table:       "settings",
		Column:      "key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 5. tools.key
	{
		Table:       "tools",
		Column:      "key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 6. scheduler_state.key
	{
		Table:       "scheduler_state",
		Column:      "key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 7. agents.key
	{
		Table:       "agents",
		Column:      "key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 8. mcp_servers.name
	{
		Table:       "mcp_servers",
		Column:      "name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 9. agent_prompt_vars.var_name
	{
		Table:       "agent_prompt_vars",
		Column:      "var_name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 10. mcp_tools_cache.tool_name
	{
		Table:       "mcp_tools_cache",
		Column:      "tool_name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 11. agent_visibility.resource_kind
	{
		Table:       "agent_visibility",
		Column:      "resource_kind",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 12. agent_visibility.mcp_tool_name
	{
		Table:       "agent_visibility",
		Column:      "mcp_tool_name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "''",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 13. agent_skill_visibility.skill_name
	{
		Table:       "agent_skill_visibility",
		Column:      "skill_name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 14. side_question_requests.client_id
	{
		Table:       "side_question_requests",
		Column:      "client_id",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 15. conversations.agent_key
	{
		Table:       "conversations",
		Column:      "agent_key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 16. agent_triggers.agent_key
	{
		Table:       "agent_triggers",
		Column:      "agent_key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 17. skill_usage.agent_key
	{
		Table:       "skill_usage",
		Column:      "agent_key",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 18. tool_usage.agent_key
	{
		Table:       "tool_usage",
		Column:      "agent_key",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 19. tool_usage.tool_key
	{
		Table:       "tool_usage",
		Column:      "tool_key",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 20. llm_records.session_id
	{
		Table:       "llm_records",
		Column:      "session_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 21. skill_usage.session_id
	{
		Table:       "skill_usage",
		Column:      "session_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 22. tool_usage.session_id
	{
		Table:       "tool_usage",
		Column:      "session_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 23. traffic_stores.directory_identity
	{
		Table:       "traffic_stores",
		Column:      "directory_identity",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 24. traffic_stores.node_identity
	{
		Table:       "traffic_stores",
		Column:      "node_identity",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 25. business_deployment.node_identity
	{
		Table:       "business_deployment",
		Column:      "node_identity",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 26. business_deployment.canonical_root
	{
		Table:       "business_deployment",
		Column:      "canonical_root",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 27. business_deployment.root_identity
	{
		Table:       "business_deployment",
		Column:      "root_identity",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 28. llm_profiles.name
	{
		Table:       "llm_profiles",
		Column:      "name",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 29. traffic_evidence_snapshots.source_traffic_id
	{
		Table:       "traffic_evidence_snapshots",
		Column:      "source_traffic_id",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 30. llm_records.task_id
	{
		Table:       "llm_records",
		Column:      "task_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 31. llm_usage.task_id
	{
		Table:       "llm_usage",
		Column:      "task_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 32. intercept_pending.task_id
	{
		Table:       "intercept_pending",
		Column:      "task_id",
		Type:        TText,
		Length:      0,
		NotNull:     false,
		Default:     "",
		Generated:   "",
		Charset:     "utf8mb4",
		Collation:   "utf8mb4_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	},
	// 33. task_archives.sha256 (Special ASCII hash payload)
	{
		Table:       "task_archives",
		Column:      "sha256",
		Type:        TText,
		Length:      0,
		NotNull:     true,
		Default:     "''",
		Generated:   "",
		Charset:     "ascii",
		Collation:   "ascii_bin",
		SQLContains: "MEDIUMTEXT CHARACTER SET ascii COLLATE ascii_bin",
	},
}

func getTableByName(name string) *Table {
	for _, t := range AllTables() {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// TestTarget33ManifestExactContracts tests all 33 explicit identity text column definitions.
func TestTarget33ManifestExactContracts(t *testing.T) {
	if len(target33Manifest) != 33 {
		t.Fatalf("manifest count = %d, want 33", len(target33Manifest))
	}

	seen := make(map[string]bool)
	for _, expected := range target33Manifest {
		key := expected.Table + "." + expected.Column
		if seen[key] {
			t.Errorf("duplicate manifest entry for %s", key)
		}
		seen[key] = true

		tbl := getTableByName(expected.Table)
		if tbl == nil {
			t.Fatalf("table %s not found in AllTables()", expected.Table)
		}

		col := findColumn(tbl, expected.Column)
		if col == nil {
			t.Fatalf("table %s: column %s not found", expected.Table, expected.Column)
		}

		if col.Type != expected.Type {
			t.Errorf("%s.%s Type = %v, want %v", expected.Table, expected.Column, col.Type, expected.Type)
		}
		if col.Length != expected.Length {
			t.Errorf("%s.%s Length = %d, want %d", expected.Table, expected.Column, col.Length, expected.Length)
		}
		if col.NotNull != expected.NotNull {
			t.Errorf("%s.%s NotNull = %v, want %v", expected.Table, expected.Column, col.NotNull, expected.NotNull)
		}
		if col.Default != expected.Default {
			t.Errorf("%s.%s Default = %q, want %q", expected.Table, expected.Column, col.Default, expected.Default)
		}
		if col.Generated != expected.Generated {
			t.Errorf("%s.%s Generated = %q, want %q", expected.Table, expected.Column, col.Generated, expected.Generated)
		}
		if col.Charset != expected.Charset {
			t.Errorf("%s.%s Charset = %q, want %q", expected.Table, expected.Column, col.Charset, expected.Charset)
		}
		if col.Collation != expected.Collation {
			t.Errorf("%s.%s Collation = %q, want %q", expected.Table, expected.Column, col.Collation, expected.Collation)
		}

		sqlType := col.SQLType()
		if !strings.Contains(sqlType, expected.SQLContains) {
			t.Errorf("%s.%s SQLType = %q, want containing %q", expected.Table, expected.Column, sqlType, expected.SQLContains)
		}
	}
}

// TestReferencedKeyCollationPairing verifies agent_key and tool_key pairings share identical collation.
func TestReferencedKeyCollationPairing(t *testing.T) {
	agentsTbl := getTableByName("agents")
	if agentsTbl == nil {
		t.Fatalf("agents table missing")
	}
	agentKey := findColumn(agentsTbl, "key")
	if agentKey == nil {
		t.Fatalf("agents.key missing")
	}

	agentRefs := []struct {
		table string
		col   string
	}{
		{"conversations", "agent_key"},
		{"agent_triggers", "agent_key"},
		{"skill_usage", "agent_key"},
		{"tool_usage", "agent_key"},
	}

	for _, ref := range agentRefs {
		tbl := getTableByName(ref.table)
		if tbl == nil {
			t.Fatalf("table %s missing", ref.table)
		}
		c := findColumn(tbl, ref.col)
		if c == nil {
			t.Fatalf("column %s.%s missing", ref.table, ref.col)
		}
		if c.Charset != agentKey.Charset || c.Collation != agentKey.Collation {
			t.Errorf("ref %s.%s charset/collation (%s/%s) does not match agents.key (%s/%s)",
				ref.table, ref.col, c.Charset, c.Collation, agentKey.Charset, agentKey.Collation)
		}
	}

	toolsTbl := getTableByName("tools")
	if toolsTbl == nil {
		t.Fatalf("tools table missing")
	}
	toolKey := findColumn(toolsTbl, "key")
	if toolKey == nil {
		t.Fatalf("tools.key missing")
	}

	toolUsageTbl := getTableByName("tool_usage")
	if toolUsageTbl == nil {
		t.Fatalf("tool_usage table missing")
	}
	toolUsageKey := findColumn(toolUsageTbl, "tool_key")
	if toolUsageKey == nil {
		t.Fatalf("tool_usage.tool_key missing")
	}

	if toolUsageKey.Charset != toolKey.Charset || toolUsageKey.Collation != toolKey.Collation {
		t.Errorf("tool_usage.tool_key charset/collation (%s/%s) does not match tools.key (%s/%s)",
			toolUsageKey.Charset, toolUsageKey.Collation, toolKey.Charset, toolKey.Collation)
	}
}

// TestCharsetGuardrails verifies that binary columns have no charset, bounded ASCII columns stay ascii_bin,
// non-identity samples retain default text behavior, and Text/TextN/TextNN helpers remain untouched.
func TestCharsetGuardrails(t *testing.T) {
	// 1. Text/TextN/TextNN helpers produce empty Charset and Collation
	t1 := Text("sample")
	if t1.Charset != "" || t1.Collation != "" {
		t.Errorf("Text() helper polluted with Charset=%q Collation=%q", t1.Charset, t1.Collation)
	}
	t2 := TextNN("sample")
	if t2.Charset != "" || t2.Collation != "" {
		t.Errorf("TextNN() helper polluted with Charset=%q Collation=%q", t2.Charset, t2.Collation)
	}
	t3 := TextN("sample", "def")
	if t3.Charset != "" || t3.Collation != "" {
		t.Errorf("TextN() helper polluted with Charset=%q Collation=%q", t3.Charset, t3.Collation)
	}

	// 2. Non-identity sample text columns retain empty Charset/Collation
	nonIdentityChecks := []struct {
		table string
		col   string
	}{
		{"findings", "name"},
		{"assets", "url"},
		{"llm_records", "model"},
		{"tasks", "name"},
		{"tasks", "description"},
	}
	for _, check := range nonIdentityChecks {
		tbl := getTableByName(check.table)
		if tbl == nil {
			t.Fatalf("non-identity check table %s missing", check.table)
		}
		c := findColumn(tbl, check.col)
		if c == nil {
			t.Fatalf("non-identity check column %s.%s missing", check.table, check.col)
		}
		if c.Charset != "" || c.Collation != "" {
			t.Errorf("non-identity column %s.%s should have empty charset/collation, got %q/%q",
				check.table, check.col, c.Charset, c.Collation)
		}
	}

	// 3. Binary columns (MEDIUMBLOB, BINARY, VARBINARY) must NOT have charset or collation
	for _, tbl := range AllTables() {
		for _, c := range tbl.Columns {
			if c.Type == TBinary || c.Type == TVarbinary || c.Type == TMediumBlob {
				if c.Charset != "" || c.Collation != "" {
					t.Errorf("binary column %s.%s must not have charset/collation, got %q/%q",
						tbl.Name, c.Name, c.Charset, c.Collation)
				}
			}
		}
	}

	// 4. Bounded ASCII columns remain ascii / ascii_bin
	asciiChecks := []struct {
		table string
		col   string
	}{
		{"traffic_exchanges", "namespace"},
		{"traffic_exchanges", "id"},
		{"traffic_evidence_snapshots", "id"},
		{"traffic_evidence_snapshots", "req_hash"},
		{"locks", "name"},
		{"business_deployment", "deployment_id"},
	}
	for _, check := range asciiChecks {
		tbl := getTableByName(check.table)
		if tbl == nil {
			t.Errorf("table %s missing", check.table)
			continue
		}
		c := findColumn(tbl, check.col)
		if c == nil {
			t.Errorf("column %s.%s missing", check.table, check.col)
			continue
		}
		if c.Charset != "ascii" || c.Collation != "ascii_bin" {
			t.Errorf("ascii column %s.%s must be ascii/ascii_bin, got %q/%q",
				check.table, check.col, c.Charset, c.Collation)
		}
	}

	// F4 闭包修订: 精确检查 Type == TText (MEDIUMTEXT) 的显式 charset 字段集合
	// Unicode 允许集合 = target33Manifest 中 32 个 utf8mb4 身份列 + 已按 spec 获准的 traffic_index_chunks.text = 33
	// ASCII 无界 TEXT 仅 task_archives.sha256 = 1
	var actualUnicodeTTexts []string
	var actualAsciiTTexts []string
	for _, tbl := range AllTables() {
		for _, c := range tbl.Columns {
			if c.Type == TText {
				if c.Charset == "utf8mb4" && c.Collation == "utf8mb4_bin" {
					actualUnicodeTTexts = append(actualUnicodeTTexts, tbl.Name+"."+c.Name)
				} else if c.Charset == "ascii" && c.Collation == "ascii_bin" {
					actualAsciiTTexts = append(actualAsciiTTexts, tbl.Name+"."+c.Name)
				} else if c.Charset != "" || c.Collation != "" {
					t.Errorf("unexpected explicit charset/collation on TText %s.%s: %q/%q",
						tbl.Name, c.Name, c.Charset, c.Collation)
				}
			}
		}
	}

	// 期望的 33 个 Unicode TText 列集合 (32 个 target33Manifest 列 + traffic_index_chunks.text)
	expectedUnicodeTTexts := make(map[string]bool)
	for _, m := range target33Manifest {
		if m.Charset == "utf8mb4" {
			expectedUnicodeTTexts[m.Table+"."+m.Column] = true
		}
	}
	expectedUnicodeTTexts["traffic_index_chunks.text"] = true

	if len(actualUnicodeTTexts) != len(expectedUnicodeTTexts) {
		t.Errorf("actual unicode TText count = %d, want %d", len(actualUnicodeTTexts), len(expectedUnicodeTTexts))
	}
	for _, key := range actualUnicodeTTexts {
		if !expectedUnicodeTTexts[key] {
			t.Errorf("unexpected unicode TText column in catalog: %s", key)
		}
	}
	for key := range expectedUnicodeTTexts {
		found := false
		for _, actualKey := range actualUnicodeTTexts {
			if actualKey == key {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected unicode TText column: %s", key)
		}
	}

	// 期望的 ASCII 无界 TEXT 列仅 task_archives.sha256 1 项
	if len(actualAsciiTTexts) != 1 {
		t.Fatalf("actual ascii TText count = %d, want 1", len(actualAsciiTTexts))
	}
	if actualAsciiTTexts[0] != "task_archives.sha256" {
		t.Errorf("actual ascii TText = %q, want task_archives.sha256", actualAsciiTTexts[0])
	}
}
