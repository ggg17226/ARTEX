package main

import (
	"fmt"

	"github.com/Autumn-27/artex/db/schema"
)

func main() {
	want := map[string]bool{}
	for _, t := range []string{"tasks", "explorations", "exploration_nodes", "exploration_edges", "exploration_anchors",
		"task_constraints", "activity", "task_relations", "task_asset_links", "task_llm_profiles", "task_scope",
		"findings", "finding_traffic_bindings", "traffic_evidence_snapshots", "llm_records", "llm_usage",
		"skill_usage", "tool_usage", "intercept_pending", "side_question_sessions", "side_question_requests",
		"assets", "task_archives"} {
		want[t] = true
	}
	for _, t := range schema.AllTables() {
		if !want[t.Name] {
			continue
		}
		fmt.Printf("## %s pk=%s autoinc=%v\n", t.Name, t.PKColumn, t.AutoIncrement)
		for _, c := range t.Columns {
			flags := ""
			if c.NotNull {
				flags += " NN"
			}
			if c.AutoInc {
				flags += " AI"
			}
			if c.Generated != "" {
				flags += " GEN=" + c.Generated
			}
			if c.Default != "" {
				flags += " DEF=" + c.Default
			}
			fmt.Printf("  %s %d%s\n", c.Name, c.Type, flags)
		}
	}
}
