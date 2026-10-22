package db

import (
	"fmt"
	"testing"
	"time"
)

func TestListTasksBulkHydratesTaskContext(t *testing.T) {
	d := openTestDB(t)

	stamp := time.Now().UnixNano()
	profileID, err := d.SaveProfile(&LLMProfile{
		Name: fmt.Sprintf("bulk-list-profile-%d", stamp), Format: "openai", Model: "test", APIKey: "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.DeleteProfile(profileID) })
	source, err := d.CreateTask(fmt.Sprintf("bulk-list-source-%d", stamp), "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.DeleteTask(source.ID) })
	companyID, _, err := d.Companies().UpsertCompany(fmt.Sprintf("Bulk List Company %d", stamp), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Companies().DeleteCompany(companyID) })
	child, err := d.CreateTaskWithOptions("bulk-list-child", "goal", TaskCreateOptions{
		LLMProfileIDs: []int64{profileID},
		SourceTaskIDs: []int64{source.ID},
		CompanyIDs:    []int64{companyID},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.DeleteTask(child.ID) })

	tasks, err := d.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	var got *Task
	for _, task := range tasks {
		if task.ID == child.ID {
			got = task
			break
		}
	}
	if got == nil {
		t.Fatalf("task %d missing from list", child.ID)
	}
	if len(got.LLMProfileIDs) != 1 || got.LLMProfileIDs[0] != profileID ||
		got.ActiveLLMProfileID == nil || *got.ActiveLLMProfileID != profileID || got.LLMFailoverState != "ready" {
		t.Fatalf("LLM context was not bulk hydrated: %+v", got)
	}
	if len(got.SourceTaskIDs) != 1 || got.SourceTaskIDs[0] != source.ID {
		t.Fatalf("source task context was not bulk hydrated: %v", got.SourceTaskIDs)
	}
	if len(got.CompanyIDs) != 1 || got.CompanyIDs[0] != companyID {
		t.Fatalf("company context was not bulk hydrated: %v", got.CompanyIDs)
	}
}

func TestTaskListMetricsAll(t *testing.T) {
	d := openTestDB(t)

	task, err := d.CreateTask("task-list-metrics", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.DeleteTask(task.ID) })

	resultAt := time.Now().Add(-time.Second).Truncate(time.Second)
	latestAt := resultAt.Add(time.Second)
	// TiDB 严格模式：activity.metadata 是 NOT NULL 且 TEXT 无 DDL 默认值，必须显式给；
	// 同一列在两条 VALUES 里各绑一次，实参按 SQL 文本出现顺序重复传。
	if _, err := d.Exec(`
INSERT INTO activity(
    exploration_id, kind, metadata, input_tokens, output_tokens,
    cache_read_tokens, cache_write_tokens, created_at
) VALUES (?,'result','{}',11,7,3,2,?), (?,'tool_result','{}',999,999,999,999,?)`,
		task.ExplorationID, resultAt, task.ExplorationID, latestAt); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(`
INSERT INTO exploration_nodes(exploration_id, kind, payload, state)
VALUES (?,'goal','{}','met'), (?,'goal','{}','open')`, task.ExplorationID, task.ExplorationID); err != nil {
		t.Fatal(err)
	}

	all, err := d.TaskListMetricsAll()
	if err != nil {
		t.Fatal(err)
	}
	metrics, ok := all[task.ExplorationID]
	if !ok {
		t.Fatalf("metrics for exploration %d missing", task.ExplorationID)
	}
	if metrics.Tokens.InputTokens != 11 || metrics.Tokens.OutputTokens != 7 ||
		metrics.Tokens.CacheReadTokens != 3 || metrics.Tokens.CacheWriteTokens != 2 {
		t.Fatalf("unexpected token metrics: %+v", metrics.Tokens)
	}
	if metrics.LastActivity != latestAt.Unix() {
		t.Fatalf("last activity=%d, want %d", metrics.LastActivity, latestAt.Unix())
	}
	if metrics.Goals.Total != 2 || metrics.Goals.Met != 1 {
		t.Fatalf("unexpected goal metrics: %+v", metrics.Goals)
	}
}
