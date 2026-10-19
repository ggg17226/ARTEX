package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db/schema"
)

// TestPoolProfilesOrder pins the failover chain query: keyless profiles can't
// serve a request and excluded ones aren't fallback targets, so neither belongs
// in the chain; the rest come back by priority, highest first.
// Deliberately does NOT touch is_default — flipping the active profile would be a
// side effect on the shared dev database.
func TestPoolProfilesOrder(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})

	mk := func(name string, priority int, exclude bool, key string) int64 {
		id, err := d.SaveProfile(&LLMProfile{
			Name: name, Format: "openai", Model: "m", APIKey: key,
			Priority: priority, PoolExclude: exclude,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := d.Exec(`DELETE FROM llm_profiles WHERE id=?`, id); err != nil {
				t.Errorf("cleanup profile %d failed: %v", id, err)
			}
			var count int
			if err := d.QueryRow(`SELECT count(*) FROM llm_profiles WHERE id=?`, id).Scan(&count); err != nil {
				t.Errorf("query residual profile %d failed: %v", id, err)
			}
			if count != 0 {
				t.Errorf("profile %d residual found after cleanup", id)
			}
		})
		return id
	}
	lo := mk("t-pool-lo", 1, false, "k1")
	hi := mk("t-pool-hi", 9, false, "k2")
	mk("t-pool-excluded", 99, true, "k3") // excluded despite the top priority
	mk("t-pool-nokey", 50, false, "")     // no key → cannot serve anything

	chain, err := d.PoolProfiles()
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, p := range chain {
		switch p.Name {
		case "t-pool-lo", "t-pool-hi":
			got = append(got, p.ID)
		case "t-pool-excluded":
			t.Fatal("pool_exclude profile entered the failover chain")
		case "t-pool-nokey":
			t.Fatal("keyless profile entered the failover chain")
		}
	}
	if len(got) != 2 || got[0] != hi || got[1] != lo {
		t.Fatalf("chain order = %v, want [hi=%d lo=%d]", got, hi, lo)
	}
}

func TestDeleteProfileContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var d DB
	if err := d.DeleteProfileContext(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteProfileContext error=%v, want context cancellation", err)
	}
}

func TestConfigStores(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatalf("open TiDB failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})

	// 记录原有的 active profile（如果存在），以便测试结束时恢复
	var origActiveID *int64
	if act, err := d.ActiveProfile(); err == nil && act != nil {
		id := act.ID
		origActiveID = &id
	}
	t.Cleanup(func() {
		if origActiveID != nil {
			_ = d.SetActiveProfile(*origActiveID)
		}
	})

	// LLM profile: save, active, key never serialized
	pid, err := d.SaveProfile(&LLMProfile{Name: "t-default", Format: "openai", Model: "gpt-x", APIKey: "secret123"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec(`DELETE FROM llm_profiles WHERE id=?`, pid); err != nil {
			t.Errorf("cleanup llm_profile %d failed: %v", pid, err)
		}
		var count int
		if err := d.QueryRow(`SELECT count(*) FROM llm_profiles WHERE id=?`, pid).Scan(&count); err != nil {
			t.Errorf("query residual llm_profile failed: %v", err)
		}
		if count != 0 {
			t.Errorf("residual llm_profile %d found after cleanup", pid)
		}
	})
	if err := d.SetActiveProfile(pid); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteProfile(pid); !errors.Is(err, ErrActiveLLMProfileDelete) {
		t.Fatalf("deleting active profile error=%v, want %v", err, ErrActiveLLMProfileDelete)
	}
	act, err := d.ActiveProfile()
	if err != nil || act == nil || act.APIKey != "secret123" {
		t.Fatalf("active profile/key: %+v err=%v", act, err)
	}
	// list must hide the key, expose hint
	list, _ := d.ListProfiles()
	for _, p := range list {
		if p.ID == pid {
			b, _ := json.Marshal(p)
			if string(b) == "" || contains(string(b), "secret123") {
				t.Fatalf("api key leaked in list json: %s", b)
			}
			if p.APIKeyHint != "…t123" {
				t.Fatalf("hint want …t123, got %q", p.APIKeyHint)
			}
		}
	}

	// agents seeded; prompt versioning
	ag, err := d.GetAgentByKey("planner")
	if err != nil || ag == nil {
		t.Fatalf("planner agent: %v", err)
	}
	v1, err := d.SavePrompt(ag.ID, "你是规划者 {{.Goal}}", "init", "test")
	if err != nil {
		t.Fatal(err)
	}
	v2, _ := d.SavePrompt(ag.ID, "你是规划者 v2 {{.Goal}} {{.Scope}}", "edit", "test")
	if v2 != v1+1 {
		t.Fatalf("version should increment: %d -> %d", v1, v2)
	}
	cur, _ := d.CurrentPrompt(ag.ID)
	if cur != "你是规划者 v2 {{.Goal}} {{.Scope}}" {
		t.Fatalf("current prompt wrong: %q", cur)
	}
	vers, _ := d.ListPromptVersions(ag.ID)
	if len(vers) < 2 {
		t.Fatalf("want >=2 versions, got %d", len(vers))
	}
	pv, _ := d.PromptVars(ag.ID)
	// planner must have at least the seeded catalog vars (Goal, AssetSummary)
	if len(pv) < 2 {
		t.Fatalf("planner catalog want >=2 vars, got %d", len(pv))
	}
	d.Exec(`DELETE FROM agent_prompts WHERE agent_id=?`, ag.ID)
	d.Exec(`UPDATE agents SET current_prompt_id=NULL WHERE id=?`, ag.ID)

	// mcp + skill + visibility (bidirectional via one join)
	// Clean up any leftover MCP from prior runs to keep this test idempotent.
	d.Exec(`DELETE FROM mcp_servers WHERE name = 't-gh'`)
	mid, err := d.SaveMCP(&MCPServer{Name: "t-gh", Transport: "stdio", Command: "npx", Args: json.RawMessage(`["server-github"]`), Env: json.RawMessage(`{"GITHUB_TOKEN":"x"}`), Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// agent-side write. MCP is id-keyed (generic visibility join); skills are now
	// filesystem-based, so their visibility is keyed by skill (directory) name in a
	// dedicated table. Clear any pre-existing MCP visibility rows so the assertions
	// below isolate on exactly what this test sets.
	d.Exec(`DELETE FROM agent_visibility WHERE agent_id = ? AND resource_kind = 'mcp'`, ag.ID)
	if err := d.ToggleVisibility(ag.ID, "mcp", mid, true); err != nil {
		t.Fatal(err)
	}
	if err := d.SetAgentSkillVisibility(ag.ID, nil); err != nil {
		t.Fatal(err)
	}
	if err := d.ToggleSkillVisibility(ag.ID, "t-skill", true); err != nil {
		t.Fatal(err)
	}
	// agent-side read
	vm, _ := d.AgentVisible(ag.ID, "mcp")
	if len(vm) != 1 || vm[0] != mid {
		t.Fatalf("agent visible mcp: %+v", vm)
	}
	// resource-side read (same join row) → bidirectional
	ra, _ := d.ResourceAgents("mcp", mid)
	if len(ra) != 1 || ra[0] != ag.ID {
		t.Fatalf("resource agents: %+v", ra)
	}
	// toggle off
	d.ToggleVisibility(ag.ID, "mcp", mid, false)
	vm2, _ := d.AgentVisible(ag.ID, "mcp")
	if len(vm2) != 0 {
		t.Fatalf("after toggle off: %+v", vm2)
	}
	// skill visibility is name-keyed: verify the read, then deleting the skill's
	// visibility rows (called when a skill is removed) clears it.
	names, _ := d.AgentSkillNames(ag.ID)
	if len(names) != 1 || names[0] != "t-skill" {
		t.Fatalf("agent visible skills: %+v", names)
	}
	if err := d.DeleteSkillVisibility("t-skill"); err != nil {
		t.Fatal(err)
	}
	names2, _ := d.AgentSkillNames(ag.ID)
	if len(names2) != 0 {
		t.Fatalf("skill visibility should be cleared on delete: %+v", names2)
	}
	d.DeleteMCP(mid)
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestSettings_GetSetting_TiDBRequirement 是 Task 1.4 Step 1 的设置项原生持久化用例：
// 写入必须同时补齐 key_identity/key_hash（新库唯一键落在 key_hash 上），读取走
// 反引号保护的 `key` 谓词，upsert 用 TiDB 原生 ON DUPLICATE KEY UPDATE。
// 未配置 ARTEX_TIDB_DSN 时显式声明跳过；配置后必须真实连库并 PASS。
func TestSettings_GetSetting_TiDBRequirement(t *testing.T) {
	dsn := os.Getenv("ARTEX_TIDB_DSN")
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set, skipping integration test")
	}
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})

	testKey := fmt.Sprintf("p08_setting_%d", time.Now().UnixNano())
	testVal := "p08_val"
	if err := d.SetSetting(testKey, testVal); err != nil {
		t.Fatalf("SetSetting failed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec("DELETE FROM settings WHERE `key` = ?", testKey); err != nil {
			t.Errorf("cleanup setting failed: %v", err)
		}
	})

	val, ok, err := d.GetSetting(testKey)
	if err != nil || !ok || val != testVal {
		t.Fatalf("GetSetting failed: got (%q, %v, %v), want (%q, true, nil)", val, ok, err, testVal)
	}

	// 覆盖写必须走 ON DUPLICATE KEY UPDATE：同 key 再写一次只改值，不新增行。
	if err := d.SetSetting(testKey, "p08_val2"); err != nil {
		t.Fatalf("SetSetting overwrite failed: %v", err)
	}
	var rows int
	if err := d.QueryRow("SELECT COUNT(*) FROM settings WHERE `key` = ?", testKey).Scan(&rows); err != nil {
		t.Fatalf("count settings failed: %v", err)
	}
	if rows != 1 {
		t.Fatalf("upsert produced %d rows, want 1", rows)
	}
	val, ok, err = d.GetSetting(testKey)
	if err != nil || !ok || val != "p08_val2" {
		t.Fatalf("GetSetting after upsert failed: got (%q, %v, %v), want (%q, true, nil)", val, ok, err, "p08_val2")
	}

	// 身份列必须落库：key_hash 是这条记录唯一的自然键载体。
	var identity, hash []byte
	if err := d.QueryRow("SELECT key_identity, key_hash FROM settings WHERE `key` = ?", testKey).Scan(&identity, &hash); err != nil {
		t.Fatalf("read identity columns failed: %v", err)
	}
	wantIdentity := schema.SettingsKeyIdentity(testKey)
	wantHash := schema.CanonicalHash(wantIdentity)
	if string(identity) != string(wantIdentity) {
		t.Fatalf("key_identity = %x, want %x", identity, wantIdentity)
	}
	if len(hash) != 32 || string(hash) != string(wantHash[:]) {
		t.Fatalf("key_hash = %x, want %x", hash, wantHash)
	}
}
