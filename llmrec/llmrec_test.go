package llmrec

import (
	"context"
	"iter"
	"os"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/transcript"
	"github.com/google/uuid"
)

type completeProvider struct{}

func (completeProvider) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(func(llm.StreamEvent, error) bool) {}
}

func (completeProvider) Complete(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	return llm.Message{
		Role: llm.RoleAssistant,
		Content: []llm.ContentBlock{
			{Type: llm.BlockThinking, Thinking: "reasoning"},
			llm.TextBlock("answer"),
		},
	}, "stop", llm.Usage{InputTokens: 7, OutputTokens: 3}, nil
}

func TestTaskIDContextUsesExplicitRegistryID(t *testing.T) {
	ctx := WithTaskID(context.Background(), " 42 ")
	if got := TaskIDFrom(ctx); got != "42" {
		t.Fatalf("TaskIDFrom()=%q want 42", got)
	}
	if got := TaskIDFrom(WithTaskID(ctx, "   ")); got != "42" {
		t.Fatalf("blank task id should preserve parent context, got %q", got)
	}
	if got := TaskIDFrom(nil); got != "" {
		t.Fatalf("nil context returned %q", got)
	}
}

func TestParseSessionFallbackIsExplorationScoped(t *testing.T) {
	taskID, worker := parseSession("exp12-worker-i99")
	if taskID != "12" || worker != "worker" {
		t.Fatalf("parseSession()=(%q,%q)", taskID, worker)
	}
	if taskID, worker := parseSession("not-a-task"); taskID != "" || worker != "" {
		t.Fatalf("unexpected non-session parse: (%q,%q)", taskID, worker)
	}
}

func TestCompleteForwardsAtomicResponse(t *testing.T) {
	recorder := Wrap(completeProvider{}, nil, "model", "profile", "", "", func() bool { return false })
	msg, stopReason, usage, err := recorder.Complete(context.Background(), llm.CompletionRequest{})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if got := msg.Text(); got != "answer" {
		t.Fatalf("Complete() text=%q want answer", got)
	}
	if stopReason != "stop" {
		t.Fatalf("Complete() stop reason=%q want stop", stopReason)
	}
	if usage.InputTokens != 7 || usage.OutputTokens != 3 {
		t.Fatalf("Complete() usage=%+v", usage)
	}
}

type meteredStreamProvider struct{ completeProvider }

func (meteredStreamProvider) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		if !yield(llm.StreamEvent{Type: llm.SEMessageStart, Usage: llm.Usage{InputTokens: 23}}, nil) {
			return
		}
		if !yield(llm.StreamEvent{Type: llm.SEMessageDelta, Usage: llm.Usage{OutputTokens: 5}}, nil) {
			return
		}
		yield(llm.StreamEvent{Type: llm.SEMessageStop}, nil)
	}
}

func TestSideUsageRecordedOnceOnConsumerCancellation(t *testing.T) {
	// 连库门控（spec §3.1.1 / §4.3）：ARTEX_PG_DSN 与 postgres:// 已彻底废除。
	//   - 没配 ARTEX_TIDB_DSN → 诚实 t.Skip，绝不伪装成通过；
	//   - 配了却打不开 → 环境违规，必须 t.Fatalf，不许再退回 Skip 假绿。
	dsn := strings.TrimSpace(os.Getenv("ARTEX_TIDB_DSN"))
	if dsn == "" {
		t.Skip("ARTEX_TIDB_DSN not set")
	}
	pg, err := db.Open(dsn)
	if err != nil {
		t.Fatalf("ARTEX_TIDB_DSN 已配置但打开 TiDB 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	t.Cleanup(func() {
		if err := pg.Close(); err != nil {
			t.Errorf("关闭 TiDB 连接池失败: %v", err)
		}
	})

	// 每一轮一个随机 profile 名，既是唯一的物理判据，也保证清理只会圈到本轮行。
	trackProfile := func(profile string) {
		t.Helper()
		t.Cleanup(func() {
			if _, err := pg.Exec(`DELETE FROM llm_usage WHERE profile_name=?`, profile); err != nil {
				t.Errorf("清理 llm_usage fixture 失败 (profile=%s): %v", profile, err)
			}
			var left int
			if err := pg.QueryRow(`SELECT count(*) FROM llm_usage WHERE profile_name=?`, profile).Scan(&left); err != nil {
				t.Errorf("回读 llm_usage fixture 失败 (profile=%s): %v", profile, err)
				return
			}
			if left != 0 {
				t.Errorf("llm_usage fixture 未归零 (profile=%s): 残留 %d 行", profile, left)
			}
		})
	}

	for _, early := range []bool{false, true} {
		profile := "btw-metering-" + uuid.NewString()
		trackProfile(profile)
		ctx, cancel := context.WithCancel(transcript.WithSessionID(t.Context(), "exp0-btw-test"))
		recorder := Wrap(meteredStreamProvider{}, pg, "fixture", profile, "", "", func() bool { return false })
		for event, err := range recorder.Stream(ctx, llm.CompletionRequest{}) {
			if err != nil {
				t.Fatal(err)
			}
			if early && event.Type == llm.SEMessageDelta {
				cancel()
				break // Consumer exits before the provider can return a cancellation event.
			}
		}
		cancel()
		var count, input, output int
		var worker, status string
		err = pg.QueryRow(`SELECT count(*),max(input_tokens),max(output_tokens),max(worker),max(status)
FROM llm_usage WHERE profile_name=?`, profile).Scan(&count, &input, &output, &worker, &status)
		if err != nil {
			t.Fatal(err)
		}
		wantStatus := "ok"
		if early {
			wantStatus = "error"
		}
		if count != 1 || input != 23 || output != 5 || worker != "btw" || status != wantStatus {
			t.Fatalf("early=%v: count=%d usage=%d/%d worker=%q status=%q", early, count, input, output, worker, status)
		}
	}
}
