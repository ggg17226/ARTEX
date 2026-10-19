package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// 本文件是 P08-01 Task 1.3 的离线单元测试：只覆盖 `artex db-migrate` 的
// 参数解析 / 分派判定 / 帮助文案 / F10 版本锁定影响行数判定，
// 不建立任何数据库连接（真实 DDL 与校验路径由集成环境执行）。

// ---------------------------------------------------------------------------
// 计划 Step 1 逐字保留的最小分派测试
// ---------------------------------------------------------------------------

func TestMigrateCommand_ParseArgs(t *testing.T) {
	cmd, err := parseMigrateCommand([]string{"db-migrate", "--status"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if cmd != "status" {
		t.Fatalf("expected status, got %s", cmd)
	}
}

// ---------------------------------------------------------------------------
// parseMigrateCommand — 三个合法动作
// ---------------------------------------------------------------------------

func TestMigrateCommand_ParseThreeActions(t *testing.T) {
	for _, tc := range []struct {
		flag string
		want string
	}{
		{"--prepare", "prepare"},
		{"--verify", "verify"},
		{"--status", "status"},
	} {
		cmd, err := parseMigrateCommand([]string{"db-migrate", tc.flag})
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.flag, err)
		}
		if cmd != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.flag, cmd, tc.want)
		}
	}
}

// parseMigrateCommand 必须容忍调用方只传 flag 片段（不带前导 "db-migrate"）。
func TestMigrateCommand_ParseWithoutLeadingSubcommand(t *testing.T) {
	for _, args := range [][]string{{"--prepare"}, {"--verify"}, {"--status"}} {
		cmd, err := parseMigrateCommand(args)
		if err != nil {
			t.Fatalf("%v: unexpected error: %v", args, err)
		}
		if want := strings.TrimPrefix(args[0], "--"); cmd != want {
			t.Fatalf("%v: got %q, want %q", args, cmd, want)
		}
	}
}

// 动作只认 `--xxx` 形式：裸词 `prepare` 不是合法动作（fail-closed，避免误触发 DDL）。
func TestMigrateCommand_RejectsBareActionWord(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate", "prepare"})
	if err == nil {
		t.Fatalf("expected error for bare action word, got nil")
	}
	if !strings.Contains(err.Error(), "--prepare") {
		t.Fatalf("error must point at the valid flag form, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// parseMigrateCommand — 缺省 / 互斥 / 未知参数
// ---------------------------------------------------------------------------

func TestMigrateCommand_NoActionFails(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate"})
	if err == nil {
		t.Fatalf("expected error when no action is given, got nil")
	}
	// 缺省必须给出可操作指引：三个动作一个都不能少。
	for _, want := range []string{"--prepare", "--verify", "--status"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must mention %s, got: %v", want, err)
		}
	}
}

func TestMigrateCommand_ExclusiveActionsFail(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate", "--prepare", "--verify"})
	if err == nil {
		t.Fatalf("expected error for two actions, got nil")
	}
	if !strings.Contains(err.Error(), "--prepare") || !strings.Contains(err.Error(), "--verify") {
		t.Fatalf("error must list the conflicting actions, got: %v", err)
	}
}

func TestMigrateCommand_RepeatedActionIsExclusive(t *testing.T) {
	if _, err := parseMigrateCommand([]string{"db-migrate", "--status", "--status"}); err == nil {
		t.Fatalf("expected error for a repeated action, got nil")
	}
}

func TestMigrateCommand_UnknownArgFails(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate", "--nope"})
	if err == nil {
		t.Fatalf("expected error for unknown flag, got nil")
	}
	if !strings.Contains(err.Error(), "--nope") {
		t.Fatalf("error must echo the unknown argument, got: %v", err)
	}
}

func TestMigrateCommand_HelpWithConflictingActionsFails(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate", "--prepare", "--verify", "--help"})
	if err == nil {
		t.Fatalf("expected error for help combined with conflicting actions, got nil")
	}
}

func TestMigrateCommand_HelpWithUnknownArgFails(t *testing.T) {
	_, err := parseMigrateCommand([]string{"db-migrate", "--bogus", "--help"})
	if err == nil {
		t.Fatalf("expected error for help combined with unknown arg, got nil")
	}
}

// ---------------------------------------------------------------------------
// parseMigrateCommand — 帮助
// ---------------------------------------------------------------------------

func TestMigrateCommand_HelpFlags(t *testing.T) {
	for _, flag := range []string{"-h", "--help", "help"} {
		cmd, err := parseMigrateCommand([]string{"db-migrate", flag})
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", flag, err)
		}
		if cmd != migrateCmdHelp {
			t.Fatalf("%s: got %q, want %q", flag, cmd, migrateCmdHelp)
		}
	}
}

// ---------------------------------------------------------------------------
// 帮助文案 — 运维必须能看到三个动作与 DSN 环境变量
// ---------------------------------------------------------------------------

func TestMigrateUsage_MentionsActionsAndDSN(t *testing.T) {
	usage := migrateUsage()
	for _, want := range []string{
		"db-migrate", "--prepare", "--verify", "--status", "ARTEX_TIDB_DSN",
	} {
		if !strings.Contains(usage, want) {
			t.Fatalf("usage must mention %s, got:\n%s", want, usage)
		}
	}
}

// ---------------------------------------------------------------------------
// 分派判定 — 只有第一个位置参数是 db-migrate 才接管
// ---------------------------------------------------------------------------

func TestDispatchMigrate_UnknownOrConflictingFlags(t *testing.T) {
	// 任何前置 flag 或非 db-migrate 首参都不进入分派
	for _, args := range [][]string{
		{"--bogus", "db-migrate", "--help"},
		{"--bogus", "db-migrate", "--prepare"},
		{"--verify", "db-migrate", "--prepare"},
		{"--addr", ":8787", "db-migrate", "--status"},
	} {
		_, handled := dispatchMigrate(args)
		if handled {
			t.Fatalf("dispatchMigrate(%v) should not be handled as db-migrate subcommand", args)
		}
	}

	// 合法 db-migrate 开头但携带未知参数或冲突动作：必须 handled=true 且 code=1
	for _, args := range [][]string{
		{"db-migrate", "--bogus"},
		{"db-migrate", "--prepare", "--verify"},
		{"db-migrate", "--status", "--prepare"},
		{"db-migrate", "--help", "--bogus"},
		{"db-migrate", "--help", "--prepare"},
	} {
		code, handled := dispatchMigrate(args)
		if !handled {
			t.Fatalf("dispatchMigrate(%v) should be handled", args)
		}
		if code != 1 {
			t.Fatalf("dispatchMigrate(%v) expected code 1, got %d", args, code)
		}
	}
}

// ---------------------------------------------------------------------------
// F10 版本锁定 — 影响行数必须恰为 1
// ---------------------------------------------------------------------------

func TestVersionStampRequiresExactlyOneRow(t *testing.T) {
	if err := checkVersionStampAffected(1); err != nil {
		t.Fatalf("affected=1 must succeed, got %v", err)
	}
	for _, affected := range []int64{0, 2, -1} {
		err := checkVersionStampAffected(affected)
		if err == nil {
			t.Fatalf("affected=%d must fail, got nil", affected)
		}
		if !strings.Contains(err.Error(), "schema_version") {
			t.Fatalf("error must name schema_version, got: %v", err)
		}
	}
}

// 版本锁定 DML 必须是 key/value 条件更新：value=0 的前置条件是幂等与并发安全的唯一凭据。
func TestVersionStampSQLShape(t *testing.T) {
	sql := strings.ToLower(versionStampSQL)
	for _, want := range []string{
		"update schema_meta",
		"set `value` = 1",
		"where `key` = 'schema_version' and `value` = 0",
		"updated_at = now(3)",
	} {
		if !strings.Contains(strings.ReplaceAll(sql, "\n", " "), strings.ReplaceAll(want, "\n", " ")) {
			t.Fatalf("version stamp SQL must contain %q, got:\n%s", want, versionStampSQL)
		}
	}
}

// 版本锁定的错误必须是可判定的业务错误（非驱动错误），调用方据此回滚并非 0 退出。
func TestVersionStampErrorIsWrapped(t *testing.T) {
	err := checkVersionStampAffected(0)
	if !errors.Is(err, errVersionStamp) {
		t.Fatalf("error must wrap errVersionStamp, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// runMigrateCommand — help / 未知命令 / 无 DSN fail-closed
// ---------------------------------------------------------------------------

// help 分支绝不能触碰数据库：打印用法并以 0 退出。
func TestRunMigrateCommand_HelpPrintsUsageAndExitsZero(t *testing.T) {
	var buf bytes.Buffer
	if code := runMigrateCommand(migrateCmdHelp, &buf); code != 0 {
		t.Fatalf("help must exit 0, got %d (output: %s)", code, buf.String())
	}
	if !strings.Contains(buf.String(), "--prepare") {
		t.Fatalf("help output must contain the usage, got:\n%s", buf.String())
	}
}

// 未知命令（含空命令）必须非 0 退出并给出指引，严禁落回主服务启动逻辑。
func TestRunMigrateCommand_UnknownCommandFails(t *testing.T) {
	for _, cmd := range []string{"", "bogus", "migrate"} {
		var buf bytes.Buffer
		if code := runMigrateCommand(cmd, &buf); code == 0 {
			t.Fatalf("unknown command %q must exit non-zero", cmd)
		}
		if !strings.Contains(buf.String(), "db-migrate") {
			t.Fatalf("unknown command output must mention db-migrate, got:\n%s", buf.String())
		}
	}
}

// 三个动作在缺少配置载体时必须 fail-closed（非 0 退出），
// 且在任何建连之前停下 —— 由 ARTEX_CONFIG 显式指向缺失文件触发 config 侧拒绝。
func TestRunMigrateCommand_FailClosedWithoutDSN(t *testing.T) {
	t.Setenv("ARTEX_CONFIG", filepath.Join(t.TempDir(), "absent.yaml"))
	for _, cmd := range []string{migrateCmdPrepare, migrateCmdVerify, migrateCmdStatus} {
		var buf bytes.Buffer
		if code := runMigrateCommand(cmd, &buf); code == 0 {
			t.Fatalf("%s must exit non-zero when the DSN cannot be resolved", cmd)
		}
		if !strings.Contains(buf.String(), "ARTEX_TIDB_DSN") {
			t.Fatalf("%s output must point at ARTEX_TIDB_DSN, got:\n%s", cmd, buf.String())
		}
	}
}
