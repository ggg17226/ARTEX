package server

import (
	"os"
	"testing"
)

// TestMain 在**整个测试二进制**生命周期内打开运行时隔离开关。
//
// server.New() 正常启动时会收养数据库里所有未终态任务并跑真实的
// planner/worker、启动触发器调度器与 IM 推送、装配共享库里保存的真实 LLM 配置。
// 业务测试跑在共享 TiDB 开发库（artex_dev）上，那里存有真实用户任务与真实的
// 模型 / IM 凭据：若照旧启动，测试进程会改写别人的业务数据、消耗别人的模型额度、
// 向真实 IM 端点发消息，且结果完全不可复现。
//
// 隔离开关只关掉「会自主扫描共享库 / 外发」的后台运行时；HTTP 处理链、任务读写、
// 引擎原语（Engine/准入/归档队列）照常工作，需要用例自行注入 scripted provider。
// 详见 server.go 的 EnvIsolateRuntimes。
func TestMain(m *testing.M) {
	os.Setenv(EnvIsolateRuntimes, "1")
	os.Exit(m.Run())
}
