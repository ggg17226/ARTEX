package server

// 本文件是 server 包业务测试访问真实 TiDB 的**唯一**入口。
//
// 纪律（spec §4.3「业务测试在 ARTEX_TIDB_DSN 开启时必须 100% 真实执行且 PASS，
// 禁止伪造全局 Skip」）：
//   - 只有「DSN 完全未配置」才允许 t.Skip，且消息必须写明原因；
//   - DSN 已配置却连不上库 / 建不出 Manager，一律属于环境违规，必须 t.Fatal 判红，
//     绝不允许再退回任何形式的无条件 t.Skip（那会把整批业务测试静默变成绿灯）；
//   - fixture 必须随机唯一命名，并按逆拓扑在 t.Cleanup 中删除、回读归零；
//     清理失败必须 t.Errorf 标红（共享库里留孤儿行会污染后续用例与终态核验）。
//
// 生命周期约定：t.Cleanup 是 LIFO。newTestManager / newTestDB **最先**登记 Close，
// 因此它**最后**执行 —— 用例随后登记的清理语句仍然跑在活着的连接池上。
// 用例不得再自己写 `defer m.Close()` / `defer pg.Close()`。

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

// tidbDSNOrSkip 返回集成测试要用的 TiDB 连接串。
// 未配置（db.DSN() 报错）→ 诚实 t.Skip；已配置 → 直接返回，由调用方判红。
func tidbDSNOrSkip(t *testing.T) string {
	t.Helper()
	dsn, _, err := db.DSN()
	if err != nil {
		t.Skipf("TiDB 数据源未配置（%v）— 本用例未执行（honest skip，需要 ARTEX_TIDB_DSN）", err)
	}
	return dsn
}

// tidbConfigured 报告当前进程是否配置了 TiDB 数据源（不触发 skip）。
func tidbConfigured() bool {
	_, _, err := db.DSN()
	return err == nil
}

// newTestManager 建出一个连着真实 TiDB 的 Manager（data 目录用 t.TempDir()）。
//
// 语义与 db 包 openTestDB 完全一致：DSN 缺失 → Skip；DSN 在但建不出 Manager → Fatal。
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	return newTestManagerIn(t, t.TempDir())
}

// newTestManagerIn 同 newTestManager，但由调用方指定 data 目录
// （需要断言任务文件落盘/回滚的用例必须自己掌握目录）。
func newTestManagerIn(t *testing.T, dir string) *Manager {
	t.Helper()
	m, err := NewManager(dir, "")
	if err != nil {
		if !tidbConfigured() {
			t.Skipf("TiDB 数据源未配置（%v）— 本用例未执行（honest skip，需要 ARTEX_TIDB_DSN）", err)
		}
		t.Fatalf("ARTEX_TIDB_DSN 已配置但 NewManager 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("关闭 Manager 失败: %v", err)
		}
	})
	return m
}

// newTestDB 打开一份真实的 TiDB 连接池（供只需要 *db.DB 的用例）。
func newTestDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open(tidbDSNOrSkip(t))
	if err != nil {
		t.Fatalf("ARTEX_TIDB_DSN 已配置但打开 TiDB 失败（环境违规，必须判红而不是 Skip）: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("关闭 TiDB 连接池失败: %v", err)
		}
	})
	return d
}

// sqlExecer / sqlQueryer 让清理 helper 同时吃 *db.DB 与 *sql.DB。
type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

type sqlQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

// mustCleanup 执行一条 fixture 清理语句并检查错误（清理失败绝不吞错）。
func mustCleanup(t *testing.T, e sqlExecer, query string, args ...any) {
	t.Helper()
	if _, err := e.Exec(query, args...); err != nil {
		t.Errorf("清理 fixture 失败: %q args=%v: %v", query, args, err)
	}
}

// mustCount 回读一个 COUNT(*)，用于清理后的归零断言。
func mustCount(t *testing.T, q sqlQueryer, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := q.QueryRow(query, args...).Scan(&n); err != nil {
		t.Errorf("回读计数失败: %q args=%v: %v", query, args, err)
		return -1
	}
	return n
}

// assertZeroAfterCleanup 断言某个作用域内已无残留 fixture。
func assertZero(t *testing.T, q sqlQueryer, query string, args ...any) {
	t.Helper()
	if n := mustCount(t, q, query, args...); n > 0 {
		t.Errorf("清理后仍有 %d 行残留: %q args=%v", n, query, args)
	}
}

// wireID 解析 wire 上的整型 id。迁移后的 DTO 把 int64 id 一律序列化成十进制字符串
// （见 dto.i64s），这里同时兼容仍以 JSON number 出现的字段，避免再用
// `.(float64)` 直接断言而 panic 掉整个测试二进制。
func wireID(t *testing.T, v any) int64 {
	t.Helper()
	switch id := v.(type) {
	case string:
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatalf("wire id %q 不是十进制整数: %v", id, err)
		}
		return n
	case float64:
		return int64(id)
	case json.Number:
		n, err := id.Int64()
		if err != nil {
			t.Fatalf("wire id %s 解析失败: %v", id, err)
		}
		return n
	default:
		t.Fatalf("无法识别的 wire id 类型 %T (%v)", v, v)
		return 0
	}
}

// insertRowID 插入一行并取回自增主键（TiDB 没有 INSERT … RETURNING）。
// 返回的 id 必须被调用方记录进 t.Cleanup 的逆拓扑删除列表。
func insertRowID(t *testing.T, e sqlExecer, query string, args ...any) int64 {
	t.Helper()
	res, err := e.Exec(query, args...)
	if err != nil {
		t.Fatalf("插入 fixture 失败: %q args=%v: %v", query, args, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("读取 LastInsertId 失败: %q: %v", query, err)
	}
	return id
}

// mustDeleteTask 用产品 API 删掉本用例创建的 task（产品删除自带引用级联清理），
// 并按精确物理主键回读归零；任何一步失败都 t.Errorf 标红 —— 共享库里留下没人认领
// 的 tasks/explorations 行，既污染下一轮用例，也让终态核验无法归因。
func mustDeleteTask(t *testing.T, m *Manager, id string, opts ...DeleteTaskOptions) {
	t.Helper()
	o := DeleteTaskOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if _, err := m.DeleteTask(id, o); err != nil {
		t.Errorf("删除任务 %s 失败: %v", id, err)
		return
	}
	n, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
	if err != nil {
		t.Errorf("任务 id %q 不是十进制主键: %v", id, err)
		return
	}
	assertZero(t, m.pg, `SELECT COUNT(*) FROM tasks WHERE id=?`, n)
}

// mustDeleteTaskRow 按精确物理主键删掉本用例创建的 task 行并回读归零。
// 用于只需要 db 层删除（不经 Manager 的运行时记账）的用例。
func mustDeleteTaskRow(t *testing.T, m *Manager, id int64) {
	t.Helper()
	if err := m.pg.DeleteTask(id); err != nil {
		t.Errorf("删除任务行 %d 失败: %v", id, err)
	}
	assertZero(t, m.pg, `SELECT COUNT(*) FROM tasks WHERE id=?`, id)
}

// deferDeleteTask 登记"用一条**全新连接**删掉自己 task"的清理。
// 专用于那些为了制造存储层失败而必须在用例体内 Close 连接池的用例：
// 那种时刻 manager 已经没有任何可用连接，常规 Cleanup 删不动，
// 共享库上就会每轮多出一行没人认领的 tasks。
func deferDeleteTask(t *testing.T, id string) {
	t.Helper()
	dsn := tidbDSNOrSkip(t)
	n, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
	if err != nil {
		t.Fatalf("任务 id %q 不是十进制主键: %v", id, err)
	}
	t.Cleanup(func() {
		conn, err := db.Open(dsn)
		if err != nil {
			t.Errorf("为清理 task %d 打开连接失败: %v", n, err)
			return
		}
		defer conn.Close()
		if err := conn.DeleteTask(n); err != nil {
			t.Errorf("删除任务行 %d 失败: %v", n, err)
		}
		assertZero(t, conn, `SELECT COUNT(*) FROM tasks WHERE id=?`, n)
	})
}

// fixtureStamps 给同一用例内的多个 fixture 名字生成随机且唯一的前缀。
//
// 只依赖测试自己的随机性（用例名 + 指针地址），因此不同用例即便在同一微秒创建
// fixture 也不会撞名；共享库里所有 fixture 都带 "__t_<用例名>_<随机>_" 前缀，
// 便于事后按前缀统计残留。
var fixtureStampRe = regexp.MustCompile(`[^A-Za-z0-9_]+`)

func fixturePrefix(t *testing.T) string {
	return fixtureStampRe.ReplaceAllString(strings.TrimPrefix(t.Name(), "Test"), "_")
}
