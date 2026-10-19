package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Autumn-27/artex/config"
	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// 本文件实现 P08 Spec §1.1 第 3 条 / §3.1.2 第 3 点要求的运维子命令体系
// `artex db-migrate`，用来彻底替代历史临时建表脚本：
//
//	artex db-migrate --prepare  建立并修复 65 表基线 + 基础设施 seed + 只读核验 + 版本锁定
//	artex db-migrate --verify   纯只读核验当前库是否等于声明式基线
//	artex db-migrate --status   纯只读输出运行时元数据快照（版本 / sql_mode / 表数 / 版本戳 / 维护锁）
//
// 三条硬约束（Spec §1.2、§3.1.2）：
//  1. --prepare 只服务全新实例初始化，不导入任何存量数据；DDL 全部幂等
//     （CREATE TABLE IF NOT EXISTS + INSERT IGNORE），但版本锁定 0→1 只允许成功一次；
//  2. --verify / --status 严格只读，绝不执行写入、锁释放或 SET GLOBAL；
//  3. 命令由 main 在任何服务启动之前分派，执行结束即以自身退出码终止进程。

const migrateSubcommand = "db-migrate"

// 动作标识（parseMigrateCommand 的返回值，不含 "--" 前缀）。
const (
	migrateCmdPrepare = "prepare"
	migrateCmdVerify  = "verify"
	migrateCmdStatus  = "status"
	migrateCmdHelp    = "help"
)

// migratePrepareTimeout / migrateReadTimeout 是两条路径的 ctx 上限：
// prepare 要跑 65 条 DDL（TiDB DDL 逐条排队，冷集群可能分钟级），读路径只查字典。
const (
	migratePrepareTimeout = 30 * time.Minute
	migrateReadTimeout    = 3 * time.Minute
)

// versionStampSQL 是 F10 版本绑定 DML（Spec §3.1.2 第 3 点逐字口径）：
// 只有 `key`='schema_version' 且 `value`=0 的那一行能被置 1，条件本身即
// 幂等与并发安全的唯一凭据（重复执行影响 0 行 → 显式失败，绝不静默二次绑定）。
// `key` / `value` 是 MySQL 保留字，必须反引号包裹。
const versionStampSQL = "UPDATE schema_meta\n" +
	"SET `value` = 1, updated_at = NOW(3)\n" +
	"WHERE `key` = 'schema_version' AND `value` = 0"

// errVersionStamp 标识"版本锁定未恰好影响 1 行"，调用方据此回滚并非 0 退出。
var errVersionStamp = errors.New("schema_version 版本锁定失败")

// migrateAction 是一条动作的元数据（解析、互斥判定与帮助文案共用同一真源）。
type migrateAction struct {
	flag string // 命令行写法，如 --prepare
	cmd  string // parseMigrateCommand 返回值，如 prepare
	desc string // 帮助文案
}

// migrateActions 是全部合法动作；三者互斥且必选其一。
var migrateActions = []migrateAction{
	{
		flag: "--prepare",
		cmd:  migrateCmdPrepare,
		desc: "建立/修复 65 表基线 + 基础设施 seed + 只读基线核验 + schema_version 版本锁定（0→1）",
	},
	{
		flag: "--verify",
		cmd:  migrateCmdVerify,
		desc: "纯只读核验当前库结构是否等于 65 表声明式基线",
	},
	{
		flag: "--status",
		cmd:  migrateCmdStatus,
		desc: "纯只读输出 TiDB 版本 / sql_mode / 表数量 / schema_version / 维护锁状态",
	},
}

// actionByFlag 把命令行写法映射到动作标识。
func actionByFlag(flag string) (string, bool) {
	for _, a := range migrateActions {
		if a.flag == flag {
			return a.cmd, true
		}
	}
	return "", false
}

// actionFlags 返回 "--prepare | --verify | --status" 形式的动作摘要（错误文案用）。
func actionFlags() string {
	flags := make([]string, 0, len(migrateActions))
	for _, a := range migrateActions {
		flags = append(flags, a.flag)
	}
	return strings.Join(flags, " | ")
}

// migrateUsage 返回 db-migrate 的完整帮助文本（运维无需翻文档即可自助）。
func migrateUsage() string {
	var b strings.Builder
	b.WriteString("artex " + migrateSubcommand + " —— ARTEX 原生 TiDB 基线运维命令\n\n")
	b.WriteString("用法：\n  artex " + migrateSubcommand + " <" + actionFlags() + ">\n\n动作（三者互斥，必须且只能指定其中一个）：\n")
	for _, a := range migrateActions {
		fmt.Fprintf(&b, "  %-10s %s\n", a.flag, a.desc)
	}
	b.WriteString(`
  -h, --help   打印本帮助并退出（退出码 0）

连接来源（由 config.TiDBDSN 解析，日志一律脱敏）：
  1. 环境变量 ARTEX_TIDB_DSN（最高优先级，整体覆盖）
  2. database.dsn（环境变量 ARTEX_DATABASE_DSN 或配置文件字段）
  3. database.host / port / user / password / dbname 散列字段（默认端口 4000）
  配置文件默认为 <二进制目录>/config.yaml，可用环境变量 ARTEX_CONFIG 指定。

注意事项：
  · --prepare 仅用于全新实例初始化，不导入任何存量数据；
  · DDL 与 seed 幂等（IF NOT EXISTS / INSERT IGNORE），但版本锁定 schema_version 0→1
    只允许成功一次，重复执行会显式失败并回滚，杜绝未绑定环境启动；
  · --verify 与 --status 全程只读，不执行任何写入、锁释放或 SET GLOBAL。
`)
	return b.String()
}

// firstPositionalIndex 返回第一个非 flag 参数的下标；不存在时返回 -1。
// 空字符串按位置参数处理（不等价于任何子命令名，因此天然不匹配）。
func firstPositionalIndex(args []string) int {
	for i, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			return i
		}
	}
	return -1
}

// migrateRequested 报告参数切片（os.Args[1:]）是否在请求 db-migrate 子命令：
// 只有第一个位置参数恰好等于 "db-migrate" 才算命中，
// 从而不会把 "--addr :8787 db-migrate" 这类"flag 取值"误判成分派点。
func migrateRequested(args []string) bool {
	return len(args) > 0 && args[0] == migrateSubcommand
}

// parseMigrateCommand 解析 db-migrate 的动作参数，返回 "prepare" | "verify" |
// "status" | "help"。
//
// args 允许携带前导的子命令名本身（["db-migrate", "--status"]，只认第一个），
// 也允许只传 flag 片段（["--status"]）。动作只认 "--xxx" 写法：裸词 "prepare"
// 一律按未知参数拒绝，避免任何误触发建表 DDL 的空间。
//
// 互斥性与严格校验：
//   - 扫描全部后续参数，若存在任何未知参数（即使同时携带了 -h/--help），一律立即返回未知参数错误并拒绝；
//   - 收集到的合法动作数量必须恰好为 1，0 个报错缺省，≥2 个报错互斥。
func parseMigrateCommand(args []string) (string, error) {
	var gotFlags, gotCmds []string
	hasHelp := false
	for i, arg := range args {
		if i == 0 && arg == migrateSubcommand {
			continue
		}
		if arg == "-h" || arg == "--help" || arg == "help" {
			hasHelp = true
			continue
		}
		cmd, ok := actionByFlag(arg)
		if !ok {
			return "", fmt.Errorf("未知参数 %s：动作只接受 %s 三种写法之一（-h 查看完整用法）", quoteArg(arg), actionFlags())
		}
		gotFlags = append(gotFlags, arg)
		gotCmds = append(gotCmds, cmd)
	}

	// 若包含帮助且没有其他未知/冲突参数
	if hasHelp {
		if len(gotCmds) > 0 {
			return "", fmt.Errorf("帮助参数与具体操作动作互斥，只允许单独指定帮助或单独指定动作之一")
		}
		return migrateCmdHelp, nil
	}

	switch len(gotCmds) {
	case 0:
		return "", fmt.Errorf("必须指定且仅指定一个动作：%s", actionFlags())
	case 1:
		return gotCmds[0], nil
	default:
		return "", fmt.Errorf("动作互斥，只允许指定其中一个，实际收到 %d 个：%s", len(gotFlags), strings.Join(gotFlags, " "))
	}
}

// quoteArg 给参数加可读引号（空值也不产生裸引号歧义）。
func quoteArg(arg string) string {
	if arg == "" {
		return `""（空参数）`
	}
	return `"` + arg + `"`
}

// dispatchMigrate 在 args（os.Args[1:]）上执行 db-migrate 分派。
// handled=false 表示本次调用与 db-migrate 无关，调用方继续主服务启动流程；
// handled=true 表示命令已终结，调用方必须以返回的退出码结束进程。
//
// 契约：
//   - 仅当首个参数即为 "db-migrate" 时触发子命令分派；
//   - 禁止静默丢弃任何前置参数：任何以 flag 开头或夹杂未知参数（例如 --bogus db-migrate）
//     均不得进入子命令分支或静默忽略，必须交由标准主程序 flag.Parse 严格报错；
//   - 若首个参数为 db-migrate，后续所有参数严格完整校验，拒绝任何未知参数与冲突动作。
func dispatchMigrate(args []string) (code int, handled bool) {
	if len(args) == 0 || args[0] != migrateSubcommand {
		return 0, false
	}
	cmd, err := parseMigrateCommand(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] %v\n\n%s", migrateSubcommand, err, migrateUsage())
		return 1, true
	}
	return runMigrateCommand(cmd, os.Stdout), true
}

// runMigrateCommand 执行一个已解析的动作并返回进程退出码（0 成功，非 0 失败）。
// out 是输出汇：main 传 os.Stdout，测试传 buffer 以覆盖 help / fail-closed 分支。
func runMigrateCommand(cmd string, out io.Writer) int {
	switch cmd {
	case migrateCmdHelp:
		fmt.Fprint(out, migrateUsage())
		return 0
	case migrateCmdPrepare, migrateCmdVerify, migrateCmdStatus:
	default:
		fmt.Fprintf(out, "[%s] 未知命令 %q\n\n%s", migrateSubcommand, cmd, migrateUsage())
		return 1
	}

	// DSN 解析失败（无任何配置来源、损坏的配置文件、残留 postgres:// 等）一律
	// fail-closed：在建连之前停下，绝不静默连到猜测的地址。
	dsn, source, err := config.TiDBDSN()
	if err != nil {
		fmt.Fprintf(out, "[%s] 解析 TiDB 连接串失败：%v\n提示：请设置环境变量 ARTEX_TIDB_DSN，"+
			"或在 config.yaml 的 database 段提供 dsn / host / user / password / dbname\n", migrateSubcommand, err)
		return 1
	}
	fmt.Fprintf(out, "[%s] 连接来源：%s\n", migrateSubcommand, source)

	timeout := migrateReadTimeout
	if cmd == migrateCmdPrepare {
		timeout = migratePrepareTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// OpenTiDB 内部完成 DSN 规范化、32 连接池、PingContext 真实建连与只读能力探测；
	// 其错误消息里的连接串已经 tidb.RedactDSN 脱敏。
	db, caps, err := tidb.OpenTiDB(dsn)
	if err != nil {
		fmt.Fprintf(out, "[%s] 连接 TiDB 失败：%v\n", migrateSubcommand, err)
		return 1
	}
	defer closeDB(out, db)
	fmt.Fprintf(out, "[%s] 已连接 TiDB：%s（sql_mode=%s）\n", migrateSubcommand, caps.Version, caps.SQLMode)

	switch cmd {
	case migrateCmdPrepare:
		return migratePrepare(ctx, out, db)
	case migrateCmdVerify:
		return migrateVerify(ctx, out, db)
	default: // migrateCmdStatus —— 函数头已把 cmd 收敛到四个合法值
		return migrateStatus(ctx, out, db)
	}
}

// closeDB 关闭连接池；关闭失败只影响本次一次性进程，报告但不改写退出码。
func closeDB(out io.Writer, db *sql.DB) {
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		fmt.Fprintf(out, "[%s] 关闭 TiDB 连接池失败：%v\n", migrateSubcommand, err)
	}
}

// migratePrepare 按 Spec §3.1.2 第 3 点顺序执行：
// ① 65 表 DDL → ② 基础设施 seed → ③ 只读基线核验 → ④ 同一事务内的版本锁定 DML。
// 任一步失败立即返回非 0；版本锁定失败由 stampSchemaVersion 回滚。
func migratePrepare(ctx context.Context, out io.Writer, db *sql.DB) int {
	stmts, err := schema.GenerateStatements()
	if err != nil {
		return failMigrate(out, "编译基线 DDL 失败", err)
	}
	fmt.Fprintf(out, "[prepare] 1/4 应用 65 表基线 DDL（%d 条语句，幂等 CREATE TABLE IF NOT EXISTS）\n", len(stmts))
	if err := schema.ApplyDDL(ctx, db, stmts); err != nil {
		return failMigrate(out, "应用基线 DDL 失败", err)
	}

	fmt.Fprintln(out, "[prepare] 2/4 补齐基础设施 seed（schema_meta / locks / maintenance singleton）")
	if err := applyInfraSeeds(ctx, db); err != nil {
		return failMigrate(out, "补齐 schema_meta / locks seed 失败", err)
	}
	if err := tidb.EnsureLockInfrastructure(ctx, db); err != nil {
		return failMigrate(out, "初始化 maintenance singleton seed 失败", err)
	}

	fmt.Fprintln(out, "[prepare] 3/4 只读基线核验 schema.VerifyBaseline")
	if err := schema.VerifyBaseline(ctx, db); err != nil {
		// 核验错误自带逐条不匹配项，原样输出后给运维 --prepare 修复指引。
		fmt.Fprintf(out, "[prepare] FAIL 基线核验未通过：%v\n", err)
		return 1
	}

	fmt.Fprintln(out, "[prepare] 4/4 版本锁定（F10）：schema_version 0 → 1")
	if err := stampSchemaVersion(ctx, db); err != nil {
		return failMigrate(out, "版本锁定失败（事务已回滚，该库未完成基线绑定）", err)
	}

	fmt.Fprintf(out, "[prepare] OK 基线就绪：%d 表 / %d 列 / %d UNIQUE 索引，schema_version=1 已锁定\n",
		schema.BaselineTableCount, schema.BaselineColumnCount, schema.BaselineUniqueIndexCount)
	return 0
}

// applyInfraSeeds 执行 schema.PrepareInfraSQL() 的幂等 seed：
// locks 的 'business_relations' 命名行 + schema_meta 的 ('schema_version', 0)。
// 与 DDL 一样逐条 autocommit（TiDB DDL/DML 幂等，且必须早于版本锁定落库）。
func applyInfraSeeds(ctx context.Context, db *sql.DB) error {
	for _, stmt := range schema.PrepareInfraSQL() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%w（语句：%s）", err, stmt)
		}
	}
	return nil
}

// stampSchemaVersion 在单个事务里执行版本锁定 DML，并把"影响行数 ≠ 1"判为失败。
//
// 事务边界只覆盖这条条件 UPDATE：TiDB 的 DDL 自动提交，前面的建表无法回滚，
// 真正需要原子性的是"版本戳写入"本身（Spec §3.1.2：影响行数不为 1 必须立即报错回滚）。
// 事务由本函数显式 Commit；任何提前返回都由 defer 的 Rollback 兜底
// （已提交的事务 Rollback 返回 sql.ErrTxDone，忽略即可）。
func stampSchemaVersion(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启版本锁定事务失败：%w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, versionStampSQL)
	if err != nil {
		return fmt.Errorf("执行版本锁定 DML 失败：%w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("读取版本锁定影响行数失败：%w", err)
	}
	if err := checkVersionStampAffected(affected); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交版本锁定失败：%w", err)
	}
	return nil
}

// checkVersionStampAffected 是 F10 闭环的判定：影响行数必须恰好为 1。
// 0 行 = seed 缺失或版本已锁定（禁止重复绑定）；>1 行 = 目录被篡改。两种都拒绝。
func checkVersionStampAffected(affected int64) error {
	if affected == 1 {
		return nil
	}
	return fmt.Errorf(
		"%w：UPDATE 影响 %d 行，期望恰好 1 行（key='schema_version' 且 value=0 的行必须存在且仅一行）。"+
			"若 schema_meta seed 缺失请先执行 artex %s --prepare；若 schema_version 已为 1，说明该实例已完成基线绑定，禁止重复锁定",
		errVersionStamp, affected, migrateSubcommand)
}

// migrateVerify 只做一件事：把 schema.VerifyBaseline 的结论映射为退出码。
// 失败时 VerifyBaseline 的错误自带逐条不匹配清单，原样打印即为运维报告。
func migrateVerify(ctx context.Context, out io.Writer, db *sql.DB) int {
	if err := schema.VerifyBaseline(ctx, db); err != nil {
		fmt.Fprintf(out, "[verify] FAIL 全表结构核验未通过：%v\n", err)
		return 1
	}
	fmt.Fprintf(out, "[verify] OK 全表结构核验通过：%d 表 / %d 列 / %d UNIQUE 索引全部等于声明式基线"+
		"（已容忍 %d 处 DESC 索引在真实 TiDB 的 ASC 等价存储）\n",
		schema.BaselineTableCount, schema.BaselineColumnCount, schema.BaselineUniqueIndexCount, schema.BaselineDescIndexCount)
	return 0
}

// qStatusBaseTables 统计当前库的 BASE TABLE 数量（视图与 TiDB 内部表不计入）。
const qStatusBaseTables = `SELECT COUNT(*) FROM information_schema.tables
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`

// migrateStatus 是纯只读快照：版本、会话 sql_mode、BASE TABLE 数量、
// schema_version 与 maintenance 两行 singleton 的持有状态。
// 不执行任何写入、不改写任何锁（严禁在本命令里强制释放维护窗口）。
// 只要三条基础探测成功就返回 0；schema_meta / maintenance 缺失属于"可报告的状态"，
// 打印出来交给运维决定下一步（--prepare 或人工排查）。
func migrateStatus(ctx context.Context, out io.Writer, db *sql.DB) int {
	var (
		version    string
		sqlMode    string
		baseTables int
	)
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return failMigrate(out, "读取 SELECT VERSION() 失败", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT @@sql_mode").Scan(&sqlMode); err != nil {
		return failMigrate(out, "读取 @@sql_mode 失败", err)
	}
	if err := db.QueryRowContext(ctx, qStatusBaseTables).Scan(&baseTables); err != nil {
		return failMigrate(out, "统计 BASE TABLE 数量失败", err)
	}

	fmt.Fprintln(out, "[status] —— 只读快照 ——")
	fmt.Fprintf(out, "[status] TiDB 版本     : %s\n", version)
	fmt.Fprintf(out, "[status] 会话 sql_mode : %s\n", sqlMode)
	fmt.Fprintf(out, "[status] 当前库表数量  : %d 张 BASE TABLE（基线要求 %d）\n", baseTables, schema.BaselineTableCount)
	writeSchemaVersionStatus(ctx, out, db)
	writeMaintenanceStatus(ctx, out, db)
	fmt.Fprintln(out, "[status] 只读检查结束：未执行任何写入、锁释放或系统变量修改")
	return 0
}

// writeSchemaVersionStatus 读取 key='schema_version' 的当前值；
// 表/行缺失或读取失败都作为一种状态打印，不中断快照。
func writeSchemaVersionStatus(ctx context.Context, out io.Writer, db *sql.DB) {
	var value sql.NullInt64
	err := db.QueryRowContext(ctx, "SELECT `value` FROM `schema_meta` WHERE `key` = 'schema_version'").Scan(&value)
	switch {
	case err != nil && errors.Is(err, sql.ErrNoRows):
		fmt.Fprintf(out, "[status] schema_version  : 缺失 —— 请执行 artex %s --prepare\n", migrateSubcommand)
	case err != nil:
		fmt.Fprintf(out, "[status] schema_version  : 读取失败 —— %v\n", err)
	case !value.Valid:
		fmt.Fprintln(out, "[status] schema_version  : NULL（异常行，请人工排查 schema_meta）")
	case value.Int64 == 1:
		fmt.Fprintln(out, "[status] schema_version  : 1 —— 已锁定，允许应用启动")
	default:
		fmt.Fprintf(out, "[status] schema_version  : %d —— 未锁定，请执行 artex %s --prepare 完成版本绑定\n",
			value.Int64, migrateSubcommand)
	}
}

// writeMaintenanceStatus 输出 maintenance 两行 singleton 的持有状态。
// status 口径来自 op_id：NULL=空闲，非 NULL=维护窗口被该 op_id 持有（revision 是 fencing 代次）。
func writeMaintenanceStatus(ctx context.Context, out io.Writer, db *sql.DB) {
	rows, err := db.QueryContext(ctx, `
SELECT singleton_key, mode, revision, IFNULL(HEX(op_id), ''),
       LEFT(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'), 23)
FROM maintenance
ORDER BY singleton_key`)
	if err != nil {
		fmt.Fprintf(out, "[status] maintenance 锁  : 读取失败 —— %v（缺失请执行 artex %s --prepare）\n", err, migrateSubcommand)
		return
	}
	defer func() { _ = rows.Close() }()

	fmt.Fprintln(out, "[status] maintenance 锁  :")
	found := false
	for rows.Next() {
		found = true
		var singletonKey, ownerID, updatedAt string
		var mode int
		var revision uint64
		if err := rows.Scan(&singletonKey, &mode, &revision, &ownerID, &updatedAt); err != nil {
			fmt.Fprintf(out, "[status] maintenance 锁  : 扫描失败 —— %v\n", err)
			return
		}
		status := "空闲"
		owner := "<none>"
		if ownerID != "" {
			status = "维护窗口持有中"
			owner = ownerID
		}
		fmt.Fprintf(out, "[status]   %-12s: status=%s owner=%s mode=%d revision=%d updated_at=%s\n",
			singletonKey, status, owner, mode, revision, updatedAt)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(out, "[status] maintenance 锁  : 遍历失败 —— %v\n", err)
		return
	}
	if !found {
		fmt.Fprintf(out, "[status]   (无 singleton 行) —— 请执行 artex %s --prepare\n", migrateSubcommand)
	}
}

// failMigrate 统一失败出口：打印原因并返回退出码 1。
func failMigrate(out io.Writer, action string, err error) int {
	fmt.Fprintf(out, "[%s] FAIL %s：%v\n", migrateSubcommand, action, err)
	return 1
}
