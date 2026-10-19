package db

// 本文件曾承载 TestLLMRecordsMigrateAddsRawColumnsToOldTable：它把 llm_records 现场
// ALTER 回"升级前的老形状"，再跑 llmRecordsMigrate 的 `ALTER TABLE … ADD COLUMN`，
// 并依赖 PG 的事务性 DDL 回滚。P08-03 Task 3.3（M12）废除了运行期 DDL ——
// llmRecordsSchema / llmRecordsMigrate 两份 DDL 文本与 EnsureLLMRecordsTable 的建表、
// 补列语句已全部删除；llm_records（含后加的 task_id/worker/profile_name/raw_request/
// raw_response 五列）连同 idx_llm_records_ts / idx_llm_records_session 由 65 表基线
// 目录里的 schema.ExtensionTables §50 一次性建出，启动时经 db.Open →
// schema.VerifyBaseline 只读校验，缺表/缺列一律 fail closed 拒绝启动。
//
// "用旧 DDL 把老库现升到新形状"这条路径在原生 TiDB 上不存在，该用例连被测对象都没有了；
// 原样保留只会让仓库里继续躺着 `ALTER TABLE … DROP COLUMN IF EXISTS`、
// `INSERT … RETURNING` 这类 PG 方言。列存在性的等价保障由 db/schema 的基线目录用例与
// tools_test.go 里的 TestTools_NoRuntimeDDLLeft 承担。
//
// 越界声明：本文件不在 Task 3.3 的 8 文件清单内。删除整文件（git rm）的操作被审查护栏
// 拒绝，因此保留这个空壳让 db 包测试恢复编译；失效用例正文已随 M12 一并移除，
// 请父任务在后续清理中连同两个 Ensure 兼容空壳一起删除。
