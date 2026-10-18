# P08 与原生 TiDB 8.5 全量方言迁移总览索引

> **计划拆分说明**：依据实施计划模块化与独立编号要求，原综合实施计划已全面拆分为 5 个自包含、具备独立编号的阶段性子计划文件。各阶段包含完整的 TDD 红绿循环、明确的输入输出契约及质量门禁：

1. **[P08-01: 核心骨架、Viper 配置与运维 CLI](<docs/superpowers/plans/2026-10-18-P08-01-skeleton-config-cli.md>)**
   - 覆盖阶段 1：`config/config.go`, `cmd/artex/main.go`, `cmd/artex/migrate.go`, `db/db.go`, `db/settings.go`, `db/config.go`, `db/asset_intercept.go`, `db/intercept.go`。
2. **[P08-02: 核心业务主干（资产、任务与级联闭包删除）](<docs/superpowers/plans/2026-10-18-P08-02-core-assets-tasks-cascade.md>)**
   - 覆盖阶段 2：`db/assets.go`, `db/asset_dsl.go`, `db/companies.go`, `db/tasks.go`, `db/task_assets.go`, `db/task_assets_context.go`, `db/task_scope.go`, `db/task_context.go`, `db/task_categories.go`, `db/task_templates.go`, `db/task_intercept.go`。
3. **[P08-03: 探索图谱、意图推进与漏洞管理](<docs/superpowers/plans/2026-10-18-P08-03-exploration-findings.md>)**
   - 覆盖阶段 3：`db/exploration.go`, `db/exploration_sources.go`, `db/findings.go`, `db/finding_assets.go`, `db/finding_retests.go`, `db/finding_traffic.go`, `db/finding_traffic_archive.go`, `db/commands.go`, `db/tools.go`, `db/llm_usage.go`, `db/llmhealth.go`, `db/logs.go`, `db/digest.go`, `db/intent_admission.go`, `db/tool_usage.go`。
4. **[P08-04: 辅助模块、通知租赁、归档白名单与依赖切除](<docs/superpowers/plans/2026-10-18-P08-04-aux-notifications-archives-cleanup.md>)**
   - 覆盖阶段 4：`db/conversation.go`, `db/triggers.go`, `db/chat_mentions.go`, `db/notification.go`, `db/notification_delivery.go`, `db/task_archives.go`, `db/task_archives_restore.go`, `db/skill_usage.go`, `db/side_questions.go`, `db/constraints.go`, `db/intercept_detail.go`, `db/intercept_execution.go`。
5. **[P08-05: W5 全量集成回归验证与质量门禁](<docs/superpowers/plans/2026-10-18-P08-05-w5-final-regression.md>)**
   - 覆盖终态端到端四重门禁（离线纯净化、前端对齐、空库 Schema 冒烟、真实 TiDB 业务库回归）。
