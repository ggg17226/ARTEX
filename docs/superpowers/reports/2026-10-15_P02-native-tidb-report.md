# ARTEX P02 原生 TiDB schema 完成报告

> **P02 代码、离线验收与独立审查已完成。真实 TiDB 8.5 集群未验收。**
> 按用户最后指令“无法完成的测试直接忽略”，缺外部环境的测试不再阻断本次 P02 完成；原始 Skip/失败结果全部保留，没有改写成 PASS，也没有修改旧测试掩盖结果。

## 1. 交付范围与版本

|项目|结果|
|---|---|
|工作分支|`dev`，保留工作区，不合并、不推送|
|P02 完整审查起点|`be5e99fcb225e46d2b88668814c0775bacaf0d49`（P01 末提交）|
|最终统一语义修复|`5fff21029394f0699a998f9cfb27a323c16c287f`|
|最终已验证源码|`0ad7256397c53febc1d23e405b2b89da41c31ffe`（随后仅文档归档提交）|
|最终整体审查|GLM high：Spec Ready / Quality Ready，Critical 0 / Important 0|
|统一修复及限定复查|17 项处理、3 项附理由保留、1 项已有覆盖关闭；无未处理 Critical/Important|
|最终格式收口|只运行 gofmt，逐行验证恰三行 doc comment，无运行语义变化|

### Task 1–7

|任务|成果|验收边界|
|---|---|---|
|1|声明式类型/表/索引元模型、Validate、索引字节预算|单元测试及独立审查|
|2|49 张业务基线表|目录合同及限定复查|
|3|16 张扩展/流量/基础设施表，AllTables 65 表，13 项 hash 映射|目录合同、typed charset 与三轮限定复查|
|4|canonical identity 编码、SHA-256、22 builders、26 四元组白名单、同事务完整 payload 碰撞仲裁|离线算法/driver 测试，非真实事务锁实测|
|5|原生 DDL 编译、逐条执行、infra seed、幂等脚本、安全测试库 harness|CACHE 落位与错误变异证明；真实 smoke 未执行|
|6|有序65表/PK/隐藏row_id/预算/字符集/生成列/hash/infra/traffic全矩阵|独立字面期望、隔离变异拒绝、补充DDL整行断言|
|7|Rows Err/Close、元数据检查、自增和ASCII探测、真实Skip证明与安全集成|fake经标准库database/sql驱动生产代码；真实集群未验证|

## 2. 最终验证（fresh，基于上述源码提交）

|验证|实际结果|
|---|---|
|`go test ./db/schema/... -count=1`|**99 PASS / 2 SKIP / 0 FAIL**，共101个顶层测试|
|两项真实 SKIP|`TestSmokeRealTiDBDDL`、`TestVerify_RealTiDB_Structure`，明确未验证|
|`go test ./... -count=1`|**exit 1**：16包PASS、2包SKIP、1包FAIL；server中22项旧PG集成测试缺配置，按用户指令忽略|
|server改动归因|`git diff be5e99f -- server` 为空，P02未修改这些测试或实现；不冒称全仓测试全绿|
|`go vet ./db/schema/...`|exit 0|
|`gofmt -l db/schema/`|无输出|
|`git diff --check`|通过|
|`go build ./...`|exit 0；最终构建日志无stat-cache告警，前期只读缓存告警仍保留在证据中|
|权威规格|1068行 / 258228字节 / SHA-256 `209f841f3858cd8c45e903923762b2a49b81f400da0e4fa6c6a176d9b02a8127`，未更改|

### 复现离线验证

```bash
env -u ARTEX_TEST_TIDB_DSN -u ARTEX_TIDB_DSN -u ARTEX_PG_DSN \
  -u ARTEX_REVIEW_LIVE_CONFIG GOCACHE=/tmp/gocache-artex \
  go test ./db/schema/... -count=1 -v
env -u ARTEX_TEST_TIDB_DSN -u ARTEX_TIDB_DSN -u ARTEX_PG_DSN \
  -u ARTEX_REVIEW_LIVE_CONFIG GOCACHE=/tmp/gocache-artex \
  go build ./...
```

不要把 `go test ./...` 的外部环境失败改成 PASS。本次未读取 secret 或任何原DSN值，未连真实数据库、未建库、未调参、未依赖 SHOW CONFIG。

## 3. 关键规格落地

- 65表物理单列unsigned BIGINT PK：37 `id` / 1 `ordinal` / 1 `profile_id` / 26隐藏 `row_id`；64自增，只有 `llm_profile_health` 非自增。
- `AUTO_INCREMENT` 只在PK列；`AUTO_ID_CACHE=1` 只在最终闭括号后的表选项。无FK/CHECK/AUTO_RANDOM，无CREATE DATABASE。
- 隐藏row_id不进入逻辑UNIQUE、API/DTO/cursor/manifest；复合普通索引可保留显式PK尾缀，不再硬编码表名豁免。
- 用户确认的33列typed字符集：32个Unicode身份TEXT用utf8mb4_bin，archive SHA256无界TEXT用ascii_bin；另有规格批准的chunks.text，故全目录显式Unicode TText为33而不是32。有界ASCII列与BINARY/BLOB不混计。
- 保留无界合法自然键，不用prefix UNIQUE/任意VARCHAR上限。LLM profile name保留原文，完整 `name_identity` 与 `CanonicalHash(payload)`，并非raw SHA256(name)。
- canonical编码含版本/branch/type tag/NULL marker/BE长度/原字节；uint64载荷8字节BE，NULL长度0且完全忽略占位Value。只有原SQL显式COALESCE字段由调用方预处理。
- 仲裁必须在调用方已按物理PK全序取得实体锁后，同一事务FOR UPDATE读完整identity精确比较；不是UQ/hash预定位器，不内部commit/rollback/replan。
- 两处VARBINARY去掉无效X'00'默认，漏写在strict SQL mode显式失败；只删除一个与逻辑UQ完全同parts的冗余普通KEY；DATETIME(3)预算为7字节。

## 4. 保留项、忽略项与下游门禁

1. **真实TiDB未验证**：真实65条DDL、information_schema口径、SHOW NEXT_ROW_ID列型、AUTO_ID_CACHE步长、ASCII大小写隔离、真实驱动高位/byte返回均未实测。P03接库前须在授权、管理员预建、独占空schema执行并留档；当前“忽略”不等于将来可声称实测通过。
2. **22个旧server PG测试**：缺ARTEX_PG_DSN或database配置，按用户指令忽略；原exit1与逐项错误保存在原始JSONL，不修复范围外测试、不补造凭据。
3. **F18版本token语义**：私有8.5守卫保留发行banner内token识别，不是引擎认证或完整banner证明；合法后缀兼容优先，未知无token失败。真实banner充分性待实测。
4. **F19历史regex报告偏差**：保留原报告，按实际源码与最终复查解释；不改写历史输出来制造一致。
5. **F21 reset覆盖**：guard入口无条件清旧许可，已有success→failure覆盖；cancel属于失败原因之一，未为此新增框架。
6. **最终小型观察O1–O3**：集成测试 `_ = fc` 与“未知SQL验证”注释、拒绝用例旧doc措辞、DDL取消ctx仅非nil直断言仍有卫生改进空间；行为由严格handler与组合断言覆盖，不影响当前离线验收，不开启第二语义修复波。
7. **F11重裁决**：Go≥1.19的gofmt自动把doc comment内连续ASCII单引号规范为排版引号。原ASCII替换建议导致格式失败，已撤回该文字偏好，按formatter规范收口。首次失败与报告“格式空输出”不可信声明均保留，不隐瞒；最终fresh格式关卡通过。
8. P01 `db/tidb` 与P02 infra seed仍有同构双份，P08应收敛单一来源；P02不提前改P01或接入业务CRUD。

## 5. 证据 → 结论 → 后续路径

|证据|本次结论|后续路径|
|---|---|---|
|最终schema JSONL：99/2/0|代码与离线矩阵通过，不证明真实集群|P03授权真实验收|
|全仓JSONL：server22项缺PG配置|非P02代码回归，按用户授权忽略；仍非全绿|后续具备PG环境再单独执行|
|最终整体审查+唯一限定复查|跨任务合同成立，无Critical/Important|保留当前dev，不自动合并或推送|
|原始Ruling/RED-GREEN/变异日志|裁决与证据边界可追溯，历史错误未擦除|归档下载后可独立复核|

### 完整证据包

- 本机归档：[完整原始证据包](../../../.superpowers/evidence/2026-10-15-P02-native-tidb-evidence.tar.gz)（被Git忽略，不属于源码提交；可作为独立交付下载）。
- SHA-256：`db47eb9b59a4471bda814de2b5eca5780fc4418e4787a5ccb04bee97e23ebe4f`。
- 归档原始文件数：**117**；归档内每个文件均与原SDD文件逐字节SHA-256比对成功。
- 关键条目：`final-review.md`、`final-rereview.md`、`final-fix-report.md`、`progress.md`、`final-verification-summary.json`、`final-tests-schema-0ad7256.jsonl`、`final-tests-all-0ad7256.jsonl`、`final-build-0ad7256.log`、所有task报告与审查包。
- 仅在归档逐文件验证后清理本计划SDD目录；不删除兄弟计划、工作树或证据包。

## 6. 我作出的裁决（现行摘要）

|决定|原因|判断错误的代价/约束|
|---|---|---|
|规格优先于错误计划样例|原计划存在列内CACHE、nullable/unsigned/列名/测试stub缺陷|错误DDL或CRUD不匹配；逐条独立回归验证|
|通用禁止单列PK重复索引，不硬编码表名|保留正确复合索引含PK尾缀|索引布局/预算偏差；65表矩阵锁定|
|撤回早期自然TEXT可隐式继承裁决，采用用户typed授权|自由Unicode与binary不能被强制ASCII|比较语义改变；显式collation、全payloadhash与exact比较|
|LLM必须完整canonical identity+hash三元组|rawnamehash不足以做碰撞精确裁决|后续writer漏写必填列失败；P03统一builder|
|修正U64向量和NULL长度0|原样例漏7字节/NULL占位污染payload|协议不兼容；精确向量与NULL不变性测试|
|CACHE与测试同步改表级|只改实现会被旧错误测试拒绝|无效DDL/假绿；每表位置+变异拒绝|
|严格空schema+owned-only清理|IFNOTEXISTS再全量DROP可能误删既有表|数据损失；严格CREATE成功归属、失败只清own|
|真实t.Run Skip、Rows.Err和Close|原stub编译缺陷/迭代错误吞没|误验收或连接泄漏；错误链与关闭断言|
|不新增未定义CountUniqueIdentityColumns/重复预算API|无消费者且计划签名/实现不自洽|少一计划列表API；静态13map/17表矩阵覆盖|
|SHOW按列名/精确row_id与byte归一|最后列假设、fmt.Sprint(byte)失配|真实驱动假失败；两阶段错误注入与高位测试|
|F4使用manifest32+chunks1精确集合|审查提出global32忽略合法列|错误测试逼改正确目录；实际UnicodeTText33|
|去两默认/一个冗余索引/预算7字节|failclosed、减少写放大、预算精度|未来writer须显式提供bytes；不改变逻辑identity|
|一次统一修复+一次限定复查|避免无边界循环，全部Minor显式分诊|剩余仅非阻断保留项；不静默丢弃|
|F11按gofmt重新裁决|文字偏好不能与工具规范冲突|排版引号保留但零运行影响；纯3注释diff证明|
|模型与执行规则按最新用户覆盖|Gemini high实现、GLM high审查；Qwen恢复1后最终改0|不重复派发/不轮询；目标功能已停用|
|不可执行外部测试按用户授权忽略|没有授权数据库环境|未实测风险仍需披露；不得宣称这些测试PASS|

## 附录：全部原始裁决源记录（按账本原始顺序）

以下含已撤销/被后续授权替代的历史判断。现行合同以正文与较新用户授权为准；保留原文用于追踪，不表示重新认可错误旧裁决。

### R1（归档源行 26）

Task6 reviewreturned6ff1998a, fullreportread: SpecPASSwithreservations/QualityPASS; unusualC14/I3/M0means14compliancechecks,3Minor/0Major—not14Critical. NormalizeC0/Important0/Minor3. Ruling: sinceexecutionbriefexplicitlyrequiresactualDDLbranch/infraidentityrendering, address3smallcoveragegapsnowasonefilebatch ratherthancarryfinalwave — currentcodecorrectbutmatrixoracleincomplete — cost: oneextraQwenimplementation+GLMlimitedreview, no productionchange.

### R2（归档源行 35）

Task7C Ruling: test-onlystrict8.5lineguardbeforeanyDDL, retainexistingIsRealTiDBminimalenginepredicate. Reason: TiDBbanneralone(e.g6.5)mustnotcountas8.5LTSstructureacceptance — cost: unknownbannerformatsfailcloseduntilreview ratherthanfalselyqualify. Oneadditionalprivatehelper/negativefixturewithinnewintegrationtestonly, publicAPIunchanged; task-7c-brief.md updated.

### R3（归档源行 40）

Archive Ruling: permanentcompletionreportindocs/superpowers/reports plusignoredlocaltar.gzunder.superpowers/evidence; exportallrawRulingrecordswithsupersededjudgmentsclearlylabelled. Deleteonlythisplan-specificSDDdirectoryaftertarentries逐文件SHA256matchverification, never siblings/secret/worktree — reasonSDDskillcleanupmustnotloseaudittrail — wrongcostevidenceloss, mitigatedbyregular-filehashcomparison+archiveSHA256+standalonereport. Noagent/jobsactivebeforecleanup; no furthercodechanges/push/merge.

### R4（归档源行 42）

Latestuserfinalacceptanceinstruction:“无法完成的测试直接忽略”。Ruling: unavailableexternalTiDBintegrationand22oldserverPGtestsareexcludedasP02completionblockersbyexplicituserauthorization; keep2actualSkipand22actualfailurelogsandlabelignored/notverified, neverclaimthemPASS/forgeDBevidence/editteststoSkip. Completedsource/99offlinePASS/reviews/vet/format/build/specgateunchanged. Userauthorizationdoesnotpermitreadingsecret/connectingnewDB/pushmerge.

### R5（归档源行 56）

Final-limitedreviewd98a9de7 complete,fullfinal-rereview.md read:17ADDRESSED/3RETAINED/1CLOSEDbCoverage,NOT0,C0/I0/M2(sameformatterroot). Ruling: F11ASCIIquoteinstructionwithdrawn—canonicalGo docgofmtoutputwins; U201Dtypographicemptyquoteisformatterbehaviornotruntimebug. Appliedonlygofmt-w hash_ddl.go,programmaticdiffguardconfirmedexact3commentlines/nothingsemantic,gofmt-lstable; committed0ad7256. Thisisfinishingformatterstep,nosecondsemanticfixwave. Cost: oldF11textpreferenceisreversed, recordskeptandfinalreportsaysso. Author“formatclean”at5fff210nottrusted; M2explicitlycorrectedbythisfreshgatefailureandnormalization, historicaloutputnotrewritten.

### R6（归档源行 57）

Finalremainingnonblockingrulings:F18token-bannerguardretainednotengineauthentication; F19historicalregexreportdeviationdisclosed; F21reset-at-startcoverageaccepted; O1/O2smallmisleadingcommentresiduals+O3ctxweakdirectassertionretainedasminor,noCritical/Important. Nooutstandingagents(allcallbacksreceived), Qwen0/goalpaused.

### R7（归档源行 64）

Ruling: adoptF1TDate3keywidth7withregression, F2removeonlysearch_bloom/checkpointX00defaultsconfirmedagainstspec496/706 (noarbitrarydefault,failclosed), F3removeonlyexactduplicatetrafficbodies(namespace,exchange_id)KEYwhilekeepingUQ+req/respKEYs — planexamplesaddedinvaliddefaults/redundantindex, specandactualidentitycostruleoutrankplan — cost: futurewritersmustsupplyrequiredbytes; noactualwriterexistsP02, lowerwriteamplification/noidentitychange. Do not mutateimmutableplan/spec; amendments+finalarchivecapturedecision.

### R8（归档源行 65）

Ruling: correctF4proposedglobalcharsetcounts: target33manifest32Unicode+1ASCII plusexistingapprovedchunks.text meansactualexplicitTTextUnicode33/ASCII1; boundedASCII27excluded. Implementexactallowset(manifestUnicode32+chunks)notbad32globalcount, no correctcolumnchanges — reasonreadactualcatalogguardsuserauthorizationclosure — costfutureapprovedexplicitTTextadditionsmustupdatetestallowset, notsilentlybroaden.

### R9（归档源行 83）

Task7B Ruling: COLUMN_NAMEwhenpresentmustexactrow_idforlocks(autoPK), notmerelyexclude_tidb_rowid; parserColumns/ambiguity/missingvalueearlyreturns mustjoinCloseerrorinsteadofdiscard. Reason: originalbriefrequiresuniquetargetcounter+allresourceerrorspreserved; sameparserlocalfixnoAPIdrift — cost: malformed/unknownSHOWshapesfailcloseduntilreal8.5verification, notsilentlychoosewrongcounter.

### R10（归档源行 110）

Task7 sizing/ruling: split7A metadataRows/PK/FK-check(verify.go/test)→7B allocator/ascii probes+VerifyStructure(separateprobe.go/test)→7Ctest-onlytrueStructureintegrationusingexistingguardedharness, sequentialreview. Reason: read-row lifecycle, statefulprobe cleanup, realtestenvironment safety are distinct units; costfilelayoutchangewithoriginalpubliccheckerinterfacesretained. task-7a-brief.md preparedonly, waitTask6review.

### R11（归档源行 127）

Task5 sizing/ruling: split5A pureofflinecompiler(generator.go/test) →5B ApplyDDL/PrepareInfraSQL/IsRealTiDB withoffline driver tests →5C safe dedicated-schema smoke harness test-only; sequentialimplementers+independentreview percohesive step, originalexportedAPIs retained. Reason: SQLrendering, DBexecution, testobjectownership have distinctvalidation/safety domains; avoid oneagentcrossinglargecoupledscope. Cost: filelayout/morecheckpoints, no APIbreak. task-5a-brief.md preparedonly; waitTask4closed, no5Aimplementationdispatched.

### R12（归档源行 141）

Task7 preflight fully read (13reported items, static only). Ruling: implementer must correct actual root causes, not blindlycopy proposed patches: single-column2rowINSERT is VALUES (?), (?) with2args (reportB2 suggested4placeholders still wrong); use realt.Run pluschildSkipped/cleanup or subprocess skip evidence instead of homemade testing.TB (private method prevents external implementation), eliminating unnecessaryhelperTB migration. Rows.Err/Close error handling andNEXT-row column-name parsing required; baseline+2 probe under exclusive test-schema assumptions with propercleanup, not hardcoded3. PartialDDLcleanup remains ownership-tracked only for successfully createdobjects, not universalDROP nor blanketignorefailure — safe-precreated-empty-schema guard outranks convenience. TrueTiDB8.5 SHOW/allocator behavior remainsunverified.

### R13（归档源行 142）

Task7: Ruling: CountUniqueIdentityColumns appears onlyin2planinterface lines, noimplementation/specacceptance/consumer; avoid inventingunusedexportedAPI withundefinedsemantics/noctx; identity layout count coveredbyTask6exact13map/17hash-table matrix. Retainrealstructurechecker APIs. Cost: planProduces list revisedbut noexternalconsumerbreak; documentandreview explicitly.

### R14（归档源行 151）

Task 4: Ruling: 拆为4A规范编码+22个builders（hash_ddl.go/test）与4B碰撞仲裁入口+白名单（hash_arbitration.go/test），串行实现/逐步独立审查，维持原导出接口 — 两文件纯字节协议与SQL事务/驱动测试是独立内聚逻辑，避免一次跨协议和DB错误路径大包揽 — 代价：源码文件布局偏离原简报但无调用接口变化；两步完成并审查前Task4不标complete。

### R15（归档源行 154）

Final acceptance preparation: final-acceptance-checklist.md saved (not a pass declaration), covers Task1–7 evidence gates, typed charset policy, table-optionCACHE/per-table proof, safeofflineDSN-unset regression, trueTiDBSkip boundary, allknownMinor/rulings and final wholeP02review. Noimplementation/acceptance claims added; pending4Areview/Task7preflight callbacks unchanged.

### R16（归档源行 157）

Task 6: Ruling: reuseexistingIndexBytes/KeyBytes inoffline matrix; anymapadapterprivate_test.go only, not newexportedIndexKeyBytes/types.go duplicatealgorithm solely forone test — currentproductionAPI already meetsbudget need, Filesheader vsStep3 self-conflict — cost: testhelper layout deviatesbut zero productionAPIchange; referenceTask6amendments. Actualproductiondefects exposedbytests must be separately reported/fixed, not hiddenbyexpected changes.

### R17（归档源行 167）

Scheduling: Ruling: 用户指令禁用 local/qwen 子代理调度 — 全部编写类任务统一改用 codex-proxy/gemini-3.8-flash-high (high)，独立审查维持 codex-proxy-compat/gpt-6.1-sol（大 diff 用 high，限定复查可用 low/medium），最终整分支审查用 gpt-6.1-sol high — 生效范围：P02 及后续所有任务派发

### R18（归档源行 169）

Scheduling: Ruling: 用户最新指令“审查模型改为glm-coding/glm-5.3-flash high”已核对路由支持high；自此所有新派发的任务审查、限定复查与最终整体审查统一采用 provider=glm-coding, model=glm-5.3-flash, reasoning_effort=high，替代此前gpt-6.1-sol审查调度。编写类任务维持codex-proxy/gemini-3.8-flash-high high；local/qwen持续禁用。已有完成报告保留原审查者归属，不追溯改写。

### R19（归档源行 184）

Task 2: Ruling: 直接以 codex-proxy/gemini-3.8-flash-high (high) 承接编写 — 简报 1199 行（Step 3 内嵌 49 表完整定义约 970 行），产出千行级 Go 文件，属授权的"编写长文/溢出"场景，且前任实现者已因模型异常截断一次，不再以 local/qwen medium 试错 — 判断错误的代价：high 档成本略高，可接受

### R20（归档源行 185）

Task 2: Ruling: 方案 A→B 改裁 — 63068f5d 落地的方案 A（删 15 处 PK 尾缀 + types.go 硬编码表名/索引名特例）废弃，改采方案 B：types.go 校验收窄为仅禁止单列纯 PK 索引（len(ix.Parts)==1 && Parts[0].Column==PKColumn），目录恢复简报原文（复合索引允许含 PK 列）— 理由：非唯一二级索引显式含 PK 列时 TiDB 物理不再重复附加 handle（零冗余），无硬编码特例且规则通用，Task 3 扩展表（规格 §3.3 traffic 排序索引含 id 尾缀）同样受益，49 表定义忠实简报；与前任实现者 0662f433 独立建议收敛 — 判断错误的代价：DDL 文本显式含 PK 尾缀略冗长（物理等价），无功能损失

### R21（归档源行 186）

Task 2: Ruling: 审查发现"自然文本键未显式 ascii_bin"（companies.nkey / settings.key / agents.key 的 TEXT 列）不成立 — 硬约束第 7 条与规格行 102 的约束对象是"字符串身份列及其 UNIQUE 索引"（键载体列）；规格行 84 明文区分"文本/大载荷 TEXT/MEDIUMTEXT；短键显式 VARCHAR"，规格行 279 明文规定自由长 key 唯一性走 BINARY(32) hash 且 identity 不索引 — 三列均 TEXT 存储列不索引、UNIQUE 在 *_hash BINARY(32) 上，UTF8MB4 继承无 collation 比较语义，不构成规格 ❌ — 判断错误的代价：若规格真实意图是 TEXT 键列也要 ascii，届时属规格自身矛盾（行 84/279 与行 102 冲突），以更具体的行 279 方案为准

### R22（归档源行 189）

Task 2: Ruling: 审查发现 llm_profiles.name 简报缺陷成立（Important）— 简报 uq_llm_profiles_name = UNIQUE(PP("name",191)) 是 prefix UNIQUE，违反规格 §3.3 行 295"不可用 prefix 唯一拒绝两个共享前缀的合法值"，且弱化原 db/schema.sql:364 `name TEXT NOT NULL UNIQUE` 的全长唯一语义 — 修复裁决：照 mcp_servers.name_hash 模式（规格行 279"…及 scope 复合键等"涵盖），name 保持 TEXT 不索引，新增 name_hash BINARY(32) NOT NULL（应用层 SHA-256 全长 byte 级唯一，忠实原语义；有界化方案被规格行 296"不得新增任意 VARCHAR 截断"排除），索引改为 IX("uq_llm_profiles_name_hash", true, P("name_hash")) — 由实现者修复轮执行，限定复查验证 — 判断错误的代价：name_hash 需 P03+ 应用层正确计算，若遗漏写入路径将失唯一保护，届时以集成测试兜底

### R23（归档源行 194）

- Task 3: review complete (replacement agent 38f2fd0f-7d30-4178-aa50-0ca882c708e9; fixed 119fa70..8866fad; task-3-review.md; Spec Needs revision / Quality Needs revision; Critical 0 / Important 5 / Minor 1). I2 nullable blob hashes, I3 unsigned size_bytes, I4 chunks utf8mb4_bin, I5 legacy cache column names verified; I1 user hard constraint versus specification binary/Unicode storage remains OPEN pending explicit user clarification, not waived by historic Task2 ruling. No Task3 complete/acceptance claim.

### R24（归档源行 202）

- Ruling: Task5 implementation and tests must moveCACHE toGenerateDDL suffix after `\n) ENGINE=...`, keep onlyAUTO_INCREMENT onPK, generate64auto table options and none forllm_profile_health; replace wrong combined-substring tests with per-table location assertions (zeroCACHE before final table suffix, one/zero accordingAutoIncrement) plus independent aggregate64 counts — userhard constraint outranks plan — wrong-ruling cost: invalidDDL/falsegreen; enforce offline mutation proof and laterTask7 truecluster/Skip honesty. Exact supplement savedtask-5-amendments.md; authoritative design fingerprint untouched.

### R25（归档源行 209）

- Ruling: I1 next metadata repair uses33 explicit text-column changes: inventory27 + llm_profiles.name + traffic_evidence_snapshots.source_traffic_id + llm_records.task_id + llm_usage.task_id + intercept_pending.task_id + task_archives.sha256. First32 use utf8mb4/utf8mb4_bin; last SHA256 textual digest uses ascii/ascii_bin. PreserveTText, unbounded width, NotNull/default and indexes — user-authorized typed charset policy; original text/source identifiers have no proven universal ASCII guards; source provenance Normalize only utf8Clean (finding_traffic.go:120-127), task strings participate exact SQL comparison (commands.go:244-246, llm_usage.go:85-93, intercept.go:299-301) — judgment cost: downstream comparisons must use binary semantics, later runtime tests remain necessary; no numeric/width restriction introduced.

### R26（归档源行 213）

- Task 3: Ruling: I2–I5 are plan/brief defects verified against specification/source; nullable req_blob/resp_blob stay CHAR(64) ascii_bin with no default, size_bytes becomes unsigned NOT NULL (retain default 0), chunks.text explicitly utf8mb4_bin, only llm_records/llm_usage restore cache_read/cache_write names — specification and existing consumer contract outrank verbatim brief — cost of wrong ruling: later schema/CRUD mismatch, prevented by independent contract assertions and limited re-review.

### R27（归档源行 221）

- Ruling: C1 修订 TestCanonicalIdentityU64 期望向量，数值7必须使用完整8字节BE（00 00 00 00 00 00 00 07），不改变正确的FieldU64编码迎合缺7字节的错误测试 — 简报自身固定8字节契约与规格类型化编码一致 — 判断错误的代价：错误改实现会使后续持久化hash格式不兼容；必须保留精确向量回归。

### R28（归档源行 222）

- Ruling: C2 CanonicalIdentity 对 IsNull=true 的字段统一输出原tag + NULL marker + uint64 BE长度0，不输出payload；完全忽略Value的内容和长度。非NULL空串仍通过不同marker与NULL区分；仅显式COALESCE字段由调用方先处理 — 满足简报Value忽略承诺与规格§3.2 NULL区别，不让占位值污染identity — 判断错误的代价：编码规则改变会影响持久化hash；当前Task4尚未实现，需在首次发布前固定精确NULL向量和占位值不变性测试。

### R29（归档源行 223）

- Ruling: C3 撤销Task2关于llm_profiles仅加name_hash、不需要name_identity即足够的推论；按规格§3.2:247-250/270-271采用完整持久化canonical协议：name保留原始文本；新增name_identity MEDIUMBLOB NOT NULL；新增LLMProfileNameIdentity(name)以固定branch llm_profile_name和FieldBytes(name,false)构造payload；name_hash=CanonicalHash(payload)，不是raw SHA256(name)。Task4白名单增加(llm_profiles,id,name_identity,name_hash)，同事务FOR UPDATE读取持久化payload与expected精确比对；Task3 HashIdentityColumns及后续矩阵纳入该配对。P03/迁移/恢复必须共用同一builder且写入name、identity、hash一致三元组；当前不得声称应用层已验证 — 修补此前跨任务裁决遗漏并复用现有完整identity接口，不引入例外比较器 — 判断错误的代价：新增必填列需所有后续写入路径同步，遗漏将失败；保持TDD和下游核对，不将hash唯一误当exact identity。
