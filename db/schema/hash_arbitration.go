package schema

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrIdentityHashCollision 是 hash 命中但完整 identity 不字节相等时的显式错误（spec §3.2）。
var ErrIdentityHashCollision = errors.New("identity hash collision")

// hashArbTuple 定义仲裁白名单四元组结构（表名、主键列、完整 identity 列、hash 列）。
type hashArbTuple struct {
	Table    string
	PKCol    string
	IdentCol string
	HashCol  string
}

// rawHashArbTuples 是仲裁四元组的完整 26 项白名单（14 项普通/复合 + assets 8 项分支 + company_scope 3 项分支 + llm_profiles 1 项 = 26）。
var rawHashArbTuples = []hashArbTuple{
	// 1-3. companies, task_categories, task_templates (nkey)
	{Table: "companies", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
	{Table: "task_categories", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
	{Table: "task_templates", PKCol: "id", IdentCol: "nkey_identity", HashCol: "nkey_hash"},
	// 4. agents (agent_key)
	{Table: "agents", PKCol: "id", IdentCol: "key_identity", HashCol: "key_hash"},
	// 5. mcp_servers (mcp_server_name)
	{Table: "mcp_servers", PKCol: "id", IdentCol: "name_identity", HashCol: "name_hash"},
	// 6-8. settings, tools, scheduler_state (hidden PK row_id)
	{Table: "settings", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
	{Table: "tools", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
	{Table: "scheduler_state", PKCol: "row_id", IdentCol: "key_identity", HashCol: "key_hash"},
	// 9-10. agent_prompt_vars, mcp_tools_cache (id)
	{Table: "agent_prompt_vars", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
	{Table: "mcp_tools_cache", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
	// 11-12. agent_visibility, agent_skill_visibility (hidden PK row_id)
	{Table: "agent_visibility", PKCol: "row_id", IdentCol: "identity", HashCol: "identity_hash"},
	{Table: "agent_skill_visibility", PKCol: "row_id", IdentCol: "identity", HashCol: "identity_hash"},
	// 13. task_scope (id)
	{Table: "task_scope", PKCol: "id", IdentCol: "identity", HashCol: "identity_hash"},
	// 14. side_question_requests (ordinal AUTO_INCREMENT)
	{Table: "side_question_requests", PKCol: "ordinal", IdentCol: "identity", HashCol: "identity_hash"},
	// 15-22. assets 8 分支
	{Table: "assets", PKCol: "id", IdentCol: "identity_root_domain", HashCol: "hash_root_domain"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_ip", HashCol: "hash_ip"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_subdomain", HashCol: "hash_subdomain"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_app_bundle", HashCol: "hash_app_bundle"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_app_name", HashCol: "hash_app_name"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_service_http", HashCol: "hash_service_http"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_service_other", HashCol: "hash_service_other"},
	{Table: "assets", PKCol: "id", IdentCol: "identity_endpoint", HashCol: "hash_endpoint"},
	// 23-25. company_scope 3 分支
	{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_domain", HashCol: "hash_scope_domain"},
	{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_net", HashCol: "hash_scope_net"},
	{Table: "company_scope", PKCol: "id", IdentCol: "identity_scope_value", HashCol: "hash_scope_value"},
	// 26. llm_profiles (id)
	{Table: "llm_profiles", PKCol: "id", IdentCol: "name_identity", HashCol: "name_hash"},
}

// hashArbWhitelistMap 白名单四元组 map，用于 O(1) 验证。
var hashArbWhitelistMap = func() map[string]struct{} {
	m := make(map[string]struct{}, len(rawHashArbTuples))
	for _, tup := range rawHashArbTuples {
		key := tup.Table + "\x00" + tup.PKCol + "\x00" + tup.IdentCol + "\x00" + tup.HashCol
		m[key] = struct{}{}
	}
	return m
}()

// hashArbitrationWhitelist 返回所有 26 项仲裁四元组的副本（同包测试使用）。
func hashArbitrationWhitelist() []hashArbTuple {
	cp := make([]hashArbTuple, len(rawHashArbTuples))
	copy(cp, rawHashArbTuples)
	return cp
}

// VerifyHashArbitration 在 hash 命中（或写入前预检）时，使用调用方传入的同一事务 *sql.Tx
// 执行 current read（FOR UPDATE）读取完整 identity，并进行精确认真字节比对：
// - 相等时返回命中行物理 PK（uint64，支持全部无符号 64 位整型域）；
// - 不等时返回 ErrIdentityHashCollision 并且 PK 为 0；
// - 查询未命中 (sql.ErrNoRows)、driver 错误、扫描失败、ctx 取消等均显式返回非 nil 错误且 PK=0。
//
// 锁序前置条件（规格 §4.3）：
//  1. 调用方在调用本 helper 之前，必须已按照物理 PK 的全局全序获得了目标候选实体行的物理行锁；
//  2. 本 helper 仅在该已有锁的保护下执行 current 状态再验证，决不能作为预定位器（不能先按 logical UQ 或 hash 抢锁）；
//  3. 任何 1062 (Duplicate entry)、1213 (Deadlock) 或 1205 (Lock wait timeout) 均由调用方外层捕获并执行完整 rollback/replan，
//     本 helper 严禁在内部执行 commit、rollback、重试或重新开事务；
//  4. 对于内部隐藏 row_id 表返回的 row_id，仅用于内部物理行定位与外层锁续期，绝对禁止暴露进入外部 API、DTO、manifest 或 logical identity。
func VerifyHashArbitration(ctx context.Context, tx *sql.Tx, table, pkCol, identCol, hashCol string, hash [32]byte, expected []byte) (uint64, error) {
	key := table + "\x00" + pkCol + "\x00" + identCol + "\x00" + hashCol
	if _, ok := hashArbWhitelistMap[key]; !ok {
		return 0, fmt.Errorf("hash arbitration: unwhitelisted tuple (%s,%s,%s,%s)", table, pkCol, identCol, hashCol)
	}

	q := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s` WHERE `%s` = ? FOR UPDATE", pkCol, identCol, table, hashCol)
	var pk uint64
	var ident []byte
	if err := tx.QueryRowContext(ctx, q, hash[:]).Scan(&pk, &ident); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("hash arbitration: no row for hash in %s", table)
		}
		return 0, err
	}

	if !bytes.Equal(ident, expected) {
		return 0, ErrIdentityHashCollision
	}

	return pk, nil
}
