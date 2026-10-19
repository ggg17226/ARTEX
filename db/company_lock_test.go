package db

import (
	"os"
	"strings"
	"testing"
)

// TestCompanyScopeMutationBarrierIsTiDBNative 取代旧的
// TestCompanyScopeMutationLockDoesNotReuseInfrastructureLocks。
//
// 原实现是一条 `SELECT pg_advisory_xact_lock($1)`：那枚锁号 7337741003 一旦与 schema
// migration（7337741001）或跨包测试套件（7337741002）撞号，两条互不相干的流程就会
// 互相死锁，所以当年只能靠"锁号不复用"守卫兜底。TiDB 没有 advisory lock，也没有可
// 复用的数字锁号概念 —— 该前提消失，守卫随之下移到真正的合同上：
//  1. 生产代码里不得再出现 advisory lock，也不得改用会话级 GET_LOCK / LOCK TABLES
//     （前者不会随事务释放、会顺着连接池泄漏成事实上的全局互斥，后者直接锁死业务）；
//  2. 变更屏障必须是随 commit/rollback 原子释放的 FOR UPDATE，且顺序恒为主键升序
//     （顺序不固定等于给每一次并发变更发一张死锁许可证）。
func TestCompanyScopeMutationBarrierIsTiDBNative(t *testing.T) {
	raw, err := os.ReadFile("companies.go")
	if err != nil {
		t.Fatalf("read companies.go: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	code := make([]string, 0, len(lines))
	for _, line := range lines {
		comment, _, _ := strings.Cut(line, "//")
		code = append(code, comment)
	}
	normalized := strings.Join(strings.Fields(strings.Join(code, "\n")), " ")
	upper := strings.ToUpper(normalized)

	for _, forbidden := range []string{"PG_ADVISORY", "GET_LOCK", "RELEASE_LOCK", "LOCK TABLES", "LOCK IN SHARE MODE"} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("company scope mutation barrier must not use %q (TiDB 下它不会随事务释放，或直接锁表)", forbidden)
		}
	}
	if !strings.Contains(normalized, "SELECT id FROM companies ORDER BY id FOR UPDATE") {
		t.Error("company scope mutation barrier must lock companies in primary-key order with FOR UPDATE")
	}
}
