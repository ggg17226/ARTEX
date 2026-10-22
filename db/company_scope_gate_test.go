package db

// 回归测试：company_scope / 归属重算的写者必须与任务写者共用同一把全局 Business Gate。
//
// 背景（R33 合并前审查 Important-1 / P1-C）：
//   - 任务绑定的资产写入（runTaskBoundUpsert 的 taskID>0 分支）持 Gate、不持 companies
//     屏障，它在 Gate 内「读 company_scope 解析归属 → 写 assets.company_id」；
//   - 企业范围写者原先只持 companies 屏障、不持 Gate，于是能在"解析"与"落库"之间提交，
//     让资产按作废的旧范围落库（漂移），并与「持 tasks 行锁 → 要 companies 屏障」的写者
//     构成 tasks ↔ companies 环；
//   - 归档恢复的 company 存活核验也依赖同一件事：它只持 Gate，因此只有当企业删除者
//     同样持 Gate 时，它的快照才不会被一次并发删除插进来（TiDB 无外键可挡悬空
//     company_id）。
//
// 判据不用计时猜测：由 runTaskWriteTx（它的第一件事就是 AcquireLocalBusiness，与产品
// 入口完全同一把门禁）先占住 Gate，再把每个写者扔进去 —— 写者若真的走 Gate，就必然
// 卡在门禁上无法完成；反之会立刻返回，用例判红。
//
// 依赖真实库（openTestDB 在 DSN 未配置时会跳过），不连库时本文件不产生任何动作。

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestCompanyScopeWritersShareBusinessGate(t *testing.T) {
	d := openTestDB(t)

	cs := d.Companies()
	suffix := time.Now().UnixNano()
	companyID, _, _, _, _, err := cs.CreateCompanyWithScope(
		fmt.Sprintf("gate-probe-%d", suffix), "",
		[]ScopeInput{{Kind: "domain", Value: fmt.Sprintf("gate-probe-%d.example.test", suffix)}},
		"company scope gate regression")
	if err != nil {
		t.Fatalf("建测试企业失败: %v", err)
	}
	t.Cleanup(func() {
		// 用例正常路径里由 DeleteCompanyWithAssets 分支删除；这里只兜底，吞掉"已不存在"。
		_, _ = cs.DeleteCompanyWithAssets(companyID, true)
	})

	// 1. 占住 Gate（与产品入口同一把：runTaskWriteTx → AcquireLocalBusiness → locks 行）。
	held := make(chan struct{})
	release := make(chan struct{})
	holderErr := make(chan error, 1)
	go func() {
		holderErr <- d.runTaskWriteTx(func(ctx context.Context, _ *sql.Tx) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	type writerCase struct {
		name string
		run  func() error
	}
	cases := []writerCase{
		{"CreateCompanyWithScope", func() error {
			_, _, _, _, _, err := cs.CreateCompanyWithScope(
				fmt.Sprintf("gate-probe-inner-%d", suffix), "", nil, "gate regression")
			return err
		}},
		{"AddScopeInputsChecked", func() error {
			_, _, _, _, err := cs.AddScopeInputsChecked(companyID,
				[]ScopeInput{{Kind: "domain", Value: fmt.Sprintf("gate-add-%d.example.test", suffix)}},
				"gate regression")
			return err
		}},
		{"UpdateScopeInputsChecked", func() error {
			_, _, _, err := cs.UpdateScopeInputsChecked(companyID,
				[]ScopeInput{{Kind: "domain", Value: fmt.Sprintf("gate-update-%d.example.test", suffix)}},
				"gate regression")
			return err
		}},
		{"RecomputeAttribution", func() error { return cs.RecomputeAttribution() }},
		{"DeleteCompanyWithAssets", func() error {
			_, err := cs.DeleteCompanyWithAssets(companyID, true)
			return err
		}},
	}

	// 2. 每个写者都必须在 Gate 被占用期间无法完成。
	const blockedWindow = 500 * time.Millisecond
	done := make([]chan error, 0, len(cases))
	for _, tc := range cases {
		tc := tc
		started := make(chan struct{})
		finished := make(chan error, 1)
		done = append(done, finished)
		go func() {
			close(started)
			finished <- tc.run()
		}()
		<-started
		select {
		case err := <-finished:
			close(release)
			<-holderErr
			t.Fatalf("%s 在 Business Gate 被占用时仍然完成了（err=%v）：它没有走 Gate，"+
				"company_scope 写者与任务写者之间的漂移窗口/锁序环仍然存在", tc.name, err)
		case <-time.After(blockedWindow):
			// 正确的行为：卡在门禁上。
		}
	}

	// 3. 放行 Gate，等全部写者收尾，确认它们能正常完成（不是永久阻塞）。
	close(release)
	if err := <-holderErr; err != nil {
		t.Fatalf("占住 Gate 的空事务失败: %v", err)
	}
	for index := range done {
		select {
		case err := <-done[index]:
			// 放行后允许业务错误（例如删企业分支先跑，后续分支撞"企业不存在"），
			// 这里只要求它会返回，不要求成功。
			_ = err
		case <-time.After(30 * time.Second):
			t.Fatalf("%s 在 Gate 放行后 30s 仍未返回：疑似死锁", cases[index].name)
		}
	}
}
