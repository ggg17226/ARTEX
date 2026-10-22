package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Autumn-27/artex/notify"
)

// 本文件的用例都会真连 TiDB（无库时跳过）。这些 SQL 用到了
// FOR UPDATE SKIP LOCKED、DATE_SUB(NOW(3), INTERVAL …)、JSON 文本列、多行 IN(...) 占位符
// 拼接，都是「编译通过但可能运行时报错」的写法，必须实跑才算验证过。

func notifyTestDB(t *testing.T) *DB {
	t.Helper()
	dsn := testDSN(t)
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open TiDB failed with DSN: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB failed: %v", err)
		}
	})
	return d
}

// mustDeleteOwned 按**本次夹具自己的物理 id**执行一条回收 DELETE。
//
// 复审 RR-I01 的硬要求：删除错误必须 t.Errorf 标红，绝不允许丢弃（旧夹具正是丢了
// 两处 d.Exec 的错误，"根表已删干净"这个结论就从没被真正验证过）。
func mustDeleteOwned(t *testing.T, d *DB, label, deleteQuery string, args ...any) {
	t.Helper()
	if _, err := d.Exec(deleteQuery, args...); err != nil {
		t.Errorf("回收%s失败: %v", label, err)
	}
}

// mustAssertOwnedGone 回读夹具自己那批主键，残留非 0 直接判红。
//
// 只按 id 集合回读，绝不按描述文本模糊统计：前缀查的是"我以为的夹具"，id 查的才是
// "本次真的建出来的那几行"。
func mustAssertOwnedGone(t *testing.T, d *DB, label, selectQuery string, args ...any) {
	t.Helper()
	var left int
	if err := d.QueryRow(selectQuery, args...).Scan(&left); err != nil {
		t.Errorf("回读%s是否归零失败: %v", label, err)
		return
	}
	if left != 0 {
		t.Errorf("%s未归零，仍残留 %d 行：本次用例把自己的业务行留在了共享库里", label, left)
	}
}

// newTestChannel 建一个渠道，测试结束自动删除，并**回读断言归零**（复审 RR-I01）。
//
// 回收失败的处置方式从"记一行日志"改成"判红"：一个仍然启用的渠道会一直吃下后续
// fan-out 的匹配结果，本次测试的残留会变成下一次测试和真实 worker 的背景噪声，
// 而测试自己仍然报绿。渠道下游的投递行一并按 channel_id 收走（TiDB 基线没有外键，
// 删渠道不会顺带清 delivery），删除错误与回读残留一律 t.Errorf。
func newTestChannel(t *testing.T, d *DB, kind, mode string, filter string) *NotificationChannel {
	t.Helper()
	if filter == "" {
		filter = `{}`
	}
	ch := &NotificationChannel{
		Name:       "测试渠道-" + t.Name(),
		Kind:       kind,
		Mode:       mode,
		Config:     json.RawMessage(`{"webhook":"https://example.com/hook"}`),
		Filter:     json.RawMessage(filter),
		RatePerMin: 100,
	}
	id, err := d.SaveNotificationChannel(context.Background(), ch)
	if err != nil {
		t.Fatalf("建渠道失败: %v", err)
	}
	t.Cleanup(func() {
		// 逆拓扑：先收渠道名下的投递行，再收渠道根行，最后回读两张表都归零。
		mustDeleteOwned(t, d, fmt.Sprintf("渠道 %d 名下的投递行", id),
			`DELETE FROM notification_deliveries WHERE channel_id=?`, id)
		mustDeleteOwned(t, d, fmt.Sprintf("渠道 %d", id),
			`DELETE FROM notification_channels WHERE id=?`, id)
		mustAssertOwnedGone(t, d, fmt.Sprintf("渠道 %d 名下的投递行", id),
			`SELECT count(*) FROM notification_deliveries WHERE channel_id=?`, id)
		mustAssertOwnedGone(t, d, fmt.Sprintf("渠道 %d", id),
			`SELECT count(*) FROM notification_channels WHERE id=?`, id)
	})
	ch.ID = id
	return ch
}

// addTestEvent 直接写一条事件（不经 finding），用于测试分派与投递。
//
// 复审 RR-I01：事件根行与它**自己产生的**投递行（按 event_id 精确匹配，一条不多删）
// 都必须在本用例收尾时彻底回收；删除错误一律 t.Errorf，收尾回读两张表归零。
// 一个没删掉的 pending 事件会被真实 dispatcher 领走并投进别人的渠道，那不是残留，
// 是持续对外部世界产生副作用。
func addTestEvent(t *testing.T, d *DB, kind string, findingID int64, snap notify.Snapshot) int64 {
	t.Helper()
	snap.Kind = kind
	snap.FindingID = findingID
	id, err := d.AddNotificationEvent(context.Background(), kind, findingID, snap)
	if err != nil {
		t.Fatalf("写事件失败: %v", err)
	}
	t.Cleanup(func() {
		mustDeleteOwned(t, d, fmt.Sprintf("事件 %d 名下的投递行", id),
			`DELETE FROM notification_deliveries WHERE event_id=?`, id)
		mustDeleteOwned(t, d, fmt.Sprintf("事件 %d", id),
			`DELETE FROM notification_events WHERE id=?`, id)
		mustAssertOwnedGone(t, d, fmt.Sprintf("事件 %d 名下的投递行", id),
			`SELECT count(*) FROM notification_deliveries WHERE event_id=?`, id)
		mustAssertOwnedGone(t, d, fmt.Sprintf("事件 %d", id),
			`SELECT count(*) FROM notification_events WHERE id=?`, id)
	})
	return id
}

func TestNotificationAssetNamesResolvesAndPreservesOrder(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	// 三类资产各有各的展示口径：域名、IP、URL。
	// 原 PG 写法是 `INSERT INTO assets(...) VALUES(...) RETURNING id`：TiDB 没有 RETURNING，
	// 且 assets 的一批 TEXT 列 NOT NULL 无默认值，统一走同包的 assetRawInsert
	// （补齐必填 JSON 列 + Exec 后用 LastInsertId 取回自增 id）。
	domID := assetRawInsert(t, d, map[string]any{"type": "subdomain", "domain": "a.example.com"})
	ipID := assetRawInsert(t, d, map[string]any{"type": "ip", "ip": "10.1.2.3"})
	svcID := assetRawInsert(t, d, map[string]any{"type": "service", "url": "https://a.example.com/admin"})
	t.Cleanup(func() {
		deleteAssets(t, d, []int64{domID, ipID, svcID}) // 先断开 link/anchor 引用，再删本体
	})

	// 传入顺序刻意乱序，且含一个不存在的 id。
	got, err := d.NotificationAssetNames(ctx, []int64{svcID, 999999999, domID, ipID, svcID})
	if err != nil {
		t.Fatalf("解析资产名失败: %v", err)
	}
	want := []string{"https://a.example.com/admin", "a.example.com", "10.1.2.3"}
	if len(got) != len(want) {
		t.Fatalf("资产名数量不符，期望 %v 得到 %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("顺序/取值不符，期望 %v 得到 %v", want, got)
		}
	}
}

// TestRecordNotificationEventTxUnwindsOnFailure 是保存点机制的核心用例：
// 在事务里先让 notification_events 的写入必然失败，断言 ① 该函数报 false
// ② 事务没有进入 aborted 状态，后续语句仍能读、也仍能写。
//
// 没有保存点的话，整条事务就作废了，后续任何语句都会失败——那正是「一个通知表的
// 问题导致漏洞存不进库」的故障路径。
//
// 失败触发方式换了：原先是临时 `ADD CONSTRAINT … CHECK (false)`，靠 PG 的「DDL
// 随事务回滚」自动撤销。TiDB 的 DDL 不参与事务、也不允许在事务里改表结构，测试
// 更没有权限动共享实例的 schema。这里改用**必然失败的写入**：
// notification_events.finding_id 是 BIGINT UNSIGNED NOT NULL，服务端 sql_mode 带
// STRICT_TRANS_TABLES，塞 -1 必然越界报错。全程零 DDL，失败语句本身不落任何行，
// 收尾仍然 ROLLBACK（原来的「约束没留下雷」等价换成「行没留下雷」）。
func TestRecordNotificationEventTxUnwindsOnFailure(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	// 唯一 finding_id：断言只数本用例自己写的那行，不与库里既有数据串台。
	sentinel := time.Now().UnixNano()

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // 撤销本用例的全部写入，见函数注释

	if RecordNotificationEventTx(ctx, tx, notify.EventFindingCreated, -1, notify.Snapshot{Severity: "high"}) {
		t.Fatal("在必然失败的写入下仍报告写入成功")
	}
	// 关键断言：事务还能用。
	var one int
	if err := tx.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("事务已被污染（保存点未生效）: %v", err)
	}
	// 失败语句的痕迹必须被保存点精确收回：同一条事务里继续写一条正常事件，
	// 既要写得进去，也只能写进这一条。
	if !RecordNotificationEventTx(ctx, tx, notify.EventFindingCreated, sentinel, notify.Snapshot{Severity: "high"}) {
		t.Fatal("失败之后同事务的正常写入被误伤（保存点未生效）")
	}
	var written int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification_events WHERE finding_id=?`, sentinel).Scan(&written); err != nil {
		t.Fatal(err)
	}
	if written != 1 {
		t.Fatalf("事务内可见事件=%d, want 1（保存点回滚不干净）", written)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("回滚失败: %v", err)
	}
	// 确认写入随回滚一起消失，不给后续用例留雷。
	var left int
	if err := d.QueryRow(`SELECT COUNT(*) FROM notification_events WHERE finding_id=?`, sentinel).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("回滚后残留 %d 行事件，会污染后续用例", left)
	}
}

func TestFanOutRoutesEventsByFilter(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	all := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	onlyCritical := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{"min_severity":"critical"}`)
	sqlOnly := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{"vulnclass_include":["SQL"]}`)

	highSQL := addTestEvent(t, d, notify.EventFindingCreated, 1001, notify.Snapshot{Severity: "high", VulnClass: "SQL注入"})
	lowXSS := addTestEvent(t, d, notify.EventFindingCreated, 1002, notify.Snapshot{Severity: "low", VulnClass: "XSS"})
	criticalXSS := addTestEvent(t, d, notify.EventFindingCreated, 1003, notify.Snapshot{Severity: "critical", VulnClass: "XSS"})

	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatalf("分派失败: %v", err)
	}

	cases := []struct {
		name    string
		eventID int64
		channel int64
		want    bool
	}{
		{"全收渠道收到 high", highSQL, all.ID, true},
		{"全收渠道收到 low", lowXSS, all.ID, true},
		{"仅严重渠道跳过 high", highSQL, onlyCritical.ID, false},
		{"仅严重渠道收到 critical", criticalXSS, onlyCritical.ID, true},
		{"仅SQL渠道收到 SQL", highSQL, sqlOnly.ID, true},
		{"仅SQL渠道跳过 XSS", lowXSS, sqlOnly.ID, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var exists bool
			if err := d.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM notification_deliveries WHERE event_id=? AND channel_id=?)`,
				tc.eventID, tc.channel).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if exists != tc.want {
				t.Fatalf("投递是否存在: 期望 %v 得到 %v", tc.want, exists)
			}
		})
	}

	// 再分派一次不应产生重复投递（fanned_out 幂等）。
	events, deliveries, err := d.FanOutPendingEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if events != 0 || deliveries != 0 {
		t.Fatalf("已分派的事件不应被再次处理，得到 events=%d deliveries=%d", events, deliveries)
	}
}

// TestFanOutMarksEventsWithNoMatchingChannel 覆盖「事件没命中任何渠道」的情况。
// 这类事件必须照样被标记为已分派，否则它会永远留在待分派集合里、每个 tick 重扫。
func TestFanOutMarksEventsWithNoMatchingChannel(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	pick := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{"vulnclass_include":["绝不匹配的类型"]}`)
	_ = pick

	ev := addTestEvent(t, d, notify.EventFindingCreated, 2001, notify.Snapshot{Severity: "high", VulnClass: "XSS"})
	// 契约是"过滤器不匹配的那台渠道不会为这条事件产生投递"。共享实例里还挂着别的
	// 用例（含历史运行）留下的渠道，它们与这条事件无关却会匹配成功，所以既不能拿
	// FanOutPendingEvents 返回的全局计数、也不能按 event_id 汇总，必须按
	// (event_id, channel_id) 这一对来数。
	_, _, err := d.FanOutPendingEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	var mine int
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification_deliveries WHERE event_id=? AND channel_id=?`,
		ev, pick.ID).Scan(&mine); err != nil {
		t.Fatal(err)
	}
	if mine != 0 {
		t.Fatalf("不该产生投递，得到 %d", mine)
	}
	var fanned bool
	if err := d.QueryRowContext(ctx, `SELECT fanned_out FROM notification_events WHERE id=?`, ev).Scan(&fanned); err != nil {
		t.Fatal(err)
	}
	if !fanned {
		t.Fatal("未命中渠道的事件也必须标记为已分派，否则会被无限重扫")
	}
}

func TestClaimRealtimeDeliveriesHonorsLeaseAndMode(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	realtime := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	digest := newTestChannel(t, d, notify.KindDingTalk, NotifyModeDigest, `{}`)

	addTestEvent(t, d, notify.EventFindingCreated, 3001, notify.Snapshot{Severity: "high", VulnClass: "XSS"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}

	// 实时领取只应拿到 realtime 渠道的那条，不该动 digest 渠道的。
	got, err := d.ClaimRealtimeDeliveries(ctx, realtime.ID, 10, time.Minute)
	if err != nil {
		t.Fatalf("领取失败: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("应领到 1 条，得到 %d", len(got))
	}
	if got[0].State != NotifyStateSending || got[0].Attempts != 1 {
		t.Fatalf("领取后应为 sending 且 attempts=1，得到 state=%s attempts=%d", got[0].State, got[0].Attempts)
	}
	// 关联加载的渲染上下文必须齐全（渠道配置 + 事件快照 + finding id）。
	if got[0].Channel == nil || len(got[0].Channel.Config) == 0 {
		t.Fatal("领取结果缺少渠道配置，渲染会失败")
	}
	if got[0].FindingID != 3001 {
		t.Fatalf("finding id 未从事件带出，得到 %d", got[0].FindingID)
	}

	// 租约未到期，第二次领取应为空——这是「同一行不会被两个 dispatcher 同时投递」
	// 的保证。
	again, err := d.ClaimRealtimeDeliveries(ctx, realtime.ID, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("租约期内不应重复领取，得到 %d 条", len(again))
	}

	// digest 渠道的投递不应被实时领取碰到。
	left, err := d.ClaimRealtimeDeliveries(ctx, digest.ID, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("实时领取不应拿到 digest 渠道的投递，得到 %d 条", len(left))
	}
}

// TestClaimExpiredLeaseRecovers 覆盖崩溃自愈：进程在投递途中挂掉会留下 sending
// 行，租约到期后必须能被重新领起来，否则这条投递永远卡住。
func TestClaimExpiredLeaseRecovers(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	addTestEvent(t, d, notify.EventFindingCreated, 4001, notify.Snapshot{Severity: "high"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	first, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("首次领取失败: %v (%d 条)", err, len(first))
	}
	// 把租约手动推到过去，模拟「租约已过期」。
	if _, err := d.Exec(`UPDATE notification_deliveries SET next_attempt_at = DATE_SUB(NOW(3), INTERVAL 1 MINUTE) WHERE id=?`, first[0].ID); err != nil {
		t.Fatal(err)
	}
	second, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("租约过期的 sending 行应可被重新领取，得到 %d 条", len(second))
	}
	if second[0].Attempts != 2 {
		t.Fatalf("重新领取应累加尝试次数，得到 %d", second[0].Attempts)
	}
}

func TestClaimSkipsDisabledChannel(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	addTestEvent(t, d, notify.EventFindingCreated, 5001, notify.Snapshot{Severity: "high"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	// 停用会把存量待发投递一起标记为 skipped。
	if err := d.SetNotificationChannelEnabled(ctx, ch.ID, false); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := d.QueryRow(`SELECT state FROM notification_deliveries WHERE channel_id=?`, ch.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != NotifyStateSkipped {
		t.Fatalf("停用渠道的存量待发投递应被标记为 skipped，得到 %s", state)
	}
	got, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("已停用渠道不应能被领取，得到 %d 条", len(got))
	}
}

func TestDigestBatchDueAndStableBatchID(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeDigest, `{}`)
	for i := 0; i < 3; i++ {
		addTestEvent(t, d, notify.EventFindingCreated, int64(6000+i), notify.Snapshot{Severity: "high"})
	}
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}

	// 批次刚建、年龄为 0，30 分钟的周期下不该到期。
	due, err := d.DigestBatchDue(ctx, ch.ID, 30*time.Minute)
	if err != nil {
		t.Fatalf("判断批次到期失败: %v", err)
	}
	if due {
		t.Fatal("刚建立的批次不应立即到期")
	}

	// 把三条投递的创建时间一起推老，模拟一个攒够周期的批次。
	if _, err := d.Exec(`UPDATE notification_deliveries SET created_at = DATE_SUB(NOW(3), INTERVAL 40 MINUTE) WHERE channel_id=?`, ch.ID); err != nil {
		t.Fatal(err)
	}
	due, err = d.DigestBatchDue(ctx, ch.ID, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !due {
		t.Fatal("超过周期的批次应判定为到期")
	}

	batch, err := d.ClaimDigestBatch(ctx, ch.ID, MaxDigestBatchSize, time.Minute)
	if err != nil {
		t.Fatalf("领取汇总批次失败: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("汇总应一次领走全部 3 条，得到 %d 条", len(batch))
	}
	if batch[0].BatchID == nil {
		t.Fatal("汇总批次必须写 batch_id，否则历史里看不出这几条是一起发的")
	}
	firstBatchID := *batch[0].BatchID
	for _, dl := range batch {
		if dl.BatchID == nil || *dl.BatchID != firstBatchID {
			t.Fatalf("同一批次应共享 batch_id，得到 %v vs %d", dl.BatchID, firstBatchID)
		}
	}

	// 让这批**整体**失败重排后再领，batch_id 必须保持原值（COALESCE 的作用）：
	// 否则一次重试就把「这批是一起发的」这个事实抹掉了。
	//
	// 必须整批重排而不是只重排一条——投递引擎发汇总消息时就是这样处理的
	// （一条消息代表整批，成败与共）。只重排一条的话，其余仍在租约期内，
	// 重领自然只拿到那一条。
	allIDs := make([]int64, 0, len(batch))
	for _, dl := range batch {
		allIDs = append(allIDs, dl.ID)
	}
	if n, err := d.RescheduleDeliveries(ctx, allIDs, batch[0].NextAttemptAt, time.Second, "模拟失败"); err != nil || n != int64(len(allIDs)) {
		t.Fatalf("整批重排应影响 %d 行，得到 n=%d err=%v", len(allIDs), n, err)
	}
	// 把租约推到过去，模拟退避时间已到。
	if _, err := d.Exec(`UPDATE notification_deliveries SET next_attempt_at = DATE_SUB(NOW(3), INTERVAL 1 MINUTE) WHERE channel_id = ?`, ch.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := d.ClaimDigestBatch(ctx, ch.ID, MaxDigestBatchSize, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 3 {
		t.Fatalf("重领应拿到全部 3 条，得到 %d", len(reclaimed))
	}
	if reclaimed[0].BatchID == nil || *reclaimed[0].BatchID != firstBatchID {
		t.Fatalf("重试后 batch_id 应保持原值 %d，得到 %v", firstBatchID, reclaimed[0].BatchID)
	}
}

func TestDeliveryStateTransitions(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	addTestEvent(t, d, notify.EventFindingCreated, 7001, notify.Snapshot{Severity: "high"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	got, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("领取失败: %v (%d)", err, len(got))
	}
	id := got[0].ID
	leaseA := got[0].NextAttemptAt

	if n, err := d.RescheduleDeliveries(ctx, []int64{id}, leaseA, time.Second, "网络抖动"); err != nil || n != 1 {
		t.Fatalf("带当前租约令牌重排应影响 1 行，得到 n=%d err=%v", n, err)
	}
	var state, lastErr string
	if err := d.QueryRow(`SELECT state, last_error FROM notification_deliveries WHERE id = ?`, id).Scan(&state, &lastErr); err != nil {
		t.Fatal(err)
	}
	if state != NotifyStatePending || lastErr != "网络抖动" {
		t.Fatalf("重排后应为 pending 并记录原因，得到 state=%s err=%q", state, lastErr)
	}

	// 代次 Fencing：重排已让行离开 sending，旧令牌再回填必须 0 行。
	if n, err := d.FailDeliveries(ctx, []int64{id}, leaseA, "过期令牌"); err != nil || n != 0 {
		t.Fatalf("旧租约令牌判失败应影响 0 行，得到 n=%d err=%v", n, err)
	}

	// 继续推进状态机必须先重新领取，拿到**新**一代租约令牌：把退避时间推到已过。
	if _, err := d.Exec(`UPDATE notification_deliveries SET next_attempt_at = DATE_SUB(NOW(3), INTERVAL 1 MINUTE) WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("退避到期后重领失败: %v (%d)", err, len(claimed))
	}
	if n, err := d.FailDeliveries(ctx, []int64{id}, claimed[0].NextAttemptAt, "重试耗尽"); err != nil || n != 1 {
		t.Fatalf("新租约令牌判失败应影响 1 行，得到 n=%d err=%v", n, err)
	}
	if err := d.QueryRow(`SELECT state FROM notification_deliveries WHERE id = ?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != NotifyStateFailed {
		t.Fatalf("应为 failed，得到 %s", state)
	}

	// 手动重发要清零重试计数并立即到期，否则会继承旧的失败预算。
	if err := d.RetryNotificationDelivery(ctx, id); err != nil {
		t.Fatalf("重发失败: %v", err)
	}
	var attempts int
	var next time.Time
	if err := d.QueryRow(`SELECT state, attempts, next_attempt_at FROM notification_deliveries WHERE id = ?`, id).Scan(&state, &attempts, &next); err != nil {
		t.Fatal(err)
	}
	if state != NotifyStatePending || attempts != 0 {
		t.Fatalf("重发后应为 pending 且 attempts=0，得到 state=%s attempts=%d", state, attempts)
	}
	if next.After(time.Now().Add(time.Second)) {
		t.Fatal("重发应立即可领")
	}

	// 已送达的投递不应能被重发：先领取拿新令牌，再前进为 sent。
	sent, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute)
	if err != nil || len(sent) != 1 {
		t.Fatalf("重发后领取失败: %v (%d)", err, len(sent))
	}
	if n, err := d.MarkDeliveriesSent(ctx, []int64{id}, sent[0].NextAttemptAt); err != nil || n != 1 {
		t.Fatalf("标记已送达应影响 1 行，得到 n=%d err=%v", n, err)
	}
	if err := d.RetryNotificationDelivery(ctx, id); err == nil {
		t.Fatal("已送达的投递不该允许重发")
	}
}

func TestListNotificationDeliveriesPagingAndFilter(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	for i := 0; i < 5; i++ {
		addTestEvent(t, d, notify.EventFindingCreated, int64(8000+i), notify.Snapshot{Severity: "high", Name: "分页测试"})
	}
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ClaimRealtimeDeliveries(ctx, ch.ID, 10, time.Minute); err != nil {
		t.Fatal(err)
	}

	page1, total, err := d.ListNotificationDeliveries(ctx, NotificationDeliveryFilter{ChannelID: ch.ID, State: NotifyStateSending}, 1, 2)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if total != 5 {
		t.Fatalf("总数应为 5，得到 %d", total)
	}
	if len(page1) != 2 {
		t.Fatalf("每页 2 条，得到 %d", len(page1))
	}
	// 新的在前：第一页首条 id 应大于第二页首条。
	page2, _, err := d.ListNotificationDeliveries(ctx, NotificationDeliveryFilter{ChannelID: ch.ID, State: NotifyStateSending}, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID >= page1[0].ID {
		t.Fatalf("分页顺序应为新的在前，得到 page1[0]=%d page2[0]=%d", page1[0].ID, page2[0].ID)
	}
	// 渲染上下文必须随历史一起返回，否则列表无法展示「推的是什么」。
	if page1[0].ChannelName == "" || page1[0].FindingID == 0 {
		t.Fatalf("历史项缺少展示字段: %+v", page1[0])
	}

	// 按状态过滤：没有 pending 的。
	pending, totalPending, err := d.ListNotificationDeliveries(ctx, NotificationDeliveryFilter{ChannelID: ch.ID, State: NotifyStatePending}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if totalPending != 0 || len(pending) != 0 {
		t.Fatalf("不该有 pending 投递，得到 %d 条 (total=%d)", len(pending), totalPending)
	}
}

func TestSetFindingStatusWithNotifyOnlyEmitsOnRealChange(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	tk, err := d.CreateTask("通知状态变更测试", "目标", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(tk.ID)
	es := d.Exploration(tk.ExplorationID)
	f, err := es.RecordFinding(ctx, RecordFindingInput{
		TaskID: tk.ID, Worker: "test", VulnClass: "SQL注入", Name: "状态变更用例",
		Severity: "high", Summary: "摘要",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Exec(`DELETE FROM notification_events WHERE finding_id=?`, f.FindingID) })

	// 落库时已登记一条 finding_created 事件，先把它数出来作为基线。
	var base int
	if err := d.QueryRow(`SELECT count(*) FROM notification_events WHERE finding_id=?`, f.FindingID).Scan(&base); err != nil {
		t.Fatal(err)
	}
	if base < 1 {
		t.Fatal("漏洞落库应在同一事务里登记一条推送事件")
	}

	// 改成同一个状态：不该产生事件（避免重复提交刷出推送噪音）。
	from, found, notified, err := d.SetFindingStatusWithNotify(ctx, f.FindingID, "pending")
	if err != nil || !found {
		t.Fatalf("状态设置失败: found=%v err=%v", found, err)
	}
	if notified {
		t.Fatal("状态未变化时不应登记推送事件")
	}
	if from != "pending" {
		t.Fatalf("应返回变更前状态 pending，得到 %q", from)
	}

	// 真正变更：应登记事件并记录 from/to。
	from, found, notified, err = d.SetFindingStatusWithNotify(ctx, f.FindingID, "fixed")
	if err != nil || !found {
		t.Fatalf("状态设置失败: found=%v err=%v", found, err)
	}
	if !notified {
		t.Fatal("状态实际变更时应登记推送事件")
	}
	if from != "pending" {
		t.Fatalf("from 应为 pending，得到 %q", from)
	}
	var snapshot []byte
	if err := d.QueryRow(`SELECT snapshot FROM notification_events WHERE finding_id=? AND kind=?`,
		f.FindingID, notify.EventFindingStatusChanged).Scan(&snapshot); err != nil {
		t.Fatalf("未找到状态变更事件: %v", err)
	}
	var snap notify.Snapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.FromStatus != "pending" || snap.ToStatus != "fixed" {
		t.Fatalf("快照里的状态流转不对: %s → %s", snap.FromStatus, snap.ToStatus)
	}
	// 快照要带上渲染所需字段，否则状态变更消息会是空壳。
	if snap.VulnClass != "SQL注入" || snap.Severity != "high" || snap.Name != "状态变更用例" {
		t.Fatalf("快照缺少渲染字段: %+v", snap)
	}
	var status string
	if err := d.QueryRow(`SELECT status FROM findings WHERE id=?`, f.FindingID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "fixed" {
		t.Fatalf("状态应已更新为 fixed，得到 %s", status)
	}

	// 不存在的漏洞：found=false，不报错。
	if _, found, _, err := d.SetFindingStatusWithNotify(ctx, 999999999, "fixed"); err != nil || found {
		t.Fatalf("不存在的漏洞应返回 found=false 且无错，得到 found=%v err=%v", found, err)
	}
}

func TestNotificationStatsSnapshot(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	ev := addTestEvent(t, d, notify.EventFindingCreated, 9001, notify.Snapshot{Severity: "high"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	stats, err := d.NotificationStatsSnapshot(ctx)
	if err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if stats.Channels < 1 || stats.ChannelsOn < 1 {
		t.Fatalf("渠道计数不对: %+v", stats)
	}
	if stats.Pending < 1 {
		t.Fatalf("应统计到待发投递: %+v", stats)
	}
	// 积压年龄统计的是**全库**口径（最老一条待发投递距今的毫秒数）：共享实例里
	// 别的用例（以及历史残留的 pending 投递）会一起进这条 MIN()，所以"刚建的投递
	// ⇒ 接近 0"这种绝对值断言在真库上不成立。改成两条都成立的口径：
	// 	① 本用例自己那条投递的年龄确实接近 0（原意图）；
	//	② 统计值必须等于按同一定义独立复算的结果（把"秒当毫秒"这类单位错误照旧判红）。
	var ownAge int64
	if err := d.QueryRowContext(ctx, `SELECT COALESCE(TIMESTAMPDIFF(MICROSECOND, created_at, NOW(3)) DIV 1000, 0)
		FROM notification_deliveries WHERE event_id=? ORDER BY id LIMIT 1`, ev).Scan(&ownAge); err != nil {
		t.Fatalf("读取本次投递年龄失败: %v", err)
	}
	if ownAge < 0 || ownAge > int64(time.Hour/time.Millisecond) {
		t.Fatalf("刚建的投递年龄不合法: %d ms", ownAge)
	}
	var wantAge int64
	if err := d.QueryRowContext(ctx, `SELECT COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(3)) DIV 1000, 0)
		FROM notification_deliveries WHERE state=?`, NotifyStatePending).Scan(&wantAge); err != nil {
		t.Fatalf("独立复算积压年龄失败: %v", err)
	}
	if stats.BacklogAgeMS < 0 || stats.BacklogAgeMS-wantAge > 5000 || wantAge-stats.BacklogAgeMS > 5000 {
		t.Fatalf("积压年龄与定义不符: stats=%d ms, 同定义复算=%d ms", stats.BacklogAgeMS, wantAge)
	}
	_ = ch
}

func TestNotificationChannelCRUDRoundTrip(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	ch := &NotificationChannel{
		Name:       "CRUD 往返",
		Kind:       notify.KindEmail,
		Mode:       NotifyModeDigest,
		Config:     json.RawMessage(`{"host":"smtp.example.com","port":587,"from":"a@b.c","to":["x@y.z"]}`),
		Filter:     json.RawMessage(`{"min_severity":"medium","on_status_change":true}`),
		RatePerMin: 42,
	}
	id, err := d.SaveNotificationChannel(ctx, ch)
	if err != nil {
		t.Fatalf("新建失败: %v", err)
	}
	t.Cleanup(func() { d.Exec(`DELETE FROM notification_channels WHERE id=?`, id) })

	got, err := d.NotificationChannelByID(ctx, id)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if got.Mode != NotifyModeDigest || got.RatePerMin != 42 || got.Name != "CRUD 往返" {
		t.Fatalf("往返字段不一致: %+v", got)
	}
	if !got.IsEnabled() {
		t.Fatal("默认应为启用")
	}
	var cfg map[string]any
	if err := json.Unmarshal(got.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["host"] != "smtp.example.com" {
		t.Fatalf("配置未正确落库: %v", cfg)
	}
	var filter notify.Filter
	if err := json.Unmarshal(got.Filter, &filter); err != nil {
		t.Fatal(err)
	}
	if filter.MinSeverity != "medium" || !filter.OnStatusChange {
		t.Fatalf("过滤条件未正确落库: %+v", filter)
	}

	// 更新后再读。
	got.Name = "改名了"
	off := false
	got.Enabled = &off
	if _, err := d.SaveNotificationChannel(ctx, got); err != nil {
		t.Fatal(err)
	}
	after, err := d.NotificationChannelByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Name != "改名了" || after.IsEnabled() {
		t.Fatalf("更新未生效: %+v", after)
	}

	// 删除后应报「不存在」而不是静默成功。
	if err := d.DeleteNotificationChannel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := d.NotificationChannelByID(ctx, id); err != ErrNotificationChannelNotFound {
		t.Fatalf("期望 ErrNotificationChannelNotFound，得到 %v", err)
	}
	if err := d.DeleteNotificationChannel(ctx, id); err != ErrNotificationChannelNotFound {
		t.Fatalf("重复删除应报不存在，得到 %v", err)
	}
}

// TestSaveNotificationChannelKeepsExplicitZeroRate 锁住一个曾经写错的地方：
// **0 是合法配置，含义是「不限流」，不能被 db 层当成「未指定」覆盖成默认值**。
//
// 历史 bug：SaveNotificationChannel 里写了 `if RatePerMin <= 0 { 取默认值 }`，
// 于是文档、UI 提示、takeTokens 都按「0=不限流」解释，唯独写库这一层悄悄改成
// 20（钉钉/企微/Telegram）或 100（飞书）——操作者以为放开了限流、实际被卡着，
// 而且没有任何提示。「未指定」与「显式 0」的区别只有请求体能表达，
// 所以默认值在 server 层填（见 notifyCreateChannel），db 层只管存。
func TestSaveNotificationChannelKeepsExplicitZeroRate(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	// 显式 0（不限流）：必须原样存下来。
	unlimited := &NotificationChannel{
		Name: "不限流", Kind: notify.KindDingTalk, RatePerMin: 0,
		Config: json.RawMessage(`{"webhook":"https://example.com/h"}`),
	}
	id, err := d.SaveNotificationChannel(ctx, unlimited)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Exec(`DELETE FROM notification_channels WHERE id=?`, id) })
	got, err := d.NotificationChannelByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.RatePerMin != 0 {
		t.Fatalf("显式 0 表示不限流，必须原样保存，得到 %d", got.RatePerMin)
	}
	if got.Mode != NotifyModeRealtime {
		t.Fatalf("默认模式应为 realtime，得到 %s", got.Mode)
	}

	// 负值是非法输入，应被拒绝而不是悄悄改成别的值。
	bad := &NotificationChannel{
		Name: "负限流", Kind: notify.KindDingTalk, RatePerMin: -1,
		Config: json.RawMessage(`{"webhook":"https://example.com/h"}`),
	}
	if _, err := d.SaveNotificationChannel(ctx, bad); err == nil {
		t.Fatal("负限流应被拒绝")
	}
}

// TestDeleteChannelCascadesDeliveries 锁住外键行为：渠道删除后其投递历史一并消失
// （配置都没了，历史无从解读），但事件本身要留下——它可能还被别的渠道引用。
func TestDeleteChannelCascadesDeliveries(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeRealtime, `{}`)
	ev := addTestEvent(t, d, notify.EventFindingCreated, 9101, notify.Snapshot{Severity: "high"})
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := d.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE channel_id=?`, ch.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("前置条件不成立：未产生投递")
	}
	if err := d.DeleteNotificationChannel(ctx, ch.ID); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := d.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE channel_id=?`, ch.ID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 0 {
		t.Fatalf("渠道删除后其投递应级联删除，仍有 %d 条", after)
	}
	var evExists bool
	if err := d.QueryRow(`SELECT EXISTS(SELECT 1 FROM notification_events WHERE id=?)`, ev).Scan(&evExists); err != nil {
		t.Fatal(err)
	}
	if !evExists {
		t.Fatal("删渠道不应连带删除事件本身")
	}
}

// TestClaimDigestBatchHonorsCallerLimit 覆盖审计指出的一处口子：
// 汇总渠道此前完全绕过令牌桶——allow 被 takeTokens 扣掉却没人用，
// rate_per_min 对 digest 模式毫无作用。现在 limit 也参与约束。
func TestClaimDigestBatchHonorsCallerLimit(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()
	ch := newTestChannel(t, d, notify.KindDingTalk, NotifyModeDigest, `{}`)
	for i := 0; i < 10; i++ {
		addTestEvent(t, d, notify.EventFindingCreated, int64(7000+i), notify.Snapshot{Severity: "high"})
	}
	if _, _, err := d.FanOutPendingEvents(ctx, 100); err != nil {
		t.Fatal(err)
	}
	// 取 limit=3：只能领到 3 条，其余留在库里。
	got, err := d.ClaimDigestBatch(ctx, ch.ID, 3, time.Minute)
	if err != nil {
		t.Fatalf("领取失败: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("应按调用方限流额度只领 3 条，得到 %d", len(got))
	}
	// limit=0 表示本轮额度用尽：一条都不该领，也不该报错。
	if got, err := d.ClaimDigestBatch(ctx, ch.ID, 0, time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("额度为 0 时应领 0 条且不报错，得到 %d 条 err=%v", len(got), err)
	}
}

// TestFinishFindingRetestEmitsStatusChange 覆盖审计指出的一处完整性缺口：
// 复测结论为「已修复」时，状态确实变了，但那条 UPDATE 是直接写库的、
// 绕过了带通知的版本——于是配了 on_status_change 的渠道对这种状态流转
// 完全收不到推送，界面上状态悄悄变了，运维要打开平台才知道。
//
// 这条用例锁住「所有改状态的路径都要登记状态变更事件」。
func TestFinishFindingRetestEmitsStatusChange(t *testing.T) {
	d := notifyTestDB(t)
	ctx := context.Background()

	tk, err := d.CreateTask("复测推送测试", "目标", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.DeleteTask(tk.ID)
	es := d.Exploration(tk.ExplorationID)
	f, err := es.RecordFinding(ctx, RecordFindingInput{
		TaskID: tk.ID, Worker: "test", VulnClass: "SQL注入", Name: "复测目标",
		Severity: "high", Summary: "摘要",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Exec(`DELETE FROM notification_events WHERE finding_id=?`, f.FindingID) })

	// 建一条复测记录并直接推到完成态。
	rt, _, _, err := d.CreateFindingRetest(ctx, f.FindingID, "复核")
	if err != nil {
		t.Fatal(err)
	}
	if rt.ConversationID == nil {
		t.Fatal("复测应关联一个会话")
	}
	// 复测必须先进入 running 才能落结论（与真实流程一致）。
	if ok, err := d.StartFindingRetest(ctx, rt.ID); err != nil || !ok {
		t.Fatalf("启动复测失败: ok=%v err=%v", ok, err)
	}
	if err := d.RecordFindingRetestResult(ctx, *rt.ConversationID, "fixed", "已修复", "证据"); err != nil {
		t.Fatal(err)
	}
	if err := d.FinishFindingRetest(rt.ID, "completed", ""); err != nil {
		t.Fatalf("结束复测失败: %v", err)
	}

	var status string
	if err := d.QueryRow(`SELECT status FROM findings WHERE id=?`, f.FindingID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != FindingFixed {
		t.Fatalf("复测判已修复后状态应为 fixed，得到 %s", status)
	}

	// 关键断言：必须有一条状态变更事件，且 from/to 正确。
	var snapshot []byte
	err = d.QueryRow(`SELECT snapshot FROM notification_events WHERE finding_id=? AND kind=? ORDER BY id DESC LIMIT 1`,
		f.FindingID, notify.EventFindingStatusChanged).Scan(&snapshot)
	if err != nil {
		t.Fatalf("复测判已修复应登记状态变更推送事件（否则配了 on_status_change 的渠道收不到）: %v", err)
	}
	var snap notify.Snapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.FromStatus != "pending" || snap.ToStatus != FindingFixed {
		t.Fatalf("快照的状态流转不对: %s → %s", snap.FromStatus, snap.ToStatus)
	}
	// 快照要带渲染所需字段，否则推送出来是空壳。
	if snap.Name != "复测目标" || snap.Severity != "high" {
		t.Fatalf("快照缺少渲染字段: %+v", snap)
	}
}
