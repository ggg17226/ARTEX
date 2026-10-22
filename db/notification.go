package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Autumn-27/artex/notify"
)

// fanOutDeliveryHookFunc 是 fan-out 的测试屏障：在**已经读出启用渠道列表、
// 还没有写入任何投递行**时被调用。
//
// 为什么需要它：投递行一旦插下去，channel_id 指向的就是"读渠道那一刻"的快照。
// TiDB 基线没有外键，渠道被并发删除/停用都挡不住这条 INSERT，留下的就是列表
// JOIN 看不见、队列与统计照算的孤儿投递。这段窗口在真机上只有几十微秒，
// sleep 式并发测试既复现不稳、也证明不了什么，所以在这里留一道显式屏障，
// 让测试能把"已读渠道 → 渠道被删并提交 → 继续插投递"钉成确定交错。
//
// 契约同 db 包其它测试钩子（见 TaskCascadeTestHookAfterSnapshot）：
//   - 默认永远为 nil，生产路径只多一次 atomic.Pointer.Load；
//   - 只允许 db 包内测试经 SetFanOutDeliveryTestHook 赋值；
//   - 回调在 fan-out 事务内执行，回调自身不得再写这个库。
type fanOutDeliveryHookFunc func(stage string, channelIDs []int64)

var fanOutDeliveryHook atomic.Pointer[fanOutDeliveryHookFunc]

// SetFanOutDeliveryTestHook 安装 fan-out 屏障；传 nil 卸载。
func SetFanOutDeliveryTestHook(h fanOutDeliveryHookFunc) {
	if h == nil {
		fanOutDeliveryHook.Store(nil)
		return
	}
	fanOutDeliveryHook.Store(&h)
}

// notifyFanOutDeliveryHook 是无钩子时零成本的调用点包装。
func notifyFanOutDeliveryHook(stage string, channelIDs []int64) {
	if hook := fanOutDeliveryHook.Load(); hook != nil {
		(*hook)(stage, channelIDs)
	}
}

// 本文件是 IM 推送的渠道配置与事件层。投递任务的领取与状态流转见
// db/notification_delivery.go。
//
// 两条不变量，改这个文件时务必保持：
//
//  1. 写漏洞的事务(RecordFindingTx)只调用 InsertNotificationEventTx 做一次盲插，
//     不读任何通知相关的表、不做过滤匹配。任何在这里引入的读操作都可能因为
//     用户配错的过滤条件而污染甚至中止漏洞写入事务。
//  2. 过滤匹配永不报错：配置畸形一律按「命中」处理(见 notify.Match)。宁可多推，
//     不可漏推。

// ErrNotificationChannelNotFound 渠道不存在。
var ErrNotificationChannelNotFound = errors.New("通知渠道不存在")

// 投递状态。
const (
	NotifyStatePending = "pending" // 待发
	NotifyStateSending = "sending" // 已被某个 dispatcher 领取，租约未到期
	NotifyStateSent    = "sent"    // 已送达
	NotifyStateFailed  = "failed"  // 重试耗尽或永久失败，可手动重发
	NotifyStateSkipped = "skipped" // 渠道已停用，不再发送
)

// 推送模式。
const (
	NotifyModeRealtime = "realtime"
	NotifyModeDigest   = "digest"
)

// ValidNotifyMode 白名单校验推送模式（与 findings.status 同理：不用 DB CHECK，
// 便于后续扩展）。
func ValidNotifyMode(m string) bool {
	return m == NotifyModeRealtime || m == NotifyModeDigest
}

// NotificationChannel 是一个渠道实例配置。Config 与 Filter 保持原始 JSON，
// 解析交给 notify 包——db 层不理解它们的字段含义。
type NotificationChannel struct {
	ID     int64           `json:"id"`
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Mode   string          `json:"mode"`
	Config json.RawMessage `json:"config"`
	Filter json.RawMessage `json:"filter"`
	// Enabled 用指针是为了区分「没传这个字段」与「显式传 false」——
	// 前端开关控件只提交被改动的字段。
	Enabled    *bool     `json:"enabled,omitempty"`
	RatePerMin int       `json:"rate_per_min"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// IsEnabled 返回渠道是否启用；Enabled 为 nil（未加载）时按启用处理。
func (c *NotificationChannel) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

// NotificationEvent 是一条事件事实。
type NotificationEvent struct {
	ID        int64           `json:"id"`
	Kind      string          `json:"kind"`
	FindingID int64           `json:"finding_id"`
	Snapshot  json.RawMessage `json:"snapshot"`
	CreatedAt time.Time       `json:"created_at"`
}

const notificationChannelCols = `id, name, kind, enabled, config, mode, filter, rate_per_min, created_at, updated_at`

func scanNotificationChannel(sc interface{ Scan(...any) error }) (*NotificationChannel, error) {
	var c NotificationChannel
	var enabled bool
	if err := sc.Scan(&c.ID, &c.Name, &c.Kind, &enabled, &c.Config, &c.Mode, &c.Filter, &c.RatePerMin, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	c.Enabled = &enabled
	return &c, nil
}

// ListNotificationChannels 返回全部渠道实例，启用的排在前面、同级按 id。
// 排序放在 SQL 里是为了让 UI 与 dispatcher 看到同一个稳定顺序。
func (d *DB) ListNotificationChannels(ctx context.Context) ([]*NotificationChannel, error) {
	rows, err := d.QueryContext(ctx, `SELECT `+notificationChannelCols+` FROM notification_channels
ORDER BY enabled DESC, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*NotificationChannel{}
	for rows.Next() {
		c, err := scanNotificationChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// NotificationChannelByID 取单个渠道。
func (d *DB) NotificationChannelByID(ctx context.Context, id int64) (*NotificationChannel, error) {
	row := d.QueryRowContext(ctx, `SELECT `+notificationChannelCols+` FROM notification_channels WHERE id = ?`, id)
	c, err := scanNotificationChannel(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotificationChannelNotFound
	}
	return c, err
}

// SaveNotificationChannel 新建或更新一个渠道。
//
// 更新时只覆盖调用方显式给出的字段（非 nil / 非空），这样前端可以提交局部
// 修改的抽屉表单，而不必回传 config 里那些它没展示的字段——回传反而会造成
// 「掩码值把真实密钥覆盖掉」的事故。
func (d *DB) SaveNotificationChannel(ctx context.Context, c *NotificationChannel) (int64, error) {
	if c.Mode == "" {
		c.Mode = NotifyModeRealtime
	}
	// 这里刻意**不**对 0 做任何加工：0 是合法配置，含义是「不限流」。
	//
	// 曾经写成 `if c.RatePerMin <= 0 { c.RatePerMin = 默认值 }`，本意是「未指定时
	// 给个安全默认」，但那把「显式设成 0」也一起吞掉了——文档、UI 提示与
	// takeTokens 都把 0 解释为不限流，唯独这里悄悄改成 20（钉钉/企微/Telegram）
	// 或 100（飞书），操作者以为放开了限流、实际被 20/分钟卡着且没有任何提示。
	//
	// 「未指定」与「显式 0」的区别只有调用方知道（请求体里字段缺省 vs 明确传 0），
	// 所以默认值由 server 层在字段缺省时填，见 notifyCreateChannel。
	if c.RatePerMin < 0 {
		return 0, errors.New("限流值不能为负")
	}
	if c.Config == nil {
		c.Config = json.RawMessage(`{}`)
	}
	if c.Filter == nil {
		c.Filter = json.RawMessage(`{}`)
	}
	enabled := c.IsEnabled()

	if c.ID == 0 {
		// TiDB 没有 INSERT … RETURNING：自增主键一律走 LastInsertId，并校验它真的
		// 是一个合法主键（拿 0 或负数当 id 返回，调用方会去更新一条不存在的渠道）。
		res, err := d.ExecContext(ctx, `INSERT INTO notification_channels(name,kind,enabled,config,mode,filter,rate_per_min)
VALUES(?, ?, ?, ?, ?, ?, ?)`,
			c.Name, c.Kind, enabled, string(c.Config), c.Mode, string(c.Filter), c.RatePerMin)
		if err != nil {
			return 0, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return 0, err
		}
		if id <= 0 {
			return 0, fmt.Errorf("新建通知渠道未返回合法自增主键：%d", id)
		}
		return id, nil
	}
	// 先核验渠道真实存在，解耦存在性与 changed rows（解决相同配置保存/重复启停问题，闭环 F03）
	var exists int
	if err := d.QueryRowContext(ctx, `SELECT 1 FROM notification_channels WHERE id = ?`, c.ID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotificationChannelNotFound
		}
		return 0, err
	}
	_, err := d.ExecContext(ctx, `UPDATE notification_channels
SET name = ?, kind = ?, enabled = ?, config = ?, mode = ?, filter = ?, rate_per_min = ?
WHERE id = ?`,
		c.Name, c.Kind, enabled, string(c.Config), c.Mode, string(c.Filter), c.RatePerMin, c.ID)
	if err != nil {
		return 0, err
	}
	return c.ID, nil
}

// SetNotificationChannelEnabled 切换启停。
//
// 停用一个渠道时，把它尚未发出的投递一并标记为 skipped：否则重新启用后
// 会突然收到一批「停用期间积压」的旧漏洞，时效已失且容易误判为新增。
func (d *DB) SetNotificationChannelEnabled(ctx context.Context, id int64, enabled bool) error {
	return d.WithEvidenceTx(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM notification_channels WHERE id = ? FOR UPDATE`, id).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotificationChannelNotFound
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE notification_channels SET enabled = ? WHERE id = ?`, enabled, id); err != nil {
			return err
		}
		if !enabled {
			if _, err := tx.ExecContext(ctx, `UPDATE notification_deliveries SET state = ?, last_error = ?
WHERE channel_id = ? AND state IN (?, ?)`,
				NotifyStateSkipped, "渠道已停用", id, NotifyStatePending, NotifyStateSending); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteNotificationChannel 删除渠道。其投递历史一并删除
// （渠道配置都没了，历史无从解读）。
//
// PG 时代 notification_deliveries.channel_id 带 REFERENCES … ON DELETE CASCADE，
// 级联由数据库顺带完成；TiDB 基线不建外键，这道级联必须在这里显式做掉，否则会
// 留下指向已删渠道的孤儿投递，被 worker 继续领取、甚至继续往外发。
// 两步放同一个事务：渠道删了、投递没删干净，就是那种要等下一轮 tick 才暴露的脏数据。
//
// 渠道行锁是与 fan-out 共同的串行点（审查 I-02）：这里**先锁渠道再删**。
// FanOutPendingEvents 在 INSERT 投递行之前同样按 id 升序锁这批渠道行并持锁重验存活，
// 两边同向排队，不会出现"fan-out 拿着旧渠道快照往已删渠道里插投递"的孤儿。
// 统一全序：渠道行 → 该渠道的投递行（SetNotificationChannelEnabled 同序）；
// fan-out 拿齐全部渠道锁之后才插投递，因此不存在"持着投递锁回头等渠道锁"的反向边。
func (d *DB) DeleteNotificationChannel(ctx context.Context, id int64) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // 任一步失败都整笔撤销，不留半删状态
	// 先持渠道行锁：从这一刻起，任何 fan-out 的"插投递前重验"都必须排在本事务后面，
	// 它要么看见还活着的渠道，要么看见本事务删完之后的结果，不存在中间态。
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM notification_channels WHERE id = ? FOR UPDATE`, id).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotificationChannelNotFound
		}
		return err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM notification_channels WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// 已持锁却删不到行：与"渠道不存在"同义（并发删除的赢家已先提交）。
		return ErrNotificationChannelNotFound
	}
	// 按 channel_id 的**范围删除**（而非资产侧那种"按物理主键分批"）在这里是安全且
	// 有意的（R33 审查 Minor-3）：上面那条渠道行锁已经把"插入新投递"的唯一入口
	// （lockNotificationChannelsForInsert 的 FOR UPDATE 重验）挡在事务外 —— 没有并发
	// 写者能在本事务持锁期间改出指向 id 的新投递行，因此不存在"漏删"的目录漂移。
	// 反之若改成先查 id 再按 PK 删，就会退化成"快照列 id → 逐条删"，把已被锁排除的
	// 竞态重新请回来。投递表没有别的删除入口能绕过这把锁。
	if _, err := tx.ExecContext(ctx, `DELETE FROM notification_deliveries WHERE channel_id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// lockNotificationChannelsForInsert 按 id 升序锁住本轮要写投递行的渠道，返回持锁后
// **仍然存活且启用**的渠道集合（审查 I-02）。
//
// 为什么必须在 INSERT 之前锁着再看一眼：投递行里的 channel_id 记的是"读渠道那一刻"
// 的快照。TiDB 基线没有外键，渠道被并发删除/停用都挡不住这条 INSERT，插下去就是
// 指向已删渠道的孤儿 —— 列表 JOIN 渠道把它藏起来，投递队列与统计照算不误。
//
// 锁序与渠道删除/启停一致（渠道行按 id 升序 → 之后才碰投递行），两边不会反向。
// 这里必须是 FOR UPDATE：TiDB 下锁定读才是当前读，返回缺行代表渠道已被删除，
// 而本事务此前那句普通 SELECT 完全可能还看得见它。
func lockNotificationChannelsForInsert(ctx context.Context, tx *sql.Tx, channelIDs []int64) (map[int64]bool, error) {
	alive := make(map[int64]bool, len(channelIDs))
	if len(channelIDs) == 0 {
		return alive, nil
	}
	ph, args := markList(channelIDs)
	rows, err := tx.QueryContext(ctx, `SELECT id, enabled FROM notification_channels
WHERE id IN (`+ph+`) ORDER BY id FOR UPDATE`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id      int64
			enabled bool
		)
		if err := rows.Scan(&id, &enabled); err != nil {
			return nil, err
		}
		// 渠道还在但已被停用：本轮不给它补新行（停用者已把存量置为 skipped）。
		alive[id] = enabled
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return alive, nil
}

// RecordNotificationEventTx 在调用方的事务里**尽力**写入一条推送事件。
//
// 这是漏洞写入路径上唯一的通知相关改动：一次 INSERT，不读任何表、不认识渠道、
// 不跑过滤。事务提交即保证「漏洞落库」与「推送任务存在」原子一致，
// 不存在提交成功却没入队、消息永久丢失的窗口。
//
// 两个关键设计，都不是随手写的：
//
//  1. **为什么用 SAVEPOINT 包住这条 INSERT**：TiDB 不像 PostgreSQL 会把出错的事务
//     整体置成 aborted，但保存点仍然是这里正确的隔离手段——它把「这条 INSERT 可能
//     牵连到的东西」精确圈在一个名字里，失败时 `ROLLBACK TO` 一次性收回，
//     调用方（漏洞写入事务）里已经拿到的行锁与已写的其它行都不受影响。
//     保存点本身失败时**不放弃推送**：继续裸插一次，最差退化成「没隔离」。
//
//  2. **为什么整笔回滚是错的**：推送是便利功能，漏洞记录才是产品本身。一个通知
//     表的问题（旧库未迁移、磁盘瞬时故障）不该让高危漏洞存不进库。所以这里隔离
//     错误、记日志、返回 false，让漏洞写入照常提交——代价是丢掉这一条推送。
//     返回 bool 而非 error 是刻意的：调用方不该把它当作会影响写入成败的错误。
func RecordNotificationEventTx(ctx context.Context, tx *sql.Tx, kind string, findingID int64, snap notify.Snapshot) bool {
	raw, err := json.Marshal(snap)
	if err != nil {
		log.Printf("[notify] 序列化推送事件失败 finding=%d: %v", findingID, err)
		return false
	}
	fenced := true
	if _, err := tx.ExecContext(ctx, `SAVEPOINT notify_event`); err != nil {
		// 建不了保存点不代表这条 INSERT 会毁掉调用方的事务，记一笔继续裸插。
		log.Printf("[notify] 建立保存点失败 finding=%d，改为裸插: %v", findingID, err)
		fenced = false
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO notification_events(kind,finding_id,snapshot) VALUES(?, ?, ?)`,
		kind, findingID, string(raw)); err != nil {
		log.Printf("[notify] 写入推送事件失败 finding=%d（漏洞记录不受影响）: %v", findingID, err)
		if fenced {
			// 回滚到保存点，把这条 INSERT 的痕迹精确收回，调用方的事务继续可写。
			if _, rbErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT notify_event`); rbErr != nil {
				log.Printf("[notify] 回滚到保存点失败 finding=%d: %v", findingID, rbErr)
			}
		}
		return false
	}
	// 释放保存点，避免长事务里积攒无用的保存点。
	if fenced {
		_, _ = tx.ExecContext(ctx, `RELEASE SAVEPOINT notify_event`)
	}
	return true
}

// AddNotificationEvent 是 InsertNotificationEventTx 的独立事务版本，供不在
// 既有事务里的调用点使用（如渠道的「发送测试消息」，它没有真实 finding）。
func (d *DB) AddNotificationEvent(ctx context.Context, kind string, findingID int64, snap notify.Snapshot) (int64, error) {
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, fmt.Errorf("序列化通知事件快照失败: %w", err)
	}
	var id int64
	res, err := d.ExecContext(ctx, `INSERT INTO notification_events(kind,finding_id,snapshot) VALUES(?, ?, ?)`,
		kind, findingID, string(raw))
	if err != nil {
		return 0, err
	}
	if id, err = res.LastInsertId(); err != nil {
		return 0, err
	}
	if id <= 0 {
		return 0, fmt.Errorf("写入通知事件未返回合法自增主键：%d", id)
	}
	return id, nil
}

// FanOutPendingEvents 把尚未分派的漏洞事件按当前启用的渠道展开成投递任务，
// 返回本轮处理的事件数与新建的投递数。
//
// 整轮操作在一个事务里：事件用 SELECT … FOR UPDATE 有界领取，多个进程同时跑
// 也各自拿到不同的行（拿不到锁的一方会等上一轮提交，然后看见这些行已被标记
// fanned_out、自然落空）。刻意不用 SKIP LOCKED：投递队列已整体改成「有界锁定 +
// CAS 推进」的两阶段租约状态机（见 db/notification_delivery.go），事件分派沿用
// 同一种有界锁定手法，不再在同一套代码里混用两种并发原语。
//
// 过滤匹配刻意放在 Go 侧而非 SQL：渠道的过滤条件是一组可选字段的 JSON，
// 用 SQL 表达六种组合的匹配会让查询难以维护，而渠道数量是「人手配的几条」，
// 全量加载后在内存里逐条比对更快也更好测。
//
// 未命中任何渠道的事件同样会被标记 fanned_out ——否则它会永远留在待分派集合里，
// 每个 tick 被重扫一遍。
//
// 生产语义 = **全局 FIFO 有界领取**：本函数不认事件归属，队首是谁就分派谁。
// 需要在共享库上"只分派自己那一条"的调用方（db 包内的竞态回归测试）请使用
// fanOutOwnEventForChannels —— 它与本函数共用同一个核心，只是把领取与匹配
// 钉死在自有行上（复审 RR-I02）。
func (d *DB) FanOutPendingEvents(ctx context.Context, limit int) (eventCount, deliveryCount int, err error) {
	return d.fanOutPendingEvents(ctx, limit, fanOutOwnership{})
}

// fanOutOwnership 把一次 fan-out 允许触碰的行钉死在调用方**自有**的那一批。
//
// 零值 = 生产语义（全局 FIFO 有界领取 + 全量启用渠道匹配），一个字节都不改变。
type fanOutOwnership struct {
	// eventID > 0 ⇒ 领取语句里追加 `AND id = ?`：这条 SELECT 在 SQL 层面就**不可能**
	// 返回别人的事件行，因此后面所有 UPDATE fanned_out / INSERT delivery 都只可能
	// 落在自有事件上。这是结构性的，不依赖"跑前先 COUNT 一次队列是干净的"。
	eventID int64
	// allowedChannelIDs 非空 ⇒ 匹配集合先被裁剪成这批渠道，INSERT 之前再逐个复核。
	// 挡住的是另一条侵害路径：共享库里某个既有渠道在本用例运行期间把过滤条件改宽
	// （on_status_change 打开 + 不限 vuln-class），自有事件仍会被投进别人家。
	allowedChannelIDs []int64
}

// errFanOutClaimNotOwned 表示受控领取读到了不属于自有的行。
//
// 走到这里只有两种可能：SQL 绑定被后人改坏，或数据库行为异常。无论哪种，本函数
// **在第一条 DML 之前**返回错误，外层 defer Rollback 把行锁原样归还 —— 别人的事件
// 一个字节都不会被改写。
var errFanOutClaimNotOwned = errors.New("fan-out 受控领取读到了非自有事件")

// fanOutOwnEventForChannels 是**仅供 db 包内测试**使用的受控分派入口
// （复审 RR-I02：跑前 COUNT、随机哨兵、事后断言都不是隔离权）。
//
// 与生产 FanOutPendingEvents 的区别只有"归属绑定"这一条，核心处理逻辑完全共用：
//  1. 领取语句绑定 `AND id = eventID` ⇒ 领取集合在 SQL 层面就等于 {eventID}；
//  2. 渠道集合裁剪到 channelIDs ⇒ 匹配与 INSERT 都不可能落到别人的渠道；
//  3. 领取之后、任何 DML 之前再复核一次归属，读到非自有行立即带错误返回并回滚。
//
// 于是"替别人分派事件""把事件投进别人的渠道"这两条侵害路径都在**写入之前**
// 就不成立，与归档侧 claimOwnArchiveJob 的"先核归属、后发 UPDATE"同一强度。
func (d *DB) fanOutOwnEventForChannels(ctx context.Context, eventID int64, channelIDs []int64) (eventCount, deliveryCount int, err error) {
	if eventID <= 0 {
		return 0, 0, fmt.Errorf("受控 fan-out 需要正数事件 id，实际 %d", eventID)
	}
	allowed := make([]int64, 0, len(channelIDs))
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		if id <= 0 {
			return 0, 0, fmt.Errorf("受控 fan-out 的渠道 id 必须为正数，实际 %d", id)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		allowed = append(allowed, id)
	}
	if len(allowed) == 0 {
		return 0, 0, fmt.Errorf("受控 fan-out 至少需要一个自有渠道")
	}
	return d.fanOutPendingEvents(ctx, 1, fanOutOwnership{eventID: eventID, allowedChannelIDs: allowed})
}

// fanOutPendingEvents 是分派核心；ownership 零值时与生产 FanOutPendingEvents 完全同语义。
func (d *DB) fanOutPendingEvents(ctx context.Context, limit int, ownership fanOutOwnership) (eventCount, deliveryCount int, err error) {
	if limit <= 0 {
		limit = 200
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后是 no-op

	channels, err := listEnabledNotificationChannelsTx(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	// 受控模式：把"本轮可能收到投递的渠道"裁剪到自有集合（见 fanOutOwnership）。
	// 裁剪在屏障**之前**，钩子里看到的渠道集合与真正要写的一致。
	if owned := ownership.allowedChannelIDs; len(owned) > 0 {
		allow := make(map[int64]struct{}, len(owned))
		for _, id := range owned {
			allow[id] = struct{}{}
		}
		kept := channels[:0]
		for _, ch := range channels {
			if _, ok := allow[ch.ID]; ok {
				kept = append(kept, ch)
			}
		}
		channels = kept
	}
	hookChannelIDs := make([]int64, 0, len(channels))
	for _, ch := range channels {
		hookChannelIDs = append(hookChannelIDs, ch.ID)
	}
	notifyFanOutDeliveryHook("channels_read", hookChannelIDs)
	// 锁序说明（R33 审查 Minor-1，刻意保留的偏差）：本事务先锁 notification_events
	// (TableOrder 37)、后锁 notification_channels (35)，与全局升序相反的**字面**顺序。
	// 之所以不成环：
	//   - 反向路径只有"锁 channels 再锁 events"这一种；全仓没有这样的写者 ——
	//     UpdateNotificationChannel / DeleteNotificationChannel 只碰 channels 与其
	//     deliveries(36)；事件侧其余写点只有两条 INSERT 与一条标记 fanned_out 的
	//     UPDATE，都不预锁渠道；
	//   - 投递行的真正串行点是"插入前按渠道行锁重验"（lockNotificationChannelsForInsert），
	//     渠道的**内容**快照读在前、加锁重验在后，两者之间的偏差由 35 行锁 + 存活复核闭合。
	// 若要改成严格升序，就必须在领取事件前锁全部启用渠道；那会把"少数几条渠道"的
	// 写锁窗口拉长到整个事件领取 + 匹配阶段，收益是零（无环），代价是渠道管理操作
	// 被 fan-out 阻塞。因此保留字面偏差，用注释固化论证。
	claimQuery := `SELECT id, kind, finding_id, snapshot FROM notification_events
WHERE NOT fanned_out`
	claimArgs := make([]any, 0, 2)
	if ownership.eventID > 0 {
		claimQuery += ` AND id = ?`
		claimArgs = append(claimArgs, ownership.eventID)
	}
	claimQuery += ` ORDER BY id LIMIT ? FOR UPDATE`
	claimArgs = append(claimArgs, limit)
	rows, err := tx.QueryContext(ctx, claimQuery, claimArgs...)
	if err != nil {
		return 0, 0, err
	}
	var (
		events      []NotificationEvent
		parsedSnaps []notify.Snapshot
	)
	for rows.Next() {
		var ev NotificationEvent
		if err := rows.Scan(&ev.ID, &ev.Kind, &ev.FindingID, &ev.Snapshot); err != nil {
			rows.Close()
			return 0, 0, err
		}
		var snap notify.Snapshot
		// 快照是我们自己写的，理论上必定可解析；解析失败不阻断投递流程，
		// 但这条事件会因字段全空而被所有带过滤条件的渠道跳过——宁可少推一条
		// 也不让一个坏行卡死整个队列。
		_ = json.Unmarshal(ev.Snapshot, &snap)
		// kind 以行内值为准：快照里那份是渲染用的副本，可能被旧版本写过。
		snap.Kind = ev.Kind
		events = append(events, ev)
		parsedSnaps = append(parsedSnaps, snap)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	// 受控领取的**事务内归属复核**（复审 RR-I02）：读到非自有事件就在第一条 DML
	// 之前上抛，由 defer Rollback 归还行锁。这里没有"先标记再补救"的分支。
	if ownership.eventID > 0 {
		for _, ev := range events {
			if ev.ID != ownership.eventID {
				return 0, 0, fmt.Errorf("%w: id=%d，本调用只拥有 id=%d", errFanOutClaimNotOwned, ev.ID, ownership.eventID)
			}
		}
	}
	if len(events) == 0 {
		return 0, 0, tx.Commit()
	}

	type pending struct {
		eventID   int64
		channelID int64
	}
	var toInsert []pending
	for i, snap := range parsedSnaps {
		for _, ch := range channels {
			if !notify.Match(notify.ParseFilter(ch.Filter), snap) {
				continue
			}
			toInsert = append(toInsert, pending{eventID: events[i].ID, channelID: ch.ID})
		}
	}
	// 投递行只能指向"此刻仍然存活"的渠道，因此 INSERT 之前按 id 升序锁住本轮要用的渠道行
	// 并持锁重验（审查 I-02）：上面那份渠道列表是加锁之前的普通读，期间渠道可能被删除
	// （DeleteNotificationChannel）或被停用（SetNotificationChannelEnabled），
	// 两个写者都按同一把渠道行锁与本处排队。重验不通过的渠道整条丢弃，
	// 绝不留下指向已删 / 已停渠道的孤儿投递。
	//
	// 锁序：本事务此刻已持有事件行锁，再按 id 升序取渠道行锁，最后才写投递行；
	// 渠道侧的删除/启停都是「渠道行 → 投递行」，且从不碰事件行，两边不会反向。
	if len(toInsert) > 0 {
		refs := make([]int64, 0, len(toInsert))
		seen := make(map[int64]struct{}, len(channels))
		for _, p := range toInsert {
			if _, ok := seen[p.channelID]; !ok {
				seen[p.channelID] = struct{}{}
				refs = append(refs, p.channelID)
			}
		}
		sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
		alive, err := lockNotificationChannelsForInsert(ctx, tx, refs)
		if err != nil {
			return 0, 0, err
		}
		kept := toInsert[:0]
		for _, p := range toInsert {
			if alive[p.channelID] {
				kept = append(kept, p)
			}
		}
		toInsert = kept
	}

	// 受控模式的第二道复核（复审 RR-I02）：INSERT 之前逐条核对渠道归属，越界就在
	// **第一条投递写入之前**上抛，由 defer Rollback 归还全部行锁。
	if owned := ownership.allowedChannelIDs; len(owned) > 0 {
		allow := make(map[int64]struct{}, len(owned))
		for _, id := range owned {
			allow[id] = struct{}{}
		}
		for _, p := range toInsert {
			if _, ok := allow[p.channelID]; !ok {
				return 0, 0, fmt.Errorf("fan-out 受控渠道集合越界：事件 %d 命中非自有渠道 %d", p.eventID, p.channelID)
			}
		}
	}

	if len(toInsert) > 0 {
		var (
			vals []string
			args []any
		)
		for _, p := range toInsert {
			vals = append(vals, "(?, ?, 'pending', '')")
			args = append(args, p.eventID, p.channelID)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO notification_deliveries(event_id, channel_id, state, last_error) VALUES `+strings.Join(vals, ","), args...); err != nil {
			return 0, 0, err
		}
	}

	// 标记本轮事件已分派。未命中任何渠道的事件也一起标记（见函数注释）。
	markArgs := make([]any, 0, len(events))
	marks := make([]string, 0, len(events))
	for _, ev := range events {
		markArgs = append(markArgs, ev.ID)
		marks = append(marks, "?")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_events SET fanned_out = true WHERE id IN (`+strings.Join(marks, ", ")+`)`, markArgs...); err != nil {
		return 0, 0, err
	}
	return len(events), len(toInsert), tx.Commit()
}

// listEnabledNotificationChannelsTx 在事务里取启用中的渠道。数量很少，
// 不做分页也不加缓存——缓存会引入「改了配置何时生效」这个额外的时序问题。
func listEnabledNotificationChannelsTx(ctx context.Context, tx *sql.Tx) ([]*NotificationChannel, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, name, kind, config, mode, filter, rate_per_min
FROM notification_channels WHERE enabled ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*NotificationChannel{}
	for rows.Next() {
		var c NotificationChannel
		if err := rows.Scan(&c.ID, &c.Name, &c.Kind, &c.Config, &c.Mode, &c.Filter, &c.RatePerMin); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

// NotificationAssetNames 把资产 id 解析成简短展示名，供推送消息使用。
//
// 返回顺序与入参一致、长度可能小于入参（不存在的 id 被跳过）。保持入参顺序是
// 为了让同一条漏洞的消息在多次投递里资产顺序稳定——否则重试后收到的消息里
// 资产次序变了，会被误读成「资产变了」。
func (d *DB) NotificationAssetNames(ctx context.Context, ids []int64) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph, args := markList(ids)
	rows, err := d.QueryContext(ctx, `SELECT id, type, domain, ip, url, app_name, bundle_id FROM assets WHERE id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	labels := map[int64]string{}
	for rows.Next() {
		var (
			id                int64
			typ               string
			domain, ip, url   sql.NullString
			appName, bundleID sql.NullString
		)
		if err := rows.Scan(&id, &typ, &domain, &ip, &url, &appName, &bundleID); err != nil {
			return nil, err
		}
		labels[id] = assetDisplayName(typ, domain.String, ip.String, url.String, appName.String, bundleID.String)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	seen := map[int64]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if label, ok := labels[id]; ok && label != "" {
			out = append(out, label)
		}
	}
	return out, nil
}

// assetDisplayName 按资产类型挑选最具辨识度的标识。
// 兜底返回空串，由调用方决定怎么呈现「名字取不到的资产」——本函数不臆造占位符，
// 否则「资产#42」这种噪音会混进推送消息里，读者还以为是真实域名。
func assetDisplayName(typ, domain, ip, url, appName, bundleID string) string {
	pick := func(vals ...string) string {
		for _, v := range vals {
			if strings.TrimSpace(v) != "" {
				return v
			}
		}
		return ""
	}
	switch typ {
	case "root_domain", "subdomain":
		return domain
	case "ip":
		return ip
	case "app":
		return pick(appName, bundleID)
	case "service", "endpoint":
		return pick(url, domain, ip)
	default:
		return pick(domain, ip, url, appName)
	}
}

// SetFindingStatusWithNotify 更新漏洞处置状态，并在同一事务里登记一条状态变更
// 推送事件。
//
// 返回 from=变更前的状态；found=漏洞是否存在；notified=事件是否登记成功。
//
// 三条刻意的行为：
//   - 状态未实际变化时不登记事件。前端抽屉重复提交同一个值、或自动化脚本
//     幂等重放，都不该产出推送噪音。
//   - 漏洞不存在时返回 found=false 且不做任何写入，由调用方翻译成 404。
//   - 事件登记失败不影响状态更新（见 RecordNotificationEventTx 的保存点说明），
//     所以 notified=false 时状态已经改成功了，调用方不应因此报错。
func (d *DB) SetFindingStatusWithNotify(ctx context.Context, id int64, status string) (from string, found bool, notified bool, err error) {
	err = d.WithEvidenceTx(ctx, func(tx *sql.Tx) error {
		var txErr error
		from, found, _, notified, txErr = SetFindingStatusTx(ctx, tx, id, status)
		return txErr
	})
	return from, found, notified, err
}

// SetFindingStatusTx 在**调用方的事务**内更新漏洞状态并登记状态变更推送事件。
//
// 抽成事务级函数是为了让所有改状态的路径共用同一套语义——此前只有
// patchFinding 走带通知的版本，而**复测结论为「已修复」时**（finding_retests
// 里那条 `UPDATE findings SET status=...`）是直接写库的，于是配了
// `on_status_change` 的渠道对这类状态流转完全收不到推送：界面上状态悄悄变了，
// 运维要到打开平台才发现。
//
// 返回 from=变更前状态、found=漏洞是否存在、changed=状态是否真的变了、
// notified=事件是否登记成功（登记失败不影响状态更新，见 RecordNotificationEventTx）。
func SetFindingStatusTx(ctx context.Context, tx *sql.Tx, id int64, status string) (from string, found bool, changed bool, notified bool, err error) {
	var (
		vulnclass, name, severity, summary string
		taskID                             sql.NullInt64
		assetIDs                           []byte
	)
	scanErr := tx.QueryRowContext(ctx, `SELECT vulnclass, name, severity, summary, task_id, asset_ids, status
FROM findings WHERE id = ? FOR UPDATE`, id).
		Scan(&vulnclass, &name, &severity, &summary, &taskID, &assetIDs, &from)
	if scanErr == sql.ErrNoRows {
		return "", false, false, false, nil
	}
	if scanErr != nil {
		return "", false, false, false, scanErr
	}
	found = true
	if from == status {
		// 状态没有真的变化就不登记事件：重复提交同一个值、幂等重放都不该
		// 产生推送噪音。
		return from, true, false, false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE findings SET status = ? WHERE id = ?`, status, id); err != nil {
		return from, true, false, false, err
	}
	var assets []int64
	_ = json.Unmarshal(assetIDs, &assets)
	notified = RecordNotificationEventTx(ctx, tx, notify.EventFindingStatusChanged, id, notify.Snapshot{
		Kind:       notify.EventFindingStatusChanged,
		FindingID:  id,
		TaskID:     taskID.Int64,
		VulnClass:  vulnclass,
		Name:       name,
		Severity:   severity,
		Summary:    summary,
		AssetIDs:   assets,
		FromStatus: from,
		ToStatus:   status,
	})
	return from, true, true, notified, nil
}

// NotificationStats 是通知页顶部的概览计数。
type NotificationStats struct {
	Channels     int   `json:"channels"`
	ChannelsOn   int   `json:"channels_on"`
	Pending      int   `json:"pending"`
	Failed       int   `json:"failed"`
	SentToday    int   `json:"sent_today"`
	BacklogAgeMS int64 `json:"backlog_age_ms"` // 最老的待发投递距今毫秒数
}

// NotificationStatsSnapshot 汇总通知系统的健康度。
// BacklogAgeMS 是「推送是不是卡住了」最直接的指标——比 pending 计数有用得多，
// 因为积压 3 条和积压 3 条的差别可以是从 3 秒到 3 小时。
func (d *DB) NotificationStatsSnapshot(ctx context.Context) (*NotificationStats, error) {
	var s NotificationStats
	if err := d.QueryRowContext(ctx, `SELECT
    (SELECT count(*) FROM notification_channels),
    (SELECT count(*) FROM notification_channels WHERE enabled),
    (SELECT count(*) FROM notification_deliveries WHERE state IN (?, ?)),
    (SELECT count(*) FROM notification_deliveries WHERE state = ?),
    (SELECT count(*) FROM notification_deliveries WHERE state = ? AND sent_at >= CURDATE()),
    COALESCE((SELECT TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(3)) DIV 1000
              FROM notification_deliveries WHERE state = ?), 0)`,
		NotifyStatePending, NotifyStateSending, NotifyStateFailed, NotifyStateSent, NotifyStatePending).
		Scan(&s.Channels, &s.ChannelsOn, &s.Pending, &s.Failed, &s.SentToday, &s.BacklogAgeMS); err != nil {
		return nil, err
	}
	return &s, nil
}
