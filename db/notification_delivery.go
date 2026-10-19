package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 本文件是投递任务的领取与状态流转。
//
// 领取用「两阶段 CAS 租约」而不是「长事务 + SKIP LOCKED」：
//
//	阶段 1：事务内有界锁定候选行（SELECT … FOR UPDATE），只读 id/channel_id/attempts；
//	阶段 2：把状态机原子推进为 sending，并把 Go 侧算好的绝对截止时间 assignedDeadline
//	        写进 next_attempt_at —— 这一列同时是「下次可领取时间」与「租约到期时间」。
//	读取完整行后立刻提交，网络投递发生在**提交之后**：投递期间不持有数据库锁
//	（渠道 HTTP 客户端超时 15 秒，占着行锁不放会拖垮同库的其它写操作）。
//
// 为什么不用 SKIP LOCKED：它只保证「不撞车」，不保证「谁在什么时候拥有这一行」。
// 租约把后者变成可验证的事实——next_attempt_at 就是这次领取的**代次令牌**，
// 于是所有回填都必须带着它（见下面的 Fencing 契约）。
//
// 崩溃自愈是**可预期**的：进程若在投递途中挂掉，行会停在 sending；租约到期后
// next_attempt_at 落入过去，下一轮领取会把同一行重新捞起来（阶段 1 的第二支谓词）。
// 重试计数在领取时就已 +1，所以崩溃不会造成无限重试——MaxNotifyAttempts 次机会
// 用完后落入 failed 等人工处理。
//
// **代次 Fencing 回填契约（闭环 F05）**：MarkDeliveriesSent / RescheduleDeliveries /
// DeferDeliveries / FailDeliveries 四个回填必须携带领取时拿到的那份租约截止时间，
// 且 UPDATE 的 WHERE 必须含 `state = 'sending' AND next_attempt_at = ?`。
// 若本行租约已超时被下一个 worker 抢走，它的 next_attempt_at 已被换成**新的**
// 截止时间，旧 worker 的迟到 ACK 匹配行数为 0 —— 迟到 ACK 被丢弃，新租约分毫不动。
// 回填返回的影响行数就是这件事的可观测出口：0 行意味着「我已经不是这次投递的主人」。

// MaxNotifyAttempts 是一条投递的最大尝试次数（含首次）。
// 定义在这里而非投递引擎里：它是状态机自身的策略，引擎只是执行者。
const MaxNotifyAttempts = 3

// MaxDigestBatchSize 是单个汇总批次一次最多合并多少条投递。
//
// 存在的理由是资源：一个汇总周期内如果扫出几万个漏洞（完全可能——一次全量扫描
// 就能做到），不设上界的话领取会把全部行读进内存、渲染成一条超长消息，
// 然后被渠道的长度上限截掉大半——既浪费内存，又**静默丢失**被截掉的那些漏洞。
// 设上界后，超出的部分留在库里成为下一个批次，下个周期自然发出去，不会丢。
//
// 取 500 的依据：它是渲染成消息后在企微 4096 字节上限内还"有内容可读"的量级；
// 再大也只是让截断发生在更靠后的位置而已。
const MaxDigestBatchSize = 500

// NotificationDelivery 是一条投递任务，含渲染所需的渠道配置与事件快照。
type NotificationDelivery struct {
	ID            int64           `json:"id"`
	EventID       int64           `json:"event_id"`
	ChannelID     int64           `json:"channel_id"`
	State         string          `json:"state"`
	Attempts      int             `json:"attempts"`
	NextAttemptAt time.Time       `json:"next_attempt_at"`
	LastError     string          `json:"last_error"`
	BatchID       *int64          `json:"batch_id,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	SentAt        *time.Time      `json:"sent_at,omitempty"`
	Snapshot      json.RawMessage `json:"snapshot,omitempty"`
	// 联合加载的渲染上下文，不进 JSON（由 server 层组装 DTO）。
	Channel *NotificationChannel `json:"-"`
	// FindingID/EventKind 从事件带出，供历史列表直接跳转漏洞详情。
	FindingID int64  `json:"finding_id,string"`
	EventKind string `json:"event_kind"`
	// ChannelName/ChannelKind 是列表展示用的冗余字段，省掉前端二次查询。
	ChannelName string `json:"channel_name"`
	ChannelKind string `json:"channel_kind"`
}

// LeaseToken 返回本次领取分配到的租约截止时间，即四类回填的代次 fencing 令牌。
// 它的值就是 NextAttemptAt——阶段 2 写进 next_attempt_at 的那个时刻。单独起名字
// 是为了让投递引擎里的传递路径可读，并与「下次可领取时间」这个字段语义区分开。
func (dl *NotificationDelivery) LeaseToken() time.Time { return dl.NextAttemptAt }

const notificationDeliveryCols = `d.id, d.event_id, d.channel_id, d.state, d.attempts, d.next_attempt_at,
       d.last_error, d.batch_id, d.created_at, d.sent_at`

// joinedDeliveryQuery 是投递行的统一读取形状：投递 + 事件快照 + 渠道配置。
// 渲染一条消息三者缺一不可，分开查会写出三次往返。
const joinedDeliveryQuery = `SELECT ` + notificationDeliveryCols + `,
       e.snapshot, e.kind, e.finding_id,
       c.id, c.name, c.kind, c.enabled, c.config, c.mode, c.filter, c.rate_per_min
FROM notification_deliveries d
JOIN notification_events e ON e.id = d.event_id
JOIN notification_channels c ON c.id = d.channel_id`

func scanNotificationDelivery(sc interface{ Scan(...any) error }) (*NotificationDelivery, error) {
	var (
		dl        NotificationDelivery
		lastErr   sql.NullString
		batchID   sql.NullInt64
		sentAt    sql.NullTime
		snapshot  []byte
		eventKind string
		channel   NotificationChannel
		chEnabled bool
	)
	if err := sc.Scan(&dl.ID, &dl.EventID, &dl.ChannelID, &dl.State, &dl.Attempts, &dl.NextAttemptAt,
		&lastErr, &batchID, &dl.CreatedAt, &sentAt,
		&snapshot, &eventKind, &dl.FindingID,
		&channel.ID, &channel.Name, &channel.Kind, &chEnabled, &channel.Config, &channel.Mode, &channel.Filter, &channel.RatePerMin); err != nil {
		return nil, err
	}
	dl.LastError = lastErr.String
	if batchID.Valid {
		dl.BatchID = &batchID.Int64
	}
	if sentAt.Valid {
		dl.SentAt = &sentAt.Time
	}
	dl.Snapshot = json.RawMessage(snapshot)
	dl.EventKind = eventKind
	dl.ChannelName = channel.Name
	dl.ChannelKind = channel.Kind
	channel.Enabled = &chEnabled
	dl.Channel = &channel
	return &dl, nil
}

// 投递状态机的三条核心语句。表名与列名严格对齐真实 Catalog（M33）：
// 真实表是 notification_deliveries，租约与调度时间列是 next_attempt_at，
// 不存在 PG 时代的 scheduled_at。
const (
	// claimCandidateQuery 是阶段 1：单表、有界、按 id 升序的候选锁定。
	// 两支谓词的差别是刻意的：
	//   - pending 用 `<=`：到点即可领；
	//   - sending 用 `<`：只捞**严格已过期**的租约（崩溃自愈），不抢仍在租约里的行。
	// NOW(3) 与 DATETIME(3) 同精度，比较两侧不会引入亚毫秒偏差。
	claimCandidateQuery = `SELECT id, channel_id, attempts FROM notification_deliveries
WHERE channel_id = ?
  AND ((state = '` + NotifyStatePending + `' AND next_attempt_at <= NOW(3)) OR (state = '` + NotifyStateSending + `' AND next_attempt_at < NOW(3)))
ORDER BY id ASC
LIMIT ?
FOR UPDATE`

	// claimAssignQuery 是阶段 2：候选原子置 sending、递增尝试次数，并写入本次分配的
	// 租约截止时间（第 1 个实参；%s 展开成 id 的 `?` 列表）。
	// 该截止时间随后被四类回填当代次令牌逐位比对，所以它必须由 Go 侧算好且已对齐
	// 到毫秒，见 deliveryClockAt。
	claimAssignQuery = `UPDATE notification_deliveries
SET state = '` + NotifyStateSending + `', next_attempt_at = ?, attempts = attempts + 1
WHERE id IN (%s)`

	// deliveryLeaseFence 是四类回填共用的代次 fencing 谓词：状态必须仍是 sending，
	// 且租约截止时间必须与令牌逐位相等。
	deliveryLeaseFence = "state = '" + NotifyStateSending + `' AND next_attempt_at = ?`
)

// deliveryClockAt 把「相对时长」换算成可直接写进 DATETIME(3) 列的绝对 UTC 时刻。
//
// 两处刻意的处理：
//   - **UTC**：连接池强制会话时区 +00:00（见 db/tidb），Go 侧同样按 UTC 传，
//     两侧不会因时区差把租约算成八小时后或八小时前；
//   - **截到毫秒**：next_attempt_at 是 DATETIME(3)，落库会舍掉亚毫秒。租约截止时间
//     同时是回填的 fencing 令牌，令牌必须在「写入」与「比较」两侧逐位相等；
//     若把带纳秒噪声的原值留在内存里当令牌，正确持有者也会被自己的舍入噪声判成过期。
func deliveryClockAt(d time.Duration) time.Time {
	return time.Now().UTC().Add(d).Truncate(time.Millisecond)
}

// ClaimRealtimeDeliveries 领取某渠道一批到期的实时投递，最多 limit 条。
//
// 刻意按**单个渠道**领取而不是「全局领一批再挑着发」：限流闸在投递引擎里按渠道
// 维护，只有先知道这个渠道这一轮还能发几条、再去领同样多的行，限流才不会消耗
// 重试次数。若反过来先领后弃，被限流挡下的行已经被计过一次 attempts，
// 3 次预算会被纯粹的等待耗光，最后落进 failed。
//
// 返回的每条投递的 NextAttemptAt 就是本次分配的租约截止时间；投递引擎回填状态时
// 必须把它原样带回来（那四胞胎参数的由来），否则自己的回填会被 fencing 判成迟到。
func (d *DB) ClaimRealtimeDeliveries(ctx context.Context, channelID int64, limit int, lease time.Duration) ([]*NotificationDelivery, error) {
	if limit <= 0 {
		return nil, nil
	}
	return d.claimDeliveries(ctx, channelID, limit, lease, NotifyModeRealtime, nil)
}

// DigestBatchDue 报告该渠道是否已攒够一个到期批次：存在待发投递，且**最老的那条**
// 年龄已达到汇总周期。
//
// 判定依据是最老投递的年龄而非墙上时钟：这样刚建好的渠道不会因为对齐到整点而
// 立刻吐出一条只有一条的「汇总」，积压很久的批次也不会再白等一轮。
//
// 与 ClaimDigestBatch 分开是因为语义不同：本函数只回答「该不该发」，
// 而领取要拿走该渠道**全部**待发行（包括尚未满年龄的那些）——否则一个周期
// 会被拆成多条消息，汇总就失去意义了。
func (d *DB) DigestBatchDue(ctx context.Context, channelID int64, minAge time.Duration) (bool, error) {
	var due bool
	err := d.QueryRowContext(ctx, `SELECT EXISTS (
  SELECT 1 FROM notification_deliveries d
  WHERE d.channel_id = ? AND d.state IN ('`+NotifyStatePending+`','`+NotifyStateSending+`')
  GROUP BY d.channel_id
  HAVING TIMESTAMPDIFF(SECOND, MIN(d.created_at), NOW(3)) >= ?
)`, channelID, int64(minAge.Seconds())).Scan(&due)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return due, err
}

// ClaimDigestBatch 领取某渠道当前到期的待发投递，作为一个汇总批次，
// 单批最多 MaxDigestBatchSize 条。
//
// 同批次的所有投递共享 batch_id，用集合里的最小 id 作批次号（稳定、可读、
// 无需额外序列）。重试时用 COALESCE 保留原批次号，使「这批 N 条是一起发的」
// 在多次重试后依然成立。
//
// 按 id 升序取前 N 条而非随机取：最早产生的投递最先发出去，积压时不会出现
// 「新漏洞先发、老漏洞永远排在后面」的饥饿。
func (d *DB) ClaimDigestBatch(ctx context.Context, channelID int64, limit int, lease time.Duration) ([]*NotificationDelivery, error) {
	if limit <= 0 {
		return nil, nil
	}
	// limit 是**内存上界**，调用方传 MaxDigestBatchSize；这里再夹一道，
	// 防止调用方传进一个更大的值。
	//
	// 刻意不接受「限流额度」充当批次大小：限流的单位是消息条数——一个批次只发
	// 一条消息、消耗一个令牌，由 server 层的 takeTokens 扣除——与「一批装几条
	// 漏洞」是两个不同的量纲。曾经为了让 rate_per_min 对 digest 生效而把每轮
	// 请求预算传进来当批次大小，结果 rate=20/min 的渠道每批只装 1 条漏洞，
	// digest 退化成带汇总文案的实时推送。要改限流请改 takeTokens 的 want，
	// 不要动这里。
	if limit > MaxDigestBatchSize {
		limit = MaxDigestBatchSize
	}
	out, err := d.claimDeliveries(ctx, channelID, limit, lease, "", func(tx *sql.Tx, ids []int64) error {
		batchID := ids[0]
		for _, id := range ids {
			if id < batchID {
				batchID = id
			}
		}
		ph, idArgs := markList(ids)
		_, err := tx.ExecContext(ctx, `UPDATE notification_deliveries SET batch_id = COALESCE(batch_id, ?)
WHERE id IN (`+ph+`)`, append([]any{batchID}, idArgs...)...)
		return err
	})
	return out, err
}

// claimDeliveries 是两阶段 CAS 抢占的公共实现：
//
//	渠道闸门（启用 + 模式）→ 事务 → 阶段 1 有界候选锁定 → 阶段 2 原子置 sending
//	并写下租约截止时间 → 可选的批次盖章 → 回读完整行 → 提交。
//
// mode 传空串表示「不限模式，只要渠道启用」（汇总领取）；realtime 领取必须精确
// 匹配 realtime。渠道闸门刻意**放在事务外**：它只是一次存在性/配置读取，
// 放进事务里会让阶段 1 的行锁多跨一次往返，白白延长锁持有时间。
// 停用渠道与领取并发时，这道闸门就是「已停用渠道的存量投递一条都不会被发出去」
// 的保证（停用操作本身已把存量置为 skipped，这里是第二道闸）。
//
// postClaim 是可选的附加步骤（汇总批次用它写 batch_id），仍在同一事务、
// 同一批行锁之内。
func (d *DB) claimDeliveries(
	ctx context.Context,
	channelID int64,
	limit int,
	lease time.Duration,
	mode string,
	postClaim func(*sql.Tx, []int64) error,
) ([]*NotificationDelivery, error) {
	claimable, err := d.deliveryChannelClaimable(ctx, channelID, mode)
	if err != nil || !claimable {
		return nil, err
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // 提交成功后是 no-op

	ids, err := selectClaimCandidates(ctx, tx, channelID, limit)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	// 阶段 2：assignedDeadline 即本次领取的代次，提交后随投递结果一起被带回。
	assignedDeadline := deliveryClockAt(lease)
	ph, idArgs := markList(ids)
	res, err := tx.ExecContext(ctx, fmt.Sprintf(claimAssignQuery, ph), append([]any{assignedDeadline}, idArgs...)...)
	if err != nil {
		return nil, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return nil, err
	} else if n != int64(len(ids)) {
		// 行锁在手，候选行只可能已经被删掉（渠道级联删除）。这不是可以忽略的偏差：
		// 少更新的那几行会被永久遗留在 sending，fail-closed 让下一轮从头重领。
		return nil, fmt.Errorf("CAS 抢占投递：候选 %d 行，实际置 sending %d 行", len(ids), n)
	}
	if postClaim != nil {
		if err := postClaim(tx, ids); err != nil {
			return nil, err
		}
	}
	out, err := loadDeliveriesTx(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

// deliveryChannelClaimable 报告该渠道现在能不能领投递：必须存在、启用，
// 且（给定 mode 时）模式匹配。渠道不存在按「不可领」处理而不是报错——
// 渠道被删掉与被停用对投递引擎是同一件事。
func (d *DB) deliveryChannelClaimable(ctx context.Context, channelID int64, mode string) (bool, error) {
	var (
		enabled bool
		current string
	)
	err := d.QueryRowContext(ctx, `SELECT enabled, mode FROM notification_channels WHERE id = ?`, channelID).
		Scan(&enabled, &current)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return enabled && (mode == "" || current == mode), nil
}

// selectClaimCandidates 执行阶段 1，返回按 id 升序的候选主键。
//
// 三个阶段列都扫出来是刻意的：`SELECT id, channel_id, attempts` 是契约语句
// （attempts 让排查「这条投递为什么被判失败」时不必回查），而 channel_id
// 回读后立刻比对一次——阶段 2 会拿这批 id 直接改状态，捞到别的渠道的行
// 是一次必须当场炸出来的事故，不是可以留到日志里的异常。
func selectClaimCandidates(ctx context.Context, tx *sql.Tx, channelID int64, limit int) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, claimCandidateQuery, channelID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var (
			id         int64
			rowChannel int64
			attempts   int
		)
		if err := rows.Scan(&id, &rowChannel, &attempts); err != nil {
			return nil, err
		}
		if rowChannel != channelID {
			return nil, fmt.Errorf("CAS 抢占候选行 %d 属于渠道 %d，与请求渠道 %d 不符", id, rowChannel, channelID)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func loadDeliveriesTx(ctx context.Context, tx *sql.Tx, ids []int64) ([]*NotificationDelivery, error) {
	ph, args := markList(ids)
	rows, err := tx.QueryContext(ctx, joinedDeliveryQuery+` WHERE d.id IN (`+ph+`) ORDER BY d.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*NotificationDelivery{}
	for rows.Next() {
		dl, err := scanNotificationDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, dl)
	}
	return out, rows.Err()
}

// fencedDeliveryUpdate 是四类回填的唯一出口：任何推进投递状态机的写都必须带着
// 自己的租约令牌（leaseDeadline）过这道 fence。
//
// 影响行数原样返回，因为「0 行」是**正常且有信息量**的结果：它说明本行已经被
// 下一个 worker 领走，本次回填是一次迟到的 ACK，应当丢弃而不是把新租约覆盖掉。
func (d *DB) fencedDeliveryUpdate(ctx context.Context, setClause string, setArgs []any, ids []int64, leaseDeadline time.Time) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	ph, idArgs := markList(ids)
	query := "UPDATE notification_deliveries SET " + setClause + " WHERE " + deliveryLeaseFence + " AND id IN (" + ph + ")"
	args := make([]any, 0, len(setArgs)+1+len(idArgs))
	args = append(args, setArgs...)
	args = append(args, leaseDeadline.UTC().Truncate(time.Millisecond))
	args = append(args, idArgs...)
	res, err := d.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// MarkDeliveriesSent 把一批投递标记为已送达，返回真正被推进的行数。
//
// leaseDeadline 必须是这批投递**领取时拿到的那份**租约截止时间；带旧令牌调用
// 只会得到 0 行（本行已归别人），不会覆盖新租约。
func (d *DB) MarkDeliveriesSent(ctx context.Context, ids []int64, leaseDeadline time.Time) (int64, error) {
	return d.fencedDeliveryUpdate(ctx,
		"state = '"+NotifyStateSent+"', sent_at = NOW(3), last_error = ''",
		nil, ids, leaseDeadline)
}

// RescheduleDeliveries 把一批投递退回 pending 并推后重试时间，返回真正被推进的行数。
//
// 退回 pending 而不是引入新的中间状态，是为了让「还剩几次机会」只由一个地方
// 表达（MaxNotifyAttempts），避免状态机的分支随重试策略膨胀。
// attempts 保留领取时 +1 的那一次——重排就是失败重试。
func (d *DB) RescheduleDeliveries(ctx context.Context, ids []int64, leaseDeadline time.Time, delay time.Duration, errMsg string) (int64, error) {
	return d.fencedDeliveryUpdate(ctx,
		"state = '"+NotifyStatePending+"', next_attempt_at = ?, last_error = ?",
		[]any{deliveryClockAt(delay), truncateNotifyError(errMsg)}, ids, leaseDeadline)
}

// DeferDeliveries 把一批投递退回 pending、立即可再领，并**撤销领取时计的那一次尝试**，
// 返回真正被推进的行数。
//
// 用途只有一个：汇总消息按渠道长度上限分段发送时，没装进本条的条目要留到下一批。
// 那不是失败，所以不该消耗重试预算——领取时 attempts 已经乐观地 +1 了，
// 这里必须减回去。否则一个 500 条的积压会按每段 20 条切成 25 段，
// 尾部条目在第 3 段就被 MaxNotifyAttempts 判成 failed，而它们从未出过任何错。
//
// GREATEST(...,0) 兜住「有人手工重发把 attempts 清零后又走到这里」的情况，
// 不让计数变成负数。
func (d *DB) DeferDeliveries(ctx context.Context, ids []int64, leaseDeadline time.Time, reason string) (int64, error) {
	return d.fencedDeliveryUpdate(ctx,
		"state = '"+NotifyStatePending+"', attempts = GREATEST(attempts - 1, 0), next_attempt_at = ?, last_error = ?",
		[]any{deliveryClockAt(0), truncateNotifyError(reason)}, ids, leaseDeadline)
}

// FailDeliveries 把一批投递标记为最终失败，等待人工在投递历史里重发，
// 返回真正被推进的行数。
func (d *DB) FailDeliveries(ctx context.Context, ids []int64, leaseDeadline time.Time, errMsg string) (int64, error) {
	return d.fencedDeliveryUpdate(ctx,
		"state = '"+NotifyStateFailed+"', last_error = ?",
		[]any{truncateNotifyError(errMsg)}, ids, leaseDeadline)
}

// RetryNotificationDelivery 手动重发一条投递：重置为 pending、清零重试计数、
// 立即到期。清计数是刻意的——人工点「重发」意味着前几次失败的原因已被处理，
// 再拿旧计数限制它没有道理。
//
// 这条 UPDATE 刻意**不带**租约 fencing：它作用于终态（failed/skipped），发起者是
// 拿着屏幕上的记录点按钮的人，不是某个持有租约的 worker。
func (d *DB) RetryNotificationDelivery(ctx context.Context, id int64) error {
	res, err := d.ExecContext(ctx, `UPDATE notification_deliveries
SET state = ?, attempts = 0, next_attempt_at = NOW(3), last_error = ''
WHERE id = ? AND state IN (?, ?)`, NotifyStatePending, id, NotifyStateFailed, NotifyStateSkipped)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("投递 %d 不存在或当前状态不允许重发", id)
	}
	return nil
}

// NotificationDeliveryFilter 是投递历史的查询条件。
type NotificationDeliveryFilter struct {
	ChannelID int64
	State     string
	EventKind string
}

func (f NotificationDeliveryFilter) where() (string, []any) {
	var conds []string
	var args []any
	if f.ChannelID > 0 {
		args = append(args, f.ChannelID)
		conds = append(conds, "d.channel_id = ?")
	}
	if f.State != "" {
		args = append(args, f.State)
		conds = append(conds, "d.state = ?")
	}
	if f.EventKind != "" {
		args = append(args, f.EventKind)
		conds = append(conds, "e.kind = ?")
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// ListNotificationDeliveries 分页返回投递历史，新的在前。
func (d *DB) ListNotificationDeliveries(ctx context.Context, f NotificationDeliveryFilter, page, pageSize int) ([]*NotificationDelivery, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 50
	}
	where, args := f.where()

	var total int
	if err := d.QueryRowContext(ctx, `SELECT count(*) FROM notification_deliveries d
JOIN notification_events e ON e.id = d.event_id`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	q := fmt.Sprintf("%s%s ORDER BY d.id DESC LIMIT ? OFFSET ?", joinedDeliveryQuery, where)
	rows, err := d.QueryContext(ctx, q, append(args, pageSize, (page-1)*pageSize)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []*NotificationDelivery{}
	for rows.Next() {
		dl, err := scanNotificationDelivery(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, dl)
	}
	return out, total, rows.Err()
}

// truncateNotifyError 把错误信息截到列可接受的长度。渠道返回的响应体可能很长
// （通用 Webhook 打到自建服务时尤甚），不截断会让历史列表的载荷膨胀。
func truncateNotifyError(msg string) string {
	const max = 500
	if len(msg) <= max {
		return msg
	}
	// 按字符边界回退，避免留下半个 UTF-8 字符让前端显示成乱码。
	cut := max
	for cut > 0 && !isUTF8Start(msg[cut]) {
		cut--
	}
	return msg[:cut] + "…"
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }

// markList 生成 IN (...) 用的 `?` 占位串与实参表（TiDB/MySQL 只认 `?`）。
// 例如 ids=[7,8] → "?, ?", [7,8]。
func markList(ids []int64) (string, []any) {
	ph := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		ph = append(ph, "?")
		args = append(args, id)
	}
	return strings.Join(ph, ", "), args
}
