package lockorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	// MaxLocatorCandidates 单次 locator 快照候选实体数量硬上限，防止无界查询拖垮事务
	MaxLocatorCandidates = 1000
)

var (
	ErrCandidateLimitExceeded = errors.New("lockorder: candidate limit exceeded in locator query")
	ErrNilDatabase            = errors.New("lockorder: nil database handle")
	ErrNilLocatorFunc         = errors.New("lockorder: nil locator function")
)

// ---------------------------------------------------------------------------
// READ ONLY 事务声明的兼容性降级（真实 TiDB Error 1235）
// ---------------------------------------------------------------------------
//
// 实测事实（TiDB v8.5.8 Community，192.168.27.79:6000，tidb_enable_noop_functions=OFF 默认值）：
//
//	db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
//	→ Error 1235 (42000): function READ ONLY has only noop implementation in tidb now,
//	  use tidb_enable_noop_functions to enable these functions
//
// 即 TiDB 把 START TRANSACTION READ ONLY 归入 noop 函数族，未显式开启
// tidb_enable_noop_functions 时**直接拒绝执行**。1235 不在 1062/1205/1213 可重试编号内，
// 因此降级前的实现在真实 TiDB 上会让每一次快照定位 100% 失败（fail closed，
// 且本包无法通过重试自愈）。本包不得反向依赖 db/tidb，也不得改动服务端设置，
// 只能在客户端做兼容性降级。
//
// 降级的安全性论证（为什么退回普通事务不损失任何保证）：
//  1. TiDB 自述 READ ONLY 在其上是 noop —— 声明本身不产生任何服务端效果；
//  2. 定位器契约（LocatorFunc 文档 + P04b）要求 fn 只执行**非加锁纯读**查询，
//     不写、不加 FOR UPDATE，因此"只读"由 SQL 形态保证，而非由事务声明保证；
//  3. 快照新鲜度来自"独立的新事务 + 独立连接"（本函数刻意在 BeginTx 阶段重新取连接，
//     避开外层事务的旧快照），与 READ ONLY 声明无关；
//  4. 降级只发生在 BeginTx **失败**之后：此时服务端根本没有开启任何事务，
//     不存在"半个事务"的状态需要清理，重开一次普通事务是幂等且无副作用的。
//
// 能力探测按 *sql.DB 缓存（locatorReadOnlyUnsupported）：同一实例只在首次命中时
// 吃一次 1235 往返，之后直接走普通事务，避免每次定位都多一次失败往返。
const (
	// mysqlErrUnsupportedFeature 是 MySQL/TiDB 的 ER_UNSUPPORTED_ACTION（1235）。
	// 在 BeginTx(READ ONLY) 这一步，服务端能报出的 1235 只可能指向 READ ONLY 子句。
	mysqlErrUnsupportedFeature uint16 = 1235

	// readOnlyNoopMarker TiDB 拒绝 READ ONLY 声明的消息特征（小写匹配，兜住无编号的包装错误）。
	readOnlyNoopMarker = "read only has only noop implementation"
)

// locatorReadOnlyUnsupported 能力缓存：key = *sql.DB，value = struct{}。
// 命中表示该实例所在服务端拒绝 READ ONLY 事务声明，后续直接开普通事务。
var locatorReadOnlyUnsupported sync.Map // sync.Map[*sql.DB, struct{}]

// locatorReadOnlyKnownUnsupported 查询能力缓存。
func locatorReadOnlyKnownUnsupported(db *sql.DB) bool {
	_, ok := locatorReadOnlyUnsupported.Load(db)
	return ok
}

// markLocatorReadOnlyUnsupported 记录"该实例不支持 READ ONLY 事务声明"。
func markLocatorReadOnlyUnsupported(db *sql.DB) {
	locatorReadOnlyUnsupported.Store(db, struct{}{})
}

// isReadOnlyUnsupportedErr 判定 BeginTx 错误是否为"服务端拒绝 READ ONLY 事务声明"。
//
// 判定刻意只认这一件事（结构化编号 1235，或 TiDB 的 noop 拒绝消息特征），
// 其他任何 BeginTx 错误都必须原样上抛：把 1064 语法错误、1045 认证失败之类
// 降级成"再试一次普通事务"只会掩盖真实故障并白白多打一次往返。
func isReadOnlyUnsupportedErr(err error) bool {
	if err == nil {
		return false
	}
	if num, ok := sqlErrorNumber(err); ok && num == mysqlErrUnsupportedFeature {
		return true
	}
	// 无编号的包装/代理层错误：只认 TiDB 的 noop 消息特征（保守方向，不参与任何重试判定）。
	return strings.Contains(strings.ToLower(err.Error()), readOnlyNoopMarker)
}

// beginLocatorTx 为 fresh locator 开辟独立事务：
//   - 优先声明 READ ONLY（在 MySQL 等支持的服务端上获得"定位不可能写"的额外保护）；
//   - 命中 TiDB 的 noop 拒绝（Error 1235 / "READ ONLY has only noop implementation"）时，
//     记一次能力缓存 + 告警，并安全降级为普通 BeginTx(ctx, nil)（安全性见本节文档）；
//   - 其他错误原样包装上抛，绝不降级。
func beginLocatorTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if !locatorReadOnlyKnownUnsupported(db) {
		locTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err == nil {
			return locTx, nil
		}
		if !isReadOnlyUnsupportedErr(err) {
			return nil, fmt.Errorf("fresh locator begin tx: %w", err)
		}
		markLocatorReadOnlyUnsupported(db)
		log.Printf("[lockorder] fresh locator: 服务端拒绝 READ ONLY 事务声明（%v）；"+
			"已按 TiDB noop 语义降级为普通独立事务执行纯读定位（该实例后续不再重复尝试，"+
			"定位器仅执行非加锁纯读查询，快照新鲜度与不加锁性质不变）", err)
	}

	locTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fresh locator begin tx: %w", err)
	}
	return locTx, nil
}

// LocatorSnapshot 只读 locator 提取的候选集快照
type LocatorSnapshot struct {
	CandidateKeys    []EntityKey
	StableIdentities map[EntityKey]string
	CreatedAt        time.Time
}

// LocatorFunc 在独立只读事务中执行的纯读查询函数
type LocatorFunc func(ctx context.Context, tx *sql.Tx) (*LocatorSnapshot, error)

// ExecuteFreshLocator 使用同一连接池另开独立事务，执行非加锁快照查询并安全回收连接。
//
// 使用进程级共享的 DefaultLocatorAdmission() 做连接准入（R2 I2）：
// 本函数会在调用方已持有一条连接（外层事务）的情况下再取第二条，
// 不限流时 32 个并发 Run 会在 maxOpenConns=32 的共享池上形成 hold-and-wait。
// 需要自定义容量时改用 ExecuteFreshLocatorWithAdmission。
//
// 事务声明兼容性（真实 TiDB Error 1235）：优先声明 READ ONLY；服务端把 READ ONLY
// 归为 noop 函数并拒绝（tidb_enable_noop_functions=OFF 的 TiDB 默认行为）时，
// 自动降级为普通独立事务继续定位，**绝不因 1235 阻断快照定位**（详见文件头论证）。
func ExecuteFreshLocator(ctx context.Context, db *sql.DB, fn LocatorFunc) (*LocatorSnapshot, error) {
	return ExecuteFreshLocatorWithAdmission(ctx, db, fn, DefaultLocatorAdmission())
}

// ExecuteFreshLocatorWithAdmission 与 ExecuteFreshLocator 相同，但显式指定连接准入信号量。
//
// admission 为 nil 时退回进程级共享单例；admission.Capacity() <= 0 表示不限流。
// 等待许可可被 ctx 取消，并有等待上限（默认 5s）：拿不到许可时返回包装
// ErrAdmissionTimeout 的错误，**不会**在连接池上无限排队（R2 I2）。
//
// 许可生命周期：在 BeginTx **之前**取得、在连接干净归还**之后**才归还（defer），
// fn panic 时同样归还。
func ExecuteFreshLocatorWithAdmission(
	ctx context.Context,
	db *sql.DB,
	fn LocatorFunc,
	admission *AdmissionControl,
) (*LocatorSnapshot, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}
	if fn == nil {
		return nil, ErrNilLocatorFunc
	}
	if admission == nil {
		admission = DefaultLocatorAdmission()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 准入许可必须在 BeginTx **之前**取得、在连接干净归还**之后**才释放。
	// defer 顺序（LIFO）：先注册 release → 最后执行；后注册 rollback → 先执行。
	releaseAdmission, err := admission.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("fresh locator admission: %w", err)
	}
	defer releaseAdmission()

	// 明确开辟独立事务：快照建立在全部门禁已获取之后，排除外层 data TX 老快照漏成员。
	// 事务声明优先 READ ONLY，服务端拒绝（真实 TiDB 的 Error 1235 noop）时安全降级，
	// 见文件头"READ ONLY 事务声明的兼容性降级"一节。
	locTx, err := beginLocatorTx(ctx, db)
	if err != nil {
		return nil, err
	}

	closed := false
	defer func() {
		if !closed {
			_ = locTx.Rollback()
		}
	}()

	snapshot, err := fn(ctx, locTx)
	if err != nil {
		return nil, fmt.Errorf("fresh locator query: %w", err)
	}
	if snapshot == nil {
		snapshot = &LocatorSnapshot{}
	}

	if len(snapshot.CandidateKeys) > MaxLocatorCandidates {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrCandidateLimitExceeded, len(snapshot.CandidateKeys), MaxLocatorCandidates)
	}

	// 显式结束独立只读事务，将连接干净交还连接池
	if err := locTx.Commit(); err != nil {
		return nil, fmt.Errorf("fresh locator commit: %w", err)
	}
	closed = true

	// 自动排序去重，保证满足全序实体加锁前置条件
	snapshot.CandidateKeys = SortEntityKeys(snapshot.CandidateKeys)
	if snapshot.StableIdentities == nil {
		snapshot.StableIdentities = make(map[EntityKey]string)
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}

	return snapshot, nil
}
