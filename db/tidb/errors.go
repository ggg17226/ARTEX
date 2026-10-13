package tidb

import (
	"context"
	"errors"
	"net"

	"github.com/go-sql-driver/mysql"
)

// Kind 是 MySQL/TiDB 错误的分类（跨计划统一的重试/语义判定依据，P03/P04 沿用）。
type Kind int

const (
	KindUnknown        Kind = iota // 未识别：语法、约束、权限等，一律不自动重试
	KindDuplicateKey               // 1062 唯一键冲突（含命名 UNIQUE 冲突）
	KindDeadlock                   // 1213 死锁（可重试）
	KindLockTimeout                // 1205 锁等待超时（可重试）
	KindConnectionLost             // 2006/2013 连接断开（可重试）
	KindConnError                  // driver 前置错误（拨号/认证/握手），区别于业务错误
	KindCancelled                  // context 取消/超时，不重试、不掩盖
)

// Classify 按 driver 错误类型归类。errors.As 兼容任意包装层。
func Classify(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return KindCancelled
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1062:
			return KindDuplicateKey
		case 1213:
			return KindDeadlock
		case 1205:
			return KindLockTimeout
		case 2006, 2013:
			return KindConnectionLost
		default:
			return KindUnknown
		}
	}
	// v1.9.3 最小偏差：driver 已无 MySQLDriverError 类型，前置 dial 错误实测为 *net.OpError（见 task-4-report.md 顾虑 1）
	var drvErr *net.OpError
	if errors.As(err, &drvErr) {
		return KindConnError
	}
	return KindUnknown
}

func IsDuplicateKey(err error) bool   { return Classify(err) == KindDuplicateKey }
func IsDeadlock(err error) bool       { return Classify(err) == KindDeadlock }
func IsLockTimeout(err error) bool    { return Classify(err) == KindLockTimeout }
func IsConnectionLost(err error) bool { return Classify(err) == KindConnectionLost }

// IsRetryable 是跨计划统一的可重试判定：仅死锁、锁超时、连接断开可重试。
// 唯一键冲突、语法/权限/外键类错误与 context 取消一律不可重试。
func IsRetryable(err error) bool {
	switch Classify(err) {
	case KindDeadlock, KindLockTimeout, KindConnectionLost, KindConnError:
		return true
	default:
		return false
	}
}
