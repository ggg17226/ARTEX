package db

import (
	"database/sql"

	"github.com/Autumn-27/artex/db/schema"
	"github.com/Autumn-27/artex/db/tidb"
)

// Settings is a tiny key-value store for global app config the UI toggles at
// runtime (e.g. traffic_capture). Missing keys fall back to caller defaults.
//
// 新库（P08 原生 TiDB 65 表基线）里 settings 的主键是隐藏的 row_id，自然键
// `key` 是 MEDIUMTEXT（无法直接建唯一索引），唯一性由 uq_settings_key_hash
// (key_hash BINARY(32)) 承载：
//
//	key_identity = schema.SettingsKeyIdentity(key) 的 canonical 载荷
//	key_hash     = SHA-256(key_identity)
//
// 因此每一条写入都必须同时补齐这两列（缺失会让唯一键退化为 NULL，重复行再也
// 挡不住），`key` / `value` 是 MySQL 关键字，列名一律反引号包裹。

// settingsKeySQL 是 settings 表按自然键取值的语句；identity/hash 由调用方补齐。
const settingsKeySQL = "SELECT `value` FROM settings WHERE `key` = ?"

// settingsKeyIdentity 是 settings 表（以及同构的 tools / scheduler_state）的
// canonical 身份对：载荷进 key_identity，SHA-256 进 key_hash。
func settingsKeyIdentity(key string) (payload []byte, hash []byte) {
	return canonicalIdentityPair(schema.SettingsKeyIdentity(key))
}

// GetSetting returns the stored value and ok=false when the key is unset.
func (d *DB) GetSetting(key string) (value string, ok bool, err error) {
	err = d.QueryRow(settingsKeySQL, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// SetSetting upserts a setting value.
//
// 冲突判定落在 uq_settings_key_hash 上（TiDB 原生 ON DUPLICATE KEY UPDATE）；
// identity/hash 属于不可变身份列，只在插入分支写入，更新分支只改 value/updated_at。
func (d *DB) SetSetting(key, value string) error {
	identity, hash := settingsKeyIdentity(key)
	_, err := d.Exec(`
INSERT INTO settings (`+"`key`"+`, key_identity, key_hash, `+"`value`"+`, updated_at)
VALUES (?, ?, ?, ?, NOW(3))
ON DUPLICATE KEY UPDATE `+"`value`"+` = VALUES(`+"`value`"+`), updated_at = NOW(3)`,
		key, identity, hash, value)
	return err
}

// InsertSettingIfAbsent 只在 key 尚未存在时写入，已存在则原样保留并返回 inserted=false。
// 给 auth.password_hash 这类"只允许首次设置"的键用：判定落在数据库的唯一键上
// （uq_settings_key_hash），调用方先 GetSetting 再写的那种检查就只是快速失败路径——
// 读出错或并发撞车时，已有的值也不会被顺手覆盖掉。
//
// 这里刻意不使用 ON DUPLICATE KEY UPDATE / INSERT IGNORE：前者的"影响行数 0/1/2"
// 语义在"更新未改变任何列"时依赖驱动能力位，后者会把真正的写入错误一并降级成
// 告警。裸 INSERT + 1062 判重在两种引擎上都是确定的：要么写进去，要么显式撞键。
func (d *DB) InsertSettingIfAbsent(key, value string) (inserted bool, err error) {
	identity, hash := settingsKeyIdentity(key)
	res, err := d.Exec(`
INSERT INTO settings (`+"`key`"+`, key_identity, key_hash, `+"`value`"+`, updated_at)
VALUES (?, ?, ?, ?, NOW(3))`, key, identity, hash, value)
	if err != nil {
		if tidb.IsDuplicateKey(err) {
			return false, nil
		}
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// GetBool returns the boolean setting, or def when unset/unparseable.
func (d *DB) GetBool(key string, def bool) bool {
	v, ok, err := d.GetSetting(key)
	if err != nil || !ok {
		return def
	}
	return v == "true" || v == "1"
}

// SetBool stores a boolean setting as "true"/"false".
func (d *DB) SetBool(key string, val bool) error {
	if val {
		return d.SetSetting(key, "true")
	}
	return d.SetSetting(key, "false")
}
