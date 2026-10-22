package db

import (
	"fmt"
	"testing"
	"time"
)

// InsertSettingIfAbsent 是 auth.password_hash 的兜底：调用方的 GetSetting 检查可能
// 因为数据库报错而失效，也可能被并发请求插队（bcrypt 要跑几十毫秒），所以"仅首次
// 可设"的保证必须落在唯一约束上，而不是上层的 if 判断：基线里 settings 的自然键是
// uq_settings_key_hash（TEXT 不能直接建唯一索引，改由 key_hash 承载）。
func TestInsertSettingIfAbsentDoesNotOverwrite(t *testing.T) {
	d := openTestDB(t)

	// 用测试专属 key，绝不碰开发库里真实的 auth.password_hash。
	key := fmt.Sprintf("test.insert_if_absent.%d", time.Now().UnixNano())
	defer func() { _, _ = d.Exec("DELETE FROM settings WHERE `key` = ?", key) }()

	inserted, err := d.InsertSettingIfAbsent(key, "first")
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("首次写入应返回 inserted=true")
	}

	inserted, err = d.InsertSettingIfAbsent(key, "second")
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("键已存在时应返回 inserted=false")
	}

	got, ok, err := d.GetSetting(key)
	if err != nil || !ok {
		t.Fatalf("GetSetting: ok=%v err=%v", ok, err)
	}
	if got != "first" {
		t.Fatalf("值被覆盖成 %q，应保持 %q", got, "first")
	}
}
