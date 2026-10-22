package evidence

import (
	"os"
	"testing"
)

// TestMain 不做任何全局初始化：本包的连库门控全部落在用例级
// （evidenceDSN：未配置 ARTEX_TIDB_DSN → 诚实 t.Skip；配置了却打不开 → t.Fatal）。
// 这里绝不设全局 Skip，也不允许任何包级开关把整包用例抹成"通过"。
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
