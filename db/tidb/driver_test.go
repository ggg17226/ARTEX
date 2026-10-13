package tidb_test

import (
	"testing"

	"github.com/Autumn-27/artex/db/tidb"
)

func TestDriverName(t *testing.T) {
	if got := tidb.DriverName(); got != "mysql" {
		t.Fatalf("DriverName() = %q, want %q", got, "mysql")
	}
}
