package server

import (
	"os"
	"testing"
)

// TestMain skips PostgreSQL initialization during the native TiDB migration.
// It directly runs the package tests without acquiring any database connection or advisory lock.
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
