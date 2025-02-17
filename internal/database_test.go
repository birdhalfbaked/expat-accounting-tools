package internal_test

import (
	"accounting/internal"
	"os"
	"testing"
)

func TestDatabaseOpens(t *testing.T) {
	internal.InitializeDB()
	_, err := os.Stat("test_ledger.db")
	if err != nil {
		t.Error(err)
	}
}

func TestMain(m *testing.M) {
	os.Remove("test_ledger.db")
	m.Run()
	if internal.GlobalDB != nil {
		internal.GlobalDB.Close()
	}
	os.Remove("test_ledger.db")
}
