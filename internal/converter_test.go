package internal_test

import (
	"accounting/internal"
	"log"
	"testing"
)

func TestReadNordnetExport(t *testing.T) {
	// Use fake file
	records, _ := internal.ReadNordnetExport("../testing/nordnet-transactions.csv", "1234")
	for _, v := range records {
		log.Printf("%#v\n", v)
	}
}
