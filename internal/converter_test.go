package internal_test

import (
	"accounting/internal"
	"log"
	"testing"
)

func TestReadNordnetExport(t *testing.T) {
	// Use fake file
	records, _ := internal.ReadNordnetExport("../testing/nordnet-transactions.csv")
	for _, v := range records {
		log.Printf("%#v\n", v)
	}
}
