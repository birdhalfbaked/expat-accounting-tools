package main

import (
	"accounting/internal"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"
)

type ImportConfig struct {
	importLocation string
	importSource   string
}

var impCfg = new(ImportConfig)

var f = flag.String("file", "", "When importing, the location of the file you containing records")
var s = flag.String("source", "", "When importing, the source. supports: [ nordnet | etrade ]")

func importUsage() {
	fmt.Println(`
Usage: go run main.go import --file ./file.csv --source nordnet

	--file: import file location
	--source: import record source. Supports: [ nordnet | etrade ]
	`)
}

func defaultUsage() {
	fmt.Println(`
Usage: go run main.go [ import | mark | export ]

	import: imports records from transaction exports
	mark: marks to market transactions
	export: exports your ledger into csv exports for reporting
	`)
}

func setImportFlags() {
	flag.Parse()
	impCfg.importLocation = *f
	impCfg.importSource = *s
}

func doImport() {
	if impCfg.importLocation == "" {
		fmt.Println("Missing --file flag")
		importUsage()
		return
	}
	if impCfg.importSource == "" {
		fmt.Println("Missing --source flag")
		importUsage()
		return
	}
	var err error
	var importRecords []internal.ImportRecord
	if impCfg.importSource == "nordnet" {
		importRecords, err = internal.ReadNordnetExport(impCfg.importLocation)
		if err != nil {
			internal.ErrLogger.Println(err)
			return
		}
	} else if impCfg.importSource == "etrade" {
		importRecords, err = internal.ReadETradeExport(impCfg.importLocation)
		if err != nil {
			internal.ErrLogger.Println(err)
			return
		}
	} else {
		fmt.Println("Import source not supported")
		importUsage()
		return
	}
	err = internal.HandleImport(importRecords)
	if err != nil {
		internal.ErrLogger.Println(err)
	}
}

func main() {
	setImportFlags()
	flag.Usage = defaultUsage
	internal.InitializeDB()
	switch flag.Arg(0) {
	case "import":
		doImport()
	case "rates":
		internal.UpdateRates()
	default:
		flag.Usage()
		os.Exit(1)
	}
}
