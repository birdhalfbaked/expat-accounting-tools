package main

import (
	"accounting/internal"
	"fmt"
	"os"
	"time"

	flag "github.com/spf13/pflag"
)

type ImportConfig struct {
	accountNumber  string
	importLocation string
	importSource   string
}

type MarkConfig struct {
	markDate string
}

func importUsage() {
	fmt.Println(`
	Usage: go run main.go import --file ./file.csv --source nordnet

	--file: import file location
	--source: import record source. Supports: [ nordnet | etrade ]
	`)
}

func markUsage() {
	fmt.Println(`
	Usage: go run main.go mark --date 2024-12-31

	--date: date of market-mark
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

func setImportFlags() ImportConfig {
	var impCfg = ImportConfig{}
	var a = flag.String("account", "", "When importing, your account id associated with the import record")
	var f = flag.String("file", "", "When importing, the location of the file you containing records")
	var s = flag.String("source", "", "When importing, the source. supports: [ nordnet | etrade ]")
	flag.Parse()
	impCfg.accountNumber = *a
	impCfg.importLocation = *f
	impCfg.importSource = *s
	return impCfg
}

func setMarkFlags() MarkConfig {
	var cfg = MarkConfig{}
	var d = flag.String("date", "", "When marking, date in YYYY-MM-DD format")
	flag.Parse()
	cfg.markDate = *d
	return cfg
}

func doImport() {
	impCfg := setImportFlags()
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
		importRecords, err = internal.ReadNordnetExport(impCfg.importLocation, impCfg.accountNumber)
		if err != nil {
			internal.ErrLogger.Println(err)
			return
		}
	} else if impCfg.importSource == "etrade" {
		importRecords, err = internal.ReadETradeExport(impCfg.importLocation, impCfg.accountNumber)
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

func doMark() {
	cfg := setMarkFlags()
	if cfg.markDate == "" {
		fmt.Println("Missing --date flag")
		markUsage()
		return
	}
	var err error
	date, err := time.Parse(time.DateOnly, cfg.markDate)
	if err != nil {
		panic(err.Error())
	}
	internal.MarkStocks(date)
}

func main() {
	flag.Usage = defaultUsage
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	internal.InitializeDB()
	switch os.Args[1] {
	case "import":
		doImport()
	case "rates":
		internal.UpdateRates()
	case "mark":
		doMark()
	default:
		flag.Usage()
		os.Exit(1)
	}
}
