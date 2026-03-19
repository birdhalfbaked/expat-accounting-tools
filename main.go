package main

import (
	"accounting/internal"
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	flag "github.com/spf13/pflag"
)

type ImportConfig struct {
	accountNumber  string
	importLocation string
	importSource   string
	transactionsFile string
	assetsFile       string
	replaceExisting  bool
}

type MarkConfig struct {
	markDate string
}

type ExportConfig struct {
	outDir        string
	accountNumber string
}

func importUsage() {
	fmt.Println(`
	Usage: go run main.go import --file ./file.csv --source nordnet
	--file: import file location (broker exports)
	--source: import record source. Supports: [ nordnet | etrade | reporting ]

	Reporting exports:
	go run main.go import --source reporting --transactions ./transactions.csv --assets ./assets.csv [--replace=false]

	--transactions: reporting transactions export csv file
	--assets: reporting assets export csv file
	--replace: replace existing rows for accounts found in the import (default: true)
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
	Usage: go run main.go [ import | rates | mark | export ]

	import: imports records from transaction exports
	mark: marks to market transactions
	rates: seeds currency_rates
	export: exports your ledger into csv exports for reporting
	`)
}

func setImportFlags() ImportConfig {
	var impCfg = ImportConfig{}
	var a = flag.String("account", "", "When importing, your account id associated with the import record")
	var f = flag.String("file", "", "When importing, the location of the file you containing records")
	var s = flag.String("source", "", "When importing, the source. supports: [ nordnet | etrade | reporting ]")
	var tf = flag.String("transactions", "", "When importing reporting exports: reporting transactions csv file")
	var af = flag.String("assets", "", "When importing reporting exports: reporting assets csv file")
	var r = flag.Bool("replace", true, "When importing reporting exports: replace existing rows for accounts found in the import")
	flag.Parse()
	impCfg.accountNumber = *a
	impCfg.importLocation = *f
	impCfg.importSource = *s
	impCfg.transactionsFile = *tf
	impCfg.assetsFile = *af
	impCfg.replaceExisting = *r
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
	if impCfg.importLocation == "" && impCfg.importSource != "reporting" {
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
	} else if impCfg.importSource == "reporting" {
		if impCfg.assetsFile == "" {
			fmt.Println("Missing --assets flag")
			importUsage()
			return
		}
		if impCfg.transactionsFile == "" {
			fmt.Println("Missing --transactions flag")
			importUsage()
			return
		}
		err = internal.ImportReportingExports(impCfg.assetsFile, impCfg.transactionsFile, impCfg.replaceExisting)
		if err != nil {
			internal.ErrLogger.Println(err)
			return
		}
		return
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
		// Prompt user if not provided. This avoids panics due to missing flags.
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter mark date (YYYY-MM-DD): ")
			text, _ := reader.ReadString('\n')
			text = strings.TrimSpace(text)
			d, err := time.Parse(time.DateOnly, text)
			if err != nil {
				fmt.Printf("Invalid date. Expected format: YYYY-MM-DD\n")
				continue
			}
			internal.MarkStocks(d)
			return
		}
	}

	// Validate input date strictly.
	d, err := time.Parse(time.DateOnly, cfg.markDate)
	if err != nil {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("Invalid --date (%s). Expected YYYY-MM-DD.\n", cfg.markDate)
			fmt.Print("Enter mark date (YYYY-MM-DD): ")
			text, _ := reader.ReadString('\n')
			text = strings.TrimSpace(text)
			d, err = time.Parse(time.DateOnly, text)
			if err != nil {
				fmt.Printf("Invalid date. Expected format: YYYY-MM-DD\n")
				continue
			}
			break
		}
	}
	internal.MarkStocks(d)
}

func setExportFlags() ExportConfig {
	var cfg = ExportConfig{}
	var out = flag.String("out", "./reporting", "Output directory for reporting csv exports")
	var account = flag.String("account", "", "Optional: only export rows for this account")
	flag.Parse()
	cfg.outDir = *out
	cfg.accountNumber = *account
	return cfg
}

func doExport() {
	cfg := setExportFlags()
	if cfg.outDir == "" {
		fmt.Println("Missing --out flag")
		return
	}
	err := internal.ExportReportingExports(cfg.outDir, cfg.accountNumber)
	if err != nil {
		internal.ErrLogger.Println(err)
	}
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
	case "export":
		doExport()
	default:
		flag.Usage()
		os.Exit(1)
	}
}
