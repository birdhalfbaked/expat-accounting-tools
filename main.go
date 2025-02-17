package main

import (
	"accounting/internal"
	"flag"
	"fmt"
)

var importConfig = struct {
	importLocation string
	importSource   string
}{}

func importUsage() {
	fmt.Println(`
Usage: go run main.go import --file ./file.csv --source nordnet

	--file: import file location
	--source: import record source. Supports: [ nordnet ]
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
	flag.Usage = importUsage
	flag.StringVar(&importConfig.importLocation, "file", "", "When importing, the location of the file you containing records")
	flag.StringVar(&importConfig.importSource, "source", "", "When importing, the source. supports: [ nordnet ]")
	flag.Parse()
}

func doImport() {
	if importConfig.importLocation == "" {
		fmt.Println("Missing --file flag")
		flag.Usage()
		return
	}
	if importConfig.importSource == "" {
		fmt.Println("Missing --source flag")
		flag.Usage()
		return
	}
	var err error
	var importRecords []internal.ImportRecord
	if importConfig.importSource == "nordnet" {
		importRecords, err = internal.ReadNordnetExport(importConfig.importLocation)
		if err != nil {
			internal.ErrLogger.Println(err)
		}
	} else {
		fmt.Println("Import source not supported")
		importUsage()
	}
	err = internal.HandleImport(importRecords)
	if err != nil {
		internal.ErrLogger.Println(err)
	}
}

func main() {
	flag.Parse()
	flag.Usage = defaultUsage
	internal.InitializeDB()
	switch flag.Arg(0) {
	case "import":
		setImportFlags()
		doImport()
	default:
		flag.Usage()
	}

}
