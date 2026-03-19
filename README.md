# Expat Accounting Tools

A local CLI for importing broker transactions, marking holdings to market, and exporting reporting-ready CSVs.

## Requirements

- Go `1.23.1+`
- SQLite (via `github.com/mattn/go-sqlite3`, bundled through Go module)

## Quick Start

From the project root:

```bash
go test ./...
go run . --help
```

The app creates and uses `ledger.db` in the project root.

## Commands

### Import broker exports

#### Nordnet

```bash
go run . import --source nordnet --file ./path/to/nordnet-export.csv --account 123456
```

#### E*TRADE

```bash
go run . import --source etrade --file ./path/to/etrade-export.csv --account 123456
```

### Import reporting exports

Imports previously exported reporting files (`assets.csv` + `transactions.csv`).

```bash
go run . import --source reporting --assets ./reporting/assets.csv --transactions ./reporting/transactions.csv
```

Optional:

- `--replace=false` to append instead of replacing existing rows for imported accounts.

### Mark holdings

```bash
go run . mark --date 2025-12-31
```

- Date format is strict: `YYYY-MM-DD`.
- If `--date` is missing or invalid, CLI prompts until a valid date is entered.
- If Yahoo price lookup fails, CLI prompts for a manual price per share (blank input skips that symbol).
- Manual answers are cached for the run to avoid repeated prompts.

### Export reporting CSVs

```bash
go run . export --out ./reporting
```

Optional:

- `--account <id>` to export only one account.

This writes:

- `reporting/transactions.csv`
- `reporting/assets.csv`

## Reporting CSV Formats

### transactions.csv

Header:

`Account,Date Settled,Symbol,Share Lot ID,Transaction Type,Total Amount,Total Amount Currency,Total Amount USD`

### assets.csv

Header:

`Account,Exchange,Symbol,ISIN,Asset Lot,Date Attained,Originated Shares,Shares Left,Cost Basis,Cost Basis Currency,Cost Basis USD,Marked Date,Marked Shares,Marked Share Value,Marked Share Value Currency,Marked Share Value USD,Marked Capital Gain,Marked Capital Gain Currency,Marked Capital Gain USD`

## Exchange and Currency Notes

- Exchange is stored on asset lots and exported/imported in reporting files.
- Broker import defaults:
  - Nordnet -> `Nasdaq OMX Stockholm AB`
  - E*TRADE -> `Nasdaq`
- USD conversions in reporting export:
  - Prefer rate from the same calendar year as the marked/settled date.
  - Fallback to latest prior available rate if that year is missing.

## Seed FX Rates

To seed built-in currency rows/rates:

```bash
go run . rates
```

## Project Layout

- `main.go` - CLI entrypoint and command routing
- `internal/converter.go` - broker/reporting import parsing and transforms
- `internal/database.go` - schema + data access
- `internal/operations.go` - import handlers and mark-to-market logic
- `internal/reporting.go` - reporting CSV export/import
- `testing/` - sample input files

