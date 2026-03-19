package internal

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ericlagergren/decimal"
)

type reportingTransactionRow struct {
	Account       string
	DateSettled   time.Time
	Symbol        string
	ShareLotID    string
	Transaction   TransactionType
	TotalAmount   *decimal.Big
	TotalCurrency CurrencyUnit
}

type reportingAssetRow struct {
	Account           string
	Exchange          string
	Symbol            string
	ISIN              string
	AssetLotID        string
	DateAttained      time.Time
	OriginatedShares  *decimal.Big
	SharesLeft        *decimal.Big
	CostBasis         *decimal.Big
	CostBasisCurrency CurrencyUnit

	MarkedDate              *time.Time
	MarkedShares            *decimal.Big
	MarkedShareValue        *decimal.Big
	MarkedShareValueCurrency CurrencyUnit
	MarkedCapitalGain       *decimal.Big
	MarkedCapitalGainCurrency CurrencyUnit
}

func parseCurrencyUnit(s string) (CurrencyUnit, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return "", errors.New("missing currency")
	}
	switch CurrencyUnit(strings.ToUpper(v)) {
	case USD, SEK:
		return CurrencyUnit(strings.ToUpper(v)), nil
	default:
		return "", fmt.Errorf("unsupported currency: %s", v)
	}
}

func currencyToLocale(c CurrencyUnit) (LocaleUnit, error) {
	switch c {
	case USD:
		return US, nil
	case SEK:
		return SE, nil
	default:
		return "", fmt.Errorf("unsupported currency for locale: %s", c)
	}
}

func parseReportDecimal(s string, currency CurrencyUnit) (*decimal.Big, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, nil
	}
	trimmed = strings.ReplaceAll(trimmed, "\u00a0", "")
	trimmed = strings.ReplaceAll(trimmed, " ", "")

	// Normalize thousands/decimal separators based on expected locale.
	locale, err := currencyToLocale(currency)
	if err != nil {
		return nil, err
	}

	// If value already contains the locale decimal separator, just strip opposite separators.
	// Otherwise, attempt a best-effort conversion of decimal separator.
	if locale == SE {
		// Expected decimal separator is ',' and thousands separator is '.'
		if strings.Contains(trimmed, ",") && strings.Contains(trimmed, ".") {
			trimmed = strings.ReplaceAll(trimmed, ".", "")
		} else if strings.Contains(trimmed, ".") && !strings.Contains(trimmed, ",") {
			trimmed = strings.Replace(trimmed, ".", ",", 1)
		}
	} else {
		// Expected decimal separator is '.' and thousands separator is ','
		if strings.Contains(trimmed, ",") && strings.Contains(trimmed, ".") {
			trimmed = strings.ReplaceAll(trimmed, ",", "")
		} else if strings.Contains(trimmed, ",") && !strings.Contains(trimmed, ".") {
			trimmed = strings.Replace(trimmed, ",", ".", 1)
		}
	}

	val, err := ProcessStringAmount(trimmed, locale)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func decimalToLocaleString(val *decimal.Big, currency CurrencyUnit) string {
	if val == nil {
		return ""
	}
	locale, err := currencyToLocale(currency)
	if err != nil {
		// Fallback: output raw
		return val.String()
	}
	s := val.String()
	if locale == SE {
		s = strings.ReplaceAll(s, ".", ",")
	}
	return s
}

func parseDateReport(s string) (time.Time, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return time.Time{}, errors.New("missing date")
	}
	// Prefer YYYY-MM-DD.
	if t, err := time.Parse("2006-01-02", trimmed); err == nil {
		return t, nil
	}
	// Fallbacks (if exports include times).
	if t, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", trimmed); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date: %s", s)
}

func parseOptionalDateReport(s string) (*time.Time, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, nil
	}
	t, err := parseDateReport(trimmed)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func transactionTypeFromString(s string) (TransactionType, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return 0, errors.New("missing transaction type")
	}
	upper := strings.ToUpper(v)
	// Match our TransactionType.String() values (e.g. PURCHASE_TRANSACTION).
	switch upper {
	case "PURCHASE_TRANSACTION":
		return PURCHASE_TRANSACTION, nil
	case "SALE_TRANSACTION":
		return SALE_TRANSACTION, nil
	case "TRANSFERIN_TRANSACTION":
		return TRANSFERIN_TRANSACTION, nil
	case "TRANSFEROUT_TRANSACTION":
		return TRANSFEROUT_TRANSACTION, nil
	case "SPLITIN_TRANSACTION":
		return SPLITIN_TRANSACTION, nil
	case "SPLITOUT_TRANSACTION":
		return SPLITOUT_TRANSACTION, nil
	case "DIVIDEND":
		return DIVIDEND, nil
	case "QUALIFIED_DIVIDEND":
		return QUALIFIED_DIVIDEND, nil
	default:
		// Best-effort: if export used a friendly name, try substring match.
		if strings.Contains(upper, "PURCHASE") {
			return PURCHASE_TRANSACTION, nil
		}
		if strings.Contains(upper, "SOLD") || strings.Contains(upper, "SALE") {
			return SALE_TRANSACTION, nil
		}
		if strings.Contains(upper, "TRANSFERIN") || strings.Contains(upper, "TRANSFER IN") {
			return TRANSFERIN_TRANSACTION, nil
		}
		if strings.Contains(upper, "TRANSFEROUT") || strings.Contains(upper, "TRANSFER OUT") {
			return TRANSFEROUT_TRANSACTION, nil
		}
		if strings.Contains(upper, "SPLITIN") || strings.Contains(upper, "SPLIT IN") {
			return SPLITIN_TRANSACTION, nil
		}
		if strings.Contains(upper, "SPLITOUT") || strings.Contains(upper, "SPLIT OUT") {
			return SPLITOUT_TRANSACTION, nil
		}
		if strings.Contains(upper, "DIVIDEND") {
			return DIVIDEND, nil
		}
		return 0, fmt.Errorf("unknown transaction type: %s", s)
	}
}

func transactionTypeToExportString(t TransactionType) string {
	return t.String()
}

func getRateToOneUSD(currency CurrencyUnit, asOf time.Time) (*decimal.Big, error) {
	if currency == USD {
		return decimal.New(1, 0), nil
	}

	var rateInt int64
	queryYear := `
		SELECT rate
		FROM currency_rates
		-- We treat currency_rates as "per year" rates. The table is seeded with
		-- e.g. 2024-12-31, but marks may happen earlier in 2024. Selecting by
		-- calendar year avoids falling back to the previous year.
		WHERE currency_code = ?
			AND strftime('%Y', as_of_date) = strftime('%Y', ?)
		ORDER BY as_of_date DESC
		LIMIT 1;
	`
	// SQLite driver typically accepts time.Time; formatting is handled by database/sql.
	row := GlobalDB.QueryRow(queryYear, string(currency), asOf)
	err := row.Scan(&rateInt)
	if err != nil {
		// If the exact calendar year rate doesn't exist (e.g. exports into 2025
		// but `rates` hasn't been extended beyond 2024), fall back to the latest
		// available rate up to the requested date so exported USD fields aren't blank.
		if errors.Is(err, sql.ErrNoRows) {
			queryLatest := `
				SELECT rate
				FROM currency_rates
				WHERE currency_code = ? AND as_of_date <= ?
				ORDER BY as_of_date DESC
				LIMIT 1;
			`
			row2 := GlobalDB.QueryRow(queryLatest, string(currency), asOf)
			if err2 := row2.Scan(&rateInt); err2 != nil {
				return nil, err2
			}
		} else {
			return nil, err
		}
	}
	// Rates are stored as BIGINT with 4-decimal fixed-point precision
	// (e.g. SEK rate 101220 represents 10.1220 SEK per 1.0000 USD).
	return decimal.New(rateInt, 4), nil
}

func convertFromCurrencyToUSD(amount *decimal.Big, amountCurrency CurrencyUnit, asOf time.Time) (*decimal.Big, error) {
	if amount == nil {
		return nil, nil
	}
	if amountCurrency == USD {
		return amount, nil
	}
	rate, err := getRateToOneUSD(amountCurrency, asOf)
	if err != nil {
		return nil, err
	}
	usd := decimal.New(0, 4).Quo(amount, rate).Quantize(4)
	return usd, nil
}

func ExportReportingExports(outDir string, accountNumber string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Ensure currency conversion data exists for USD columns.
	if err := ensureCurrencyRates(); err != nil {
		return err
	}

	transactionsPath := filepath.Join(outDir, "transactions.csv")
	assetsPath := filepath.Join(outDir, "assets.csv")

	if err := exportTransactionsCSV(transactionsPath, accountNumber); err != nil {
		return err
	}
	if err := exportAssetsCSV(assetsPath, accountNumber); err != nil {
		return err
	}
	return nil
}

func exportTransactionsCSV(path string, accountNumber string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header must match the format you provided.
	if err := w.Write([]string{
		"Account",
		"Date Settled",
		"Symbol",
		"Share Lot ID",
		"Transaction Type",
		"Total Amount",
		"Total Amount Currency",
		"Total Amount USD",
	}); err != nil {
		return err
	}

	var rowsAccountClause string
	var args []any
	if accountNumber != "" {
		rowsAccountClause = "WHERE account = ?"
		args = append(args, accountNumber)
	}

	query := fmt.Sprintf(`
		SELECT account, settlement_date, symbol, share_lot, transaction_type, total_amount, currency
		FROM transactions
		%s
		ORDER BY settlement_date ASC;
	`, rowsAccountClause)

	rows, err := GlobalDB.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var account string
		var settlementDate time.Time
		var symbol string
		var shareLot sql.NullString
		var transactionTypeInt int64
		var totalAmountInt int64
		var currency string

		if err := rows.Scan(&account, &settlementDate, &symbol, &shareLot, &transactionTypeInt, &totalAmountInt, &currency); err != nil {
			return err
		}

		curr := CurrencyUnit(strings.ToUpper(strings.TrimSpace(currency)))
		totalAmount := decimal.New(totalAmountInt, 4)

		totalAmountUSD := (*decimal.Big)(nil)
		if curr == USD {
			totalAmountUSD = totalAmount
		} else {
			usd, err := convertFromCurrencyToUSD(totalAmount, curr, settlementDate)
			if err == nil {
				totalAmountUSD = usd
			}
		}

		if err := w.Write([]string{
			account,
			settlementDate.Format("2006-01-02"),
			symbol,
			shareLot.String,
			transactionTypeToExportString(TransactionType(transactionTypeInt)),
			decimalToLocaleString(totalAmount, curr),
			string(curr),
			decimalToLocaleString(totalAmountUSD, USD),
		}); err != nil {
			return err
		}
	}

	return rows.Err()
}

func exportAssetsCSV(path string, accountNumber string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{
		"Account",
		"Exchange",
		"Symbol",
		"ISIN",
		"Asset Lot",
		"Date Attained",
		"Originated Shares",
		"Shares Left",
		"Cost Basis",
		"Cost Basis Currency",
		"Cost Basis USD",
		"Marked Date",
		"Marked Shares",
		"Marked Share Value",
		"Marked Share Value Currency",
		"Marked Share Value USD",
		"Marked Capital Gain",
		"Marked Capital Gain Currency",
		"Marked Capital Gain USD",
	}); err != nil {
		return err
	}

	var rowsAccountClause string
	var args []any
	if accountNumber != "" {
		rowsAccountClause = "WHERE account = ?"
		args = append(args, accountNumber)
	}

	query := fmt.Sprintf(`
		SELECT id, account, exchange, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
		FROM asset_lots
		%s
		ORDER BY created_date ASC;
	`, rowsAccountClause)

	rows, err := GlobalDB.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var assetLotID string
		var account string
		var exchangeRaw sql.NullString
		var symbol string
		var isin string
		var sharesLeftInt int64
		var costBasisPerShareInt int64
		var costBasisCurrencyRaw string
		var createdDate time.Time

		if err := rows.Scan(&assetLotID, &account, &exchangeRaw, &symbol, &isin, &sharesLeftInt, &costBasisPerShareInt, &costBasisCurrencyRaw, &createdDate); err != nil {
			return err
		}

		costBasisCurrency := CurrencyUnit(strings.ToUpper(strings.TrimSpace(costBasisCurrencyRaw)))
		sharesLeft := decimal.New(sharesLeftInt, 4)
		costBasis := decimal.New(costBasisPerShareInt, 4)

		originatedShares, originatedCostCurrency := (*decimal.Big)(nil), CurrencyUnit("")
		{
			row := GlobalDB.QueryRow(`
				SELECT shares, cost_basis_per_share, cost_basis_currency, as_of_date
				FROM asset_lots_history
				WHERE id = ?
				ORDER BY as_of_date ASC
				LIMIT 1;
			`, assetLotID)
			var originSharesInt int64
			var originCostBasisInt int64
			var originCostCurrencyRaw string
			var originDate time.Time
			if err := row.Scan(&originSharesInt, &originCostBasisInt, &originCostCurrencyRaw, &originDate); err == nil {
				originatedShares = decimal.New(originSharesInt, 4)
				originatedCostCurrency = CurrencyUnit(strings.ToUpper(strings.TrimSpace(originCostCurrencyRaw)))
			}
		}

		// If history is missing, fall back to current asset_lots values.
		if originatedShares == nil {
			originatedShares = sharesLeft
			originatedCostCurrency = costBasisCurrency
		}

		// Mark data (latest mark).
		var markedDate *time.Time
		var markedShares *decimal.Big
		var markedValuePerShare *decimal.Big
		var markedValueCurrency CurrencyUnit
		var gainLoss *decimal.Big
		{
			row := GlobalDB.QueryRow(`
				SELECT market_mark_date, marked_shares, marked_value_per_share, marked_value_currency, gain_loss
				FROM market_marks
				WHERE asset_lot_id = ?
				ORDER BY market_mark_date DESC
				LIMIT 1;
			`, assetLotID)
			var md time.Time
			var msInt int64
			var mvpInt int64
			var mvcRaw string
			var glInt int64
			if err := row.Scan(&md, &msInt, &mvpInt, &mvcRaw, &glInt); err == nil {
				markedDate = &md
				markedShares = decimal.New(msInt, 4)
				markedValuePerShare = decimal.New(mvpInt, 4)
				markedValueCurrency = CurrencyUnit(strings.ToUpper(strings.TrimSpace(mvcRaw)))
				gainLoss = decimal.New(glInt, 4)
			}
		}

		costBasisUSD, _ := convertFromCurrencyToUSD(costBasis, costBasisCurrency, createdDate)

		var markedSharesStr, markedValueStr, markedDateStr string
		var markedValueUSDStr, markedGainStr, markedGainCurrencyStr, markedGainUSDStr string
		var markedValueCurrencyStr string

		if markedDate != nil {
			markedDateStr = markedDate.Format("2006-01-02")
			markedSharesStr = decimalToLocaleString(markedShares, markedValueCurrency)
			markedValueStr = decimalToLocaleString(markedValuePerShare, markedValueCurrency)
			markedValueCurrencyStr = string(markedValueCurrency)

			markedValueUSD, err := convertFromCurrencyToUSD(markedValuePerShare, markedValueCurrency, *markedDate)
			if err == nil {
				markedValueUSDStr = decimalToLocaleString(markedValueUSD, USD)
			}

			markedGainStr = decimalToLocaleString(gainLoss, markedValueCurrency)
			markedGainCurrencyStr = string(markedValueCurrency)
			markedGainUSD, err := convertFromCurrencyToUSD(gainLoss, markedValueCurrency, *markedDate)
			if err == nil {
				markedGainUSDStr = decimalToLocaleString(markedGainUSD, USD)
			}
		}

		exchange := exchangeRaw.String

		row := []string{
			account,
			exchange,
			symbol,
			isin,
			assetLotID,
			createdDate.Format("2006-01-02"),
			decimalToLocaleString(originatedShares, originatedCostCurrency),
			decimalToLocaleString(sharesLeft, costBasisCurrency),
			decimalToLocaleString(costBasis, costBasisCurrency),
			string(costBasisCurrency),
			decimalToLocaleString(costBasisUSD, USD),
			markedDateStr,
			markedSharesStr,
			markedValueStr,
			markedValueCurrencyStr,
			markedValueUSDStr,
			markedGainStr,
			markedGainCurrencyStr,
			markedGainUSDStr,
		}

		if err := w.Write(row); err != nil {
			return err
		}
	}

	return rows.Err()
}

func ReadReportingTransactionsExport(filepath string) ([]reportingTransactionRow, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	// Default is ','.
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	if len(header) < 8 {
		return nil, errors.New("invalid transactions export: unexpected header length")
	}

	rows := make([]reportingTransactionRow, 0)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 8 {
			continue
		}

		acc := strings.TrimSpace(rec[0])
		date, err := parseDateReport(rec[1])
		if err != nil {
			return nil, err
		}
		symbol := strings.TrimSpace(rec[2])
		shareLotID := strings.TrimSpace(rec[3])
		txType, err := transactionTypeFromString(rec[4])
		if err != nil {
			return nil, err
		}
		totalCurrency, err := parseCurrencyUnit(rec[6])
		if err != nil {
			return nil, err
		}
		totalAmt, err := parseReportDecimal(rec[5], totalCurrency)
		if err != nil {
			return nil, err
		}

		rows = append(rows, reportingTransactionRow{
			Account:       acc,
			DateSettled:   date,
			Symbol:        symbol,
			ShareLotID:    shareLotID,
			Transaction:   txType,
			TotalAmount:   totalAmt,
			TotalCurrency: totalCurrency,
		})
	}

	return rows, nil
}

func ReadReportingAssetsExport(filepath string) ([]reportingAssetRow, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	if len(header) < 19 {
		return nil, errors.New("invalid assets export: unexpected header length")
	}

	rows := make([]reportingAssetRow, 0)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 19 {
			continue
		}

		acc := strings.TrimSpace(rec[0])
		exchange := strings.TrimSpace(rec[1])
		symbol := strings.TrimSpace(rec[2])
		isin := strings.TrimSpace(rec[3])
		assetLotID := strings.TrimSpace(rec[4])

		dateAttained, err := parseDateReport(rec[5])
		if err != nil {
			return nil, err
		}

		costBasisCurrency, err := parseCurrencyUnit(rec[9])
		if err != nil {
			return nil, err
		}

		originatedShares, err := parseReportDecimal(rec[6], costBasisCurrency)
		if err != nil {
			return nil, err
		}
		sharesLeft, err := parseReportDecimal(rec[7], costBasisCurrency)
		if err != nil {
			return nil, err
		}
		costBasis, err := parseReportDecimal(rec[8], costBasisCurrency)
		if err != nil {
			return nil, err
		}

		markedDate, err := parseOptionalDateReport(rec[11])
		if err != nil {
			return nil, err
		}

		// Marked fields: if Marked Date is empty, treat the rest as empty.
		var markedShares *decimal.Big
		var markedValuePerShare *decimal.Big
		var markedValueCurrency CurrencyUnit
		var markedGain *decimal.Big
		var markedGainCurrency CurrencyUnit

		markedValueCurrency, _ = parseCurrencyUnit(rec[14])
		markedGainCurrency, _ = parseCurrencyUnit(rec[17])

		if markedDate != nil {
			// If currency cells are empty, fall back to cost basis currency.
			if markedValueCurrency == "" {
				markedValueCurrency = costBasisCurrency
			}
			if markedGainCurrency == "" {
				markedGainCurrency = costBasisCurrency
			}
			if markedValueCurrency != markedGainCurrency {
				return nil, fmt.Errorf("marked currency mismatch in assets export: marked share value currency %s != marked capital gain currency %s", markedValueCurrency, markedGainCurrency)
			}
			// From here on, marked numeric values are interpreted using the same currency.
			markedGainCurrency = markedValueCurrency

			markedShares, err = parseReportDecimal(rec[12], markedValueCurrency)
			if err != nil {
				return nil, err
			}
			markedValuePerShare, err = parseReportDecimal(rec[13], markedValueCurrency)
			if err != nil {
				return nil, err
			}
			// Marked capital gain uses the same currency as marked share value (or we error above).
			markedGain, err = parseReportDecimal(rec[16], markedGainCurrency)
			if err != nil {
				return nil, err
			}
		}

		rows = append(rows, reportingAssetRow{
			Account:             acc,
			Exchange:            exchange,
			Symbol:              symbol,
			ISIN:                isin,
			AssetLotID:          assetLotID,
			DateAttained:        dateAttained,
			OriginatedShares:    originatedShares,
			SharesLeft:          sharesLeft,
			CostBasis:           costBasis,
			CostBasisCurrency:   costBasisCurrency,
			MarkedDate:          markedDate,
			MarkedShares:        markedShares,
			MarkedShareValue:    markedValuePerShare,
			MarkedShareValueCurrency: markedValueCurrency,
			MarkedCapitalGain:  markedGain,
			MarkedCapitalGainCurrency: markedGainCurrency,
		})
	}

	return rows, nil
}

func ImportReportingExports(assetsFile string, transactionsFile string, replaceExisting bool) error {
	// Ensure foreign-key targets exist even if user hasn't run `rates` yet.
	if err := ensureSupportedCurrencies(); err != nil {
		return err
	}

	assetRows, err := ReadReportingAssetsExport(assetsFile)
	if err != nil {
		return err
	}
	txRows, err := ReadReportingTransactionsExport(transactionsFile)
	if err != nil {
		return err
	}

	accounts := make(map[string]struct{})
	for _, r := range assetRows {
		if r.Account != "" {
			accounts[r.Account] = struct{}{}
		}
	}
	for _, r := range txRows {
		if r.Account != "" {
			accounts[r.Account] = struct{}{}
		}
	}

	if len(accounts) == 0 {
		return errors.New("import: no accounts found in export files")
	}

	if replaceExisting {
		for account := range accounts {
			// Delete in FK-safe order.
			if _, err := GlobalDB.Exec(`DELETE FROM market_marks WHERE account = ?;`, account); err != nil {
				return err
			}
			if _, err := GlobalDB.Exec(`DELETE FROM transactions WHERE account = ?;`, account); err != nil {
				return err
			}
			if _, err := GlobalDB.Exec(`DELETE FROM asset_lots_history WHERE account = ?;`, account); err != nil {
				return err
			}
			if _, err := GlobalDB.Exec(`DELETE FROM asset_lots WHERE account = ?;`, account); err != nil {
				return err
			}
		}
	}

	// Import assets first so transactions can reference Share Lot IDs.
	for _, r := range assetRows {
		// Upsert (keeps the CSV-provided Asset Lot ID).
		costBasisCur := r.CostBasisCurrency
		if _, err := GlobalDB.Exec(`
			INSERT OR REPLACE INTO asset_lots (
				id, account, exchange, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
		`, r.AssetLotID, r.Account, r.Exchange, r.Symbol, r.ISIN,
			getDbDecimalValue(r.SharesLeft), getDbDecimalValue(r.CostBasis), string(costBasisCur), r.DateAttained); err != nil {
			return err
		}

		if _, err := GlobalDB.Exec(`
			INSERT INTO asset_lots_history (
				id, account, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, as_of_date
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
		`, r.AssetLotID, r.Account, r.Symbol, r.ISIN,
			getDbDecimalValue(r.OriginatedShares), getDbDecimalValue(r.CostBasis), string(costBasisCur), r.DateAttained); err != nil {
			return err
		}

		if r.MarkedDate != nil && r.MarkedShares != nil && r.MarkedShareValue != nil && r.MarkedCapitalGain != nil {
			markedValueCurrency := r.MarkedShareValueCurrency
			if markedValueCurrency == "" {
				markedValueCurrency = costBasisCur
			}
			if r.MarkedCapitalGainCurrency != "" {
				markedValueCurrency = r.MarkedCapitalGainCurrency
			}
			if _, err := GlobalDB.Exec(`
				INSERT INTO market_marks (
					account, asset_lot_id, market_mark_date, marked_shares, marked_value_per_share, marked_value_currency, gain_loss
				) VALUES (?, ?, ?, ?, ?, ?, ?);
			`, r.Account, r.AssetLotID, *r.MarkedDate,
				getDbDecimalValue(r.MarkedShares), getDbDecimalValue(r.MarkedShareValue), string(markedValueCurrency), getDbDecimalValue(r.MarkedCapitalGain)); err != nil {
				return err
			}
		}
	}

	// Now import transactions.
	for _, r := range txRows {
		totalCur := r.TotalCurrency
		if _, err := GlobalDB.Exec(`
			INSERT INTO transactions (
				account, transaction_reference, transaction_type, settlement_date, symbol,
				share_lot, shares, price_per_share, share_value, fees_amount,
				total_amount, currency
			) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?);
		`, r.Account, "", int64(r.Transaction), r.DateSettled, r.Symbol,
			r.ShareLotID, int64(0), getDbDecimalValue(r.TotalAmount), string(totalCur)); err != nil {
			return err
		}
	}

	return nil
}

func ensureSupportedCurrencies() error {
	// supported_currencies.id is the CHAR(3) currency code.
	_, err := GlobalDB.Exec(`
		INSERT OR IGNORE INTO supported_currencies (id, name)
		SELECT 'USD', 'US Dollar'
		UNION ALL
		SELECT 'SEK', 'Svensk krona';
	`)
	return err
}

func ensureCurrencyRates() error {
	if err := ensureSupportedCurrencies(); err != nil {
		return err
	}

	// This table has no unique constraint, so inserts are safe (we always pick the latest as_of_date).
	_, err := GlobalDB.Exec(`
		INSERT INTO "currency_rates" (currency_code, rate, as_of_date)
		SELECT 'USD', 10000, '2022-12-31'
		UNION ALL
		SELECT 'USD', 10000, '2023-12-31'
		UNION ALL
		SELECT 'USD', 10000, '2024-12-31'
		UNION ALL
		SELECT 'SEK', 101220, '2022-12-31'
		UNION ALL
		SELECT 'SEK', 106130, '2023-12-31'
		UNION ALL
		SELECT 'SEK', 105770, '2024-12-31';
	`)
	return err
}

