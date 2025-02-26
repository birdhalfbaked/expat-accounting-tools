package internal

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/ericlagergren/decimal"
	_ "github.com/mattn/go-sqlite3"
)

var GlobalDB *sql.DB

func getDbDecimalValue(val *decimal.Big) int64 {
	uintVal, _ := val.Mantissa()
	intVal := int64(uintVal)
	if val.Cmp(ZeroPrecisionValue) == -1 {
		return -1 * intVal
	}
	return intVal
}

/**
ASSET LOT DATA ACCESS
*/
// GetAssetLotsBySymbol returns a list of AssetLots when given a symbol
func GetAssetLotBySymbol(symbol string, openOnly bool) ([]AssetLot, error) {
	sql := `
	SELECT
		id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
	FROM asset_lots
	WHERE symbol = ? AND shares > ?
	ORDER BY cost_basis_per_share DESC;
	`
	shareLimit := -1
	if openOnly {
		shareLimit = 0
	}
	rows, err := GlobalDB.Query(sql, symbol, shareLimit)
	if err != nil {
		return nil, err
	}
	var results = make([]AssetLot, 0)
	for rows.Next() {
		var shares, cost_basis_per_share int64
		var assetLot AssetLot
		err = rows.Scan(
			&assetLot.ID,
			&assetLot.Symbol,
			&assetLot.ISIN,
			&shares,
			&cost_basis_per_share,
			&assetLot.CostBasisCurrency,
			&assetLot.CreatedDate,
		)
		assetLot.Shares = decimal.New(shares, 4)
		assetLot.CostBasisPerShare = decimal.New(cost_basis_per_share, 4)
		if err != nil {
			return nil, err
		}
		results = append(results, assetLot)
	}
	return results, err
}

// GetOpenAssetLotsBySymbolBeforeDate returns a list of AssetLots when given a symbol
func GetOpenAssetLotsBySymbolBeforeDate(symbol string, date time.Time) ([]AssetLot, error) {
	sql := `
	SELECT
		id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
	FROM asset_lots
	WHERE symbol = ? AND shares > 0 AND created_date < ?
	ORDER BY cost_basis_per_share DESC;
	`
	rows, err := GlobalDB.Query(sql, symbol, date.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	var results = make([]AssetLot, 0)
	for rows.Next() {
		var shares, cost_basis_per_share int64
		var assetLot AssetLot
		err = rows.Scan(
			&assetLot.ID,
			&assetLot.Symbol,
			&assetLot.ISIN,
			&shares,
			&cost_basis_per_share,
			&assetLot.CostBasisCurrency,
			&assetLot.CreatedDate,
		)
		assetLot.Shares = decimal.New(shares, 4)
		assetLot.CostBasisPerShare = decimal.New(cost_basis_per_share, 4)
		if err != nil {
			return nil, err
		}
		results = append(results, assetLot)
	}
	return results, err
}

// GetAssetLotById returns a AssetLot when given an AssetLot id
func GetAssetLotById(id int) (AssetLot, error) {
	sql := `
	SELECT
	id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
	FROM asset_lots
	WHERE id = ?;
	`
	row := GlobalDB.QueryRow(sql, id)
	var assetLot AssetLot
	var shares, costBasisPerShare int64
	err := row.Scan(
		&assetLot.ID,
		&assetLot.Symbol,
		&assetLot.ISIN,
		&shares,
		&costBasisPerShare,
		&assetLot.CostBasisCurrency,
		&assetLot.CreatedDate,
	)
	assetLot.Shares = decimal.New(shares, 4)
	assetLot.CostBasisPerShare = decimal.New(costBasisPerShare, 4)
	if err != nil {
		assetLot.ID = ""
		return assetLot, err
	}
	return assetLot, err
}

// InsertAssetLot inserts a asset lot record and then returns the resulting id
func InsertAssetLot(assetLot AssetLot, tx *sql.Tx) (string, error) {
	sql := `
	INSERT INTO asset_lots (
		id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
	) VALUES (?, ?, ?, ?, ?, ?, ?);
	`
	derivedIdCountSQL := `
	SELECT COUNT(*)+1
	FROM asset_lots
	WHERE symbol = ?;
	`

	derivedId := assetLot.Symbol + assetLot.CreatedDate.Format("-20060102")
	var idCount int

	row := tx.QueryRow(derivedIdCountSQL, assetLot.Symbol)
	row.Scan(&idCount)
	derivedId += fmt.Sprintf("-%06d", idCount)

	immediateCommit := false
	var err error
	if tx == nil {
		immediateCommit = true
		tx, err = GlobalDB.Begin()
		if err != nil {
			return "", err
		}
	}
	var shares = getDbDecimalValue(assetLot.Shares)
	var costBasisPerShare = getDbDecimalValue(assetLot.CostBasisPerShare)
	assetLot.ID = derivedId
	_, err = tx.Exec(sql,
		derivedId, assetLot.Symbol, assetLot.ISIN, shares,
		costBasisPerShare, assetLot.CostBasisCurrency, assetLot.CreatedDate,
	)
	if err != nil {
		ErrLogger.Println(derivedId)
		return "", err
	}
	InsertAssetLotHistory(assetLot, assetLot.CreatedDate, tx)
	if immediateCommit {
		err = tx.Commit()
		if err != nil {
			return "", err
		}
	}
	return derivedId, err
}

// InsertAssetLotHistory inserts a asset lot record and then returns
func InsertAssetLotHistory(assetLot AssetLot, asOfDate time.Time, tx *sql.Tx) error {
	sql := `
	INSERT INTO asset_lots_history (
		id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, as_of_date
	) VALUES (?, ?, ?, ?, ?, ?, ?);
	`

	immediateCommit := false
	var err error
	if tx == nil {
		immediateCommit = true
		tx, err = GlobalDB.Begin()
		if err != nil {
			return err
		}
	}
	var shares = getDbDecimalValue(assetLot.Shares)
	var costBasisPerShare = getDbDecimalValue(assetLot.CostBasisPerShare)
	_, err = tx.Exec(sql,
		assetLot.ID, assetLot.Symbol, assetLot.ISIN, shares,
		costBasisPerShare, assetLot.CostBasisCurrency, asOfDate,
	)
	if err != nil {
		ErrLogger.Println(assetLot.ID)
		return err
	}
	if immediateCommit {
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	return err
}

// UpdateAssetLot updates a asset lot record
func UpdateAssetLot(assetLot AssetLot, tx *sql.Tx) error {
	sql := `
	UPDATE asset_lots SET shares=? WHERE id=?;
	`
	immediateCommit := false
	var err error
	if tx == nil {
		immediateCommit = true
		tx, err = GlobalDB.Begin()
		if err != nil {
			return err
		}
	}
	var shares, _ = assetLot.Shares.Mantissa()
	_, err = tx.Exec(sql,
		shares,
		assetLot.ID,
	)
	if err != nil {
		return err
	}
	if immediateCommit {
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	return err
}

/**
TRANSACTION DATA ACCESS
*/

// GetTransactionBetweenDate returns a transaction when given a date range
func GetTransactionBetweenDate(startDate time.Time, endDate time.Time) ([]Transaction, error) {
	sql := `
	SELECT
		id, transaction_reference, transaction_type, settlement_date,
		share_lot, shares, price_per_share, share_value, fees_amount,
		total_amount, currency
	FROM transactions
	WHERE settlement_date BETWEEN ? AND ?;
	`
	rows, err := GlobalDB.Query(sql, startDate, endDate)
	if err != nil {
		return nil, err
	}
	var transactions []Transaction
	for rows.Next() {
		var transaction Transaction
		var shares, pricePerShare, shareValue, feesAmount, totalAmount int64
		err := rows.Scan(
			&transaction.ID,
			&transaction.TransactionReference,
			&transaction.TransactionType,
			&transaction.SettlementDate,
			&transaction.Symbol,
			&transaction.ShareLot,
			&shares,
			&pricePerShare,
			&shareValue,
			&feesAmount,
			&totalAmount,
			&transaction.Currency,
		)
		transaction.Shares = decimal.New(shares, 4)
		transaction.PricePerShare = decimal.New(pricePerShare, 4)
		transaction.ShareValue = decimal.New(shareValue, 4)
		transaction.FeesAmount = decimal.New(feesAmount, 4)
		transaction.TotalAmount = decimal.New(totalAmount, 4)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, transaction)
	}
	return transactions, err
}

// GetTransactionById returns a transaction when given a transaction id
func GetTransactionById(id int) (Transaction, error) {
	sql := `
	SELECT
		id, transaction_reference, transaction_type, settlement_date, symbol,
		share_lot, shares, price_per_share, share_value, fees_amount,
		total_amount, currency
	FROM transactions
	WHERE id = ?;
	`
	row := GlobalDB.QueryRow(sql, id)
	var transaction Transaction
	var shares, pricePerShare, shareValue, feesAmount, totalAmount int64
	err := row.Scan(
		&transaction.ID,
		&transaction.TransactionReference,
		&transaction.TransactionType,
		&transaction.SettlementDate,
		&transaction.Symbol,
		&transaction.ShareLot,
		&shares,
		&pricePerShare,
		&shareValue,
		&feesAmount,
		&totalAmount,
		&transaction.Currency,
	)
	transaction.Shares = decimal.New(shares, 4)
	transaction.PricePerShare = decimal.New(pricePerShare, 4)
	transaction.ShareValue = decimal.New(shareValue, 4)
	transaction.FeesAmount = decimal.New(feesAmount, 4)
	transaction.TotalAmount = decimal.New(totalAmount, 4)
	if err != nil {
		transaction.ID = -1
		return transaction, err
	}
	return transaction, err
}

// InsertTransaction inserts a transaction record and then returns the resulting id
func InsertTransaction(transaction Transaction, tx *sql.Tx) (int, error) {
	sql := `
	INSERT INTO transactions (
		transaction_reference, transaction_type, settlement_date, symbol,
		share_lot, shares, price_per_share, share_value, fees_amount,
		total_amount, currency
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	immediateCommit := false
	var err error
	if tx == nil {
		immediateCommit = true
		tx, err = GlobalDB.Begin()
		if err != nil {
			return -1, err
		}
	}
	shares := getDbDecimalValue(transaction.Shares)
	pricePerShare := getDbDecimalValue(transaction.PricePerShare)
	shareValue := getDbDecimalValue(transaction.ShareValue)
	feesAmount := getDbDecimalValue(transaction.FeesAmount)
	totalAmount := getDbDecimalValue(transaction.TotalAmount)
	result, err := tx.Exec(sql, transaction.TransactionReference, transaction.TransactionType, transaction.SettlementDate,
		transaction.Symbol, transaction.ShareLot, shares, pricePerShare, shareValue, feesAmount,
		totalAmount, transaction.Currency)
	if err != nil {
		return -1, err
	}
	lastId, err := result.LastInsertId()
	if immediateCommit {
		err = tx.Commit()
		if err != nil {
			return -1, err
		}
	}
	return int(lastId), err
}

func UpdateRates() {
	supportedCurrenciesInsert := `
	INSERT INTO "supported_currencies" (id, name)
		SELECT "USD", "US Dollar"
		UNION ALL
		SELECT "SEK", "Svensk krona"
	`
	insertCurrencyRatesTable := `
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
		SELECT 'SEK', 105770, '2024-12-31'
	`
	tx, _ := GlobalDB.Begin()
	tx.Exec(supportedCurrenciesInsert)
	tx.Exec(insertCurrencyRatesTable)
}

/*
*

	INITIALIZATION
*/
func createTables() {
	supportedCurrenciesTable := `
	CREATE TABLE IF NOT EXISTS "supported_currencies" (
		 id                   	CHAR(3) PRIMARY KEY
		,name      				TEXT NOT NULL
	)
	`
	lotsTable := `
	CREATE TABLE IF NOT EXISTS "asset_lots" (
		 id                   	TEXT PRIMARY KEY
		,symbol				 	TEXT NOT NULL
		,isin      				TEXT NOT NULL
		,shares       			BIGINT NOT NULL

		,cost_basis_per_share   TEXT
		,cost_basis_currency   	BIGINT
		,created_date   		TIMESTAMP NOT NULL
		,FOREIGN KEY (cost_basis_currency) REFERENCES supported_currencies(id)
	)
	`
	lotsHistoryTable := `
	CREATE TABLE IF NOT EXISTS "asset_lots_history" (
		 int_id                 INTEGER PRIMARY KEY AUTOINCREMENT
		,id                   	TEXT
		,symbol				 	TEXT NOT NULL
		,isin      				TEXT NOT NULL
		,shares       			BIGINT NOT NULL

		,cost_basis_per_share   TEXT
		,cost_basis_currency   	BIGINT
		,as_of_date   			TIMESTAMP NOT NULL
		,FOREIGN KEY (cost_basis_currency) REFERENCES supported_currencies(id)
	)
	`
	transactionsTable := `
	CREATE TABLE IF NOT EXISTS "transactions" (
		 id                   	INTEGER PRIMARY KEY AUTOINCREMENT
		,transaction_reference 	TEXT
		,transaction_type      	SMALLINT NOT NULL
		,settlement_date       	TIMESTAMP NOT NULL

		,symbol					TEXT
		,share_lot      		TEXT
		,shares        			BIGINT
		,price_per_share 		BIGINT
		,share_value    		BIGINT

		,fees_amount 			BIGINT NOT NULL DEFAULT 0

		,total_amount 			BIGINT NOT NULL
		,currency    			CHAR(3) NOT NULL
		,FOREIGN KEY (share_lot) REFERENCES asset_lots(id)
		,FOREIGN KEY (currency) REFERENCES supported_currencies(id)
		)
	`
	currencyRatesTable := `
		CREATE TABLE IF NOT EXISTS "currency_rates" (
			id                   	INTEGER PRIMARY KEY AUTOINCREMENT
			,currency_code 			CHAR(3)
			,rate 		     		BIGINT -- how much to one USD
			,as_of_date    			TIMESTAMP
			,FOREIGN KEY (currency_code) REFERENCES supported_currencies(id)
		)
	`

	marketMarksTable := `
		CREATE TABLE IF NOT EXISTS "market_marks" (
			id                   		INTEGER PRIMARY KEY AUTOINCREMENT
			,asset_lot_id 				TEXT
			,market_mark_date			TIMESTAMP
			,marked_value_per_share 	BIGINT
			,marked_value_currency		CHAR(3)
			,gain_loss				 	BIGINT
			,FOREIGN KEY (asset_lot_id) REFERENCES asset_lots(id)
			,FOREIGN KEY (marked_value_currency) REFERENCES supported_currencies(id)
		)
	`

	tx, _ := GlobalDB.Begin()
	_, err := tx.Exec(supportedCurrenciesTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(lotsTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(lotsHistoryTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(transactionsTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(currencyRatesTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(marketMarksTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		ErrLogger.Fatal(err)
	}
}

func InitializeDB() {
	var err error
	var dbPath = "ledger.db"
	if flag.Lookup("test.v") != nil {
		dbPath = "test_" + dbPath
	}
	GlobalDB, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	createTables()
}
