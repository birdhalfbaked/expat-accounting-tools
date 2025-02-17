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

/**
ASSET LOT DATA ACCESS
*/
// GetAssetLotsByISIN returns a list of AssetLots when given an ISIN
func GetAssetLotByISIN(isin string, openOnly bool) ([]AssetLot, error) {
	sql := `
	SELECT
		id, symbol, isin, shares, cost_basis_per_share, cost_basis_currency, created_date
	FROM asset_lots
	WHERE isin = ? AND shares > ?
	ORDER BY cost_basis_per_share DESC;
	`
	shareLimit := -1
	if openOnly {
		shareLimit = 0
	}
	rows, err := GlobalDB.Query(sql, isin, shareLimit)
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
	WHERE isin = ?;
	`

	derivedId := assetLot.ISIN + assetLot.CreatedDate.Format("-20060102")
	var idCount int

	row := tx.QueryRow(derivedIdCountSQL, assetLot.ISIN)
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
	var shares, _ = assetLot.Shares.Mantissa()
	var costBasisPerShare, _ = assetLot.CostBasisPerShare.Mantissa()
	_, err = tx.Exec(sql,
		derivedId, assetLot.Symbol, assetLot.ISIN, shares,
		costBasisPerShare, assetLot.CostBasisCurrency, assetLot.CreatedDate,
	)
	if err != nil {
		ErrLogger.Println(derivedId)
		return "", err
	}
	if immediateCommit {
		err = tx.Commit()
		if err != nil {
			return "", err
		}
	}
	return derivedId, err
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
		id, transaction_reference, transaction_type, settlement_date,
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
		transaction_reference, transaction_type, settlement_date,
		share_lot, shares, price_per_share, share_value, fees_amount,
		total_amount, currency
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
	shares, _ := transaction.Shares.Mantissa()
	pricePerShare, _ := transaction.PricePerShare.Mantissa()
	shareValue, _ := transaction.ShareValue.Mantissa()
	feesAmount, _ := transaction.FeesAmount.Mantissa()
	totalAmount, _ := transaction.TotalAmount.Mantissa()
	result, err := tx.Exec(sql, transaction.TransactionReference, transaction.TransactionType, transaction.SettlementDate,
		transaction.ShareLot, shares, pricePerShare, shareValue, feesAmount,
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

/*
*

	INITIALIZATION
*/
func createTables() {
	lotsTable := `
	CREATE TABLE IF NOT EXISTS "asset_lots" (
		 id                   	TEXT PRIMARY KEY
		,symbol				 	TEXT NOT NULL
		,isin      				TEXT NOT NULL
		,shares       			BIGINT NOT NULL

		,cost_basis_per_share   TEXT
		,cost_basis_currency   	BIGINT
		,created_date   		TIMESTAMP NOT NULL
	)
	`
	transactionsTable := `
	CREATE TABLE IF NOT EXISTS "transactions" (
		 id                   	INTEGER PRIMARY KEY AUTOINCREMENT
		,transaction_reference 	TEXT
		,transaction_type      	SMALLINT NOT NULL
		,settlement_date       	TIMESTAMP NOT NULL

		,share_lot      		TEXT
		,shares        			BIGINT
		,price_per_share 		BIGINT
		,share_value    		BIGINT

		,fees_amount 			BIGINT NOT NULL DEFAULT 0

		,total_amount 			BIGINT NOT NULL
		,currency    			CHAR(3) NOT NULL
		,FOREIGN KEY (share_lot) REFERENCES asset_lots(id)
	)
	`

	tx, _ := GlobalDB.Begin()
	_, err := tx.Exec(lotsTable)
	if err != nil {
		ErrLogger.Fatal(err)
	}
	_, err = tx.Exec(transactionsTable)
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
