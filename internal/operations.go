package internal

import (
	"github.com/ericlagergren/decimal"
)

func HandleImport(records []ImportRecord) error {
	for _, record := range records {
		switch record.transaction.TransactionType {
		case PURCHASE_TRANSACTION:
			err := handlePurchaseImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case TRANSFERIN_TRANSACTION, SPLITIN_TRANSACTION:
			err := handleTransferSplitInImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case SALE_TRANSACTION:
			err := handleSaleImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case TRANSFEROUT_TRANSACTION, SPLITOUT_TRANSACTION:
			err := handleTransferSplitOutImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		}
	}
	return nil
}

func handlePurchaseImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	lotId, err := InsertAssetLot(record.lot, tx)
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	record.lot.ID = lotId
	record.transaction.ShareLot = lotId
	InsertTransaction(record.transaction, tx)
	err = tx.Commit()
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	return nil
}

func handleTransferSplitInImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	lotId, err := InsertAssetLot(record.lot, tx)
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	record.lot.ID = lotId
	record.transaction.ShareLot = lotId
	InsertTransaction(record.transaction, tx)
	err = tx.Commit()
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	return nil
}

func handleSaleImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		return err
	}
	lots, err := GetAssetLotByISIN(record.lot.ISIN, true)
	if err != nil {
		ErrLogger.Println(err)
		return err
	}
	sharesLeft := decimal.New(0, 4).Copy(record.transaction.Shares)
	transactionsProcessed := 0
	for _, lot := range lots {
		// skip if no more shares
		if lot.Shares.Cmp(ZeroPrecisionValue) == 0 {
			continue
		}
		if sharesLeft.Cmp(ZeroPrecisionValue) == 0 {
			break
		}
		sharesProcessed := decimal.New(0, 4).Copy(decimal.Min(lot.Shares, sharesLeft))
		sharesLeft = sharesLeft.Sub(sharesLeft, sharesProcessed)
		lot.Shares = lot.Shares.Sub(lot.Shares, sharesProcessed)
		newTransaction := record.transaction.CopyFromShares(sharesProcessed)
		newTransaction.ShareLot = lot.ID
		if transactionsProcessed == 0 {
			newTransaction.FeesAmount = record.transaction.FeesAmount
			newTransaction.TotalAmount = newTransaction.TotalAmount.Add(newTransaction.TotalAmount, record.transaction.FeesAmount)
		}
		_, err := InsertTransaction(newTransaction, tx)
		if err != nil {
			ErrLogger.Println(err)
			tx.Rollback()
			return err
		}
		err = UpdateAssetLot(lot, tx)
		if err != nil {
			ErrLogger.Println(err)
		}
		transactionsProcessed++
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func handleTransferSplitOutImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		return err
	}
	lots, err := GetAssetLotByISIN(record.lot.ISIN, true)
	if err != nil {
		ErrLogger.Println(err)
		return err
	}
	sharesLeft := decimal.New(0, 4).Copy(record.transaction.Shares)
	transactionsProcessed := 0
	for _, lot := range lots {
		// skip if no more shares
		if lot.Shares.Cmp(ZeroPrecisionValue) == 0 {
			continue
		}
		if sharesLeft.Cmp(ZeroPrecisionValue) == 0 {
			break
		}
		sharesProcessed := decimal.New(0, 4).Copy(decimal.Min(lot.Shares, sharesLeft))
		sharesLeft = sharesLeft.Sub(sharesLeft, sharesProcessed)
		lot.Shares = lot.Shares.Sub(lot.Shares, sharesProcessed)
		newTransaction := record.transaction.CopyFromShares(sharesProcessed)
		newTransaction.ShareLot = lot.ID
		_, err := InsertTransaction(newTransaction, tx)
		if err != nil {
			ErrLogger.Println(err)
			tx.Rollback()
			return err
		}
		err = UpdateAssetLot(lot, tx)
		if err != nil {
			ErrLogger.Println(err)
		}
		transactionsProcessed++
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
