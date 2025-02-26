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
		case TRANSFERIN_TRANSACTION:
			err := handleTransferInImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case SPLITIN_TRANSACTION:
			err := handleSplitInImport(record)
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
		case TRANSFEROUT_TRANSACTION:
			err := handleTransferOutImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case SPLITOUT_TRANSACTION:
			err := handleSplitOutImport(record)
			if err != nil {
				ErrLogger.Println(err)
				return err
			}
		case DIVIDEND:
			_, err := InsertTransaction(record.transaction, nil)
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

func handleSplitInImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		ErrLogger.Fatal(err)
		return err
	}
	var newTotalCostBasis = decimal.New(0, 4)
	beforeAssetLots, err := GetOpenAssetLotsBySymbolBeforeDate(record.transaction.Symbol, record.transaction.SettlementDate)
	if err != nil {
		return err
	}
	for _, lot := range beforeAssetLots {
		newTotalCostBasis = newTotalCostBasis.Add(newTotalCostBasis, lot.CostBasisPerShare.Mul(lot.CostBasisPerShare, lot.Shares).Quantize(4))
	}
	record.lot.CostBasisPerShare = newTotalCostBasis.Quo(newTotalCostBasis, record.lot.Shares).Quantize(4)
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

func handleTransferInImport(record ImportRecord) error {
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
	lots, err := GetAssetLotBySymbol(record.lot.Symbol, true)
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
		err = InsertAssetLotHistory(lot, record.transaction.SettlementDate, tx)
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

func handleSplitOutImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		return err
	}
	lots, err := GetOpenAssetLotsBySymbolBeforeDate(record.lot.Symbol, record.transaction.SettlementDate)
	if err != nil {
		ErrLogger.Println(err)
		return err
	}
	for _, lot := range lots {
		lot.Shares = ZeroPrecisionValue
		err = UpdateAssetLot(lot, tx)
		if err != nil {
			return err
		}
		err = InsertAssetLotHistory(lot, record.transaction.SettlementDate, tx)
		if err != nil {
			ErrLogger.Println(err)
		}
	}
	err = tx.Commit()
	return err
}

func handleTransferOutImport(record ImportRecord) error {
	tx, err := GlobalDB.Begin()
	if err != nil {
		return err
	}
	lots, err := GetAssetLotBySymbol(record.lot.Symbol, true)
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
		err = InsertAssetLotHistory(lot, record.transaction.SettlementDate, tx)
		if err != nil {
			ErrLogger.Println(err)
		}
		transactionsProcessed++
	}
	err = tx.Commit()
	return err
}
