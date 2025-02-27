package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ericlagergren/decimal"
)

var ErrNoSymbolFound = errors.New("no symbol found")

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

type StockPriceResponse struct {
	Chart struct {
		Result []struct {
			Indicators struct {
				Adjclose []struct {
					AdjClose []float64
				}
			}
		}
	}
}

type SymbolSearchResponse struct {
	Quotes []struct {
		Symbol string
	}
}

func getReq(url string) *http.Request {
	req, _ := http.NewRequest("GET", url, nil)
	req.AddCookie(&http.Cookie{Name: "_cb_svref", Value: "https%3A%2F%2Ffinance.yahoo.com%2Fquote%2FSSAB-B.ST%2F"})
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
	return req
}

func RetrieveStockPrice(assetLot AssetLot, valueDate time.Time) (*decimal.Big, error) {
	PriceUrlBase := "https://query2.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&includePrePost=true&lang=en-US&region=SE"
	SearchUrlBase := "https://query2.finance.yahoo.com/v1/finance/search?q=%s&lang=en-US&region=US"
	var symbol = assetLot.Symbol
	if assetLot.CostBasisCurrency != USD {
		// if not US, we need to do a search on ISIN
		searchData := SymbolSearchResponse{}
		resp, err := http.DefaultClient.Do(getReq(fmt.Sprintf(SearchUrlBase, assetLot.ISIN)))
		if err != nil {
			ErrLogger.Println(err)
			return nil, err
		}
		searchBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			ErrLogger.Println(err)
			return nil, err
		}
		err = json.Unmarshal(searchBytes, &searchData)
		if err != nil {
			ErrLogger.Println(err)
			return nil, err
		}
		if len(searchData.Quotes) > 0 {
			symbol = searchData.Quotes[0].Symbol
		} else {
			return nil, ErrNoSymbolFound
		}
	}

	unixTime := valueDate.Unix()
	formattedUrl := fmt.Sprintf(PriceUrlBase, symbol, unixTime, unixTime+80000)
	var stockPrice StockPriceResponse
	response, err := http.DefaultClient.Do(getReq(formattedUrl))
	if err != nil {
		ErrLogger.Println(err)
		return nil, err
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		ErrLogger.Println(err)
		return nil, err
	}
	err = json.Unmarshal(data, &stockPrice)
	if err != nil {
		ErrLogger.Println(err, string(data))
		return nil, err
	}
	stringedClose := fmt.Sprintf("%.4f", stockPrice.Chart.Result[0].Indicators.Adjclose[0].AdjClose[0])
	InfoLogger.Println(stringedClose)
	value, err := ProcessStringAmount(stringedClose, US)
	if err != nil {
		ErrLogger.Println(err)
		return nil, err
	}
	return value, err
}

func MarkStocks(date time.Time) {
	prices := make(map[string]*decimal.Big)
	prices["TRACKER GULD NORDNET"] = decimal.New(2426500, 4)
	lots, err := getUnMarkedSymbols(date)
	if err != nil {
		ErrLogger.Println(err)
		return
	}
	// start tx
	tx, err := GlobalDB.Begin()
	if err != nil {
		panic("can't do transaction")
	}
	for _, lot := range lots {
		marketPrice, ok := prices[lot.Symbol]
		if !ok {
			InfoLogger.Printf("retrieving price for %s\n", lot.Symbol)
			price, err := RetrieveStockPrice(lot, date)
			if err != nil {
				if err == ErrNoSymbolFound {
					InfoLogger.Println("Skipping non-equity")
					continue
				}
				ErrLogger.Println(err)
				panic("uh oh")
			}
			// sleep to avoid ban from
			time.Sleep(1 * time.Second)
			prices[lot.Symbol] = price
			marketPrice = price
		}
		err = MarkAssetLot(date, lot, marketPrice, tx)
		if err != nil {
			ErrLogger.Println(err)
			tx.Rollback()
			panic("uh oh")
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		panic("wtf")
	}
}
