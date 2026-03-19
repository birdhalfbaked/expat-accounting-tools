package internal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
		ErrLogger.Fatal(err, record)
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
	yahooSymbol, err := ResolveYahooSymbol(assetLot)
	if err != nil {
		return nil, err
	}
	return RetrieveStockPriceByYahooSymbol(yahooSymbol, valueDate)
}

func ResolveYahooSymbol(assetLot AssetLot) (string, error) {
	SearchUrlBase := "https://query2.finance.yahoo.com/v1/finance/search?q=%s&lang=en-US&region=US"
	symbol := assetLot.Symbol
	if assetLot.CostBasisCurrency != USD {
		// If not US, do a search using ISIN.
		searchData := SymbolSearchResponse{}
		resp, err := http.DefaultClient.Do(getReq(fmt.Sprintf(SearchUrlBase, assetLot.ISIN)))
		if err != nil {
			ErrLogger.Println(err)
			return "", err
		}
		searchBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			ErrLogger.Println(err)
			return "", err
		}
		if err := json.Unmarshal(searchBytes, &searchData); err != nil {
			ErrLogger.Println(err)
			return "", err
		}
		if len(searchData.Quotes) > 0 && strings.TrimSpace(searchData.Quotes[0].Symbol) != "" {
			symbol = searchData.Quotes[0].Symbol
		} else {
			return "", ErrNoSymbolFound
		}
	}
	return symbol, nil
}

func RetrieveStockPriceByYahooSymbol(yahooSymbol string, valueDate time.Time) (*decimal.Big, error) {
	PriceUrlBase := "https://query2.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&includePrePost=true&lang=en-US&region=SE"
	unixTime := valueDate.Unix()
	formattedUrl := fmt.Sprintf(PriceUrlBase, yahooSymbol, unixTime, unixTime+80000)
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
	if err := json.Unmarshal(data, &stockPrice); err != nil {
		ErrLogger.Println(err, string(data))
		return nil, err
	}

	// Yahoo may return empty chart results for non-equities / unknown symbols.
	if len(stockPrice.Chart.Result) == 0 {
		return nil, ErrNoSymbolFound
	}
	first := stockPrice.Chart.Result[0]
	if len(first.Indicators.Adjclose) == 0 || len(first.Indicators.Adjclose[0].AdjClose) == 0 {
		return nil, ErrNoSymbolFound
	}

	stringedClose := fmt.Sprintf("%.4f", first.Indicators.Adjclose[0].AdjClose[0])
	InfoLogger.Println(stringedClose)
	value, err := ProcessStringAmount(stringedClose, US)
	if err != nil {
		ErrLogger.Println(err)
		return nil, err
	}
	return value, nil
}

func MarkStocks(date time.Time) {
	// Cache by resolved Yahoo symbol so different lot symbols that map to the same Yahoo symbol
	// don't trigger repeated HTTP requests.
	prices := make(map[string]*decimal.Big)
	nonEquitySymbols := make(map[string]bool)
	reader := bufio.NewReader(os.Stdin)
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
		resolveKey := lot.Symbol
		if lot.CostBasisCurrency != USD {
			resolveKey = lot.ISIN
		}
		if nonEquitySymbols[resolveKey] {
			continue
		}

		// If we already have a (manual or cached) price, don't resolve / fetch again.
		if cached, ok := prices[resolveKey]; ok {
			if err := MarkAssetLot(date, lot, cached, tx); err != nil {
				ErrLogger.Println(err)
				tx.Rollback()
				return
			}
			continue
		}

		// Hardcoded special case from earlier behavior.
		if lot.Symbol == "TRACKER GULD NORDNET" {
			marketPrice := decimal.New(2426500, 4)
			if err := MarkAssetLot(date, lot, marketPrice, tx); err != nil {
				ErrLogger.Println(err)
				tx.Rollback()
				return
			}
			continue
		}

		yahooSymbol, err := ResolveYahooSymbol(lot)
		if err != nil {
			if err == ErrNoSymbolFound {
				manualPrice, promptErr := promptManualPrice(lot, date, reader, "symbol not found from Yahoo")
				if promptErr != nil {
					ErrLogger.Println(promptErr)
					tx.Rollback()
					return
				}
				if manualPrice == nil {
					nonEquitySymbols[resolveKey] = true
					InfoLogger.Println("Skipping non-equity")
					continue
				}
				// Cache manual answer.
				prices[resolveKey] = manualPrice
				// If we can't resolve a Yahoo symbol, we can't populate yahooSymbol-based caches.
				if err := MarkAssetLot(date, lot, manualPrice, tx); err != nil {
					ErrLogger.Println(err)
					tx.Rollback()
					return
				}
				continue
			}
			// For other errors (network/JSON/etc) we still offer a manual override.
			manualPrice, promptErr := promptManualPrice(lot, date, reader, err.Error())
			if promptErr != nil {
				ErrLogger.Println(promptErr)
				tx.Rollback()
				return
			}
			if manualPrice == nil {
				nonEquitySymbols[resolveKey] = true
				ErrLogger.Println("Skipping due to Yahoo resolution error (manual price blank)")
				continue
			}
			prices[resolveKey] = manualPrice
			if err := MarkAssetLot(date, lot, manualPrice, tx); err != nil {
				ErrLogger.Println(err)
				tx.Rollback()
				return
			}
			continue
		}

		if nonEquitySymbols[yahooSymbol] {
			continue
		}

		marketPrice, ok := prices[yahooSymbol]
		if !ok {
			InfoLogger.Printf("retrieving price for %s (%s)\n", lot.Symbol, yahooSymbol)
			price, err := RetrieveStockPriceByYahooSymbol(yahooSymbol, date)
			if err != nil {
				if err == ErrNoSymbolFound {
					manualPrice, promptErr := promptManualPrice(lot, date, reader, "price not found from Yahoo")
					if promptErr != nil {
						ErrLogger.Println(promptErr)
						tx.Rollback()
						return
					}
					if manualPrice == nil {
						nonEquitySymbols[resolveKey] = true
						nonEquitySymbols[yahooSymbol] = true
						InfoLogger.Println("Skipping non-equity")
						continue
					}
					prices[resolveKey] = manualPrice
					prices[yahooSymbol] = manualPrice
					marketPrice = manualPrice
				} else {
					// Other Yahoo errors: prompt for a manual override.
					manualPrice, promptErr := promptManualPrice(lot, date, reader, err.Error())
					if promptErr != nil {
						ErrLogger.Println(promptErr)
						tx.Rollback()
						return
					}
					if manualPrice == nil {
						nonEquitySymbols[resolveKey] = true
						nonEquitySymbols[yahooSymbol] = true
						ErrLogger.Println("Skipping due to Yahoo price error (manual price blank)")
						continue
					}
					prices[resolveKey] = manualPrice
					prices[yahooSymbol] = manualPrice
					marketPrice = manualPrice
				}
			} else {
				// sleep to avoid ban from
				time.Sleep(1 * time.Second)
				prices[yahooSymbol] = price
				prices[resolveKey] = price
				marketPrice = price
			}
		} else {
			// Cached by yahooSymbol, also store under resolveKey for consistent future hits.
			prices[resolveKey] = marketPrice
		}

		err = MarkAssetLot(date, lot, marketPrice, tx)
		if err != nil {
			ErrLogger.Println(err)
			tx.Rollback()
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return
	}
}

func promptManualPrice(assetLot AssetLot, date time.Time, reader *bufio.Reader, reason string) (*decimal.Big, error) {
	example := "0.00"
	if assetLot.CostBasisCurrency == SEK {
		example = "0,00"
	}

	for {
		if reason != "" {
			fmt.Printf("Yahoo price unavailable for %s (ISIN=%s, %s). Enter manual price per share in %s (format like %s; blank to skip): ",
				assetLot.Symbol, assetLot.ISIN, reason, assetLot.CostBasisCurrency, example)
		} else {
			fmt.Printf("Yahoo price not found for %s (ISIN=%s). Enter manual price per share in %s (format like %s; blank to skip): ",
				assetLot.Symbol, assetLot.ISIN, assetLot.CostBasisCurrency, example)
		}
		text, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return nil, nil
		}

		val, err := parseReportDecimal(text, assetLot.CostBasisCurrency)
		if err != nil {
			fmt.Printf("Invalid number. %v\n", err)
			continue
		}
		return val, nil
	}
}
