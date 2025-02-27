package internal

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ericlagergren/decimal"
	"golang.org/x/text/encoding/unicode"
)

var (
	ErrUnhandledTransactionType = errors.New("unhandled transaction type")
	ErrValueConversionFailed    = errors.New("value conversion failed")
)

type ImportRecord struct {
	lot         AssetLot
	transaction Transaction
}

/*
Id
Bokföringsdag
Affärsdag
Likviddag
Depå
Transaktionstyp
Värdepapper
ISIN
Antal
Kurs
Ränta
Total Avgift
Valuta
Belopp
Valuta
Inköpsvärde
Valuta
Resultat
Valuta
Totalt antal
Saldo
Växlingskurs
Transaktionstext
Makuleringsdatum
Notanummer
Verifikationsnummer
Courtage
Valuta
Referensvalutakurs
Initial låneränta
*/

type NordnetTransaction struct {
	Id                  string
	Bokföringsdag       string
	Affärsdag           string
	Likviddag           string
	Depå                string
	Transaktionstyp     string
	Värdepapper         string
	ISIN                string
	Antal               string
	Kurs                string
	Ränta               string
	TotalAvgift         string
	TotalAvgiftValuta   string
	Belopp              string
	BeloppValuta        string
	Inköpsvärde         string
	InköpsvärdeValuta   string
	Resultat            string
	ResultatValuta      string
	TotaltAntal         string
	Saldo               string
	Växlingskurs        string
	Transaktionstext    string
	Makuleringsdatum    string
	Notanummer          string
	Verifikationsnummer string
	Courtage            string
	CourtageValuta      string
	Referensvalutakurs  string
	InitialLåneränta    string
}

func TransformNordnetTransaction(transaction NordnetTransaction) (ImportRecord, error) {
	var result = ImportRecord{}
	var transactionType TransactionType
	switch transaction.Transaktionstyp {
	case "KÖPT":
		transactionType = PURCHASE_TRANSACTION
	case "SÅLT":
		transactionType = SALE_TRANSACTION
	case "BYTE INLÄGG VP":
		transactionType = TRANSFERIN_TRANSACTION
	case "BYTE UTTAG VP":
		transactionType = TRANSFEROUT_TRANSACTION
	case "SPLIT INLÄGG VP":
		transactionType = SPLITIN_TRANSACTION
	case "SPLIT UTTAG VP":
		transactionType = SPLITOUT_TRANSACTION
	case "UTDELNING":
		transactionType = DIVIDEND
	default:
		return result, ErrUnhandledTransactionType
	}
	shares, err := ProcessStringAmount(transaction.Antal, SE)
	if err != nil {
		if transaction.Antal == "" {
			shares, _ = ProcessStringAmount("0", SE)
		} else {
			ErrLogger.Printf("failed to process shares with value: %s %s\n", transaction.Transaktionstyp, transaction.Antal)
			return result, ErrValueConversionFailed
		}
	}
	var pricePerShare *decimal.Big
	if transactionType == SPLITIN_TRANSACTION || transactionType == TRANSFERIN_TRANSACTION {
		pricePerShare, err = ProcessStringAmount(transaction.Inköpsvärde, SE)
		if err != nil {
			if transaction.Kurs == "" {
				pricePerShare, _ = ProcessStringAmount("0", SE)
			} else {
				ErrLogger.Println("failed to process pricePerShare")
				return result, ErrValueConversionFailed
			}
		}
		pricePerShare = pricePerShare.Quo(pricePerShare, shares).Quantize(4)
	} else {
		pricePerShare, err = ProcessStringAmount(transaction.Kurs, SE)
		if err != nil {
			if transaction.Kurs == "" {
				pricePerShare, _ = ProcessStringAmount("0", SE)
			} else {
				ErrLogger.Println("failed to process pricePerShare")
				return result, ErrValueConversionFailed
			}
		}
	}
	feeAmount, err := ProcessStringAmount(transaction.TotalAvgift, SE)
	if err != nil {
		if transaction.TotalAvgift == "" {
			feeAmount, _ = ProcessStringAmount("0", SE)
		} else {
			ErrLogger.Printf("failed to process feeAmount %s \n", transaction.TotalAvgift)
			return result, ErrValueConversionFailed
		}
	}

	settlementDate, err := time.Parse(time.DateOnly, transaction.Likviddag)
	if err != nil {
		ErrLogger.Println("failed to process settlementDate")
		return result, ErrValueConversionFailed
	}
	var shareValue = decimal.New(0, 4)
	shareValue.Mul(pricePerShare, shares).Quantize(4)

	var mappedAssetLot = AssetLot{
		ID:                "",
		Symbol:            transaction.Värdepapper,
		ISIN:              transaction.ISIN,
		Shares:            shares,
		CostBasisPerShare: pricePerShare,
		CostBasisCurrency: SEK,
		CreatedDate:       settlementDate,
	}
	var mappedTransaction = Transaction{
		ID:                   -1,
		TransactionReference: transaction.Id,
		TransactionType:      transactionType,
		SettlementDate:       settlementDate,

		Symbol:        transaction.Värdepapper,
		ShareLot:      "",
		Shares:        shares,
		PricePerShare: pricePerShare,
		ShareValue:    shareValue,

		FeesAmount: feeAmount,

		TotalAmount: shareValue.Sub(shareValue, feeAmount),
		Currency:    SEK,
	}
	if transactionType == TRANSFERIN_TRANSACTION || transactionType == SPLITIN_TRANSACTION || transactionType == TRANSFEROUT_TRANSACTION || transactionType == SPLITOUT_TRANSACTION {
		mappedTransaction.TotalAmount = decimal.New(0, 4)
		mappedTransaction.ShareValue = decimal.New(0, 4)
	}
	return ImportRecord{lot: mappedAssetLot, transaction: mappedTransaction}, nil
}

func ReadNordnetExport(filepath string, accountNumber string) ([]ImportRecord, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	encoding := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)
	decodedFile := encoding.NewDecoder().Reader(file)
	reader := csv.NewReader(decodedFile)
	reader.Comma = '\t'
	reader.Read() // toss header
	result := make([]ImportRecord, 0)
	var record []string
	for {
		record, err = reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		transformedRecord, err := TransformNordnetTransaction(NordnetTransaction{
			record[0], record[1], record[2], record[3], record[4], record[5],
			record[6], record[7], record[8], record[9], record[10], record[11],
			record[12], record[13], record[14], record[15], record[16], record[17],
			record[18], record[19], record[20], record[21], record[22], record[23],
			record[24], record[25], record[26], record[27], record[28], record[29],
		})
		if err == ErrUnhandledTransactionType {
			continue
		}
		if err != nil {
			return nil, err
		}
		transformedRecord.lot.AccountID = accountNumber
		transformedRecord.transaction.AccountID = accountNumber
		result = append(result, transformedRecord)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].transaction.SettlementDate.Before(result[j].transaction.SettlementDate)
	})
	return result, nil
}

type ETradeTransaction struct {
	TransactionDate string
	TransactionType string
	SecurityType    string
	Symbol          string
	Quantity        string
	Amount          string
	Price           string
	Commission      string
	Description     string
}

func TransformETradeTransaction(transaction ETradeTransaction) (ImportRecord, error) {
	var result = ImportRecord{}
	var transactionType TransactionType

	switch transaction.TransactionType {
	case "Bought":
		transactionType = PURCHASE_TRANSACTION
	case "Sold":
		transactionType = SALE_TRANSACTION
	case "SplitIn":
		transactionType = SPLITIN_TRANSACTION
	case "SplitOut":
		transactionType = SPLITOUT_TRANSACTION
	case "TransferIn":
		transactionType = TRANSFERIN_TRANSACTION
	case "TransferOut":
		transactionType = TRANSFEROUT_TRANSACTION
	case "Dividend":
		transactionType = DIVIDEND
	case "Qualified Dividend":
		transactionType = QUALIFIED_DIVIDEND
	default:
		return result, ErrUnhandledTransactionType
	}
	shares, err := ProcessStringAmount(transaction.Quantity, US)
	if err != nil {
		if transaction.Quantity == "" {
			shares, _ = ProcessStringAmount("0", US)
		} else {
			ErrLogger.Printf("failed to process shares with value: %s %s\n", transaction.TransactionType, transaction.Quantity)
			return result, ErrValueConversionFailed
		}
	}
	if transactionType == SPLITOUT_TRANSACTION || transactionType == TRANSFEROUT_TRANSACTION {
		shares = shares.Neg(shares)
	}
	var pricePerShare *decimal.Big
	if transactionType == SPLITIN_TRANSACTION || transactionType == TRANSFERIN_TRANSACTION {
		pricePerShare, err = ProcessStringAmount(transaction.Amount, US)
		if err != nil {
			if transaction.Price == "" {
				pricePerShare, _ = ProcessStringAmount("0", US)
			} else {
				ErrLogger.Println("failed to process pricePerShare")
				return result, ErrValueConversionFailed
			}
		}
		pricePerShare = pricePerShare.Quo(pricePerShare, shares).Quantize(4)
	} else {
		pricePerShare, err = ProcessStringAmount(transaction.Price, US)
		if err != nil {
			if transaction.Price == "" {
				pricePerShare, _ = ProcessStringAmount("0", US)
			} else {
				ErrLogger.Println("failed to process pricePerShare")
				return result, ErrValueConversionFailed
			}
		}
	}
	feeAmount, err := ProcessStringAmount(transaction.Commission, US)
	if err != nil {
		if transaction.Commission == "" {
			feeAmount, _ = ProcessStringAmount("0", SE)
		} else {
			ErrLogger.Printf("failed to process feeAmount %s \n", transaction.Commission)
			return result, ErrValueConversionFailed
		}
	}
	settlementDate, err := time.Parse("01/02/06", transaction.TransactionDate)
	if err != nil {
		ErrLogger.Println("failed to process settlementDate")
		return result, ErrValueConversionFailed
	}
	var shareValue = decimal.New(0, 4)
	shareValue.Mul(pricePerShare, shares).Quantize(4)

	var mappedAssetLot = AssetLot{
		ID:                "",
		Symbol:            transaction.Symbol,
		ISIN:              "",
		Shares:            shares,
		CostBasisPerShare: pricePerShare,
		CostBasisCurrency: USD,
		CreatedDate:       settlementDate,
	}
	var mappedTransaction = Transaction{
		ID:                   -1,
		TransactionReference: "NOREF",
		TransactionType:      transactionType,
		SettlementDate:       settlementDate,

		Symbol:        transaction.Symbol,
		ShareLot:      "",
		Shares:        shares,
		PricePerShare: pricePerShare,
		ShareValue:    shareValue,

		FeesAmount: feeAmount,

		TotalAmount: shareValue.Sub(shareValue, feeAmount),
		Currency:    USD,
	}
	if transactionType == TRANSFERIN_TRANSACTION || transactionType == SPLITIN_TRANSACTION || transactionType == TRANSFEROUT_TRANSACTION || transactionType == SPLITOUT_TRANSACTION {
		mappedTransaction.TotalAmount = decimal.New(0, 4)
		mappedTransaction.ShareValue = decimal.New(0, 4)
	}
	if transactionType == DIVIDEND {
		mappedTransaction.TotalAmount, err = ProcessStringAmount(transaction.Amount, US)
		if err != nil {
			ErrLogger.Printf("failed to process divident amount: %s", transaction.Amount)
			return result, ErrValueConversionFailed
		}
	}
	return ImportRecord{lot: mappedAssetLot, transaction: mappedTransaction}, nil
}

func ReadETradeExport(filepath string, accountNumber string) ([]ImportRecord, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(file)
	reader.Comma = ','
	reader.Read() // toss header
	result := make([]ImportRecord, 0)
	var record []string
	for {
		record, err = reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		transformedRecord, err := TransformETradeTransaction(ETradeTransaction{
			record[0], record[1], record[2], record[3], record[4], record[5],
			record[6], record[7], record[8],
		})
		if err == ErrUnhandledTransactionType {
			continue
		}
		if err != nil {
			return nil, err
		}
		transformedRecord.lot.AccountID = accountNumber
		transformedRecord.transaction.AccountID = accountNumber
		result = append(result, transformedRecord)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].transaction.SettlementDate.Equal(result[j].transaction.SettlementDate) {
			return strings.Contains(result[i].transaction.TransactionType.String(), "IN")
		}
		return result[i].transaction.SettlementDate.Before(result[j].transaction.SettlementDate)
	})
	return result, nil
}
