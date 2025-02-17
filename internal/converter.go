package internal

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"sort"
	"time"

	"github.com/ericlagergren/decimal"
	"golang.org/x/text/encoding/unicode"
)

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

var (
	ErrUnhandledTransactionType = errors.New("unhandled transaction type")
	ErrValueConversionFailed    = errors.New("value conversion failed")
)

type ImportRecord struct {
	lot         AssetLot
	transaction Transaction
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

func ReadNordnetExport(filepath string) ([]ImportRecord, error) {
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
		result = append(result, transformedRecord)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].transaction.SettlementDate.Before(result[j].transaction.SettlementDate)
	})
	return result, nil
}
