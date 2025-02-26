package internal

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ericlagergren/decimal"
)

var ZeroPrecisionValue = decimal.New(0, 4)

type CurrencyUnit string

var (
	SEK = CurrencyUnit("SEK")
	USD = CurrencyUnit("USD")
)

type LocaleUnit string

var (
	SE = LocaleUnit("SE")
	US = LocaleUnit("US")
)

var ERROR_VALUE = decimal.New(-999999999999999999, 4)

var (
	ErrInvalidPrecisionValue = errors.New("value is invalid for precision value")
	ErrInvalidLocale         = errors.New("invalid locale")
)

// ProcessStringAmount returns a very clear error amount
// This is necessary as we need to really make sure mistakes
// are very transparent in any ledger we maintain
func ProcessStringAmount(amount string, locale LocaleUnit) (*decimal.Big, error) {
	var parts []string
	switch locale {
	case SE:
		parts = strings.Split(strings.Replace(amount, " ", "", -1), ",")
	case US:
		parts = strings.Split(strings.Replace(amount, " ", "", -1), ".")
	default:
		return ERROR_VALUE, ErrInvalidLocale
	}

	value := parts[0]
	if len(parts) > 1 {
		value += (parts[1] + "0000")[:4]
	} else {
		value += "0000"
	}
	val, err := strconv.Atoi(value)
	if err != nil {
		return ERROR_VALUE, ErrInvalidPrecisionValue
	}
	newValue := decimal.New(int64(val), 4)
	return newValue, nil
}

type TransactionType int

const (
	PURCHASE_TRANSACTION TransactionType = iota
	SALE_TRANSACTION
	TRANSFERIN_TRANSACTION
	TRANSFEROUT_TRANSACTION
	SPLITIN_TRANSACTION
	SPLITOUT_TRANSACTION
	DIVIDEND
	QUALIFIED_DIVIDEND
)

func (t TransactionType) String() string {
	switch t {
	case SALE_TRANSACTION:
		return "SALE_TRANSACTION"
	case PURCHASE_TRANSACTION:
		return "PURCHASE_TRANSACTION"
	case DIVIDEND:
		return "DIVIDEND"
	case QUALIFIED_DIVIDEND:
		return "QUALIFIED_DIVIDEND"
	case SPLITOUT_TRANSACTION:
		return "SPLITOUT_TRANSACTION"
	case SPLITIN_TRANSACTION:
		return "SPLITIN_TRANSACTION"
	case TRANSFERIN_TRANSACTION:
		return "TRANSFERIN_TRANSACTION"
	case TRANSFEROUT_TRANSACTION:
		return "TRANSFEROUT_TRANSACTION"
	}
	return "UNKNOWN"
}

/*
ID
Transaction Reference
Settlement Date
Lot
Transaction Type
Shares
PricePerShare
TotalValue
Currency
*/
type Transaction struct {
	ID                   int
	TransactionReference string
	TransactionType      TransactionType
	SettlementDate       time.Time

	Symbol        string
	ShareLot      string
	Shares        *decimal.Big
	PricePerShare *decimal.Big
	ShareValue    *decimal.Big

	FeesAmount *decimal.Big

	TotalAmount *decimal.Big
	Currency    CurrencyUnit
}

func (t Transaction) CopyFromShares(newShares *decimal.Big) Transaction {
	return Transaction{
		t.ID,
		t.TransactionReference,
		t.TransactionType,
		t.SettlementDate,
		t.Symbol,
		t.ShareLot,
		newShares,
		decimal.New(0, 4).Copy(t.PricePerShare),
		decimal.New(0, 4).Mul(t.PricePerShare, newShares).Quantize(4),
		decimal.New(0, 4).Copy(ZeroPrecisionValue), // remove fee to prevent duplication
		decimal.New(0, 4).Mul(t.PricePerShare, newShares).Quantize(4),
		t.Currency,
	}
}

type AssetLot struct {
	ID                string
	Symbol            string
	ISIN              string
	Shares            *decimal.Big
	CostBasisPerShare *decimal.Big
	CostBasisCurrency CurrencyUnit
	CreatedDate       time.Time
}
