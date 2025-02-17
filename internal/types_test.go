package internal_test

import (
	"accounting/internal"
	"testing"

	"github.com/ericlagergren/decimal"
)

func TestProcessStringAmount(t *testing.T) {
	type TestArg struct {
		amount   string
		currency internal.LocaleUnit
	}
	type TestResult struct {
		amount *decimal.Big
		err    error
	}
	var args []TestArg = []TestArg{
		{"100.10", internal.US},
		{"234,1200", internal.SE},
		{"1010.0001", internal.US},
		{"0.0001", internal.US},
		{"1010.0001", internal.SE},
		{"1010.0001", "DK"},
	}
	var results []TestResult = []TestResult{
		{decimal.New(1001000, 4), nil},
		{decimal.New(2341200, 4), nil},
		{decimal.New(10100001, 4), nil},
		{decimal.New(1, 4), nil},
		{internal.ERROR_VALUE, internal.ErrInvalidPrecisionValue},
		{internal.ERROR_VALUE, internal.ErrInvalidLocale},
	}
	for i, v := range args {
		r, e := internal.ProcessStringAmount(v.amount, v.currency)
		var result = TestResult{r, e}
		if result.amount.Cmp(results[i].amount) != 0 || result.err != results[i].err {
			t.Errorf("Failed test %d", i)
		}
	}
}
