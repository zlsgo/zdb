package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
)

func TestCond(t *testing.T) {
	tt := zlsgo.NewTest(t)

	tests := map[string]func() string{
		"$0":                      func() string { return builder.NewCond().Var(1) },
		"a = $0":                  func() string { return builder.NewCond().EQ("a", 1) },
		"$$a = $0":                func() string { return builder.NewCond().EQ("$a", 1) },
		"a <> $0":                 func() string { return builder.NewCond().NotEQ("a", 1) },
		"a > $0":                  func() string { return builder.NewCond().GT("a", 1) },
		"a >= $0":                 func() string { return builder.NewCond().GE("a", 1) },
		"a < $0":                  func() string { return builder.NewCond().LT("a", 1) },
		"a <= $0":                 func() string { return builder.NewCond().LE("a", 1) },
		"a IN ($0, $1, $2)":       func() string { return builder.NewCond().In("a", 1, 2, "3") },
		"a NOT IN ($0, $1, $2)":   func() string { return builder.NewCond().NotIn("a", 1, 2, "3") },
		"a LIKE $0":               func() string { return builder.NewCond().Like("a", "3") },
		"a NOT LIKE $0":           func() string { return builder.NewCond().NotLike("a", "3") },
		"a IS NULL":               func() string { return builder.NewCond().IsNull("a") },
		"a IS NOT NULL":           func() string { return builder.NewCond().IsNotNull("a") },
		"a BETWEEN $0 AND $1":     func() string { return builder.NewCond().Between("a", 1, 2) },
		"a NOT BETWEEN $0 AND $1": func() string { return builder.NewCond().NotBetween("a", 1, 2) },
		"(1=2 AND 3=4)":           func() string { return builder.NewCond().And("1=2", "3=4") },
		"(1=2 OR 3=4)":            func() string { return builder.NewCond().Or("1=2", "3=4") },
	}
	for expected, fn := range tests {
		tt.Equal(expected, fn())
	}
}
