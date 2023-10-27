package builder

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zsync"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/postgres"
)

func TestCond(t *testing.T) {
	tt := zlsgo.NewTest(t)
	tests := map[string]func() string{
		"$0":                        func() string { return newCond(&mysql.Config{}, false).Var(1) },
		"`a` = $0":                  func() string { return newCond(&mysql.Config{}, false).EQ("a", 1) },
		`"$$a" = $0`:                func() string { return newCond(&postgres.Config{}, false).EQ("$a", 1) },
		"`a` <> $0":                 func() string { return newCond(&mysql.Config{}, false).NE("a", 1) },
		"`a` > $0":                  func() string { return newCond(&mysql.Config{}, false).GT("a", 1) },
		"`a` >= $0":                 func() string { return newCond(&mysql.Config{}, false).GE("a", 1) },
		"`a` < $0":                  func() string { return newCond(&mysql.Config{}, false).LT("a", 1) },
		"`a` <= $0":                 func() string { return newCond(&mysql.Config{}, false).LE("a", 1) },
		"`a` IN ($0, $1, $2)":       func() string { return newCond(&mysql.Config{}, false).In("a", 1, 2, "3") },
		"`a` NOT IN ($0, $1, $2)":   func() string { return newCond(&mysql.Config{}, false).NotIn("a", 1, 2, "3") },
		"`a` LIKE $0":               func() string { return newCond(&mysql.Config{}, false).Like("a", "3") },
		"`a` NOT LIKE $0":           func() string { return newCond(&mysql.Config{}, false).NotLike("a", "3") },
		"`a` IS NULL":               func() string { return newCond(&mysql.Config{}, false).IsNull("a") },
		"`a` IS NOT NULL":           func() string { return newCond(&mysql.Config{}, false).IsNotNull("a") },
		"`a` BETWEEN $0 AND $1":     func() string { return newCond(&mysql.Config{}, false).Between("a", 1, 2) },
		"`a` NOT BETWEEN $0 AND $1": func() string { return newCond(&mysql.Config{}, false).NotBetween("a", 1, 2) },
		"(1=2 AND 3=4)":             func() string { return newCond(&mysql.Config{}, false).And("1=2", "3=4") },
		"(1=2 OR 3=4)":              func() string { return newCond(&mysql.Config{}, false).Or("1=2", "3=4") },
	}
	var wg zsync.WaitGroup
	for i := range tests {
		expected := i
		fn := tests[expected]
		wg.Go(func() {
			tt.Equal(expected, fn())
		})
	}
	wg.Wait()
}
