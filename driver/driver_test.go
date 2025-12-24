package driver_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/driver"
)

func TestTypString(t *testing.T) {
	tt := zlsgo.NewTest(t)

	tt.Equal("MySQL", driver.MySQL.String())
	tt.Equal("PostgreSQL", driver.PostgreSQL.String())
	tt.Equal("SQLite", driver.SQLite.String())
	tt.Equal("MsSQL", driver.MsSQL.String())
	tt.Equal("ClickHouse", driver.ClickHouse.String())
	tt.Equal("Doris", driver.Doris.String())
	tt.Equal("<invalid>", driver.Typ(0).String())
}

func TestTypQuote(t *testing.T) {
	tt := zlsgo.NewTest(t)

	tt.Equal("*", driver.MySQL.Quote("*"))
	tt.Equal("(SELECT 1)", driver.MySQL.Quote("(SELECT 1)"))
	tt.Equal("`name`", driver.MySQL.Quote("name"))
	tt.Equal(`"name"`, driver.PostgreSQL.Quote("name"))
	tt.Equal(`"name"`, driver.SQLite.Quote("name"))
	tt.Equal(`"name"`, driver.MsSQL.Quote("name"))

	tt.Equal("`table`.`column`", driver.MySQL.Quote("table.column"))
	tt.Equal(`"table"."column"`, driver.PostgreSQL.Quote("table.column"))

	tt.Equal("`table`.`column` alias", driver.MySQL.Quote("table.column alias"))

	tt.Equal("`table`.*", driver.MySQL.Quote("table.*"))

	tt.Equal("count(*)", driver.MySQL.Quote("count(*)"))
}

func TestTypQuoteCols(t *testing.T) {
	tt := zlsgo.NewTest(t)

	cols := []string{"id", "name", "age"}
	quoted := driver.MySQL.QuoteCols(cols)
	tt.Equal(3, len(quoted))
	tt.Equal("`id`", quoted[0])
	tt.Equal("`name`", quoted[1])
	tt.Equal("`age`", quoted[2])

	empty := driver.MySQL.QuoteCols([]string{})
	tt.Equal(0, len(empty))
}

func TestDorisQuote(t *testing.T) {
	tt := zlsgo.NewTest(t)

	tt.Equal("`name`", driver.Doris.Quote("name"))
	tt.Equal("`table`.`column`", driver.Doris.Quote("table.column"))
}
