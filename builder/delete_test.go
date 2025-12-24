package builder_test

import (
	"strings"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func TestDelete(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&mysql.Config{})

	d.Where(
		d.Cond.EQ("id", 108),
		"age >"+d.Cond.Var(88),
	)

	d.OrderBy("id")
	d.Asc()

	sql, values, err := d.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("DELETE FROM `user` WHERE `id` = ? AND age >? ORDER BY `id` ASC", sql)
	tt.Equal([]interface{}{108, 88}, values)
}

func TestDeleteLimitRequiresLimitBy(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&sqlite3.Config{})
	d.Where(d.Cond.EQ("id", 1))
	d.Limit(1)

	_, _, err := d.Build()
	tt.EqualTrue(err != nil)
}

func TestDeleteDesc(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&mysql.Config{})
	d.Where(d.Cond.EQ("status", 0))
	d.OrderBy("created_at").Desc()
	d.Limit(10)

	sql, values, err := d.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "DESC"))
	tt.EqualTrue(strings.Contains(sql, "LIMIT 10"))
	tt.Log(sql, values)
}

func TestDeleteLimitBy(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&sqlite3.Config{})
	d.Where(d.Cond.EQ("status", 0))
	d.OrderBy("id")
	d.LimitBy("id")
	d.Limit(5)

	sql, values, err := d.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "IN (SELECT"))
	tt.Log(sql, values)
}

func TestDeleteString(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&mysql.Config{})
	d.Where(d.Cond.EQ("id", 1))

	sql := d.String()
	tt.EqualTrue(strings.Contains(sql, "DELETE FROM"))
	tt.EqualTrue(strings.Contains(sql, "`user`"))
}

func TestDeleteNoWhere(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")
	d.SetDriver(&mysql.Config{})

	_, _, err := d.Build()
	tt.EqualTrue(err != nil)
}
