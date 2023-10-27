package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
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
