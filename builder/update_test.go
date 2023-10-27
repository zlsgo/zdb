package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
)

func TestUpdate(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})

	u.Where(
		u.Cond.EQ("id", 108),
	)

	u.Set(
		u.Incr("age"),
		u.Assign("name", "hi"),
		u.Sub("num", 100),
		u.Decr("i1"),
		u.Add("i2", 2),
		u.Mul("i3", 3),
		u.Div("i4", 4),
	)
	u.Limit(1)
	u.OrderBy("id ASC", "age")
	u.Desc()
	sql, values, err := u.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("UPDATE `user` SET `age` = `age` + 1, `name` = ?, `num` = `num` - ?, `i1` = `i1` - 1, `i2` = `i2` + ?, `i3` = `i3` * ?, `i4` = `i4` / ? WHERE `id` = ? ORDER BY `id` ASC, `age` DESC LIMIT 1", sql)
	tt.Equal([]interface{}{"hi", 100, 2, 3, 4, 108}, values)
}
