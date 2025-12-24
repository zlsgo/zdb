package builder_test

import (
	"strings"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/sqlite3"
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

func TestUpdateLimitRequiresLimitBy(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&sqlite3.Config{})
	u.Where(u.Cond.EQ("id", 1))
	u.Limit(1)

	_, _, err := u.Build()
	tt.EqualTrue(err != nil)
}

func TestUpdateSetMore(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})
	u.Where(u.Cond.EQ("id", 1))
	u.Set(u.Assign("name", "test"))
	u.SetMore(u.Assign("age", 20), u.Assign("status", 1))

	sql, values, err := u.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "`name` = ?"))
	tt.EqualTrue(strings.Contains(sql, "`age` = ?"))
	tt.EqualTrue(strings.Contains(sql, "`status` = ?"))
	tt.Log(sql, values)
}

func TestUpdateAsc(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})
	u.Where(u.Cond.EQ("status", 0))
	u.Set(u.Assign("status", 1))
	u.OrderBy("id").Asc()
	u.Limit(10)

	sql, values, err := u.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "ORDER BY `id` ASC"))
	tt.Log(sql, values)
}

func TestUpdateLimitBy(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&sqlite3.Config{})
	u.Where(u.Cond.EQ("status", 0))
	u.Set(u.Assign("status", 1))
	u.OrderBy("id")
	u.LimitBy("id")
	u.Limit(5)

	sql, values, err := u.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "IN (SELECT"))
	tt.Log(sql, values)
}

func TestUpdateOption(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})
	u.Where(u.Cond.EQ("id", 1))
	u.Set(u.Assign("name", "test"))
	u.Option("IGNORE")

	sql, values, err := u.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "IGNORE"))
	tt.Log(sql, values)
}

func TestUpdateString(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})
	u.Where(u.Cond.EQ("id", 1))
	u.Set(u.Assign("name", "test"))

	sql := u.String()
	tt.EqualTrue(strings.Contains(sql, "UPDATE"))
	tt.EqualTrue(strings.Contains(sql, "`user`"))
}

func TestUpdateNoWhere(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")
	u.SetDriver(&mysql.Config{})
	u.Set(u.Assign("name", "test"))

	_, _, err := u.Build()
	tt.EqualTrue(err != nil)
}
