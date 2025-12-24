package builder_test

import (
	dbsql "database/sql"
	"strings"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mssql"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/postgres"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func TestSelect(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user")
	sb.Select("id", "username", "count(*) as count")
	sb.SetDriver(&mysql.Config{})
	sb.Where(sb.Cond.GE("age", 18))
	sb.Where(sb.Cond.LE("age", 38))
	sb.Where("username = " + sb.Cond.Var("manage"))
	sb.OrderBy("id").Desc()

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("SELECT `id`, `username`, count(*) as count FROM `user` WHERE `age` >= ? AND `age` <= ? AND username = ? ORDER BY id DESC", sql)
	tt.Equal([]interface{}{18, 38, "manage"}, values)
}

func TestSelectCommaSplit(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("id, username").From("user")
	sb.SetDriver(&mysql.Config{})

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Equal("SELECT `id`, `username` FROM `user`", sql)
	tt.Equal(0, len(values))
}

func TestSelectCommaSplitFunctions(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("concat(first, last) as full, count(*) as c").From("user")
	sb.SetDriver(&mysql.Config{})

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Equal("SELECT concat(first, last) as full, count(*) as c FROM `user`", sql)
	tt.Equal(0, len(values))
}

func TestSelectJoin(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*")
	sb.SetDriver(&mysql.Config{})
	sb.From(sb.As("user", "u"))
	sb.Where(sb.Cond.EQ("u.username", "manage"))
	sb.Join("log l", "u.id", "l.uid")
	sb.JoinWithOption(builder.RightOuterJoin, sb.As("role", "r"),
		"u.id = r.uid",
		sb.Cond.Like("r.alias", "M%"),
	)
	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("SELECT * FROM `user` AS u JOIN log l ON u.id AND l.uid RIGHT OUTER JOIN role AS r ON u.id = r.uid AND `r`.`alias` LIKE ? WHERE `u`.`username` = ?", sql)
	tt.Equal([]interface{}{"M%", "manage"}, values)
}

func TestSelectNested(t *testing.T) {
	tt := zlsgo.NewTest(t)

	childSb := builder.Select("*").From("user")
	childSb.Where(childSb.Cond.GE("id", 1))

	tt.Run("from", func(tt *zlsgo.TestUtil) {
		sb := builder.Select("*")
		sb.SetDriver(&sqlite3.Config{})
		sb.From(sb.BuilderAs(childSb, "u"))
		sb.Where(sb.Cond.EQ("age", 18))

		sql, values, err := sb.Build()
		tt.NoError(err)
		tt.Log(sql, values)

		tt.Equal(`SELECT * FROM (SELECT * FROM "user" WHERE "id" >= ?) AS u WHERE "age" = ?`, sql)
		tt.Equal([]interface{}{1, 18}, values)
	})

	tt.Run("where", func(tt *zlsgo.TestUtil) {
		sb := builder.Select("*")
		sb.Where(sb.Cond.In("id", childSb))
		sb.From("user").Where(sb.Cond.EQ("age", 108))

		sql, values, err := sb.Build()
		tt.NoError(err)
		tt.Log(sql, values)

		tt.Equal(`SELECT * FROM "user" WHERE "id" IN (SELECT * FROM "user" WHERE "id" >= ?) AND "age" = ?`, sql)
		tt.Equal([]interface{}{1, 108}, values)
	})
}

func TestSelectSetDriver(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").Offset(10).Limit(10)
	tt.Equal(`SELECT * FROM "user" ORDER BY 1 OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY`, sb.SetDriver(&mssql.Config{}).String())
	tt.Equal("SELECT * FROM `user` LIMIT 10 OFFSET 10", sb.SetDriver(&mysql.Config{}).String())
	tt.Equal(`SELECT * FROM "user" LIMIT 10 OFFSET 10`, sb.SetDriver(&sqlite3.Config{}).String())
	tt.Equal(`SELECT * FROM "user" LIMIT 10 OFFSET 10`, sb.SetDriver(&postgres.Config{}).String())
}

func TestSelectComplex(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("id", "username", "group_id").From("user")
	sb.Where(sb.Cond.GE("age", dbsql.Named("age", 18)), sb.Cond.Or(sb.Cond.EQ("id", 1), sb.Cond.EQ("id", 108)))
	sb.OrderBy("id").OrderBy("username").Asc()
	sb.GroupBy("group_id")
	sb.Having(sb.Cond.GE("group_id", 1))
	sb.Limit(5).Offset(2)
	sb.Distinct()

	sql := sb.String()
	tt.Log(sql)
}

func TestSelectOther(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("id", "username", "group_id").From("user")
	sb.SetDriver(&postgres.Config{})
	sb.Where(sb.Cond.GE("age", dbsql.Named("age", 18)), sb.Cond.Or(sb.Cond.EQ("id", 1), sb.Cond.EQ("id", 108)))
	sb.OrderBy("id").OrderBy("username").Asc()
	sb.GroupBy("group_id")
	sb.Having(sb.Cond.GE("group_id", 1))
	sb.Limit(5).Offset(2)
	sb.Distinct()

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)
}

func TestNamedArgBindingMySQL(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user")
	sb.SetDriver(&mysql.Config{})
	sb.Where(
		sb.Cond.EQ("age", dbsql.Named("age", 18)),
		sb.Cond.EQ("name", dbsql.Named("name", "tom")),
	)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Equal("SELECT * FROM `user` WHERE `age` = ? AND `name` = ?", sql)
	tt.Equal([]interface{}{18, "tom"}, values)
}

func TestNamedArgBindingMsSQL(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user")
	sb.SetDriver(&mssql.Config{})
	sb.Where(
		sb.Cond.EQ("age", dbsql.Named("age", 18)),
		sb.Cond.EQ("name", dbsql.Named("name", "tom")),
	)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Equal(`SELECT * FROM "user" WHERE "age" = @age AND "name" = @name`, sql)
	tt.Equal([]interface{}{dbsql.Named("age", 18), dbsql.Named("name", "tom")}, values)
}

func TestSelectForUpdate(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").ForUpdate()
	sb.SetDriver(&mysql.Config{})

	sql, _, err := sb.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "FOR UPDATE"))
}

func TestSelectForShare(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").ForShare()
	sb.SetDriver(&mysql.Config{})

	sql, _, err := sb.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "FOR SHARE"))
}

func TestSelectSafety(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*")
	err := sb.Safety()
	tt.EqualTrue(err != nil)

	sb = builder.Select("*").From("user")
	err = sb.Safety()
	tt.EqualTrue(err != nil)

	sb = builder.Select("*").From("user").Where("id = 1")
	err = sb.Safety()
	tt.NoError(err)

	sb = builder.Select("*").From("user").Limit(10)
	err = sb.Safety()
	tt.NoError(err)

	sb = builder.Select("*").From("user", "order")
	err = sb.Safety()
	tt.NoError(err)
}

func TestSelectOrderByClear(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").OrderBy("id").OrderBy()
	sb.SetDriver(&mysql.Config{})

	sql, _, err := sb.Build()
	tt.NoError(err)
	tt.EqualTrue(!strings.Contains(sql, "ORDER BY"))
}

func TestSelectAscWithCol(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").Asc("id", "name")
	sb.SetDriver(&mysql.Config{})

	sql, _, err := sb.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "ORDER BY id, name ASC"))
}

func TestSelectDescWithCol(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").Desc("id", "name")
	sb.SetDriver(&mysql.Config{})

	sql, _, err := sb.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "ORDER BY id, name DESC"))
}
