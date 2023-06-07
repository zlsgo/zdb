package builder_test

import (
	dbsql "database/sql"
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
	sb.Where(sb.GE("age", 18))
	sb.Where("username = " + sb.Var("manage"))
	sb.OrderBy("id").Desc()

	sql, values := sb.Build()
	tt.Log(sql, values)

	tt.Equal("SELECT * FROM user WHERE age >= ? AND username = ? ORDER BY id DESC", sql)
	tt.Equal([]interface{}{18, "manage"}, values)
}

func TestSelectJoin(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*")
	sb.From(sb.As("user", "u"))
	sb.Where(sb.EQ("u.username", "manage"))
	sb.Join("log l", "u.id", "l.uid")
	sb.JoinWithOption(builder.RightOuterJoin, sb.As("role", "r"),
		"u.id = r.uid",
		sb.Like("r.alias", "M%"),
	)
	sql, values := sb.Build()
	tt.Log(sql, values)

	tt.Equal("SELECT * FROM user AS u JOIN log l ON u.id AND l.uid RIGHT OUTER JOIN role AS r ON u.id = r.uid AND r.alias LIKE ? WHERE u.username = ?", sql)
	tt.Equal([]interface{}{"M%", "manage"}, values)
}

func TestSelectNested(t *testing.T) {
	tt := zlsgo.NewTest(t)

	childSb := builder.Select("*").From("user")
	childSb.Where(childSb.GE("id", 1))

	tt.Run("from", func(tt *zlsgo.TestUtil) {
		sb := builder.Select("*")
		sb.From(sb.BuilderAs(childSb, "u"))
		sb.Where(sb.EQ("age", 18))

		sql, values := sb.Build()
		tt.Log(sql, values)

		tt.Equal("SELECT * FROM (SELECT * FROM user WHERE id >= ?) AS u WHERE age = ?", sql)
		tt.Equal([]interface{}{1, 18}, values)
	})

	tt.Run("where", func(tt *zlsgo.TestUtil) {
		sb := builder.Select("*")
		sb.Where(sb.In("id", childSb))
		sb.From("user").Where(sb.EQ("age", 108))

		sql, values := sb.Build()
		tt.Log(sql, values)

		tt.Equal("SELECT * FROM user WHERE id IN (SELECT * FROM user WHERE id >= ?) AND age = ?", sql)
		tt.Equal([]interface{}{1, 108}, values)
	})
}

func TestSelectSetDriver(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("*").From("user").Offset(10).Limit(10)
	tt.Equal("SELECT * FROM user ORDER BY 1 OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", sb.SetDriver(&mssql.Config{}).String())
	tt.Equal("SELECT * FROM user LIMIT 10 OFFSET 10", sb.SetDriver(&mysql.Config{}).String())
	tt.Equal("SELECT * FROM user LIMIT 10 OFFSET 10", sb.SetDriver(&sqlite3.Config{}).String())
	tt.Equal("SELECT * FROM user LIMIT 10 OFFSET 10", sb.SetDriver(&postgres.Config{}).String())
}

func TestSelectComplex(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("id", "username", "group_id").From("user")
	sb.Where(sb.GE("age", dbsql.Named("age", 18)), sb.Or(sb.EQ("id", 1), sb.EQ("id", 108)))
	sb.OrderBy("id").OrderBy("username").Asc()
	sb.GroupBy("group_id")
	sb.Having(sb.GE("group_id", 1))
	sb.Limit(5).Offset(2)
	sb.Distinct()

	sql := sb.String()
	tt.Log(sql)
}

func TestSelectOther(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Select("id", "username", "group_id").From("user")
	sb.SetDriver(&postgres.Config{})
	sb.Where(sb.GE("age", dbsql.Named("age", 18)), sb.Or(sb.EQ("id", 1), sb.EQ("id", 108)))
	sb.OrderBy("id").OrderBy("username").Asc()
	sb.GroupBy("group_id")
	sb.Having(sb.GE("group_id", 1))
	sb.Limit(5).Offset(2)
	sb.Distinct()

	more := (func(c builder.Cond) []string {
		return []string{c.NE("id", 99)}
	})(sb.Cond)

	sb.Where(more...)

	sql, values := sb.Build()
	tt.Log(sql, values)
}
