package builder_test

import (
	"strings"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
)

func TestUnion(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Where("id > 100")
	user.Where(user.Cond.EQ("age", 18))
	user2 := builder.Query("user2").Select("*")

	union := builder.Union(user, user2)

	sql, values, err := union.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal(`SELECT * FROM "user" WHERE id > 100 AND "age" = ? UNION SELECT * FROM "user2"`, sql)
	tt.Equal([]interface{}{18}, values)

	unionAll := builder.UnionAll(user, user2)

	sql, values, err = unionAll.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal(`SELECT * FROM "user" WHERE id > 100 AND "age" = ? UNION ALL SELECT * FROM "user2"`, sql)
	tt.Equal([]interface{}{18}, values)
}

func TestUnionSetDriver(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Select("*")
	user2 := builder.Query("user2").Select("*")

	union := builder.Union(user, user2)
	union.SetDriver(&mysql.Config{})

	sql, _, err := union.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "UNION"))
}

func TestUnionOrderByAscDesc(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Select("id", "name")
	user2 := builder.Query("user2").Select("id", "name")

	union := builder.Union(user, user2)
	union.OrderBy("id").Asc()

	sql, _, err := union.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "ORDER BY id ASC"))

	union2 := builder.Union(user, user2)
	union2.OrderBy("name").Desc()

	sql, _, err = union2.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "ORDER BY name DESC"))
}

func TestUnionLimitOffset(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Select("*")
	user2 := builder.Query("user2").Select("*")

	union := builder.Union(user, user2)
	union.Limit(10).Offset(5)

	sql, _, err := union.Build()
	tt.NoError(err)
	tt.EqualTrue(strings.Contains(sql, "LIMIT 10"))
	tt.EqualTrue(strings.Contains(sql, "OFFSET 5"))
}

func TestUnionString(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Select("*")
	user2 := builder.Query("user2").Select("*")

	union := builder.Union(user, user2)
	sql := union.String()

	tt.EqualTrue(strings.Contains(sql, "UNION"))
}

func TestUnionSafety(t *testing.T) {
	tt := zlsgo.NewTest(t)

	union := builder.Union()
	err := union.Safety()
	tt.EqualTrue(err != nil)

	user := builder.Query("user").Select("*").Limit(10)
	union = builder.Union(user)
	err = union.Safety()
	tt.EqualTrue(err != nil)

	user2 := builder.Query("user2").Select("*").Limit(10)
	union = builder.Union(user, user2)
	err = union.Safety()
	tt.NoError(err)
}

func TestUnionVar(t *testing.T) {
	tt := zlsgo.NewTest(t)

	user := builder.Query("user").Select("*")
	user2 := builder.Query("user2").Select("*")
	union := builder.Union(user, user2)

	placeholder := union.Var("test_value")
	tt.EqualTrue(placeholder != "")
}
