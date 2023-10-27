package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
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
