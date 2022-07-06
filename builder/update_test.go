package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
)

func TestUpdate(t *testing.T) {
	tt := zlsgo.NewTest(t)

	u := builder.Update("user")

	u.Where(
		u.EQ("id", 108),
	)

	u.Set(
		u.Incr("age"),
		u.Assign("name", "hi"),
	)

	sql, values := u.Build()
	tt.Log(sql, values)

	tt.Equal(`UPDATE user SET age = age + 1, name = ? WHERE id = ?`, sql)
	tt.Equal([]interface{}{"hi", 108}, values)
}
