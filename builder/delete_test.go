package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
)

func TestDelete(t *testing.T) {
	tt := zlsgo.NewTest(t)

	d := builder.Delete("user")

	d.Where(
		d.EQ("id", 108),
		"age >"+d.Var(88),
	)

	sql, values := d.Build()
	tt.Log(sql, values)

	tt.Equal(`DELETE FROM user WHERE id = ? AND age >?`, sql)
	tt.Equal([]interface{}{108, 88}, values)
}
