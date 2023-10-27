package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
)

func TestCompiledBuild(t *testing.T) {
	tt := zlsgo.NewTest(t)

	bd := builder.Build("EXPLAIN SELECT * FROM `user` where a=$? AND b=$?", 1234, "zls")
	sql, values, err := bd.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("EXPLAIN SELECT * FROM `user` where a=? AND b=?", sql)
	tt.Equal([]interface{}{1234, "zls"}, values)

	bd = builder.BuildNamed("EXPLAIN SELECT * FROM user where a=${a} AND b=${b}", map[string]interface{}{"a": 1234, "b": "zls"})

	sql, values, err = bd.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("EXPLAIN SELECT * FROM user where a=? AND b=?", sql)
	tt.Equal([]interface{}{1234, "zls"}, values)
}
