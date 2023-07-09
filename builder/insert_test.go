package builder_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/postgres"
)

func TestInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&postgres.Config{})
	sb.Cols("username", "age").Values("new user", 18)

	sql, values := sb.Build()
	tt.Log(sql, values)

	tt.Equal(`INSERT INTO user (username, age) VALUES ($1, $2)`, sql)
	tt.Equal([]interface{}{"new user", 18}, values)

	sb = builder.Insert("user")
	sb.Cols("username", "age", "create_at").Values("new user", 18, builder.Raw("UNIX_TIMESTAMP(NOW())"))

	sql, values = sb.Build()
	tt.Log(sql, values)

	tt.Equal(`INSERT INTO user (username, age, create_at) VALUES (?, ?, UNIX_TIMESTAMP(NOW()))`, sql)
	tt.Equal([]interface{}{"new user", 18}, values)

	sb = builder.Insert("user")
	// Not many times pass cols
	sb.Cols("username").Values("new user")
	sb.Cols("age").Values(18)

	sql, values = sb.Build()
	tt.Log(sql, values)

	tt.EqualTrue(sql != `INSERT INTO user (username, age) VALUES (?, ?)`)
}

func TestBatchInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")

	sb.Cols("username", "age")
	sb.Values("new user", 18)
	sb.Values("new user2", 199)

	sql, values := sb.Build()
	tt.Log(sql, values)

	tt.Equal(`INSERT INTO user (username, age) VALUES (?, ?), (?, ?)`, sql)
	tt.Equal([]interface{}{"new user", 18, "new user2", 199}, values)
}

func TestReplaceInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Replace("user")
	sb.Cols("username", "age")
	sb.Values("new user", 18)

	sql, values := sb.Build()
	tt.Log(sql, values)

	tt.Equal(`REPLACE INTO user (username, age) VALUES (?, ?)`, sql)
	tt.Equal([]interface{}{"new user", 18}, values)
}
