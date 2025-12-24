package builder_test

import (
	"strings"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/postgres"
)

func TestInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&postgres.Config{})
	sb.Cols("username", "age").Values("new user", 18)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal(`INSERT INTO "user" ("username", "age") VALUES ($1, $2)`, sql)
	tt.Equal([]interface{}{"new user", 18}, values)

	sb = builder.Insert("user")
	sb.SetDriver(&mysql.Config{})
	sb.Cols("username", "age", "create_at").Values("new user", 18, builder.Raw("UNIX_TIMESTAMP(NOW())"))
	sb.Option("ON DUPLICATE KEY UPDATE age = VALUES(age)")
	sql, values, err = sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("INSERT INTO `user` (`username`, `age`, `create_at`) VALUES (?, ?, UNIX_TIMESTAMP(NOW())) ON DUPLICATE KEY UPDATE age = VALUES(age)", sql)
	tt.Equal([]interface{}{"new user", 18}, values)

	sb = builder.Insert("user")
	// Not many times pass cols
	sb.Cols("username").Values("new user")
	sb.Cols("age").Values(18)

	sql, values, err = sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.EqualTrue(sql != `INSERT INTO "user" ("username", "age", "create_at") VALUES (?, ?)`)
}

func TestBatchInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&mysql.Config{})

	sb.Cols("username", "age")
	sb.Values("new user", 18)
	sb.Values("new user2", 199)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("INSERT INTO `user` (`username`, `age`) VALUES (?, ?), (?, ?)", sql)
	tt.Equal([]interface{}{"new user", 18, "new user2", 199}, values)
}

func TestReplaceInsert(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Replace("user")
	sb.SetDriver(&mysql.Config{})
	sb.Cols("username", "age")
	sb.Values("new user", 18)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("REPLACE INTO `user` (`username`, `age`) VALUES (?, ?)", sql)
	tt.Equal([]interface{}{"new user", 18}, values)
}

func TestInsertIgnore(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.InsertIgnore("user")
	sb.SetDriver(&mysql.Config{})
	sb.Cols("username", "age")
	sb.Values("new user", 18)

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("INSERT IGNORE INTO `user` (`username`, `age`) VALUES (?, ?)", sql)
	tt.Equal([]interface{}{"new user", 18}, values)
}

func TestInsertString(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&mysql.Config{})
	sb.Cols("username", "age")
	sb.Values("new user", 18)

	sql := sb.String()
	tt.EqualTrue(strings.Contains(sql, "INSERT INTO"))
	tt.EqualTrue(strings.Contains(sql, "`user`"))
}

func TestInsertVar(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&mysql.Config{})

	placeholder := sb.Var("test_value")
	tt.EqualTrue(placeholder != "")
}

func TestInsertSafety(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("")
	err := sb.Safety()
	tt.EqualTrue(err != nil)

	sb = builder.Insert("user")
	err = sb.Safety()
	tt.EqualTrue(err != nil)

	sb = builder.Insert("user")
	sb.Cols("username")
	err = sb.Safety()
	tt.EqualTrue(err != nil)

	sb = builder.Insert("user")
	sb.Cols("username", "age")
	sb.Values("user1", 18)
	err = sb.Safety()
	tt.NoError(err)

	sb = builder.Insert("user")
	sb.Cols("username", "age")
	sb.Values("user1")
	err = sb.Safety()
	tt.EqualTrue(err != nil)
}

func TestInsertBatchValues(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sb := builder.Insert("user")
	sb.SetDriver(&mysql.Config{})
	sb.Cols("username", "age")
	sb.BatchValues([][]interface{}{
		{"user1", 18},
		{"user2", 25},
		{"user3", 30},
	})

	sql, values, err := sb.Build()
	tt.NoError(err)
	tt.Log(sql, values)

	tt.Equal("INSERT INTO `user` (`username`, `age`) VALUES (?, ?), (?, ?), (?, ?)", sql)
	tt.Equal([]interface{}{"user1", 18, "user2", 25, "user3", 30}, values)
}
