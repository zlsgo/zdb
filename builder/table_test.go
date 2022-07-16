package builder_test

import (
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/driver/mssql"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/postgres"
	"github.com/zlsgo/zdb/driver/sqlite3"
	"github.com/zlsgo/zdb/schema"
)

func TestCreateTable(t *testing.T) {
	tt := zlsgo.NewTest(t)

	b := builder.CreateTable("user").IfNotExists()
	b.Define("id", "BIGINT(20)", "NOT NULL", "AUTO_INCREMENT", "PRIMARY KEY", `COMMENT "用户ID"`)

	sql := b.String()
	t.Log(sql)

	tt.Equal(`CREATE TABLE IF NOT EXISTS user (id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT "用户ID")`, sql)

	b = builder.CreateTempTable("user").IfNotExists()
	b.SetDriver(&mysql.Config{})
	b.Define("id", "BIGINT(20)", "NOT NULL", "AUTO_INCREMENT", "PRIMARY KEY")
	b.Define("name", "VARCHAR(255)", "NOT NULL")
	b.Define("created_at", "DATETIME", "NOT NULL")
	b.Define("modified_at", "DATETIME", "NOT NULL")
	b.Define("KEY", "idx_name_modified_at", "name, modified_at")
	b.Option("DEFAULT CHARACTER SET", "utf8mb4")

	sql = b.String()
	t.Log(sql)

	tt.Equal(`CREATE TEMPORARY TABLE IF NOT EXISTS user (id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, created_at DATETIME NOT NULL, modified_at DATETIME NOT NULL, KEY idx_name_modified_at name, modified_at) DEFAULT CHARACTER SET utf8mb4`, sql)
}

func TestCreateTableQuick(t *testing.T) {
	tt := zlsgo.NewTest(t)

	for dialect, expected := range map[driver.Dialect]string{
		&mysql.Config{}:   "CREATE TABLE user (id bigint UNSIGNED PRIMARY KEY COMMENT 'ID', name varchar(100) COMMENT '用户名', created_at datetime NOT NULL COMMENT '创建时间') DEFAULT CHARACTER SET utf8mb4",
		&sqlite3.Config{}: "CREATE TABLE user (id integer PRIMARY KEY, name text, created_at datetime NOT NULL)",
	} {
		b := builder.CreateTable("user")
		d, ok := dialect.(driver.IfeConfig)
		if !ok {
			t.Errorf("%T is not zdb.IfeConfig", dialect)
		}
		_ = d
		b.SetDriver(dialect)

		b.Column(schema.NewField("id", uint8(0), func(field *schema.Field) {
			field.PrimaryKey = true
			field.Comment = "ID"
		}))

		b.Column(schema.NewField("name", "", func(field *schema.Field) {
			field.Size = 100
			field.NotNull = false
			field.Comment = "用户名"
		}))

		b.Column(schema.NewField("created_at", time.Time{}, func(field *schema.Field) {
			field.Size = 100
			field.Comment = "创建时间"
		}))

		if dialect.Value() == driver.MySQL {
			b.Option("DEFAULT CHARACTER SET", "utf8mb4")
		}

		sql := b.String()
		tt.Equal(expected, sql)
	}
}

func TestDropTable(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sql := builder.NewTable("user").Drop()

	tt.Equal("DROP TABLE user", sql)
}

func TestHasTable(t *testing.T) {
	tt := zlsgo.NewTest(t)

	sql, values, _ := builder.NewTable("shop").Has()
	tt.Equal("SELECT count(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?", sql)
	tt.Equal([]interface{}{"shop"}, values)

	table := builder.NewTable("shop")
	dialect := &sqlite3.Config{Memory: true}
	table.SetDriver(dialect)
	sql, values, process := table.Has()
	tt.Equal("SELECT count(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?", sql)
	tt.Equal([]interface{}{"shop"}, values)
	rows, err := dialect.DB().Query(sql, values...)
	tt.NoError(err)

	data, _, _ := zdb.ScanToMap(rows)
	t.Log(process(data))

	{
		table = builder.NewTable("shop")
		table.SetDriver(&mysql.Config{
			Dsn: "root:root@(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local",
		})
		sql, values, _ = table.Has()
		tt.Equal("SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", sql)
		tt.Equal([]interface{}{"test", "shop", "BASE TABLE"}, values)
	}

	{
		table = builder.NewTable("shop")
		table.SetDriver(&postgres.Config{
			Dsn: "host=192.168.3.378 port=5432 user=postgres password=12345678 dbname=test sslmode=disable",
		})
		sql, values, _ = table.Has()
		tt.Equal("SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", sql)
		tt.Equal([]interface{}{"test", "shop", "BASE TABLE"}, values)
	}

	{
		table = builder.NewTable("shop")
		table.SetDriver(&mssql.Config{
			Dsn: "sqlserver://mssql:12345678@localhost:9930?database=test",
		})
		sql, values, _ = table.Has()
		tt.Equal("SELECT count(*) AS count FROM INFORMATION_SCHEMA.tables WHERE table_name = ? AND table_catalog = ?", sql)
		tt.Equal([]interface{}{"shop", "test"}, values)
	}
}