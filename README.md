# zdb

小巧的 Golang 数据库操作库，内置 SQL Builder 与轻量 ORM。

## 特性

- 多数据库驱动：SQLite3、MySQL、PostgreSQL、MS SQL、ClickHouse、Doris
- SQL Builder：SELECT/INSERT/UPDATE/DELETE/UNION/CREATE TABLE
- ORM 风格 API：Find/FindOne/Pages/Insert/Update/Delete/Replace
- 批量写入：BatchInsert/BatchReplace
- 结果扫描：Scan/ScanToMap/QueryTo/QueryToMaps
- 事务、连接池、读写分离集群、迁移辅助

## 环境

- Go 1.24+（go.mod）
- 依赖：zlsgo 及对应数据库驱动

## 安装

```bash
go get github.com/zlsgo/zdb
```

## 驱动与构建标签

| 数据库     | 包路径                                   | 说明                                                                |
| ---------- | ---------------------------------------- | ------------------------------------------------------------------- |
| SQLite     | `github.com/zlsgo/zdb/driver/sqlite3`    | 默认驱动，cgo 使用 mattn/go-sqlite3；非 cgo 使用 modernc.org/sqlite |
| MySQL      | `github.com/zlsgo/zdb/driver/mysql`      |                                                                     |
| PostgreSQL | `github.com/zlsgo/zdb/driver/postgres`   |                                                                     |
| MS SQL     | `github.com/zlsgo/zdb/driver/mssql`      |                                                                     |
| ClickHouse | `github.com/zlsgo/zdb/driver/clickhouse` | 需要 `-tags clickhouse`                                             |
| Doris      | `github.com/zlsgo/zdb/driver/doris`      | 需要 `-tags doris`                                                  |

补充：

- SQLite 可使用 `CGO_ENABLED=1` 获得更好的性能
- Builder 默认驱动为 SQLite，可用 `-tags nosqlite` 切换为 MySQL

## 快速开始（SQLite）

```go
package main

import (
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func main() {
	cfg := &sqlite3.Config{File: "./test.db"}
	zdb.Debug.Store(true)

	db, err := zdb.New(cfg)
	if err != nil {
		panic(err)
	}

	_ = db
}
```

## 连接配置示例

### SQLite

```go
cfg := &sqlite3.Config{File: "./data.db", ForeignKeys: true}
db, err := zdb.New(cfg)
```

### MySQL

```go
cfg := &mysql.Config{
	Host:     "127.0.0.1",
	Port:     3306,
	User:     "root",
	Password: "password",
	DBName:   "demo",
	Charset:  "utf8mb4",
}

db, err := zdb.New(cfg)
```

### PostgreSQL

```go
cfg := &postgres.Config{
	Host:     "127.0.0.1",
	Port:     5432,
	User:     "postgres",
	Password: "password",
	DBName:   "demo",
	SSLMode:  "disable",
}

db, err := zdb.New(cfg)
```

### MS SQL

```go
cfg := &mssql.Config{
	Host:     "127.0.0.1",
	Port:     1433,
	User:     "sa",
	Password: "password",
	DBName:   "demo",
}

db, err := zdb.New(cfg)
```

### ClickHouse

ClickHouse 示例需要 `-tags clickhouse` 构建后使用。

```go
cfg := &clickhouse.Config{
	Host:     "127.0.0.1",
	Port:     9000,
	User:     "default",
	Password: "",
	DBName:   "default",
	Compress: true,
}

db, err := zdb.New(cfg)
```

### Doris

Doris 示例需要 `-tags doris` 构建后使用。

```go
cfg := &doris.Config{
	Host:     "127.0.0.1",
	Port:     9030,
	User:     "root",
	Password: "",
	DBName:   "demo",
}

db, err := zdb.New(cfg)
```

自定义 DSN 可使用 `SetDsn` 覆盖自动拼接结果。

## 基础使用

### Exec / Query / Scan

```go
type User struct {
	ID        int          `zdb:"id"`
	Name      string       `zdb:"name"`
	CreatedAt zdb.JsonTime `zdb:"created_at"`
}

res, err := db.Exec("INSERT INTO user(name) VALUES(?)", "hi")

rows, err := db.Query("SELECT * FROM user WHERE name = ?", "hi")
if err != nil {
	panic(err)
}

defer rows.Close()

var users []User
count, err := zdb.Scan(rows, &users)
```

### QueryTo / QueryToMaps

```go
maps, err := db.QueryToMaps("SELECT * FROM user WHERE id > ?", 0)

var out []User
err = db.QueryTo(&out, "SELECT * FROM user WHERE id > ?", 0)
```

### ORM 与 Builder

```go
rows, err := db.Find("user", func(b *builder.SelectBuilder) error {
	b.Where(b.Cond.GE("id", 1))
	b.OrderBy("id").Desc()
	b.Limit(10)
	return nil
})

row, err := db.FindOne("user", func(b *builder.SelectBuilder) error {
	b.Where(b.Cond.EQ("id", 1))
	return nil
})
```

### 分页

```go
rows, pages, err := db.Pages("user", 1, 20, func(b *builder.SelectBuilder) error {
	b.OrderBy("id").Desc()
	return nil
})
```

### 插入与批量写入

```go
id, err := db.Insert("user", map[string]interface{}{"name": "hi", "age": 18})

ids, err := db.BatchInsert("user", []map[string]interface{}{
	{"name": "a", "age": 18},
	{"name": "b", "age": 20},
})

id, err = db.Replace("user", map[string]interface{}{"id": 1, "name": "hi"})
```

### 更新与删除

```go
n, err := db.Update("user", map[string]interface{}{"name": "new"}, func(b *builder.UpdateBuilder) error {
	b.Where(b.Cond.EQ("id", 1))
	return nil
})

n, err = db.Delete("user", func(b *builder.DeleteBuilder) error {
	b.Where(b.Cond.EQ("id", 1))
	return nil
})
```

### 事务

```go
err := db.Transaction(func(tx *zdb.DB) error {
	_, err := tx.Exec("UPDATE user SET name = ? WHERE id = ?", "hi", 1)
	return err
})
```

### 集群（读写分离）

```go
db, err := zdb.NewCluster([]driver.IfeConfig{cfg1, cfg2, cfg3}, "main")
err = db.Source(func(e *zdb.DB) error {
	_, err := e.Exec("INSERT INTO user(name) VALUES(?)", "source")
	return err
})

err = db.Replica(func(e *zdb.DB) error {
	_, err := e.Query("SELECT * FROM user")
	return err
})

ref := zdb.Instance("main")
```

### 迁移与 Schema

```go
err := db.Migration(func(db *zdb.DB, d driver.Dialect) error {
	sql, args, has := d.HasTable("user")
	rows, err := db.QueryToMaps(sql, args...)
	if err != nil {
		return err
	}
	if !has(rows) {
		return nil
	}
	return nil
})
```

```go
tb := builder.CreateTable("user").
	IfNotExists().
	Column(
		schema.NewField("id", schema.Int, func(f *schema.Field) { f.PrimaryKey = true; f.AutoIncrement = true }),
		schema.NewField("name", schema.String),
	)

sql, args, err := tb.Build()
if err != nil {
	panic(err)
}

_, err = db.Exec(sql, args...)
```

## Builder 速览

- `builder.Build("... a=$? AND b=$?", 1, "x")`
- `builder.BuildNamed("... a=${a} AND b=${b}", map[string]interface{}{"a": 1, "b": "x"})`
- `builder.Select/Query/Insert/Update/Delete/Union/CreateTable`
- `b.Cond`：EQ/NE/GT/GE/LT/LE/In/NotIn/Like/Between/And/Or/IsNull/IsNotNull（各 Builder 回调内）
- `builder.Raw` 用于嵌入原生表达式，`builder.Named` 用于命名参数

## 行为说明

- `Find` / `FindOne` / `Scan` / `QueryTo`(非 slice) 在无结果时返回 `ErrNotFound`
- `Update` / `Delete` 必须带 `Where` 条件；非 MySQL 使用 `Limit` 时需要 `LimitBy`
- PostgreSQL 插入会使用 `RETURNING`，默认主键字段为 `id`，可用 `SetIDKey` 修改
- `Replace` / `BatchReplace` 使用 REPLACE 语法（MySQL 风格），请确认目标数据库支持

## 连接池与日志

```go
zdb.Debug.Store(true)
db.Debug = true

db.Options(func(o *zdb.Options) {
	o.MaxIdleConns = 10
	o.MaxOpenConns = 100
	o.ConnMaxLifetime = time.Minute * 30
})
```
