package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/testdata"
)

type user struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	ID   string `json:"id"`
}

func TestBuilder(t *testing.T) {
	tt := zlsgo.NewTest(t)

	// zdb.Debug.Store(true)
	dbConf, clera, err := testdata.GetDbConf("")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	data := map[string]interface{}{
		"name": "test1",
		"age":  18,
	}

	table := testdata.TestTable.TableName()

	id, err := db.Insert(table, ztype.Map{})
	t.Log(id, err)
	tt.EqualTrue(err != nil)

	id, err = db.Insert(table, data)
	tt.NoError(err)
	t.Log(id)

	ids, err := db.BatchInsert(table, []map[string]interface{}{
		{
			"name": "test2",
			"age":  38,
		},
		{
			"name": "test3",
			"age":  58,
		},
	})
	tt.NoError(err)
	t.Log(ids)

	id, err = db.Replace(table, map[string]interface{}{
		"name": "new2",
		"id":   2,
		"age":  888,
	})
	tt.NoError(err)
	t.Log(id)

	ids, err = db.BatchReplace(table, []map[string]interface{}{
		{
			"name": "new3",
			"id":   3,
			"age":  666,
		},
	})
	tt.NoError(err)
	t.Log(ids)

	_ = db.Source(func(e *zdb.DB) error {
		id, err = e.Insert(table, map[string]interface{}{
			"name":  "ok",
			"is_ok": 100,
		})
		tt.NoError(err)
		t.Log(id)
		return nil
	})

	_, err = db.Insert(table, map[string]interface{}{"xx": "xx"})
	tt.EqualTrue(err != nil)
	t.Log(err)

	rows, err := db.Find(table, func(b *builder.SelectBuilder) error {
		return nil
	})
	tt.NoError(err)
	tt.Equal(4, len(rows))

	row, err := db.FindOne(table, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.Cond.GE("is_ok", 1))
		t.Log(sb.String())
		return nil
	})
	tt.NoError(err)
	t.Log(row)
	t.Log(row["name"].(string), row.Get("name").String())
	tt.Equal(row["name"].(string), row.Get("name").String())

	rows, err = db.Find(table, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.Cond.GE("id", 1))
		t.Log(sb.String())
		return nil
	})
	tt.NoError(err)
	t.Log(rows)

	rows, pages, err := db.Pages(table, 2, 3, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.Cond.GE("id", 1))
		t.Log(sb.String())
		return nil
	})
	t.Log(rows)
	t.Logf("%+v", pages)
	tt.NoError(err)

	u, err := zdb.Find[user](db, table, nil)
	tt.NoError(err)
	t.Log(u)

	i, err := db.Update(table, ztype.Map{"name": "new name", "age": 66}, func(b *builder.UpdateBuilder) error {
		b.Where(b.Cond.EQ("id", 1))
		sql, values, _ := b.Build()
		tt.Equal(`UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ?`, sql)
		tt.Log(sql, values)
		return nil
	})

	tt.NoError(err)
	tt.Equal(int64(1), i)

	ur, err := zdb.FindOne[user](db, table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("id", 1))
		return nil
	})
	tt.NoError(err)
	tt.Equal("new name", ur.Name)
	t.Logf("%+v\n", u)

	ul, err := zdb.Find[user](db, table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("id", 1))
		return nil
	})
	tt.NoError(err)
	tt.Equal("new name", ul[0].Name)
	t.Logf("%+v\n", u)

	_, err = db.Update(table, ztype.Map{}, func(b *builder.UpdateBuilder) error {
		b.Set(b.Decr("age"), b.Assign("name", "666"))
		b.Where(b.Cond.NE("name", ""))
		tt.Equal(`UPDATE "user" SET "age" = "age" - 1, "name" = 666 WHERE "name" <> `, b.String())
		tt.Log(b.String())
		return nil
	})
	tt.NoError(err)

	_, err = db.Update(table, ztype.Map{}, func(b *builder.UpdateBuilder) error {
		b.Set(b.Decr("age"), b.Assign("name", "666"))
		tt.Equal(`UPDATE "user" SET "age" = "age" - 1, "name" = 666`, b.String())
		tt.Log(b.String())
		return nil
	})
	tt.EqualTrue(err != nil)
}

func TestFindComplex(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	childTable := builder.Select().From("user")
	childTable.Where(childTable.Cond.GE("id", 1))

	_, err = db.FindOne(testdata.TestTable.TableName(), func(sb *builder.SelectBuilder) error {
		sb.From(sb.BuilderAs(childTable, "c"))
		t.Log(sb.String())
		return nil
	})
	t.Log(err)

	_, err = db.FindOne(testdata.TestTable.TableName(), func(sb *builder.SelectBuilder) error {
		sb.Where(sb.Cond.In("id", childTable))
		t.Log(sb.String())
		return nil
	})
	t.Log(err)
}
