package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
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

	id, err := db.Insert(table, data)
	tt.NoError(err)
	t.Log(id)

	_ = db.Source(func(e *zdb.DB) error {
		t.Log(&db, &e)
		t.Logf("%+v %+v\n", db, e)
		id, err = e.Insert(table, map[string]interface{}{
			"name":  "ok",
			"is_ok": 100,
		})
		tt.NoError(err)
		t.Log(id)
		return nil
	})

	id, err = db.Insert(table, []map[string]interface{}{data, data, {"name": "test2", "age": 999, "xx": "xx"}})
	tt.NoError(err)
	t.Log(id)

	_, err = db.Insert(table, []map[string]interface{}{data, {"xx": "xx"}})
	tt.EqualTrue(err != nil)
	t.Log(err)

	row, err := db.Find(table, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.GE("is_ok", 1))
		t.Log(sb.String())
		return nil
	})
	tt.NoError(err)
	t.Log(row)

	rows, err := db.FindAll(table, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.GE("id", 1))
		t.Log(sb.String())
		return nil
	})
	tt.NoError(err)
	t.Log(rows)

	rows, pages, err := db.Pages(table, 2, 3, func(sb *builder.SelectBuilder) error {
		sb.Where(sb.GE("id", 1))
		t.Log(sb.String())
		return nil
	})
	t.Log(rows)
	t.Logf("%+v", pages)
	tt.NoError(err)

	u, err := zdb.Find[user](db, table, nil)
	tt.NoError(err)
	t.Log(u)

	i, err := db.Update(table, zdb.QuoteCols(map[string]interface{}{"name": "new name", "age": 66}), func(b *builder.UpdateBuilder) error {
		b.Where(b.EQ("id", 1))
		t.Log(b.Build())
		t.Log(33)
		return nil
	})

	tt.NoError(err)
	tt.Equal(int64(1), i)

	u, err = zdb.Find[user](db, table, func(b *builder.SelectBuilder) error {
		b.Where(b.EQ("id", 1))
		return nil
	})
	tt.NoError(err)
	tt.Equal("new name", u.Name)
	t.Logf("%+v\n", u)

	_, err = db.Update(table, nil, func(b *builder.UpdateBuilder) error {
		b.Set(b.Decr("age"), b.Assign("name", "666"))
		tt.Equal("UPDATE user SET age = age - 1, name = 666", b.String())
		return nil
	})
	tt.NoError(err)
}

func TestFindComplex(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	childTable := builder.Select().From("user")
	childTable.Where(childTable.GE("id", 1))

	_, err = db.Find(testdata.TestTable.TableName(), func(sb *builder.SelectBuilder) error {
		sb.From(sb.BuilderAs(childTable, "c"))
		t.Log(sb.String())
		return nil
	})
	t.Log(err)

	_, err = db.Find(testdata.TestTable.TableName(), func(sb *builder.SelectBuilder) error {
		sb.Where(sb.In("id", childTable))
		t.Log(sb.String())
		return nil
	})
	t.Log(err)
}
