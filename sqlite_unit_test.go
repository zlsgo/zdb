package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver/sqlite3"
	"github.com/zlsgo/zdb/testdata"
)

func TestSQLiteBatchInsertIDs(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("sqlite_batch_ids")
	tt.NoError(err)
	defer clear()

	if _, ok := dbConf.(*sqlite3.Config); !ok {
		t.Skip("sqlite only")
	}

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	data := []map[string]interface{}{
		{"name": "b1", "age": 10},
		{"name": "b2", "age": 11},
		{"name": "b3", "age": 12},
		{"name": "b4", "age": 13},
		{"name": "b5", "age": 14},
	}

	ids, err := db.BatchInsertWithConfig(table, data, zdb.BatchConfig{MaxBatch: 2})
	tt.NoError(err)
	tt.Equal(len(data), len(ids))

	for i := 1; i < len(ids); i++ {
		tt.Equal(ids[i-1]+1, ids[i])
	}

	names := make([]interface{}, 0, len(data))
	ordered := make([]string, 0, len(data))
	for _, item := range data {
		name := item["name"].(string)
		names = append(names, name)
		ordered = append(ordered, name)
	}

	rows, err := db.Find(table, func(b *builder.SelectBuilder) error {
		b.Select("id", "name")
		b.Where(b.Cond.In("name", names...))
		return nil
	})
	tt.NoError(err)
	tt.Equal(len(data), len(rows))

	nameToID := map[string]int64{}
	for i := range rows {
		nameToID[rows[i].Get("name").String()] = rows[i].Get("id").Int64()
	}

	for i := range ordered {
		tt.Equal(nameToID[ordered[i]], ids[i])
	}
}

func TestSQLiteLimitByUpdateDelete(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("sqlite_limit_by")
	tt.NoError(err)
	defer clear()

	if _, ok := dbConf.(*sqlite3.Config); !ok {
		t.Skip("sqlite only")
	}

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	_, err = db.BatchInsertWithConfig(table, []map[string]interface{}{
		{"name": "limit_by", "age": 21},
		{"name": "limit_by", "age": 22},
		{"name": "limit_by", "age": 23},
	}, zdb.BatchConfig{MaxBatch: 2})
	tt.NoError(err)

	updated, err := db.Update(table, map[string]interface{}{"age": 99}, func(b *builder.UpdateBuilder) error {
		b.Where(b.Cond.EQ("name", "limit_by"))
		b.OrderBy("id").Asc()
		b.Limit(1)
		b.LimitBy("id")
		return nil
	})
	tt.NoError(err)
	tt.Equal(int64(1), updated)

	rows, err := db.Find(table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "limit_by"))
		b.Where(b.Cond.EQ("age", 99))
		return nil
	})
	tt.NoError(err)
	tt.Equal(1, len(rows))

	deleted, err := db.Delete(table, func(b *builder.DeleteBuilder) error {
		b.Where(b.Cond.EQ("name", "limit_by"))
		b.OrderBy("id").Asc()
		b.Limit(1)
		b.LimitBy("id")
		return nil
	})
	tt.NoError(err)
	tt.Equal(int64(1), deleted)

	rows, err = db.Find(table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "limit_by"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(2, len(rows))
}

func TestDBGetSQLDB(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("get_sqldb")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	sqlDB, err := db.GetSQLDB()
	tt.NoError(err)
	tt.EqualTrue(sqlDB != nil)

	err = sqlDB.Ping()
	tt.NoError(err)
}

func TestDBClose(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("close_test")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = db.Close()
	tt.NoError(err)
}

func TestDBSetIDKey(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("set_idkey")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	db.SetIDKey("custom_id")
	tt.EqualTrue(true)
}

func TestDBGetDriver(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("get_driver")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	driver := db.GetDriver()
	tt.EqualTrue(driver != nil)
}

func TestJsonTime(t *testing.T) {
	tt := zlsgo.NewTest(t)

	jt := zdb.JsonTime{}
	str := jt.String()
	tt.Equal("0000-00-00 00:00:00", str)

	tm := jt.Time()
	tt.EqualTrue(tm.IsZero())

	json, err := jt.MarshalJSON()
	tt.NoError(err)
	tt.EqualTrue(len(json) > 0)
}

func TestMustInstance(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("must_instance")
	tt.NoError(err)
	defer clear()

	_, err = zdb.New(dbConf, "test_alias")
	tt.NoError(err)

	db := zdb.Instance("test_alias")
	tt.EqualTrue(db != nil)

	db, err = zdb.MustInstance("test_alias")
	tt.NoError(err)
	tt.EqualTrue(db != nil)

	_, err = zdb.MustInstance("not_exist")
	tt.EqualTrue(err != nil)
}

func TestDBTransaction(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("transaction")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()

	err = db.Transaction(func(tx *zdb.DB) error {
		_, err := tx.Insert(table, map[string]interface{}{
			"name": "tx_test",
			"age":  25,
		})
		return err
	})
	tt.NoError(err)

	rows, err := db.Find(table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "tx_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(1, len(rows))
}

func TestDBSource(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("source")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()

	err = db.Source(func(tx *zdb.DB) error {
		_, err := tx.Insert(table, map[string]interface{}{
			"name": "source_test",
			"age":  30,
		})
		return err
	})
	tt.NoError(err)
}

func TestDBReplica(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("replica")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	_, err = db.Insert(table, map[string]interface{}{
		"name": "replica_test",
		"age":  35,
	})
	tt.NoError(err)

	err = db.Replica(func(tx *zdb.DB) error {
		rows, err := tx.Find(table, func(b *builder.SelectBuilder) error {
			b.Where(b.Cond.EQ("name", "replica_test"))
			return nil
		})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return zdb.ErrNotFound
		}
		return nil
	})
	tt.NoError(err)
}

func TestDBFindOne(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("findone")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	_, err = db.Insert(table, map[string]interface{}{
		"name": "findone_test",
		"age":  40,
	})
	tt.NoError(err)

	row, err := db.FindOne(table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "findone_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal("findone_test", row.Get("name").String())

	_, err = db.FindOne(table, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "not_exist"))
		return nil
	})
	tt.EqualTrue(err == zdb.ErrNotFound)
}

func TestDBPages(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("pages")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	for i := 0; i < 15; i++ {
		_, err = db.Insert(table, map[string]interface{}{
			"name": "page_test",
			"age":  i,
		})
		tt.NoError(err)
	}

	rows, pages, err := db.Pages(table, 1, 5, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "page_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(5, len(rows))
	tt.Equal(uint(15), pages.Total)
	tt.Equal(uint(3), pages.Count)
	tt.Equal(uint(1), pages.Curpage)

	rows, pages, err = db.Pages(table, 2, 5, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "page_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(5, len(rows))
	tt.Equal(uint(2), pages.Curpage)
}

func TestDBReplace(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("replace")
	tt.NoError(err)
	defer clear()

	if _, ok := dbConf.(*sqlite3.Config); !ok {
		t.Skip("sqlite only for replace test")
	}

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	id, err := db.Replace(table, map[string]interface{}{
		"name": "replace_test",
		"age":  50,
	})
	tt.NoError(err)
	tt.EqualTrue(id > 0)
}

func TestDBBatchReplace(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clear, err := testdata.GetDbConf("batch_replace")
	tt.NoError(err)
	defer clear()

	if _, ok := dbConf.(*sqlite3.Config); !ok {
		t.Skip("sqlite only for batch replace test")
	}

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()
	ids, err := db.BatchReplace(table, []map[string]interface{}{
		{"name": "br1", "age": 51},
		{"name": "br2", "age": 52},
	})
	tt.NoError(err)
	tt.Equal(2, len(ids))
}
