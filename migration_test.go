package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/driver/sqlite3"
	"github.com/zlsgo/zdb/testdata"
)

func TestMigration(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	db.Migration(func(db *zdb.DB, d driver.Dialect) error {
		sql, values, process := d.HasTable(testdata.TestTable.TableName())
		rows, err := db.QueryToMaps(sql, values...)
		if err != nil {
			return err
		}

		t.Log(process(rows))

		if s, ok := d.(*sqlite3.Config); ok {
			sql, p := s.GetVersion()
			rows, _ := db.QueryToMaps(sql)
			t.Log(p(rows))
		}

		return nil
	})
}
