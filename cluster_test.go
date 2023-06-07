package zdb_test

import (
	"fmt"
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/testdata"
)

func TestCluster(t *testing.T) {
	var (
		err   error
		clera func()
	)
	tt := zlsgo.NewTest(t)
	zdb.Debug = true
	dbConfs := make([]driver.IfeConfig, 3)
	dbConfs[0], clera, err = testdata.GetDbConf("1")
	tt.NoError(err)
	defer clera()
	dbConfs[1], clera, err = testdata.GetDbConf("2")
	tt.NoError(err)
	defer clera()
	dbConfs[2], clera, err = testdata.GetDbConf("3")
	tt.NoError(err)
	defer clera()

	db, err := zdb.NewCluster(dbConfs, "c")
	tt.NoError(err)

	err = db.Source(func(db *zdb.DB) error {
		err = testdata.InitTable(db)
		tt.NoError(err)
		baseExec(db, tt)
		baseQuery(db, tt)
		baseTransaction(db, tt)
		_, err = db.Exec(`INSERT INTO ` + testdata.TestTable.TableName() + ` (name) VALUES ('Source')`)
		tt.NoError(err)
		rows, err := db.Query(fmt.Sprintf("select * from %s where name = ?", testdata.TestTable.TableName()), "Source")
		tt.NoError(err)
		rowsMap, _, err := zdb.ScanToMap(rows)
		tt.EqualExit(nil, err)
		tt.EqualExit(1, len(rowsMap))
		tt.EqualExit("Source", rowsMap[0]["name"])
		t.Log(rowsMap)
		return nil
	})

	tt.NoError(err)
	err = db.Replica(func(db *zdb.DB) error {
		err = testdata.InitTable(db)
		tt.NoError(err)
		baseExec(db, tt)
		baseQuery(db, tt)
		baseTransaction(db, tt)
		_, err = db.Exec(`INSERT INTO ` + testdata.TestTable.TableName() + ` (name) VALUES ('Replica')`)
		tt.NoError(err)
		rows, err := db.Query(fmt.Sprintf("select * from %s where name = ?", testdata.TestTable.TableName()), "Replica")
		tt.NoError(err)
		rowsMap, _, err := zdb.ScanToMap(rows)
		tt.EqualExit(nil, err)
		tt.EqualExit(1, len(rowsMap))
		tt.EqualExit("Replica", rowsMap[0]["name"])
		t.Log(rowsMap)
		return nil
	})
	tt.NoError(err)
	db2 := zdb.Instance("c")
	tt.EqualExit(db, db2)
}
