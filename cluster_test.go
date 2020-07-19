package zdb_test

import (
	"fmt"
	"testing"

	"github.com/sohaha/zdb"
	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zutil"
)

func TestCluster(t *testing.T) {
	var err error
	tt := zlsgo.NewTest(t)
	zdb.Debug = true
	dbConfs := make([]zdb.IfeConfig, 3)
	dbConfs[0], err = getDbConf("1")
	zutil.CheckErr(err, true)
	dbConfs[1], err = getDbConf("2")
	zutil.CheckErr(err, true)
	dbConfs[2], err = getDbConf("3")
	zutil.CheckErr(err, true)

	db, err := zdb.NewCluster(dbConfs, "c")
	zutil.CheckErr(err, true)

	err = db.Source(func(db *zdb.Engine) error {
		err = initTable(db, dbType)
		zutil.CheckErr(err, true)
		baseExec(db, tt)
		baseQuery(db, t, tt)
		baseTransaction(db, t, tt)
		_, err = db.Exec(`INSERT INTO ` + table.TableName() + ` (name) VALUES ('Source')`)
		zutil.CheckErr(err, true)
		rows, err := db.Query(fmt.Sprintf("select * from %s where name = ?", table.TableName()), "Source")
		zutil.CheckErr(err, true)
		rowsMap, err := zdb.ScanToMap(rows)
		tt.EqualExit(nil, err)
		tt.EqualExit(1, len(rowsMap))
		tt.EqualExit("Source", rowsMap[0]["name"])
		t.Log(rowsMap)
		return nil
	})

	zutil.CheckErr(err, true)
	err = db.Replica(func(db *zdb.Engine) error {
		err = initTable(db, dbType)
		zutil.CheckErr(err, true)
		baseExec(db, tt)
		baseQuery(db, t, tt)
		baseTransaction(db, t, tt)
		_, err = db.Exec(`INSERT INTO ` + table.TableName() + ` (name) VALUES ('Replica')`)
		zutil.CheckErr(err, true)
		rows, err := db.Query(fmt.Sprintf("select * from %s where name = ?", table.TableName()), "Replica")
		zutil.CheckErr(err, true)
		rowsMap, err := zdb.ScanToMap(rows)
		tt.EqualExit(nil, err)
		tt.EqualExit(1, len(rowsMap))
		tt.EqualExit("Replica", rowsMap[0]["name"])
		t.Log(rowsMap)
		return nil
	})
	zutil.CheckErr(err, true)

	db2 := zdb.DB("c")
	tt.EqualExit(db, db2)
}
