package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/testdata"
)

func TestDB_Query(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("TestDB_Query")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	table := testdata.TestTable.TableName()

	data := map[string]interface{}{
		"name": "test1",
		"age":  18,
	}
	_, _ = db.Insert(table, data)
	rows, err := db.QueryToMaps("select * from " + table)
	tt.NoError(err)
	tt.Log(rows)

	var resps []struct {
		Name string `json:"name"`
		Id   int    `json:"id"`
	}
	err = db.QueryTo(&resps, "select * from "+table)
	tt.NoError(err)
	t.Log(resps)

	var resp struct {
		Name string `json:"name"`
		Id   int    `json:"id"`
	}
	err = db.QueryTo(&resp, "select * from "+table)
	tt.NoError(err)
	t.Log(resp)

	var emptyResp struct {
		Name string `json:"name"`
	}
	err = db.QueryTo(&emptyResp, "select * from "+table+" where name = ?", "not_exist")
	tt.EqualExit(zdb.ErrNotFound, err)

	var emptySlice []struct {
		Name string `json:"name"`
	}
	err = db.QueryTo(&emptySlice, "select * from "+table+" where name = ?", "not_exist")
	tt.NoError(err)
	tt.Equal(0, len(emptySlice))
}
