package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/testdata"
)

func TestDB_SwapPoolsKeepsHandleUsable(t *testing.T) {
	tt := zlsgo.NewTest(t)

	oldConf, clearOld, err := testdata.GetDbConf("TestDB_SwapPools_old")
	tt.NoError(err)
	defer clearOld()

	newConf, clearNew, err := testdata.GetDbConf("TestDB_SwapPools_new")
	tt.NoError(err)
	defer clearNew()

	handle, err := zdb.New(oldConf)
	tt.NoError(err)
	defer func() { _ = handle.Close() }()

	rows, err := handle.Query("SELECT 1")
	tt.NoError(err)
	tt.NoError(rows.Close())

	fresh, err := zdb.New(newConf)
	tt.NoError(err)

	old := handle.SwapPools(fresh.TakePools())
	tt.Equal(true, len(old) > 0)
	tt.NoError(zdb.ClosePools(old))

	rows, err = handle.Query("SELECT 1")
	tt.NoError(err)
	tt.NoError(rows.Close())

	tt.Equal(0, len(fresh.TakePools()))
}
