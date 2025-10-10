package zdb_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zjson"
	"github.com/sohaha/zlsgo/ztime"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/testdata"
)

func TestBase(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	err = testdata.InitTable(db)
	tt.NoError(err)

	baseExec(db, tt)
	baseQuery(db, t)
	baseTransaction(db, t, tt)
}

func baseExec(db *zdb.DB, tt *zlsgo.TestUtil) {
	res, err := db.Exec(`INSERT INTO ` + testdata.TestTable.TableName() + ` (name,date,is_ok) VALUES ('init','` + ztime.Now() + `',1)`)
	tt.EqualExit(nil, err)
	id, err := res.LastInsertId()
	tt.EqualExit(nil, err)
	affected, err := res.RowsAffected()
	tt.EqualExit(nil, err)
	tt.EqualExit(1, int(affected))
	tt.EqualExit(1, int(id))

	_, _ = db.Exec(`INSERT INTO ` + testdata.TestTable.TableName() + ` (name) VALUES ('test')`)
}

func baseQuery(db *zdb.DB, t *testing.T) {
	tt := zlsgo.NewTest(t)
	rows, err := db.Query(fmt.Sprintf("select * from %s", testdata.TestTable.TableName()))
	tt.EqualExit(nil, err)
	rowsMap, count, err := zdb.ScanToMap(rows)
	tt.EqualExit(nil, err)
	t.Log(count, rowsMap)
	tt.EqualExit(2, len(rowsMap))
	tt.EqualExit("init", rowsMap[0]["name"])

	var users []testdata.TestTableUser
	rows, _ = db.Query(fmt.Sprintf("select * from %s", testdata.TestTable.TableName()))
	count, err = zdb.Scan(rows, &users)
	tt.EqualExit(nil, err)
	t.Log(count, users)
	t.Log(users[0])
	t.Log(users[0].Date.Time())
	tt.EqualExit(2, len(users))
	tt.EqualExit(1, users[0].ID)
	tt.EqualExit("init", users[0].Name)
	tt.EqualExit(ztime.Now(), users[0].Date.String())
	json, _ := zjson.Marshal(users)
	t.Log(string(json))

	var user testdata.TestTableUser
	rows, _ = db.Query(fmt.Sprintf("select * from %s where id = ? limit 1", testdata.TestTable.TableName()), 1)
	count, err = zdb.Scan(rows, &user)
	tt.EqualExit(nil, err)
	tt.EqualExit(1, user.ID)
	tt.EqualExit("init", user.Name)
	tt.EqualExit(ztime.Now(), user.Date.String())

	json, _ = zjson.Marshal(user)
	t.Log(count, string(json))
}

func baseTransaction(db *zdb.DB, t *testing.T, tt *zlsgo.TestUtil) {
	var err error
	for _, v := range [][]string{
		{"false", `INSERT INTO ` + testdata.TestTable.TableName() + ` (name) VALUES ('Rollback -')`},
		{"true", `INSERT INTO ` + testdata.TestTable.TableName() + ` (name) VALUES ('Commit -')`},
	} {
		ifRollback := v[0] == "false"
		rollbackErr := errors.New("测试回滚")
		err = db.Transaction(func(e *zdb.DB) error {
			_, err := e.Exec(v[1])
			if err != nil {
				return err
			}
			if ifRollback {
				return rollbackErr
			}
			return nil
		})
		t.Log("是否回滚", ifRollback, err)
		if !ifRollback {
			tt.EqualNil(err)
		} else {
			tt.EqualExit(rollbackErr, err)
		}
	}
}

func TestOptions(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, clera, err := testdata.GetDbConf("0")
	tt.NoError(err)
	defer clera()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	db.Options(func(o *zdb.Options) {
		o.MaxOpenConns = 1
		// o.ExecMaxLifetime = 1 * time.Second
		// o.ExecMaxLifetime = 3000 * time.Millisecond
		o.MaxIdleConns = 1
	})

	err = testdata.InitTable(db)
	tt.NoError(err)

	baseExec(db, tt)

	var g sync.WaitGroup
	ctx, can := context.WithTimeout(context.Background(), 3*time.Second)

	finish := make(chan struct{}, 1)
	now := time.Now()
	defer can()

	total, ok, done := zutil.NewInt64(0), zutil.NewInt64(0), zutil.NewInt64(0)
	max := 10
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			total.Add(1)
			ctx, c := context.WithTimeout(context.Background(), 1*time.Second)
			db.Source(func(db *zdb.DB) error {
				_, err := db.Query("SELECT * FROM " + testdata.TestTable.TableName())
				if err == nil {
					ok.Add(1)
				} else {
					defer c()
				}

				done.Add(1)
				return nil
			}, ctx)
			g.Done()
		}(i)
		i++
	}

	runCtx(t, finish, ctx, &g)
	t.Log(time.Since(now), "成功数量:", ok, "失败数量:", int(total.Load()-ok.Load()), "跑完数量:", done)
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(true, dbConf.DB().Stats().WaitCount > 6)
	tt.EqualExit(10, int(done.Load()))

	ok.Swap(0)
	done.Swap(0)
	total.Swap(0)
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			total.Add(1)
			r, err := db.Query("SELECT * FROM  " + testdata.TestTable.TableName())
			if err == nil {
				ok.Add(1)
				err = r.Close()
			}
			done.Add(1)
			g.Done()
			tt.EqualNil(err)
		}(i)
		i++
	}
	runCtx(t, finish, ctx, &g)
	t.Log(time.Since(now), "成功数量:", ok, "失败数量:", int(total.Load()-ok.Load()), "跑完数量:", done)
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(0, int(total.Load()-ok.Load()))
	tt.EqualExit(10, int(done.Load()))

	ok.Swap(0)
	done.Swap(0)
	total.Swap(0)
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			total.Add(1)
			_, err := db.Query("SELECT * FROM  " + testdata.TestTable.TableName())
			if err == nil {
				ok.Add(1)
			}
			done.Add(1)
			g.Done()
		}(i)
		i++
	}
	runCtx(t, finish, ctx, &g)

	t.Log(time.Since(now), "成功数量:", ok.Load(), "失败数量:", int(total.Load()-ok.Load()), "跑完数量:", done.Load())
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(9, int(total.Load()-ok.Load()))
	tt.EqualExit(1, int(done.Load()))

}

func runCtx(t *testing.T, finish chan struct{}, ctx context.Context, g *sync.WaitGroup) {
	go func() {
		g.Wait()
		finish <- struct{}{}
	}()
	select {
	case <-finish:
		t.Log("跑完")
	case <-ctx.Done():
		t.Log("超时")
	}
}
