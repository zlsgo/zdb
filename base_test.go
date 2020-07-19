package zdb_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sohaha/zdb"
	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zjson"
	"github.com/sohaha/zlsgo/ztime"
	"github.com/sohaha/zlsgo/zutil"
)

func TestBase(t *testing.T) {
	tt := zlsgo.NewTest(t)

	dbConf, err := getDbConf("")
	zutil.CheckErr(err, true)

	db, err := zdb.New(dbConf)
	zutil.CheckErr(err, true)

	err = initTable(db, dbType)
	zutil.CheckErr(err, true)

	baseExec(db, tt)
	baseQuery(db, t, tt)
	baseTransaction(db, t, tt)
}

func baseExec(db *zdb.Engine, tt *zlsgo.TestUtil) {
	res, err := db.Exec(`INSERT INTO ` + table.TableName() + ` (name,date,is_ok) VALUES ('init','` + ztime.Now() + `',1)`)
	tt.EqualExit(nil, err)
	id, err := res.LastInsertId()
	tt.EqualExit(nil, err)
	affected, err := res.RowsAffected()
	tt.EqualExit(nil, err)
	tt.EqualExit(1, int(affected))
	tt.EqualExit(1, int(id))

	_, _ = db.Exec(`INSERT INTO ` + table.TableName() + ` (name) VALUES ('test')`)
}

func baseQuery(db *zdb.Engine, t *testing.T, tt *zlsgo.TestUtil) {
	rows, err := db.Query(fmt.Sprintf("select * from %s", table.TableName()))
	tt.EqualExit(nil, err)
	rowsMap, err := zdb.ScanToMap(rows)
	tt.EqualExit(nil, err)
	t.Log(rowsMap)
	tt.EqualExit(2, len(rowsMap))
	tt.EqualExit("init", rowsMap[0]["name"])

	var users []TestTableUser
	rows, _ = db.Query(fmt.Sprintf("select * from %s", table.TableName()))
	err = zdb.Scan(rows, &users)
	tt.EqualExit(nil, err)
	t.Log(users)
	tt.EqualExit(2, len(users))
	tt.EqualExit(1, users[0].ID)
	tt.EqualExit("init", users[0].Name)
	tt.EqualExit(ztime.Now(), users[0].Date.String())
	t.Log(users[0].Date.Time())
	json, _ := zjson.Marshal(users)
	t.Log(string(json))

	var user TestTableUser
	rows, _ = db.Query(fmt.Sprintf("select * from %s where id = ? limit 1", table.TableName()), 1)
	err = zdb.Scan(rows, &user)
	tt.EqualExit(nil, err)
	tt.EqualExit(1, user.ID)
	tt.EqualExit("init", user.Name)
	tt.EqualExit(ztime.Now(), user.Date.String())

	json, _ = zjson.Marshal(user)
	t.Log(string(json))
}

func baseTransaction(db *zdb.Engine, t *testing.T, tt *zlsgo.TestUtil) {
	var err error
	for _, v := range [][]string{
		{"false", `INSERT INTO ` + table.TableName() + ` (name) VALUES ('Rollback -')`},
		{"true", `INSERT INTO ` + table.TableName() + ` (name) VALUES ('Commit -')`},
	} {
		ifRollback := v[0] == "false"
		rollbackErr := errors.New("测试回滚")
		err = db.Transaction(func(e *zdb.Engine) error {
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
	dbConf, err := getDbConf("0")
	zutil.CheckErr(err, true)

	db, err := zdb.New(dbConf)
	zutil.CheckErr(err, true)

	db.Options(func(o *zdb.Options) {
		o.MaxOpenConns = 1
		// o.ExecMaxLifetime = 1 * time.Second
		// o.ExecMaxLifetime = 3000 * time.Millisecond
		o.MaxIdleConns = 1
	})

	err = initTable(db, dbType)
	zutil.CheckErr(err, true)

	baseExec(db, tt)

	var g sync.WaitGroup
	ctx, can := context.WithTimeout(context.Background(), 15*time.Second)

	finish := make(chan struct{}, 1)
	now := time.Now()
	defer can()

	var ok int64 = 0
	var total int64 = 0
	var done int64 = 0
	max := 10
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			atomic.AddInt64(&total, 1)
			ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
			db.Source(func(db *zdb.Engine) error {
				_, err := db.Query("SELECT * FROM " + table.TableName())
				if err == nil {
					atomic.AddInt64(&ok, 1)
				} else {
					defer c()
				}
				atomic.AddInt64(&done, 1)
				return nil
			}, ctx)
			g.Done()
		}(i)
		i++
	}

	runCtx(t, finish, ctx, &g)
	t.Log(time.Since(now), "成功数量:", ok, "失败数量:", int(total-ok), "跑完数量:", done)
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(true, dbConf.DB().Stats().WaitCount > 6)
	tt.EqualExit(9, int(total-ok))
	tt.EqualExit(10, int(done))

	ok = 0
	total = 0
	done = 0
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			atomic.AddInt64(&total, 1)
			r, err := db.Query("SELECT * FROM  " + table.TableName())
			if err == nil {
				atomic.AddInt64(&ok, 1)
				err = r.Close()
			}
			atomic.AddInt64(&done, 1)
			g.Done()
			tt.EqualNil(err)
		}(i)
		i++
	}
	runCtx(t, finish, ctx, &g)
	t.Log(time.Since(now), "成功数量:", ok, "失败数量:", int(total-ok), "跑完数量:", done)
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(0, int(total-ok))
	tt.EqualExit(10, int(done))

	ok = 0
	total = 0
	done = 0
	for i := 0; ; {
		if i >= max {
			break
		}
		g.Add(1)
		go func(i int) {
			atomic.AddInt64(&total, 1)
			_, err := db.Query("SELECT * FROM  " + table.TableName())
			if err == nil {
				atomic.AddInt64(&ok, 1)
			}
			atomic.AddInt64(&done, 1)
			g.Done()
		}(i)
		i++
	}
	runCtx(t, finish, ctx, &g)

	t.Log(time.Since(now), "成功数量:", atomic.LoadInt64(&ok), "失败数量:", int(atomic.LoadInt64(&total)-atomic.LoadInt64(&ok)), "跑完数量:", atomic.LoadInt64(&done))
	t.Logf("%+v", dbConf.DB().Stats())
	tt.EqualExit(9, int(total-ok))
	tt.EqualExit(1, int(done))

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
