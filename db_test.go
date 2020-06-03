package zdb

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zfile"
	"github.com/sohaha/zlsgo/zstring"

	"github.com/sohaha/zdb/Driver/sqlite3"
)

var (
	table         *TestTableUser
	testDB        *Engine
	testClusterDB *Engine
	c             = &sqlite3.Config{
		File: "./test1.db",
	}
	dbs = []IfeConfig{
		&sqlite3.Config{
			File: "./test1.db",
		},
		&sqlite3.Config{
			File: "./test2.db",
		},
		&sqlite3.Config{
			File: "./test3.db",
		},
		&sqlite3.Config{
			File: "./test4.db",
		},
	}
)

type TestTableUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func (*TestTableUser) TableName() string {
	return "t_user"
}

func TestNewCluster(t *testing.T) {
	var err error
	tt := zlsgo.NewTest(t)
	testClusterDB, err = NewCluster(dbs...)
	t.Log(err)
	for k, v := range dbs {
		s, err := v.GetDB().Exec(`CREATE TABLE "` + table.TableName() + `" ("id" integer,"name" text, PRIMARY KEY (id));`)
		t.Log(k, s, err)
		tt.EqualExit(true, err == nil)
	}
}

func TestInitDB(t *testing.T) {
	var err error
	tt := zlsgo.NewTest(t)

	_, err = c.GetDBE()
	t.Log("sql.DB", err)

	testDB, err = New(c)
	tt.EqualNil(err)

	// s, err := testDB.Exec(`CREATE TABLE "` + table.TableName() + `" ("id" integer,"name" text, PRIMARY KEY (id));`)
	// tt.EqualNil(err)
	// t.Log(s)

	_, err = testDB.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('init')`)
	tt.EqualNil(err)
	_, err = testDB.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('ok')`)
	tt.EqualNil(err)
}

func TestExec(t *testing.T) {
	if testClusterDB == nil {
		TestNewCluster(t)
	}
	var g sync.WaitGroup
	tt := zlsgo.NewTest(t)
	for i := 0; i < 60; i++ {
		g.Add(1)
		go func(i int) {
			time.Sleep(time.Duration(zstring.RandInt(4000, 16000)) * time.Nanosecond)
			var db *Engine
			db, _ = testClusterDB.Slave()
			_, err := db.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('Exec-` + strconv.Itoa(i) + `')`)
			tt.EqualNil(err)
			g.Done()
		}(i)
	}
	g.Wait()
	db, _ := testClusterDB.Master()
	_, err := db.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('Master')`)
	tt.EqualNil(err)
	for _, v := range dbs {
		rows, err := v.GetDB().Query("select * from "+table.TableName()+" where id > ?", 0)
		tt.EqualNil(err)
		data, err := ScanMap(rows)
		t.Log(len(data), err)
		tt.EqualNil(err)
	}
	// t.Log(testClusterDB.vs)
}

func TestTransaction(t *testing.T) {
	var err error
	tt := zlsgo.NewTest(t)
	for _, v := range [][]string{
		{"false", `INSERT INTO "` + table.TableName() + `" ("name") VALUES ('Rollback -')`},
		{"true", `INSERT INTO "` + table.TableName() + `" ("name") VALUES ('Commit -')`},
	} {
		ifRollback := v[0] == "false"
		err = testDB.Transaction(func(e *Engine) error {
			_, err := e.Exec(v[1])
			if err != nil {
				return err
			}
			if ifRollback {
				return errors.New("测试回滚")
			}
			return nil
		})
		t.Log(err)
		_, err = testDB.Exec(v[1])
		t.Log(err)
		if !ifRollback {
			tt.EqualNil(err)
		}
	}
}

func TestQuery(t *testing.T) {
	tt := zlsgo.NewTest(t)
	rows, err := testDB.Query("select * from " + table.TableName())
	tt.EqualNil(err)
	data, err := ScanMap(rows)
	t.Log(data, err)
	validData := []string{
		"init",
		"ok",
		"Master",
		"Rollback -",
		"Commit -",
		"Commit -",
	}
	for _, v := range data {
		for i, vv := range validData {
			if v["name"] == vv {
				validData = append(validData[:i], validData[i+1:]...)
				break
			}
		}
	}
	tt.EqualExit([]string{}, validData)
	tt.EqualNil(err)
	t.Log(c.GetDB().Stats())
	testDB.SetOptions(func(o *Options) {
		o.MaxOpenConns = 1
		o.MaxIdleConns = 1
	})
	for i := 0; i < 100; i++ {
		r, err := testClusterDB.Query("select * from " + table.TableName())
		if err == nil {
			r.Close()
		} else {
			t.Log(err)
		}
		tt.EqualNil(err)
	}

	var g sync.WaitGroup
	for i := 0; i < 10; i++ {
		g.Add(1)
		go func(i int) {
			r, err := testDB.Query("select * from " + table.TableName())
			if err == nil {
				time.Sleep(time.Duration(zstring.RandInt(100, 1000)) * time.Microsecond)
				r.Close()
			} else {
				t.Log(err)
			}
			tt.EqualNil(err)
			_, err = testDB.Exec(`INSERT INTO "` + table.TableName() +
				`" ("name") VALUES ('pool -` + strconv.Itoa(i) + `')`)

			tt.EqualNil(err)

			_, err = testDB.FindAllMap("select * from "+table.TableName()+" where id = ?", 1)
			tt.EqualNil(err)
			g.Done()
		}(i)
	}
	g.Wait()
	t.Logf("%+v", c.GetDB().Stats())
	tt.EqualExit(true, int(c.GetDB().Stats().WaitCount) > 20)
}

func TestMain(m *testing.M) {
	s := m.Run()

	zfile.Rmdir("./test1.db")
	zfile.Rmdir("./test2.db")
	zfile.Rmdir("./test3.db")
	zfile.Rmdir("./test4.db")

	os.Exit(s)
}
