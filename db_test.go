package zdb

import (
	"os"
	"strconv"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zfile"
)

var table *TestTableUser
var testDB *Engine
var testClusterDB *Engine
var dbs = []Config{
	{
		Driver: "sqlite3",
		Dsn:    "./test1.db",
	}, {
		Driver: "sqlite3",
		Dsn:    "./test2.db",
	}, {
		Driver: "sqlite3",
		Dsn:    "./test3.db",
	}, {
		Driver: "sqlite3",
		Dsn:    "./test4.db",
	},
}

type TestTableUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func (*TestTableUser) TableName() string {
	return "t_user"
}

func TestNewCluster(t *testing.T) {
	tt := zlsgo.NewTest(t)

	var err error
	testClusterDB, err = NewCluster(dbs...)
	t.Log(err)
	for _, v := range dbs {
		s, err := v.db.Exec(`CREATE TABLE "` + table.TableName() + `" ("id" integer,"name" text, PRIMARY KEY (id));`)
		tt.EqualNil(err)
		t.Log(s)
	}
}

func TestInitDB(t *testing.T) {
	var err error
	tt := zlsgo.NewTest(t)
	testDB, err = New("sqlite3", "./test1.db")
	tt.EqualNil(err)

	// s, err := testDB.Exec(`CREATE TABLE "` + table.TableName() + `" ("id" integer,"name" text, PRIMARY KEY (id));`)
	// tt.EqualNil(err)
	// t.Log(s)

	s, err := testDB.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('ok')`)
	tt.EqualNil(err)
	t.Log(s)
}

func TestExec(t *testing.T) {
	var g sync.WaitGroup
	tt := zlsgo.NewTest(t)
	for i := 0; i < 100; i++ {
		g.Add(1)
		go func(i int) {
			db, _ := testClusterDB.Slave()
			_, err := db.Exec(`INSERT INTO  "` + table.TableName() + `" ("name") VALUES ('Exec-` + strconv.Itoa(i) + `')`)
			tt.EqualNil(err)
			g.Done()
		}(i)
	}
	g.Wait()
	for _, v := range dbs {
		rows, err := v.db.Query("select * from "+table.TableName()+" where id > ?", 0)
		tt.EqualNil(err)
		data, err := ScanMap(rows)
		t.Log(len(data), err)
		tt.EqualNil(err)
	}
	t.Log(testClusterDB.vs)
}

func TestTransaction(t *testing.T) {
	tt := zlsgo.NewTest(t)
	for _, v := range [][]string{
		{"false", `INSERT INTO "` + table.TableName() + `" ("name") VALUES ('Rollback')`},
		{"true", `INSERT INTO "` + table.TableName() + `" ("name") VALUES ('Commit')`},
	} {
		// 开始事务
		s, err := testDB.Begin()
		tt.EqualNil(err)
		_, err = s.Exec(v[1])
		tt.EqualNil(err)
		if v[0] == "false" {
			err = testDB.Rollback(s)
			t.Log("回滚")
		} else {
			err = testDB.Commit(s)
			t.Log("提交")
		}
		tt.EqualNil(err)
	}
}

func TestQuery(t *testing.T) {
	tt := zlsgo.NewTest(t)
	rows, err := testDB.Query("select * from " + table.TableName())
	tt.EqualNil(err)
	data, err := ScanMap(rows)
	t.Log(data, err)
	tt.EqualNil(err)
}

func TestMain(m *testing.M) {
	s := m.Run()

	zfile.Rmdir("./test1.db")
	zfile.Rmdir("./test2.db")
	zfile.Rmdir("./test3.db")
	zfile.Rmdir("./test4.db")

	os.Exit(s)
}
