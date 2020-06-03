package zdb_test

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/sohaha/zlsgo"

	"github.com/sohaha/zdb"
	"github.com/sohaha/zdb/Driver/sqlite3"
)

var (
	e    *zdb.Engine
	mock sqlmock.Sqlmock
	err  error
)

func TestMock(t *testing.T) {
	tt := zlsgo.NewTest(t)
	var db *sql.DB
	db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	tt.EqualNil(err)

	cfg := &sqlite3.Config{}
	cfg.SetDB(db)
	e, err = zdb.New(cfg)
	e.SetOptions(func(o *zdb.Options) {
		// o.MaxOpenConns = 20
	})
	rows := sqlmock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
	mock.ExpectQuery("SELECT * FROM users").WillReturnRows(rows)
	list, err := e.FindAllMap("SELECT * FROM users")
	tt.EqualNil(err)
	t.Log(list)

}
