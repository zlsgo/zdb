package zdb

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type Sqlite3Config struct {
	File string
	db   *sql.DB
}

var _ IfeConfig = &Sqlite3Config{}

func (d *Sqlite3Config) GetDB() *sql.DB {
	db, _ := d.GetDBE()
	return db
}

func (d *Sqlite3Config) GetDBE() (*sql.DB, error) {
	var err error
	if d.db == nil {
		d.db, err = connect(d.getDriver(), d.getDsn())
	}
	return d.db, err
}

func (d *Sqlite3Config) setDB(db *sql.DB) {
	d.db = db
}

func (d *Sqlite3Config) getDsn() string {
	return d.File
}

func (d *Sqlite3Config) getDriver() string {
	return "sqlite3"
}
