package zdb

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlConfig struct {
	db *sql.DB
}

var _ IfeConfig = &MssqlConfig{}

func (d *MssqlConfig) GetDB() *sql.DB {
	db, _ := d.GetDBE()
	return db
}

func (d *MssqlConfig) GetDBE() (*sql.DB, error) {
	var err error
	if d.db == nil {
		d.db, err = connect(d.getDriver(), d.getDsn())
	}
	return d.db, err
}

func (d *MssqlConfig) setDB(db *sql.DB) {
	d.db = db
}

func (m *MssqlConfig) getDsn() string {
	return ""
}
func (m *MssqlConfig) getDriver() string {
	return "mssql"
}
