package zdb

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlConfig struct {
	db *sql.DB
}

var _ IfeConfig = &MysqlConfig{}

func (d *MysqlConfig) GetDB() *sql.DB {
	db, _ := d.GetDBE()
	return db
}

func (d *MysqlConfig) GetDBE() (*sql.DB, error) {
	var err error
	if d.db == nil {
		d.db, err = connect(d.getDriver(), d.getDsn())
	}
	return d.db, err
}

func (d *MysqlConfig) setDB(db *sql.DB) {
	d.db = db
}

func (m *MysqlConfig) getDsn() string {
	return ""
}
func (m *MysqlConfig) getDriver() string {
	return "mysql"
}
