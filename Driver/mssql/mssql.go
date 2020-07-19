package mssql

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"

	"github.com/sohaha/zdb"
)

var _ zdb.IfeConfig = &Config{}

// Config database configuration
type Config struct {
	db  *sql.DB
	Dsn string
}

func (c *Config) DB() *sql.DB {
	db, _ := c.MustDB()
	return db
}

func (c *Config) MustDB() (*sql.DB, error) {
	var err error
	if c.db == nil {
		c.db, err = sql.Open(c.GetDriver(), c.GetDsn())
	}
	return c.db, err
}

func (c *Config) SetDB(db *sql.DB) {
	c.db = db
}

func (c *Config) GetDsn() string {
	if c.Dsn != "" {
		return c.Dsn
	}
	return c.Dsn
}

func (c *Config) GetDriver() string {
	return "mssql"
}
