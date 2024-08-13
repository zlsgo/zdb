package sqlite3

import (
	"database/sql"

	"github.com/zlsgo/zdb/driver"
)

var _ driver.IfeConfig = &Config{}
var _ driver.Dialect = &Config{}

// Config database configuration
type Config struct {
	db         *sql.DB
	File       string
	dsn        string
	Parameters string
	driver.Typ
	Memory      bool
	ForeignKeys bool
}

func (c *Config) DB() *sql.DB {
	db, _ := c.MustDB()
	return db
}

func (c *Config) MustDB() (*sql.DB, error) {
	var err error
	if c.db == nil {
		c.db, err = sql.Open(c.GetDriver(), c.GetDsn())
		if c.db != nil && c.ForeignKeys {
			_, _ = c.db.Exec("PRAGMA foreign_keys = ON")
		}
	}
	return c.db, err
}

func (c *Config) SetDB(db *sql.DB) {
	c.db = db
}

func (c *Config) SetDsn(dsn string) {
	c.dsn = dsn
}

func (c *Config) Value() driver.Typ {
	return driver.SQLite
}
