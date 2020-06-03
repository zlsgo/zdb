package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// Config database configuration
type Config struct {
	db *sql.DB
}

func (c *Config) GetDB() *sql.DB {
	db, _ := c.GetDBE()
	return db
}

func (c *Config) GetDBE() (*sql.DB, error) {
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
	return ""
}

func (c *Config) GetDriver() string {
	return "mysql"
}
