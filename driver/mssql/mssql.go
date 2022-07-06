package mssql

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config database configuration
type Config struct {
	driver.Typ
	db         *sql.DB
	Dsn        string
	Host       string
	Port       int
	User       string
	Password   string
	DBName     string
	Parameters string
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
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s?%s",
		c.User, c.Password, zutil.IfVal(c.Host == "", "127.0.0.1", c.Host), zutil.IfVal(c.Port == 0, 1433, c.Port), c.DBName, c.Parameters)
}

func (c *Config) GetDriver() string {
	return "mssql"
}

func (c *Config) Value() driver.Typ {
	return driver.MsSQL
}
