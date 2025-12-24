package mssql

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config database configuration
type Config struct {
	db         *sql.DB
	dsn        string
	Host       string
	User       string
	Password   string
	DBName     string
	Parameters string
	driver.Typ
	Port int
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

func (c *Config) SetDsn(dsn string) {
	c.dsn = dsn
}

func (c *Config) GetDsn() string {
	if c.dsn != "" {
		return c.dsn
	}
	params := c.Parameters
	if len(params) > 0 {
		if params[0] == '?' || params[0] == '&' {
			params = params[1:]
		}
	}
	c.dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
		c.User, c.Password, ztype.ToString(zutil.IfVal(c.Host == "", "127.0.0.1", c.Host)), ztype.ToInt(zutil.IfVal(c.Port == 0, 1433, c.Port)), c.DBName)
	if params != "" {
		c.dsn += "&" + params
	}
	return c.dsn
}

func (c *Config) GetDriver() string {
	return "mssql"
}

func (c *Config) Value() driver.Typ {
	return driver.MsSQL
}
