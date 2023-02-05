package mysql

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zlsgo/zdb/driver"

	"github.com/sohaha/zlsgo/ztime"
	"github.com/sohaha/zlsgo/zutil"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config databaseName configuration
type Config struct {
	driver.Typ
	db         *sql.DB
	dsn        string
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

func (c *Config) SetDsn(dsn string) {
	c.dsn = dsn
}

func (c *Config) GetDsn() string {
	if c.dsn != "" {
		return c.dsn
	}

	loc := "Local"
	timezone := ztime.GetTimeZone().String()
	if timezone != "" {
		loc = url.QueryEscape(timezone)
		timezone = url.QueryEscape("'" + timezone + "'")
	}
	c.dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		c.User, c.Password, zutil.IfVal(c.Host == "", "127.0.0.1", c.Host), zutil.IfVal(c.Port == 0, 3306, c.Port), c.DBName, zutil.IfVal(c.Parameters == "", "parseTime=true&charset=utf8&loc="+loc+"&time_zone="+timezone, c.Parameters))
	return c.dsn
}

func (c *Config) GetDriver() string {
	return "mysql"
}

func (c *Config) Value() driver.Typ {
	return driver.MySQL
}
