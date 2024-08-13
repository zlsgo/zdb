package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config database configuration
type Config struct {
	db       *sql.DB
	dsn      string
	Host     string
	User     string
	Password string
	DBName   string
	SSLMode  string
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
	if c.dsn == "" {
		c.dsn = fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
			zutil.IfVal(c.Host == "", "127.0.0.1", c.Host), zutil.IfVal(c.Port == 0, 5432, c.Port), c.User, c.DBName, c.Password, zutil.IfVal(c.SSLMode == "", "disable", c.SSLMode))
	}

	return c.dsn
}

func (c *Config) GetDriver() string {
	return "postgres"
}

func (c *Config) Value() driver.Typ {
	return driver.PostgreSQL
}
