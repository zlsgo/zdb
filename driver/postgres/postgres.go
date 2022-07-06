package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config database configuration
type Config struct {
	driver.Typ
	db       *sql.DB
	Dsn      string
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
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
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
		c.Host, c.Port, c.User, c.DBName, c.Password, c.SSLMode)
}

func (c *Config) GetDriver() string {
	return "postgres"
}

func (c *Config) Value() driver.Typ {
	return driver.PostgreSQL
}
