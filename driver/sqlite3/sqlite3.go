package sqlite3

import (
	"database/sql"

	"github.com/sohaha/zlsgo/zfile"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var _ driver.IfeConfig = &Config{}
var _ driver.Dialect = &Config{}

// Config database configuration
type Config struct {
	driver.Typ
	File        string
	Dsn         string
	Parameters  string
	db          *sql.DB
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

func (c *Config) GetDsn() string {
	if c.Dsn != "" {
		return c.Dsn
	}
	c.Typ = driver.SQLite
	f := c.File
	if f == "" {
		f = "zlsgo.db"
	}
	return "file:" + zfile.RealPath(f) + zutil.IfVal(c.Memory, "?cache=shared&mode=memory", "?").(string) + "&" + c.Parameters
}

func (c *Config) Value() driver.Typ {
	return driver.SQLite
}
