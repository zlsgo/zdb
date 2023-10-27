//go:build cgo
// +build cgo

package sqlite3

import (
	"database/sql"

	"github.com/mattn/go-sqlite3"
	"github.com/sohaha/zlsgo/zfile"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

func init() {
	sql.Register("sqlite",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				_, err := conn.Exec(`
					PRAGMA busy_timeout       = 10000;
					PRAGMA journal_mode       = WAL;
					PRAGMA journal_size_limit = 200000000;
					PRAGMA synchronous        = NORMAL;
					PRAGMA foreign_keys       = ON;
					PRAGMA temp_store         = MEMORY;
					PRAGMA cache_size         = -16000;
				`, nil)

				return err
			},
		},
	)
}

func (c *Config) GetDriver() string {
	return "sqlite"
}

func (c *Config) GetDsn() string {
	if c.dsn == "" {
		c.Typ = driver.SQLite
		f := c.File
		if f == "" {
			f = "zlsgo.db"
		}
		c.dsn = "file:" + zfile.RealPath(f) + zutil.IfVal(c.Memory, "?cache=shared&mode=memory", "?").(string) + "&" + c.Parameters
	}

	return c.dsn
}
