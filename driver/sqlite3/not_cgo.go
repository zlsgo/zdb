//go:build !cgo
// +build !cgo

package sqlite3

import (
	"github.com/sohaha/zlsgo/zfile"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
	_ "modernc.org/sqlite"
)

func (c *Config) GetDriver() string {
	return "sqlite"
}

func (c *Config) GetDsn() string {
	pragmas := "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=journal_size_limit(200000000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-16000)&_txlock=immediate"
	if c.Parameters != "" {
		pragmas = "?" + c.Parameters
	}
	if c.dsn == "" {
		c.Typ = driver.SQLite
		f := c.File
		if f == "" {
			f = "zlsgo.db"
		}
		c.dsn = "file:" + zfile.RealPath(f) + pragmas + zutil.IfVal(c.Memory, "&cache=shared&mode=memory", "").(string)
	}

	return c.dsn
}
