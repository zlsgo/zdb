// Package zdb provides ...
package zdb

import (
	"database/sql"
	"time"
)

type Options struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

var defOption = Options{
	MaxIdleConns:    0,
	MaxOpenConns:    0,
	ConnMaxLifetime: 0,
}

func connect(driver, dsn string) (*sql.DB, error) {
	return sql.Open(driver, dsn)
}

func (c *Engine) SetOptions(fn func(o *Options)) {
	options := defOption
	fn(&options)
	for _, p := range c.pools {
		p.db.SetMaxIdleConns(options.MaxIdleConns)
		p.db.SetConnMaxLifetime(options.ConnMaxLifetime)
		p.db.SetMaxOpenConns(options.MaxOpenConns)
	}
}
