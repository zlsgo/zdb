// Package zdb provides ...
package zdb

import (
	"time"
)

type Options struct {
	// MaxIdleConns maximum hold connections
	MaxIdleConns int
	// MaxOpenConns maximum connection
	MaxOpenConns int
	// ConnMaxLifetime ConnMaxLifetime
	ConnMaxLifetime time.Duration
}

var defOption = Options{
	MaxIdleConns:    10,
	MaxOpenConns:    100,
	ConnMaxLifetime: time.Hour / 2,
}

func (e *DB) Options(fn func(o *Options)) {
	options := defOption
	fn(&options)
	for _, p := range e.pools {
		p.db.SetMaxIdleConns(options.MaxIdleConns)
		p.db.SetConnMaxLifetime(options.ConnMaxLifetime)
		p.db.SetMaxOpenConns(options.MaxOpenConns)
	}
}
