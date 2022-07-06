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
	// ExecMaxLifetime ExecMaxLifetime
	// ExecMaxLifetime time.Duration
}

var defOption = Options{
	MaxIdleConns:    0,
	MaxOpenConns:    0,
	ConnMaxLifetime: 0,
}

func (e *DB) Options(fn func(o *Options)) {
	options := defOption
	fn(&options)
	// e.execMaxLifetime = options.ExecMaxLifetime
	for _, p := range e.pools {
		p.db.SetMaxIdleConns(options.MaxIdleConns)
		p.db.SetConnMaxLifetime(options.ConnMaxLifetime)
		p.db.SetMaxOpenConns(options.MaxOpenConns)
	}
}
