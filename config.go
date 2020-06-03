// Package zdb provides ...
package zdb

import (
	"time"
)

type Options struct {
	// MaxIdleConns maximum hold connections
	MaxIdleConns int
	// MaxOpenConns maximum connection
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

var defOption = Options{
	MaxIdleConns:    0,
	MaxOpenConns:    0,
	ConnMaxLifetime: 0,
}

func (e *Engine) SetOptions(fn func(o *Options)) {
	options := defOption
	fn(&options)
	for _, p := range e.pools {
		p.db.SetMaxIdleConns(options.MaxIdleConns)
		p.db.SetConnMaxLifetime(options.ConnMaxLifetime)
		p.db.SetMaxOpenConns(options.MaxOpenConns)
	}
}
