package zdb

import (
	"context"
	"database/sql"
	"sync"

	"github.com/sohaha/zlsgo/zstring"
)

type Session struct {
	tx     *sql.Tx
	config *Config
	ctx    context.Context
}

type DBCallback func(e *DB) error

var sessionPool = sync.Pool{
	New: func() interface{} {
		return &Session{}
	},
}

func (e *DB) getSessionPool() *Session {
	s, _ := sessionPool.Get().(*Session)
	return s
}

func (e *DB) putSessionPool(s *Session, force bool) {
	if e.session != nil && !force && !e.force {
		return
	}
	s.tx = nil
	s.config = nil
	sessionPool.Put(s)
}

// Debug ...
var Debug = false

func (e *DB) getSession(s *Session, master bool, ctx ...context.Context) (*Session, error) {
	if e.session != nil {
		return e.session, nil
	}
	n := len(e.pools)
	if n == 0 {
		return nil, ErrDBNotExist
	}
	if s != nil {
		return s, nil
	}
	s = e.getSessionPool()
	if len(ctx) > 0 && ctx[0] != nil {
		s.ctx = ctx[0]
	} else {
		s.ctx = context.Background()
	}
	i := 0
	if !master {
		if n > 1 {
			i = zstring.RandInt(1, n-1)
		}
	}
	s.config = e.pools[i]
	return s, nil
}

func (s *Session) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var err error
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, query)
	} else {
		stmt, err = s.config.db.PrepareContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (s *Session) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := s.config.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (s *Session) transaction(run DBCallback) error {
	db, err := s.config.db.Begin()
	if err != nil {
		return err
	}
	s.tx = db
	e := &DB{
		session: s,
	}
	err = run(e)
	if err != nil {
		_ = db.Rollback()
		return err
	}
	defer func() {
		s.tx = nil
	}()
	return db.Commit()
}
