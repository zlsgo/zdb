package zdb

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/zutil"
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
	if e.session != nil && !force {
		return
	}
	s.tx = nil
	s.config = nil
	sessionPool.Put(s)
}

var Debug = zutil.NewBool(false)

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
	if Debug.Load() {
		now := time.Now()
		defer func() {
			log.Debugf("SQL [%s]: %s %v\n", time.Since(now), query, args)
		}()
	}

	if s.tx != nil {
		return s.tx.ExecContext(ctx, query, args...)
	}
	return s.config.db.ExecContext(ctx, query, args...)
}

func (s *Session) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if Debug.Load() {
		now := time.Now()
		defer func() {
			log.Debugf("SQL [%s]: %s %v\n", time.Since(now), query, args)
		}()
	}

	if s.tx != nil {
		return s.tx.QueryContext(ctx, query, args...)
	}
	return s.config.db.QueryContext(ctx, query, args...)
}

func (s *Session) transaction(run DBCallback) error {
	if s.tx != nil {
		e := &DB{
			session: s,
			driver:  s.config.driver,
		}
		return run(e)
	}
	db, err := s.config.db.Begin()
	if err != nil {
		return err
	}
	s.tx = db
	defer func() {
		s.tx = nil
	}()
	e := &DB{
		session: s,
		driver:  s.config.driver,
	}
	err = run(e)
	if err != nil {
		_ = db.Rollback()
		return err
	}
	return db.Commit()
}
