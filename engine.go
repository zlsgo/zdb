package zdb

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
)

func (c *Engine) putSession(s *Session) {
	c.sessionPool.Put(s)
}

func (c *Engine) getSession(s *Session, master bool) (*Session, error) {
	n := len(c.pools)
	if n == 0 {
		return nil, errors.New("not found db")
	}
	if s != nil {
		return s, nil
	}
	s = c.sessionPool.Get().(*Session)
	s.v = atomic.AddUint64(&c.vs, 1)
	s.ctx = context.Background()
	if master {
		s.config = c.pools[0]
	} else {
		var i int
		if n > 1 {
			i = 1 + int(s.v)%(n-1)
		}
		s.config = c.pools[i]
	}
	return s, nil
}

func (e *Engine) Master(query string, args ...interface{}) (*Session, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (e *Engine) Slave() (*Session, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (e *Engine) Exec(query string, args ...interface{}) (sql.Result, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	return db.Exec(query, args...)
}

func (e *Engine) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	return db.Query(query, args...)
}

func (e *Engine) Begin() (*Session, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	return db, db.Begin()
}

func (e *Engine) Rollback(s *Session) error {
	db, err := e.getSession(s, false)
	if err != nil {
		return err
	}
	return db.Rollback()
}

func (e *Engine) Commit(s *Session) error {
	db, err := e.getSession(s, false)
	if err != nil {
		return err
	}
	return db.Commit()
}
