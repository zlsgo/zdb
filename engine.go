package zdb

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/sohaha/zlsgo/zstring"
)

var sessionPool = sync.Pool{
	New: func() interface{} {
		return &Session{
			// engine: e,
		}
	},
}

func (c *Engine) getSessionPool() *Session {
	s, _ := sessionPool.Get().(*Session)
	// s.v = atomic.AddUint64(&c.vs, 1)
	return s
}

func (c *Engine) putSessionPool(s *Session, force bool) {
	if c.session != nil && !force && !c.force {
		return
	}
	// s.config.db.Close()
	s.tx = nil
	s.config = nil
	sessionPool.Put(s)
}

func (c *Engine) getSession(s *Session, master bool) (*Session, error) {
	if c.session != nil {
		return c.session, nil
	}
	n := len(c.pools)
	if n == 0 {
		return nil, errors.New("not found db")
	}
	if s != nil {
		return s, nil
	}
	s = c.getSessionPool()
	s.ctx = context.Background()
	if master {
		s.config = c.pools[0]
	} else {
		var i int
		if n > 1 {
			i = zstring.RandInt(1, n-1)
			// i = 1 + int(s.v)%(n-1)
		}
		s.config = c.pools[i]
	}
	return s, nil
}

func (e *Engine) Master() (*Engine, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	return &Engine{
		session: db,
		force:   true,
	}, nil
}

func (e *Engine) Slave() (*Engine, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	return &Engine{
		session: db,
		force:   true,
	}, nil
}

func (e *Engine) Exec(query string, args ...interface{}) (sql.Result, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)
	return db.exec(query, args...)
}

func (e *Engine) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)
	return db.query(query, args...)
}

func (e *Engine) Transaction(run TransactionFn) error {
	db, err := e.getSession(nil, true)
	if err != nil {
		return err
	}
	defer e.putSessionPool(db, true)
	return db.transaction(run)
}
