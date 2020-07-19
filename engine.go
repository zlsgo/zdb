package zdb

import (
	"context"
	"database/sql"
)

func (e *Engine) Exec(query string, args ...interface{}) (sql.Result, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)
	return db.execContext(db.ctx, query, args...)
}

func (e *Engine) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)
	return db.queryContext(db.ctx, query, args...)
}

func (e *Engine) Transaction(run DBCallback, ctx ...context.Context) error {
	if e.session == nil {
		db, err := e.getSession(nil, true, ctx...)
		if err != nil {
			return err
		}
		defer e.putSessionPool(db, true)
		return db.transaction(run)
	}
	return e.session.transaction(run)
}

func (e *Engine) Source(run DBCallback, ctx ...context.Context) error {
	s, err := e.getSession(nil, true, ctx...)
	if err != nil {
		return err
	}
	defer e.putSessionPool(s, true)
	return run(&Engine{
		session: s,
		isFixed: true,
	})
}

func (e *Engine) Replica(run DBCallback, ctx ...context.Context) error {
	s, err := e.getSession(nil, false, ctx...)
	if err != nil {
		return err
	}
	defer e.putSessionPool(s, true)
	return run(&Engine{
		session: s,
		isFixed: true,
	})
}
