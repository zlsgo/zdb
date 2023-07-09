package zdb

import (
	"context"
	"database/sql"

	"github.com/zlsgo/zdb/driver"
)

func (e *DB) GetDriver() driver.Dialect {
	return e.pools[0].driver
}

func (e *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)
	return db.execContext(db.ctx, query, args...)
}

func (e *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	return db.queryContext(db.ctx, query, args...)
}

func (e *DB) Transaction(run DBCallback, ctx ...context.Context) error {
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

func (e *DB) Source(run DBCallback, ctx ...context.Context) error {
	s, err := e.getSession(nil, true, ctx...)
	if err != nil {
		return err
	}
	defer e.putSessionPool(s, true)

	nEngine := *e
	nEngine.session = s
	nEngine.isFixed = true
	return run(&nEngine)
}

func (e *DB) Replica(run DBCallback, ctx ...context.Context) error {
	s, err := e.getSession(nil, false, ctx...)
	if err != nil {
		return err
	}
	defer e.putSessionPool(s, true)

	nEngine := *e
	nEngine.session = s
	nEngine.isFixed = true
	return run(&nEngine)
}
