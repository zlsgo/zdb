package zdb

import (
	"context"
	"database/sql"

	"github.com/zlsgo/zdb/driver"
)

func (e *DB) GetDriver() driver.Dialect {
	return e.driver
}

func (e *DB) Exec(sql string, values ...interface{}) (sql.Result, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	return db.execContext(db.ctx, sql, values...)
}

func (e *DB) Query(sql string, values ...interface{}) (*sql.Rows, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	return db.queryContext(db.ctx, sql, values...)
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
	return run(&nEngine)
}
