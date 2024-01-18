package zdb

import (
	"github.com/zlsgo/zdb/driver"
)

func (e *DB) Migration(fn func(db *DB, d driver.Dialect) error) error {
	db, err := e.getSession(nil, false)
	if err != nil {
		return err
	}
	defer e.putSessionPool(db, false)
	return fn(e, db.config.driver)
}
