package zdb

import (
	"github.com/zlsgo/zdb/driver"
)

func (e *DB) Migration(fn func(db *DB, d driver.Dialect) error) error {
	s, err := e.getSession(nil, true)
	if err != nil {
		return err
	}
	defer e.putSessionPool(s, false)

	nEngine := *e
	nEngine.session = s
	return fn(&nEngine, s.config.driver)
}
