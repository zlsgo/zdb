package zdb

import "database/sql"

func (e *Engine) FindAllMap(query string, args ...interface{}) (list []map[string]interface{}, err error) {
	var db *Session
	db, err = e.getSession(nil, true)
	if err != nil {
		return
	}
	defer e.putSessionPool(db, false)
	var rows *sql.Rows
	rows, err = e.Query(query, args...)
	if err != nil {
		return
	}
	return ScanMap(rows)
}
