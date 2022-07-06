package zdb

func (e *DB) QueryToMaps(query string, args ...interface{}) ([]map[string]interface{}, error) {
	db, err := e.getSession(nil, false)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	rows, err := db.queryContext(db.ctx, query, args...)

	if err != nil {
		return nil, err
	}

	result, _, err := ScanToMap(rows)
	return result, err
}
