package zdb

import (
	"database/sql"
	"fmt"
)

func ScanMap(rows *sql.Rows) (list []map[string]interface{}, err error) {
	list = make([]map[string]interface{}, 0)
	if rows == nil {
		err = fmt.Errorf("rows is a pointer, but not be a nil")
		return
	}
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index := range cache {
		var a interface{}
		cache[index] = &a
	}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *data.(*interface{})
		}
		list = append(list, item)
	}
	defer rows.Close()
	return list, rows.Err()
}
