package zdb

import (
	"reflect"

	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/ztype"
)

func (e *DB) QueryToMaps(query string, args ...interface{}) (ztype.Maps, error) {
	rows, err := e.Query(query, args...)
	if err != nil {
		return ztype.Maps{}, err
	}

	result, _, err := ScanToMap(rows)
	return result, err
}

func (e *DB) QueryTo(out interface{}, query string, args ...interface{}) error {
	rows, err := e.Query(query, args...)
	if err != nil {
		return err
	}

	result, _, err := ScanToMap(rows)
	if err != nil {
		return err
	}
	v := zreflect.ValueOf(out)
	if reflect.Indirect(v).Kind() != reflect.Slice {
		return ztype.ValueConv(result[0], v)
	}

	return ztype.ValueConv(result, v)
}
