package zdb

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/ztype"
)

type (
	ByteUnmarshaler interface {
		UnmarshalByte(data []byte) error
	}
	// IfeRows defines methods that scanner needs
	IfeRows interface {
		Close() error
		Columns() ([]string, error)
		Next() bool
		Scan(dest ...interface{}) error
	}
)

var (
	// ErrDBNotExist db not exist
	ErrDBNotExist = errors.New("database instance does not exist")

	errNoData      = sql.ErrNoRows
	errInsertEmpty = errors.New("insert data can not be empty")
	errDataInvalid = errors.New("data is illegal")
)

var convOption = func(conver *ztype.Conver) {
	conver.ConvHook = func(name string, i reflect.Value, o reflect.Type) (reflect.Value, bool) {
		if i.Type().AssignableTo(timeType) {
			return i.Convert(o), false
		}
		return i, true
	}
}

func Scan(rows IfeRows, out interface{}) (int, error) {
	data, count, err := resolveDataFromRows(rows)
	if err != nil {
		return 0, err
	}

	v := zreflect.ValueOf(out)
	if reflect.Indirect(v).Kind() != reflect.Slice {
		return count, ztype.ValueConv(data[0], v, convOption)
	}

	return count, ztype.ValueConv(data, v, convOption)
}

// ScanToMap returns the result in the form of []map[string]interface{}
func ScanToMap(rows IfeRows) (ztype.Maps, int, error) {
	return resolveDataFromRows(rows)
}

func resolveDataFromRows(rows IfeRows) (ztype.Maps, int, error) {
	result := make([]ztype.Map, 0)
	if nil == rows {
		return result, 0, ErrNotFound
	}
	columns, err := rows.Columns()
	if nil != err {
		return result, 0, err
	}
	length := len(columns)
	values := make([]interface{}, length)
	valuePtrs := make([]interface{}, length)
	count := 0
	for rows.Next() {
		for i := 0; i < length; i++ {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return result, 0, err
		}
		entry := make(ztype.Map, length)
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				entry[col] = zstring.Bytes2String(b)
			} else {
				entry[col] = val
			}
		}
		result = append(result, entry)
		count++
	}
	return result, count, nil
}
