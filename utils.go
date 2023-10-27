package zdb

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/sohaha/zlsgo/zlog"
	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
)

var (
	timeType        = reflect.TypeOf(time.Time{})
	jsontimeType    = reflect.TypeOf(JsonTime{})
	timePtrType     = reflect.TypeOf(&time.Time{})
	jsontimePtrType = reflect.TypeOf(&JsonTime{})
	log             = zlog.New("[zdb] ")
)

func init() {
	log.ResetFlags(zlog.BitLevel)
}

func (j JsonTime) String() string {
	t := time.Time(j)
	if t.IsZero() {
		return "0000-00-00 00:00:00"
	}
	return t.Format("2006-01-02 15:04:05")
}

func (j JsonTime) Time() time.Time {
	return time.Time(j)
}

func (j JsonTime) MarshalJSON() ([]byte, error) {
	res := bytes.NewBufferString("\"")
	res.WriteString(j.String())
	res.WriteString("\"")
	return res.Bytes(), nil
}

func parseQuery(e *DB, b builder.Builder) (ztype.Maps, error) {
	sql, values, err := b.Build()
	if err != nil {
		return make(ztype.Maps, 0), err
	}

	if e.Debug {
		zlog.Debug(sql, values)
	}

	rows, err := e.Query(sql, values...)
	if err != nil {
		return make(ztype.Maps, 0), err
	}

	result, total, err := ScanToMap(rows)
	if total == 0 {
		return make(ztype.Maps, 0), ErrNotFound
	}

	return result, err
}

func parseExec(e *DB, b builder.Builder) (int64, error) {
	sql, values, err := b.Build()
	if err != nil {
		return 0, err
	}

	if e.Debug {
		zlog.Debug(sql, values)
	}

	result, err := e.Exec(sql, values...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func parseMaps(val []map[string]interface{}) (cols []string, args [][]interface{}, err error) {
	colsLen := 0
	for i := 0; i < len(val); i++ {
		v := val[i]
		if i == 0 {
			colArgs := make([]interface{}, 0, len(v))
			for key := range v {
				v := v[key]
				cols = append(cols, key)
				colArgs = append(colArgs, v)
			}
			args = append(args, colArgs)
			colsLen = len(cols)
		} else {
			colArgs := make([]interface{}, 0, colsLen)
			for ii := 0; ii < colsLen; ii++ {
				key := cols[ii]
				val, ok := v[key]
				if !ok {
					return nil, nil, errors.New("invalid values[" + strconv.FormatInt(int64(i), 10) + "] for column: " + key)
				}
				colArgs = append(colArgs, val)
			}
			args = append(args, colArgs)
		}
	}
	return cols, args, nil
}

func parseValues(data interface{}) (cols []string, args [][]interface{}, err error) {
	if data == nil {
		return nil, nil, errNoData
	}

	switch val := data.(type) {
	case map[string]string:
		l := len(val)
		cols = make([]string, 0, l)
		colArgs := make([]interface{}, 0, l)
		for key := range val {
			v := val[key]
			cols = append(cols, key)
			colArgs = append(colArgs, v)
		}
		args = append(args, colArgs)
	case map[string]interface{}:
		return parseMap(val, nil)
	case ztype.Map:
		return parseMap(*(*map[string]interface{})(unsafe.Pointer(&val)), nil)
	case []map[string]interface{}:
		return parseMaps(val)
	case ztype.Maps:
		return parseMaps(*(*[]map[string]interface{})(unsafe.Pointer(&val)))
	default:
		err = errDataInvalid
	}

	return cols, args, err
}

func parseStruct(data interface{}) (cols []string, args [][]interface{}, err error) {
	vof := reflect.ValueOf(data)
	vof = reflect.Indirect(vof)
	kind := vof.Kind()
	if kind == reflect.Struct {
		typ := vof.Type()
		numField := vof.NumField()
		cols = make([]string, 0, numField)
		colArgs := make([]interface{}, 0, numField)
		for i := 0; i < numField; i++ {
			field := vof.Field(i)
			if field.IsZero() {
				continue
			}
			v := field.Interface()
			structField := typ.Field(i)
			name := structField.Name
			if zstring.IsLcfirst(name) {
				continue
			}
			tag, _ := zreflect.GetStructTag(structField)
			if tag != "" {
				name = tag
			}
			cols = append(cols, name)
			colArgs = append(colArgs, v)
		}

		args = append(args, colArgs)
		return
	} else if kind == reflect.Slice {
		for i := 0; i < vof.Len(); i++ {
			val := vof.Index(i).Interface()
			col, arg, err := parseStruct(val)
			if err != nil {
				return nil, nil, err
			}
			if i == 0 {
				cols = col
			}
			args = append(args, arg[0])
		}
		return
	}

	err = errors.New("insert data is illegal")
	return
}

func parseAll(data interface{}) (cols []string, args [][]interface{}, err error) {
	cols, args, err = parseValues(data)
	if err != nil && err == errDataInvalid {
		cols, args, err = parseStruct(data)
	}
	return
}

func parseMap(val ztype.Map, specify []string) ([]string, [][]interface{}, error) {
	valLen := len(val)
	cols := make([]string, 0, valLen)
	colArgs := make([]interface{}, 0, valLen)
	l := len(specify)
	if l > 0 {
		for k := range specify {
			val, ok := val[specify[k]]
			if ok {
				cols = append(cols, specify[k])
				colArgs = append(colArgs, val)
			}
		}
		if len(cols) != l {
			return nil, nil, errors.New("invalid values for column: " + strings.Join(specify, ","))
		}
	} else {
		for key := range val {
			cols = append(cols, key)
			colArgs = append(colArgs, val[key])
		}
	}

	return cols, [][]interface{}{colArgs}, nil
}

func parseMaps2(val ztype.Maps) ([]string, [][]interface{}, error) {
	valLen := len(val)
	if valLen == 0 {
		return nil, nil, errDataInvalid
	}
	var cols []string
	colArgs := make([][]interface{}, 0, valLen)
	for i := range val {
		c, a, err := parseMap(val[i], cols)
		if err != nil {
			return nil, nil, err
		}
		if i == 0 {
			cols = c
		}
		colArgs = append(colArgs, a[0])
	}
	return cols, colArgs, nil
}
