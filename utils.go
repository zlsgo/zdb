package zdb

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/zstring"
	"github.com/zlsgo/zdb/builder"
)

var (
	timeType     = reflect.TypeOf(time.Time{})
	jsontimeType = reflect.TypeOf(JsonTime{})
)

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

type QuoteData struct {
	data interface{}
}

func QuoteCols(data interface{}) *QuoteData {
	return &QuoteData{data: data}
}

func (e *DB) QuoteCols(cols []string) []string {
	d := e.driver.Value()
	nm := make([]string, 0, len(cols))

	for i := range cols {
		col := cols[i]
		if strings.IndexRune(col, '.') > 0 {
			s := strings.Split(col, ".")
			for i := range s {
				s[i] = d.Quote(s[i])
			}
			nm = append(nm, strings.Join(s, "."))
			continue
		}
		nm = append(nm, d.Quote(col))
	}

	return nm
}

func parseQuery(e *DB, b builder.Builder) ([]map[string]interface{}, error) {
	sql, values := b.Build()

	rows, err := e.Query(sql, values...)
	if err != nil {
		return nil, err
	}

	result, total, err := ScanToMap(rows)
	if total == 0 {
		return []map[string]interface{}{}, ErrRecordNotFound
	}

	return result, err
}

func parseExec(e *DB, b builder.Builder) (int64, error) {
	sql, values := b.Build()

	result, err := e.Exec(sql, values...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func parseValues(data interface{}) (cols []string, args [][]interface{}, err error) {
	if data == nil {
		return nil, nil, errNoData
	}

	switch val := data.(type) {
	case *QuoteData:
		return parseValues(val.data)
	case map[string]interface{}:
		l := len(val)
		cols = make([]string, 0, l)
		colArgs := make([]interface{}, 0, l)
		for key := range val {
			v := val[key]
			cols = append(cols, key)
			colArgs = append(colArgs, v)
		}
		args = append(args, colArgs)
	case []map[string]interface{}:
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
	default:
		err = errors.New("insert data is illegal")
	}

	return cols, args, err
}

func parseStruct(data interface{}) (cols []string, args [][]interface{}, err error) {
	vof := reflect.ValueOf(data)
	vof = reflect.Indirect(vof)
	if vof.Kind() == reflect.Struct {
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
			tag := zreflect.GetStructTag(structField)
			if tag != "" {
				name = tag
			}
			cols = append(cols, name)
			colArgs = append(colArgs, v)
		}

		args = append(args, colArgs)
		return
	}

	err = errors.New("insert data is illegal")
	return
}
