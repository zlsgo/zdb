package zdb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/ztime"
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
	// BindTag is the default struct tag name
	BindTag = "zdb"
	// ErrTargetNotSettable means the second param of Bind is not settable
	ErrTargetNotSettable = errors.New("target is not settable! a pointer is required")
	// ErrSliceToString means only []uint8 can be transmuted into string
	ErrSliceToString = errors.New("can't transmute a non-uint8 slice to string")
	// ErrConversionFailed conversion failed
	ErrConversionFailed = errors.New("conversion failed")
	// ErrDBNotExist db not exist
	ErrDBNotExist = errors.New("database instance does not exist")

	errNoData      = errors.New("no data")
	errInsertEmpty = errors.New("insert data can not be empty")
	errDataInvalid = errors.New("data is illegal")
)

func Scan(rows IfeRows, out interface{}) (int, error) {
	data, count, err := resolveDataFromRows(rows)
	if err != nil {
		return 0, err
	}
	if nil == data {
		return count, ErrNotFound
	}
	return count, scan(data, out)
}

func scan(data []ztype.Map, out interface{}) (err error) {
	targetValueOf := reflect.ValueOf(out)
	if nil == out || targetValueOf.Kind() != reflect.Ptr || targetValueOf.IsNil() {
		return ErrTargetNotSettable
	}

	targetValueOf = targetValueOf.Elem()
	switch targetValueOf.Kind() {
	case reflect.Slice:
		err = bindSlice(data, targetValueOf)
	default:
		err = bind(data[0], targetValueOf)
	}

	return err
}

// ScanToMap returns the result in the form of []map[string]interface{}
func ScanToMap(rows IfeRows) ([]ztype.Map, int, error) {
	return resolveDataFromRows(rows)
}

func bindSlice(arr []ztype.Map, elem reflect.Value) error {
	if !elem.CanSet() {
		return ErrTargetNotSettable
	}
	length := len(arr)
	valueArrObj := reflect.MakeSlice(elem.Type(), 0, length)
	typeObj := valueArrObj.Type().Elem()
	var err error
	for i := 0; i < length; i++ {
		newObj := reflect.New(typeObj)
		err = bind(arr[i], newObj.Elem())
		if nil != err {
			return err
		}
		valueArrObj = reflect.Append(valueArrObj, newObj.Elem())
	}
	elem.Set(valueArrObj)
	return nil
}

func bind(result map[string]interface{}, rv reflect.Value) (resp error) {
	defer func() {
		if r := recover(); nil != r {
			resp = fmt.Errorf("error:[%v], stack:[%s]", r, string(debug.Stack()))
		}
	}()
	if !rv.CanSet() {
		return ErrTargetNotSettable
	}
	typeObj := rv.Type()
	if typeObj.Kind() == reflect.Ptr {
		ptrType := typeObj.Elem()
		newObj := reflect.New(ptrType)
		err := bind(result, newObj.Elem())
		if nil == err {
			rv.Set(newObj)
		}
		return err
	}

	if typeObj.Kind() == reflect.Struct {
		for i := 0; i < rv.NumField(); i++ {
			fieldTypeI := typeObj.Field(i)
			fieldName := fieldTypeI.Name
			valuei := rv.Field(i)
			if !valuei.CanSet() {
				continue
			}
			tagName, _ := lookUpTagName(fieldTypeI)
			if tagName == "" {
				if fieldName == "ID" {
					tagName = "id"
				} else {
					tagName = zstring.CamelCaseToSnakeCase(fieldName)
				}
			}
			mapValue, ok := result[tagName]
			if !ok || mapValue == nil {
				continue
			}
			if fieldTypeI.Type.Kind() == reflect.Ptr && !fieldTypeI.Type.Implements(reflect.TypeOf(new(ByteUnmarshaler)).Elem()) {
				valuei.Set(reflect.New(fieldTypeI.Type.Elem()))
				valuei = valuei.Elem()
			}
			err := convert(mapValue, valuei)
			if nil != err {
				return err
			}
		}
	} else if rv.CanSet() {
		for i := range result {
			return convert(result[i], rv)
		}
		return nil
	}
	return nil
}

func isIntSeriesType(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}

func isUintSeriesType(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uint64
}

func isFloatSeriesType(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

func resolveDataFromRows(rows IfeRows) ([]ztype.Map, int, error) {
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

func lookUpTagName(rf reflect.StructField) (string, bool) {
	name, ok := rf.Tag.Lookup(BindTag)
	if !ok {
		return "", false
	}
	name = resolveTagName(name)
	return name, true
}

func resolveTagName(tag string) string {
	idx := strings.IndexByte(tag, ',')
	if idx == -1 {
		return tag
	}
	return tag[:idx]
}

func convert(out interface{}, rv reflect.Value) error {
	vit := rv.Type()
	mvt := reflect.TypeOf(out)
	if nil == mvt {
		return nil
	}
	if mvt.AssignableTo(vit) {
		rv.Set(reflect.ValueOf(out))
		return nil
	}

	switch assertT := out.(type) {
	case time.Time:
		return handleConvertTime(assertT, mvt, vit, &rv)
	}

	if scanner, ok := rv.Addr().Interface().(sql.Scanner); ok {
		return scanner.Scan(out)
	}
	vk := vit.Kind()
	switch mvt.Kind() {
	case reflect.Int64:
		if isIntSeriesType(vk) {
			rv.SetInt(out.(int64))
		} else if isUintSeriesType(vk) {
			rv.SetUint(uint64(out.(int64)))
		} else if vk == reflect.Bool {
			v := out.(int64)
			if v > 0 {
				rv.SetBool(true)
			} else {
				rv.SetBool(false)
			}
		} else if vk == reflect.String {
			rv.SetString(strconv.FormatInt(out.(int64), 10))
		} else {
			return ErrConversionFailed
		}
	case reflect.Float32:
		if isFloatSeriesType(vk) {
			rv.SetFloat(float64(out.(float32)))
		} else {
			return ErrConversionFailed
		}
	case reflect.Float64:
		if isFloatSeriesType(vk) {
			rv.SetFloat(out.(float64))
		} else {
			return ErrConversionFailed
		}
	case reflect.Slice:
		return handleConvertSlice(out, mvt, vit, &rv)
	default:
		if mvt.Kind() == reflect.String && vit.ConvertibleTo(timeType) {
			t, err := ztime.Parse(out.(string))
			if err == nil {
				if vit.AssignableTo(timeType) {
					rv.Set(reflect.ValueOf(t))
				} else if vit.AssignableTo(jsontimeType) {
					rv.Set(reflect.ValueOf(JsonTime(t)))
				}
				return nil
			}
		}

		return ErrConversionFailed
	}
	return nil
}

func handleConvertSlice(mapValue interface{}, mvt, vit reflect.Type, valuei *reflect.Value) error {
	mapValueSlice, ok := mapValue.([]byte)
	if !ok {
		return ErrSliceToString
	}
	mapValueStr := string(mapValueSlice)
	vitKind := vit.Kind()
	switch {
	case vitKind == reflect.String:
		valuei.SetString(mapValueStr)
	case isIntSeriesType(vitKind):
		intVal, err := strconv.ParseInt(mapValueStr, 10, 64)
		if nil != err {
			return err
		}
		valuei.SetInt(intVal)
	case isUintSeriesType(vitKind):
		uintVal, err := strconv.ParseUint(mapValueStr, 10, 64)
		if nil != err {
			return err
		}
		valuei.SetUint(uintVal)
	case isFloatSeriesType(vitKind):
		floatVal, err := strconv.ParseFloat(mapValueStr, 64)
		if nil != err {
			return err
		}
		valuei.SetFloat(floatVal)
	case vitKind == reflect.Bool:
		intVal, err := strconv.ParseInt(mapValueStr, 10, 64)
		if nil != err {
			return err
		}
		if intVal > 0 {
			valuei.SetBool(true)
		} else {
			valuei.SetBool(false)
		}
	default:
		if _, ok := valuei.Interface().(ByteUnmarshaler); ok {
			return byteUnmarshal(mapValueSlice, valuei)
		}
		return ErrConversionFailed
	}
	return nil
}

func byteUnmarshal(mapValueSlice []byte, valuei *reflect.Value) error {
	var pt reflect.Value
	initFlag := false
	if valuei.IsNil() {
		pt = reflect.New(valuei.Type().Elem())
		initFlag = true
	} else {
		pt = *valuei
	}
	err := pt.Interface().(ByteUnmarshaler).UnmarshalByte(mapValueSlice)
	if nil != err {
		structName := pt.Elem().Type().Name()
		return fmt.Errorf("%s.UnmarshalByte fail to unmarshal the bytes, err: %s", structName, err)
	}
	if initFlag {
		valuei.Set(pt)
	}
	return nil
}

func handleConvertTime(assertT time.Time, mvt, vit reflect.Type, rv *reflect.Value) error {
	switch vit.Kind() {
	case reflect.String:
		sTime := assertT.Format(ztime.TimeTpl)
		rv.SetString(sTime)
		return nil
	case reflect.Struct:
		if vit.ConvertibleTo(mvt) {
			v := reflect.ValueOf(assertT)
			vv := v.Convert(vit)
			rv.Set(vv)
			return nil
		}
	}
	return errors.New("convert time failed")
}
