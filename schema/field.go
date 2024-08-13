package schema

import (
	"time"

	"github.com/sohaha/zlsgo/zjson"
)

type (
	// DataType field data type
	DataType string
	Field    struct {
		Name          string
		DataType      DataType
		RawDataType   string
		Comment       string
		Size          uint64
		Precision     int
		Scale         int
		PrimaryKey    bool
		AutoIncrement bool
		NotNull       bool
	}
)

const (
	Bool   DataType = "bool"
	Int    DataType = "int"
	Int8   DataType = "int8"
	Int16  DataType = "int16"
	Int32  DataType = "int32"
	Int64  DataType = "int64"
	Uint   DataType = "uint"
	Uint8  DataType = "uint8"
	Uint16 DataType = "uint16"
	Uint32 DataType = "uint32"
	Uint64 DataType = "uint64"
	Float  DataType = "float"
	String DataType = "string"
	Text   DataType = "text"
	JSON   DataType = "json"
	Time   DataType = "time"
	Bytes  DataType = "bytes"
)

var Uints = []DataType{Uint, Uint8, Uint16, Uint32, Uint64}
var Ints = []DataType{Int, Int8, Int16, Int32, Int64}

func NewFieldForValue(fieldName string, fieldType interface{}, fieldOption ...func(*Field)) *Field {
	return NewField(fieldName, getDataType(fieldType), fieldOption...)
}

func NewField(fieldName string, dataType DataType, fieldOption ...func(*Field)) *Field {
	f := &Field{
		Name:     fieldName,
		NotNull:  true,
		DataType: dataType,
	}

	for _, opt := range fieldOption {
		opt(f)
	}

	if f.Size == 0 {
		switch dataType {
		case Int8:
			f.Size = 127
		case Int16:
			f.Size = 32767
		case Int32:
			f.Size = 2147483647
		case Int64:
			f.Size = 9223372036854775807
		case Uint8:
			f.Size = 255
		case Uint16:
			f.Size = 65535
		case Uint32:
			f.Size = 4294967295
		case Uint64:
			f.Size = 18446744073709551615
		}
	}

	switch dataType {
	case Int8, Int16, Int32, Int64:
		f.DataType = Int
	case Uint8, Uint16, Uint32, Uint64:
		f.DataType = Uint
	}

	return f
}

func getDataType(fieldType interface{}) DataType {
	switch v := fieldType.(type) {
	default:
		return String
	case DataType:
		return v
	case bool:
		return Bool
	case zjson.Res:
		return JSON
	case int:
		return Int
	case int8:
		return Int8
	case int16:
		return Int16
	case int32:
		return Int32
	case int64:
		return Int64
	case uint:
		return Uint
	case uint8:
		return Uint8
	case uint16:
		return Uint16
	case uint32:
		return Uint32
	case uint64:
		return Uint64
	case float32, float64:
		return Float
	case time.Time:
		return Time
	case []byte:
		return Bytes
	}
}

func (field *Field) fieldType() string {
	t := "bigint"
	switch {
	case field.Size <= 8:
		t = "tinyint"
	case field.Size <= 16:
		t = "smallint"
	case field.Size <= 24:
		t = "mediumint"
	case field.Size <= 32:
		t = "int"
	}

	if field.DataType == Uint {
		t += " unsigned"
	}

	if field.AutoIncrement {
		t += " AUTO_INCREMENT"
	}
	return t
}
