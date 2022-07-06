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
		PrimaryKey    bool
		AutoIncrement bool
		NotNull       bool
		Comment       string
		Size          int
		Precision     int
		Scale         int
	}
)

const (
	Bool   DataType = "bool"
	Int    DataType = "int"
	Uint   DataType = "uint"
	Float  DataType = "float"
	String DataType = "string"
	JSON   DataType = "json"
	Time   DataType = "time"
	Bytes  DataType = "bytes"
)

func NewField(fieldName string, fieldType interface{}, fieldOption ...func(*Field)) *Field {
	f := &Field{
		Name:     fieldName,
		NotNull:  true,
		DataType: getDataType(fieldType),
	}
	for _, opt := range fieldOption {
		opt(f)
	}
	return f
}

func getDataType(fieldType interface{}) DataType {
	switch fieldType.(type) {
	default:
		return String
	case bool:
		return Bool
	case zjson.Res:
		return JSON
	case int, int8, int16, int32, int64:
		return Int
	case uint, uint8, uint16, uint32, uint64:
		return Uint
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
