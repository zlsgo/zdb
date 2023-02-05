package mssql

import (
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/schema"
)

func TestDataTypeOf(t *testing.T) {
	tt := zlsgo.NewTest(t)
	c := &Config{}
	tests := map[*schema.Field]string{
		schema.NewFieldForValue("id", int64(0)):   "bigint",
		schema.NewFieldForValue("id", uint8(0)):   "smallint",
		schema.NewFieldForValue("id", float32(0)): "float",
		schema.NewFieldForValue("id", time.Now()): "datetimeoffset",
		schema.NewFieldForValue("id", true):       "bit",
		schema.NewFieldForValue("id", ""):         "nvarchar(256)",
		schema.NewFieldForValue("id", []byte("")): "varbinary(MAX)",
		schema.NewFieldForValue("id", int64(0), func(field *schema.Field) {
			field.Size = 999999
		}): "bigint",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 1000
		}): "nvarchar(1000)",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 999990
		}): "nvarchar(MAX)",
	}

	for tv, expected := range tests {
		of := c.DataTypeOf(tv)
		t.Log(of)
		tt.Equal(expected, of)
	}
}
