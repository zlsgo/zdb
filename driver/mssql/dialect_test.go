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
		schema.NewField("id", int64(0)):   "smallint",
		schema.NewField("id", uint8(0)):   "smallint",
		schema.NewField("id", float32(0)): "float",
		schema.NewField("id", time.Now()): "datetimeoffset",
		schema.NewField("id", true):       "bit",
		schema.NewField("id", ""):         "nvarchar(256)",
		schema.NewField("id", []byte("")): "varbinary(MAX)",
		schema.NewField("id", int64(0), func(field *schema.Field) {
			field.Size = 999999
		}): "bigint",
		schema.NewField("id", "", func(field *schema.Field) {
			field.Size = 1000
		}): "nvarchar(1000)",
		schema.NewField("id", "", func(field *schema.Field) {
			field.Size = 999990
		}): "nvarchar(MAX)",
	}

	for tv, expected := range tests {
		of := c.DataTypeOf(tv)
		t.Log(of)
		tt.Equal(expected, of)
	}
}
