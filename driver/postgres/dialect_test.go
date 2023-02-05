package postgres

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
		schema.NewFieldForValue("id", float32(0)): "decimal",
		schema.NewFieldForValue("id", time.Now()): "timestamptz",
		schema.NewFieldForValue("id", true):       "boolean",
		schema.NewFieldForValue("id", ""):         "text",
		schema.NewFieldForValue("id", []byte("")): "bytea",
		schema.NewFieldForValue("id", int64(0), func(field *schema.Field) {
			field.Size = 999999
		}): "integer",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 1000
		}): "varchar(1000)",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 999990
		}): "varchar(999990)",
	}

	for tv, expected := range tests {
		of := c.DataTypeOf(tv)
		t.Log(of, tv.DataType)
		tt.Equal(expected, of)
	}
}
