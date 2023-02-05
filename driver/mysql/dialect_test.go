package mysql

import (
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/schema"
)

func TestDataTypeOf(t *testing.T) {
	tt := zlsgo.NewTest(t)
	c := &Config{}
	n := func(f *schema.Field) {
		f.NotNull = false
	}
	tests := map[*schema.Field]string{
		schema.NewFieldForValue("id", int64(0), n):   "bigint",
		schema.NewFieldForValue("id", uint8(0), n):   "tinyint UNSIGNED",
		schema.NewFieldForValue("id", float32(0), n): "float",
		schema.NewFieldForValue("id", time.Now(), n): "datetime NULL",
		schema.NewFieldForValue("id", true, n):       "boolean",
		schema.NewFieldForValue("id", "", n):         "varchar(250)",
		schema.NewFieldForValue("id", []byte(""), n): "longblob",
		schema.NewFieldForValue("id", int64(0), func(field *schema.Field) {
			field.Size = 2147483649
		}, n): "bigint",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 1000
		}, n): "varchar(1000)",
		schema.NewFieldForValue("id", "", func(field *schema.Field) {
			field.Size = 999999
		}, n): "mediumtext",
	}

	for tv, expected := range tests {
		of := c.DataTypeOf(tv)
		t.Log(of)
		tt.Equal(expected, of)
	}
}
