package schema_test

import (
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/sohaha/zlsgo/zjson"
	"github.com/zlsgo/zdb/schema"
)

func TestNewField(t *testing.T) {
	tt := zlsgo.NewTest(t)

	f := schema.NewField("id", schema.Int)
	tt.Equal("id", f.Name)
	tt.Equal(schema.Int, f.DataType)
	tt.EqualTrue(f.NotNull)

	f = schema.NewField("name", schema.String, func(field *schema.Field) {
		field.Size = 255
		field.NotNull = false
	})
	tt.Equal("name", f.Name)
	tt.Equal(uint64(255), f.Size)
	tt.EqualTrue(!f.NotNull)
}

func TestNewFieldForValue(t *testing.T) {
	tt := zlsgo.NewTest(t)

	f := schema.NewFieldForValue("age", 0)
	tt.Equal("age", f.Name)
	tt.Equal(schema.Int, f.DataType)

	f = schema.NewFieldForValue("active", true)
	tt.Equal(schema.Bool, f.DataType)

	f = schema.NewFieldForValue("score", float64(0))
	tt.Equal(schema.Float, f.DataType)

	f = schema.NewFieldForValue("created", time.Time{})
	tt.Equal(schema.Time, f.DataType)

	f = schema.NewFieldForValue("data", []byte{})
	tt.Equal(schema.Bytes, f.DataType)

	f = schema.NewFieldForValue("name", "")
	tt.Equal(schema.String, f.DataType)

	f = schema.NewFieldForValue("i8", int8(0))
	tt.Equal(schema.Int, f.DataType)

	f = schema.NewFieldForValue("i16", int16(0))
	tt.Equal(schema.Int, f.DataType)

	f = schema.NewFieldForValue("i32", int32(0))
	tt.Equal(schema.Int, f.DataType)

	f = schema.NewFieldForValue("i64", int64(0))
	tt.Equal(schema.Int, f.DataType)

	f = schema.NewFieldForValue("u8", uint8(0))
	tt.Equal(schema.Uint, f.DataType)

	f = schema.NewFieldForValue("u16", uint16(0))
	tt.Equal(schema.Uint, f.DataType)

	f = schema.NewFieldForValue("u32", uint32(0))
	tt.Equal(schema.Uint, f.DataType)

	f = schema.NewFieldForValue("u64", uint64(0))
	tt.Equal(schema.Uint, f.DataType)

	f = schema.NewFieldForValue("f32", float32(0))
	tt.Equal(schema.Float, f.DataType)

	f = schema.NewFieldForValue("json", zjson.Res{})
	tt.Equal(schema.JSON, f.DataType)

	f = schema.NewFieldForValue("uint", uint(0))
	tt.Equal(schema.Uint, f.DataType)
}

func TestFieldDefaultSizes(t *testing.T) {
	tt := zlsgo.NewTest(t)

	f := schema.NewField("i8", schema.Int8)
	tt.Equal(uint64(127), f.Size)

	f = schema.NewField("i16", schema.Int16)
	tt.Equal(uint64(32767), f.Size)

	f = schema.NewField("i32", schema.Int32)
	tt.Equal(uint64(2147483647), f.Size)

	f = schema.NewField("i64", schema.Int64)
	tt.Equal(uint64(9223372036854775807), f.Size)

	f = schema.NewField("u8", schema.Uint8)
	tt.Equal(uint64(255), f.Size)

	f = schema.NewField("u16", schema.Uint16)
	tt.Equal(uint64(65535), f.Size)

	f = schema.NewField("u32", schema.Uint32)
	tt.Equal(uint64(4294967295), f.Size)

	f = schema.NewField("u64", schema.Uint64)
	tt.Equal(uint64(18446744073709551615), f.Size)
}

func TestFieldWithDataType(t *testing.T) {
	tt := zlsgo.NewTest(t)

	f := schema.NewFieldForValue("dt", schema.Text)
	tt.Equal(schema.Text, f.DataType)
}

func TestDataTypeConstants(t *testing.T) {
	tt := zlsgo.NewTest(t)

	tt.EqualTrue(len(schema.Uints) > 0)
	tt.EqualTrue(len(schema.Ints) > 0)
}
