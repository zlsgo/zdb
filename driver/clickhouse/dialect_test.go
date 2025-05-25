//go:build clickhouse
// +build clickhouse

package clickhouse

import (
	"testing"

	"github.com/zlsgo/zdb/schema"
)

func TestDataTypeOf(t *testing.T) {
	c := &Config{}

	f := schema.NewField("test", schema.Bool)
	if got := c.DataTypeOf(f, true); got != "UInt8" {
		t.Errorf("DataTypeOf(Bool) = %s, want UInt8", got)
	}

	f = schema.NewField("test", schema.Int)
	f.Size = 32
	if got := c.DataTypeOf(f, true); got != "Int32" {
		t.Errorf("DataTypeOf(Int32) = %s, want Int32", got)
	}

	f = schema.NewField("test", schema.Uint)
	f.Size = 16
	if got := c.DataTypeOf(f, true); got != "UInt16" {
		t.Errorf("DataTypeOf(UInt16) = %s, want UInt16", got)
	}

	f = schema.NewField("test", schema.Float)
	f.Size = 64
	if got := c.DataTypeOf(f, true); got != "Float64" {
		t.Errorf("DataTypeOf(Float) = %s, want Float64", got)
	}

	f = schema.NewField("test", schema.String)
	if got := c.DataTypeOf(f, true); got != "String" {
		t.Errorf("DataTypeOf(String) = %s, want String", got)
	}

	f = schema.NewField("test", schema.String)
	f.Size = 20
	if got := c.DataTypeOf(f, true); got != "FixedString(20)" {
		t.Errorf("DataTypeOf(FixedString) = %s, want FixedString(20)", got)
	}

	f = schema.NewField("test", schema.Time)
	if got := c.DataTypeOf(f, true); got != "DateTime" {
		t.Errorf("DataTypeOf(Time) = %s, want DateTime", got)
	}

	f = schema.NewField("test", schema.Time)
	f.Precision = 3
	if got := c.DataTypeOf(f, true); got != "DateTime64(3)" {
		t.Errorf("DataTypeOf(Time with precision) = %s, want DateTime64(3)", got)
	}
}
