package sqlite3_test

import (
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/driver/sqlite3"
	"github.com/zlsgo/zdb/schema"
)

func TestDataTypeOf(t *testing.T) {
	tt := zlsgo.NewTest(t)
	c := &sqlite3.Config{}
	tests := map[string]*schema.Field{
		"integer":  schema.NewFieldForValue("id", uint8(0)),
		"real":     schema.NewFieldForValue("id", float32(0)),
		"datetime": schema.NewFieldForValue("id", time.Now()),
		"numeric":  schema.NewField("id", "bool"),
		"text":     schema.NewFieldForValue("id", ""),
		"blob":     schema.NewFieldForValue("id", []byte("")),
	}
	for expected, tv := range tests {
		of := c.DataTypeOf(tv, true)
		t.Log(of)
		tt.Equal(expected, of)
	}
}
