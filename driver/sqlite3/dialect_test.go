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
		"integer":  schema.NewField("id", uint8(0)),
		"real":     schema.NewField("id", float32(0)),
		"datetime": schema.NewField("id", time.Now()),
		"numeric":  schema.NewField("id", true),
		"text":     schema.NewField("id", ""),
		"blob":     schema.NewField("id", []byte("")),
	}
	for expected, tv := range tests {
		of := c.DataTypeOf(tv)
		t.Log(of)
		tt.Equal(expected, of)
	}
}
