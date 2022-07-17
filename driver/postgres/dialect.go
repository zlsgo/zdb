package postgres

import (
	"fmt"
	"strings"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

func (c *Config) databaseName() string {
	if len(c.DBName) == 0 {
		tmp := c.GetDsn()
		for _, v := range strings.Split(tmp, " ") {
			val := strings.Split(v, "=")
			if len(val) == 2 && strings.ToLower(val[0]) == "dbname" {
				c.DBName = val[1]
				break
			}
		}
	}
	return c.DBName
}

func (c *Config) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}
		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "smallserial"
			case size <= 32:
				return "serial"
			default:
				return "bigserial"
			}
		} else {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("numeric(%d)", field.Precision)
		}
		return "decimal"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("timestamptz(%d)", field.Precision)
		}
		return "timestamptz"
	case schema.Bytes:
		return "bytea"
	}

	return string(field.DataType)
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result []ztype.Map) bool) {
	return "SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", []interface{}{c.databaseName(), table, "BASE TABLE"}, func(data []ztype.Map) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}
