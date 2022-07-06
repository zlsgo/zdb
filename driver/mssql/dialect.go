package mssql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

func (c *Config) databaseName() string {
	if len(c.DBName) == 0 {
		tmp := c.GetDsn()
		s := strings.Split(tmp, "?")
		tmp = strings.Split(s[len(s)-1], "?")[0]
		for _, v := range strings.Split(tmp, " ") {
			val := strings.Split(v, "=")
			if len(val) == 2 && strings.ToLower(val[0]) == "database" {
				c.DBName = val[1]
				break
			}
		}
	}

	return c.DBName
}

func (c *Config) GetVersion() (string, error) {
	return "", errors.New("can't get the version")
}

func (c *Config) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "bit"
	case schema.Int, schema.Uint:
		var sqlType string
		switch {
		case field.Size < 16:
			sqlType = "smallint"
		case field.Size < 31:
			sqlType = "int"
		default:
			sqlType = "bigint"
		}

		if field.AutoIncrement {
			return sqlType + " IDENTITY(1,1)"
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("decimal(%d)", field.Precision)
		}
		return "float"
	case schema.String:
		size := field.Size
		if size == 0 {
			size = 256
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("nvarchar(%d)", size)
		}
		return "nvarchar(MAX)"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("datetimeoffset(%d)", field.Precision)
		}
		return "datetimeoffset"
	case schema.Bytes:
		return "varbinary(MAX)"
	}

	return string(field.DataType)
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result []map[string]interface{}) bool) {
	return "SELECT count(*) AS count FROM INFORMATION_SCHEMA.tables WHERE table_name = ? AND table_catalog = ?", []interface{}{table, c.databaseName()}, func(data []map[string]interface{}) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}
