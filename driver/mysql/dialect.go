package mysql

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

func (c *Config) databaseName() string {
	if len(c.DBName) == 0 {
		tmp := c.GetDsn()
		s := strings.Split(tmp, "/")
		tmp = strings.Split(s[len(s)-1], "?")[0]
		c.DBName = tmp
	}
	return c.DBName
}

func (c *Config) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		return c.getSchemaNumberType(field)
	case schema.Float:
		return c.getSchemaFloatType(field)
	case schema.String:
		return c.getSchemaStringType(field)
	case schema.Time:
		return c.getSchemaTimeType(field)
	case schema.Bytes:
		return c.getSchemaBytesType(field)
	}

	return string(field.DataType)
}

func (c *Config) getSchemaFloatType(field *schema.Field) string {
	if field.Precision > 0 {
		return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
	}

	if field.Size <= 32 {
		return "float"
	}

	return "double"
}

func (c *Config) getSchemaStringType(field *schema.Field) string {
	size := field.Size
	if size == 0 {
		size = 250
	}

	if size >= 65536 && size <= int(math.Pow(2, 24)) {
		return "mediumtext"
	}

	if size > int(math.Pow(2, 24)) || size <= 0 {
		return "longtext"
	}

	return "varchar(" + strconv.Itoa(size) + ")"
}

func (c *Config) getSchemaTimeType(field *schema.Field) string {
	precision := ""

	if field.Precision > 0 {
		precision = fmt.Sprintf("(%d)", field.Precision)
	}

	if field.NotNull || field.PrimaryKey {
		return "datetime" + precision
	}

	return "datetime" + precision + " NULL"
}

func (c *Config) getSchemaBytesType(field *schema.Field) string {
	if field.Size > 0 && field.Size < 65536 {
		return fmt.Sprintf("varbinary(%d)", field.Size)
	}

	if field.Size >= 65536 && field.Size <= int(math.Pow(2, 24)) {
		return "mediumblob"
	}

	return "longblob"
}

func (c *Config) getSchemaNumberType(field *schema.Field) string {
	sqlType := "bigint"
	if field.Size == 0 && field.PrimaryKey {
		field.Size = 64
	}
	switch {
	case field.Size <= 8:
		sqlType = "tinyint"
	case field.Size <= 16:
		sqlType = "smallint"
	case field.Size <= 24:
		sqlType = "mediumint"
	case field.Size <= 32:
		sqlType = "int"
	}

	if field.DataType == schema.Uint {
		sqlType += " UNSIGNED"
	}

	return sqlType
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result []map[string]interface{}) bool) {
	return "SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", []interface{}{c.databaseName(), table, "BASE TABLE"}, func(data []map[string]interface{}) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}
