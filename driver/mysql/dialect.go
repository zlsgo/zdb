package mysql

import (
	"fmt"
	"math"
	"strings"

	"github.com/sohaha/zlsgo/zarray"
	"github.com/sohaha/zlsgo/zstring"
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

func (c *Config) DataTypeOf(f *schema.Field, only ...bool) string {
	t := zstring.Buffer()

	switch f.DataType {
	case schema.Bool:
		t.WriteString("boolean")
	case schema.Int, schema.Uint:
		if f.AutoIncrement {
			f.Size = 4294967295
			t.WriteString(c.getSchemaNumberType(f))
		} else {
			t.WriteString(c.getSchemaNumberType(f))
		}
	case schema.Float:
		t.WriteString(c.getSchemaFloatType(f))
	case schema.String:
		t.WriteString(c.getSchemaStringType(f))
	case schema.Text:
		t.WriteString("longtext")
	case schema.Time:
		t.WriteString(c.getSchemaTimeType(f))
	case schema.Bytes:
		t.WriteString(c.getSchemaBytesType(f))
	default:
		t.WriteString(string(f.DataType))
	}

	if !(len(only) > 0 && only[0]) {
		if f.NotNull && !f.PrimaryKey {
			t.WriteString(" NOT NULL")
		}

		if f.AutoIncrement {
			t.WriteString(" AUTO_INCREMENT")
		}

		if f.PrimaryKey {
			t.WriteString(" PRIMARY KEY")
		}

		if len(f.Comment) > 0 {
			t.WriteString(" COMMENT '" + f.Comment + "'")
		}
	}
	return t.String()
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

	if size >= 65536 && size <= uint64(math.Pow(2, 24)) {
		return "mediumtext"
	}

	if size > uint64(math.Pow(2, 24)) || size <= 0 {
		return "longtext"
	}

	return "varchar(" + ztype.ToString(size) + ")"
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

	if field.Size >= 65536 && field.Size <= uint64(math.Pow(2, 24)) {
		return "mediumblob"
	}

	return "longblob"
}

func (c *Config) getSchemaNumberType(field *schema.Field) string {
	sqlType := "bigint"
	size := field.Size

	if size == 0 && field.PrimaryKey {
		return sqlType + " UNSIGNED"
	}

	if field.DataType == schema.Uint {
		switch {
		case size == 0:
			sqlType = "int"
		case size <= 255:
			sqlType = "tinyint"
		case size <= 65535:
			sqlType = "smallint"
		case size <= 16777215:
			sqlType = "mediumint"
		case size <= 4294967295:
			sqlType = "int"
		}
		sqlType += " UNSIGNED"
	} else {
		switch {
		case size == 0:
			sqlType = "int"
		case size <= 127:
			sqlType = "tinyint"
		case size <= 32767:
			sqlType = "smallint"
		case size <= 8388607:
			sqlType = "mediumint"
		case size <= 2147483647:
			sqlType = "int"
		}
	}

	return sqlType
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return "SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", []interface{}{c.databaseName(), table, "BASE TABLE"}, func(data ztype.Maps) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return "SELECT column_name, column_default, is_nullable = 'YES', data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema = ? AND table_name =? ORDER BY ORDINAL_POSITION", []interface{}{c.databaseName(), table}, func(data ztype.Maps) ztype.Map {
		columns := make(ztype.Map, len(data))
		data.ForEach(func(i int, val ztype.Map) bool {
			name := ztype.ToString(val["column_name"])
			columns[name] = ztype.Map{"type": ztype.ToString(val["column_type"])}
			return true
		})
		return columns
	}
}

func (c *Config) RenameColumn(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName), []interface{}{}
}

func (c *Config) HasIndex(table, name string) (sql string, values []interface{}, process func(ztype.Maps) bool) {
	return `SELECT TABLE_NAME,COLUMN_NAME,INDEX_NAME,NON_UNIQUE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY INDEX_NAME,SEQ_IN_INDEX`, []interface{}{
			c.databaseName(),
			table,
		}, func(data ztype.Maps) bool {
			for i := range data {
				c := data[i]
				if c["INDEX_NAME"] == name {
					return true
				}
			}
			return false
		}
}

func (c *Config) CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{}) {
	fields := zarray.Map(columns, func(i int, val string) string {
		return "`" + val + "`"
	})
	return fmt.Sprintf("ALTER TABLE `%s` ADD %s INDEX `%s`(%s)", table, indexType, name, strings.Join(fields, ",")), []interface{}{}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	panic("implement me")
}
