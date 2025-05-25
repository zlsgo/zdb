//go:build clickhouse
// +build clickhouse

package clickhouse

import (
	"fmt"
	"strings"

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
		t.WriteString("UInt8") // ClickHouse没有布尔类型，使用UInt8代替
	case schema.Int, schema.Uint:
		t.WriteString(c.getSchemaNumberType(f))
	case schema.Float:
		t.WriteString(c.getSchemaFloatType(f))
	case schema.String:
		t.WriteString(c.getSchemaStringType(f))
	case schema.Text:
		t.WriteString("String") // ClickHouse没有TEXT类型，使用String
	case schema.Time:
		t.WriteString(c.getSchemaTimeType(f))
	case schema.Bytes:
		t.WriteString("String") // 使用String存储二进制数据
	default:
		t.WriteString(string(f.DataType))
	}

	if !(len(only) > 0 && only[0]) {
		if !f.NotNull {
			typeStr := t.String()
			t.Reset()
			t.WriteString("Nullable(" + typeStr + ")")
		}

		if f.Comment != "" {
			t.WriteString(" COMMENT '")
			t.WriteString(f.Comment)
			t.WriteString("'")
		}
	}

	return t.String()
}

func (c *Config) getSchemaNumberType(field *schema.Field) string {
	if field.DataType == schema.Uint {
		switch {
		case field.Size <= 8:
			return "UInt8"
		case field.Size <= 16:
			return "UInt16"
		case field.Size <= 32:
			return "UInt32"
		default:
			return "UInt64"
		}
	} else {
		switch {
		case field.Size <= 8:
			return "Int8"
		case field.Size <= 16:
			return "Int16"
		case field.Size <= 32:
			return "Int32"
		default:
			return "Int64"
		}
	}
}

func (c *Config) getSchemaFloatType(field *schema.Field) string {
	if field.Precision > 0 {
		return fmt.Sprintf("Decimal(%d, %d)", field.Precision, field.Scale)
	}

	if field.Size <= 32 {
		return "Float32"
	}

	return "Float64"
}

func (c *Config) getSchemaStringType(field *schema.Field) string {
	if field.Size > 0 {
		return fmt.Sprintf("FixedString(%d)", field.Size)
	}

	return "String"
}

func (c *Config) getSchemaTimeType(field *schema.Field) string {
	if field.Precision > 0 {
		return fmt.Sprintf("DateTime64(%d)", field.Precision)
	}

	return "DateTime"
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return `SELECT count(*) AS count FROM system.tables WHERE database = ? AND name = ?`,
		[]interface{}{c.databaseName(), table},
		func(data ztype.Maps) bool {
			if len(data) > 0 {
				return ztype.ToInt64(data[0]["count"]) > 0
			}
			return false
		}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return `SELECT name, type, default_expression, is_in_primary_key 
			FROM system.columns 
			WHERE database = ? AND table = ?`,
		[]interface{}{c.databaseName(), table},
		func(data ztype.Maps) ztype.Map {
			columns := make(ztype.Map, len(data))
			data.ForEach(func(i int, val ztype.Map) bool {
				name := ztype.ToString(val["name"])
				if name == "" {
					return true
				}
				columns[name] = ztype.Map{"type": ztype.ToString(val["type"])}
				return true
			})
			return columns
		}
}

func (c *Config) RenameColumn(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		c.Quote(table), c.Quote(oldName), c.Quote(newName)), []interface{}{}
}

func (c *Config) HasIndex(table, name string) (sql string, values []interface{}, process func(ztype.Maps) bool) {
	return `SELECT count(*) AS count FROM system.data_skipping_indices 
			WHERE database = ? AND table = ? AND name = ?`,
		[]interface{}{c.databaseName(), table, name},
		func(data ztype.Maps) bool {
			if len(data) > 0 {
				return ztype.ToInt64(data[0]["count"]) > 0
			}
			return false
		}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	// ClickHouse不支持直接重命名索引
	return "", []interface{}{}
}

func (c *Config) CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{}) {
	// ClickHouse的索引创建方式与传统数据库不同
	// ClickHouse支持多种类型的索引：
	// 1. 主键索引 - 由ORDER BY定义
	// 2. 数据跳过索引 (data skipping indices) - 如minmax, set, bloom_filter等
	// 3. 次级索引 - 由物化视图提供

	cols := strings.Join(c.QuoteCols(columns), ", ")

	// 如果没有指定索引类型，默认使用minmax
	if indexType == "" {
		indexType = "minmax"
	}

	// 根据索引类型生成不同的SQL
	switch strings.ToLower(indexType) {
	case "primary", "primarykey", "primary_key":
		// 主键索引在ClickHouse中通过表创建时的ORDER BY定义
		// 不支持后期添加主键索引
		return "", []interface{}{}
	case "minmax", "set", "bloom_filter", "ngrambf_v1", "tokenbf_v1":
		// 数据跳过索引
		return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s) TYPE %s GRANULARITY 1",
			c.Quote(table), c.Quote(name), cols, indexType), []interface{}{}
	default:
		// 其他类型的索引
		return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s) TYPE %s GRANULARITY 1",
			c.Quote(table), c.Quote(name), cols, indexType), []interface{}{}
	}
}

func (c *Config) Quote(name string) string {
	return c.Value().Quote(name)
}

func (c *Config) QuoteCols(cols []string) []string {
	return c.Value().QuoteCols(cols)
}
