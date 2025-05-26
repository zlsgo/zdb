//go:build doris
// +build doris

package doris

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

func (c *Config) BuildCreateTableSQL(table string, columns []string) string {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		c.Quote(table), strings.Join(columns, ",\n  "))

	// 添加 Doris 特有的表选项
	return sql + c.GetTableOptions()
}

func (c *Config) DataTypeOf(f *schema.Field, only ...bool) string {
	t := zstring.Buffer()

	if f.RawDataType != "" {
		t.WriteString(f.RawDataType)
		return t.String()
	}

	switch f.DataType {
	case schema.Bool:
		t.WriteString("BOOLEAN")
	case schema.Int, schema.Uint:
		t.WriteString(c.getSchemaNumberType(f))
	case schema.Float:
		t.WriteString(c.getSchemaFloatType(f))
	case schema.String:
		t.WriteString(c.getSchemaStringType(f))
	case schema.Text:
		t.WriteString("STRING")
	case schema.Time:
		t.WriteString(c.getSchemaTimeType(f))
	case schema.Bytes:
		t.WriteString("STRING")
	case schema.JSON:
		t.WriteString("STRING")
	default:

		if strings.HasPrefix(string(f.DataType), "ARRAY<") {
			t.WriteString(string(f.DataType))
		} else if strings.HasPrefix(string(f.DataType), "MAP<") {
			t.WriteString(string(f.DataType))
		} else {
			t.WriteString(string(f.DataType))
		}
	}

	if !(len(only) > 0 && only[0]) {

		if !f.NotNull {
			t.WriteString(" NULL")
		} else {
			t.WriteString(" NOT NULL")
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
	isPrimaryOrDistributionKey := field.PrimaryKey

	hasColocate := false
	if c.CustomProperties != nil {
		_, hasColocate = c.CustomProperties["colocate_with"]
	}

	if isPrimaryOrDistributionKey && hasColocate {
		if field.DataType == schema.Uint {
			return "BIGINT UNSIGNED"
		}
		return "BIGINT"
	}

	if field.DataType == schema.Uint {
		switch {
		case field.Size <= 8:
			return "TINYINT UNSIGNED"
		case field.Size <= 16:
			return "SMALLINT UNSIGNED"
		case field.Size <= 32:
			return "INT UNSIGNED"
		default:
			return "BIGINT UNSIGNED"
		}
	} else {
		switch {
		case field.Size <= 8:
			return "TINYINT"
		case field.Size <= 16:
			return "SMALLINT"
		case field.Size <= 32:
			return "INT"
		default:
			return "BIGINT"
		}
	}
}

func (c *Config) getSchemaFloatType(field *schema.Field) string {
	if field.Precision > 0 {
		return fmt.Sprintf("DECIMAL(%d, %d)", field.Precision, field.Scale)
	}

	if field.Size <= 32 {
		return "FLOAT"
	}

	return "DOUBLE"
}

func (c *Config) getSchemaStringType(field *schema.Field) string {
	if field.Size > 0 && field.Size <= 65533 {
		return fmt.Sprintf("VARCHAR(%d)", field.Size)
	}

	return "STRING"
}

func (c *Config) getSchemaTimeType(field *schema.Field) string {
	if strings.Contains(field.Comment, "date_only") || strings.Contains(field.Comment, "date only") {
		return "DATE"
	}

	return "DATETIME"
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return `SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		[]interface{}{c.databaseName(), table},
		func(data ztype.Maps) bool {
			if len(data) > 0 {
				return ztype.ToInt64(data[0]["count"]) > 0
			}
			return false
		}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return `SELECT column_name, column_type, column_default, is_nullable, column_comment 
			FROM information_schema.columns 
			WHERE table_schema = ? AND table_name = ?`,
		[]interface{}{c.databaseName(), table},
		func(data ztype.Maps) ztype.Map {
			columns := make(ztype.Map, len(data))
			data.ForEach(func(i int, val ztype.Map) bool {
				name := ztype.ToString(val["column_name"])
				if name == "" {
					return true
				}
				columns[name] = ztype.Map{
					"type":    ztype.ToString(val["column_type"]),
					"default": ztype.ToString(val["column_default"]),
					"null":    ztype.ToString(val["is_nullable"]) == "YES",
					"comment": ztype.ToString(val["column_comment"]),
				}
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
	// Doris 使用 SHOW INDEX 命令查询索引
	return `SHOW INDEX FROM ` + c.Quote(table) + ` WHERE Key_name = ?`,
		[]interface{}{name},
		func(data ztype.Maps) bool {
			return len(data) > 0
		}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	// Doris 不支持直接重命名索引，返回空实现
	return "", []interface{}{}
}

func (c *Config) CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{}) {
	// Doris 的索引创建方式与传统数据库不同，主要通过建表
	var idx string

	switch strings.ToUpper(indexType) {
	case "UNIQUE":
		idx = "UNIQUE KEY"
	case "FULLTEXT":
		idx = "FULLTEXT KEY"
	case "SPATIAL":
		idx = "SPATIAL KEY"
	default:
		idx = "KEY"
	}

	quotedColumns := make([]string, len(columns))
	for i, column := range columns {
		quotedColumns[i] = c.Quote(column)
	}

	return fmt.Sprintf("ALTER TABLE %s ADD %s %s (%s)",
		c.Quote(table), idx, c.Quote(name), strings.Join(quotedColumns, ", ")), []interface{}{}
}

func (c *Config) Quote(name string) string {
	if name == "*" || (len(name) > 0 && name[0] == '(') {
		return name
	}

	sl := strings.SplitN(name, " ", 2)
	if strings.IndexRune(sl[0], '.') > 0 {
		s := strings.Split(sl[0], ".")
		for i := range s {
			if s[i] == "*" {
				continue
			}
			s[i] = "`" + s[i] + "`"
		}
		sl[0] = strings.Join(s, ".")
	} else {
		sl[0] = "`" + sl[0] + "`"
	}

	return strings.Join(sl, " ")
}

func (c *Config) QuoteCols(cols []string) []string {
	nm := make([]string, 0, len(cols))

	for i := range cols {
		nm = append(nm, c.Quote(cols[i]))
	}

	return nm
}
