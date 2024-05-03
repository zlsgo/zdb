package sqlite3

import (
	"fmt"
	"strings"

	"github.com/sohaha/zlsgo/zarray"
	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

func (c *Config) GetVersion() (sql string, process func([]ztype.Map) string) {
	return "SELECT SQLITE_VERSION()", func(data []ztype.Map) string {
		if v, ok := data[0]["SQLITE_VERSION()"]; ok {
			return v.(string)
		}
		return ""
	}
}

func (c *Config) DataTypeOf(f *schema.Field, only ...bool) string {
	t := zstring.Buffer()

	switch f.DataType {
	case schema.Bool:
		t.WriteString("numeric")
	case schema.String, schema.Text:
		t.WriteString("text")
	case schema.Int, schema.Uint:
		t.WriteString("integer")
	case schema.Float:
		t.WriteString("real")
	case schema.Time:
		t.WriteString("datetime")
	case schema.Bytes:
		t.WriteString("blob")
	default:
		t.WriteString(string(f.DataType))
	}

	if !(len(only) > 0 && only[0]) {
		if f.NotNull && !f.PrimaryKey {
			t.WriteString(" NOT NULL")
		}

		if f.PrimaryKey {
			t.WriteString(" PRIMARY KEY")
		}

		if f.AutoIncrement {
			t.WriteString(" AUTOINCREMENT")
		}
	}
	return t.String()
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return "SELECT count(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?", []interface{}{table}, func(data ztype.Maps) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return "PRAGMA table_info('" + table + "')", []interface{}{}, func(data ztype.Maps) ztype.Map {
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
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName), []interface{}{}
}

func (c *Config) HasIndex(table, name string) (sql string, values []interface{}, process func(ztype.Maps) bool) {
	return "SELECT * FROM sqlite_master WHERE type = 'index' AND tbl_name = '" + table + "' AND name = '" + name + "'", []interface{}{}, func(data ztype.Maps) bool {
		for i := range data {
			c := data[i]
			if c["name"] == name {
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

	return fmt.Sprintf(`CREATE %s INDEX "%s" ON "%s" (%s)`, indexType, name, table, strings.Join(fields, ",")), []interface{}{}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	panic("implement me")
}
