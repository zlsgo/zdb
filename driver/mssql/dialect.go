package mssql

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

func autoIncrement(f *schema.Field, sqlType string) string {
	if f.AutoIncrement {
		if f.Size == 0 {
			sqlType = "bigint"
		}
		return sqlType + " IDENTITY(1,1)"
	} else {
		return sqlType
	}
}

func (c *Config) DataTypeOf(f *schema.Field, only ...bool) string {
	t := zstring.Buffer()
	switch f.DataType {
	case schema.Bool:
		t.WriteString("bit")
	case schema.Uint:
		size, sqlType := f.Size, ""
		switch {
		case size == 0:
			sqlType = "int"
		case size <= 255:
			sqlType = "smallint"
		case size <= 65535:
			sqlType = "int"
		default:
			sqlType = "bigint"
		}
		t.WriteString(autoIncrement(f, sqlType))
	case schema.Int:
		size, sqlType := f.Size, ""
		switch {
		case size == 0:
			sqlType = "int"
		case size < 127:
			sqlType = "smallint"
		case size < 32767:
			sqlType = "int"
		default:
			sqlType = "bigint"
		}
		t.WriteString(autoIncrement(f, sqlType))

	case schema.Float:
		if f.Precision > 0 {
			if f.Scale > 0 {
				t.WriteString(fmt.Sprintf("decimal(%d, %d)", f.Precision, f.Scale))
			} else {
				t.WriteString(fmt.Sprintf("decimal(%d)", f.Precision))
			}
		} else {
			t.WriteString("float")
		}
	case schema.Text:
		t.WriteString("nvarchar(MAX)")
	case schema.String:
		size := f.Size
		if size == 0 {
			size = 256
		}
		if size > 0 && size <= 4000 {
			t.WriteString(fmt.Sprintf("nvarchar(%d)", size))
		} else {
			t.WriteString("nvarchar(MAX)")
		}
	case schema.Time:
		if f.Precision > 0 {
			t.WriteString(fmt.Sprintf("datetimeoffset(%d)", f.Precision))
		} else {

			t.WriteString("datetimeoffset")
		}
	case schema.Bytes:
		t.WriteString("varbinary(MAX)")
	default:
		t.WriteString(string(f.DataType))
	}

	return t.String()
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return "SELECT count(*) AS count FROM INFORMATION_SCHEMA.tables WHERE table_name = ? AND table_catalog = ?", []interface{}{table, c.databaseName()}, func(data ztype.Maps) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_CATALOG = ?", []interface{}{table, c.databaseName()}, func(data ztype.Maps) ztype.Map {
		columns := make(ztype.Map, len(data))
		data.ForEach(func(i int, val ztype.Map) bool {
			name := ztype.ToString(val["column_name"])
			if name == "" {
				return true
			}
			columns[name] = ztype.Map{"type": ztype.ToString(val["data_type"])}
			return true
		})
		return columns
	}
}

func (c *Config) RenameColumn(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("EXEC sp_rename '%s.%s', '%s', 'COLUMN'", table, oldName, newName), []interface{}{}
}

func (c *Config) HasIndex(table, name string) (sql string, values []interface{}, process func(ztype.Maps) bool) {
	return "SELECT count(*) AS count FROM sys.indexes WHERE name = ? AND object_id = OBJECT_ID(?)", []interface{}{name, table}, func(data ztype.Maps) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}

func (c *Config) CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{}) {
	prefix := strings.TrimSpace(indexType)
	if prefix != "" {
		prefix = strings.ToUpper(prefix) + " "
	}
	cols := make([]string, 0, len(columns))
	for i := range columns {
		cols = append(cols, fmt.Sprintf("[%s]", columns[i]))
	}
	return fmt.Sprintf("CREATE %sINDEX [%s] ON [%s] (%s)", prefix, name, table, strings.Join(cols, ", ")), []interface{}{}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("EXEC sp_rename '%s.%s', '%s', 'INDEX'", table, oldName, newName), []interface{}{}
}
