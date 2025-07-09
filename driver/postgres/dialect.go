package postgres

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/sohaha/zlsgo/zarray"
	"github.com/sohaha/zlsgo/zstring"
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

func (c *Config) DataTypeOf(f *schema.Field, only ...bool) string {
	t := zstring.Buffer()
	switch f.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		size := f.Size

		if f.AutoIncrement {
			switch {
			case size == 0:
				t.WriteString("bigserial")
			case size <= 32767:
				t.WriteString("smallserial")
			case size <= 2147483647:
				t.WriteString("serial")
			default:
				t.WriteString("bigserial")
			}
		} else {
			switch {
			case size == 0:
				t.WriteString("integer")
			case size <= 32767:
				t.WriteString("smallint")
			case size <= 2147483647:
				t.WriteString("integer")
			default:
				t.WriteString("bigint")
			}
		}
	case schema.Float:
		if f.Precision > 0 {
			if f.Scale > 0 {
				t.WriteString(fmt.Sprintf("numeric(%d, %d)", f.Precision, f.Scale))
			} else {
				t.WriteString(fmt.Sprintf("numeric(%d)", f.Precision))
			}
		} else {

			t.WriteString("decimal")
		}
	case schema.Text:
		t.WriteString("text")
	case schema.String:
		if f.Size > 0 {
			t.WriteString(fmt.Sprintf("varchar(%d)", f.Size))
		} else {
			t.WriteString("text")
		}
	case schema.Time:
		if f.Precision > 0 {
			t.WriteString(fmt.Sprintf("timestamptz(%d)", f.Precision))
		} else {
			t.WriteString("timestamptz")
		}
	case schema.Bytes:
		t.WriteString("bytea")
	default:
		t.WriteString(string(f.DataType))
	}

	if !(len(only) > 0 && only[0]) {
		if f.PrimaryKey {
			t.WriteString(" PRIMARY KEY")
		}
	}
	return t.String()
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return "SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 AND table_type = $3 AND table_catalog = $4", []interface{}{"public", table, "BASE TABLE", c.databaseName()}, func(data ztype.Maps) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}

func (c *Config) GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return "SELECT table_catalog ,c.column_name, c.is_nullable = 'YES', c.udt_name, c.character_maximum_length, c.numeric_precision, c.numeric_precision_radix, c.numeric_scale, c.datetime_precision, 8 * typlen, c.column_default, pd.description, c.identity_increment FROM information_schema.columns AS c JOIN pg_type AS pgt ON c.udt_name = pgt.typname LEFT JOIN pg_catalog.pg_description as pd ON pd.objsubid = c.ordinal_position AND pd.objoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema)) where table_catalog = $1 AND table_schema = $2 AND table_name = $3", []interface{}{c.databaseName(), "public", table}, func(data ztype.Maps) ztype.Map {
		columns := make(ztype.Map, len(data))
		data.ForEach(func(_ int, val ztype.Map) bool {
			name := ztype.ToString(val["column_name"])
			if name == "" {
				return true
			}
			f := schema.Field{
				Name:        name,
				RawDataType: ztype.ToString(val["udt_name"]),
			}
			columns[name] = f
			return true
		})
		return columns
	}
}

func (c *Config) RenameColumn(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", pq.QuoteIdentifier(table), pq.QuoteIdentifier(oldName), pq.QuoteIdentifier(newName)), []interface{}{}
}

func (c *Config) HasIndex(table, name string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return "SELECT ix.relname as index_name, upper(am.amname) AS index_algorithm, indisunique as is_unique FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid JOIN pg_class ix ON ix.oid = i.indexrelid JOIN pg_namespace n ON t.relnamespace = n.oid JOIN pg_am as am ON ix.relam = am.oid WHERE t.relname = '" + table + "'", []interface{}{}, func(data ztype.Maps) bool {
		for _, v := range data {
			if v.Get("index_name").String() == name {
				return true
			}
		}
		return false
	}
}

func (c *Config) CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{}) {
	fields := zarray.Map(columns, func(i int, val string) string {
		return "\"" + val + "\""
	})

	return fmt.Sprintf(`CREATE %s INDEX "%s" ON "%s" USING BTREE (%s)`, indexType, name, table, strings.Join(fields, ",")), []interface{}{}
}

func (c *Config) RenameIndex(table, oldName, newName string) (sql string, values []interface{}) {
	return fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pq.QuoteIdentifier(oldName), pq.QuoteIdentifier(newName)), []interface{}{}
}
