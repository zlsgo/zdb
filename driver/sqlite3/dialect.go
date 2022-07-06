package sqlite3

import (
	"errors"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

func (c *Config) GetVersion() (string, error) {
	// sql := "SELECT SQLITE_VERSION()"

	// data, err := quick.QueryToMap(c.DB(), sql)
	// if err != nil {
	// 	return "", err
	// }

	// if v, ok := data[0]["SQLITE_VERSION()"]; ok {
	// 	return v.(string), nil
	// }

	return "", errors.New("can't get the version")
}

func (c *Config) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "numeric"
	case schema.String:
		return "text"
	case schema.Int, schema.Uint:
		return "integer"
	case schema.Float:
		return "real"
	case schema.Time:
		return "datetime"
	case schema.Bytes:
		return "blob"
	}

	return string(field.DataType)
}

func (c *Config) HasTable(table string) (sql string, values []interface{}, process func(result []map[string]interface{}) bool) {
	return "SELECT count(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?", []interface{}{table}, func(data []map[string]interface{}) bool {
		if len(data) > 0 {
			return ztype.ToInt64(data[0]["count"]) > 0
		}
		return false
	}
}
