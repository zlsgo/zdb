package driver

import (
	"database/sql"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

type (
	IfeConfig interface {
		GetDsn() string
		GetDriver() string
		DB() *sql.DB
		MustDB() (*sql.DB, error)
		SetDB(*sql.DB)
	}
)

type Typ int

type Dialect interface {
	Value() Typ
	DataTypeOf(field *schema.Field) string
	HasTable(table string) (sql string, values []interface{}, process func([]ztype.Map) bool)
}

const (
	MySQL Typ = iota + 1
	PostgreSQL
	SQLite
	MsSQL
)

// String returns the name of driver
func (f Typ) String() string {
	switch f {
	case MySQL:
		return "MySQL"
	case PostgreSQL:
		return "PostgreSQL"
	case SQLite:
		return "SQLite"
	case MsSQL:
		return "MsSQL"
	}

	return "<invalid>"
}

// Quote adds quote for name to make sure the name can be used safely
func (f Typ) Quote(name string) string {
	switch f {
	case MySQL:
		return "`" + name + "`"
	case PostgreSQL, MsSQL, SQLite:
		return `"` + name + `"`
	}

	return name
}
