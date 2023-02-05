package driver

import (
	"database/sql"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/schema"
)

type (
	IfeConfig interface {
		GetDsn() string
		SetDsn(string)
		GetDriver() string
		DB() *sql.DB
		MustDB() (*sql.DB, error)
		SetDB(*sql.DB)
	}
)

type Typ int

type Dialect interface {
	Value() Typ
	DataTypeOf(field *schema.Field, only ...bool) string
	HasTable(table string) (sql string, values []interface{}, process func(ztype.Maps) bool)
	GetColumn(table string) (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map)
	RenameColumn(table, oldName, newName string) (sql string, values []interface{})
	HasIndex(table, name string) (sql string, values []interface{}, process func(ztype.Maps) bool)
	RenameIndex(table, oldName, newName string) (sql string, values []interface{})
	CreateIndex(table, name string, columns []string, indexType string) (sql string, values []interface{})
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
