package driver

import (
	"database/sql"
	"strings"

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
	ClickHouse
	Doris
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
	case ClickHouse:
		return "ClickHouse"
	case Doris:
		return "Doris"
	}

	return "<invalid>"
}

// Quote adds quote for name to make sure the name can be used safely
func (f Typ) Quote(col string) string {
	if col == "*" || (len(col) > 0 && col[0] == '(') {
		return col
	}

	if spaceIdx := strings.IndexByte(col, ' '); spaceIdx > 0 {
		parts := strings.SplitN(col, " ", 2)
		parts[0] = f.quoteSingleIdentifier(parts[0])
		return strings.Join(parts, " ")
	}

	return f.quoteSingleIdentifier(col)
}

// quoteSingleIdentifier quotes a single identifier, handling dot notation
func (f Typ) quoteSingleIdentifier(col string) string {
	if dotIdx := strings.IndexByte(col, '.'); dotIdx > 0 {
		parts := strings.Split(col, ".")
		for i, part := range parts {
			if part != "*" {
				parts[i] = f.quote(part)
			}
		}
		return strings.Join(parts, ".")
	}
	return f.quote(col)
}

// quote quotes a single identifier
func (f Typ) quote(col string) string {
	if strings.ContainsRune(col, '(') {
		return col
	}
	switch f {
	case MySQL, Doris:
		return "`" + col + "`"
	case PostgreSQL, MsSQL, SQLite, ClickHouse:
		return `"` + col + `"`
	}

	return col
}

// QuoteCols quotes a list of identifiers
func (f Typ) QuoteCols(cols []string) []string {
	if len(cols) == 0 {
		return cols
	}

	nm := make([]string, len(cols))
	for i, col := range cols {
		nm[i] = f.Quote(col)
	}
	return nm
}
