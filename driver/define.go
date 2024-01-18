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
func (f Typ) Quote(col string) string {
	if col == "*" || col[0] == '(' {
		return col
	}

	sl := strings.SplitN(col, " ", 2)
	if strings.IndexRune(sl[0], '.') > 0 {
		s := strings.Split(sl[0], ".")
		for i := range s {
			if s[i] == "*" {
				continue
			}
			s[i] = f.quote(s[i])
		}
		sl[0] = strings.Join(s, ".")
	} else {
		sl[0] = f.quote(sl[0])
	}

	return strings.Join(sl, " ")
}

func (f Typ) quote(col string) string {
	if strings.ContainsRune(col, '(') {
		return col
	}
	switch f {
	case MySQL:
		return "`" + col + "`"
	case PostgreSQL, MsSQL, SQLite:
		return `"` + col + `"`
	}

	return col
}

func (f Typ) QuoteCols(cols []string) []string {
	nm := make([]string, 0, len(cols))

	for i := range cols {
		nm = append(nm, f.Quote(cols[i]))
	}

	return nm
}
