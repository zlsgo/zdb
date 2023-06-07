package builder

import (
	"fmt"
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

// InsertBuilder is a builder to build INSERT
type InsertBuilder struct {
	args   *buildArgs
	verb   string
	table  string
	cols   []string
	values [][]string
}

var _ Builder = new(InsertBuilder)

// Insert sets table name in INSERT
func Insert(table string) *InsertBuilder {
	cond := NewCond()
	return &InsertBuilder{
		args:  cond.Args,
		verb:  "INSERT",
		table: table,
	}
}

// Replace Insert sets table name in INSERT, REPLACE is a MySQL to the SQL standard
func Replace(table string) *InsertBuilder {
	cond := NewCond()
	return &InsertBuilder{
		args:  cond.Args,
		verb:  "REPLACE",
		table: table,
	}
}

// InsertIgnore sets table name in INSERT IGNORE
func InsertIgnore(table string) *InsertBuilder {
	cond := NewCond()
	return &InsertBuilder{
		args:  cond.Args,
		verb:  "INSERT IGNORE",
		table: table,
	}
}

// Cols sets columns in INSERT
func (b *InsertBuilder) Cols(col ...string) *InsertBuilder {
	b.cols = EscapeAll(col...)
	return b
}

// Values adds a list of values for a row in INSERT
func (b *InsertBuilder) Values(value ...interface{}) *InsertBuilder {
	placeholders := make([]string, 0, len(value))

	for _, v := range value {
		placeholders = append(placeholders, b.args.Map(v))
	}

	b.values = append(b.values, placeholders)
	return b
}

// String returns the compiled INSERT string
func (b *InsertBuilder) String() string {
	sql, _ := b.build(true)
	return sql
}

// Build returns compiled INSERT string and args
func (b *InsertBuilder) Build() (sql string, args []interface{}) {
	return b.build(false)
}

func (b *InsertBuilder) build(blend bool) (sql string, args []interface{}) {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)
	buf.WriteString(b.verb)
	buf.WriteString(" INTO ")
	buf.WriteString(b.table)
	if len(b.cols) > 0 {
		buf.WriteString(" (")
		buf.WriteString(strings.Join(b.cols, ", "))
		buf.WriteString(")")
	}

	buf.WriteString(" VALUES ")
	values := make([]string, 0, len(b.values))

	for _, v := range b.values {
		values = append(values, fmt.Sprintf("(%v)", strings.Join(v, ", ")))
	}

	buf.WriteString(strings.Join(values, ", "))

	if blend {
		return b.args.CompileString(buf.String()), nil
	}

	return b.args.Compile(buf.String())
}

// SetDriver Set the compilation statements driver
func (b *InsertBuilder) SetDriver(driver driver.Dialect) *InsertBuilder {
	b.args.driver = driver
	return b
}

// Var returns a placeholder for value
func (b *InsertBuilder) Var(arg interface{}) string {
	return b.args.Map(arg)
}

func (b *InsertBuilder) Safety() error {
	return nil
}

// BatchValues adds a list of values for a batch in INSERT
func (b *InsertBuilder) BatchValues(values [][]interface{}) *InsertBuilder {
	for _, v := range values {
		b.Values(v...)
	}
	return b
}
