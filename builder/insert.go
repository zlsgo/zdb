package builder

import (
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

// InsertBuilder is a builder to build INSERT
type InsertBuilder struct {
	cond    *BuildCond
	verb    string
	table   string
	cols    []string
	values  [][]string
	options [][]string
}

var _ Builder = new(InsertBuilder)

// Insert sets table name in INSERT
func Insert(table string) *InsertBuilder {
	cond := newCond(DefaultDriver, false)
	return &InsertBuilder{
		cond:  cond,
		verb:  "INSERT",
		table: table,
	}
}

// Replace Insert sets table name in INSERT, REPLACE is a MySQL to the SQL standard
func Replace(table string) *InsertBuilder {
	cond := newCond(DefaultDriver, false)
	return &InsertBuilder{
		cond:  cond,
		verb:  "REPLACE",
		table: table,
	}
}

// InsertIgnore sets table name in INSERT IGNORE
func InsertIgnore(table string) *InsertBuilder {
	cond := newCond(DefaultDriver, false)
	return &InsertBuilder{
		cond:  cond,
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
func (b *InsertBuilder) Values(v ...interface{}) *InsertBuilder {
	placeholders := make([]string, 0, len(v))

	for _, v := range v {
		placeholders = append(placeholders, b.cond.Var(v))
	}

	b.values = append(b.values, placeholders)
	return b
}

func (b *InsertBuilder) Option(opt ...string) *InsertBuilder {
	b.options = append(b.options, opt)
	return b
}

// String returns the compiled INSERT string
func (b *InsertBuilder) String() string {
	sql, _ := b.build(true)
	return sql
}

// Build returns compiled INSERT string and Cond
func (b *InsertBuilder) Build() (sql string, values []interface{}, err error) {
	sql, values = b.build(false)
	return
}

func (b *InsertBuilder) build(blend bool) (sql string, args []interface{}) {
	estimatedSize := 256
	estimatedSize += len(b.table) * 2
	if len(b.cols) > 0 {
		estimatedSize += len(b.cols) * 10
	}
	if len(b.values) > 0 {
		estimatedSize += len(b.values) * 20
	}
	if len(b.options) > 0 {
		estimatedSize += len(b.options) * 15
	}

	buf := zutil.GetBuff(uint(estimatedSize))
	defer zutil.PutBuff(buf)

	driverValue := b.cond.driver.Value()

	buf.WriteString(b.verb)
	buf.WriteString(" INTO ")
	buf.WriteString(driverValue.Quote(b.table))

	if len(b.cols) > 0 {
		buf.WriteString(" (")

		quotedCols := driverValue.QuoteCols(b.cols)
		for i, col := range quotedCols {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}

		buf.WriteString(")")
	}

	buf.WriteString(" VALUES ")

	for i, v := range b.values {
		if i > 0 {
			buf.WriteString(", ")
		}

		buf.WriteRune('(')

		for j, val := range v {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(val)
		}

		buf.WriteRune(')')
	}

	if len(b.options) > 0 {
		buf.WriteRune(' ')

		for i, opt := range b.options {
			if i > 0 {
				buf.WriteString(", ")
			}

			for j, o := range opt {
				if j > 0 {
					buf.WriteRune(' ')
				}
				buf.WriteString(o)
			}
		}
	}

	if blend {
		return b.cond.CompileString(buf.String()), nil
	}

	return b.cond.Compile(buf.String())
}

// SetDriver Set the compilation statements driver
func (b *InsertBuilder) SetDriver(driver driver.Dialect) *InsertBuilder {
	b.cond.driver = driver
	return b
}

// Var returns a placeholder for value
func (b *InsertBuilder) Var(arg interface{}) string {
	return b.cond.Var(arg)
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
