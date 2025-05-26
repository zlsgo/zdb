package builder

import (
	"errors"
	"strconv"
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

// UpdateBuilder is a builder to build UPDATE
type UpdateBuilder struct {
	Cond        *BuildCond
	table       string
	order       string
	assignments []string
	whereExprs  []string
	orderByCols []string
	options     [][]string
	limit       int
	allowEmpty  bool
}

var _ Builder = new(UpdateBuilder)

// Update creates a new UPDATE builder
func Update(table string) *UpdateBuilder {
	cond := newCond(DefaultDriver, false)
	return &UpdateBuilder{
		limit: -1,
		Cond:  cond,
		table: Escape(table),
	}
}

// SetDriver Set the compilation statements driver
func (b *UpdateBuilder) SetDriver(driver driver.Dialect) *UpdateBuilder {
	b.Cond.driver = driver
	return b
}

// Set sets the assignments in SET
func (b *UpdateBuilder) Set(assignment ...string) *UpdateBuilder {
	b.assignments = assignment
	return b
}

// SetMore appends the assignments in SET
func (b *UpdateBuilder) SetMore(assignment ...string) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment...)
	return b
}

// Where sets expressions of WHERE in UPDATE
func (b *UpdateBuilder) Where(andExpr ...string) *UpdateBuilder {
	b.whereExprs = append(b.whereExprs, andExpr...)
	return b
}

// Assign represents SET "field = value" in UPDATE
func (b *UpdateBuilder) Assign(field string, value interface{}) string {
	return b.Cond.Cond(field, " = ", value)
}

// Incr represents SET "field = field + 1" in UPDATE
func (b *UpdateBuilder) Incr(field string) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " + 1"
}

// Decr represents SET "field = field - 1" in UPDATE
func (b *UpdateBuilder) Decr(field string) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " - 1"
}

// Add represents SET "field = field + value" in UPDATE
func (b *UpdateBuilder) Add(field string, value interface{}) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " + " + b.Cond.Var(value)
}

// Sub represents SET "field = field - value" in UPDATE
func (b *UpdateBuilder) Sub(field string, value interface{}) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " - " + b.Cond.Var(value)
}

// Mul represents SET "field = field * value" in UPDATE
func (b *UpdateBuilder) Mul(field string, value interface{}) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " * " + b.Cond.Var(value)
}

// Div represents SET "field = field / value" in UPDATE
func (b *UpdateBuilder) Div(field string, value interface{}) string {
	f := b.Cond.quoteField(field)
	return f + " = " + f + " / " + b.Cond.Var(value)
}

// OrderBy sets columns of ORDER BY in UPDATE
func (b *UpdateBuilder) OrderBy(col ...string) *UpdateBuilder {
	b.orderByCols = col
	return b
}

// Asc sets order of ORDER BY to ASC
func (b *UpdateBuilder) Asc() *UpdateBuilder {
	b.order = "ASC"
	return b
}

// Desc sets order of ORDER BY to DESC
func (b *UpdateBuilder) Desc() *UpdateBuilder {
	b.order = "DESC"
	return b
}

// Limit sets the LIMIT in UPDATE
func (b *UpdateBuilder) Limit(limit int) *UpdateBuilder {
	b.limit = limit
	return b
}

func (b *UpdateBuilder) Option(opt ...string) *UpdateBuilder {
	b.options = append(b.options, opt)
	return b
}

// String returns the compiled UPDATE string
func (b *UpdateBuilder) String() string {
	s, _ := b.build(true)
	return s
}

// Build returns compiled UPDATE string and Cond
func (b *UpdateBuilder) Build() (sql string, value []interface{}, err error) {
	if len(b.whereExprs) == 0 {
		return "", nil, errors.New("update safety error: no where condition")
	}

	sql, value = b.build(false)
	return
}

func (b *UpdateBuilder) buildStatement() []byte {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)
	if len(b.whereExprs) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(strings.Join(b.whereExprs, " AND "))
	}

	if len(b.orderByCols) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(b.Cond.driver.Value().QuoteCols(b.orderByCols), ", "))

		if b.order != "" {
			buf.WriteRune(' ')
			buf.WriteString(b.order)
		}
	}

	return buf.Bytes()
}

func (b *UpdateBuilder) build(blend bool) (sql string, args []interface{}) {
	estimatedSize := 256
	estimatedSize += len(b.table) * 2
	if len(b.assignments) > 0 {
		estimatedSize += len(b.assignments) * 15
	}
	if len(b.whereExprs) > 0 {
		estimatedSize += len(b.whereExprs) * 15
	}
	if len(b.orderByCols) > 0 {
		estimatedSize += len(b.orderByCols) * 10
	}
	if len(b.options) > 0 {
		estimatedSize += len(b.options) * 15
	}

	buf := zutil.GetBuff(uint(estimatedSize))
	defer zutil.PutBuff(buf)

	driverValue := b.Cond.driver.Value()

	buf.WriteString("UPDATE ")
	buf.WriteString(driverValue.Quote(b.table))

	buf.WriteString(" SET ")
	for i, assignment := range b.assignments {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(assignment)
	}

	if b.limit >= 0 {
		if driverValue == driver.SQLite {
			buf.WriteString(" WHERE ")
			buf.WriteString(IDKey)
			buf.WriteString(" IN (")

			buf.WriteString("SELECT ")
			buf.WriteString(IDKey)
			buf.WriteString(" FROM ")
			buf.WriteString(driverValue.Quote(b.table))
			buf.Write(b.buildStatement())
			buf.WriteString(" LIMIT ")
			buf.WriteString(strconv.Itoa(b.limit))

			buf.WriteString(")")
		} else {
			buf.Write(b.buildStatement())

			buf.WriteString(" LIMIT ")
			buf.WriteString(strconv.Itoa(b.limit))
		}
	} else {
		buf.Write(b.buildStatement())
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
		return b.Cond.CompileString(buf.String()), nil
	}

	return b.Cond.Compile(buf.String())
}
