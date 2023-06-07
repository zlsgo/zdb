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
	Cond
	args        *buildArgs
	order       string
	table       string
	assignments []string
	whereExprs  []string
	orderByCols []string
	limit       int
}

var _ Builder = new(UpdateBuilder)

// Update creates a new UPDATE builder
func Update(table string) *UpdateBuilder {
	cond := NewCond()
	return &UpdateBuilder{
		Cond:  *cond,
		limit: -1,
		args:  cond.Args,
		table: Escape(table),
	}
}

// SetDriver Set the compilation statements driver
func (b *UpdateBuilder) SetDriver(driver driver.Dialect) *UpdateBuilder {
	b.args.driver = driver
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
	return Escape(field) + " = " + b.args.Map(value)
}

// Incr represents SET "field = field + 1" in UPDATE
func (b *UpdateBuilder) Incr(field string) string {
	f := Escape(field)
	return f + " = " + f + " + 1"
}

// Decr represents SET "field = field - 1" in UPDATE
func (b *UpdateBuilder) Decr(field string) string {
	f := Escape(field)
	return f + " = " + f + " - 1"
}

// Add represents SET "field = field + value" in UPDATE
func (b *UpdateBuilder) Add(field string, value interface{}) string {
	f := Escape(field)
	return f + " = " + f + " + " + b.args.Map(value)
}

// Sub represents SET "field = field - value" in UPDATE
func (b *UpdateBuilder) Sub(field string, value interface{}) string {
	f := Escape(field)
	return f + " = " + f + " - " + b.args.Map(value)
}

// Mul represents SET "field = field * value" in UPDATE
func (b *UpdateBuilder) Mul(field string, value interface{}) string {
	f := Escape(field)
	return f + " = " + f + " * " + b.args.Map(value)
}

// Div represents SET "field = field / value" in UPDATE
func (b *UpdateBuilder) Div(field string, value interface{}) string {
	f := Escape(field)
	return f + " = " + f + " / " + b.args.Map(value)
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

// String returns the compiled UPDATE string
func (b *UpdateBuilder) String() string {
	s, _ := b.build(true)
	return s
}

// Build returns compiled UPDATE string and args
func (b *UpdateBuilder) Build() (sql string, args []interface{}) {
	return b.build(false)
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
		buf.WriteString(strings.Join(b.orderByCols, ", "))

		if b.order != "" {
			buf.WriteRune(' ')
			buf.WriteString(b.order)
		}
	}

	return buf.Bytes()
}

func (b *UpdateBuilder) build(blend bool) (sql string, args []interface{}) {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)

	buf.WriteString("UPDATE ")
	buf.WriteString(b.table)

	buf.WriteString(" SET ")
	buf.WriteString(strings.Join(b.assignments, ", "))

	if b.limit >= 0 {
		if b.args.driver.Value() == driver.SQLite {
			buf.WriteString(" WHERE ")
			buf.WriteString(IDKey)
			buf.WriteString(" IN (")

			buf.WriteString("SELECT ")
			buf.WriteString(IDKey)
			buf.WriteString(" FROM ")
			buf.WriteString(b.table)
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

	if blend {
		return b.args.CompileString(buf.String()), nil
	}

	return b.args.Compile(buf.String())
}

func (b *UpdateBuilder) Safety() error {
	if len(b.whereExprs) == 0 {
		return errors.New("update safety error: no where condition")
	}
	return nil
}
