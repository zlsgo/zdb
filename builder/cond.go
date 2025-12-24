package builder

import (
	"database/sql"
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

type BuildCond struct {
	driver driver.Dialect
	zutil.Args
}

func newCond(d driver.Dialect, onlyNamed bool) *BuildCond {
	args := &BuildCond{
		driver: d,
	}

	opts := []zutil.ArgsOpt{argsCompileHandler(args)}
	if onlyNamed {
		opts = append(opts, zutil.WithOnlyNamed())
	}

	a := zutil.NewArgs(opts...)
	args.Args = *a
	return args
}

func (c *BuildCond) Var(value interface{}) string {
	if arg, ok := value.(sql.NamedArg); ok && c.driver.Value() != driver.MsSQL {
		value = arg.Value
	}
	return c.Args.Var(value)
}

// EQ represents "Field = value"
func (c *BuildCond) EQ(field string, value interface{}) string {
	return c.Cond(field, " = ", value)
}

// NE represents "Field != value"
func (c *BuildCond) NE(field string, value interface{}) string {
	return c.Cond(field, " <> ", value)
}

// GT represents "Field > value"
func (c *BuildCond) GT(field string, value interface{}) string {
	return c.Cond(field, " > ", value)
}

// GE represents "Field >= value"
func (c *BuildCond) GE(field string, value interface{}) string {
	return c.Cond(field, " >= ", value)
}

// LT represents "Field < value"
func (c *BuildCond) LT(field string, value interface{}) string {
	return c.Cond(field, " < ", value)
}

// LE represents "Field <= value"
func (c *BuildCond) LE(field string, value interface{}) string {
	return c.Cond(field, " <= ", value)
}

// In represents "Field IN (value...)"
func (c *BuildCond) In(field string, value ...interface{}) string {
	if len(value) == 0 {
		// Empty IN clause is always false in SQL
		return "1 = 0"
	}

	vs := make([]string, 0, len(value))
	for _, v := range value {
		vs = append(vs, c.Var(v))
	}
	return c.quoteField(field) + " IN (" + strings.Join(vs, ", ") + ")"
}

// NotIn represents "Field NOT IN (value...)"
func (c *BuildCond) NotIn(field string, value ...interface{}) string {
	if len(value) == 0 {
		// Empty NOT IN clause is always true in SQL
		return "1 = 1"
	}

	vs := make([]string, 0, len(value))
	for _, v := range value {
		vs = append(vs, c.Var(v))
	}
	return c.quoteField(field) + " NOT IN (" + strings.Join(vs, ", ") + ")"
}

// Like represents "Field LIKE value"
func (c *BuildCond) Like(field string, value interface{}) string {
	return c.quoteField(field) + " LIKE " + c.Var(value)
}

// NotLike represents "Field NOT LIKE value"
func (c *BuildCond) NotLike(field string, value interface{}) string {
	return c.quoteField(field) + " NOT LIKE " + c.Var(value)
}

// IsNull represents "Field IS NULL"
func (c *BuildCond) IsNull(field string) string {
	return c.quoteField(field) + " IS NULL"
}

// IsNotNull represents "Field IS NOT NULL"
func (c *BuildCond) IsNotNull(field string) string {
	return c.quoteField(field) + " IS NOT NULL"
}

// Between represents "Field BETWEEN lower AND upper"
func (c *BuildCond) Between(field string, lower, upper interface{}) string {
	return c.quoteField(field) + " BETWEEN " + c.Var(lower) + " AND " + c.Var(upper)
}

// NotBetween represents "Field NOT BETWEEN lower AND upper"
func (c *BuildCond) NotBetween(field string, lower, upper interface{}) string {
	return c.quoteField(field) + " NOT BETWEEN " + c.Var(lower) + " AND " + c.Var(upper)
}

// Or represents OR logic like "expr1 OR expr2 OR expr3"
func (c *BuildCond) Or(orExpr ...string) string {
	if len(orExpr) == 0 {
		return ""
	}
	if len(orExpr) == 1 {
		return "(" + orExpr[0] + ")"
	}

	capacity := 2
	for _, expr := range orExpr {
		capacity += len(expr) + 4 // expr + " OR "
	}

	var b strings.Builder
	b.Grow(capacity)
	b.WriteRune('(')
	b.WriteString(orExpr[0])
	for _, expr := range orExpr[1:] {
		b.WriteString(" OR ")
		b.WriteString(expr)
	}
	b.WriteRune(')')
	return b.String()
}

// And represents AND logic like "expr1 AND expr2 AND expr3"
func (c *BuildCond) And(expr ...string) string {
	if len(expr) == 0 {
		return ""
	}
	if len(expr) == 1 {
		return "(" + expr[0] + ")"
	}

	capacity := 2
	for _, e := range expr {
		capacity += len(e) + 5 // expr + " AND "
	}

	var b strings.Builder
	b.Grow(capacity)
	b.WriteRune('(')
	b.WriteString(expr[0])
	for _, e := range expr[1:] {
		b.WriteString(" AND ")
		b.WriteString(e)
	}
	b.WriteRune(')')
	return b.String()
}

func (c *BuildCond) Cond(field, condition string, value interface{}) string {
	switch v := value.(type) {
	case func() string:
		return c.quoteField(field) + condition + "(" + v() + ")"
	default:
		return c.quoteField(field) + condition + c.Var(value)
	}
}

func (c *BuildCond) quoteField(field string) string {
	return c.driver.Value().Quote(Escape(field))
}
