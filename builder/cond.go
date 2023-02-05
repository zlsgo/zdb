package builder

import (
	"strings"
)

// Cond provides several helper methods to build conditions
type Cond struct {
	Args *buildArgs
}

// NewCond returns a new Cond
func NewCond() *Cond {
	args := NewArgs(false)
	return &Cond{
		Args: args,
	}
}

// EQ represents "Field = value"
func (c *Cond) EQ(field string, value interface{}) string {
	return Escape(field) + " = " + c.Args.Map(value)
}

// NE represents "Field != value"
func (c *Cond) NE(field string, value interface{}) string {
	return Escape(field) + " <> " + c.Args.Map(value)
}

// GT represents "Field > value"
func (c *Cond) GT(field string, value interface{}) string {
	return Escape(field) + " > " + c.Args.Map(value)
}

// GE represents "Field >= value"
func (c *Cond) GE(field string, value interface{}) string {
	return Escape(field) + " >= " + c.Args.Map(value)
}

// LT represents "Field < value"
func (c *Cond) LT(field string, value interface{}) string {
	return Escape(field) + " < " + c.Args.Map(value)
}

// LE represents "Field <= value"
func (c *Cond) LE(field string, value interface{}) string {
	return Escape(field) + " <= " + c.Args.Map(value)
}

// In represents "Field IN (value...)"
func (c *Cond) In(field string, value ...interface{}) string {
	vs := make([]string, 0, len(value))

	for _, v := range value {
		vs = append(vs, c.Args.Map(v))
	}

	return Escape(field) + " IN (" + strings.Join(vs, ", ") + ")"
}

// NotIn represents "Field NOT IN (value...)"
func (c *Cond) NotIn(field string, value ...interface{}) string {
	vs := make([]string, 0, len(value))

	for _, v := range value {
		vs = append(vs, c.Args.Map(v))
	}

	return Escape(field) + " NOT IN (" + strings.Join(vs, ", ") + ")"
}

// Like represents "Field LIKE value"
func (c *Cond) Like(field string, value interface{}) string {
	return Escape(field) + " LIKE " + c.Args.Map(value)
}

// NotLike represents "Field NOT LIKE value"
func (c *Cond) NotLike(field string, value interface{}) string {
	return Escape(field) + " NOT LIKE " + c.Args.Map(value)
}

// IsNull represents "Field IS NULL"
func (c *Cond) IsNull(field string) string {
	return Escape(field) + " IS NULL"
}

// IsNotNull represents "Field IS NOT NULL"
func (c *Cond) IsNotNull(field string) string {
	return Escape(field) + " IS NOT NULL"
}

// Between represents "Field BETWEEN lower AND upper"
func (c *Cond) Between(field string, lower, upper interface{}) string {
	return Escape(field) + " BETWEEN " + c.Args.Map(lower) + " AND " + c.Args.Map(upper)
}

// NotBetween represents "Field NOT BETWEEN lower AND upper"
func (c *Cond) NotBetween(field string, lower, upper interface{}) string {
	return Escape(field) + " NOT BETWEEN " + c.Args.Map(lower) + " AND " + c.Args.Map(upper)
}

// Or represents OR logic like "expr1 OR expr2 OR expr3"
func (c *Cond) Or(orExpr ...string) string {
	return "(" + strings.Join(orExpr, " OR ") + ")"
}

// And represents AND logic like "expr1 AND expr2 AND expr3"
func (c *Cond) And(expr ...string) string {
	return "(" + strings.Join(expr, " AND ") + ")"
}

// Var returns a placeholder for value
func (c *Cond) Var(value interface{}) string {
	return c.Args.Map(value)
}
