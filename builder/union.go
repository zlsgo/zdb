package builder

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/zlsgo/zdb/driver"
)

// UnionBuilder is a builder to build UNION
type UnionBuilder struct {
	limit       int
	offset      int
	opt         string
	order       string
	cond        *BuildCond
	builders    []*SelectBuilder
	orderByCols []string
}

var _ Builder = new(UnionBuilder)

// Union creates a new UNION builder
func Union(builders ...*SelectBuilder) *UnionBuilder {
	cond := newCond(DefaultDriver, false)
	return &UnionBuilder{
		opt:      " UNION ",
		limit:    -1,
		offset:   -1,
		cond:     cond,
		builders: builders,
	}
}

// UnionAll creates a new UNION builder using UNION ALL operator
func UnionAll(builders ...*SelectBuilder) *UnionBuilder {
	cond := newCond(DefaultDriver, false)
	return &UnionBuilder{
		opt:      " UNION ALL ",
		limit:    -1,
		offset:   -1,
		cond:     cond,
		builders: builders,
	}
}

// SetDriver Set the compilation statements driver
func (b *UnionBuilder) SetDriver(driver driver.Dialect) *UnionBuilder {
	b.cond.driver = driver
	return b
}

// OrderBy sets columns of ORDER BY in SELECT
func (b *UnionBuilder) OrderBy(col ...string) *UnionBuilder {
	b.orderByCols = col
	return b
}

// Asc sets order of ORDER BY to ASC
func (b *UnionBuilder) Asc() *UnionBuilder {
	b.order = "ASC"
	return b
}

// Desc sets order of ORDER BY to DESC
func (b *UnionBuilder) Desc() *UnionBuilder {
	b.order = "DESC"
	return b
}

// Limit sets the LIMIT in SELECT
func (b *UnionBuilder) Limit(limit int) *UnionBuilder {
	b.limit = limit
	return b
}

// Offset sets the LIMIT offset in SELECT
func (b *UnionBuilder) Offset(offset int) *UnionBuilder {
	b.offset = offset
	return b
}

// String returns the compiled SELECT string
func (b *UnionBuilder) String() string {
	s, _ := b.build(true)
	return s
}

// Build returns compiled SELECT string and Cond
func (b *UnionBuilder) Build() (sql string, values []interface{}, err error) {
	sql, values = b.build(false)
	return
}

func (b *UnionBuilder) build(blend bool) (sql string, args []interface{}) {
	buf := &bytes.Buffer{}
	driverType := b.cond.driver.Value()
	if len(b.builders) > 0 {
		needParen := driverType != driver.SQLite

		if needParen {
			buf.WriteRune('(')
		}

		buf.WriteString(b.Var(b.builders[0]))

		if needParen {
			buf.WriteRune(')')
		}

		for _, v := range b.builders[1:] {
			buf.WriteString(b.opt)

			if needParen {
				buf.WriteRune('(')
			}

			buf.WriteString(b.Var(v))

			if needParen {
				buf.WriteRune(')')
			}
		}
	}

	if len(b.orderByCols) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(b.orderByCols, ", "))

		if b.order != "" {
			buf.WriteRune(' ')
			buf.WriteString(b.order)
		}
	}

	if b.limit >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.Itoa(b.limit))

	}

	if driver.MySQL == driverType && b.limit >= 0 || driver.PostgreSQL == driverType {
		if b.offset >= 0 {
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
		}
	}

	if blend {
		return b.cond.CompileString(buf.String()), nil
	}

	return b.cond.Compile(buf.String())
}

// Var returns a placeholder for value
func (b *UnionBuilder) Var(arg interface{}) string {
	return b.cond.Var(arg)
}

func (b *UnionBuilder) Safety() error {
	return nil
}
