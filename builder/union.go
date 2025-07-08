package builder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

// UnionBuilder is a builder to build UNION
type UnionBuilder struct {
	cond        *BuildCond
	opt         string
	order       string
	builders    []*SelectBuilder
	orderByCols []string
	limit       int
	offset      int
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
	estimatedSize := 256
	if len(b.builders) > 0 {
		estimatedSize += len(b.builders) * 50
	}
	if len(b.orderByCols) > 0 {
		estimatedSize += len(b.orderByCols) * 10
	}

	buf := zutil.GetBuff(uint(estimatedSize))
	defer zutil.PutBuff(buf)

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

		for i, col := range b.orderByCols {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}

		if b.order != "" {
			buf.WriteRune(' ')
			buf.WriteString(b.order)
		}
	}

	if b.limit >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.Itoa(b.limit))
	}

	if b.offset >= 0 {
		switch driverType {
		case driver.MySQL:
			// MySQL requires LIMIT when using OFFSET
			if b.limit >= 0 {
				buf.WriteString(" OFFSET ")
				buf.WriteString(strconv.Itoa(b.offset))
			}
		case driver.PostgreSQL:
			// PostgreSQL supports OFFSET without LIMIT
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
		case driver.SQLite:
			// SQLite supports OFFSET without LIMIT (since version 3.8.0)
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
		case driver.MsSQL:
			// SQL Server uses OFFSET ROWS syntax, but only with ORDER BY
			// Since this is UNION, we'll follow the same pattern as MySQL
			if b.limit >= 0 {
				buf.WriteString(" OFFSET ")
				buf.WriteString(strconv.Itoa(b.offset))
				buf.WriteString(" ROWS")
			}
		case driver.ClickHouse:
			// ClickHouse supports OFFSET
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
		case driver.Doris:
			// Doris (based on MySQL) requires LIMIT with OFFSET
			if b.limit >= 0 {
				buf.WriteString(" OFFSET ")
				buf.WriteString(strconv.Itoa(b.offset))
			}
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

// Safety performs safety checks on the UNION builder
func (b *UnionBuilder) Safety() error {
	if len(b.builders) == 0 {
		return errors.New("union safety error: no SELECT builders specified")
	}
	if len(b.builders) == 1 {
		return errors.New("union safety warning: only one SELECT builder specified, UNION not needed")
	}

	for i, builder := range b.builders {
		if err := builder.Safety(); err != nil {
			return fmt.Errorf("union safety error: builder %d - %v", i, err)
		}
	}

	return nil
}
