package builder

import (
	"errors"
	"strconv"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

// DeleteBuilder is a builder to build DELETE
type DeleteBuilder struct {
	Cond        *BuildCond
	table       string
	order       string
	whereExprs  []string
	orderByCols []string
	limit       int
}

var _ Builder = new(DeleteBuilder)

// Delete creates a new DELETE builder
func Delete(table string) *DeleteBuilder {
	cond := newCond(DefaultDriver, false)
	return &DeleteBuilder{
		Cond:  cond,
		limit: -1,
		table: Escape(table),
	}
}

// SetDriver Set the compilation statements driver
func (b *DeleteBuilder) SetDriver(driver driver.Dialect) *DeleteBuilder {
	b.Cond.driver = driver
	return b
}

// Where sets expressions of WHERE in DELETE
func (b *DeleteBuilder) Where(andExpr ...string) *DeleteBuilder {
	b.whereExprs = append(b.whereExprs, andExpr...)
	return b
}

// OrderBy sets columns of ORDER BY in DELETE
func (b *DeleteBuilder) OrderBy(col ...string) *DeleteBuilder {
	b.orderByCols = col
	return b
}

// Asc sets order of ORDER BY to ASC
func (b *DeleteBuilder) Asc() *DeleteBuilder {
	b.order = "ASC"
	return b
}

// Desc sets order of ORDER BY to DESC
func (b *DeleteBuilder) Desc() *DeleteBuilder {
	b.order = "DESC"
	return b
}

// Limit sets the LIMIT in DELETE
func (b *DeleteBuilder) Limit(limit int) *DeleteBuilder {
	b.limit = limit
	return b
}

// String returns the compiled DELETE string
func (b *DeleteBuilder) String() string {
	s, _ := b.build(true)
	return s
}

// Build returns compiled DELETE string and Cond
func (b *DeleteBuilder) Build() (sql string, values []interface{}, err error) {
	if len(b.whereExprs) == 0 {
		return "", nil, errors.New("delete safety error: no where condition")
	}

	sql, values = b.build(false)
	return
}

func (b *DeleteBuilder) build(blend bool) (sql string, args []interface{}) {
	// More accurate size estimation
	estimatedSize := 12 + len(b.table) + 4 // "DELETE FROM " + table + quotes
	
	if len(b.whereExprs) > 0 {
		estimatedSize += 7 // " WHERE "
		for _, expr := range b.whereExprs {
			estimatedSize += len(expr) + 5 // expr + " AND "
		}
	}
	
	if len(b.orderByCols) > 0 {
		estimatedSize += 10 // " ORDER BY "
		for _, col := range b.orderByCols {
			estimatedSize += len(col) + 2 // col + ", "
		}
		if b.order != "" {
			estimatedSize += len(b.order) + 1 // order + space
		}
	}
	
	if b.limit >= 0 {
		estimatedSize += 15 // " LIMIT " + number or complex subquery
	}

	buf := zutil.GetBuff(uint(estimatedSize))
	defer zutil.PutBuff(buf)

	driverValue := b.Cond.driver.Value()

	buf.WriteString("DELETE FROM ")
	buf.WriteString(driverValue.Quote(b.table))

	if b.limit >= 0 {
		if driverValue != driver.MySQL {
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

	if blend {
		return b.Cond.CompileString(buf.String()), nil
	}

	return b.Cond.Compile(buf.String())
}

func (b *DeleteBuilder) buildStatement() []byte {
	return buildWhereOrderStatement(b.Cond, b.whereExprs, b.orderByCols, b.order)
}
