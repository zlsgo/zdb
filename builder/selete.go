package builder

import (
	"strconv"
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

type (
	// JoinOption is the option in JOIN
	JoinOption string
	// SelectBuilder is a builder to build SELECT
	SelectBuilder struct {
		Cond        *BuildCond
		order       string
		forWhat     string
		havingExprs []string
		joinOptions []JoinOption
		joinTables  []string
		joinExprs   [][]string
		whereExprs  []string
		groupByCols []string
		orderByCols []string
		selectCols  []string
		tables      []string
		limit       int
		offset      int
		done        bool
		distinct    bool
	}
)

const (
	FullJoin       JoinOption = "FULL"
	FullOuterJoin  JoinOption = "FULL OUTER"
	InnerJoin      JoinOption = "INNER"
	LeftJoin       JoinOption = "LEFT"
	LeftOuterJoin  JoinOption = "LEFT OUTER"
	RightJoin      JoinOption = "RIGHT"
	RightOuterJoin JoinOption = "RIGHT OUTER"
)

var _ Builder = new(SelectBuilder)

// Query return new SELECT builder
func Query(table string) *SelectBuilder {
	cond := newCond(DefaultDriver, false)
	return &SelectBuilder{
		limit:  -1,
		offset: -1,
		Cond:   cond,
		tables: []string{table},
	}
}

// Select  return new SELECT builder and sets columns in SELECT
func Select(cols ...string) *SelectBuilder {
	cond := newCond(DefaultDriver, false)
	return &SelectBuilder{
		limit:      -1,
		offset:     -1,
		Cond:       cond,
		selectCols: cols,
	}
}

// SetDriver Set the compilation statements driver
func (b *SelectBuilder) SetDriver(driver driver.Dialect) *SelectBuilder {
	b.Cond.driver = driver
	return b
}

// Distinct marks this SELECT as DISTINCT
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true
	return b
}

// From sets table names in SELECT
func (b *SelectBuilder) From(table ...string) *SelectBuilder {
	b.tables = table
	return b
}

// Select sets columns in SELECT
func (b *SelectBuilder) Select(cols ...string) *SelectBuilder {
	b.selectCols = b.selectCols[:0]
	for i := range cols {
		b.selectCols = append(b.selectCols, strings.Split(cols[i], ",")...)
	}

	return b
}

// Join sets expressions of JOIN in SELECT
func (b *SelectBuilder) Join(table string, onExpr ...string) *SelectBuilder {
	return b.JoinWithOption("", table, onExpr...)
}

// JoinWithOption sets expressions of JOIN with an option
func (b *SelectBuilder) JoinWithOption(option JoinOption, table string, onExpr ...string) *SelectBuilder {
	b.joinOptions = append(b.joinOptions, option)
	b.joinTables = append(b.joinTables, table)
	b.joinExprs = append(b.joinExprs, onExpr)
	return b
}

// Where sets expressions of WHERE in SELECT
func (b *SelectBuilder) Where(expr ...string) *SelectBuilder {
	b.whereExprs = append(b.whereExprs, expr...)
	return b
}

// Having sets expressions of HAVING in SELECT
func (b *SelectBuilder) Having(expr ...string) *SelectBuilder {
	b.havingExprs = append(b.havingExprs, expr...)
	return b
}

// GroupBy sets columns of GROUP BY in SELECT
func (b *SelectBuilder) GroupBy(col ...string) *SelectBuilder {
	b.groupByCols = append(b.groupByCols, col...)
	return b
}

// OrderBy sets columns of ORDER BY in SELECT
func (b *SelectBuilder) OrderBy(col ...string) *SelectBuilder {
	if len(col) == 0 {
		b.orderByCols = b.orderByCols[:0]
		return b
	}
	b.orderByCols = append(b.orderByCols, col...)
	return b
}

// Asc sets order of ORDER BY to ASC
func (b *SelectBuilder) Asc(col ...string) *SelectBuilder {
	b.order = "ASC"
	if len(col) > 0 {
		b.orderByCols = col
	}
	return b
}

// Desc sets order of ORDER BY to DESC
func (b *SelectBuilder) Desc(col ...string) *SelectBuilder {
	b.order = "DESC"
	if len(col) > 0 {
		b.orderByCols = col
	}
	return b
}

// Limit sets the LIMIT in SELECT
func (b *SelectBuilder) Limit(limit int) *SelectBuilder {
	b.limit = limit
	return b
}

// Offset sets the LIMIT offset in SELECT
func (b *SelectBuilder) Offset(offset int) *SelectBuilder {
	b.offset = offset
	return b
}

// ForUpdate adds FOR UPDATE at the end of SELECT statement
func (b *SelectBuilder) ForUpdate() *SelectBuilder {
	b.forWhat = "UPDATE"
	return b
}

// ForShare adds FOR SHARE at the end of SELECT statement
func (b *SelectBuilder) ForShare() *SelectBuilder {
	b.forWhat = "SHARE"
	return b
}

// As returns an AS expression
func (b *SelectBuilder) As(name, alias string) string {
	return name + " AS " + alias
}

// BuilderAs returns an AS expression wrapping a complex SQL
func (b *SelectBuilder) BuilderAs(builder Builder, alias string) string {
	return "(" + b.Cond.Var(builder) + ") AS " + alias
}

// Build returns compiled SELECT string
func (b *SelectBuilder) String() string {
	sql, _ := b.build(true)
	return sql
}

// Build returns compiled SELECT string and Cond
func (b *SelectBuilder) Build() (sql string, values []interface{}, err error) {
	sql, values = b.build(false)
	return
}

func (b *SelectBuilder) build(blend bool) (sql string, values []interface{}) {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)
	buf.WriteString("SELECT ")

	if b.distinct {
		buf.WriteString("DISTINCT ")
	}

	if len(b.selectCols) == 0 {
		buf.WriteString("*")
	} else {
		buf.WriteString(strings.Join(b.Cond.driver.Value().QuoteCols(b.selectCols), ", "))
	}

	buf.WriteString(" FROM ")
	buf.WriteString(strings.Join(b.Cond.driver.Value().QuoteCols(b.tables), ", "))

	for i := range b.joinTables {
		if option := b.joinOptions[i]; option != "" {
			buf.WriteRune(' ')
			buf.WriteString(string(option))
		}

		buf.WriteString(" JOIN ")
		buf.WriteString(b.joinTables[i])

		if exprs := b.joinExprs[i]; len(exprs) > 0 {
			buf.WriteString(" ON ")
			buf.WriteString(strings.Join(b.joinExprs[i], " AND "))
		}

	}

	if len(b.whereExprs) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(strings.Join(b.whereExprs, " AND "))
	}

	if len(b.groupByCols) > 0 {
		buf.WriteString(" GROUP BY ")
		buf.WriteString(strings.Join(b.groupByCols, ", "))

		if len(b.havingExprs) > 0 {
			buf.WriteString(" HAVING ")
			buf.WriteString(strings.Join(b.havingExprs, " AND "))
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

	switch b.Cond.driver.Value() {
	default:
		if b.limit > 0 {
			buf.WriteString(" LIMIT ")
			buf.WriteString(strconv.Itoa(b.limit))

			if b.offset > 0 {
				buf.WriteString(" OFFSET ")
				buf.WriteString(strconv.Itoa(b.offset))
			}
		}
	case driver.PostgreSQL:
		if b.limit > 0 {
			buf.WriteString(" LIMIT ")
			buf.WriteString(strconv.Itoa(b.limit))
		}

		if b.offset > 0 {
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
		}

	case driver.MsSQL:
		if len(b.orderByCols) == 0 && (b.limit >= 0 || b.offset >= 0) {
			buf.WriteString(" ORDER BY 1")
		}

		if b.offset > 0 {
			buf.WriteString(" OFFSET ")
			buf.WriteString(strconv.Itoa(b.offset))
			buf.WriteString(" ROWS")
		}

		if b.limit > 0 {
			if b.offset < 0 {
				buf.WriteString(" OFFSET 0 ROWS")
			}

			buf.WriteString(" FETCH NEXT ")
			buf.WriteString(strconv.Itoa(b.limit))
			buf.WriteString(" ROWS ONLY")
		}
	}

	if b.forWhat != "" {
		buf.WriteString(" FOR ")
		buf.WriteString(b.forWhat)
	}

	if blend {
		return b.Cond.CompileString(buf.String()), nil
	}

	return b.Cond.Compile(buf.String())
}
