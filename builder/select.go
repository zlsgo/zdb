package builder

import (
	"errors"
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
	if cap(b.selectCols) >= len(cols)*2 {
		b.selectCols = b.selectCols[:0]
	} else {
		b.selectCols = make([]string, 0, len(cols)*2)
	}

	for i := range cols {
		if cols[i][0] == '(' {
			b.selectCols = append(b.selectCols, cols[i])
			continue
		}
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
	if alias == "" {
		return name
	}
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
	// More accurate size estimation
	estimatedSize := 64 // Base size for "SELECT" + "FROM" + spaces

	if b.distinct {
		estimatedSize += 9 // "DISTINCT "
	}

	if len(b.selectCols) == 0 {
		estimatedSize += 1 // "*"
	} else {
		for _, col := range b.selectCols {
			estimatedSize += len(col) + 4 // column + quotes + comma + space
		}
	}

	for _, table := range b.tables {
		estimatedSize += len(table) + 4 // table + quotes + comma + space
	}

	if len(b.joinTables) > 0 {
		for i, table := range b.joinTables {
			estimatedSize += len(table) + 10 // table + " JOIN "
			if len(b.joinOptions) > i && b.joinOptions[i] != "" {
				estimatedSize += len(string(b.joinOptions[i])) + 1 // option + space
			}
			if len(b.joinExprs) > i {
				for _, expr := range b.joinExprs[i] {
					estimatedSize += len(expr) + 5 // expr + " AND "
				}
			}
		}
	}

	if len(b.whereExprs) > 0 {
		estimatedSize += 7 // " WHERE "
		for _, expr := range b.whereExprs {
			estimatedSize += len(expr) + 5 // expr + " AND "
		}
	}

	if len(b.groupByCols) > 0 {
		estimatedSize += 10 // " GROUP BY "
		for _, col := range b.groupByCols {
			estimatedSize += len(col) + 2 // col + ", "
		}

		if len(b.havingExprs) > 0 {
			estimatedSize += 8 // " HAVING "
			for _, expr := range b.havingExprs {
				estimatedSize += len(expr) + 5 // expr + " AND "
			}
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

	if b.limit > 0 {
		estimatedSize += 20 // " LIMIT " + number
	}
	if b.offset > 0 {
		estimatedSize += 20 // " OFFSET " + number
	}

	if b.forWhat != "" {
		estimatedSize += len(b.forWhat) + 5 // " FOR " + forWhat
	}

	buf := zutil.GetBuff(uint(estimatedSize))
	defer zutil.PutBuff(buf)
	buf.WriteString("SELECT ")

	if b.distinct {
		buf.WriteString("DISTINCT ")
	}

	if len(b.selectCols) == 0 {
		buf.WriteString("*")
	} else {
		driverValue := b.Cond.driver.Value()
		quotedCols := driverValue.QuoteCols(b.selectCols)

		for i, col := range quotedCols {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}
	}

	buf.WriteString(" FROM ")

	driverValue := b.Cond.driver.Value()
	quotedTables := driverValue.QuoteCols(b.tables)
	for i, table := range quotedTables {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(table)
	}

	for i := range b.joinTables {
		if option := b.joinOptions[i]; option != "" {
			buf.WriteRune(' ')
			buf.WriteString(string(option))
		}

		buf.WriteString(" JOIN ")
		buf.WriteString(b.joinTables[i])

		if exprs := b.joinExprs[i]; len(exprs) > 0 {
			buf.WriteString(" ON ")

			for j, expr := range exprs {
				if j > 0 {
					buf.WriteString(" AND ")
				}
				buf.WriteString(expr)
			}
		}
	}

	if len(b.whereExprs) > 0 {
		buf.WriteString(" WHERE ")

		for i, expr := range b.whereExprs {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			buf.WriteString(expr)
		}
	}

	if len(b.groupByCols) > 0 {
		buf.WriteString(" GROUP BY ")

		for i, col := range b.groupByCols {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}

		if len(b.havingExprs) > 0 {
			buf.WriteString(" HAVING ")

			for i, expr := range b.havingExprs {
				if i > 0 {
					buf.WriteString(" AND ")
				}
				buf.WriteString(expr)
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

	switch driverValue {
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

// Safety performs safety checks on the SELECT builder
func (b *SelectBuilder) Safety() error {
	if len(b.tables) == 0 {
		return errors.New("select safety error: no tables specified")
	}

	if len(b.whereExprs) == 0 && len(b.joinExprs) == 0 && b.limit < 0 {
		if len(b.tables) == 1 {
			return errors.New("select safety warning: query may result in full table scan (no WHERE clause or LIMIT)")
		}
	}

	return nil
}
