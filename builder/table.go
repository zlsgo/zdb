package builder

import (
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/schema"
)

type TableBuilder struct {
	table string
	args  *buildArgs
}
type (
	// CreateTableBuilder is a builder to build CREATE TABLE
	CreateTableBuilder struct {
		*TableBuilder
		ifNotExists bool
		verb        string
		defines     [][]string
		options     [][]string
		columns     []*schema.Field
	}
)

var _ Builder = new(CreateTableBuilder)

// NewTable creates a new table builder
func NewTable(table string) *TableBuilder {
	args := NewArgs(false)
	b := &TableBuilder{
		args:  args,
		table: Escape(table),
	}
	return b
}

// SetDriver Set the compilation statements driver
func (b *TableBuilder) SetDriver(driver driver.Dialect) {
	b.args.driver = driver
}

// Create creates a new CREATE TABLE builder
func (b *TableBuilder) Create() *CreateTableBuilder {
	table := NewTable(b.table)
	cb := &CreateTableBuilder{
		verb:         "CREATE TABLE",
		TableBuilder: table,
	}
	return cb
}

// Drop delete table
func (b *TableBuilder) Drop() string {
	return "DROP TABLE " + b.table
}

// Has queried whether the table exists
func (b *TableBuilder) Has() (sql string, values []interface{}, process func(result []map[string]interface{}) bool) {
	return b.args.driver.HasTable(b.table)
}

// CreateTable creates a new CREATE TABLE builder
func CreateTable(table string) *CreateTableBuilder {
	return NewTable(table).Create()
}

// CreateTempTable creates a new CREATE TEMPORARY TABLE builder
func CreateTempTable(table string) *CreateTableBuilder {
	return CreateTable(table).TempTable()
}

// TempTable changes to CREATE TEMPORARY TABLE
func (b *CreateTableBuilder) TempTable() *CreateTableBuilder {
	b.verb = "CREATE TEMPORARY TABLE"
	return b
}

// IfNotExists adds IF NOT EXISTS before table name in CREATE TABLE
func (b *CreateTableBuilder) IfNotExists() *CreateTableBuilder {
	b.ifNotExists = true
	return b
}

// Define adds definition of a column or index in CREATE TABLE
func (b *CreateTableBuilder) Define(def ...string) *CreateTableBuilder {
	b.defines = append(b.defines, def)
	return b
}

// Option adds a table option in CREATE TABLE
func (b *CreateTableBuilder) Option(opt ...string) *CreateTableBuilder {
	b.options = append(b.options, opt)
	return b
}

// Column adds definition of a column in CREATE TABLE
func (b *CreateTableBuilder) Column(fields ...*schema.Field) *CreateTableBuilder {
	b.columns = append(b.columns, fields...)

	return b
}

func (b *CreateTableBuilder) buildColumns() {
	l := len(b.columns)
	if l == 0 {
		return
	}

	typ := b.args.driver.Value()
	columns := make([][]string, 0, l)

	for _, f := range b.columns {
		def := make([]string, 0, 3)

		def = append(def, f.Name)
		def = append(def, b.args.driver.DataTypeOf(f))

		if f.NotNull && !f.PrimaryKey {
			def = append(def, "NOT NULL")
		}

		if f.AutoIncrement {
			def = append(def, "AUTO_INCREMENT")
		}

		if f.PrimaryKey {
			def = append(def, "PRIMARY KEY")
		}

		if len(f.Comment) > 0 && typ == driver.MySQL {
			def = append(def, "COMMENT '"+f.Comment+"'")
		}

		columns = append(columns, def)
	}
	b.defines = append(b.defines, columns...)
	b.columns = b.columns[0:0:0]
}

// Build returns compiled CREATE TABLE string and args
func (b *CreateTableBuilder) Build() (sql string, values []interface{}) {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)

	buf.WriteString(b.verb)

	if b.ifNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}

	buf.WriteRune(' ')
	buf.WriteString(b.table)

	b.buildColumns()
	if len(b.defines) > 0 {
		buf.WriteString(" (")

		defs := make([]string, 0, len(b.defines))
		for _, def := range b.defines {
			defs = append(defs, strings.Join(def, " "))
		}
		buf.WriteString(strings.Join(defs, ", "))

		buf.WriteRune(')')
	}

	if len(b.options) > 0 {
		buf.WriteRune(' ')

		opts := make([]string, 0, len(b.options))
		for _, opt := range b.options {
			opts = append(opts, strings.Join(opt, " "))
		}
		buf.WriteString(strings.Join(opts, ", "))
	}

	return b.args.Compile(buf.String())
}

// String returns the compiled INSERT string
func (b *CreateTableBuilder) String() string {
	s, _ := b.Build()
	return s
}
