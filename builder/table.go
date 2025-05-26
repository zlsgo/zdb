package builder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/schema"
)

type TableBuilder struct {
	args  *BuildCond
	table string
}

type (
	// CreateTableBuilder is a builder to build CREATE TABLE
	CreateTableBuilder struct {
		*TableBuilder
		verb        string
		defines     [][]string
		options     [][]string
		columns     []*schema.Field
		ifNotExists bool
	}
)

var _ Builder = new(CreateTableBuilder)

// NewTable creates a new table builder
func NewTable(table string) *TableBuilder {
	args := newCond(DefaultDriver, false)
	b := &TableBuilder{
		args:  args,
		table: table,
	}
	return b
}

// SetDriver Set the compilation statements driver
func (b *TableBuilder) SetDriver(driver driver.Dialect) {
	b.args.driver = driver
}

func (b *TableBuilder) GetDriver() driver.Dialect {
	return b.args.driver
}

// Drop delete table
func (b *TableBuilder) tableName() string {
	typ := b.args.driver.Value()
	// switch typ {
	// case driver.PostgreSQL:
	// 	if !strings.ContainsRune(b.table, '.') {
	// 		return pq.QuoteIdentifier(b.table)
	// 	}
	// }

	return typ.Quote(b.table)
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
	return "DROP TABLE " + b.tableName()
}

// Has queried whether the table exists
func (b *TableBuilder) Has() (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return b.args.driver.HasTable(b.table)
}

// HasIndex queried whether the table index exists
func (b *TableBuilder) HasIndex(name string) (sql string, values []interface{}, process func(result ztype.Maps) bool) {
	return b.args.driver.HasIndex(b.table, name)
}

func (b *TableBuilder) CreateIndex(name string, columns []string, indexType string) (sql string, values []interface{}) {
	return b.args.driver.CreateIndex(b.table, name, columns, indexType)
}

// GetColumn return table column
func (b *TableBuilder) GetColumn() (sql string, values []interface{}, process func(result ztype.Maps) ztype.Map) {
	return b.args.driver.GetColumn(b.table)
}

// RenameColumn rename table column
func (b *TableBuilder) RenameColumn(oldName, newName string) (sql string, values []interface{}) {
	return b.args.driver.RenameColumn(b.table, b.args.driver.Value().Quote(oldName), b.args.driver.Value().Quote(newName))
}

func (b *TableBuilder) AddColumn(name string, dataType schema.DataType, fieldOption ...func(*schema.Field)) (sql string, values []interface{}) {
	f := schema.NewField(name, dataType, fieldOption...)
	t := b.args.driver.DataTypeOf(f)
	return fmt.Sprintf("ALTER TABLE %s ADD %s %s", b.args.quoteField(b.table), b.args.driver.Value().Quote(f.Name), t), []interface{}{}
}

func (b *TableBuilder) DropColumn(name string) (sql string, values []interface{}) {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", b.table, b.args.driver.Value().Quote(name)), []interface{}{}
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

	// typ := b.Cond.driver.Value()
	columns := make([][]string, 0, l)

	for _, f := range b.columns {
		def := make([]string, 0, 3)
		def = append(def, b.args.driver.Value().Quote(f.Name))
		def = append(def, b.args.driver.DataTypeOf(f))

		columns = append(columns, def)
	}
	b.defines = append(b.defines, columns...)
	b.columns = b.columns[0:0:0]
}

// Build returns compiled CREATE TABLE string and Cond
func (b *CreateTableBuilder) Build() (sql string, values []interface{}, err error) {
	buf := zutil.GetBuff(256)
	defer zutil.PutBuff(buf)

	buf.WriteString(b.verb)

	if b.ifNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}

	buf.WriteRune(' ')
	buf.WriteString(b.tableName())

	b.buildColumns()
	if len(b.defines) > 0 {
		buf.WriteString(" (")

		defs := make([]string, 0, len(b.defines))
		for i := range b.defines {
			defs = append(defs, strings.Join(b.defines[i], " "))
		}
		buf.WriteString(strings.Join(defs, ", "))

		buf.WriteRune(')')
	}

	if len(b.options) > 0 {
		buf.WriteRune(' ')

		opts := make([]string, 0, len(b.options))
		for i := range b.options {
			opts = append(opts, strings.Join(b.options[i], " "))
		}
		buf.WriteString(strings.Join(opts, ", "))
	}

	sql, values = b.args.Compile(buf.String())

	if b.args.driver != nil && b.args.driver.Value() == driver.Doris {
		defer func() {
			if r := recover(); r != nil {
				// Log the error or handle it appropriately
				err = fmt.Errorf("failed to modify SQL for Doris driver: %v", r)
			}
		}()
		// 检查驱动是否支持 ModifyCreateTableSQL 方法
		driverType := zreflect.TypeOf(b.args.driver)
		m, ok := driverType.MethodByName("ModifyCreateTableSQL")
		if ok {
			driverValue := zreflect.ValueOf(b.args.driver)
			sqlValue := zreflect.ValueOf(sql)
			r := m.Func.Call([]reflect.Value{driverValue, sqlValue})
			if len(r) > 0 && r[0].IsValid() {
				sql = r[0].String()
			}
		}
	}
	return
}

// String returns the compiled INSERT string
func (b *CreateTableBuilder) String() string {
	s, _, _ := b.Build()
	return s
}

func (b *CreateTableBuilder) Safety() error {
	return nil
}
