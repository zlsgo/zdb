package builder

import (
	"github.com/zlsgo/zdb/driver"
)

// Builder is a general SQL builder
type Builder interface {
	Build() (sql string, values []interface{}, err error)
}

type compiledBuilder struct {
	Cond   *BuildCond
	format string
}

var _ Builder = new(compiledBuilder)

func (cb *compiledBuilder) Build() (sql string, values []interface{}, err error) {
	sql, values = cb.Cond.Compile(cb.format)
	return
}

func (cb *compiledBuilder) BuildWithFlavor(flavor driver.Typ, initialArg ...interface{}) (sql string, args []interface{}) {
	return cb.Cond.Compile(cb.format, initialArg...)
}

// Build creates a Builder from a format string
func Build(format string, arg ...interface{}) Builder {
	args := newCond(DefaultDriver, false)

	for _, a := range arg {
		args.Var(a)
	}

	return &compiledBuilder{
		Cond:   args,
		format: format,
	}
}

// BuildNamed creates a Builder from a format string
func BuildNamed(format string, named map[string]interface{}) Builder {
	args := newCond(DefaultDriver, true)

	for k := range named {
		args.Var(Named(k, named[k]))
	}

	return &compiledBuilder{
		Cond:   args,
		format: format,
	}
}
