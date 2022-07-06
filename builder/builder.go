package builder

import (
	"github.com/zlsgo/zdb/driver"
)

// Builder is a general SQL builder
type Builder interface {
	Build() (sql string, values []interface{})
}

type compiledBuilder struct {
	args   *buildArgs
	format string
}

var _ Builder = new(compiledBuilder)

func (cb *compiledBuilder) Build() (sql string, values []interface{}) {
	return cb.args.Compile(cb.format)
}

func (cb *compiledBuilder) BuildWithFlavor(flavor driver.Typ, initialArg ...interface{}) (sql string, args []interface{}) {
	return cb.args.Compile(cb.format, initialArg...)
}

// Build creates a Builder from a format string
func Build(format string, arg ...interface{}) Builder {
	args := NewArgs(false)

	for _, a := range arg {
		args.Map(a)
	}

	return &compiledBuilder{
		args:   args,
		format: format,
	}
}

// BuildNamed creates a Builder from a format string
func BuildNamed(format string, named map[string]interface{}) Builder {
	args := NewArgs(true)

	for k := range named {
		args.Map(Named(k, named[k]))
	}

	return &compiledBuilder{
		args:   args,
		format: format,
	}
}
