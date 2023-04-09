package builder

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var IDKey = "id"

// Escape replaces `$` with `$$` in ident
func Escape(ident string) string {
	return strings.Replace(ident, "$", "$$", -1)
}

// EscapeAll replaces `$` with `$$` in all strings of ident
func EscapeAll(ident ...string) []string {
	escaped := make([]string, 0, len(ident))

	for _, i := range ident {
		escaped = append(escaped, Escape(i))
	}

	return escaped
}

type rawArgs struct {
	expr string
}

// Raw marks the expr as a raw value which will not be added to args.
func Raw(expr string) interface{} {
	return rawArgs{expr}
}

// Named creates a named argument
func Named(name string, arg interface{}) interface{} {
	return zutil.Named(name, arg)
}

func argsCompileHandler(args *buildArgs) zutil.ArgsOpt {
	return zutil.WithCompileHandler(func(buf *bytes.Buffer, values []interface{}, arg interface{}) ([]interface{}, bool) {
		switch a := arg.(type) {
		case Builder:
			s, args := a.Build()
			buf.WriteString(s)
			if len(args) > 0 {
				values = append(values, args...)
			}
		case sql.NamedArg:
			buf.WriteRune('@')
			buf.WriteString(a.Name)
		case rawArgs:
			buf.WriteString(a.expr)
		default:
			switch args.driver.Value() {
			default:
				buf.WriteRune('?')
			case driver.PostgreSQL:
				_, _ = fmt.Fprintf(buf, "$%d", len(values)+1)
			case driver.MsSQL:
				_, _ = fmt.Fprintf(buf, "@p%d", len(values)+1)
			}

			values = append(values, arg)
		}

		return values, true
	})
}

type buildArgs struct {
	zutil.Args
	driver driver.Dialect
}

func NewArgs(onlyNamed bool) *buildArgs {
	args := &buildArgs{
		driver: DefaultDriver,
	}

	opts := []zutil.ArgsOpt{argsCompileHandler(args)}
	if onlyNamed {
		opts = append(opts, zutil.WithOnlyNamed())
	}

	a := zutil.NewArgs(opts...)
	args.Args = *a
	return args
}
