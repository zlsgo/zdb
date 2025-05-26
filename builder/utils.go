package builder

import (
	"bytes"
	"database/sql"
	"strconv"
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

// Raw marks the expr as a raw value which will not be added to Cond.
func Raw(expr string) interface{} {
	return rawArgs{expr}
}

// Named creates a named argument
func Named(name string, arg interface{}) interface{} {
	return zutil.Named(name, arg)
}

func argsCompileHandler(args *BuildCond) zutil.ArgsOpt {
	return zutil.WithCompileHandler(func(buf *bytes.Buffer, values []interface{}, arg interface{}) ([]interface{}, bool) {
		switch a := arg.(type) {
		case Builder:
			s, args, _ := a.Build()
			buf.WriteString(s)

			if argsLen := len(args); argsLen > 0 {
				valuesLen := len(values)
				neededCap := valuesLen + argsLen
				if cap(values) < neededCap {
					newValues := make([]interface{}, valuesLen, neededCap)
					copy(newValues, values)
					values = newValues
				}

				values = append(values, args...)
			}
		case sql.NamedArg:
			buf.WriteRune('@')
			buf.WriteString(a.Name)
		case rawArgs:
			buf.WriteString(a.expr)
		default:
			driverType := args.driver.Value()

			switch driverType {
			case driver.PostgreSQL:
				buf.WriteRune('$')
				buf.WriteString(strconv.Itoa(len(values) + 1))
			case driver.MsSQL:
				buf.WriteString("@p")
				buf.WriteString(strconv.Itoa(len(values) + 1))
			default:
				buf.WriteRune('?')
			}

			values = append(values, arg)
		}

		return values, true
	})
}
