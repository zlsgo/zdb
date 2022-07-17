//go:build go1.18
// +build go1.18

package zdb

import (
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
)

func Find[T any](e *DB, table string, fn func(b *builder.SelectBuilder) error) (*T, error) {
	data, err := e.Find(table, fn)
	if err != nil {
		return nil, err
	}
	var m T
	err = scan([]ztype.Map{data}, &m)

	return &m, err
}
