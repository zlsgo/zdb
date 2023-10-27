//go:build go1.18
// +build go1.18

package zdb

import (
	"github.com/sohaha/zlsgo/zreflect"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
)

func Find[T any](e *DB, table string, fn func(b *builder.SelectBuilder) error) ([]T, error) {
	data, err := e.Find(table, fn)
	if err != nil {
		return nil, err
	}
	var m []T

	v := zreflect.ValueOf(&m)
	return m, ztype.ValueConv(data, v)
}

func FindOne[T any](e *DB, table string, fn func(b *builder.SelectBuilder) error) (T, error) {
	var m T
	data, err := e.FindOne(table, fn)
	if err != nil {
		return m, err
	}

	v := zreflect.ValueOf(&m)
	return m, ztype.ValueConv(data, v)
}
