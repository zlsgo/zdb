package zdb

import (
	"errors"
	"math"

	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver"
)

var IDKey = "id"

func (e *DB) InsertAny(table string, data interface{}) (lastId int64, err error) {
	return e.insert(table, data, parseAll)
}

func (e *DB) Insert(table string, data interface{}) (lastId int64, err error) {
	return e.insert(table, data, parseValues)
}

func (e *DB) insert(table string, data interface{}, parseFn func(data interface{}) (cols []string, args [][]interface{}, err error)) (lastId int64, err error) {
	b := builder.Insert(table).SetDriver(e.driver)

	cols, args, err := parseFn(data)
	if err != nil {
		return 0, err
	}

	if _, ok := data.(*QuoteData); ok {
		cols = e.QuoteCols(cols)
	}

	b.Cols(cols...)
	for i := range args {
		b.Values(args[i]...)
	}

	sql, values := b.Build()

	if len(values) == 0 {
		return 0, errInsertEmpty
	}
	isPostgreSQL := e.driver.Value() == driver.PostgreSQL
	if !isPostgreSQL {
		result, err := e.Exec(sql, values...)
		if err != nil {
			return 0, err
		}

		if i, _ := result.RowsAffected(); i == 0 {
			return 0, errInsertEmpty
		}

		return result.LastInsertId()
	}

	result, err := e.QueryToMaps(sql+" RETURNING "+IDKey, values...)
	if err != nil {
		return 0, err
	}

	return result[0].Get(IDKey).Int64(), nil
}

func (e *DB) FindOne(table string, fn func(b *builder.SelectBuilder) error) (ztype.Map, error) {
	resultMap, err := e.Find(table, func(sb *builder.SelectBuilder) error {
		sb.Limit(1)
		if fn == nil {
			return nil
		}
		return fn(sb)
	})

	if err != nil {
		return ztype.Map{}, err
	}

	return resultMap[0], err
}

type Pages struct {
	Total   uint `json:"total"`
	Count   uint `json:"count"`
	Curpage uint `json:"curpage"`
}

func (e *DB) Pages(table string, page, pagesize int, fn ...func(b *builder.SelectBuilder) error) (ztype.Maps, Pages, error) {
	var b *builder.SelectBuilder
	resultMap, err := e.Find(table, func(bui *builder.SelectBuilder) error {
		bui.Limit(pagesize)
		if page > 0 {
			bui.Offset((page - 1) * pagesize)
		}

		b = bui

		if len(fn) > 0 && fn[0] != nil {
			return fn[0](bui)
		}
		return nil
	})

	pages := Pages{
		Curpage: uint(page),
	}

	if err != nil {
		return resultMap, Pages{}, err
	}

	sql, values := b.Select(b.As("count(*)", "total")).Limit(-1).Offset(-1).Build()
	rows, err := e.Query(sql, values...)

	if err == nil {
		if m, _, err := ScanToMap(rows); err == nil {
			pages.Total = uint(m[0]["total"].(int64))
			pages.Count = uint(math.Ceil(float64(pages.Total) / float64(pagesize)))
		}
	}

	return resultMap, pages, err
}

func (e *DB) Find(table string, fn func(b *builder.SelectBuilder) error) (ztype.Maps, error) {
	b := builder.Query(table).SetDriver(e.driver)
	if fn != nil {
		if err := fn(b); err != nil {
			return []ztype.Map{}, err
		}
	}

	return parseQuery(e, b)
}

func (e *DB) Delete(table string, fn func(b *builder.DeleteBuilder) error) (int64, error) {
	b := builder.Delete(table).SetDriver(e.driver)
	if fn == nil {
		return 0, errors.New("delete the condition cannot be empty")
	}

	if err := fn(b); err != nil {
		return 0, err
	}

	return parseExec(e, b)
}

func (e *DB) update(table string, data interface{}, parseFn func(data interface{}) (cols []string, args [][]interface{}, err error), fn func(b *builder.UpdateBuilder) error) (int64, error) {
	b := builder.Update(table).SetDriver(e.driver)
	if fn == nil {
		return 0, errors.New("update the condition cannot be empty")
	}

	cols, args, err := parseFn(data)
	if err != nil && err != errNoData {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, errors.New("update the data cannot be empty")
	}

	if _, ok := data.(*QuoteData); ok {
		cols = e.QuoteCols(cols)
	}

	clen := len(cols)
	for i := 0; i < len(args); i++ {
		a := args[i]
		for c := 0; c < clen; c++ {
			col := cols[c]
			b.SetMore(b.Assign(col, a[c]))
		}
	}

	if err := fn(b); err != nil {
		return 0, err
	}

	return parseExec(e, b)
}

func (e *DB) Update(table string, data interface{}, fn func(b *builder.UpdateBuilder) error) (int64, error) {
	return e.update(table, data, parseAll, fn)
}

func (e *DB) UpdateMaps(table string, data interface{}, fn func(b *builder.UpdateBuilder) error) (int64, error) {
	return e.update(table, data, parseValues, fn)
}
