package zdb

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"

	"github.com/sohaha/zlsgo/zarray"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver"
)

// BatchConfig batch config
type BatchConfig struct {
	MaxBatch int // max batch size
	Workers  int // worker count
}

// DefaultBatchConfig default batch config
var DefaultBatchConfig = BatchConfig{
	MaxBatch: 1000,
	Workers:  5,
}

func (e *DB) Insert(table string, data interface{}, options ...string) (lastId int64, err error) {
	cols, args, err := parseMap(ztype.ToMap(data), nil)
	if err != nil {
		return 0, err
	}
	return e.insertData(builder.Insert(table), cols, args, options...)
}

func (e *DB) BatchInsert(
	table string,
	data interface{},
	options ...string,
) (lastId []int64, err error) {
	return e.BatchInsertWithConfig(table, data, DefaultBatchConfig, options...)
}

// BatchInsertWithConfig support custom config
func (e *DB) BatchInsertWithConfig(
	table string,
	data interface{},
	config BatchConfig,
	options ...string,
) (lastId []int64, err error) {
	cols, args, err := parseMaps2(ztype.ToMaps(data))
	if err != nil {
		return []int64{0}, err
	}

	datas := zarray.Chunk(args, config.MaxBatch)

	if len(datas) <= 1 {
		var id int64
		id, err = e.insertData(builder.Insert(table), cols, args, options...)
		if err != nil {
			return []int64{0}, err
		}
		return e.batchIds(args, id, err)
	}

	var finalId int64

	workers := config.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	pool := make(chan struct{}, workers)
	var wg sync.WaitGroup

	type result struct {
		index int
		id    int64
		err   error
	}
	resultChan := make(chan result, len(datas))

	err = e.Transaction(func(tx *DB) error {
		for i := range datas {
			pool <- struct{}{}
			wg.Add(1)

			go func(chunk [][]interface{}, index int) {
				defer func() {
					<-pool
					wg.Done()
				}()

				id, err := tx.insertData(builder.Insert(table), cols, chunk, options...)
				resultChan <- result{index: index, id: id, err: err}
			}(datas[i], i)
		}

		wg.Wait()
		close(resultChan)

		results := make([]result, 0, len(datas))
		var errs []error

		for r := range resultChan {
			results = append(results, r)
			if r.err != nil {
				errs = append(errs, fmt.Errorf("batch %d: %w", r.index, r.err))
			}
		}

		if len(errs) > 0 {
			if len(errs) == 1 {
				return errs[0]
			}
			return fmt.Errorf("batch insert failed with %d errors: %v", len(errs), errs)
		}

		sort.Slice(results, func(i, j int) bool {
			return results[i].index < results[j].index
		})
		finalId = results[len(results)-1].id

		return nil
	})
	if err != nil {
		return []int64{0}, err
	}

	return e.batchIds(args, finalId, err)
}

func (e *DB) batchIds(args [][]interface{}, id int64, err error) ([]int64, error) {
	var sub bool
	if e.driver.Value() == driver.MySQL {
		sub = true
	}
	ids := make([]int64, len(args))
	for i := 0; i < len(args); i++ {
		if sub {
			ids[i] = id + int64(i)
		} else {
			ids[i] = id - int64(i)
		}
	}
	return ids, err
}

func (e *DB) insertData(
	b *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	options ...string,
) (lastId int64, err error) {
	b.SetDriver(e.driver)

	if len(options) > 0 {
		b.Option(options...)
	}

	b.Cols(cols...)
	for i := range args {
		b.Values(args[i]...)
	}

	sql, values, err := b.Build()
	if err != nil {
		return 0, err
	}

	if len(values) == 0 {
		return 0, errInsertEmpty
	}

	isPostgreSQL := e.driver.Value() == driver.PostgreSQL
	if !isPostgreSQL {
		result, err := e.Exec(sql, values...)
		if err != nil {
			return 0, err
		}

		// if i, _ := result.RowsAffected(); i == 0 {
		// 	return 0, errInsertEmpty
		// }

		return result.LastInsertId()
	}

	result, err := e.QueryToMaps(sql+" RETURNING "+builder.IDKey, values...)
	if err != nil {
		return 0, err
	}

	return result[0].Get(builder.IDKey).Int64(), nil
}

func (e *DB) Replace(table string, data interface{}, options ...string) (lastId int64, err error) {
	cols, args, err := parseMap(ztype.ToMap(data), nil)
	if err != nil {
		return 0, err
	}
	return e.insertData(builder.Replace(table), cols, args, options...)
}

func (e *DB) BatchReplace(
	table string,
	data interface{},
	options ...string,
) (lastId []int64, err error) {
	cols, args, err := parseMaps2(ztype.ToMaps(data))
	if err != nil {
		return []int64{0}, err
	}

	datas := zarray.Chunk(args, DefaultBatchConfig.MaxBatch)

	if len(datas) <= 1 {
		var id int64
		id, err = e.insertData(builder.Replace(table), cols, args, options...)
		if err != nil {
			return []int64{0}, err
		}
		return e.batchIds(args, id, err)
	}

	var id int64
	var finalId int64

	err = e.Transaction(func(tx *DB) error {
		for i := range datas {
			id, err = tx.insertData(builder.Replace(table), cols, datas[i], options...)
			if err != nil {
				return err
			}
			if i == len(datas)-1 {
				finalId = id
			}
		}
		return nil
	})
	if err != nil {
		return []int64{0}, err
	}

	return e.batchIds(args, finalId, err)
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

func (e *DB) Pages(
	table string,
	page, pagesize int,
	fn ...func(b *builder.SelectBuilder) error,
) (ztype.Maps, Pages, error) {
	var b *builder.SelectBuilder
	if pagesize < 0 {
		pagesize = 1
	}
	resultMap, err := e.Find(table, func(bui *builder.SelectBuilder) error {
		if page > 0 && pagesize > 0 {
			bui.Limit(pagesize)
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

	sql, values, err := b.Select(b.As("count(*)", "total")).Limit(-1).OrderBy().Offset(-1).Build()
	if err != nil {
		return resultMap, Pages{}, err
	}

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
	if err := fn(b); err != nil {
		return 0, err
	}

	return parseExec(e, b)
}

func (e *DB) update(
	table string,
	cols []string,
	args [][]interface{},
	fn func(b *builder.UpdateBuilder) error,
	options ...string,
) (int64, error) {
	b := builder.Update(table).SetDriver(e.driver)
	if fn == nil {
		return 0, errors.New("update the condition cannot be empty")
	}

	// if len(cols) == 0 {
	// 	return 0, errors.New("update the data cannot be empty")
	// }
	if len(options) > 0 {
		b.Option(options...)
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

func (e *DB) Update(
	table string,
	data interface{},
	fn func(b *builder.UpdateBuilder) error,
) (int64, error) {
	cols, args, err := parseMap(ztype.ToMap(data), nil)
	if err != nil {
		return 0, err
	}
	return e.update(table, cols, args, fn)
}
