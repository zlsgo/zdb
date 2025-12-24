package zdb

import (
	"errors"
	"math"

	"github.com/sohaha/zlsgo/zarray"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver"
)

// BatchConfig batch config
type BatchConfig struct {
	MaxBatch int // max batch size
	InferIDs bool
}

// DefaultBatchConfig default batch config
var DefaultBatchConfig = BatchConfig{
	MaxBatch: 1000,
	InferIDs: false,
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
	return e.batchWriteWithConfig(func() *builder.InsertBuilder {
		return builder.Insert(table)
	}, cols, args, config, options...)
}

func (e *DB) batchIds(total int, lastID int64) []int64 {
	ids := make([]int64, total)
	if total == 0 {
		return ids
	}
	if e.driver.Value() == driver.MySQL {
		for i := 0; i < total; i++ {
			ids[i] = lastID + int64(i)
		}
		return ids
	}

	base := lastID - int64(total-1)
	for i := 0; i < total; i++ {
		ids[i] = base + int64(i)
	}
	return ids
}

func (e *DB) insertData(
	b *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	options ...string,
) (lastId int64, err error) {
	ids, err := e.insertDataReturningIDs(b, cols, args, options...)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, errInsertEmpty
	}
	return ids[len(ids)-1], nil
}

func (e *DB) insertDataReturningIDs(
	b *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	options ...string,
) ([]int64, error) {
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
		return nil, err
	}

	if len(values) == 0 {
		return nil, errInsertEmpty
	}

	driverValue := e.driver.Value()
	if driverValue == driver.PostgreSQL {
		idKey := e.idKey
		if idKey == "" {
			idKey = builder.IDKey
		}
		rows, err := e.QueryToMaps(sql+" RETURNING "+driverValue.Quote(idKey), values...)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, ErrNotFound
		}
		ids := make([]int64, len(rows))
		for i := range rows {
			ids[i] = rows[i].Get(idKey).Int64()
		}
		return ids, nil
	}

	result, err := e.Exec(sql, values...)
	if err != nil {
		return nil, err
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	if len(args) <= 1 {
		return []int64{lastID}, nil
	}

	return e.batchIds(len(args), lastID), nil
}

func (e *DB) batchWriteWithConfig(
	builderFn func() *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	config BatchConfig,
	options ...string,
) ([]int64, error) {
	if config.MaxBatch <= 0 {
		config.MaxBatch = DefaultBatchConfig.MaxBatch
	}
	if len(args) == 0 {
		return []int64{0}, errInsertEmpty
	}

	if e.driver.Value() == driver.PostgreSQL || config.InferIDs || len(args) == 1 {
		return e.batchWriteFast(builderFn, cols, args, config.MaxBatch, options...)
	}

	return e.batchWriteStrict(builderFn, cols, args, options...)
}

func (e *DB) batchWriteFast(
	builderFn func() *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	maxBatch int,
	options ...string,
) ([]int64, error) {
	datas := zarray.Chunk(args, maxBatch)

	if len(datas) <= 1 {
		ids, err := e.insertDataReturningIDs(builderFn(), cols, args, options...)
		if err != nil {
			return []int64{0}, err
		}
		return ids, nil
	}

	ids := make([]int64, 0, len(args))
	err := e.Transaction(func(tx *DB) error {
		for i := range datas {
			chunkIDs, err := tx.insertDataReturningIDs(builderFn(), cols, datas[i], options...)
			if err != nil {
				return err
			}
			ids = append(ids, chunkIDs...)
		}
		return nil
	})
	if err != nil {
		return []int64{0}, err
	}

	return ids, nil
}

func (e *DB) batchWriteStrict(
	builderFn func() *builder.InsertBuilder,
	cols []string,
	args [][]interface{},
	options ...string,
) ([]int64, error) {
	ids := make([]int64, 0, len(args))
	err := e.Transaction(func(tx *DB) error {
		for i := range args {
			rowIDs, err := tx.insertDataReturningIDs(builderFn(), cols, [][]interface{}{args[i]}, options...)
			if err != nil {
				return err
			}
			ids = append(ids, rowIDs...)
		}
		return nil
	})
	if err != nil {
		return []int64{0}, err
	}
	return ids, nil
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
	return e.BatchReplaceWithConfig(table, data, DefaultBatchConfig, options...)
}

func (e *DB) BatchReplaceWithConfig(
	table string,
	data interface{},
	config BatchConfig,
	options ...string,
) (lastId []int64, err error) {
	cols, args, err := parseMaps2(ztype.ToMaps(data))
	if err != nil {
		return []int64{0}, err
	}
	return e.batchWriteWithConfig(func() *builder.InsertBuilder {
		return builder.Replace(table)
	}, cols, args, config, options...)
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
	if len(resultMap) == 0 {
		return ztype.Map{}, ErrNotFound
	}

	return resultMap[0], nil
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
