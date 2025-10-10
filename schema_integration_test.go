package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/testdata"
	"github.com/zlsgo/zdb/builder"
)

// TestSchemaIntegration 通过数据库操作间接测试Schema功能
func TestSchemaIntegration(t *testing.T) {
	tt := zlsgo.NewTest(t)

	// 初始化数据库
	dbConf, clear, err := testdata.GetDbConf("schema_integration")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	// 测试不同数据类型的插入，间接验证Schema功能
	tableName := testdata.TestTable.TableName()

	// 初始化测试表
	err = testdata.InitTable(db)
	tt.NoError(err)

	// 插入各种类型的数据，验证Schema字段类型推断和转换
	testData := map[string]interface{}{
		"name":   "schema_integration_test", // string
		"age":    42,                        // int
		"is_ok":  1,                         // bool (as int)
	}

	insertedID, err := db.Insert(tableName, testData)
	tt.NoError(err)
	tt.Log("Schema集成测试数据插入成功, ID:", insertedID)

	// 查询数据验证类型转换是否正确
	result, err := db.Find(tableName, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "schema_integration_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(1, len(result))

	record := result[0]
	tt.Equal("schema_integration_test", record["name"])
	tt.Equal(int64(42), record["age"].(int64)) // SQLite返回int64
	tt.Log("Schema字段类型验证成功:", record)

	// 测试不同数据类型的批量插入
	batchData := []map[string]interface{}{
		{"name": "batch1", "age": 18, "is_ok": 1},
		{"name": "batch2", "age": 25, "is_ok": 0},
		{"name": "batch3", "age": 30, "is_ok": 1},
	}

	ids, err := db.BatchInsert(tableName, batchData)
	tt.NoError(err)
	tt.Log("Schema批量插入测试成功, IDs:", ids)

	// 验证批量插入的数据 - 使用IN条件查询
	batchResult, err := db.Find(tableName, func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.In("name", "batch1", "batch2", "batch3"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(3, len(batchResult))
	tt.Log("Schema批量数据验证成功，共", len(batchResult), "条记录")

	tt.Log("Schema集成测试通过")
}