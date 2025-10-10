package zdb_test

import (
	"testing"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/testdata"
	"github.com/zlsgo/zdb/builder"
)

// TestCRUDDelete 测试数据库DELETE操作
func TestCRUDDelete(t *testing.T) {
	tt := zlsgo.NewTest(t)

	// 初始化数据库
	dbConf, clear, err := testdata.GetDbConf("crud_delete")
	tt.NoError(err)
	defer clear()

	db, err := zdb.New(dbConf)
	tt.NoError(err)

	// 初始化测试表
	err = testdata.InitTable(db)
	tt.NoError(err)

	// 插入测试数据
	insertedID, err := db.Insert(testdata.TestTable.TableName(), map[string]interface{}{
		"name":  "delete_test",
		"age":   25,
		"is_ok": 1,
	})
	tt.NoError(err)
	tt.Log("插入测试数据, ID:", insertedID)

	// 再插入一条数据确保有多条记录
	insertedID2, err := db.Insert(testdata.TestTable.TableName(), map[string]interface{}{
		"name":  "keep_test",
		"age":   30,
		"is_ok": 1,
	})
	tt.NoError(err)
	tt.Log("插入第二条测试数据, ID:", insertedID2)

	// 测试: 基本DELETE操作
	deletedCount, err := db.Delete(testdata.TestTable.TableName(), func(b *builder.DeleteBuilder) error {
		b.Where(b.Cond.EQ("name", "delete_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(int64(1), deletedCount)
	tt.Log("删除了", deletedCount, "条记录")

	// 验证数据确实被删除
	result, err := db.Find(testdata.TestTable.TableName(), func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "delete_test"))
		return nil
	})
	// 删除后查询可能返回"找不到记录"错误，这是正常的
	if err != nil {
		tt.Log("查询已删除记录返回错误（正常）:", err)
	}
	tt.Equal(0, len(result))
	tt.Log("被删除的记录不再存在")

	// 验证其他数据未被删除
	result, err = db.Find(testdata.TestTable.TableName(), func(b *builder.SelectBuilder) error {
		b.Where(b.Cond.EQ("name", "keep_test"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(1, len(result))
	tt.Log("其他记录保持不变")

	// 测试: 无WHERE条件的删除应该报错（安全检查）
	_, err = db.Delete(testdata.TestTable.TableName(), func(b *builder.DeleteBuilder) error {
		// 没有调用Where方法
		return nil
	})
	tt.NotNil(err)
	tt.Log("无WHERE条件删除报错:", err)

	// 测试: 删除不存在的记录
	deletedCount, err = db.Delete(testdata.TestTable.TableName(), func(b *builder.DeleteBuilder) error {
		b.Where(b.Cond.EQ("name", "not_exist"))
		return nil
	})
	tt.NoError(err)
	tt.Equal(int64(0), deletedCount)
	tt.Log("删除不存在记录返回0")

	tt.Log("CRUD DELETE操作测试全部通过")
}