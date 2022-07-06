package testdata

import (
	"errors"
	"time"

	"github.com/sohaha/zlsgo/zfile"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/driver/mssql"
	"github.com/zlsgo/zdb/driver/mysql"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

var dbType = zutil.Getenv("dbtype", "sqlite3")

type TestTableUser struct {
	ID   int          `zdb:"id"`
	Name string       `zdb:"name"`
	Is   bool         `zdb:"is_ok"`
	Date zdb.JsonTime `zdb:"date"`
	Time time.Time
}

func (*TestTableUser) TableName() string {
	return "user"
}

var TestTable *TestTableUser

func GetDbConf(id string) (dbConf driver.IfeConfig, clera func(), err error) {
	clera = func() {}
	switch dbType {
	case "sqlite3":
		conf := &sqlite3.Config{
			File: zfile.TmpPath("test") + "/" + id + ".db",
			// Memory: true,
		}
		dbConf = conf
		clera = func() {
			zfile.Rmdir(conf.File)
		}
	case "mysql":
		dbConf = &mysql.Config{
			Host:     "127.0.0.1",
			Port:     33061,
			User:     "root",
			Password: "666666",
			DBName:   "test" + id,
		}
	case "mssql":
		dbConf = &mssql.Config{
			Dsn: "",
		}
	default:
		return nil, nil, errors.New("未知数据库类型")
	}
	return
}

func InitTable(db *zdb.DB) error {
	var err error
	switch dbType {
	case "sqlite3":
		_, _ = db.Exec(`DROP TABLE ` + TestTable.TableName())
		_, err = db.Exec(`CREATE TABLE ` + TestTable.TableName() + ` (id INTEGER NOT NULL,is_ok INTEGER,name TEXT,age INTEGER,date DATE,time DATE,PRIMARY KEY (id));`)
	case "mysql":
		_, _ = db.Exec(`DROP TABLE ` + TestTable.TableName())
		_, err = db.Exec(`CREATE TABLE ` + TestTable.TableName() + ` (id int(0) NOT NULL AUTO_INCREMENT,is_ok int(1),name varchar(255) NULL,date datetime(0) NULL,PRIMARY KEY (id));`)
	case "mssql":
	}
	return err
}
