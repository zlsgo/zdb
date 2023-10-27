package zdb

import (
	"database/sql"
	"sync"
	"time"

	"github.com/zlsgo/zdb/builder"
	"github.com/zlsgo/zdb/driver"
)

type (
	Config struct {
		driver driver.Dialect
		dsn    string
		db     *sql.DB
	}
	DB struct {
		pools   []*Config
		session *Session
		force   bool
		Debug   bool
		isFixed bool
		driver  driver.Dialect
	}
	JsonTime time.Time
)

var engines sync.Map

func New(cfg driver.IfeConfig, alias ...string) (e *DB, err error) {
	e = &DB{}
	err = e.add(cfg)
	if len(alias) > 0 {
		engines.Store(alias[0], e)
	}
	return
}

func NewCluster(cfgs []driver.IfeConfig, alias ...string) (e *DB, err error) {
	e = &DB{}
	for i := range cfgs {
		err = e.add(cfgs[i])
		if err != nil {
			return nil, err
		}
	}
	if len(alias) > 0 {
		engines.Store(alias[0], e)
	}
	return
}

func Instance(alias string) *DB {
	db, _ := MustInstance(alias)
	return db
}

func MustInstance(alias string) (*DB, error) {
	db, ok := engines.Load(alias)
	if ok {
		return db.(*DB), nil
	}

	return &DB{
		driver: builder.DefaultDriver,
	}, ErrDBNotExist
}

func (e *DB) add(c driver.IfeConfig) (err error) {
	cfg := &Config{
		dsn: c.GetDsn(),
	}
	cfg.db, err = c.MustDB()
	if err != nil {
		return
	}

	cfg.driver = e.toDialect(c)

	if err = cfg.db.Ping(); err == nil {
		e.pools = append(e.pools, cfg)
	}
	return err
}

func (e *DB) toDialect(c driver.IfeConfig) driver.Dialect {
	if dd, ok := c.(driver.Dialect); ok {
		e.driver = dd
	}
	return e.driver
}
