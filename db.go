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
		db     *sql.DB
		dsn    string
	}
	DB struct {
		driver  driver.Dialect
		session *Session
		pools   []*Config
		force   bool
		Debug   bool
		isFixed bool
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

func (e *DB) GetSQLDB() (*sql.DB, error) {
	db, err := e.getSession(nil, true)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	return db.config.db, nil
}

func (e *DB) Close() error {
	var firstErr error
	for i := range e.pools {
		if err := e.pools[i].db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
