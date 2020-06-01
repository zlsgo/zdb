package zdb

import (
	"database/sql"
	"sync"
)

type (
	Config struct {
		Driver string
		Dsn    string
		db     *sql.DB
	}
	Engine struct {
		vs          uint64
		pools       []*Config
		sessionPool sync.Pool
	}
)

func newEngine() *Engine {
	e := &Engine{}
	e.sessionPool.New = func() interface{} {
		return &Session{
			engine: e,
		}
	}
	return e
}

func New(driver, dsn string) (c *Engine, err error) {
	c = newEngine()
	err = addDB(c, &Config{
		Driver: driver,
		Dsn:    dsn,
	})
	return
}

func NewCluster(cfgs ...Config) (c *Engine, err error) {
	c = newEngine()
	for i := range cfgs {
		err = addDB(c, &cfgs[i])
		if err != nil {
			return nil, err
		}
	}

	return
}

func addDB(p *Engine, cfg *Config) error {
	p.pools = append(p.pools, cfg)
	d, err := cfg.Connect()
	if err != nil {
		return err
	}
	return d.Ping()
}

func (d *Config) Connect() (*sql.DB, error) {
	if d.db == nil {
		db, err := sql.Open(d.Driver, d.Dsn)
		if err != nil {
			return db, err
		}
		d.db = db
	}
	return d.db, nil
}
