package zdb

import (
	"database/sql"
	"sync"
	"time"
)

type (
	Config struct {
		driver string
		dsn    string
		db     *sql.DB
	}
	Engine struct {
		pools           []*Config
		session         *Session
		force           bool
		execMaxLifetime time.Duration
		isFixed         bool
	}
	JsonTime time.Time
)

type (
	IfeConfig interface {
		GetDsn() string
		GetDriver() string
		DB() *sql.DB
		MustDB() (*sql.DB, error)
		SetDB(*sql.DB)
	}
)

var engines sync.Map

func New(cfg IfeConfig, alias ...string) (c *Engine, err error) {
	c = &Engine{}
	err = add(c, cfg)
	if len(alias) > 0 {
		engines.Store(alias[0], c)
	}
	return
}

func NewCluster(cfgs []IfeConfig, alias ...string) (c *Engine, err error) {
	c = &Engine{}
	for i := range cfgs {
		err = add(c, cfgs[i])
		if err != nil {
			return nil, err
		}
	}
	if len(alias) > 0 {
		engines.Store(alias[0], c)
	}
	return
}

func DB(alias string) *Engine {
	db, _ := MustDB(alias)
	return db
}

func MustDB(alias string) (*Engine, error) {
	db, ok := engines.Load(alias)
	if ok {
		return db.(*Engine), nil
	}
	return nil, ErrDBNotExist
}

func add(p *Engine, c IfeConfig) (err error) {
	cfg := &Config{
		driver: c.GetDriver(),
		dsn:    c.GetDsn(),
	}
	cfg.db, err = c.MustDB()
	if err != nil {
		return
	}
	if err = cfg.db.Ping(); err == nil {
		p.pools = append(p.pools, cfg)
	}
	return err
}
