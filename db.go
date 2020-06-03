package zdb

import (
	"database/sql"
)

type (
	Config struct {
		driver string
		dsn    string
		db     *sql.DB
	}
	Engine struct {
		pools   []*Config
		session *Session
		force   bool
	}
)

type (
	IfeConfig interface {
		GetDsn() string
		GetDriver() string
		GetDB() *sql.DB
		GetDBE() (*sql.DB, error)
		SetDB(*sql.DB)
	}
)

func New(cfg IfeConfig) (c *Engine, err error) {
	c = &Engine{}
	err = addDB(c, cfg)
	return
}

func NewCluster(cfgs ...IfeConfig) (c *Engine, err error) {
	c = &Engine{}
	for i := range cfgs {
		err = addDB(c, cfgs[i])
		if err != nil {
			return nil, err
		}
	}

	return
}

func addDB(p *Engine, c IfeConfig) error {
	var err error
	cfg := &Config{
		driver: c.GetDriver(),
		dsn:    c.GetDsn(),
	}
	db := c.GetDB()
	if db == nil {
		db, err = sql.Open(cfg.driver, cfg.dsn)
		if err != nil {
			return err
		}
		cfg.db = db
		err = db.Ping()
		if err != nil {
			return err
		}
		c.SetDB(db)
	} else {
		cfg.db = db
	}
	p.pools = append(p.pools, cfg)
	return err
}
