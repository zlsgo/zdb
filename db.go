package zdb

import (
	"database/sql"
	"sync"
	"sync/atomic"
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
		cur     atomic.Pointer[poolGen]
		Debug   bool
		idKey   string
	}

	poolGen struct {
		pools    []*Config
		inflight atomic.Int64
	}
	JsonTime time.Time
)

var engines sync.Map

func New(cfg driver.IfeConfig, alias ...string) (e *DB, err error) {
	e = &DB{
		idKey: builder.IDKey,
	}
	err = e.add(cfg)
	if len(alias) > 0 {
		engines.Store(alias[0], e)
	}
	return
}

func NewCluster(cfgs []driver.IfeConfig, alias ...string) (e *DB, err error) {
	e = &DB{
		idKey: builder.IDKey,
	}
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
		idKey:  builder.IDKey,
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
		var pools []*Config
		if g := e.cur.Load(); g != nil {
			pools = append(pools, g.pools...)
		}
		pools = append(pools, cfg)
		e.cur.Store(&poolGen{pools: pools})
	}
	return err
}

func (e *DB) toDialect(c driver.IfeConfig) driver.Dialect {
	if dd, ok := c.(driver.Dialect); ok {
		e.driver = dd
	}
	return e.driver
}

func (e *DB) GetSQLDB(isMaster ...bool) (*sql.DB, error) {
	master := true
	if len(isMaster) > 0 {
		master = isMaster[0]
	}
	db, err := e.getSession(nil, master)
	if err != nil {
		return nil, err
	}
	defer e.putSessionPool(db, false)

	return db.config.db, nil
}

func (e *DB) poolSlice() []*Config {
	g := e.cur.Load()
	if g == nil {
		return nil
	}
	return g.pools
}

func (e *DB) curGen() *poolGen {
	return e.cur.Load()
}

func (e *DB) SwapPools(next []*Config) (old []*Config) {
	oldGen := e.cur.Swap(&poolGen{pools: next})
	if oldGen == nil {
		return nil
	}
	oldGen.drain(swapDrainTimeout)
	return oldGen.pools
}

const swapDrainTimeout = 5 * time.Second

func (g *poolGen) drain(timeout time.Duration) {
	if g.inflight.Load() == 0 {
		return
	}
	deadline := time.Now().Add(timeout)
	for g.inflight.Load() > 0 {
		if time.Now().After(deadline) {
			log.Warnf("pool swap drain timeout: %d session(s) still in flight, old connections will be released once they finish", g.inflight.Load())
			return
		}

		time.Sleep(200 * time.Microsecond)
	}
}

func (e *DB) TakePools() []*Config {
	return e.SwapPools(nil)
}

func ClosePools(pools []*Config) error {
	var firstErr error
	for _, c := range pools {
		if c == nil || c.db == nil {
			continue
		}
		if err := c.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *DB) Close() error {
	return ClosePools(e.poolSlice())
}

func (e *DB) SetIDKey(key string) {
	e.idKey = key
}

func (e *DB) withSession(s *Session) *DB {
	n := &DB{
		driver:  e.driver,
		session: s,
		Debug:   e.Debug,
		idKey:   e.idKey,
	}
	if g := e.cur.Load(); g != nil {
		n.cur.Store(g)
	}
	if s != nil && s.config != nil {
		n.driver = s.config.driver
	}
	return n
}
