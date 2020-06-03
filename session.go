package zdb

import (
	"context"
	"database/sql"
)

type Session struct {
	tx     *sql.Tx
	config *Config
	// engine *Engine
	ctx context.Context
}

type TransactionFn func(s *Engine) error

func (s *Session) exec(query string, args ...interface{}) (sql.Result, error) {
	if s.tx != nil {
		return s.tx.Exec(query, args...)
	}
	return s.config.db.Exec(query, args...)
}

func (s *Session) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if s.tx != nil {
		return s.tx.ExecContext(ctx, query, args...)
	}
	return s.config.db.ExecContext(ctx, query, args...)
}

func (s *Session) query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.queryContext(s.ctx, query, args...)
}

func (s *Session) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return s.config.db.QueryContext(ctx, query, args...)
}

// transaction 使用事务
func (s *Session) transaction(run TransactionFn) error {
	db, err := s.config.db.Begin()
	if err != nil {
		return err
	}
	s.tx = db
	e := &Engine{
		session: s,
	}
	err = run(e)
	if err != nil {
		_ = db.Rollback()
		return err
	}
	return db.Commit()
}
