package zdb

import (
	"context"
	"database/sql"
)

type Session struct {
	v      uint64
	tx     *sql.Tx
	config *Config
	engine *Engine
	ctx    context.Context
}

func (s *Session) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer s.put()
	if s.tx != nil {
		return s.tx.Exec(query, args...)
	}
	return s.config.db.Exec(query, args...)
}

func (s *Session) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer s.put()
	if s.tx != nil {
		return s.tx.ExecContext(ctx, query, args...)
	}
	return s.config.db.ExecContext(ctx, query, args...)
}

func (s *Session) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(s.ctx, query, args...)
}

func (s *Session) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer s.put()
	return s.config.db.QueryContext(ctx, query, args...)
}

func (s *Session) Begin() error {
	db, err := s.config.db.Begin()
	s.tx = db
	return err
}

func (s *Session) Commit() error {
	defer s.put()
	return s.tx.Commit()
}

func (s *Session) Rollback() error {
	defer s.put()
	return s.tx.Rollback()
}

func (s *Session) put() {
	s.engine.sessionPool.Put(s)
}
