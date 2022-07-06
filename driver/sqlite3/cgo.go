//go:build cgo
// +build cgo

package sqlite3

import (
	_ "github.com/mattn/go-sqlite3"
)

func (c *Config) GetDriver() string {
	return "sqlite3"
}
