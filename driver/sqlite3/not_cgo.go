//go:build !cgo
// +build !cgo

package sqlite3

import (
	_ "github.com/glebarez/go-sqlite"
)

func (c *Config) GetDriver() string {
	return "sqlite"
}
