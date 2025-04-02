//go:build nosqlite
// +build nosqlite

package builder

import (
	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/driver/mysql"
)

// DefaultDriver is the default flavor for all builders
var DefaultDriver driver.Dialect = &mysql.Config{}
