package builder

import (
	"github.com/zlsgo/zdb/driver/sqlite3"
)

var (
	// DefaultDriver is the default flavor for all builders
	DefaultDriver = &sqlite3.Config{}
)
