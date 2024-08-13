package mysql

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sohaha/zlsgo/zlog"
	"github.com/zlsgo/zdb/driver"

	"github.com/sohaha/zlsgo/zstring"
	"github.com/sohaha/zlsgo/zutil"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config databaseName configuration
type Config struct {
	db         *sql.DB
	dsn        string
	Host       string
	User       string
	Password   string
	DBName     string
	Charset    string
	Zone       string
	Parameters string
	driver.Typ
	Port int
}

func (c *Config) DB() *sql.DB {
	db, _ := c.MustDB()
	return db
}

func (c *Config) MustDB() (*sql.DB, error) {
	var err error
	if c.db == nil {
		c.db, err = sql.Open(c.GetDriver(), c.GetDsn())
	}
	return c.db, err
}

func (c *Config) SetDB(db *sql.DB) {
	c.db = db
}

func (c *Config) SetDsn(dsn string) {
	c.dsn = dsn
}

func (c *Config) GetDsn() string {
	if c.dsn != "" {
		return c.dsn
	}

	// loc := "Local"
	// timezone := ztime.GetTimeZone().String()
	// if timezone != "" {
	// 	loc = url.QueryEscape(timezone)
	// 	timezone = url.QueryEscape("'" + timezone + "'")
	// }

	loc := ""
	if c.Zone != "" {
		loc = "loc=" + url.QueryEscape(c.Zone) + "&"
	}

	charset := "utf8mb4"
	if c.Charset != "" {
		charset = zstring.TrimSpace(c.Charset)
	}
	if charset != "" {
		charset = "charset=" + charset + "&"
	}

	c.dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s%s%s",
		c.User, c.Password, zutil.IfVal(c.Host == "", "127.0.0.1", c.Host), zutil.IfVal(c.Port == 0, 3306, c.Port), c.DBName, charset, loc, zutil.IfVal(c.Parameters == "", "parseTime=true", c.Parameters))

	return c.dsn
}

func (c *Config) GetDriver() string {
	return "mysql"
}

func (c *Config) Value() driver.Typ {
	return driver.MySQL
}

func init() {
	_ = mysql.SetLogger(&log{})
}

type log struct {
}

func (l log) Print(v ...interface{}) {
	zlog.Debug(append([]interface{}{"[mysql] "}, v...)...)
}
