//go:build clickhouse
// +build clickhouse

package clickhouse

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sohaha/zlsgo/zlog"
	"github.com/sohaha/zlsgo/ztype"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

// Config ClickHouse configuration
type Config struct {
	db         *sql.DB
	dsn        string
	Host       string
	User       string
	Password   string
	DBName     string
	Compress   bool
	Parameters string
	driver.Typ
	Port int
	// ClickHouse特有配置
	Timeout          time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	AltHosts         []string
	Debug            bool
	UseSSL           bool
	SkipVerify       bool
	ClusterName      string
	ConnOpenStrategy string
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

	host := ztype.ToString(zutil.IfVal(c.Host == "", "127.0.0.1", c.Host))
	port := ztype.ToInt(zutil.IfVal(c.Port == 0, 9000, c.Port))

	query := url.Values{}

	if c.Compress {
		query.Add("compress", "true")
	}

	if c.Debug {
		query.Add("debug", "true")
	}

	if c.Timeout > 0 {
		query.Add("timeout", c.Timeout.String())
	}

	if c.ReadTimeout > 0 {
		query.Add("read_timeout", c.ReadTimeout.String())
	}

	if c.WriteTimeout > 0 {
		query.Add("write_timeout", c.WriteTimeout.String())
	}

	if len(c.AltHosts) > 0 {
		for i, host := range c.AltHosts {
			query.Add("alt_hosts["+strconv.Itoa(i)+"]", host)
		}
	}

	if c.UseSSL {
		query.Add("secure", "true")
		if c.SkipVerify {
			query.Add("skip_verify", "true")
		}
	}

	if c.ClusterName != "" {
		query.Add("cluster", c.ClusterName)
	}

	if c.ConnOpenStrategy != "" {
		query.Add("connection_open_strategy", c.ConnOpenStrategy)
	}

	if c.Parameters != "" {
		query.Add("custom", c.Parameters)
	}

	c.dsn = fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?%s",
		c.User, c.Password, host, port, c.DBName, query.Encode())

	return c.dsn
}

func (c *Config) GetDriver() string {
	return "clickhouse"
}

func (c *Config) Value() driver.Typ {
	return driver.ClickHouse
}

func init() {
	// 配置日志
	zlog.Debug("[clickhouse] Driver initialized")
}

// 日志结构体
type log struct{}

func (l log) Print(v ...interface{}) {
	zlog.Debug(append([]interface{}{"[clickhouse] "}, v...)...)
}
