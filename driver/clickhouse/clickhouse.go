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
	"github.com/sohaha/zlsgo/ztype"
	"github.com/sohaha/zlsgo/zutil"
	"github.com/zlsgo/zdb/driver"
)

var (
	_ driver.IfeConfig = &Config{}
	_ driver.Dialect   = &Config{}
)

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

	Timeout          time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	ConnTimeout      time.Duration
	AltHosts         []string
	ConnOpenStrategy string

	UseSSL     bool
	SkipVerify bool
	CertPath   string
	KeyPath    string
	CAPath     string

	ClusterName string
	Distributed bool

	Debug        bool
	TraceLogging bool

	BlockBufferSize  int
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	MaxExecutionTime time.Duration
}

func (c *Config) DB() *sql.DB {
	db, _ := c.MustDB()
	return db
}

func (c *Config) MustDB() (*sql.DB, error) {
	var err error
	if c.db == nil {
		c.db, err = sql.Open(c.GetDriver(), c.GetDsn())
		if err != nil {
			return nil, err
		}

		if c.MaxOpenConns > 0 {
			c.db.SetMaxOpenConns(c.MaxOpenConns)
		}
		if c.MaxIdleConns > 0 {
			c.db.SetMaxIdleConns(c.MaxIdleConns)
		}
		if c.ConnMaxLifetime > 0 {
			c.db.SetConnMaxLifetime(c.ConnMaxLifetime)
		}
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
	if c.TraceLogging {
		query.Add("trace_logging", "true")
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
	if c.ConnTimeout > 0 {
		query.Add("connection_timeout", c.ConnTimeout.String())
	}
	if c.MaxExecutionTime > 0 {
		query.Add("max_execution_time", c.MaxExecutionTime.String())
	}

	if c.MaxOpenConns > 0 {
		query.Add("max_open_conns", strconv.Itoa(c.MaxOpenConns))
	}
	if c.MaxIdleConns > 0 {
		query.Add("max_idle_conns", strconv.Itoa(c.MaxIdleConns))
	}
	if c.ConnMaxLifetime > 0 {
		query.Add("conn_max_lifetime", c.ConnMaxLifetime.String())
	}

	if len(c.AltHosts) > 0 {
		for i, host := range c.AltHosts {
			query.Add("alt_hosts["+strconv.Itoa(i)+"]", host)
		}
	}
	if c.ConnOpenStrategy != "" {
		query.Add("connection_open_strategy", c.ConnOpenStrategy)
	}
	if c.ClusterName != "" {
		query.Add("cluster", c.ClusterName)
	}
	if c.Distributed {
		query.Add("distributed", "true")
	}

	if c.UseSSL {
		query.Add("secure", "true")
		if c.SkipVerify {
			query.Add("skip_verify", "true")
		}
		if c.CertPath != "" {
			query.Add("tls_cert", c.CertPath)
		}
		if c.KeyPath != "" {
			query.Add("tls_key", c.KeyPath)
		}
		if c.CAPath != "" {
			query.Add("tls_ca", c.CAPath)
		}
	}

	if c.BlockBufferSize > 0 {
		query.Add("block_buffer_size", strconv.Itoa(c.BlockBufferSize))
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
