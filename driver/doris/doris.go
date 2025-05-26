//go:build doris
// +build doris

package doris

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sohaha/zlsgo/zstring"
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
	Charset    string
	Zone       string
	Parameters string
	driver.Typ
	Port int

	
	QueryTimeout  int 
	MaxRowsBuffer int 
	BatchSize     int 
	RetryTimes    int 

	
	Engine           string            
	DuplicateKeys    []string          
	DistributedBy    string            
	Buckets          int               
	ReplicationNum   int               
	InMemory         bool              
	StorageFormat    string            
	CustomProperties map[string]string 
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

	c.dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s%s",
		c.User, c.Password,
		ztype.ToString(zutil.IfVal(c.Host == "", "127.0.0.1", c.Host)),
		ztype.ToInt(zutil.IfVal(c.Port == 0, 9030, c.Port)), // Doris 默认端口为 9030
		c.DBName,
		charset,
		loc)

	params := ""
	if c.QueryTimeout > 0 {
		params += fmt.Sprintf("query_timeout=%d&", c.QueryTimeout)
	}
	if c.MaxRowsBuffer > 0 {
		params += fmt.Sprintf("max_rows_buffer=%d&", c.MaxRowsBuffer)
	}
	if c.BatchSize > 0 {
		params += fmt.Sprintf("batch_size=%d&", c.BatchSize)
	}
	if c.RetryTimes > 0 {
		params += fmt.Sprintf("retry_times=%d&", c.RetryTimes)
	}

	if c.Parameters != "" {
		params += c.Parameters
		if !strings.HasSuffix(params, "&") {
			params += "&"
		}
	}

	if params == "" {
		params = "parseTime=true"
	} else if !strings.Contains(params, "parseTime=") {
		params += "parseTime=true"
	}
	c.dsn += params

	return c.dsn
}

func (c *Config) GetDriver() string {
	return "mysql" 
}

func (c *Config) Value() driver.Typ {
	return driver.Doris
}
