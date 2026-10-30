package zdb_test

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func memoryConf() *sqlite3.Config {
	return &sqlite3.Config{Memory: true, Parameters: "_pragma=busy_timeout(3000)"}
}

func TestDB_SwapPoolsUnderLoad(t *testing.T) {
	tt := zlsgo.NewTest(t)

	handle, err := zdb.New(memoryConf())
	tt.NoError(err)
	defer func() { _ = handle.Close() }()

	var (
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		closed   atomic.Int64
		firstErr atomic.Value
	)

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rows, err := handle.Query("SELECT 1")
				if err != nil {
					if strings.Contains(err.Error(), "database is closed") {
						closed.Add(1)
						if firstErr.Load() == nil {
							firstErr.Store(err.Error())
						}
					}
					continue
				}
				_ = rows.Close()
			}
		}()
	}

	for i := 0; i < 40; i++ {
		fresh, err := zdb.New(memoryConf())
		tt.NoError(err)

		old := handle.SwapPools(fresh.TakePools())
		tt.NoError(zdb.ClosePools(old))

		time.Sleep(time.Millisecond)
	}

	close(stop)
	wg.Wait()

	if n := closed.Load(); n > 0 {
		t.Fatalf("swapping pools under load produced %d `database is closed` errors, first: %v", n, firstErr.Load())
	}
}

func TestDB_CloseRejectsFurtherQueries(t *testing.T) {
	tt := zlsgo.NewTest(t)

	db, err := zdb.New(memoryConf())
	tt.NoError(err)
	tt.NoError(db.Close())

	_, err = db.Query("SELECT 1")
	tt.Equal(true, err != nil)
	tt.Equal(true, strings.Contains(err.Error(), "database is closed"))
}
