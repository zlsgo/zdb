package zdb

import (
	"runtime"
	"testing"
	"time"

	"github.com/sohaha/zlsgo"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func newPoolLifecycleDB(t *testing.T) *DB {
	t.Helper()
	db, err := New(&sqlite3.Config{Memory: true, Parameters: "_pragma=busy_timeout(3000)"})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
}

func TestSwapPoolsReleasesOldConnections(t *testing.T) {
	tt := zlsgo.NewTest(t)

	handle := newPoolLifecycleDB(t)
	defer func() { _ = handle.Close() }()

	warm, err := handle.Query("SELECT 1")
	if err != nil {
		t.Fatalf("warm up: %v", err)
	}
	_ = warm.Close()

	for i := 0; i < 200; i++ {
		fresh := newPoolLifecycleDB(t)
		old := handle.SwapPools(fresh.TakePools())
		tt.Equal(true, len(old) > 0)

		rows, err := handle.Query("SELECT 1")
		if err != nil {
			t.Fatalf("query after swap #%d: %v", i, err)
		}
		_ = rows.Close()

		tt.NoError(ClosePools(old))
		for _, c := range old {
			if c == nil || c.db == nil {
				continue
			}
			if n := c.db.Stats().OpenConnections; n != 0 {
				t.Fatalf("swap #%d left %d open connections in retired pool", i, n)
			}
			if _, err := c.db.Query("SELECT 1"); err == nil {
				t.Fatalf("swap #%d retired pool still accepts queries", i)
			}
		}
	}
}

func TestSwapPoolsWaitsForInflightSession(t *testing.T) {
	tt := zlsgo.NewTest(t)

	handle := newPoolLifecycleDB(t)
	defer func() { _ = handle.Close() }()

	started := make(chan struct{})
	release := make(chan struct{})
	queryDone := make(chan error, 1)

	go func() {
		_ = handle.Source(func(tx *DB) error {
			close(started)
			<-release
			rows, err := tx.Query("SELECT 1")
			if err == nil {
				_ = rows.Close()
			}
			queryDone <- err
			return nil
		})
	}()

	<-started

	swapped := make(chan []*Config, 1)
	go func() {
		fresh := newPoolLifecycleDB(t)
		swapped <- handle.SwapPools(fresh.TakePools())
	}()

	select {
	case <-swapped:
		t.Fatal("SwapPools returned while a session was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	tt.NoError(<-queryDone)

	old := <-swapped
	tt.Equal(true, len(old) > 0)
	tt.NoError(ClosePools(old))
}

func TestSwapPoolsDoesNotLeakGoroutines(t *testing.T) {
	handle := newPoolLifecycleDB(t)
	warm, err := handle.Query("SELECT 1")
	if err != nil {
		t.Fatalf("warm up: %v", err)
	}
	_ = warm.Close()

	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		fresh := newPoolLifecycleDB(t)
		old := handle.SwapPools(fresh.TakePools())
		_ = ClosePools(old)
	}
	_ = handle.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines grew from %d to %d after swapping pools", before, runtime.NumGoroutine())
}
