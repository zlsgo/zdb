package zdb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/zlsgo/zdb/driver"
	"github.com/zlsgo/zdb/driver/sqlite3"
)

func newSQLiteTestDB(t *testing.T, name string) *DB {
	t.Helper()

	cfg := &sqlite3.Config{
		File: filepath.Join(t.TempDir(), name+".db"),
	}
	db, err := New(cfg)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	return db
}

func TestTransactionPreservesDBState(t *testing.T) {
	db := newSQLiteTestDB(t, "tx_state")
	db.Debug = true
	db.SetIDKey("custom_id")

	if _, err := db.Exec(`CREATE TABLE tx_state (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := db.Transaction(func(tx *DB) error {
		if tx.session == nil {
			t.Fatal("expected transaction session")
		}
		if tx.idKey != "custom_id" {
			t.Fatalf("unexpected idKey: %q", tx.idKey)
		}
		if !tx.Debug {
			t.Fatal("expected debug flag to propagate")
		}

		if err := tx.Source(func(source *DB) error {
			if source.session == nil {
				t.Fatal("expected source session")
			}
			if source.idKey != "custom_id" {
				t.Fatalf("unexpected source idKey: %q", source.idKey)
			}
			if !source.Debug {
				t.Fatal("expected source debug flag to propagate")
			}
			if _, err := source.Exec(`INSERT INTO tx_state(name) VALUES(?)`, "from_source"); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		if _, err := tx.Exec(`INSERT INTO tx_state(name) VALUES(?)`, "after_source"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("transaction failed: %v", err)
	}

	rows, err := db.QueryToMaps(`SELECT name FROM tx_state ORDER BY id`)
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Get("name").String() != "from_source" {
		t.Fatalf("unexpected first row: %v", rows[0])
	}
	if rows[1].Get("name").String() != "after_source" {
		t.Fatalf("unexpected second row: %v", rows[1])
	}
}

func TestTransactionUsesContextForBegin(t *testing.T) {
	db := newSQLiteTestDB(t, "tx_ctx")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := db.Transaction(func(tx *DB) error {
		return nil
	}, ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSessionBoundFlowsPreserveDBState(t *testing.T) {
	db := newSQLiteTestDB(t, "session_bound")
	db.Debug = true
	db.SetIDKey("custom_id")

	check := func(label string, child *DB) error {
		if child.session == nil {
			t.Fatalf("%s: expected session", label)
		}
		if child.idKey != "custom_id" {
			t.Fatalf("%s: unexpected idKey %q", label, child.idKey)
		}
		if !child.Debug {
			t.Fatalf("%s: expected debug flag", label)
		}
		return nil
	}

	if err := db.Source(func(source *DB) error {
		return check("source", source)
	}); err != nil {
		t.Fatalf("source failed: %v", err)
	}

	if err := db.Replica(func(replica *DB) error {
		return check("replica", replica)
	}); err != nil {
		t.Fatalf("replica failed: %v", err)
	}

	if err := db.Migration(func(migration *DB, _ driver.Dialect) error {
		return check("migration", migration)
	}); err != nil {
		t.Fatalf("migration failed: %v", err)
	}
}
