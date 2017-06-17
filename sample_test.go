package pgxsample

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

func testNewDB(t *testing.T) *sql.DB {
	cfg := &stdlib.DriverConfig{
		ConnConfig: pgx.ConnConfig{
			Database: "pgtest",
			User:     "pgtest",
		},
	}
	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestNewDB(t *testing.T) {
	cfg := &stdlib.DriverConfig{
		ConnConfig: pgx.ConnConfig{
			Database: "pgtest",
			User:     "pgtest",
		},
	}
	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	var tm time.Time
	if err := db.QueryRow(`select now()`).Scan(&tm); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", tm)
	t.Logf("OpenConn=%d", db.Stats().OpenConnections)
}

func TestSetConConfig(t *testing.T) {
	db := testNewDB(t)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)
	// db.SetConnMaxLifetime(time.Duration(1))

	var (
		tm time.Time
		wg sync.WaitGroup
	)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		x := i
		go func() {
			if err := db.QueryRow(`select now()`).Scan(&tm); err != nil {
				t.Fatal(err)
			}
			t.Logf("%d: %s", x, tm)
			t.Logf("OpenConn=%d", db.Stats().OpenConnections)
			wg.Done()
		}()
	}
	wg.Wait()
	t.Logf("Final OpenConn=%d", db.Stats().OpenConnections)
}

func TestAcquireConn(t *testing.T) {
	db := testNewDB(t)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)
	// db.SetConnMaxLifetime(time.Duration(1))

	var (
		tm time.Time
		wg sync.WaitGroup
	)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		x := i
		go func() {
			conn, err := stdlib.AcquireConn(db)
			if err != nil {
				t.Fatal(err)
			}
			defer stdlib.ReleaseConn(db, conn)

			if err := conn.QueryRow(`select now()`).Scan(&tm); err != nil {
				t.Fatal(err)
			}
			t.Logf("%d: %s", x, tm)
			t.Logf("OpenConn=%d", db.Stats().OpenConnections)
			wg.Done()
		}()
	}
	wg.Wait()
	t.Logf("Final OpenConn=%d", db.Stats().OpenConnections)
}
