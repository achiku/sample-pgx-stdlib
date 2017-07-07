package main

import (
	"database/sql"
	"testing"
	"time"
)

func testNewDB(t *testing.T) *sql.DB {
	db, err := NewPqDB("dbname=pgtest user=pgtest sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testDBSetup(t *testing.T, db *sql.DB) func() {
	db.Exec(`drop table t1`)
	db.Exec(`drop table t2`)
	_, err := db.Exec(`
	create table t1 (
		id serial
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
	create table t2 (
		id serial
		,tm timestamp with time zone not null
		,dt date
	)
	`)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		db.Exec(`drop table t1`)
		db.Exec(`drop table t2`)
		db.Close()
	}
}

func TestPqPing(t *testing.T) {
	db := testNewDB(t)
	cleanup := testDBSetup(t, db)
	defer cleanup()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeWithTz(t *testing.T) {
	db := testNewDB(t)
	f := testDBSetup(t, db)
	defer f()

	now := time.Date(2017, 8, 18, 0, 0, 0, 0, time.Local)
	t.Logf("%s", now)
	var ID int64
	err := db.QueryRow(`insert into t2 (tm, dt) values ($1, $2) returning id`, now, now).Scan(&ID)
	if err != nil {
		t.Fatal(err)
	}

	var (
		tm time.Time
		dt time.Time
	)
	err = db.QueryRow(`select tm, dt from t2 where id = $1`, ID).Scan(&tm, &dt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tm=%s, tz=%s", tm, tm.Location())
	t.Logf("dt=%s, tz=%s", dt, dt.Location())

	a := time.Date(2017, 8, 18, 0, 0, 0, 0, time.UTC)
	t.Logf("a=%s, tz=%s", a, a.Location())

	b := time.Date(2017, 8, 18, 0, 0, 0, 0, &time.Location{})
	t.Logf("b=%s, tz=%s", b, b.Location())

	c := time.Date(2017, 8, 18, 0, 0, 0, 0, time.Local)
	t.Logf("c=%s, tz=%s", c, c.Location())

	if !a.Equal(b) {
		t.Errorf("a != b")
	}
	if c.Equal(b) || c.Equal(a) {
		t.Errorf("c == b || c == a")
	}
}
