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
	db.SetConnMaxLifetime(time.Duration(10 * time.Second))

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
	// time.Sleep(10 * time.Second)
	t.Logf("Final OpenConn=%d", db.Stats().OpenConnections)
}

func TestAcquireConnWithMaxLifetime(t *testing.T) {
	db := testNewDB(t)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)
	db.SetConnMaxLifetime(time.Duration(10 * time.Second))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		x := i
		go func() {
			conn, err := stdlib.AcquireConn(db)
			if err != nil {
				t.Fatal(err)
			}
			defer stdlib.ReleaseConn(db, conn)

			var pid1, pid2, pid3, pid4 int64
			if err := conn.QueryRow(`select pg_backend_pid()`).Scan(&pid1); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(`select pg_backend_pid()`).Scan(&pid2); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(`select pg_backend_pid()`).Scan(&pid3); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(`select pg_backend_pid()`).Scan(&pid4); err != nil {
				t.Fatal(err)
			}
			t.Logf("%d: %d=%d=%d=%d", x, pid1, pid2, pid3, pid4)
			t.Logf("OpenConn=%d", db.Stats().OpenConnections)
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(3 * time.Second)
	t.Logf("Final OpenConn=%d", db.Stats().OpenConnections)
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

func TestTx(t *testing.T) {
	db := testNewDB(t)
	f := testDBSetup(t, db)
	defer f()

	conn, err := stdlib.AcquireConn(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`insert into t1 (id) values (10)`); err != nil {
		t.Fatal(err)
	}

	var cnt1 int
	if err := tx.QueryRow(`select count(*) from t1`).Scan(&cnt1); err != nil {
		t.Fatal(err)
	}
	t.Logf("TxCount=%d", cnt1)

	var cnt2 int
	if err := conn.QueryRow(`select count(*) from t1`).Scan(&cnt2); err != nil {
		t.Fatal(err)
	}
	t.Logf("ConnCount=%d", cnt2)

	var cnt3 int
	if err := db.QueryRow(`select count(*) from t1`).Scan(&cnt3); err != nil {
		t.Fatal(err)
	}
	t.Logf("DBCount=%d", cnt3)
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

func TestCreateTimeWithDateUTC(t *testing.T) {
	db := testNewDB(t)
	f := testDBSetup(t, db)
	defer f()
	// testDBSetup(t, db)

	cases := []time.Time{
		time.Now(),
		time.Date(2017, 8, 18, 23, 59, 59, 0, time.Local),
		time.Date(2017, 8, 18, 0, 0, 0, 0, time.Local),
	}
	for _, c := range cases {
		var ID int64
		err := db.QueryRow(`insert into t2 (tm, dt) values ($1, $2) returning id`, c, c).Scan(&ID)
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
		//  select * from t2;
		//  id |              tm               |     dt
		// ----+-------------------------------+------------
		//   1 | 2017-08-02 09:46:59.712814+00 | 2017-08-02
		//   2 | 2017-08-18 14:59:59+00        | 2017-08-18
		//   3 | 2017-08-17 15:00:00+00        | 2017-08-18
	}
}

func TestCompareTimeWithDateUTC(t *testing.T) {
	db := testNewDB(t)
	f := testDBSetup(t, db)
	defer f()
	// testDBSetup(t, db)

	cases := []struct {
		DBVal time.Time
		GoVal time.Time
		Equal bool
	}{
		{
			DBVal: time.Date(2017, 8, 18, 23, 59, 59, 0, time.Local),
			GoVal: time.Date(2017, 8, 18, 0, 0, 0, 0, time.UTC),
			Equal: true,
		},
		{
			DBVal: time.Date(2017, 8, 18, 23, 59, 59, 0, time.Local),
			GoVal: time.Date(2017, 8, 18, 1, 0, 0, 0, time.Local),
			Equal: false,
		},
	}
	for _, c := range cases {
		var ID int64
		err := db.QueryRow(
			`insert into t2 (tm, dt) values ($1, $2) returning id`, c.DBVal, c.DBVal).Scan(&ID)
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
		JST := time.FixedZone("JST", 9*60*60)
		t.Logf("%s", dt.In(JST))
		t.Logf("%s", c.GoVal.Sub(dt))
		t.Logf("%s", c.GoVal.Sub(dt.In(JST)))
		t.Logf("%t", IsAfter(c.GoVal, dt, JST))
	}
}
