package pgxsample

import (
	"database/sql"
	"time"

	"github.com/jackc/pgx/stdlib"
)

// NewDB new db
func NewDB(cfg *stdlib.DriverConfig) (*sql.DB, error) {
	stdlib.RegisterDriverConfig(cfg)
	db, err := sql.Open("pgx", cfg.ConnectionString(""))
	if err != nil {
		return nil, err
	}
	return db, nil
}

// IsAfter after
func IsAfter(t time.Time, u time.Time, l *time.Location) bool {
	tl := time.Date(
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l)
	ul := time.Date(
		u.Year(), u.Month(), u.Day(), u.Hour(), u.Minute(), u.Second(), u.Nanosecond(), l)
	return tl.After(ul)
}
