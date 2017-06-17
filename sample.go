package pgxsample

import (
	"database/sql"

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
