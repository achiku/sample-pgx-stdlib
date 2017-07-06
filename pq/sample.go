package main

import (
	"database/sql"

	_ "github.com/lib/pq" // postgres
)

// NewPqDB new pq database
func NewPqDB(conn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
