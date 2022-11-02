package pgcache

import (
	"database/sql"

	"github.com/bendersilver/pgcache/replica"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tidwall/redcon"
)

var db *sql.DB

// Init -
func Init(pgURL string) error {
	err := replica.Run(pgURL)
	if err != nil {
		return err
	}
	db, err = sql.Open("sqlite3", "file:redispg?mode=memory&cache=shared&_auto_vacuum=1")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	return nil
}

// Start -
func Start(addr string) error {
	return redcon.ListenAndServe(addr, handler, accept, closed)
}
