package pgcache

import (
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tidwall/redcon"
)

var db *sqlite.Conn

// Init -
func Init(pgURL string) error {
	err := replica.Run(pgURL)
	if err != nil {
		return err
	}
	db, err = sqlite.NewMemConn()
	if err != nil {
		return err
	}
	return nil
}

// Start -
func Start(addr string) error {
	return redcon.ListenAndServe(addr, handler, accept, closed)
}
