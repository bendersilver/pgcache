package pgcache

import (
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/go-redis/redis/v9"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tidwall/redcon"
)

var db *sqlite.Conn
var rdb *redis.Client

// Init -
func Init(pgURL string, redisOpt *redis.Options) error {
	err := replica.Run(pgURL)
	if err != nil {
		return err
	}
	db, err = sqlite.NewMemConn()
	if err != nil {
		return err
	}
	if redisOpt != nil {
		rdb = redis.NewClient(redisOpt)
	}

	return nil
}

// Start -
func Start(addr string) error {
	return redcon.ListenAndServe(addr, handler, accept, closed)
}
