package pgcache

import (
	"database/sql"
	"net/url"

	"github.com/bendersilver/pgcache/replica"
	"github.com/mattn/go-sqlite3"
	"github.com/tidwall/redcon"
)

// ListenAndServe -
func (pc *PgCache) ListenAndServe(addr string) error {
	go pc.startRedConn(addr)
	return <-pc.errChan
}

// New -
func New(pgURL string) (*PgCache, error) {
	if cache != nil {
		return cache, nil
	}
	u, err := url.Parse(pgURL)
	if err != nil {
		return nil, err
	}

	cache = new(PgCache)
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("application_name", "redispg_copy")
	u.RawQuery = param.Encode()
	cache.pgURL = u.String()

	sql.Register("sqlite3_custom", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {

			return nil
		},
	})
	cache.db, err = sql.Open("sqlite3_custom", ":memory:")
	if err != nil {
		return nil, err
	}
	cache.db.SetMaxOpenConns(1)
	cache.db.SetConnMaxIdleTime(0)
	cache.db.SetConnMaxLifetime(0)

	cache.msgChan = make(chan *replica.Row)
	cache.cancel, err = replica.New(pgURL, cache.msgChan)
	if err != nil {
		return nil, err
	}
	go cache.watchMessage()

	cache.errChan = make(chan error)
	go cache.startReplica()

	cache.tables = make(map[string]*dbTable)

	return cache, nil
}

// startReplica -
func (pc *PgCache) startReplica() {
	pc.errChan <- replica.Start()
}

// startReplica -
func (pc *PgCache) startRedConn(addr string) {
	pc.errChan <- redcon.ListenAndServe(addr, handler, accept, closed)
	pc.cancel()
}
