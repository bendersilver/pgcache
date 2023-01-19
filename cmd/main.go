package main

import (
	"context"
	"database/sql/driver"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"runtime"
	"sync"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pgx/v5/pgconn"
)

var db *sqlite.Conn

// Query -
type Query struct {
	SQL  string
	Args []driver.Value
}

// QueryResult -
type QueryResult struct {
	ColumnName []string
	Result     [][]any
}

// DB -
type DB struct {
	sync.Mutex
}

// Exec -
func (d *DB) Exec(args *Query, r *int) error {
	d.Lock()
	defer d.Unlock()
	return db.Exec(args.SQL, args.Args...)
}

// Query -
func (d *DB) Query(args *Query, r *QueryResult) error {
	d.Lock()
	defer d.Unlock()
	rows, err := db.Query(args.SQL, args.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	r.ColumnName = rows.Columns()

	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return err
		}
		r.Result = append(r.Result, v)
	}
	return rows.Err()
}

const sockAddr = "/tmp/pgcache.sock"

func main() {
	u, err := url.Parse("postgresql://tdot:qrODSbRu7R2byqQ@188.225.83.211:5432/tdot")
	if err != nil {
		glog.Fatal(err)
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("replication", "database")
	param.Add("application_name", "test_slot")
	u.RawQuery = param.Encode()
	conn, err := pgconn.Connect(context.Background(), u.String())
	if err != nil {
		glog.Fatal(err)
	}
	err = conn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS pb._replica_rule
		(
			sheme_name VARCHAR(50) NOT NULL,
			table_name VARCHAR(150) NOT NULL,
			initsql TEXT,
			cleansql TEXT,
			cleantimeout INT,
			CONSTRAINT cleansql_chek CHECK (cleansql NOTNULL AND GREATEST(cleantimeout, 0) > 0),
			PRIMARY KEY (sheme_name, table_name)
		);
	`).Close()
	glog.Fatal(err)
	runtime.GOMAXPROCS(runtime.NumCPU())
	replica.SetSlotName("temp_test_slot")
	err = replica.Run(os.Getenv("PG_URL"))
	if err != nil {
		glog.Fatal(err)
	}
	db, err = sqlite.NewConn()
	if err != nil {
		glog.Fatal(err)
	}

	listener, err := net.Listen("unix", sockAddr)
	if err != nil {
		glog.Fatal(err)
	}

	rpc.Register(new(DB))

	for {
		conn, err := listener.Accept()
		glog.Notice(conn.RemoteAddr())
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func init() {
	if err := os.RemoveAll(sockAddr); err != nil {
		glog.Fatal(err)
	}
}
