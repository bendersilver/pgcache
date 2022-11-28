package main

import (
	"database/sql/driver"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"sync"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	replica.SetSlotName("temp_test_slot")
	err := replica.Run(os.Getenv("PG_URL"))
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
