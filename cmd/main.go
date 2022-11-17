package main

import (
	"database/sql/driver"
	"net"
	"net/rpc"
	"os"
	"runtime"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
)

var db *sqlite.Conn

// DB -
type DB struct{}

// Exec -
func (d *DB) Exec(args *Query, r *int) error {
	return db.Exec(args.SQL, args.Args...)
}

// Query -
func (d *DB) Query(args *Query, r *[][]any) error {
	rows, err := db.Query(args.SQL, args.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return err
		}
		*r = append(*r, v)
	}
	return rows.Err()
}

// Query -
type Query struct {
	SQL  string
	Args []driver.Value
}

const sockAddr = "/tmp/pgcache.sock"

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
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
