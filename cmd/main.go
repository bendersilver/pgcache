package main

import (
	"context"
	"net/url"
	"os"
	"runtime"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/joho/godotenv"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// var opt *replica.Options

	godotenv.Load()
	if err := os.RemoveAll(os.Getenv("SOCK")); err != nil {
		glog.Fatal(err)
	}

	u, err := url.Parse(os.Getenv("PG_URL"))
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
	db, err := sqlite.NewConn()
	if err != nil {
		glog.Fatal(err)
	}

	db.Close()
}

func init() {

}
