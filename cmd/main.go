package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"runtime"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/cli"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	os.RemoveAll(os.Getenv("PGC_SOCK"))

	rc, err := replica.NewConn(&replica.Options{
		PgURL:     os.Getenv("PG_URL"),
		SlotName:  os.Getenv("SLOT"),
		TableName: os.Getenv("TABLE"),
	})
	if err != nil {
		glog.Fatal(err)
	}

	defer rc.Close()

	db, err := sqlite.NewConn()
	if err != nil {
		rc.Close()
		return
	}
	defer db.Close()

	svr := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rsp, err := cli.SvrJSON(r, db)
			if err != nil {
				rsp = &cli.Response{
					Error:  err,
					Status: 500,
				}
			}
			json.NewEncoder(w).Encode(rsp)
		}),
	}

	var e chan error
	go func() {
		ux, err := net.Listen("unix", os.Getenv("PGC_SOCK"))
		if err != nil {
			e <- err
		}
		e <- svr.Serve(ux)
	}()

	go func() {
		e <- rc.Run()
	}()
	glog.Error(<-e)
}
