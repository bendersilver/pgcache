package main

import (
	"os"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache"
)

func main() {
	pc, err := pgcache.New(os.Getenv("PG_URL"))
	if err != nil {
		glog.Fatal(err)
	}
	err = pc.AddTable(`pb.users`, true)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Debug("listen")
	err = pc.ListenAndServe(":6677")
	if err != nil {
		glog.Fatal(err)
	}
}
