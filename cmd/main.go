package main

import (
	"os"
	"runtime"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/joho/godotenv"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	godotenv.Load()
	if err := os.RemoveAll(os.Getenv("SOCK")); err != nil {
		glog.Fatal(err)
	}

	c, err := replica.NewConn(&replica.Options{
		PgURL:     os.Getenv("PG_URL"),
		SlotName:  os.Getenv("SLOT"),
		TableName: os.Getenv("TABLE"),
	})

	if err != nil {
		glog.Fatal(err)
	}

	glog.Error(c.Close())
}

func init() {

}
