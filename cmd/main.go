package main

import (
	"fmt"
	"os"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache"
	"github.com/tidwall/redcon"
)

func main() {
	pc, err := pgcache.New(os.Getenv("PG_URL"))
	if err != nil {
		glog.Fatal(err)
	}
	err = pgcache.AddCommand(&echo)
	if err != nil {
		glog.Fatal(err)
	}

	err = pc.AddTable(`pb.users`, true)
	if err != nil {
		glog.Fatal(err)
	}
	// glog.Debug("listen")
	err = pc.ListenAndServe(":6677")
	if err != nil {
		glog.Fatal(err)
	}
}

var echo = pgcache.Command{
	Usage:    "ECHO args",
	Name:     "ECHO",
	Flags:    "admin write blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 2 {
			return fmt.Errorf("wrong number")
		}
		cmd.Args = cmd.Args[1:]
		conn.WriteArray(len(cmd.Args))
		for _, v := range cmd.Args {
			conn.WriteBulk(v)
		}
		return nil
	},
}
