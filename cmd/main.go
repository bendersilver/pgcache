package main

import (
	"fmt"
	"os"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache"
	"github.com/bendersilver/pgcache/replica"
	"github.com/tidwall/redcon"
)

func main() {
	glog.Info(os.Getenv("PG_URL"))
	err := pgcache.Init(os.Getenv("PG_URL"))
	if err != nil {
		glog.Fatal(err)
	}
	err = replica.TableAdd(`pb.users`, true)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Fatal(pgcache.Start(":6677"))
	// err := replica.Run(os.Getenv("PG_URL"))
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	// err = replica.TableAdd(`pb.users`, true)
	// glog.Error(err)
	// select {}
	// pgcache.AddCommand(&echo)
	// err = pc.AddTable(`pb.users`, true)
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	// // glog.Debug("listen")
	// err = pc.ListenAndServe(":6677")
	// if err != nil {
	// 	glog.Fatal(err)
	// }
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
