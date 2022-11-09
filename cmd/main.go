package main

import (
	"fmt"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache"
	"github.com/bendersilver/pgcache/replica"
	"github.com/go-redis/redis/v9"
	"github.com/tidwall/redcon"
)

func main() {
	// glog.Notice(os.Getenv("PG_URL"))
	err := pgcache.Init("postgresql://7keys:TfHaOw92QMaS9lN15Q@7keys-develop/7keys", &redis.Options{})
	if err != nil {
		glog.Fatal(err)
	}
	glog.Notice("comm")
	// err = replica.TableAdd(`pb.temp`, true)
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	err = replica.TableAdd(&replica.AddOptions{
		TableName: "pb.tariff",
		Init:      true,
	})
	err = replica.TableAdd(&replica.AddOptions{
		TableName: "pb.temp",
		Init:      true,
	})
	if err != nil {
		glog.Fatal(err)
	}
	glog.Notice("Add table")
	// err = replica.TableAdd(`pb.users_base`, true)
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	// err = replica.TableAdd(`pb.const`, true)
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	glog.Fatal(pgcache.Start(":6677"))
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
