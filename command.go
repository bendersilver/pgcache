package pgcache

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/bendersilver/pgcache/replica"
	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(&tableAdd, &tableDel, &pcQuery, &pcQueryRow, &pcRawQuery)
}

var wrongArity = fmt.Errorf("wrong number of arguments")

var tableAdd = Command{
	Usage:    "TableAdd shema.table_name [bool load all data]",
	Name:     "TABLEADD",
	Flags:    "admin write blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 3,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) != 3 {
			return wrongArity
		}
		b, err := strconv.ParseBool(string(cmd.Args[2]))
		if err != nil {
			return err
		}
		err = replica.TableAdd(string(cmd.Args[1]), b)
		if err != nil {
			return err
		}
		conn.WriteString("OK")
		return nil
	},
}

var tableDel = Command{
	Usage:    "TableDel shema.table_name [bool load all data]",
	Name:     "TABLEDEL",
	Flags:    "admin write blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) != 2 {
			return wrongArity
		}
		err := replica.TableDrop(string(cmd.Args[1]))
		if err != nil {
			return err
		}
		conn.WriteString("OK")
		return nil
	},
}

var pcQueryRow = Command{
	Usage:    "QUERYROW shema.table_name [params] [values...]",
	Name:     "QUERYROW",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 4 {
			return wrongArity
		}
		arr, err := query(cmd.Args[1:]...)
		if err != nil {
			return err
		}
		if len(arr) > 0 {
			b, err := json.Marshal(arr[0])
			if err != nil {
				return err
			}
			conn.WriteBulk(b)
		} else {
			conn.WriteNull()
		}
		return nil
	},
}

var pcQuery = Command{
	Usage:    "QUERY shema.table_name [params] [values...]",
	Name:     "QUERY",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 4 {
			return wrongArity
		}

		arr, err := query(cmd.Args[1:]...)
		if err != nil {
			return err
		}
		if len(arr) > 0 {
			conn.WriteArray(len(arr))
			for _, item := range arr {
				b, err := json.Marshal(item)
				if err != nil {
					return err
				}
				conn.WriteBulk(b)
			}
		} else {
			conn.WriteNull()
		}
		return nil
	},
}

var pcRawQuery = Command{
	Usage:    "RAWQUERY sql [values...]",
	Name:     "RAWQUERY",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 1 {
			return wrongArity
		}

		arr, err := rawQuery(string(cmd.Args[1]), cmd.Args[1:]...)
		if err != nil {
			return err
		}
		if len(arr) > 0 {
			conn.WriteArray(len(arr))
			for _, item := range arr {
				b, err := json.Marshal(item)
				if err != nil {
					return err
				}
				conn.WriteBulk(b)
			}
		} else {
			conn.WriteNull()
		}
		return nil
	},
}
