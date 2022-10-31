package pgcache

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(&tableAdd, &tableDel, &pcQuery, &pcQueryRow, &pcRawQuery, &pcQueryPK)
}

var wrongArity = fmt.Errorf("wrong number of arguments")

var tableAdd = Command{
	Usage:    "PC.TableAdd shema.table_name [bool load all data]",
	Name:     "PC.TABLEADD",
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
		err = cache.AddTable(string(cmd.Args[1]), b)
		if err != nil {
			return err
		}
		conn.WriteString("OK")
		return nil
	},
}

var tableDel = Command{
	Usage:    "PC.TableDel shema.table_name [bool load all data]",
	Name:     "PC.TABLEDEL",
	Flags:    "admin write blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) != 2 {
			return wrongArity
		}
		err := cache.DropTable(string(cmd.Args[1]))
		if err != nil {
			return err
		}
		conn.WriteString("OK")
		return nil
	},
}

var pcQueryRow = Command{
	Usage:    "PC.QUERYROW shema.table_name [params] [values...]",
	Name:     "PC.QUERYROW",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 2 {
			return wrongArity
		}
		var args [][]byte
		if len(cmd.Args) > 2 {
			args = cmd.Args[2:]
		}
		arr, err := cache.Query(string(cmd.Args[1]), args...)
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
	Usage:    "PC.QUERY shema.table_name [params] [values...]",
	Name:     "PC.QUERY",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 2 {
			return wrongArity
		}
		var args [][]byte
		if len(cmd.Args) > 2 {
			args = cmd.Args[2:]
		}
		arr, err := cache.Query(string(cmd.Args[1]), args...)
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

var pcQueryPK = Command{
	Usage:    "PC.QUERYPK shema.table_name pk",
	Name:     "PC.QUERYPK",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 3,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) != 3 {
			return wrongArity
		}
		table, ok := cache.tables[string(cmd.Args[1])]
		if !ok {
			return fmt.Errorf("no such table: %s", cmd.Args[1])
		}
		arr, err := cache.Query(string(cmd.Args[1]), []byte(fmt.Sprintf("%s = ?", table.pk)), cmd.Args[2])
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

var pcRawQuery = Command{
	Usage:    "PC.RAWQUERY sql [values...]",
	Name:     "PC.RAWQUERY",
	Flags:    "readonly blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: -2,
	Action: func(conn redcon.Conn, cmd redcon.Command) error {
		if len(cmd.Args) < 2 {
			return wrongArity
		}
		var args [][]byte
		if len(cmd.Args) > 1 {
			args = cmd.Args[1:]
		}
		arr, err := cache.RawQuery(string(cmd.Args[1]), args...)
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
