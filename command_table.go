package pgcache

import (
	"fmt"
	"strconv"

	"github.com/bendersilver/pgcache/replica"
	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(
		&Command{
			Usage:    "Table.Info shema.table_name",
			Name:     "TABLE.INFO",
			Flags:    "admin readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: 2,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				if len(cmd.Args) != 2 {
					return wrongArity
				}
				arr, err := rawQuery(fmt.Sprintf(`PRAGMA table_info(%s);`, tableName(cmd.Args[1])))
				if err != nil {
					return err
				}
				conn.WriteArray(len(arr))
				for _, t := range arr {
					var name, typ string
					v, ok := t["name"]
					if ok {
						name = v.Str()
					}

					v, ok = t["type"]
					if ok {
						typ = v.Str()
					}
					v, ok = t["pk"]
					if ok && v.Str() == "1" {
						typ = fmt.Sprintf("%-14sPRIMARY KEY", typ)
					}
					conn.WriteString(fmt.Sprintf("%-25s%s", name, typ))
				}
				return nil
			},
		},
		&Command{
			Usage:    "Table.All",
			Name:     "TABLE.ALL",
			Flags:    "admin readonly blocking",
			FirstKey: 0, LastKey: 0, KeyStep: 0,
			Arity: 1,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {

				arr, err := rawQuery(`
						SELECT name
						FROM sqlite_schema
						WHERE type ='table'
							AND name NOT LIKE 'sqlite_%';
					`, nil)
				if err != nil {
					return err
				}
				conn.WriteArray(len(arr))
				for _, t := range arr {
					conn.WriteString(t["name"].Str())
				}
				return nil
			},
		},
		&Command{
			Usage:    "Table.Add shema.table_name [bool load all data]",
			Name:     "TABLE.ADD",
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
		},
		&Command{
			Usage:    "Table.Del shema.table_name [bool load all data]",
			Name:     "TABLE.DEL",
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
		},
	)
}
