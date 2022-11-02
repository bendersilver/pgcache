package pgcache

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/tidwall/redcon"
)

var wrongArity = fmt.Errorf("wrong number of arguments")

func init() {
	AddCommand(
		&Command{
			Usage:    "Table.All",
			Name:     "TABLE.ALL",
			Flags:    "admin write blocking",
			FirstKey: 0, LastKey: 0, KeyStep: 0,
			Arity: 1,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				rows, err := db.Query(`
					SELECT 
						name
					FROM 
						sqlite_schema
					WHERE 
						type ='table' AND 
						name NOT LIKE 'sqlite_%';
				`)
				if err != nil {
					return err
				}
				defer rows.Close()

				var items []string
				for rows.Next() {
					var item string
					err = rows.Scan(&item)
					if err != nil {
						glog.Error(err)
					}
					items = append(items, item)

				}
				if rows.Err() != nil {
					return rows.Err()
				}
				if len(items) == 0 {
					conn.WriteNull()
					return nil
				}
				conn.WriteArray(len(items))
				for _, t := range items {
					conn.WriteString(t)
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
		}, &Command{
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
		}, &Command{
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
		}, &Command{
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
		},
		&Command{
			Usage:    "COMMAND",
			Name:     "COMMAND",
			Flags:    "random loading stale",
			FirstKey: 0, LastKey: 0, KeyStep: 0,
			Arity: 1,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				conn.WriteArray(len(commands))
				for _, cm := range commands {

					conn.WriteArray(6)
					conn.WriteString(strings.ToLower(cm.Name))

					conn.WriteInt(cm.Arity)

					flags := strings.Split(cm.Flags, " ")
					conn.WriteArray(len(flags))
					for _, f := range flags {
						conn.WriteString(strings.ToLower(f))
					}
					conn.WriteInt(cm.FirstKey)
					conn.WriteInt(cm.LastKey)
					conn.WriteInt(cm.KeyStep)
				}
				return nil
			},
		},
		&Command{
			Usage:    "COMMAND.INFO",
			Name:     "COMMAND.INFO",
			Flags:    "random loading stale",
			FirstKey: 0, LastKey: 0, KeyStep: 0,
			Arity: 1,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				conn.WriteArray(len(commands))
				for _, cm := range commands {
					conn.WriteString(cm.Usage)
				}
				return nil
			},
		},
	)
}
