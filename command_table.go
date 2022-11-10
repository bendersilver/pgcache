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
				qs, err := db.QuerySet(fmt.Sprintf(`PRAGMA table_info(%s);`, tableName(cmd.Args[1])))
				if err != nil {
					return err
				}
				if len(qs.Rows) == 0 {
					conn.WriteNull()
					return nil
				}
				conn.WriteArray(len(qs.Rows))
				for _, vals := range qs.Rows {
					conn.WriteString(fmt.Sprintf("%-25v%v", vals[1], vals[2]))
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
				qs, err := db.QuerySet(`
					SELECT name
					FROM sqlite_schema
					WHERE type ='table'
						AND name NOT LIKE 'sqlite_%';
				`)
				if err != nil {
					return err
				}
				if len(qs.Rows) == 0 {
					conn.WriteNull()
					return nil
				}

				conn.WriteArray(len(qs.Rows))
				for _, vals := range qs.Rows {
					conn.WriteAny(vals[0])
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
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 3 {
					return wrongArity
				}
				var opt replica.AddOptions
				opt.TableName = string(cmd.Args[1])
				opt.Init, err = strconv.ParseBool(string(cmd.Args[2]))
				if err != nil {
					return err
				}
				if len(cmd.Args) > 3 {
					opt.Query = string(cmd.Args[3])
				}
				err = replica.TableAdd(&opt)
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
