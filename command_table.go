package pgcache

import (
	"strconv"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(
		&Command{
			Usage:    "Table.Info shema.table_name",
			Name:     "TABLE.INFO",
			Flags:    "admin readonly blocking",
			FirstKey: 1, LastKey: 0, KeyStep: 0,
			Arity: 2,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				if len(cmd.Args) != 3 {
					return wrongArity
				}
				var create string
				err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE name=?;`, string(cmd.Args[1])).Scan(&create)
				if err != nil {
					return err
				}
				conn.WriteString(create)
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
				rows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';`)
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
		},
	)
}
