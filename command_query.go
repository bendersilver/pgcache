package pgcache

import (
	"encoding/json"

	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(
		&Command{
			Usage:    "QueryRow shema.table_name [params] [values...]",
			Name:     "QueryRow",
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
		},
		&Command{
			Usage:    "QueryRow.Fld shema.table_name [col1,col2...] [params] [values...]",
			Name:     "QueryRow.Fld",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				if len(cmd.Args) < 5 {
					return wrongArity
				}
				arr, err := queryFld(cmd.Args[1:]...)
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
		},
		&Command{
			Usage:    "Query shema.table_name [params] [values...]",
			Name:     "Query",
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
		},
		&Command{
			Usage:    "Query.Fld shema.table_name [col1,col2...] [params] [values...]",
			Name:     "Query.Fld",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				if len(cmd.Args) < 5 {
					return wrongArity
				}
				arr, err := queryFld(cmd.Args[1:]...)
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
			Usage:    "RawQuery sql [values...]",
			Name:     "RawQuery",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) error {
				if len(cmd.Args) < 1 {
					return wrongArity
				}

				arr, err := rawQuery(string(cmd.Args[1]), cmd.Args[2:]...)
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
	)
}
