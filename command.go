package pgcache

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

var wrongArity = fmt.Errorf("wrong number of arguments")

func init() {
	AddCommand(&Command{
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
			Usage:    "CMD",
			Name:     "CMD",
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
			Usage:    "CMD.INFO",
			Name:     "CMD.INFO",
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
