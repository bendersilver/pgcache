package pgcache

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

var wrongArity = fmt.Errorf("wrong number of arguments")

func init() {
	AddCommand(
		&Command{
			Usage:    "Cmd",
			Name:     "Cmd",
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
			Usage:    "Cmd.Info",
			Name:     "Cmd.Info",
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
