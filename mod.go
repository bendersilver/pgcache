package pgcache

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/tidwall/redcon"
)

// CmdFunc -
type CmdFunc func(conn redcon.Conn, cmd redcon.Command) error

// Command -
type Command struct {
	Usage  string
	Desc   string
	Name   string //This is the command's name in lowercase.
	Action CmdFunc
	// Use BuildCommandFLags to generate this flags
	// Arity is the number of arguments a command expects. It follows a simple pattern:

	// A positive integer means a fixed number of arguments.
	// A negative integer means a minimal number of arguments.
	// Command arity always includes the command's name itself (and the subcommand when applicable).
	Arity                      int
	Flags                      string
	FirstKey, LastKey, KeyStep int
}

func accept(conn redcon.Conn) bool {
	glog.Debug("connect", conn.RemoteAddr())
	return true
}

func closed(conn redcon.Conn, err error) {
	glog.Debug("conn close", conn.RemoteAddr())
}

var commands = make(map[string]*Command)

// AddCommand -
func AddCommand(cmd ...*Command) error {
	for _, c := range cmd {
		name := strings.ToLower(c.Name)
		if _, ok := commands[name]; ok {
			return fmt.Errorf("command %s exists", c.Name)
		}
		commands[name] = c
	}
	return nil
}

func handler(conn redcon.Conn, cmd redcon.Command) {
	if cm, ok := commands[strings.ToLower(string(cmd.Args[0]))]; ok {
		err := cm.Action(conn, cmd)
		if err != nil {
			if err != nil {

				glog.Noticef("%s", bytes.Join(cmd.Args, []byte(" ")))
				glog.Error(err)
			}
			conn.WriteError(err.Error())
		}
	} else {
		glog.Warningf("unknown command `%s`", cmd.Args[0])
		for _, v := range cmd.Args[1:] {
			glog.Warningf("\targ: %s", v)
		}
		conn.WriteError(fmt.Sprintf("ERR unknown command `%s`", cmd.Args[0]))
	}
}
