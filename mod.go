package pgcache

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

func accept(conn redcon.Conn) bool {
	// glog.Debug("accept", conn.RemoteAddr())
	return true
}

func closed(conn redcon.Conn, err error) {
	// glog.Debug("closed", conn.RemoteAddr())
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

var mx sync.Mutex

func handler(conn redcon.Conn, cmd redcon.Command) {
	mx.Lock()
	defer mx.Unlock()
	if cm, ok := commands[strings.ToLower(string(cmd.Args[0]))]; ok {
		err := cm.Action(conn, cmd)
		if err != nil {
			conn.WriteError(err.Error())
		}
	} else {
		conn.WriteError(fmt.Sprintf(" ERR unknown command `%s`", cmd.Args[0]))
	}
}
