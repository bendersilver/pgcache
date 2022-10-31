package pgcache

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/bendersilver/glog"
	"github.com/tidwall/redcon"
)

func accept(conn redcon.Conn) bool {
	glog.Debug("accept", conn.RemoteAddr())
	return true
}

func closed(conn redcon.Conn, err error) {
	glog.Debug("closed", conn.RemoteAddr())
}

var commands []*Command

// AddCommand -
func AddCommand(cmd ...*Command) {
	commands = append(commands, cmd...)
}

var tableAdd = Command{
	Usage: "PC.TableAdd shema.table_name [bool load all data]",
	// Desc:     " добавление таблицы <shema.table> в отслеживание ",
	Name:     "PC.TableAdd",
	Flags:    "admin write blocking",
	FirstKey: 1, LastKey: 1, KeyStep: 1,
	Arity: 3,
	Action: func(conn redcon.Conn, cmd redcon.Command) int {
		b, err := strconv.ParseBool(string(cmd.Args[3]))
		if err != nil {
			conn.WriteError(err.Error())
			return 0
		}
		return 0
	},
}

var mx sync.Mutex

func handler(conn redcon.Conn, cmd redcon.Command) {
	mx.Lock()
	defer mx.Unlock()
	command := string(cmd.Args[0])
	args := cmd.Args[1:]

	switch strings.ToLower(command) {
	case "pc.tableadd":
		if len(args) != 2 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for `%s` command", strings.ToUpper(command)))
			return
		}
		b, err := strconv.ParseBool(string(args[1]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		err = cache.AddTable(string(args[0]), b)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	case "pc.tabledel":
		if len(args) != 1 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for `%s` command", strings.ToUpper(command)))
			return
		}
		err := cache.DropTable(string(args[0]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")

	case "query":
		if len(args) < 2 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for `%s` command", strings.ToUpper(command)))
			return
		}

		b, err := cache.Get(string(args[0]), string(args[1]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteBulk(b)

	// case "set":
	// 	if len(args) != 2 {
	// 		conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for `%s` command", strings.ToUpper(command)))
	// 		return
	// 	}
	// 	conn.WriteString("OK")

	case "ping":
		conn.WriteString("PONG")

	case "info":
		conn.WriteArray(1)
		conn.WriteBulkString("")

	case "select":
		conn.WriteString("OK")
	default:
		glog.Warning(command)
		conn.WriteError(fmt.Sprintf(" ERR unknown command `%s`", strings.ToUpper(command)))
	}
}
