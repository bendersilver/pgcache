package sqlite

import (
	"bytes"
	"context"
	"database/sql/driver"
	"log"
	"net/rpc"
	"runtime"
	"testing"

	"github.com/go-redis/redis/v9"
)

func BenchmarkSqliteMem(b *testing.B) {

	runtime.GOMAXPROCS(8)
	c, err := newConn()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	err = c.Exec("CREATE TABLE js (i INTEGER  PRIMARY KEY, bo BOOLEAN, f FLOAT, t TEXT, b BLOB);")
	if err != nil {
		log.Fatal(err)
	}

	var i int
	for i = 0; i < b.N; i++ {
		err = c.Exec("INSERT INTO js VALUES (?, ?, ?, ?, ?)", int64(i), true, float64(i), "txt\n2line", []byte("sadkj\nasdasd"))
		if err != nil {
			log.Fatal(err)
		}
	}

}

func BenchmarkSqliteMemPrepare(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	c, err := newConn()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	err = c.Exec("CREATE TABLE js (i INTEGER PRIMARY KEY);")
	if err != nil {
		log.Fatal(err)
	}
	e, err := c.prepare("INSERT INTO js VALUES (?)")
	if err != nil {
		log.Fatal(err)
	}
	var i int
	for i = 0; i < b.N; i++ {
		err = e.Exec(int64(i))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkRedis(b *testing.B) {
	cli := redis.NewClient(&redis.Options{})
	var i int
	for i = 0; i < b.N; i++ {
		cli.HSet(context.Background(), "BenchmarkRedis", i, i)
	}

}

// func dropCR(data []byte) []byte {
// 	if len(data) > 0 && data[len(data)-1] == '\r' {
// 		return data[0 : len(data)-1]
// 	}
// 	return data
// }

func split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// glog.Debugf("%v %s", atEOF, data)
	if atEOF && len(data) == 0 {
		return
	}
	// glog.Noticef("%s", data)

	if rn := bytes.Index(data, []byte("\r\n")); rn > 0 {
		// We have a full newline-terminated line.
		return rn + 2, data[0:rn], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return
}

type Query struct {
	SQL  string
	Args []driver.Value
}

func TestConnRPC(t *testing.T) {
	client, err := rpc.Dial("unix", "/tmp/echo.sock")
	if err != nil {
		t.Fatal(err)
	}
	client2, err := rpc.Dial("unix", "/tmp/echo.sock")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Call("DB.Exec", &Query{
		SQL: "create table if not exists x (id INT);",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		err = client2.Call("DB.Exec", &Query{
			SQL:  "INSERT INTO x VALUES (?);",
			Args: []driver.Value{int64(i)},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var vals [][]any

	err = client.Call("DB.Query", &Query{
		SQL: "SELECT * FROM x;",
	}, &vals)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal(vals)
	// err = client.Call("DB.Exec", &Query{
	// 	SQL:  "INSERT INTO x VALUES (?);",
	// 	Args: []driver.Value{12.2},
	// }, nil)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
