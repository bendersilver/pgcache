package main

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"runtime"

	"github.com/bendersilver/glog"
)

type reader struct {
}

func (r *reader) Write(src []byte) (n int, err error) {
	// buf := bytes.NewBuffer(src)
	// tupleLen := readInt16(buf)
	// // EOF
	// if tupleLen == -1 {
	// 	return
	// }
	if src[len(src)-1] == '\n' {
		return len(src), io.EOF
	}
	glog.Noticef("%d, %s", len(src), src)
	return len(src), nil
}

func readRequest(conn net.Conn) {

	defer conn.Close()
	var item struct {
		Met string `json:"met"`
	}
	dec := json.NewDecoder(conn) //.Decode(&item)
	// if err != nil {
	// 	glog.Error(err)
	// }
	for dec.More() {
		err := dec.Decode(&item)
		if err != nil {
			glog.Error(err)
			break
		}
		glog.Debug(item)
	}
	// conn.SetDeadline(time.Now().Add(time.Second))
	// r := new(reader)
	// _, err := io.Copy(r, conn)
	// if err != nil {
	// 	glog.Error(err)
	// 	return
	// }
	// glog.Debugf("%s", b)
	conn.Write([]byte("pong"))
	// defer conn.Close()

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	listener, err := net.Listen("unix", "/tmp/test-server.sock")
	if err != nil {
		glog.Fatal(err)
	}
	var counter int
	for {
		counter++
		glog.Notice(counter)
		conn, err := listener.Accept()
		// conn.SetDeadline(time.Now().Add(time.Second))
		if err != nil {
			continue
		}
		readRequest(conn)
	}
}

func init() {
	if err := os.RemoveAll("/tmp/test-server.sock"); err != nil {
		glog.Fatal(err)
	}
}
