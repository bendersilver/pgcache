package main

import (
	"encoding/json"
	"net"
	"unsafe"

	"github.com/bendersilver/glog"
)

func main() {
	cli, err := net.Dial("unix", "/tmp/test-server.sock")
	if err != nil {
		glog.Fatal(err)
	}
	var item struct {
		Met string `json:"met"`
	}
	glog.Notice(unsafe.Sizeof(item))
	for i := 0; i < 1024000; i++ {
		item.Met += "1"
	}
	glog.Notice(unsafe.Sizeof(item))
	err = json.NewEncoder(cli).Encode(&item)
	if err != nil {
		glog.Error(err)
	}
	glog.Debug("send")
	// cli.Write([]byte(`{"met": "1"}`))
	// cli.Write([]byte(`{"met": "2"}`))
	// cli.Write([]byte(`{"met": "3"}`))
	// cli.Write([]byte(`{"met": "4"}`))
	// cli.Write([]byte(`{"met": "5"}`))
	// cli.Write([]byte(`{"met": "6"}`))
	// cli.Write([]byte(`{"met": "7"}`))
	var b = make([]byte, 512)
	n, err := cli.Read(b)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Debug(b[:n])
	cli.Close()
}
