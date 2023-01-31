package main

import (
	"database/sql/driver"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"runtime"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/joho/godotenv"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	godotenv.Load()
	if err := os.RemoveAll(os.Getenv("SOCK")); err != nil {
		glog.Fatal(err)
	}

	c, err := replica.NewConn(&replica.Options{
		PgURL:     os.Getenv("PG_URL"),
		SlotName:  os.Getenv("SLOT"),
		TableName: os.Getenv("TABLE"),
	})

	if err != nil {
		glog.Fatal(err)
	}

	var s http.Server

	db, err := sqlite.NewConn()
	if err != nil {
		glog.Fatal(err)
	}
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Sql  string
			Args []driver.Value
		}
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{
				"status": 500,
				"error":  err.Error(),
			})
			return
		}

		rows, err := db.Query(body.Sql, body.Args...)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{
				"status": 500,
				"error":  err.Error(),
			})
			return
		}
		defer rows.Close()

		var response struct {
			ColumnName []string
			Result     [][]any
		}
		response.ColumnName = rows.Columns()

		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				break
			}
			response.Result = append(response.Result, vals)
		}
		if rows.Err() != nil {
			json.NewEncoder(w).Encode(map[string]any{
				"status": 500,
				"error":  err.Error(),
			})
		} else {
			json.NewEncoder(w).Encode(&response)
		}
	})

	var sError = make(chan error)
	go func() {
		os.RemoveAll(os.Getenv("SOCK"))
		ux, err := net.Listen("unix", os.Getenv("SOCK"))
		if err != nil {
			glog.Fatal(err)
		}
		sError <- s.Serve(ux)
	}()

	select {
	case err = <-c.ErrCH:
		s.Close()
	case err = <-sError:
		c.Close()
		// glog.Error(e)
	}
	// c.ErrCH

	glog.Error(err)
}
