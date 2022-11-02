package pgcache

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/bendersilver/pgcache/replica"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tidwall/redcon"
)

// Init -
func Init(pgURL string) error {
	err := replica.Run(pgURL)
	if err != nil {
		return err
	}
	db, err = sql.Open("sqlite3", "file:redispg?mode=memory&cache=shared&_auto_vacuum=1")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	return nil
}

// Start -
func Start(addr string) error {
	return redcon.ListenAndServe(addr, handler, accept, closed)
}

func rawQuery(sql string, args ...[]byte) ([]map[string]any, error) {
	var values []any
	for _, v := range args {
		values = append(values, string(v))
	}
	rows, err := db.Query(sql, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var res []map[string]any
	for rows.Next() {
		var scan = make([]any, len(types))
		var m = make(map[string]any)
		for i, f := range types {
			var col any
			switch f.DatabaseTypeName() {
			case "INTEGER":
				col = new(Integer)
			case "REAL":
				col = new(Numeric)
			case "TEXT":
				col = new(Text)
			case "BOOLEAN":
				col = new(Boolean)
			default:
				col = new(Blob)
			}
			scan[i] = col
			m[f.Name()] = col
		}
		err = rows.Scan(scan...)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

func query(args ...[]byte) ([]map[string]any, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		bytes.ReplaceAll(args[0], []byte("."), []byte("_")),
		args[1],
	)

	return rawQuery(sql, args[2:]...)
}
