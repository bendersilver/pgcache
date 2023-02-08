package cli

import (
	"encoding/json"
	"net/http"

	"github.com/bendersilver/pgcache/sqlite"
)

// SvrJSON -
func SvrJSON(r *http.Request, db *sqlite.Conn) (rsp *Response, err error) {
	var body ReqBody
	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		return
	}

	rows, err := db.Query(body.SQL, body.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp = new(Response)
	rsp.ColumnName = rows.Columns()
	rsp.ColumnType = rows.ColumnsType()

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			break
		}

		arr := make([]json.RawMessage, len(rsp.ColumnName))
		for i := 0; i < len(rsp.ColumnName)-1; i++ {
			switch v := vals[i].(type) {
			case json.RawMessage:
				arr[i] = v
			default:
				arr[i], err = json.Marshal(v)
				if err != nil {
					return nil, err
				}
			}
		}
		rsp.Result = append(rsp.Result, arr)
	}

	return rsp, rows.Err()
}
