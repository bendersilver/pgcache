package cli

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
)

// Cli -
type Cli struct {
	cli http.Client
}

// ReqBody -
type ReqBody struct {
	SQL  string         `json:"sql"`
	Args []driver.Value `json:"args"`
}

// Response -
type Response struct {
	Status int   `json:"status,omitempty"`
	Error  error `json:"error,omitempty"`

	ColumnName []string            `json:"columnName,omitempty"`
	ColumnType []string            `json:"columnType,omitempty"`
	Result     [][]json.RawMessage `json:"result,omitempty"`
}
