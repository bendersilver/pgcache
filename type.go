package pgcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bendersilver/pgcache/replica"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tidwall/redcon"
)

var ctx = context.Background()
var mi = pgtype.NewMap()
var cache *PgCache

// PgCache -
type PgCache struct {
	db      *sql.DB
	pgURL   string
	tables  map[string]*dbTable
	msgChan chan *replica.Row
	errChan chan error
	cancel  context.CancelFunc
}

type dbTable struct {
	pk          string
	readSign    bool
	fldOID      []uint32
	fldName     []string
	fldDataType []*pgtype.Type

	insert, update, delete, truncate, selectPK *sql.Stmt
}

// Get -
func (pc *PgCache) Query(name string, args ...[]byte) ([]map[string]interface{}, error) {
	table, ok := pc.tables[name]
	if !ok {
		return nil, fmt.Errorf("no such table: %s", name)
	}
	var values []any
	sql := fmt.Sprintf("SELECT * FROM %s", strings.ReplaceAll(name, ".", "_"))
	if len(args) > 0 {
		sql += fmt.Sprintf(" WHERE %s;", args[0])
		for i, a := range args {
			if i > 0 {
				values = append(values, string(a))
			}
		}
	} else {
		sql += ";"
	}
	var res []map[string]interface{}
	rows, err := pc.db.Query(sql, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var scan = make([]any, len(table.fldName))
		var m = make(map[string]any)
		for i, f := range table.fldDataType {
			var col any
			switch f.Name {
			case "int2", "int4", "int8", "timestamp", "timestamptz", "date":
				col = new(Integer)
			case "numeric", "float4", "float8":
				col = new(Numeric)
			case "text", "varchar", "name":
				col = new(Text)
			case "bool":
				col = new(Boolean)
			default:
				col = new(Blob)
			}
			scan[i] = col
			m[table.fldName[i]] = col
		}
		err = rows.Scan(scan...)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

// Get -
func (pc *PgCache) RawQuery(sql string, args ...[]byte) ([]map[string]interface{}, error) {
	cmt, err := pc.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer cmt.Close()
	var values []any
	if len(args) > 0 {
		sql += fmt.Sprintf(" WHERE %s;", args[0])
		for i, a := range args {
			if i > 0 {
				values = append(values, string(a))
			}
		}
	}
	rows, err := cmt.Query(values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var res []map[string]interface{}
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

// Blob -
type Blob struct {
	Byte []byte
}

// Scan -
func (n *Blob) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		n.Byte = v
		return nil

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Blob", value)
}

// MarshalJSON -
func (n Blob) MarshalJSON() ([]byte, error) {
	if n.Byte == nil {
		return []byte("null"), nil
	}
	return n.Byte, nil
}

// Boolean -
type Boolean struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

// Scan -
func (n *Boolean) Scan(value interface{}) error {
	switch v := value.(type) {
	case int64:
		n.Bool, n.Valid = v == 1, true
		return nil

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Boolean", value)
}

// MarshalJSON - redis protocol response
func (n Boolean) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Bool)
}

// Integer - int64
type Integer struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
}

// Scan -
func (n *Integer) Scan(value interface{}) error {
	switch v := value.(type) {
	case int64:
		n.Int64, n.Valid = v, true
		return nil

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for integer", value)
}

// MarshalJSON -
func (n Integer) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Int64)
}

// Numeric - float64
type Numeric struct {
	Float64 float64
	Valid   bool // Valid is true if Float64 is not NULL
}

// Scan - pgsql scan set value
func (n *Numeric) Scan(value interface{}) error {
	switch v := value.(type) {
	case float64:
		n.Float64, n.Valid = v, true
		return nil

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Numeric", value)
}

// MarshalJSON - redis protocol response
func (n Numeric) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Float64)
}

// Text -
type Text struct {
	String string
	Valid  bool
}

// Scan -
func (n *Text) Scan(value interface{}) (err error) {
	switch value := value.(type) {
	case string:
		n.String, n.Valid = value, true
		return nil
	case []byte:
		n.String, n.Valid = string(value), true
		return nil
	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Text", value)
}

// MarshalJSON -
func (n Text) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.String)
}

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
