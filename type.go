package pgcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bendersilver/pgcache/replica"
	"github.com/jackc/pgx/v5/pgtype"
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
}

type dbTable struct {
	pk          string
	readSign    bool
	fldOID      []uint32
	fldName     []string
	fldDataType []*pgtype.Type

	insert, update, delete, truncate, selectPK *sql.Stmt
}

func (pc *PgCache) Get(name, where string) ([]byte, error) {
	table, ok := pc.tables[name]
	if !ok {
		return nil, fmt.Errorf("no such table: %s", name)
	}
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
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		strings.ReplaceAll(name, ".", "_"),
		where,
	)
	err := pc.db.QueryRow(sql).Scan(scan...)
	if err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

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

type Boolean struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

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

type Text struct {
	String string
	Valid  bool
}

func (n *Text) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case string:
		n.String, n.Valid = v, true
		return nil

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Text", value)
}

func (n Text) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.String)
}
