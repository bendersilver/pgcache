package pgcache

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tidwall/redcon"
)

// DataType -
type DataType interface {
	Scan(value any) error
	MarshalJSON() ([]byte, error)
	Str() string
}

// Blob -
type Blob struct {
	Byte []byte
}

// Scan -
func (n *Blob) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case []byte:
		n.Byte = v
		return nil
	case float64, int64, string, bool:
		n.Byte, err = json.Marshal(value)
		return err

	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Blob", value)
}

// Str -
func (n *Blob) Str() string {
	if n.Byte == nil {
		return "(nil)"
	}
	v, _ := strconv.Unquote(string(n.Byte))
	return v
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

	case bool:
		n.Bool, n.Valid = v, true
		return nil
	case nil:
		return nil
	}
	return fmt.Errorf("invalid type %T for Boolean", value)
}

// Str -
func (n *Boolean) Str() string {
	if !n.Valid {
		return "(nil)"
	}
	if n.Bool {
		return "true"
	}
	return "false"
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

// Str -
func (n *Integer) Str() string {
	if !n.Valid {
		return "(nil)"
	}
	return fmt.Sprintf("%d", n.Int64)
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

// Str -
func (n *Numeric) Str() string {
	if !n.Valid {
		return "(nil)"
	}
	return fmt.Sprintf("%.10f", n.Float64)
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

// Str -
func (n *Text) Str() string {
	if !n.Valid {
		return "(nil)"
	}
	return n.String
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
