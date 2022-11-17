package sqlite

import (
	"encoding/json"
	"fmt"
	"io"

	sqlite3 "modernc.org/sqlite/lib"
)

// Rows -
type Rows struct {
	allocs  []uintptr
	c       *Conn
	columns []string
	pstmt   uintptr

	doStep bool
	empty  bool
	result []any
	err    error
}

func newRows(c *Conn, pstmt uintptr, allocs []uintptr, empty bool) (r *Rows, err error) {
	r = &Rows{c: c, pstmt: pstmt, allocs: allocs, empty: empty}

	defer func() {
		if err != nil {
			r.Close()
			r = nil
		}
	}()

	n, err := c.columnCount(pstmt)
	if err != nil {
		return nil, err
	}

	r.columns = make([]string, n)
	for i := range r.columns {
		if r.columns[i], err = r.c.columnName(pstmt, i); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Close -
func (r *Rows) Close() (err error) {
	for _, v := range r.allocs {
		r.c.free(v)
	}
	r.allocs = nil
	return r.c.finalize(r.pstmt)
}

// Columns -
func (r *Rows) Columns() (c []string) {
	return r.columns
}

// Values -
func (r *Rows) Values() ([]any, error) {
	return r.result, r.err
}

// Err -
func (r *Rows) Err() error {
	return r.err
}

// Next -
func (r *Rows) Next() bool {
	r.result = nil
	err := r.next()
	if err != nil {
		if err != io.EOF {
			r.err = err
		}
		return false
	}
	return true
}

func (r *Rows) next() (err error) {
	if r.empty {
		return io.EOF
	}

	rc := sqlite3.SQLITE_ROW
	if r.doStep {
		if rc, err = r.c.step(r.pstmt); err != nil {
			return err
		}
	}

	r.doStep = true
	switch rc {
	case sqlite3.SQLITE_ROW:
		r.result = make([]any, len(r.columns))
		for i := range r.columns {
			ct, err := r.c.columnType(r.pstmt, i)
			if err != nil {
				return err
			}
			switch ct {
			case sqlite3.SQLITE_INTEGER:
				v, err := r.c.columnInt64(r.pstmt, i)
				if err != nil {
					return err
				}
				if r.c.columnDeclType(r.pstmt, i) == "BOOLEAN" {
					r.result[i] = v > 0
				} else {
					r.result[i] = v
				}
			case sqlite3.SQLITE_FLOAT:
				v, err := r.c.columnDouble(r.pstmt, i)
				if err != nil {
					return err
				}

				r.result[i] = v
			case sqlite3.SQLITE_TEXT:
				v, err := r.c.columnText(r.pstmt, i)
				if err != nil {
					return err
				}
				r.result[i] = v
			case sqlite3.SQLITE_BLOB:
				v, err := r.c.columnBlob(r.pstmt, i)
				if err != nil {
					return err
				}
				r.result[i] = json.RawMessage(v)
			case sqlite3.SQLITE_NULL:
				r.result[i] = nil
			default:
				return fmt.Errorf("internal error: rc %d", rc)
			}
		}
		return nil
	case sqlite3.SQLITE_DONE:
		return io.EOF
	default:
		return r.c.errstr(int32(rc))
	}
}
