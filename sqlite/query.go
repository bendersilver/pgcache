package sqlite

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type QuerySet struct {
	Column []string
	Rows   [][]any
}

func (qs *QuerySet) Json(ix int) ([]byte, error) {
	if ix > len(qs.Rows)-1 {
		return nil, fmt.Errorf("index out of range")
	}
	m := make(map[string]any)
	for i, v := range qs.Rows[ix] {
		m[qs.Column[i]] = v
	}
	return json.Marshal(&m)
}

func (c *Conn) QueryRow(query string, args ...[]byte) (*QuerySet, error) {
	var vals []driver.Value = make([]driver.Value, len(args))
	for i, v := range args {
		vals[i] = string(v)
	}
	rows, err := c.query(query, toNamedValues(vals...))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var qs QuerySet
	qs.Column = rows.columns
	if !rows.Next() {
		return &qs, rows.err
	}
	val, err := rows.Values()
	if err != nil {
		return nil, err
	}
	qs.Rows = append(qs.Rows, val)
	return &qs, rows.err
}

func (c *Conn) QuerySet(query string, args ...[]byte) (*QuerySet, error) {
	var vals []driver.Value = make([]driver.Value, len(args))
	for i, v := range args {
		vals[i] = string(v)
	}
	rows, err := c.query(query, toNamedValues(vals...))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var qs QuerySet
	qs.Column = rows.columns
	for rows.Next() {
		val, err := rows.Values()
		if err != nil {
			return nil, err
		}
		qs.Rows = append(qs.Rows, val)
	}
	return &qs, rows.err
}
