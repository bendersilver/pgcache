package cli

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
)

// New -
func New(sock string) *Cli {
	return &Cli{
		cli: http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", sock)
				},
			},
		},
	}
}

// Query -
func (c *Cli) Query(query string, args ...driver.Value) (*Response, error) {
	b, err := json.Marshal(ReqBody{
		SQL:  query,
		Args: args,
	})
	if err != nil {
		return nil, err
	}
	rsp, err := c.cli.Post("http://unix", "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	var data Response
	err = json.NewDecoder(rsp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	if data.Status != 0 {
		return nil, data.Error
	}
	if len(data.Result) == 0 {
		return nil, fmt.Errorf("no rows in result set")
	}
	return &data, nil
}

// Next -
func (r *Response) Next() bool {
	return len(r.Result) > 0
}

// Scan -
func (r *Response) Scan(args ...any) error {
	if len(r.Result) == 0 {
		return fmt.Errorf("empty")
	}
	line := r.Result[0]
	if len(args) > len(line) {
		return fmt.Errorf("inconsistency number argumrnts")
	}
	for i, arg := range args {
		r.Error = json.Unmarshal(line[i], arg)
		if r.Error != nil {
			return r.Error
		}
	}
	r.Result = r.Result[1:]
	return nil
}

// Err -
func (r *Response) Err() error {
	return r.Error
}

// Close -
func (r *Response) Close() {
	r.Result = nil
}
