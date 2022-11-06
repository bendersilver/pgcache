package sqlite

import (
	"testing"

	"github.com/bendersilver/glog"
)

func TestConn(t *testing.T) {
	c, err := newConn()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.Exec("CREATE TABLE js (id INT, d BLOB);")
	if err != nil {
		t.Fatal(err)
	}
	err = c.Exec("INSERT INTO js VALUES (?, ?)", 1, map[string]string{"1": "2"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Exec("INSERT INTO js VALUES (?, ?)", 2, 9999)
	if err != nil {
		t.Fatal(err)
	}
	// rows, err := c.Query("SELECT json_object('id', id, 'd', d) FROM js")
	rows, err := c.Query("SELECT ID, d FROM js")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		glog.Noticef("%s", vals)
	}

}
