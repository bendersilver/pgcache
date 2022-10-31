package pgcache

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/replica"
)

func (pc *PgCache) watchMessage() {
	var table *dbTable
	var ok bool
	var vals []any
	var val any
	var pk any
	var err error
	for r := range pc.msgChan {
		table, ok = pc.tables[fmt.Sprintf("%s.%s", r.Shema, r.Table)]
		if ok {
			if r.New != nil {
				for _, c := range r.New {
					val, err = pc.getValue(c)
					if err != nil {
						glog.Error(err)
						break
					}
					if c.PK {
						pk = val
					}
					vals = append(vals, val)
				}
			}
			if r.Old != nil {
				for _, c := range r.Old {
					if c.PK {
						pk, err = pc.getValue(c)
						if err != nil {
							glog.Error(err)
							break
						}
					}
				}
			}
			switch r.Type {
			case replica.Insert:
				if vals != nil {
					_, err = table.insert.Exec(vals...)
				}
			case replica.Update:
				if vals != nil {
					vals = append(vals, pk)
					_, err = table.update.Exec(vals...)
				}
			case replica.Delete:
				if pk != nil {
					_, err = table.delete.Exec(pk)
				}
			case replica.Truncate:
				_, err = table.truncate.Exec()
			}
		}
		if err != nil {
			glog.Error(err)
		}
		vals = nil
	}
}

func (pc *PgCache) getValue(col *replica.Col) (any, error) {
	if col.Value == nil {
		return nil, nil
	}
	switch col.UDT {
	case "timestamp", "timestamptz", "date":
		t, ok := col.Value.(time.Time)
		if ok {
			return t.UnixMicro(), nil
		}
	case "bool":
		b, ok := col.Value.(bool)
		if ok {
			if b {
				return 1, nil
			}
			return 0, nil
		}
	case "int2", "int4", "int8", "numeric", "float4", "float8", "text", "varchar", "name":
		return col.Value, nil
	}
	return json.Marshal(col.Value)
}
