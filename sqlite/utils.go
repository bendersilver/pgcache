package sqlite

import (
	"database/sql/driver"
	"time"
)

func toNamedValues(vals ...driver.Value) (r []driver.NamedValue) {
	r = make([]driver.NamedValue, len(vals))
	for i, val := range vals {
		switch v := val.(type) {
		case time.Time:
			val = v.UnixMicro()
		}
		r[i] = driver.NamedValue{Value: val, Ordinal: i + 1}
	}
	return r
}
