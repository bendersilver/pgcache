package pgcache

import (
	"fmt"
	"strings"
)

func tableName(b []byte) string {
	return strings.ReplaceAll(string(b), ".", "_")
}

func queryFld(args ...[]byte) ([]map[string]DataType, error) {
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s", args[1], tableName(args[0]), args[2])
	return rawQuery(sql, args[3:]...)
}

func query(args ...[]byte) ([]map[string]DataType, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s", tableName(args[0]), args[1])
	return rawQuery(sql, args[2:]...)
}

func rawQuery(sql string, args ...[]byte) ([]map[string]DataType, error) {
	var values []any
	for _, v := range args {
		values = append(values, string(v))
	}
	rows, err := db.Query(sql, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var res []map[string]DataType
	for rows.Next() {
		var scan = make([]any, len(types))
		var m = make(map[string]DataType)
		for i, f := range types {
			var col DataType
			switch f.DatabaseTypeName() {
			case "INTEGER":
				col = new(Integer)
			case "REAL":
				col = new(Numeric)
			case "TEXT":
				col = new(Text)
			case "BOOLEAN":
				col = new(Boolean)
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
