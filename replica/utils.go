package replica

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
)

func checkTable(t *pglogrepl.RelationMessage) error {
	var create string
	err := db.QueryRow(
		fmt.Sprintf("SELECT sql FROM sqlite_master WHERE name='%s_%s'",
			t.Namespace,
			t.RelationName),
	).Scan(&create)
	if err != nil {
		return err
	}
	for _, fld := range t.Columns {
		line := fld.Name + fldType(fld.DataType)
		if fld.Flags == 1 {
			line += " PRIMARY KEY"
		}
		if !strings.Contains(create, line) {
			glog.Warningf("table: %s, line: %s", t.RelationName, line)
			return nil
		}
	}
	return nil
}

func fldType(oid uint32) string {
	if dt, ok := mi.TypeForOID(oid); ok {
		switch dt.Name {
		case "bool":
			return " BOOLEAN"
		case "int2", "int4", "int8", "timestamp", "timestamptz", "date":
			return " INTEGER"
		case "numeric", "float4", "float8":
			return " REAL"
		case "text", "varchar", "name":
			return " TEXT"
		}
	}
	return " BLOB"
}

func converDataType(v any, udt string) (any, error) {
	switch udt {
	case "timestamp", "timestamptz", "date":
		if v, ok := v.(time.Time); ok {
			return v.UnixMicro(), nil
		}
	case "int2", "int4", "int8", "bool", "text", "varchar", "name", "numeric", "float4", "float8":
		return v, nil

	}
	return json.Marshal(v)
}

func readInt32(r io.Reader) int32 {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(buf[:]))
}
func readInt16(r io.Reader) int16 {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(buf[:]))
}
