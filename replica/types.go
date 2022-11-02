package replica

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	slotName = "pgcache_slot"
	plugin   = "pgoutput"
)

var db *sql.DB
var mi = pgtype.NewMap()
var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

// replication -
type replication struct {
	pgURL string

	conn      *pgconn.PgConn
	lsn       pglogrepl.LSN
	relations map[uint32]*pglogrepl.RelationMessage
}

type tmpTable struct {
	readSign bool
	dbName   string
	field    []pgconn.FieldDescription
	insert   *sql.Stmt
}

// Write - io.Writer
func (t *tmpTable) Write(src []byte) (n int, err error) {
	buf := bytes.NewBuffer(src)
	n = buf.Len()
	if !t.readSign {

		if !bytes.Equal(buf.Next(len(signature)), signature) {
			return 0, fmt.Errorf(`invalid file signature: expected PGCOPY\n\377\r\n\0`)
		}
		flags := readInt32(buf)
		extensionSize := readInt32(buf)
		_ = flags
		extension := make([]byte, extensionSize)

		if _, err := io.ReadFull(buf, extension); err != nil {
			return 0, fmt.Errorf("can't read header extension: %v", err)
		}
		t.readSign = true
	}

	tupleLen := readInt16(buf)
	// EOF
	if tupleLen == -1 {
		return
	}
	vals := make([]any, tupleLen)
	for i := 0; i < int(tupleLen); i++ {
		colLen := readInt32(buf)
		// column is nil
		if colLen == -1 {
			vals[i] = nil
			continue
		}
		col := make([]byte, colLen)
		if _, err := io.ReadFull(buf, col); err != nil {
			return 0, fmt.Errorf("can't read column %v", err)
		}
		vals[i], err = t.decodeColumn(pgtype.BinaryFormatCode, t.field[i], col)
		if err != nil {
			return 0, err
		}
	}
	_, err = t.insert.Exec(vals...)
	return
}

func (t *tmpTable) decodeColumn(format int16, f pgconn.FieldDescription, data []byte) (v any, err error) {
	if dt, ok := mi.TypeForOID(f.DataTypeOID); ok {
		v, err = dt.Codec.DecodeValue(mi, f.DataTypeOID, format, data)
		if err != nil {
			glog.Errorf("val %s, err: %v", data, err)
			return nil, err
		}
		return converDataType(v, dt.Name)
	} else {
		return converDataType(data, "bytea")
	}
}
