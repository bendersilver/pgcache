package replica

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	plugin = "pgoutput"
)

var slotName = "pgcache_slot"

// SetSlotName -
func SetSlotName(name string) {
	slotName = name
}

var mx sync.Mutex
var db *sqlite.Conn
var mi = pgtype.NewMap()
var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

// replication -
type replication struct {
	pgURL string

	conn      *pgconn.PgConn
	lsn       pglogrepl.LSN
	relations map[uint32]*relationItem
}

type tmpTable struct {
	readSign bool
	dbName   string
	field    []pgconn.FieldDescription
	insert   *sqlite.Stmt
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

// Write - io.Writer
func (t *tmpTable) Write(src []byte) (n int, err error) {
	buf := bytes.NewBuffer(src)
	n = buf.Len()
	if !t.readSign {

		if !bytes.Equal(buf.Next(len(signature)), signature) {
			return 0, fmt.Errorf("invalid file signature: %s", signature)
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
	vals := make([]driver.Value, tupleLen)
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
		vals[i], err = decodeColumn(pgtype.BinaryFormatCode, t.field[i].DataTypeOID, col)
		if err != nil {
			return 0, err
		}
	}

	err = t.insert.Exec(vals...)
	return
}

func decodeColumn(format int16, oid uint32, data []byte) (v driver.Value, err error) {
	if dt, ok := mi.TypeForOID(oid); ok {
		dv, err := dt.Codec.DecodeDatabaseSQLValue(mi, oid, format, data)
		if err != nil {
			glog.Errorf("val %s, err: %v", data, err)
			return nil, err
		}
		return dv, nil
	}
	return decodeColumn(format, 17, data)
}
