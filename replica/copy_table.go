package replica

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

func (c *Conn) createTableRule() error {
	err := c.conDB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		(
			sheme_name VARCHAR(50) NOT NULL,
			table_name VARCHAR(150) NOT NULL,
			initsql TEXT,
			cleansql TEXT,
			cleantimeout INT,
			CONSTRAINT cleansql_chek CHECK (
				CASE
					WHEN cleansql NOTNULL OR cleantimeout NOTNULL THEN
						cleansql NOTNULL AND GREATEST(cleantimeout, 0) > 0
					ELSE TRUE
				END
			),
			PRIMARY KEY (sheme_name, table_name)
		);
	`, c.opt.TableName)).Close()
	if err != nil {
		return err
	}
	args := strings.Split(c.opt.TableName, ".")
	if len(args) != 2 {
		return fmt.Errorf("wrong format replica rule table. TableName format `<shema>.<table_name>`")
	}

	return c.conDB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (sheme_name, table_name, initsql)
		VALUES ('%s', '%s', '*')
		ON CONFLICT (sheme_name, table_name) DO NOTHING;
	`, c.opt.TableName, args[0], args[1])).Close()
}

func (c *Conn) copy() error {
	err := c.createTableRule()
	if err != nil {
		return err
	}

	res, err := c.conDB.Exec(ctx,
		fmt.Sprintf(`SELECT sheme_name, table_name, initsql, cleansql, cleantimeout
		FROM %s`, c.opt.TableName,
		),
	).ReadAll()
	if err != nil {
		return err
	}

	if len(res) > 0 {
		for _, row := range res[0].Rows {
			glog.Noticef("%s %s %s", row[0], row[1], row[2])
			err = c.copyTable(string(row[0]), string(row[1]), string(row[2]))
			if err != nil {
				glog.Error(err)
			}
		}
	}

	return nil

}
func (c *Conn) dropTable(t *relTable) error {

	err := c.conDB.Exec(ctx, fmt.Sprintf(`
			ALTER PUBLICATION %s DROP TABLE %s;
			`, c.opt.SlotName, t.pgName)).Close()

	if err != nil {
		return fmt.Errorf("drop publication err: %v", err)
	}
	return c.db.Exec("DROP TABLE IF EXISTS " + t.sqliteName + ";")
}

func (c *Conn) copyTable(sheme, table, sql string) error {
	err := c.conDB.Exec(ctx, fmt.Sprintf(`
			ALTER PUBLICATION %s ADD TABLE %s.%s;
			`, c.opt.SlotName, sheme, table)).Close()

	if err != nil {
		return fmt.Errorf("alter publication err: %v", err)
	}

	var t relTable
	t.pgName = fmt.Sprintf("%s.%s", sheme, table)
	t.sqliteName = fmt.Sprintf("%s_%s", sheme, table)

	res, err := c.conDB.Exec(ctx, fmt.Sprintf(`
		SELECT pa.attrelid, pa.attname, pa.atttypid, pa.attnum, COALESCE(pi.indisprimary, FALSE)
		FROM pg_attribute pa
		LEFT JOIN pg_index pi ON pa.attrelid = pi.indrelid AND pa.attnum = ANY(pi.indkey)
		WHERE pa.attrelid = '%s'::regclass
			AND pa.attnum > 0
			AND NOT pa.attisdropped
		ORDER  BY pa.attnum;
		`, t.pgName)).ReadAll()
	if err != nil {
		return fmt.Errorf("pg prepare err: %v", err)
	}
	var cols, pk []string
	if len(res) > 0 {
		t.field = make([]*fieldDescription, len(res[0].Rows))
		var u64 uint64
		for i, v := range res[0].Rows {
			u64, err = parseInt(v[0]) // tableOID
			if err != nil {
				glog.Error(err)
			} else {
				t.oid = uint32(u64)
			}
			var f fieldDescription
			t.columnNum++
			t.field[i] = &f
			f.name = string(v[1])
			f.tableOID = t.oid
			f.attrNum = uint16(i)

			u64, err = parseInt(v[2]) // tableOID
			if err != nil {
				glog.Error(err)
			} else {
				f.oid = uint32(u64)
			}

			f.isPrimary, err = strconv.ParseBool(string(v[4]))
			if err != nil {
				glog.Error(err)
			}
			if f.isPrimary {
				pk = append(pk, f.name)
			}
			switch f.oid {
			case pgtype.BoolOID:
				cols = append(cols, f.name+" BOOLEAN")
			case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID:
				cols = append(cols, f.name+" INTEGER")
			case pgtype.NumericOID, pgtype.Float4OID, pgtype.Float8OID:
				cols = append(cols, f.name+" REAL")
			case pgtype.TextOID, pgtype.VarcharOID, pgtype.NameOID:
				cols = append(cols, f.name+" TEXT")
			default:
				cols = append(cols, f.name+" BLOB")
			}
		}
	}
	if pk == nil {
		return fmt.Errorf("table `%s.%s` pass. Missing primary key", sheme, table)
	}
	err = c.db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (\n%s\n,PRIMARY KEY (%s)\n);",
		t.sqliteName, strings.Join(cols, ",\n"), strings.Join(pk, ",")))
	if err != nil {
		return fmt.Errorf("sqlite create table err: %v", err)
	}

	t.insert, err = c.db.Prepare(
		fmt.Sprintf("INSERT OR REPLACE INTO %s VALUES (%s);",
			t.sqliteName, strings.Trim(strings.Repeat("?, ", t.columnNum), ", "),
		))
	if err != nil {
		return err
	}

	t.truncate, err = c.db.Prepare(
		fmt.Sprintf("DELETE FROM %s;", t.sqliteName))
	if err != nil {
		return err
	}

	t.delete, err = c.db.Prepare(
		fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (VALUES(%s));",
			t.sqliteName, strings.Join(pk, ", "), strings.Trim(strings.Repeat("?, ", t.columnNum), ", "),
		))
	if err != nil {
		return err
	}

	c.relations[t.oid] = &t

	if sql == "*" {
		sql = fmt.Sprintf("SELECT * FROM %s.%s", sheme, table)
	}
	_, err = c.conDB.CopyTo(ctx, &t, "COPY ("+sql+") TO STDOUT WITH BINARY;")
	return err

}

func parseInt(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}

type relTable struct {
	readSign    bool
	firstRelMsg bool

	pgName     string
	sqliteName string
	oid        uint32
	columnNum  int
	field      []*fieldDescription
	insert     *sqlite.Stmt
	delete     *sqlite.Stmt
	truncate   *sqlite.Stmt
}

type fieldDescription struct {
	name      string
	oid       uint32
	tableOID  uint32
	attrNum   uint16
	isPrimary bool
}

func (t *relTable) decodeTuple(tuple *pglogrepl.TupleData) (vals []driver.Value, err error) {
	// cols := ri.msg.Columns
	vals = make([]driver.Value, t.columnNum)

	for ix, col := range tuple.Columns {
		rc := t.field[ix]
		switch col.DataType {
		case 'n':
			vals[ix] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			vals[ix], err = decodeColumn(pgtype.TextFormatCode, rc.oid, col.Data)
			if err != nil {
				glog.Error(err)
				return nil, err
			}
		}
	}
	return
}

// Write - io.Writer
func (t *relTable) Write(src []byte) (n int, err error) {
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
		vals[i], err = decodeColumn(pgtype.BinaryFormatCode, t.field[i].oid, col)
		if err != nil {
			return 0, err
		}
	}

	err = t.insert.Exec(vals...)
	return
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
