package pgcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// AddTable -
func (pc *PgCache) AddTable(tableName string, initData bool) error {

	conn, err := pgconn.Connect(ctx, pc.pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION pgcache_slot ADD TABLE %s", tableName)).ReadAll()
	if err != nil {
		return err
	}

	var t dbTable
	if pc.tables == nil {
		pc.tables = make(map[string]*dbTable)
	}
	pc.tables[tableName] = &t

	res, err := conn.Exec(ctx,
		fmt.Sprintf(`SELECT a.attname
					FROM   pg_index i
					JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
					WHERE  i.indrelid = '%s'::regclass
						AND i.indisprimary;`,
			tableName),
	).ReadAll()
	if err != nil {
		return err
	}
	if len(res) == 0 || len(res[0].FieldDescriptions) == 0 {
		pc.DropTable(tableName)
		return fmt.Errorf("'%s' table missing primary key", tableName)
	}

	t.pk = string(res[0].Rows[0][0])

	cmt, err := conn.Prepare(ctx,
		tableName,
		fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName),
		nil,
	)

	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	t.fldOID = make([]uint32, len(cmt.Fields))
	t.fldName = make([]string, len(cmt.Fields))
	t.fldDataType = make([]*pgtype.Type, len(cmt.Fields))

	create := make([]string, len(cmt.Fields))
	var ok bool
	for i, f := range cmt.Fields {
		t.fldOID[i] = f.DataTypeOID
		t.fldName[i] = f.Name
		t.fldDataType[i], ok = mi.TypeForOID(f.DataTypeOID)
		if !ok {
			t.fldDataType[i], _ = mi.TypeForName("bytea")
			t.fldOID[i] = t.fldDataType[i].OID
		}

		create[i] = f.Name
		switch t.fldDataType[i].Name {
		case "bool", "int2", "int4", "int8", "timestamp", "timestamptz", "date":
			create[i] += " INTEGER"
		case "numeric", "float4", "float8":
			create[i] += " REAL"
		case "text", "varchar", "name":
			create[i] += " TEXT"
		default:
			create[i] += " BLOB"
		}
		if t.pk == f.Name {
			create[i] += " PRIMARY KEY"
		}
	}
	sqliteName := strings.ReplaceAll(tableName, ".", "_")
	_, err = pc.db.Exec(fmt.Sprintf("CREATE TABLE %s (\n%s\n);", sqliteName, strings.Join(create, ",\n")))
	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	t.delete, err = pc.db.Prepare(fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", sqliteName, t.pk))
	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	for i := range t.fldName {
		create[i] = "?"
	}
	t.insert, err = pc.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s);", sqliteName, strings.Join(create, ", ")))
	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	for i, n := range t.fldName {
		create[i] = n + " = ?"
	}
	t.update, err = pc.db.Prepare(fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?;", sqliteName, strings.Join(create, ",\n"), t.pk))
	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	t.truncate, err = pc.db.Prepare(fmt.Sprintf("DELETE FROM %s;", sqliteName))
	if err != nil {
		pc.DropTable(tableName)
		return err
	}

	t.selectPK, err = pc.db.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE %s = ?;", sqliteName, t.pk))
	if err != nil {
		pc.DropTable(tableName)
		return fmt.Errorf("sqlite prepare truncate: %v", err)
	}

	if initData {
		_, err = conn.CopyTo(ctx, &t, fmt.Sprintf(`COPY BINARY %s TO STDOUT;`, tableName))
		if err != nil {
			pc.DropTable(tableName)
			return fmt.Errorf("conn copyTo: %v", err)
		}
	}
	return err
}

// const signature = "PGCOPY\n\377\r\n\x00" // \0 is replaced with \x00, due to Golang syntax
var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

func (t *dbTable) decodeColumn(format int16, data []byte, ix int) (any, error) {
	tp := t.fldDataType[ix]
	if tp != nil {
		val, err := tp.Codec.DecodeValue(
			mi,
			tp.OID,
			format,
			data,
		)
		if err != nil {
			return nil, err
		}
		return converDataType(val, tp.Name)
	}
	return data, nil
}

func (t *dbTable) Write(src []byte) (n int, err error) {
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
		vals[i], err = t.decodeColumn(pgtype.BinaryFormatCode, col, i)
		if err != nil {
			return 0, err
		}
	}
	_, err = t.insert.Exec(vals...)
	return
}

func converDataType(v interface{}, udt string) (interface{}, error) {
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
