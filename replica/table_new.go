package replica

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// AddOptions -
type AddOptions struct {
	TableName string
	Reset     bool
	InitData  bool
	Query     string
}

// TableAdd -
func TableAdd(opt *AddOptions) error {
	u, err := url.Parse(r.pgURL)
	if err != nil {
		return err
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("application_name", "redispg_copy")
	u.RawQuery = param.Encode()

	conn, err := pgconn.Connect(ctx, u.String())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if !opt.Reset {
		_, err = conn.Exec(ctx, "ALTER PUBLICATION "+slotName+" ADD TABLE "+opt.TableName).ReadAll()
		if err != nil {
			return err
		}
	}

	cmt, err := conn.Prepare(ctx,
		opt.TableName,
		"SELECT * FROM "+opt.TableName+" LIMIT 1",
		nil,
	)
	if err != nil {
		return err
	}
	var t tmpTable
	t.field = cmt.Fields
	t.dbName = opt.TableName

	create := make([]string, len(cmt.Fields))
	for i, f := range cmt.Fields {
		create[i] = f.Name
		switch f.DataTypeOID {
		case pgtype.BoolOID:
			create[i] += " BOOLEAN"
		case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID:
			create[i] += " INTEGER"
		case pgtype.NumericOID, pgtype.Float4OID, pgtype.Float8OID:
			create[i] += " REAL"
		case pgtype.TextOID, pgtype.VarcharOID, pgtype.NameOID:
			create[i] += " TEXT"
		default:
			create[i] += " BLOB"
		}
	}
	dbName := strings.ReplaceAll(opt.TableName, ".", "_")
	err = db.Exec(fmt.Sprintf("CREATE TABLE %s (\n%s\n);", dbName, strings.Join(create, ",\n")))
	if err != nil {
		return err
	}
	for i := range create {
		create[i] = "?"
	}

	t.insert, err = db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s);", dbName, strings.Join(create, ", ")))
	if err != nil {
		return err
	}
	defer t.insert.Close()

	mx.Lock()
	defer mx.Unlock()

	if opt.InitData {
		if opt.Query != "" {
			_, err = conn.CopyTo(ctx, &t, "COPY ("+opt.Query+") TO STDOUT WITH BINARY;")
		} else {
			_, err = conn.CopyTo(ctx, &t, "COPY BINARY "+t.dbName+" TO STDOUT;")
		}
		if err != nil {
			return err
		}
	}

	return nil
}
