package replica

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func TableAdd(tableName string, initData bool) error {
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

	_, err = conn.Exec(ctx, "ALTER PUBLICATION "+slotName+" ADD TABLE "+tableName).ReadAll()
	if err != nil {
		return err
	}

	cmt, err := conn.Prepare(ctx,
		tableName,
		"SELECT * FROM "+tableName+" LIMIT 1",
		nil,
	)
	if err != nil {
		return err
	}
	var t tmpTable
	t.field = cmt.Fields
	t.dbName = tableName

	create := make([]string, len(cmt.Fields))
	for i, f := range cmt.Fields {
		create[i] = f.Name

		if dt, ok := mi.TypeForOID(f.DataTypeOID); !ok {
			create[i] += " BLOB"
		} else {
			switch dt.Name {
			case "bool":
				create[i] += " BOOLEAN"
			case "int2", "int4", "int8", "timestamp", "timestamptz", "date":
				create[i] += " INTEGER"
			case "numeric", "float4", "float8":
				create[i] += " REAL"
			case "text", "varchar", "name":
				create[i] += " TEXT"
			default:
				create[i] += " BLOB"
			}
		}
	}
	dbName := strings.ReplaceAll(tableName, ".", "_")
	_, err = r.db.Exec(fmt.Sprintf("CREATE TABLE %s (\n%s\n);", dbName, strings.Join(create, ",\n")))
	if err != nil {
		return err
	}
	for i := range create {
		create[i] = "?"
	}
	t.insert, err = r.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s);", dbName, strings.Join(create, ", ")))
	if err != nil {
		return err
	}
	defer t.insert.Close()

	if initData {
		_, err := conn.CopyTo(ctx, &t, "COPY BINARY "+t.dbName+" TO STDOUT;")
		if err != nil {
			return err
		}
	}

	return nil
}
