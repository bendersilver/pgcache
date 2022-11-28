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
	InitData  bool
	Query     string
	shema     string
	table     string
}

func (o *AddOptions) tableName() string {
	return fmt.Sprintf("%s_%s", o.shema, o.table)
}

// TableAdd -
func TableAdd(opt *AddOptions) error {
	u, err := url.Parse(r.pgURL)
	if err != nil {
		return err
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("application_name", slotName)
	u.RawQuery = param.Encode()

	conn, err := pgconn.Connect(ctx, u.String())
	if err != nil {
		return fmt.Errorf("pg connerct err: %v", err)
	}
	defer conn.Close(ctx)
	args := strings.Split(opt.TableName, ".")
	if len(args) != 2 {
		return fmt.Errorf("wrong format table. TableName format `<shema>.<table_name>`")
	}
	opt.shema = args[0]
	opt.table = args[1]

	res, err := conn.Exec(ctx, fmt.Sprintf(`
		SELECT *
		FROM pg_catalog.pg_publication_tables
		WHERE pubname = '%s'
			AND schemaname = '%s'
			AND tablename = '%s';
		`, slotName, opt.shema, opt.table)).ReadAll()
	if err != nil {
		return fmt.Errorf("pg get pg_publication_tables err: %v", err)
	}
	if len(res) > 0 && res[0].Rows != nil {
		err = TableDrop(opt.TableName)
		if err != nil {
			return fmt.Errorf("pg drop publication err: %v", err)
		}
	} else {
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			ALTER PUBLICATION %s ADD TABLE %s;
			`, slotName, opt.TableName)).ReadAll()
		if err != nil {
			return fmt.Errorf("alter publication err: %v", err)
		}
	}

	cmt, err := conn.Prepare(ctx,
		opt.TableName,
		fmt.Sprintf(`SELECT * FROM %s LIMIT 1;`, opt.TableName),
		nil,
	)
	if err != nil {
		return fmt.Errorf("pg prepare err: %v", err)
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
	err = db.Exec(fmt.Sprintf("CREATE TABLE %s (\n%s\n);", opt.tableName(), strings.Join(create, ",\n")))
	if err != nil {
		return fmt.Errorf("sqlite create table err: %v", err)
	}
	for i := range create {
		create[i] = "?"
	}

	t.insert, err = db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s);", opt.tableName(), strings.Join(create, ", ")))
	if err != nil {
		return fmt.Errorf("sqlite prepare err: %v", err)
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
			return fmt.Errorf("copy err: %v", err)
		}
	}

	return nil
}
