package replica

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pgx/v5/pgtype"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

func (c *Conn) createTableRule() error {
	err := c.conn.Exec(ctx, fmt.Sprintf(`
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

	return c.conn.Exec(ctx, fmt.Sprintf(`
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

	res, err := c.conn.Exec(ctx,
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

func (c *Conn) copyTable(sheme, table, sql string) error {
	err := c.conn.Exec(ctx, fmt.Sprintf(`
			ALTER PUBLICATION %s ADD TABLE %s.%s;
			`, c.opt.SlotName, sheme, table)).Close()

	if err != nil {
		return fmt.Errorf("alter publication err: %v", err)
	}

	var t relTable
	t.pgName = fmt.Sprintf("%s.%s", sheme, table)
	t.sqliteName = fmt.Sprintf("%s_%s", sheme, table)

	res, err := c.conn.Exec(ctx, fmt.Sprintf(`
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
				f.typeOID = uint32(u64)
			}

			f.isPrimary, err = strconv.ParseBool(string(v[4]))
			if err != nil {
				glog.Error(err)
			}
			if f.isPrimary {
				pk = append(pk, f.name)
			}
			switch f.typeOID {
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
	glog.Notice(strings.Trim(strings.Repeat("?, ", t.columnNum), ", "))
	return nil
	res, err = c.conn.Exec(ctx, fmt.Sprintf(`
			SELECT attname, atttypid
			FROM   pg_attribute
			WHERE  attrelid = '%s.%s'::regclass
				AND    attnum > 0
				AND    NOT attisdropped
			ORDER  BY attnum;
		`, sheme, table)).ReadAll()
	if err != nil {
		return fmt.Errorf("pg prepare err: %v", err)
	}
	if len(res) > 0 {
		// glog.Notice(res[0].FieldDescriptions)
		for _, v := range res[0].Rows {
			glog.Debugf("%s", v[1])
		}
	}
	return nil

	res, err = c.conn.Exec(ctx, fmt.Sprintf(`
		SELECT E'CREATE TABLE %[1]s_%[2]s(\n' || string_agg(column_name || ' ' ||
			CASE
				WHEN udt_name = 'bool' THEN 'BOOLEAN'
				WHEN udt_name IN ('int2', 'int4', 'int8', 'timestamp', 'timestamptz', 'date') THEN 'INTEGER'
				WHEN udt_name IN ('numeric', 'float4', 'float8') THEN 'REAL'
				WHEN udt_name IN ('text', 'varchar') THEN 'TEXT'
				ELSE 'BLOB'
			END, E',\n') || E',\nPRIMARY KEY (%[3]s)\n);'
		FROM information_schema.columns
		WHERE table_name = '%[2]s'
			AND table_schema = '%[1]s';
	`, sheme, table, pk)).ReadAll()
	if err != nil {
		return fmt.Errorf("pg prepare err: %v", err)
	}

	if len(res) > 0 {
		err = c.db.Exec(string(res[0].Rows[0][0]))
		if err != nil {
			return err
		}
	}

	if sql == "*" {
		sql = fmt.Sprintf("SELECT * FROM %s.%s", sheme, table)
	}

	res, err = c.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return fmt.Errorf("pg prepare err: %v", err)
	}
	if len(res) > 0 {
		glog.Notice(res[0].FieldDescriptions)
		for _, v := range res[0].Rows {
			glog.Debugf("%s", v)
		}
	}

	return nil

}

func parseInt(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}

type relTable struct {
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
	tableOID  uint32
	attrNum   uint16
	typeOID   uint32
	isPrimary bool
}
