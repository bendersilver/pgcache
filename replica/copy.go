package replica

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/jackc/pgx/v5/pgconn"
)

func (c *Conn) newConn() (*pgconn.PgConn, error) {
	u, err := url.Parse(c.opt.PgURL)
	if err != nil {
		glog.Fatal(err)
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	u.RawQuery = param.Encode()
	return pgconn.Connect(ctx, u.String())
}

func (c *Conn) createTableRule(conn *pgconn.PgConn) error {
	err := conn.Exec(ctx, fmt.Sprintf(`
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

	return conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (sheme_name, table_name, initsql)
		VALUES ('%s', '%s', '*')
		ON CONFLICT (sheme_name, table_name) DO NOTHING;
	`, c.opt.TableName, args[0], args[1])).Close()
}

func (c *Conn) copyAllTable() error {
	conn, err := c.newConn()
	if err != nil {
		return err
	}
	err = c.createTableRule(conn)
	if err != nil {
		return err
	}

	res, err := conn.Exec(ctx,
		fmt.Sprintf(`SELECT sheme_name, table_name
		FROM %s`, c.opt.TableName,
		),
	).ReadAll()
	if err != nil {
		return err
	}
	conn.Close(ctx)

	if len(res) > 0 {
		for _, row := range res[0].Rows {
			err = c.alterPub(string(row[0]), string(row[1]))
			if err != nil {
				glog.Warning(err)
			}
		}
	}

	return nil

}
