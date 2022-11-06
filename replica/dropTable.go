package replica

import (
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func TableDrop(name string) error {
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

	_, err = conn.Exec(ctx, "ALTER PUBLICATION "+slotName+" DROP TABLE "+name).ReadAll()
	if err != nil {
		return err
	}
	err = db.Exec("DROP TABLE IF EXISTS " + strings.ReplaceAll(name, ".", "_") + ";")
	return err
}
