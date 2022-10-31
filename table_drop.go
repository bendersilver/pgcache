package pgcache

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// DropTable -
func (pc *PgCache) DropTable(name string) error {
	conn, err := pgconn.Connect(ctx, pc.pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION pgcache_slot DROP TABLE %s", name)).ReadAll()
	if err != nil {
		return err
	}
	_, err = pc.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", strings.ReplaceAll(name, ".", "_")))
	if err != nil {
		return err
	}
	if pc.tables[name] != nil {
		if pc.tables[name].insert != nil {
			err = pc.tables[name].insert.Close()
			if err != nil {
				return err
			}
		}
		if pc.tables[name].update != nil {
			err = pc.tables[name].update.Close()
			if err != nil {
				return err
			}
		}
		if pc.tables[name].delete != nil {
			err = pc.tables[name].delete.Close()
			if err != nil {
				return err
			}
		}
		if pc.tables[name].truncate != nil {
			err = pc.tables[name].truncate.Close()
			if err != nil {
				return err
			}
		}
		if pc.tables[name].selectPK != nil {
			err = pc.tables[name].selectPK.Close()
			if err != nil {
				return err
			}
		}
		delete(pc.tables, name)
	}
	return err
}
