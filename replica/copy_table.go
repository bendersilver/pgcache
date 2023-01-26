package replica

import (
	"fmt"
	"strings"

	"github.com/bendersilver/glog"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

func (c *Conn) createTableRule() error {
	// err := c.db.Exec(fmt.Sprintf(`
	// 	CREATE TABLE IF NOT EXISTS %s
	// 	(
	// 		sheme_name VARCHAR(50) NOT NULL,
	// 		table_name VARCHAR(150) NOT NULL,
	// 		initsql TEXT,
	// 		cleansql TEXT,
	// 		cleantimeout INT
	// 	);
	// `, strings.ReplaceAll(c.opt.TableName, ".", "_")))
	// if err != nil {
	// 	return err
	// }
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
	c.createTableRule()
	res, err := c.conn.Exec(ctx,
		fmt.Sprintf(`SELECT sheme_name, table_name, initsql, cleansql, cleantimeout
		FROM %s`, c.opt.TableName,
		),
	).ReadAll()
	if err != nil {
		return err
	}

	err = c.conn.Exec(ctx, "SELECT * FROM pb._replica_rules;").Close()
	if err != nil {
		return fmt.Errorf("alter publication err: %v", err)
	}

	// if len(res) > 0 {
	// 	for _, row := range res[0].Rows {
	// 		glog.Noticef("%s", row)
	err = c.copyTable("", "", "")
	if err != nil {
		glog.Error(err)
	}
	// }
	// }

	return nil

}

func (c *Conn) copyTable(sheme, table, sql string) error {

	err := c.conn.Exec(ctx, fmt.Sprintf(`
			ALTER PUBLICATION %s ADD TABLE %s;
			`, c.opt.SlotName, c.opt.TableName)).Close()
	if err != nil {
		return fmt.Errorf("alter publication err: %v", err)
	}

	// var sqlTable = fmt.Sprintf("%s_%s", sheme, table)
	// cmt, err := c.conn.Prepare(ctx,
	// 	sqlTable,
	// 	fmt.Sprintf(`SELECT * FROM %s.%s LIMIT 1;`, sheme, table),
	// 	nil,
	// )
	// if err != nil {
	// 	return fmt.Errorf("pg prepare err: %v", err)
	// }
	// if cmt.Fields
	// glog.Notice(cmt.Fields)
	// проверить
	// 	существует ли таблица
	// 	первичный ключ
	// 	добавить в репликацию
	// 	скопировать данные.

	return nil

}
