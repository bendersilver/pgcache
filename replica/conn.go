package replica

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type Options struct {
	PgURL     string
	SlotName  string
	TableName string
}

type Conn struct {
	db *sqlite.Conn

	opt       *Options
	conn      *pgconn.PgConn
	lsn       pglogrepl.LSN
	relations map[uint32]*relationItem
}

func NewConn(opt *Options) (*Conn, error) {
	if opt.PgURL == "" {
		return nil, fmt.Errorf("PgURL not set")
	} else if opt.SlotName == "" {
		return nil, fmt.Errorf("SlotName not set")
	} else if opt.TableName == "" {
		return nil, fmt.Errorf("TableName not set")
	}
	u, err := url.Parse(opt.PgURL)
	if err != nil {
		glog.Fatal(err)
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("replication", "database")
	param.Add("application_name", opt.SlotName)
	u.RawQuery = param.Encode()

	var c Conn
	c.opt = opt
	c.conn, err = pgconn.Connect(context.Background(), u.String())
	if err != nil {
		return nil, err
	}

	c.db, err = sqlite.NewConn()
	if err != nil {
		return nil, err
	}

	err = c.createTableRule()
	if err != nil {
		return nil, err
	}

	err = c.drop()
	// if err != nil {
	// 	return nil, err
	// }

	err = c.start()
	if err != nil {
		return nil, err
	}

	err = c.copy()
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Conn) drop() error {
	c.dropSlot()
	return c.dropPublication()
}

func (c *Conn) dropPublication() error {
	return c.conn.Exec(ctx,
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", c.opt.SlotName),
	).Close()
}

func (c *Conn) dropSlot() {
	err := pglogrepl.DropReplicationSlot(ctx,
		c.conn,
		c.opt.SlotName,
		pglogrepl.DropReplicationSlotOptions{},
	)
	if err != nil {
		glog.Warning(err)
	}

}

func (c *Conn) start() error {
	err := c.createPublication()
	if err != nil {
		return err
	}
	return c.createSlot()
}

func (c *Conn) createPublication() error {
	return c.conn.Exec(ctx,
		fmt.Sprintf("CREATE PUBLICATION %s;", c.opt.SlotName),
	).Close()
}

func (c *Conn) createSlot() error {
	_, err := pglogrepl.CreateReplicationSlot(ctx,
		c.conn,
		c.opt.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{},
	)
	return err
}

func (c *Conn) startReplication() error {
	return pglogrepl.StartReplication(ctx,
		c.conn,
		c.opt.SlotName,
		c.lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + c.opt.SlotName + "'",
			},
		},
	)
}

func (c *Conn) Close() error {
	err := c.drop()
	if err != nil {
		return err
	}
	err = c.conn.Close(ctx)
	if err != nil {
		return err
	}
	return c.db.Close()
}
