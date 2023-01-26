package replica

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

var ctx = context.Background()
var mi = pgtype.NewMap()

// Options -
type Options struct {
	PgURL     string
	SlotName  string
	TableName string
}

type sqliteTable struct {
	name     string
	cleanSQL string
	timeot   int
	pk       string
}

// Conn -
type Conn struct {
	ErrCH chan error
	db    *sqlite.Conn
	dbURL string

	opt       *Options
	conDB     *pgconn.PgConn
	conn      *pgconn.PgConn
	lsn       pglogrepl.LSN
	relations map[uint32]*relTable
}

// NewConn -
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
	c.ErrCH = make(chan error, 0)
	c.relations = make(map[uint32]*relTable)
	c.opt = opt
	c.conn, err = pgconn.Connect(context.Background(), u.String())
	if err != nil {
		return nil, err
	}
	param.Del("replication")
	c.conDB, err = pgconn.Connect(context.Background(), u.String())
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
	if err != nil {
		glog.Warning(err)
	}

	err = c.start()
	if err != nil {
		return nil, err
	}

	err = c.copy()
	if err != nil {
		return nil, err
	}

	go c.run()

	return &c, nil
}

func (c *Conn) run() error {
	c.lsn = pglogrepl.LSN(0)
	err := c.startReplication()
	if err != nil {
		return err
	}
	timeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(timeout)
	for {

		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				c.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: c.lsn,
				},
			)

			if err != nil {
				glog.Error(err)
				return err
			}
			nextStandbyMessageDeadline = time.Now().Add(timeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := c.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}

		if rawMsg == nil {
			return fmt.Errorf("replication failed: nil message received, should not happen")
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			glog.Critical(errMsg)
			// restart
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			glog.Warning("replication received unexpected message: %T\n", msg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				glog.Error(err)
				return err
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			c.handle(msg)
		}
	}
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

// Close -
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
