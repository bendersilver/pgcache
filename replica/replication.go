package replica

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

var r replication

// New -
func New(pgURL string, msgChan chan *Row) (cancel context.CancelFunc, err error) {
	u, err := url.Parse(pgURL)
	if err != nil {
		glog.Error(err)
		return
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("replication", "database")
	param.Add("application_name", "pgcache_slot")
	u.RawQuery = param.Encode()

	r.ch = msgChan
	r.mi = pgtype.NewMap()
	r.pgURL = u.String()
	r.relations = make(map[uint32]*pglogrepl.RelationMessage)
	r.ctx, cancel = context.WithCancel(context.Background())

	err = r.reconnect()
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	return cancel, r.createPublication()
}

// Start -
func Start() error {
	return r.run()
}

func (r *replication) reconnect() (err error) {
	if r.conn == nil || r.conn.IsClosed() {
		r.conn, err = pgconn.Connect(r.ctx, r.pgURL)
	}
	return
}

func (r *replication) run() error {

	err := r.createSlot()
	if err != nil {
		glog.Warning(err)
	}
	defer r.close()

RECONN:
	r.lsn = pglogrepl.LSN(0)
	err = r.reconnect()
	if err != nil {
		glog.Error(err)
		return err
	}

	err = r.startReplication()
	if err != nil {
		glog.Error(err)
		return err
	}
	timeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(timeout)
	for {
		select {
		case <-r.ctx.Done():
			return nil
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(
				r.ctx,
				r.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.lsn,
				},
			)

			if err != nil {
				glog.Error(err)
				return err
			}
			nextStandbyMessageDeadline = time.Now().Add(timeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			glog.Error(err)
			time.Sleep(time.Second * 20)
			goto RECONN
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
			r.handle(msg)
		}
	}
}

func (r *replication) dropPublication() error {
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", slotName)
	_, err := r.conn.Exec(r.ctx, sql).ReadAll()
	return err
}

func (r *replication) createPublication() error {
	err := r.dropPublication()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("CREATE PUBLICATION %s;", slotName)
	_, err = r.conn.Exec(r.ctx, sql).ReadAll()
	return err
}

func (r *replication) createSlot() error {

	_, err := pglogrepl.CreateReplicationSlot(r.ctx,
		r.conn,
		slotName,
		plugin,
		pglogrepl.CreateReplicationSlotOptions{},
	)
	return err
}

func (r *replication) close() {
	err := pglogrepl.DropReplicationSlot(r.ctx, r.conn, slotName, pglogrepl.DropReplicationSlotOptions{})
	if err != nil {
		glog.Error(err)
	}
	err = r.dropPublication()
	if err != nil {
		glog.Error(err)
	}
	err = r.conn.Close(r.ctx)
	if err != nil {
		glog.Error(err)
	}
}

func (r *replication) startReplication() error {
	return pglogrepl.StartReplication(r.ctx,
		r.conn,
		slotName,
		r.lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names 'pgcache_slot'",
			},
		},
	)
}

func (r *replication) setLsn() error {
	sysident, err := pglogrepl.IdentifySystem(r.ctx, r.conn)
	if err != nil {
		glog.Error(err)
		return err
	}
	r.lsn = sysident.XLogPos
	return nil
}
