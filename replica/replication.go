package replica

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

var r replication

func New(pgURL string, msgChan chan *Row) error {
	u, err := url.Parse(pgURL)
	if err != nil {
		return err
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("replication", "database")
	param.Add("application_name", "pgcache_slot")
	u.RawQuery = param.Encode()

	r.ch = msgChan
	r.mi = pgtype.NewMap()
	r.pgURL = pgURL
	r.relations = make(map[uint32]*pglogrepl.RelationMessage)

	r.conn, err = pgconn.Connect(ctx, u.String())
	if err != nil {
		return err
	}
	return r.createPublication()
}

func Start() error {
	return r.run()
}

func (r *replication) run() error {

	err := r.setLsn()
	if err != nil {
		return err
	}

	err = r.createSlot()
	if err != nil {
		return err
	}

	err = r.startReplication()
	if err != nil {
		return err
	}
	timeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(timeout)
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(
				context.Background(),
				r.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.lsn,
				},
			)

			if err != nil {
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
			return err
		}

		if rawMsg == nil {
			return fmt.Errorf("replication failed: nil message received, should not happen")
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
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

func (r *replication) readWithTimeout(timeout time.Duration) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(timeout))
	defer cancel()
	rawMsg, err := r.conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, os.ErrDeadlineExceeded
		}
		return nil, err
	}
	return rawMsg, err
}

func (r *replication) createPublication() error {
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", slotName)
	_, err := r.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return err
	}

	sql = fmt.Sprintf("CREATE PUBLICATION %s;", slotName)
	_, err = r.conn.Exec(ctx, sql).ReadAll()
	return err
}

func (r *replication) setLsn() error {
	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return err
	}
	r.lsn = sysident.XLogPos
	return nil
}

func (r *replication) createSlot() error {
	_, err := pglogrepl.CreateReplicationSlot(ctx,
		r.conn,
		slotName,
		plugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: true,
		},
	)
	return err
}

func (r *replication) startReplication() error {
	return pglogrepl.StartReplication(ctx,
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
