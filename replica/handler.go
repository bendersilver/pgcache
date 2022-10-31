package replica

import (
	"fmt"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *replication) handle(m *pgproto3.CopyData) error {
	xld, err := pglogrepl.ParseXLogData(m.Data[1:])
	if err != nil {
		glog.Error(err)
		return err
	}
	r.lsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		glog.Error(err)
		return err
	}
	var row Row
	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		row.Type = Relation
		r.relations[msg.RelationID] = msg
	case *pglogrepl.InsertMessage:
		row.Type = Insert
		err := r.msgData(&row, msg.RelationID, msg.Tuple, nil)
		if err != nil {
			glog.Error(err)
			return err
		}
	case *pglogrepl.UpdateMessage:
		row.Type = Update
		err := r.msgData(&row, msg.RelationID, msg.NewTuple, msg.OldTuple)
		if err != nil {
			glog.Error(err)
			return err
		}
	case *pglogrepl.DeleteMessage:
		row.Type = Delete
		err := r.msgData(&row, msg.RelationID, nil, msg.OldTuple)
		if err != nil {
			glog.Error(err)
			return err
		}
	case *pglogrepl.TruncateMessage:
		row.Type = Truncate
		err := r.msgData(&row, msg.RelationNum, nil, nil)
		if err != nil {
			glog.Error(err)
			return err
		}
	case *pglogrepl.BeginMessage:
		row.Type = Begin
	case *pglogrepl.CommitMessage:
		row.Type = Commit
	case *pglogrepl.TypeMessage:
		row.Type = Type
	case *pglogrepl.OriginMessage:
		row.Type = Origin

	}
	r.ch <- &row
	return nil
}

func (r *replication) msgData(row *Row, relID uint32, newTuple, oldTuple *pglogrepl.TupleData) error {
	rel, ok := r.relations[relID]
	if !ok {
		return fmt.Errorf("replication unknown relation ID %d", relID)
	}
	row.Shema = rel.Namespace
	row.Table = rel.RelationName
	if newTuple != nil {
		row.New = make([]*Col, newTuple.ColumnNum)
		r.decodeTuple(row.New, rel, newTuple)
	}
	if oldTuple != nil {
		row.Old = make([]*Col, oldTuple.ColumnNum)
		r.decodeTuple(row.Old, rel, oldTuple)
	}
	return nil
}

func (r *replication) decodeTuple(vals []*Col, rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (err error) {
	for ix, col := range tuple.Columns {
		rc := rel.Columns[ix]
		dt, ok := r.mi.TypeForOID(rc.DataType)

		var c Col
		c.Name = rc.Name
		c.UDT = "bytea"
		if ok {
			c.UDT = dt.Name
		}
		c.PK = rc.Flags == 1

		switch col.DataType {
		case 'n': // null
			c.Value = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			if ok {
				c.Value, err = dt.Codec.DecodeValue(r.mi, rc.DataType, pgtype.TextFormatCode, col.Data)
				if err != nil {
					glog.Error(err)
					return err
				}
			} else {
				c.Value = col.Data
			}
		}
		vals[ix] = &c
	}
	return nil
}
