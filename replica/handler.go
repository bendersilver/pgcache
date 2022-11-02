package replica

import (
	"fmt"
	"strings"

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

	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		r.relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessage:
		err = r.insert(msg)
		if err != nil {
			glog.Error(err)
		}

	case *pglogrepl.UpdateMessage:
		err = r.update(msg)
		if err != nil {
			glog.Error(err)
		}
	case *pglogrepl.DeleteMessage:
		err = r.delete(msg)
		if err != nil {
			glog.Error(err)
		}

	case *pglogrepl.TruncateMessage:
		err = r.deleteAll(msg)
		if err != nil {
			glog.Error(err)
		}

	case *pglogrepl.BeginMessage:
	case *pglogrepl.CommitMessage:
	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:
	}
	return nil
}
func (r *replication) update(msg *pglogrepl.UpdateMessage) error {
	var pkVal any
	var pkName string
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}
	if msg.OldTuple != nil {
		tuple, err := r.decodeTuple(msg.RelationID, msg.OldTuple)
		if err != nil {
			return err
		}
		for ix, col := range rel.Columns {
			if col.Flags == 1 {
				pkVal = tuple[ix]
			}
		}
	}
	if msg.NewTuple != nil {
		tuple, err := r.decodeTuple(msg.RelationID, msg.NewTuple)
		if err != nil {
			return err
		}
		rel := r.relations[msg.RelationID]
		names := make([]string, len(tuple))
		for ix, col := range rel.Columns {
			if col.Flags == 1 {
				pkName = col.Name
				if pkVal == nil {
					pkVal = tuple[ix]
				}
			}
			names[ix] = col.Name + " = ?"
		}
		sql := fmt.Sprintf("UPDATE %s_%s SET %s WHERE %s = ?;",
			rel.Namespace,
			rel.RelationName,
			strings.Join(names, ",\n"),
			pkName,
		)
		_, err = r.db.Exec(sql, append(tuple, pkVal)...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *replication) deleteAll(msg *pglogrepl.TruncateMessage) error {
	rel := r.relations[msg.RelationNum]
	sql := fmt.Sprintf("DELETE FROM %s_%s;",
		rel.Namespace,
		rel.RelationName,
	)
	_, err := r.db.Exec(sql)
	return err
}

func (r *replication) delete(msg *pglogrepl.DeleteMessage) error {
	if msg.OldTuple != nil {
		tuple, err := r.decodeTuple(msg.RelationID, msg.OldTuple)
		if err != nil {
			return err
		}
		rel := r.relations[msg.RelationID]
		var pk string
		for ix, col := range rel.Columns {
			if col.Flags == 1 {
				pk = col.Name
			}
			tuple = []any{tuple[ix]}
		}
		sql := fmt.Sprintf("DELETE FROM %s_%s WHERE %s = ?;",
			rel.Namespace,
			rel.RelationName,
			pk,
		)
		_, err = r.db.Exec(sql, tuple...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *replication) insert(msg *pglogrepl.InsertMessage) error {
	if msg.Tuple != nil {

		tuple, err := r.decodeTuple(msg.RelationID, msg.Tuple)
		if err != nil {
			return err
		}
		rel := r.relations[msg.RelationID]
		names := make([]string, len(tuple))
		params := make([]string, len(tuple))
		for ix, col := range rel.Columns {
			names[ix] = col.Name
			params[ix] = "?"
		}
		sql := fmt.Sprintf("INSERT INTO %s_%s(%s) VALUES (%s);",
			rel.Namespace,
			rel.RelationName,
			strings.Join(names, " ,"),
			strings.Join(params, " ,"),
		)
		_, err = r.db.Exec(sql, tuple...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replication) decodeTuple(relID uint32, tuple *pglogrepl.TupleData) (vals []any, err error) {
	cols := r.relations[relID].Columns
	vals = make([]any, len(cols))
	for ix, col := range tuple.Columns {
		rc := cols[ix]
		dt, ok := mi.TypeForOID(rc.DataType)

		switch col.DataType {
		case 'n':
			vals[ix] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			if ok {

				vals[ix], err = dt.Codec.DecodeValue(mi, rc.DataType, pgtype.TextFormatCode, col.Data)
				if err != nil {
					glog.Error(err)
					return nil, err
				}
				vals[ix], err = converDataType(vals[ix], dt.Name)
				if err != nil {
					glog.Error(err)
					return nil, err
				}
			} else {
				vals[ix], err = converDataType(col.Data, "bytea")
				if err != nil {
					glog.Error(err)
					return nil, err
				}
			}
		}
	}
	return
}
