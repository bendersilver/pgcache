package replica

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/bendersilver/glog"
	"github.com/bendersilver/pgcache/sqlite"
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
		r.relations[msg.RelationID], err = newRrelationItem(msg)
		if err != nil {
			return err
		}

	case *pglogrepl.InsertMessage:
		rel, ok := r.relations[msg.RelationID]
		if !ok {
			glog.Error("pglogrepl.InsertMessage.id %d not found", msg.RelationID)
		} else {
			err = rel.insertMsg(msg)
			if err != nil {
				glog.Error(err)
			}
		}

	case *pglogrepl.UpdateMessage:
		rel, ok := r.relations[msg.RelationID]
		if !ok {
			glog.Error("pglogrepl.UpdateMessage.id %d not found", msg.RelationID)
		} else {
			err = rel.updateMsg(msg)
			if err != nil {
				glog.Error(err)
			}
		}

	case *pglogrepl.DeleteMessage:
		rel, ok := r.relations[msg.RelationID]
		if !ok {
			glog.Error("pglogrepl.DeleteMessage.id %d not found", msg.RelationID)
		} else {
			err = rel.deleteMsg(msg)
			if err != nil {
				glog.Error(err)
			}
		}

	case *pglogrepl.TruncateMessage:
		for _, relID := range msg.RelationIDs {
			rel, ok := r.relations[relID]
			if !ok {
				glog.Error("pglogrepl.TruncateMessage.id %d not found", relID)
			} else {
				err = rel.deleteAll()
				if err != nil {
					glog.Error(err)
				}
			}
		}

	case *pglogrepl.BeginMessage:
	case *pglogrepl.CommitMessage:
	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:
	}
	return nil
}

type relationItem struct {
	msg       *pglogrepl.RelationMessage
	tableName string
	pkIx      int
	insert    *sqlite.Stmt
	update    *sqlite.Stmt
	delete    *sqlite.Stmt
	truncate  *sqlite.Stmt
}

func newRrelationItem(m *pglogrepl.RelationMessage) (ri *relationItem, err error) {
	ri = new(relationItem)
	ri.msg = m
	ri.tableName = fmt.Sprintf("%s_%s", m.Namespace, m.RelationName)
	names := make([]string, len(m.Columns))
	params := make([]string, len(m.Columns))
	for i, c := range m.Columns {
		if c.Flags == 1 {
			ri.pkIx = i
		}
		names[i] = c.Name
		params[i] = "?"
	}
	sql := fmt.Sprintf("INSERT OR IGNORE INTO %s(%s) VALUES (%s);",
		ri.tableName,
		strings.Join(names, " ,"),
		strings.Join(params, " ,"),
	)
	ri.insert, err = db.Prepare(sql)
	if err != nil {
		glog.Error(err)
		return
	}

	sql = fmt.Sprintf("DELETE FROM %s WHERE %s = ?;",
		ri.tableName,
		m.Columns[ri.pkIx].Name,
	)
	ri.delete, err = db.Prepare(sql)
	if err != nil {
		glog.Error(err)
		return
	}

	ri.truncate, err = db.Prepare(fmt.Sprintf("DELETE FROM %s;", ri.tableName))
	if err != nil {
		glog.Error(err)
		return
	}

	for i, v := range names {
		params[i] = v + " = ?"
	}
	sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?;",
		ri.tableName,
		strings.Join(params, " ,"),
		m.Columns[ri.pkIx].Name,
	)
	ri.update, err = db.Prepare(sql)
	return
}

func (ri *relationItem) updateMsg(msg *pglogrepl.UpdateMessage) error {
	var pk driver.Value
	if msg.OldTuple != nil {
		tuple, err := ri.decodeTuple(msg.OldTuple)
		if err != nil {
			return err
		}
		for i, c := range ri.msg.Columns {
			if c.Flags == 1 {
				pk = tuple[i]
				break
			}
		}
	}
	if msg.NewTuple != nil {
		tuple, err := ri.decodeTuple(msg.NewTuple)
		if err != nil {
			return err
		}
		if pk == nil {
			pk = tuple[ri.pkIx]
		}
		tuple = append(tuple, pk)
		return ri.update.Exec(tuple...)
	}

	return nil
}

func (ri *relationItem) deleteAll() error {
	return ri.truncate.Exec()
}

func (ri *relationItem) deleteMsg(msg *pglogrepl.DeleteMessage) error {
	if msg.OldTuple != nil {
		tuple, err := ri.decodeTuple(msg.OldTuple)
		if err != nil {
			return err
		}
		return ri.delete.Exec(tuple[ri.pkIx])
	}

	return nil
}

func (ri *relationItem) insertMsg(msg *pglogrepl.InsertMessage) error {
	if msg.Tuple != nil {

		tuple, err := ri.decodeTuple(msg.Tuple)
		if err != nil {
			return err
		}
		return ri.insert.Exec(tuple...)
	}
	return nil
}

func (ri *relationItem) decodeTuple(tuple *pglogrepl.TupleData) (vals []driver.Value, err error) {
	cols := ri.msg.Columns
	vals = make([]driver.Value, len(cols))
	for ix, col := range tuple.Columns {
		rc := cols[ix]
		switch col.DataType {
		case 'n':
			vals[ix] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			vals[ix], err = decodeColumn(pgtype.TextFormatCode, rc.DataType, col.Data)
			if err != nil {
				glog.Error(err)
				return nil, err
			}
		}
	}
	return
}
