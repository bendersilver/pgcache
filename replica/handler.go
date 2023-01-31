package replica

import (
	"database/sql/driver"
	"fmt"

	"github.com/bendersilver/glog"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

func (c *Conn) handle(m *pgproto3.CopyData) error {

	xld, err := pglogrepl.ParseXLogData(m.Data[1:])
	if err != nil {
		glog.Error(err)
		return err
	}
	c.lsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		glog.Error(err)
		return err
	}

	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		if rel, ok := c.relations[msg.RelationID]; ok {
			var restore bool
			if rel.pgName != fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName) {
				restore = true
			} else if len(rel.field) != len(msg.Columns) {
				restore = true
			}
			for i := 0; i < int(msg.ColumnNum); i++ {
				if msg.Columns[i].Name != rel.field[i].name {
					restore = true
					break
				} else if msg.Columns[i].DataType != rel.field[i].oid {
					restore = true
					break
				} else if msg.Columns[i].Flags == 1 && !rel.field[i].isPrimary {
					restore = true
					break
				}
			}
			if restore {
				err = c.dropPub(nil, rel)
				if err != nil {
					glog.Error(err)
				}
				err = c.alterPub(msg.Namespace, msg.RelationName)
				if err != nil {
					glog.Error(err)
				}
			}

		} else {
			glog.Errorf("new table %+v", msg)
		}

	case *pglogrepl.InsertMessage:
		ok, err := c.rowAction(msg.RelationID, msg.Tuple, false)
		if err != nil {
			glog.Error(err)
		} else if ok {
			glog.Noticef("add table %s.%s", msg.Tuple.Columns[0].Data, msg.Tuple.Columns[1].Data)
			err = c.alterPub(string(msg.Tuple.Columns[0].Data), string(msg.Tuple.Columns[1].Data))
			if err != nil {
				glog.Error(err)
			}
		}

	case *pglogrepl.UpdateMessage:
		_, err = c.rowAction(msg.RelationID, msg.OldTuple, true)
		if err != nil {
			glog.Error(err)
		}

		_, err := c.rowAction(msg.RelationID, msg.NewTuple, false)
		if err != nil {
			glog.Error(err)
		}

	case *pglogrepl.DeleteMessage:
		ok, err := c.rowAction(msg.RelationID, msg.OldTuple, true)
		if err != nil {
			glog.Error(err)
		} else if ok {
			pgName := fmt.Sprintf("%s.%s", msg.OldTuple.Columns[0].Data, msg.OldTuple.Columns[1].Data)
			glog.Noticef("drop table %s", pgName)
			for _, t := range c.relations {
				if t.pgName == pgName {
					err = c.dropPub(nil, t)
					if err != nil {
						glog.Error(err)
					}
					break
				}
			}
		}

	case *pglogrepl.TruncateMessage:
		for _, relID := range msg.RelationIDs {
			if rel, ok := c.relations[relID]; ok {
				err = c.db.Exec(fmt.Sprintf("DELETE FROM %s;", rel.sqliteName))
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

func (c *Conn) getRelTable(pgName string) *relTable {
	for _, t := range c.relations {
		if t.pgName == pgName {
			return t
		}
	}
	return nil
}

func (c *Conn) rowAction(relID uint32, pgTuple *pglogrepl.TupleData, del bool) (bool, error) {
	if pgTuple == nil {
		return false, nil
	}
	if rel, ok := c.relations[relID]; ok {
		ruleTable := rel.pgName == c.opt.TableName
		tuple, err := rel.decodeTuple(pgTuple)
		if err != nil {
			return false, err
		}
		if del {
			var args []driver.Value
			for i, f := range rel.field {
				if f.isPrimary {
					args = append(args, tuple[i])
				}
			}
			return ruleTable, rel.delete.Exec(args...)
		}
		if ruleTable {
			if tuple[3] == nil {
				rel.cleanSQL = ""
			} else {
				rel.cleanSQL = fmt.Sprintf("%s", pgTuple.Columns[3].Data)
				i, err := pgTuple.Columns[4].Int64()
				if err != nil {
					glog.Error(err)
					rel.cleanTimeout = 60
				} else {
					rel.cleanTimeout = uint32(i)
				}
			}
		}
		return ruleTable, rel.insert.Exec(tuple...)
	}
	return false, nil
}
