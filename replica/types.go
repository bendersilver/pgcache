package replica

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	slotName = "pgcache_slot"
	plugin   = "pgoutput"
)

type MessageType string

const (
	Insert   MessageType = "Insert"
	Update               = "Update"
	Delete               = "Delete"
	Truncate             = "Truncate"
	Relation             = "Relation"
	Begin                = "Begin"
	Commit               = "Commit"
	Type                 = "Type"
	Origin               = "Origin"
)

var ctx = context.Background()

// replication -
type replication struct {
	pgURL string

	ch        chan *Row
	conn      *pgconn.PgConn
	lsn       pglogrepl.LSN
	relations map[uint32]*pglogrepl.RelationMessage
	mi        *pgtype.Map
}

type Col struct {
	Name  string
	UDT   string
	PK    bool
	Value interface{}
}

type Row struct {
	Type  MessageType
	Shema string
	Table string
	New   []*Col
	Old   []*Col
}
