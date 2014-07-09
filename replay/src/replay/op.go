package replay

import (
	"time"
)

type Document map[string]interface{}
type OpType string

const (
	Insert  OpType = "insert"
	Update  OpType = "update"
	Remove  OpType = "remove"
	Query   OpType = "query"
	Command OpType = "command"
)

var AllOpTypes []OpType = []OpType{
	Insert,
	Update,
	Remove,
	Query,
	Command,
}

// Op represents a MongoDB operation that contains enough details to be
// replayed.
type Op struct {
	Database   string
	Collection string
	Type       OpType

	// indicates when this op was performed
	Timestamp time.Time
	// The details of this op, which may vary from different op types.
	Content Document
}
