package flashback

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	NotSupported = errors.New("op type not supported")
)

type execute func(content Document, collection *mgo.Collection) error

type OpsExecutor struct {
	session   *mgo.Session
	statsChan chan OpStat
	logger    *Logger

	// keep track of the results retrieved by find(). For verification purpose
	// only.
	lastResult  interface{}
	lastLatency time.Duration
	subExecutes map[OpType]execute
}

func NewOpsExecutor(session *mgo.Session, statsChan chan OpStat, logger *Logger) *OpsExecutor {
	e := &OpsExecutor{
		session:   session,
		statsChan: statsChan,
		logger:    logger,
	}

	e.subExecutes = map[OpType]execute{
		Query:         e.execQuery,
		Insert:        e.execInsert,
		Update:        e.execUpdate,
		Remove:        e.execRemove,
		Count:         e.execCount,
		FindAndModify: e.execFindAndModify,
	}
	return e
}

func (e *OpsExecutor) execQuery(
	content Document, coll *mgo.Collection) error {
	query := coll.Find(content["query"])
	result := []Document{}
	if content["ntoreturn"] != nil {
		if ntoreturn, err := safeGetInt(content["ntoreturn"]); err != nil {
			e.logger.Error("could not set ntoreturn: ", err)
		} else {
			query.Limit(ntoreturn)
		}
	}
	if content["ntoskip"] != nil {
		if ntoskip, err := safeGetInt(content["ntoskip"]); err != nil {
			e.logger.Error("could not set ntoskip: ", err)
		} else {
			query.Skip(ntoskip)
		}
	}
	err := query.All(&result)
	e.lastResult = &result
	return err
}

func (e *OpsExecutor) execInsert(content Document, coll *mgo.Collection) error {
	return coll.Insert(content["o"])
}

func (e *OpsExecutor) execUpdate(content Document, coll *mgo.Collection) error {
	return coll.Update(content["query"], content["updateobj"])
}

func (e *OpsExecutor) execRemove(content Document, coll *mgo.Collection) error {
	return coll.Remove(content["query"])
}

func (e *OpsExecutor) execCount(content Document, coll *mgo.Collection) error {
	_, err := coll.Count()
	return err
}

func (e *OpsExecutor) execFindAndModify(content Document, coll *mgo.Collection) error {
	result := Document{}
	var query, update bson.D

	// Maybe clean this up later using a struct
	if commandDoc, ok := content["command"].(bson.D); ok {
		if value, ok := GetElem(commandDoc, "query"); ok {
			if query, ok = value.(bson.D); !ok {
				return fmt.Errorf("bad query document in findAndModify operation")
			}
		} else {
			return fmt.Errorf("missing query document in findAndModify operation")
		}
		if value, ok := GetElem(commandDoc, "update"); ok {
			if update, ok = value.(bson.D); !ok {
				return fmt.Errorf("bad update document in findAndModify operation")
			}
		} else {
			return fmt.Errorf("missing update document in findAndModify operation")
		}
	} else {
		fmt.Errorf("bad command document in findAndModify operation")
	}

	change := mgo.Change{Update: update}
	_, err := coll.Find(query).Apply(change, result)
	return err
}

// We only support handful op types. This function helps us to process supported
// ops in a universal way.
//
// We do not canonicalize the ops in OpsReader because we hope ops reader to do
// its job honestly and the consumer of these ops decide how to further process
// the original ops.
func CanonicalizeOp(op *Op) *Op {
	if op.Type != Command {
		return op
	}

	// the command to be run is the first element in the command document
	// TODO: these unprotected type assertions aren't great, but one problem at at time
	cmd := op.Content["command"].(bson.D)[0]

	if cmd.Name == "count" || cmd.Name == "findandmodify" {
		collName := cmd.Value.(string)

		op.Type = OpType("command." + cmd.Name)
		op.Collection = collName
		//	op.Content = cmd
		return op
	}

	return nil
}

func retryOnSocketFailure(block func() error, session *mgo.Session, logger *Logger) error {
	err := block()
	if err == nil {
		return nil
	}

	switch err.(type) {
	case *mgo.QueryError, *mgo.LastError:
		return err
	}

	switch err {
	case mgo.ErrNotFound, NotSupported:
		return err
	}

	// Otherwise it's probably a socket error so we refresh the connection,
	// and try again
	session.Refresh()
	logger.Error("retrying mongo query after error: ", err)
	return block()
}

func (e *OpsExecutor) Execute(op *Op) error {
	startOp := time.Now()

	block := func() error {
		content := op.Content
		coll := e.session.DB(op.Database).C(op.Collection)
		return e.subExecutes[op.Type](content, coll)
	}
	err := retryOnSocketFailure(block, e.session, e.logger)

	latencyOp := time.Now().Sub(startOp)
	e.lastLatency = latencyOp

	if e.statsChan != nil {
		if err == nil {
			e.statsChan <- OpStat{op.Type, latencyOp, false}
		} else {
			// error condition
			e.statsChan <- OpStat{op.Type, latencyOp, true}
		}
	}

	return err
}

func (e *OpsExecutor) LastLatency() time.Duration {
	return e.lastLatency
}

func safeGetInt(i interface{}) (int, error) {
	switch i.(type) {
	case int32:
		return int(i.(int32)), nil
	case int64:
		return int(i.(int64)), nil
	case float32:
		return int(i.(float32)), nil
	case float64:
		return int(i.(float64)), nil
	default:
		return int(0), fmt.Errorf("unsupported type for %i", i)
	}
}
