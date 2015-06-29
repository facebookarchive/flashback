package flashback

import (
	"errors"
	"gopkg.in/mgo.v2"
	"time"
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
		ntoreturn := safeGetInt(content["ntoreturn"])
		query.Limit(ntoreturn)
	}
	if content["ntoskip"] != nil {
		ntoskip := safeGetInt(content["ntoskip"])
		query.Skip(ntoskip)
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
	change := mgo.Change{Update: content["update"].(map[string]interface{})}
	_, err := coll.Find(content["query"]).Apply(change, result)
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

	cmd := op.Content["command"].(map[string]interface{})

	for _, name := range []string{"findandmodify", "count"} {
		collName, exist := cmd[name]
		if !exist {
			continue
		}

		op.Type = OpType("command." + name)
		op.Collection = collName.(string)
		op.Content = cmd

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

func safeGetInt(i interface{}) int {
	switch i.(type) {
	case int32:
		return int(i.(int32))
	case int64:
		return int(i.(int64))
	case float32:
		return int(i.(float32))
	case float64:
		return int(i.(float64))
	default:
		return int(0)
	}
}
