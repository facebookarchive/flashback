package replay

import (
	"errors"
	"labix.org/v2/mgo"
)

var (
	NotSupported = errors.New("op type not supported")
)

type execute func(content Document, collection *mgo.Collection) error

type OpsExecutor struct {
	session        *mgo.Session
	statsCollector IStatsCollector

	// keep track of the results retrieved by find(). For verification purpose
	// only.
	lastResult  interface{}
	subExecutes map[OpType]execute
}

func OpsExecutorWithStats(session *mgo.Session,
	statsCollector IStatsCollector) *OpsExecutor {
	e := &OpsExecutor{
		session:        session,
		statsCollector: statsCollector,
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

func NewOpsExecutor(session *mgo.Session) *OpsExecutor {
	return OpsExecutorWithStats(session, new(NullStatsCollector))
}

func (e *OpsExecutor) execQuery(
	content Document, coll *mgo.Collection) error {
	query := coll.Find(content["query"])
	result := []Document{}
	if content["ntoreturn"] != nil {
		ntoreturn := int(content["ntoreturn"].(float64))
		query.Limit(ntoreturn)
	}
	if content["ntoskip"] != nil {
		ntoskip := int(content["ntoskip"].(float64))
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
	var result Document
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
func canonicalizeOp(op *Op) *Op {
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

func (e *OpsExecutor) Execute(op *Op) error {
	op = canonicalizeOp(op)
	if op == nil {
		return NotSupported
	}

	e.statsCollector.StartOp(op.Type)
	defer e.statsCollector.EndOp()

	content := op.Content
	coll := e.session.DB(op.Database).C(op.Collection)

	return e.subExecutes[op.Type](content, coll)
}
