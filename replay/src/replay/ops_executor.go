package replay

import (
	"errors"
	"labix.org/v2/mgo"
)

type OpsExecutor struct {
	session        *mgo.Session
	statsCollector IStatsCollector
	// keep track of the results retrieved by find(). For verification purpose
	// only.
	lastResult interface{}
}

func OpsExecutorWithStats(session *mgo.Session,
	statsCollector IStatsCollector) *OpsExecutor {
	return &OpsExecutor{session: session, statsCollector: statsCollector}
}

func NewOpsExecutor(session *mgo.Session) *OpsExecutor {
	return OpsExecutorWithStats(session, new(NullStatsCollector))
}

func (executor *OpsExecutor) ExecQuery(
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
	executor.lastResult = &result
	return err
}

func (executor *OpsExecutor) ExecInsert(
	content Document, coll *mgo.Collection) error {
	return coll.Insert(content["o"])
}

func (executor *OpsExecutor) ExecUpdate(
	content Document, coll *mgo.Collection) error {
	return coll.Update(content["query"], content["updateobj"])
}

func (executor *OpsExecutor) ExecRemove(
	content Document, coll *mgo.Collection) error {
	return coll.Remove(content["query"])
}

func (executor *OpsExecutor) ExecCommand(
	content Document, db *mgo.Database) error {
	cmd := content["command"].(map[string]interface{})
	if cmd["findandmodify"] != nil {
		result := Document{}
		coll_name := cmd["findandmodify"].(string)
		change := mgo.Change{
			Update: cmd["update"].(map[string]interface{}),
		}
		_, err := db.C(coll_name).Find(cmd["query"]).Apply(change, result)
		return err
	} else if cmd["count"] != nil {
		coll_name := cmd["count"].(string)
		_, err := db.C(coll_name).Count()
		return err
	}
	return nil
}

func (executor *OpsExecutor) Execute(op *Op) error {
	executor.statsCollector.StartOp(op.Type)
	defer executor.statsCollector.EndOp()

	db := executor.session.DB(op.Database)
	content := op.Content
	// "command" ops requires some special treatments.
	if op.Type == "command" {
		return executor.ExecCommand(content, db)
	}

	coll := db.C(op.Collection)
	switch op.Type {
	case "insert":
		return executor.ExecInsert(content, coll)
	case "update":
		return executor.ExecUpdate(content, coll)
	case "remove":
		return executor.ExecRemove(content, coll)
	case "query":
		return executor.ExecQuery(content, coll)
	default:
		return errors.New("Unsupported op type " + string(op.Type))
	}

	return nil
}
