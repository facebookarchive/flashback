package flashback

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
)

// Hook up gocheck into the "go test" runner.
func TestOps(t *testing.T) {
	TestingT(t)
}

type TestExecutorSuite struct{}

var _ = Suite(&TestExecutorSuite{})

func (s *TestExecutorSuite) TestExecution(c *C) {
	test_db := "test_db_for_executor"
	test_collection := "c1"

	session, err := mgo.Dial("localhost")
	c.Assert(err, IsNil)
	c.Assert(session, NotNil)
	defer session.Close()

	err = session.DB(test_db).DropDatabase()
	c.Assert(err, IsNil)

	// insertion
	insertCmd := fmt.Sprintf(`{"ns": "%s.%s", "ts": {"$date": 1396456709427}, `+
		`"o": {"logType": "console", "message": "start", "_id": `+
		`{"$oid": "533c3d03c23fffd217678ee8"}, "timestamp": `+
		`{"$date": 1396456707977}}, "op": "insert"}`,
		test_db, test_collection)
	cmd, err := parseJson(insertCmd)
	c.Assert(err, IsNil)
	op := makeOp(cmd)

	exec := NewOpsExecutor(session)
	err = exec.Execute(op)
	c.Assert(err, IsNil)
	coll := session.DB(test_db).C(test_collection)
	count, err := coll.Count()
	c.Assert(count, Equals, 1)

	// Find
	findCmd := fmt.Sprintf(`{"ntoskip": 0, "ts": {"$date": 1396456709472}, `+
		`"ntoreturn": 1, "query": {"$maxScan": 9000000, "$query": {"$or": `+
		`[{"_id": {"$oid": "533c3d03c23fffd217678ee8"}}]}}, `+
		`"ns": "%s.%s", "op": "query"}`, test_db, test_collection)
	cmd, err = parseJson(findCmd)
	c.Assert(err, IsNil)
	findOp := makeOp(cmd)

	err = exec.Execute(findOp)
	c.Assert(err, IsNil)

	findResult := exec.lastResult.(*[]Document)
	c.Assert(len(*findResult), Equals, 1)
	exec.lastResult = nil

	// Update
	updateCmd := fmt.Sprintf(`{"ts": {"$date": 1396456709472}, `+
		`"query": {"_id": {"$oid": "533c3d03c23fffd217678ee8"}}, `+
		`"updateobj": {"$set":{"logType": "hooo"}}, `+
		`"ns": "%s.%s", "op": "update"}`, test_db, test_collection)
	cmd, err = parseJson(updateCmd)
	c.Assert(err, IsNil)
	err = exec.Execute(makeOp(cmd))
	c.Assert(err, IsNil)

	err = exec.Execute(findOp)
	c.Assert(err, IsNil)
	findResult = exec.lastResult.(*[]Document)
	c.Assert((*findResult)[0]["logType"].(string), Equals, "hooo")
	findResult = nil

	// findAndModify
	famCmd := fmt.Sprintf(
		`{"ts": {"$date": 1396456709472}, `+
			`"ns": "%s.$cmd", "command": {"query": {"_id": {"$oid": "533c3d03c23fffd217678ee8"}}, `+
			`"findandmodify": "%s", `+
			`"update": {"$set": {"logType": "foobar"}}}, "op": "command"}`, test_db, test_collection)
	cmd, err = parseJson(famCmd)
	c.Assert(err, IsNil)
	err = exec.Execute(makeOp(cmd))
	c.Assert(err, IsNil)

	err = exec.Execute(findOp)
	c.Assert(err, IsNil)
	findResult = exec.lastResult.(*[]Document)
	c.Assert((*findResult)[0]["logType"].(string), Equals, "foobar")
	findResult = nil

	// Remove
	removeCmd := fmt.Sprintf(
		`{"query": {"_id": {"$oid": "533c3d03c23fffd217678ee8"}}, `+
			`"ns": "%s.%s", "ts": {"$date": 1396456709432}, "op": "remove"}`,
		test_db, test_collection)
	cmd, err = parseJson(removeCmd)
	c.Assert(err, IsNil)
	err = exec.Execute(makeOp(cmd))
	c.Assert(err, IsNil)

	err = exec.Execute(findOp)
	c.Assert(err, IsNil)
	findResult = exec.lastResult.(*[]Document)
	c.Assert(len(*findResult), Equals, 0)
}
