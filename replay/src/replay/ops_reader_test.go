package replay

import (
	"bytes"
	"fmt"
	. "gopkg.in/check.v1"
	"labix.org/v2/mgo/bson"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type TestFileByLineOpsReaderSuite struct{}

var _ = Suite(&TestFileByLineOpsReaderSuite{})

// Verify if the items being read are as expected.
func CheckOpsReader(c *C, loader OpsReader) {
	expectedOpsRead := 0
	const startingTs = 1396456709420
	for op := loader.Next(); op != nil; op = loader.Next() {
		expectedOpsRead += 1
		c.Assert(op, Not(Equals), nil)
		c.Assert(loader.OpsRead(), Equals, expectedOpsRead)

		// check the "ts" field
		CheckTime(c, float64(startingTs+loader.OpsRead()), op.Timestamp)

		// check the "o" field
		var content = op.Content["o"].(map[string]interface{})
		// certain key exists
		for i := 1; i <= 5; i++ {
			logTypeKey := fmt.Sprintf("logType%d", i)
			logType := content[logTypeKey]
			if i != expectedOpsRead {
				c.Assert(logType, IsNil)
			} else {
				c.Assert(logType, NotNil)
			}
		}
		// check the value for the shared key
		message := fmt.Sprintf("m%d", expectedOpsRead)
		c.Assert(message, Equals, content["message"].(string))
	}
	c.Assert(expectedOpsRead, Equals, 5)
}

func CheckSkipOps(c *C, loader OpsReader) {

	expectedOpsRead := 0
	const startingTs = 1396456709420
	
	// Skip a single op
	loader.SkipOps(1)
	
	// Read all remaining ops
	for op := loader.Next(); op != nil; op = loader.Next() {
		expectedOpsRead += 1
		c.Assert(op, Not(Equals), nil)
		c.Assert(loader.OpsRead(), Equals, expectedOpsRead)
	}
	
	// Verify that only 4 ops are read, since we skipped one
	c.Assert(expectedOpsRead, Equals, 4)
}

func (s *TestFileByLineOpsReaderSuite) TestFileByLineOpsReader(c *C) {
	testJsonString :=
		`{ "ts": {"$date" : 1396456709421}, "ns": "db.coll", "op": "insert", "o": {"logType1": "warning", "message": "m1"} }
        { "ts": {"$date": 1396456709422}, "ns": "db.coll", "op": "insert", "o": {"logType2": "warning", "message": "m2"} }
        { "ts": {"$date": 1396456709423}, "ns": "db.coll", "op": "insert", "o": {"logType3": "warning", "message": "m3"} }
        { "ts": {"$date": 1396456709424}, "ns": "db.coll", "op": "insert", "o": {"logType4": "warning", "message": "m4"} }
        { "ts": {"$date": 1396456709425}, "ns": "db.coll", "op": "insert", "o": {"logType5": "warning", "message": "m5"} }`
	reader := bytes.NewReader([]byte(testJsonString))
	err, loader := NewByLineOpsReader(reader)
	c.Assert(err, Equals, nil)
	CheckOpsReader(c, loader)
	
	// Reset the reader so that we can test SkipOps
	reader = bytes.NewReader([]byte(testJsonString))
	err, loader = NewByLineOpsReader(reader)
	c.Assert(err, Equals, nil)
	CheckSkipOps(c, loader)
}

func CheckTime(c *C, pythonTime float64, goTime time.Time) {
	c.Assert(goTime.Unix(), Equals, int64(pythonTime)/1e3)
	c.Assert(goTime.UnixNano(), Equals, int64(pythonTime)*1e6)
}

func (s *TestFileByLineOpsReaderSuite) TestSimplenormalizeObj(c *C) {
	ts := 1396510695969.0
	objWithTime := map[string]interface{}{
		"ts": map[string]interface{}{"$date": ts},
	}
	normalizeObj(objWithTime)
	CheckTime(c, ts, objWithTime["ts"].(time.Time))

}

func (s *TestFileByLineOpsReaderSuite) TestComplexnormalizeObj(c *C) {
	complicatedItem := map[string]interface{}{
		"ts": map[string]interface{}{"$date": 1388810695888.0},
		"doc1": map[string]interface{}{
			"doc2": map[string]interface{}{"$date": 1388810611111.0},
		},
		"doc3": []interface{}{
			map[string]interface{}{
				"doc4": map[string]interface{}{
					"$date": 1388810622222.0,
				},
			},
		},
		"doc5": map[string]interface{}{
			"$oid": "533c3d03c23fffd217678ee7",
		},
	}
	normalizeObj(complicatedItem)
	// check the ts
	CheckTime(c, 1388810695888.0, complicatedItem["ts"].(time.Time))

	// check ts inside map(s)
	doc1 := complicatedItem["doc1"].(map[string]interface{})
	ts1 := doc1["doc2"].(time.Time)
	CheckTime(c, 1388810611111.0, ts1)

	// check ts inside list(s)
	doc3 := complicatedItem["doc3"].([]interface{})
	ts2 := doc3[0].(map[string]interface{})["doc4"].(time.Time)
	CheckTime(c, 1388810622222.0, ts2)

	// check the id
	doc5 := complicatedItem["doc5"].(bson.ObjectId)
	c.Assert(doc5, Equals, bson.ObjectIdHex("533c3d03c23fffd217678ee7"))
}
