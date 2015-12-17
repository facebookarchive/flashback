package flashback

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/facebookgo/ensure"
)

func TestExecution(t *testing.T) {
	test_db := "test_db_for_executor"
	test_collection := "c1"

	session, err := mgo.Dial("localhost")
	ensure.Nil(t, err)
	ensure.NotNil(t, session)
	defer session.Close()

	err = session.DB(test_db).DropDatabase()
	ensure.Nil(t, err)

	testNs := fmt.Sprintf("%s.%s", test_db, test_collection)

	// insertion
	insertOp := &Op{
		Ns:        testNs,
		Timestamp: time.Unix(1396456709, int64(427*time.Millisecond)),
		InsertDoc: bson.D{
			{"logType", "console"},
			{"message", "start"},
			{"_id", bson.ObjectIdHex("533c3d03c23fffd217678ee8")},
			{"timestamp", bson.D{{"$date", 1396456707977}}},
		},
		Type: Insert,
	}
	normalizeOp(insertOp)
	logger, err := NewLogger("", "")
	ensure.Nil(t, err)
	exec := NewOpsExecutor(session, nil, logger)
	err = exec.Execute(insertOp)
	ensure.Nil(t, err)
	coll := session.DB(test_db).C(test_collection)
	count, err := coll.Count()
	ensure.DeepEqual(t, count, 1)

	// Find
	findOp := &Op{
		Ns:        testNs,
		Timestamp: time.Unix(1396456709, int64(472*time.Millisecond)),
		QueryDoc: bson.D{
			{
				"$query", bson.D{
					{"$or", []bson.D{bson.D{{"_id", bson.ObjectIdHex("533c3d03c23fffd217678ee8")}}}},
				},
			},
			{"$maxScan", 9000000},
		},
		Type:      Query,
		NToReturn: 1,
		NToSkip:   0,
	}
	normalizeOp(findOp)
	err = exec.Execute(findOp)
	ensure.Nil(t, err)

	findResult := exec.lastResult.(*[]Document)
	ensure.DeepEqual(t, len(*findResult), 1)
	exec.lastResult = nil

	// Update
	updateOp := &Op{
		Ns:        testNs,
		Timestamp: time.Unix(1396456709, int64(472*time.Millisecond)),
		QueryDoc: bson.D{
			{"_id", bson.ObjectIdHex("533c3d03c23fffd217678ee8")},
		},
		UpdateDoc: bson.D{
			{"$set", bson.D{{"logType", "hooo"}}},
		},
		Type: Update,
	}

	normalizeOp(updateOp)
	err = exec.Execute(updateOp)
	ensure.Nil(t, err)

	// Check that the document is updated
	normalizeOp(findOp)
	err = exec.Execute(findOp)
	ensure.Nil(t, err)
	findResult = exec.lastResult.(*[]Document)
	ensure.DeepEqual(t, (*findResult)[0]["logType"].(string), "hooo")
	findResult = nil

	// findAndModify
	famOp := &Op{
		Ns:        fmt.Sprintf("%s.$cmd", test_db),
		Timestamp: time.Unix(1396456709, int64(472*time.Millisecond)),
		CommandDoc: bson.D{
			{"findandmodify", test_collection},
			{"query", bson.D{{"_id", bson.ObjectIdHex("533c3d03c23fffd217678ee8")}}},
			{"update", bson.D{{"$set", bson.D{{"logType", "foobar"}}}}},
		},
		Type: Command,
	}
	normalizeOp(famOp)
	err = exec.Execute(famOp)
	ensure.Nil(t, err)

	// check that the doc is modified
	normalizeOp(findOp)
	err = exec.Execute(findOp)
	ensure.Nil(t, err)
	findResult = exec.lastResult.(*[]Document)
	ensure.DeepEqual(t, (*findResult)[0]["logType"].(string), "foobar")
	findResult = nil

	// Remove
	removeOp := &Op{
		Ns:        testNs,
		Timestamp: time.Unix(1396456709, int64(432*time.Millisecond)),
		QueryDoc:  bson.D{{"_id", bson.ObjectIdHex("533c3d03c23fffd217678ee8")}},
		Type:      Remove,
	}
	normalizeOp(removeOp)
	err = exec.Execute(removeOp)
	ensure.Nil(t, err)

	// check that the doc is gone
	normalizeOp(findOp)
	err = exec.Execute(findOp)
	ensure.Nil(t, err)
	findResult = exec.lastResult.(*[]Document)
	ensure.DeepEqual(t, len(*findResult), 0)
}

func TestSafeGetInt(t *testing.T) {
	val, err := safeGetInt(int32(11))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val, int(11))
	val, err = safeGetInt(int64(11))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val, int(11))
	val, err = safeGetInt(float32(11))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val, int(11))
	val, err = safeGetInt(float64(11))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val, int(11))
	val, err = safeGetInt("a")
	ensure.NotNil(t, err)
}
