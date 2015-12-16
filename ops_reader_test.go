package flashback

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/facebookgo/ensure"
)

var (
	logger *Logger
)

// Implements ReadCloser so that we can pass mocked op streams
// to NewByLineOpsReader
type mockOpsStreamReader struct {
	opsByteReader io.Reader
}

func newMockOpsStreamReader(t *testing.T, ops []Op) mockOpsStreamReader {
	opsByteStream := make([]byte, 0)
	for _, op := range ops {
		bytes, err := bson.Marshal(op)
		ensure.Nil(t, err)
		opsByteStream = append(opsByteStream, bytes...)
	}

	return mockOpsStreamReader{opsByteReader: bytes.NewReader(opsByteStream)}
}

func (m mockOpsStreamReader) Read(p []byte) (n int, err error) {
	return m.opsByteReader.Read(p)
}

func (m mockOpsStreamReader) Close() error {
	return nil
}

// Verify if the items being read are as expected.
func CheckOpsReader(t *testing.T, loader OpsReader) {
	expectedOpsRead := 0
	const startingTs = 1396456709420
	for op := loader.Next(); op != nil; op = loader.Next() {
		expectedOpsRead += 1
		ensure.NotNil(t, op)
		ensure.DeepEqual(t, loader.OpsRead(), expectedOpsRead)

		// check the "ts" field
		CheckTime(t, float64(startingTs+loader.OpsRead()), op.Timestamp)

		// check the insert doc
		// certain key exists
		for i := 1; i <= 5; i++ {
			logTypeKey := fmt.Sprintf("logType%d", i)
			logType, ok := GetElem(op.InsertDoc, logTypeKey)
			if i != expectedOpsRead {
				ensure.False(t, ok)
				ensure.Nil(t, logType)
			} else {
				ensure.True(t, ok)
				ensure.NotNil(t, logType)
			}
		}
		// check the value for the shared key
		message := fmt.Sprintf("m%d", expectedOpsRead)
		actualMessage, ok := GetElem(op.InsertDoc, "message")
		ensure.True(t, ok)
		ensure.DeepEqual(t, actualMessage.(string), message)
	}
	ensure.DeepEqual(t, expectedOpsRead, 5)
}

func CheckSkipOps(t *testing.T, loader OpsReader) {
	expectedOpsRead := 0

	// Skip a single op
	loader.SkipOps(1)

	// Read all remaining ops
	for op := loader.Next(); op != nil; op = loader.Next() {
		expectedOpsRead += 1
		ensure.NotNil(t, op)
		ensure.DeepEqual(t, loader.OpsRead(), expectedOpsRead)
	}

	// Verify that only 4 ops are read, since we skipped one
	ensure.DeepEqual(t, expectedOpsRead, 4)
}

func CheckSetStartTime(t *testing.T, loader OpsReader) {
	expectedOpsRead := 0
	numSkipped, err := loader.SetStartTime(1396456709424)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, numSkipped, int64(4))

	for op := loader.Next(); op != nil; op = loader.Next() {
		expectedOpsRead += 1
		ensure.NotNil(t, op)
		ensure.DeepEqual(t, loader.OpsRead(), expectedOpsRead)
	}

	// Verify that only 4 ops are read, since we skipped one
	ensure.DeepEqual(t, expectedOpsRead, 1)
}

func TestPruneEmptyKeys(t *testing.T) {
	t.Parallel()
	// Check findAndModify and update structures to ensure nil $unsets are removed
	testOps := []Op{
		Op{
			Ns:        "foo.bar",
			Timestamp: time.Unix(1396457119, int64(032*time.Millisecond)),
			Type:      Update,
			QueryDoc:  bson.D{{"_id", "foo"}},
			UpdateDoc: bson.D{{"$set", bson.D{{"a", 1}}}, {"$unset", bson.D{}}},
		},
		Op{
			Ns:        "foo.$cmd",
			Timestamp: time.Unix(1396457119, int64(032*time.Millisecond)),
			Type:      Command,
			CommandDoc: bson.D{
				{"findandmodify", "bar"},
				{"query", bson.D{{"_id", "foo"}}},
				{"update", bson.D{{"$set", bson.D{{"b", 1}}}, {"$unset", bson.D{}}}},
			},
		},
	}
	reader := newMockOpsStreamReader(t, testOps)
	err, loader := NewByLineOpsReader(reader, logger, "")
	ensure.Nil(t, err)

	for op := loader.Next(); op != nil; op = loader.Next() {
		if op.Type == Command {
			updateDoc, ok := GetElem(op.CommandDoc, "update")
			ensure.True(t, ok)
			_, found := GetElem(updateDoc.(bson.D), "$unset")
			ensure.False(t, found)
		} else if op.Type == Update {
			_, found := GetElem(op.UpdateDoc, "$unset")
			ensure.False(t, found)
		}
	}
}

func TestFileByLineOpsReader(t *testing.T) {
	t.Parallel()
	logger, _ = NewLogger("", "")

	testOps := []Op{
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(421*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType1", "warning"}, {"message", "m1"}},
		},
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(422*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType2", "warning"}, {"message", "m2"}},
		},
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(423*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType3", "warning"}, {"message", "m3"}},
		},
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(424*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType4", "warning"}, {"message", "m4"}},
		},
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(425*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType5", "warning"}, {"message", "m5"}},
		},
	}

	reader := newMockOpsStreamReader(t, testOps)
	err, loader := NewByLineOpsReader(reader, logger, "")
	ensure.Nil(t, err)
	CheckOpsReader(t, loader)

	// Reset the reader so that we can test SkipOps
	reader = newMockOpsStreamReader(t, testOps)
	err, loader = NewByLineOpsReader(reader, logger, "")
	ensure.Nil(t, err)
	CheckSkipOps(t, loader)

	// Reset the reader so that we can test SetStartTime
	reader = newMockOpsStreamReader(t, testOps)
	err, loader = NewByLineOpsReader(reader, logger, "")
	ensure.Nil(t, err)
	CheckSetStartTime(t, loader)
}

func TestOpFilter(t *testing.T) {
	logger, _ = NewLogger("", "")

	testOps := []Op{
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(421*time.Millisecond)),
			Type:      Insert,
			InsertDoc: bson.D{{"logType1", "warning"}, {"message", "m1"}},
		},
		Op{
			Ns:        "db.coll",
			Timestamp: time.Unix(1396456709, int64(421*time.Millisecond)),
			Type:      Update,
			QueryDoc:  bson.D{{"_id", "foo"}},
			UpdateDoc: bson.D{{"$set", bson.D{{"a", 1}}}},
		},
		Op{
			Ns:        "db.$cmd",
			Timestamp: time.Unix(1396456709, int64(421*time.Millisecond)),
			Type:      Command,
			CommandDoc: bson.D{
				{"findandmodify", "coll"},
				{"query", bson.D{{"_id", "foo"}}},
				{"update", bson.D{{"$set", bson.D{{"a", 1}}}}},
			},
		},
	}

	test := func(opFilter string, expectedOps int) {
		reader := newMockOpsStreamReader(t, testOps)
		err, loader := NewByLineOpsReader(reader, logger, opFilter)
		ensure.Nil(t, err)
		opsRead := 0
		for op := loader.Next(); op != nil; op = loader.Next() {
			opsRead += 1
		}
		ensure.DeepEqual(t, opsRead, expectedOps)
	}

	test("", 3)
	test("update", 2)
	test("update,insert", 1)
	test("update,insert,command", 0)
}

func TestShouldFilterOp(t *testing.T) {
	t.Parallel()

	ensure.True(t, shouldFilterOp(&Op{Type: Update}, []OpType{Update}))
	ensure.True(t, shouldFilterOp(&Op{Type: Update}, []OpType{Update, Query}))
	ensure.True(t, shouldFilterOp(&Op{Type: Query}, []OpType{Update, Query}))
	ensure.False(t, shouldFilterOp(&Op{Type: Command}, []OpType{Update, Query}))
	ensure.False(t, shouldFilterOp(&Op{Type: Command}, []OpType{}))
}

func TestNormalizeOp(t *testing.T) {
	t.Parallel()

	op := &Op{Ns: "foo.bar"}
	normalizeOp(op)
	ensure.DeepEqual(t, op.Database, "foo")
	ensure.DeepEqual(t, op.Collection, "bar")
}

func CheckTime(t *testing.T, pythonTime float64, goTime time.Time) {
	ensure.DeepEqual(t, goTime.Unix(), int64(pythonTime)/1e3)
	ensure.DeepEqual(t, goTime.UnixNano(), int64(pythonTime)*1e6)
}
