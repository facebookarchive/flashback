package flashback

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/facebookgo/ensure"
)

const (
	testTime = int64(1450208315)
)

func TestRawOpUnmarshal(t *testing.T) {
	t.Parallel()
	testCmd := bson.D{{"z", 1}, {"a", 1}}
	testQuery := bson.D{{"a", 1}, {"z", 1}}

	testCmdDoc := bson.D{
		{"ts", time.Unix(testTime, 0)},
		{"ns", "foo"},
		{"op", "command"},
		{"command", testCmd},
	}
	testQueryDoc := bson.D{
		{"ts", time.Unix(testTime, 0)},
		{"ns", "foo"},
		{"op", "query"},
		{"query", testQuery},
		{"ntoskip", 1},
		{"ntoreturn", 2},
	}

	// marshal to byte form so we can unmarshal into struct
	testCmdDocBytes, err := bson.Marshal(testCmdDoc)
	ensure.Nil(t, err)

	testQueryDocBytes, err := bson.Marshal(testQueryDoc)
	ensure.Nil(t, err)

	var testCmdRawOp, testQueryRawOp RawOp
	err = bson.Unmarshal(testCmdDocBytes, &testCmdRawOp)
	ensure.Nil(t, err)

	err = bson.Unmarshal(testQueryDocBytes, &testQueryRawOp)
	ensure.Nil(t, err)

	ensure.Subset(
		t,
		testCmdRawOp,
		RawOp{
			Timestamp:  time.Unix(testTime, 0),
			Ns:         "foo",
			Type:       Command,
			CommandDoc: testCmd,
		},
	)
	ensure.Subset(
		t,
		testQueryRawOp,
		RawOp{
			Timestamp: time.Unix(testTime, 0),
			Ns:        "foo",
			Type:      Query,
			QueryDoc:  testQuery,
			NToSkip:   1,
			NToReturn: 2,
		},
	)
}

func TestGetElem(t *testing.T) {
	doc := bson.D{{"a", 1}}
	value, exists := GetElem(doc, "a")
	ensure.True(t, exists)
	ensure.DeepEqual(t, value.(int), 1)
	value, exists = GetElem(doc, "b")
	ensure.False(t, exists)
	ensure.Nil(t, value)
}
