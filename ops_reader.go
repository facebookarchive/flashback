package flashback

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/common/bsonutil" // requires go 1.4
)

// OpsReader Reads the ops from a source and present a interface for consumers
// to fetch these ops sequentially.
type OpsReader interface {
	// Move to next op and return it. Nil will be returned if the last ops had
	// already been read, or there is any error occurred.
	// TODO change from Document to Op
	Next() *Op

	// Allow skipping the first N ops in the source file
	SkipOps(int) error

	// Start at a specific time in the set of ops
	// Return an error if we get to EOF without finding an op
	// Can be used with SkipOps, but you should call SkipOps after SetStartTime
	SetStartTime(int64) (int64, error)

	// How many ops are read so far
	OpsRead() int

	// Have all the ops been read?
	AllLoaded() bool

	// indicate the latest error occurs when reading ops.
	Err() error

	Close()
}

// ByLineOpsReader reads ops from a json file that is exported from python's
// json_util module, where each line is a json-represented op.
//
// Note: After parse each json-represented op, we need perform post-process to
// convert some "metadata" into MongoDB specific data structures, like "Object
// Id" and datetime.
type ByLineOpsReader struct {
	lineReader *bufio.Reader
	err        error
	opsRead    int
	closeFunc  func()
	logger     *Logger
}

func NewByLineOpsReader(reader io.Reader, logger *Logger) (error, *ByLineOpsReader) {
	return nil, &ByLineOpsReader{
		lineReader: bufio.NewReaderSize(reader, 5*1024*1024),
		err:        nil,
		opsRead:    0,
		logger:     logger,
	}
}

// func NewCyclicOpsReader(func() ops_reader_maker *OpsReader) (error, OpsReader)

func NewFileByLineOpsReader(filename string, logger *Logger) (error, *ByLineOpsReader) {
	file, err := os.Open(filename)
	if err != nil {
		return err, nil
	}
	err, reader := NewByLineOpsReader(file, logger)
	if err != nil {
		return err, reader
	}
	reader.closeFunc = func() {
		file.Close()
	}
	return nil, reader
}

func (r *ByLineOpsReader) SkipOps(numSkipOps int) error {
	for numSkipped := 0; numSkipped < numSkipOps; numSkipped++ {
		_, err := r.lineReader.ReadString('\n')

		// Return if we get an error reading the error, or hit EOF
		if err != nil || err == io.EOF {
			return err
		}
	}

	r.logger.Infof("Done skipping %d ops.\n", numSkipOps)
	return nil
}

func (r *ByLineOpsReader) SetStartTime(startTime int64) (int64, error) {
	var numSkipped int64
	searchTime := time.Unix(startTime/1000, startTime%1000*1000000)

	for true {
		// The nature of this function is that it will discard the first op
		jsonText, err := r.lineReader.ReadString('\n')
		numSkipped++

		// Return if we get an error reading the error, or hit EOF
		if err != nil || err == io.EOF {
			return numSkipped, err
		}

		rawObj, err := parseJson(jsonText)
		if err != nil {
			return numSkipped, err
		}

		timestamp := rawObj["ts"].(time.Time)
		if timestamp.After(searchTime) || timestamp.Equal(searchTime) {
			actualTime := timestamp
			r.logger.Infof("Skipped %d ops to begin at timestamp %d.", numSkipped, actualTime)
			return numSkipped, nil
		}
	}

	return numSkipped, errors.New("no ops found after specified start_time")
}

func (r *ByLineOpsReader) Next() *Op {
	// we may need to skip certain type of ops
	for {
		jsonText, err := r.lineReader.ReadString('\n')
		r.err = err

		if err != nil && err != io.EOF {
			return nil
		}

		rawObj, err := parseJson(jsonText)
		r.err = err
		if err != nil {
			return nil
		}
		r.opsRead++
		op := makeOp(rawObj)
		if op == nil {
			continue
		}

		return op
	}
}

func (r *ByLineOpsReader) OpsRead() int {
	return r.opsRead
}

func (r *ByLineOpsReader) AllLoaded() bool {
	return r.err == io.EOF
}

func (r *ByLineOpsReader) Err() error {
	return r.err
}
func (r *ByLineOpsReader) Close() {
	if r.closeFunc != nil {
		r.closeFunc()
	}
}

// Convert a json string to a raw document
func parseJson(jsonText string) (Document, error) {
	rawObj := Document{}
	err := json.Unmarshal([]byte(jsonText), &rawObj)

	if err != nil {
		return rawObj, err
	}
	err = normalizeObj(rawObj)
	return rawObj, err
}

// Convert mongo extended json types from their strict JSON representation
// to appropriate bson types
// http://docs.mongodb.org/manual/reference/mongodb-extended-json/
func normalizeObj(rawObj Document) error {
	return bsonutil.ConvertJSONDocumentToBSON(rawObj)
}

// Some operations are recorded with empty values for $set, $unset, and possibly $inc
// When these are replayed against a mongo instance, they generate an error and do not execute
// This method will detect and remove these empty blocks before the query is executed
// A couple of examples are below
// {"ns": "appdata68.$cmd", "command": {"query": {"$or": [{"_acl": {"$exists": false}}, {"_acl.*.w": true}], "_id": "5Npn4XbXVF"}, "findandmodify": "app_0ecb3ea0-a35a-4fa6-b1a8-2bf66ae160ff:_Installation", "update": {"$set": {"_updated_at": {"$date": 1396457187276}}, "$unset": {}, "$addToSet": {"channels": {"$each": ["", "v5420"]}}}, "new": true}, "ts": {"$date": 1396457187283}, "op": "command"}
// {"query": {"$or": [{"_acl": {"$exists": false}}, {"_acl.*.w": true}], "_id": "YDHJwP5hFX"}, "updateobj": {"$set": {"_updated_at": {"$date": 1396457119032}}, "$unset": {}}, "ns": "appdata66.app_0939ec2a-b247-4485-b741-bfe069791305:Prize", "op": "update", "ts": {"$date": 1396457119032}}
func PruneEmptyUpdateObj(doc Document, opType string) {
	var updateObj map[string]interface{}

	if opType == "command" {
		// only do this for findandmodify
		command := doc["command"].(map[string]interface{})
		if command["findandmodify"] == nil {
			return
		}
		updateObj = command["update"].(map[string]interface{})
	} else if opType == "update" {
		updateObj = doc["updateobj"].(map[string]interface{})
	} else {
		return
	}

	operators := [3]string{"$set", "$unset", "$inc"}

	for _, operator := range operators {
		if updateObj[operator] != nil {
			checkMap := updateObj[operator].(map[string]interface{})
			if len(checkMap) == 0 {
				delete(updateObj, operator)
			}
		}
	}
}

func makeOp(rawDoc Document) *Op {
	opType := rawDoc["op"].(string)
	ts := rawDoc["ts"].(time.Time)
	ns := rawDoc["ns"].(string)
	parts := strings.SplitN(ns, ".", 2)
	dbName, collName := parts[0], parts[1]

	var content Document
	// we only handpick the fields that will be of useful for a given op type.
	switch opType {
	case "insert":
		content = Document{"o": rawDoc["o"].(map[string]interface{})}
	case "query":
		content = Document{
			"query":     rawDoc["query"],
			"ntoreturn": rawDoc["ntoreturn"],
			"ntoskip":   rawDoc["ntoskip"],
		}
	case "update":
		content = Document{
			"query":     rawDoc["query"],
			"updateobj": rawDoc["updateobj"],
		}

		PruneEmptyUpdateObj(content, opType)
	case "command":
		content = Document{"command": rawDoc["command"]}
		PruneEmptyUpdateObj(content, opType)
	case "remove":
		content = Document{"query": rawDoc["query"]}
	default:
		return nil
	}
	return &Op{dbName, collName, OpType(opType), ts, content}
}

type CyclicOpsReader struct {
	maker        func() OpsReader
	reader       OpsReader
	previousRead int
	err          error
	logger       *Logger
}

func NewCyclicOpsReader(maker func() OpsReader, logger *Logger) *CyclicOpsReader {
	reader := maker()
	if reader == nil {
		return nil
	}

	return &CyclicOpsReader{
		maker,
		reader,
		0,
		nil,
		logger,
	}
}

func (c *CyclicOpsReader) Next() *Op {
	var op *Op = nil
	if op = c.reader.Next(); op == nil {
		c.logger.Info("Recycle starts")
		c.previousRead += c.reader.OpsRead()
		c.reader.Close()
		c.reader = c.maker()
		op = c.reader.Next()
	}
	if op == nil {
		c.err = errors.New("The underlying ops reader is empty or invalid")
	}
	return op

}

func (c *CyclicOpsReader) OpsRead() int {
	return c.reader.OpsRead() + c.previousRead
}

func (c *CyclicOpsReader) AllLoaded() bool {
	return false
}

func (c *CyclicOpsReader) SkipOps(numSkipOps int) error {
	return c.reader.SkipOps(numSkipOps)
}

func (c *CyclicOpsReader) SetStartTime(startTime int64) (int64, error) {
	return c.reader.SetStartTime(startTime)
}

func (c *CyclicOpsReader) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.reader.Err()
}

func (c *CyclicOpsReader) Close() {
	c.reader.Close()
}
