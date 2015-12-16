package flashback

import (
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/mongodb/mongo-tools/common/bsonutil" // requires go 1.4
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/json"
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
	err       error
	opsRead   int
	closeFunc func()
	logger    *Logger
	opFilters []OpType
	src       *db.DecodedBSONSource
}

func NewByLineOpsReader(reader io.ReadCloser, logger *Logger, opFilter string) (error, *ByLineOpsReader) {
	opFilters := make([]OpType, 0)
	if opFilter != "" {
		filterList := strings.Split(opFilter, ",")
		for _, filter := range filterList {
			opFilters = append(opFilters, OpType(filter))
		}
	}
	return nil, &ByLineOpsReader{
		src:       db.NewDecodedBSONSource(db.NewBSONSource(reader)),
		err:       nil,
		opsRead:   0,
		logger:    logger,
		opFilters: opFilters,
	}
}

func NewFileByLineOpsReader(filename string, logger *Logger, opFilter string) (error, *ByLineOpsReader) {
	file, err := os.Open(filename)
	if err != nil {
		return err, nil
	}
	err, reader := NewByLineOpsReader(file, logger, opFilter)
	if err != nil {
		return err, reader
	}
	reader.closeFunc = func() {
		file.Close()
	}
	return nil, reader
}

func (r *ByLineOpsReader) SkipOps(numSkipOps int) error {
	var op RawOp
	for numSkipped := 0; numSkipped < numSkipOps; numSkipped++ {
		if ok := r.src.Next(&op); !ok {
			return r.src.Err()
		}
	}

	r.logger.Infof("Done skipping %d ops.\n", numSkipOps)
	return nil
}

func (r *ByLineOpsReader) SetStartTime(startTime int64) (int64, error) {
	var numSkipped int64
	searchTime := time.Unix(startTime/1000, startTime%1000*1000000)

	var op RawOp
	for {
		// The nature of this function is that it will discard the first op
		if ok := r.src.Next(&op); !ok {
			return numSkipped, r.src.Err()
		}
		numSkipped++

		if op.Timestamp.After(searchTime) || op.Timestamp.Equal(searchTime) {
			actualTime := op.Timestamp
			r.logger.Infof("Skipped %d ops to begin at timestamp %d.", numSkipped, actualTime)
			return numSkipped, nil
		}
	}

	return numSkipped, errors.New("no ops found after specified start_time")
}

func (r *ByLineOpsReader) Next() *Op {
	// we may need to skip certain type of ops
	var rawOp RawOp
	for {
		if ok := r.src.Next(&rawOp); !ok {
			return nil
		}

		r.opsRead++
		op := makeOp(rawOp, r.opFilters)
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

// Some operations are recorded with empty values for $set, $unset
// When these are replayed against a mongo instance, they generate an error and do not execute
// This method will detect and remove these empty blocks before the query is executed
func PruneEmptyUpdateObj(doc Document, opType OpType) {
	keysToPrune := []string{"$set", "$unset"}

	if opType == Command {
		// only do this for findandmodify
		commandDoc := doc["command"].(bson.D)
		// on properly ordered docs the first element should always be the command name
		if commandDoc[0].Name != "findandmodify" {
			return
		}
		commandDoc = pruneEmptyKeys(commandDoc, keysToPrune)
		doc["command"] = commandDoc
	} else if opType == Update {
		updateDoc := doc["updateobj"].(bson.D)
		updateDoc = pruneEmptyKeys(updateDoc, keysToPrune)
		doc["updateobj"] = updateDoc
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

func pruneEmptyKeys(doc bson.D, keys []string) bson.D {
	keyMap := map[string]struct{}{}
	for _, key := range keys {
		keyMap[key] = struct{}{}
	}

	prunedDoc := bson.D{}
	for _, elem := range doc {
		if _, ok := keyMap[elem.Name]; ok {
			opValue := elem.Value.(bson.D)
			if len(opValue) > 0 {
				prunedDoc = append(prunedDoc, elem)
			}
		} else {
			prunedDoc = append(prunedDoc, elem)
		}
	}

	return prunedDoc
}

func makeOp(rawOp RawOp, opFilters []OpType) *Op {
	parts := strings.SplitN(rawOp.Ns, ".", 2)
	dbName, collName := parts[0], parts[1]

	if len(opFilters) != 0 {
		filtered := false
		for _, opFilter := range opFilters {
			if rawOp.Type == opFilter {
				filtered = true
				break
			}
		}
		if filtered == false {
			return nil
		}
	}

	var content Document
	// we only handpick the fields that will be of useful for a given op type.
	switch rawOp.Type {
	case Insert:
		content = Document{"o": rawOp.InsertDoc}
	case Query:
		content = Document{
			"query":     rawOp.QueryDoc,
			"ntoreturn": rawOp.NToReturn,
			"ntoskip":   rawOp.NToSkip,
		}
	case Update:
		content = Document{
			"query":     rawOp.QueryDoc,
			"updateobj": rawOp.UpdateDoc,
		}

		PruneEmptyUpdateObj(content, rawOp.Type)
	case Command:
		content = Document{"command": rawOp.CommandDoc}
		PruneEmptyUpdateObj(content, rawOp.Type)
	case Remove:
		content = Document{"query": rawOp.QueryDoc}
	default:
		return nil
	}
	return &Op{dbName, collName, rawOp.Type, rawOp.Timestamp, content}
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
