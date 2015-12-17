package flashback

import (
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/mongodb/mongo-tools/common/db"
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
	var op Op
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

	var op Op
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
	var op Op
	for {
		if ok := r.src.Next(&op); !ok {
			return nil
		}

		r.opsRead++

		// filter out unwanted ops
		if shouldFilterOp(&op, r.opFilters) {
			continue
		}

		normalizeOp(&op)

		// Clean up empty keys on specific ops
		emptyKeysToPrune := []string{"$set", "$unset"}
		switch op.Type {
		case Command:
			if op.CommandDoc[0].Name == "findandmodify" {
				for i := range op.CommandDoc {
					if op.CommandDoc[i].Name == "update" {
						if updateDoc, ok := op.CommandDoc[i].Value.(bson.D); ok {
							updateDoc = pruneEmptyKeys(updateDoc, emptyKeysToPrune)
							op.CommandDoc[i].Value = updateDoc
						}
					}
				}
			}
		case Update:
			op.UpdateDoc = pruneEmptyKeys(op.UpdateDoc, emptyKeysToPrune)
		}

		return &op
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

func shouldFilterOp(op *Op, filters []OpType) bool {
	for _, opFilter := range filters {
		if op.Type == opFilter {
			return true
		}
	}

	return false
}

func normalizeOp(op *Op) {
	// populate db and collection name
	parts := strings.SplitN(op.Ns, ".", 2)
	dbName, collName := parts[0], parts[1]
	op.Database = dbName
	op.Collection = collName
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

// Some operations are recorded with empty values for $set, $unset
// When these are replayed against a mongo instance, they generate an error and do not execute
// This method will detect and remove these empty blocks before the query is executed
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
