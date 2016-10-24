// Simple program which accepts a pcap file and prints a flashback-compatible ops stream
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/ParsePlatform/flashback"
	"github.com/google/gopacket/pcap"
	"github.com/tmc/mongocaputils"
	"github.com/tmc/mongoproto"
	"gopkg.in/mgo.v2/bson"
)

var (
	pcapFile	= flag.String("f", "-", "pcap file (or '-' for stdin)")
	bsonFile	= flag.String("o", "flashback.bson", "bson output file, will be overwritten")
	packetBufSize   = flag.Int("size", 1000, "size of packet buffer used for ordering within streams")
	continueOnError = flag.Bool("continue_on_error", false, "Continue parsing lines if an error is encountered")
	debug           = flag.Bool("debug", false, "Print debug-level output")
)

type Operation struct {
	Ns		string			`bson:"ns"`
	Timestamp	time.Time		`bson:"ts"`
	NToSkip		int32			`bson:"ntoskip,omitempty"`
	NToReturn	int32			`bson:"ntoreturn,omitempty"`
	InsertDoc	bson.D			`bson:"o,omitempty"`
	QueryDoc	bson.D			`bson:"query,omitempty"`
	UpdateDoc	bson.D			`bson:"updateobj,omitempty"`
	CommandDoc	bson.D			`bson:"command,omitempty"`
	Type		flashback.OpType	`bson:"op"`
}

func ParseQuery(opQuery []byte) (bson.D, error) {
	var query bson.D
	err := bson.Unmarshal(opQuery, &query)
	return query, err
}

func (op *Operation) HandleCommand(opCommand *mongoproto.OpQuery, f *os.File) error {
	var err error
	op.Type = flashback.Command
	op.CommandDoc, err = ParseQuery(opCommand.Query)
	if err != nil {
		return err
	}
	return op.Write(f)
}

func (op *Operation) HandleQuery(opQuery *mongoproto.OpQuery, f *os.File) error {
	var err error
	op.Ns = opQuery.FullCollectionName
	op.Type = flashback.Query
	if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
		// sometimes mongoproto returns inserts as 'commands'
		collection, exists := flashback.GetElem(op.QueryDoc, "insert")
		if exists == true {
			opQuery.FullCollectionName = strings.Replace(opQuery.FullCollectionName, "$cmd", collection.(string), 1)
			return op.HandleInsertFromQuery(opQuery, f)
		} 
		return op.HandleCommand(opQuery, f)
	}
	op.QueryDoc, err = ParseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	return op.Write(f)
}

func (op *Operation) HandleInsertDocument(document bson.D, f *os.File) error {
	op.Type = flashback.Insert
	op.InsertDoc = document
	return op.Write(f)
}

func (op *Operation) HandleInsert(opInsert *mongoproto.OpInsert, f *os.File) error {
	op.Ns = opInsert.FullCollectionName
	if opInsert.Documents != nil {
		for _, document := range opInsert.Documents {
			insert, err := ParseQuery(document)
			if err != nil {
				return err
			}
			err = op.HandleInsertDocument(insert, f)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *Operation) HandleInsertFromQuery(opQuery *mongoproto.OpQuery, f *os.File) error {
	op.Ns = opQuery.FullCollectionName
	query, err := ParseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	documents, exists := flashback.GetElem(query, "documents")
	if exists == true {
		if (reflect.TypeOf(documents).Kind() == reflect.Slice) {
			for _, document := range documents.([]interface{}) {
				err = op.HandleInsertDocument(document.(bson.D), f)
				if err != nil {
					return err
				}
			}
		} else {
			err = op.HandleInsertDocument(documents.(bson.D), f)
			if err != nil {
				return err
			}
		}
	} 
	return nil
}

func (op *Operation) HandleDelete(opDelete *mongoproto.OpDelete, f *os.File) error {
	var err error
	op.Type = flashback.Remove
	op.Ns = opDelete.FullCollectionName
	op.QueryDoc, err = ParseQuery(opDelete.Selector)
	if err != nil {
		return err
	}
	return op.Write(f)
}

func (op *Operation) Write(f *os.File) error {
	opBson, err := bson.Marshal(op)
	if err != nil {
		return err
	}
	f.Write(opBson)
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	logger := log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	pcap, err := pcap.OpenOffline(*pcapFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening pcap file:", err)
		os.Exit(1)
	}
	h := mongocaputils.NewPacketHandler(pcap)
	m := mongocaputils.NewMongoOpStream(*packetBufSize)

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		f, err := os.Create(*bsonFile)
		if err != nil {
			logger.Println(err)
			os.Exit(1)
		}
		defer f.Close()
		for op := range m.Ops {
			var err error
			fbOp := &Operation{
				Timestamp: op.Seen,
			}
			// todo: fix mongoproto.OpUpdate and mongoproto.OpDelete so they can be added
			if opInsert, ok := op.Op.(*mongoproto.OpInsert); ok {
				err = fbOp.HandleInsert(opInsert, f)
			} else if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				err = fbOp.HandleQuery(opQuery, f)
			} else if *debug == true {
				if _, ok := op.Op.(*mongoproto.OpUnknown); ok {
					fmt.Println("Found mongoproto.OpUnknown operation: ", op)
				} else {
					fmt.Println("No known type for operation: ", op)
				}
			}
			if err != nil {
				logger.Println(err)
				if !*continueOnError {
					os.Exit(1)
				}
			}
		}
	}()

	if err := h.Handle(m, -1); err != nil {
		fmt.Fprintln(os.Stderr, "pcap_converter: error handling packet stream:", err)
	}
	<-ch
}
