// Simple program which accepts a pcap file and prints a flashback-compatible ops stream
package main

import (
	"fmt"
	"flag"
	"time"
	"log"
	"os"
	"runtime"
	"strings"
	"reflect"

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

func parseQuery(opQuery []byte) (bson.D, error) {
	var query bson.D
	err := bson.Unmarshal(opQuery, &query)
	return query, err
}

func (fbOp *Operation) handleCommand(opCommand *mongoproto.OpQuery, f *os.File) error {
	var err error
	fbOp.Type = flashback.Command
	fbOp.CommandDoc, err = parseQuery(opCommand.Query)
	if err != nil {
		return err
	}
	return fbOp.writeOp(f)
}

func (fbOp *Operation) handleQuery(opQuery *mongoproto.OpQuery, f *os.File) error {
	var err error
	fbOp.Ns = opQuery.FullCollectionName
	fbOp.Type = flashback.Query
	query, err := parseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
		// sometimes mongoproto returns inserts as 'commands'
		collection, exists := flashback.GetElem(fbOp.QueryDoc, "insert")
		if exists == true {
			opQuery.FullCollectionName = strings.Replace(opQuery.FullCollectionName, "$cmd", collection.(string), 1)
			return fbOp.handleInsertFromQuery(opQuery, f)
		} else {
			return fbOp.handleCommand(opQuery, f)
		}
	} else {
		fbOp.QueryDoc = query
		return fbOp.writeOp(f)
	}
}

func (fbOp *Operation) handleInsertDocument(ns string, document bson.D, f *os.File) error {
	fbOp.Ns = ns
	fbOp.Type = flashback.Insert
	fbOp.InsertDoc = document
	return fbOp.writeOp(f)
}

func (fbOp *Operation) handleInsert(opInsert *mongoproto.OpInsert, f *os.File) error {
	var err error
	fbOp.Ns = opInsert.FullCollectionName
	if opInsert.Documents != nil {
		for _, document := range opInsert.Documents {
			insert, err := parseQuery(document)
			if err != nil {
				return err
			}
			err = fbOp.handleInsertDocument(fbOp.Ns, insert, f)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (fbOp *Operation) handleInsertFromQuery(opQuery *mongoproto.OpQuery, f *os.File) error {
	var inserts []bson.D
	fbOp.Ns = opQuery.FullCollectionName
	query, err := parseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	documents, exists := flashback.GetElem(query, "documents")
	if exists == true {
		if (reflect.TypeOf(documents).Kind() == reflect.Slice) {
			for _, document := range documents.([]interface{}) {
				inserts = append(inserts, document.(bson.D))
			}
		} else {
			inserts = append(inserts, documents.(bson.D))
		}
	} 
	for _, insert := range inserts {
		err = fbOp.handleInsertDocument(fbOp.Ns, insert, f)
	}
	return fbOp.writeOp(f) 
}

func (fbOp *Operation) handleDelete(opDelete *mongoproto.OpDelete, f *os.File) error {
	var err error
	fbOp.Type = flashback.Remove
	fbOp.Ns = opDelete.FullCollectionName
	fbOp.QueryDoc, err = parseQuery(opDelete.Selector)
	if err != nil {
		return err
	}
	return fbOp.writeOp(f)
}

func (op *Operation) writeOp(f *os.File) error {
	opBson, err := bson.Marshal(op)
	f.Write(opBson)
	return err
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
			// todo: fix mongoproto.OpUpdate and mongoproto.OpDelete so they can be added
			var err error
			fbOp := &Operation{
				Timestamp: op.Seen,
			}
			if opDelete, ok := op.Op.(*mongoproto.OpDelete); ok {
				err = fbOp.handleDelete(opDelete, f)
			} else if opInsert, ok := op.Op.(*mongoproto.OpInsert); ok {
				err = fbOp.handleInsert(opInsert, f)
			} else if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				err = fbOp.handleQuery(opQuery, f)
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
