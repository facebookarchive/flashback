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

func ParseQuery(opQuery []byte) (bson.D, error) {
	var query bson.D
	err := bson.Unmarshal(opQuery, &query)
	return query, err
}

func HandleCommand(op *flashback.Op, opCommand *mongoproto.OpQuery, f *os.File) error {
	var err error
	op.Type = flashback.Command
	op.CommandDoc, err = ParseQuery(opCommand.Query)
	if err != nil {
		return err
	}
	return Write(op, f)
}

func HandleQuery(op *flashback.Op, opQuery *mongoproto.OpQuery, f *os.File) error {
	var err error
	op.Ns = opQuery.FullCollectionName
	op.Type = flashback.Query
	if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
		// sometimes mongoproto returns inserts as 'commands'
		collection, exists := flashback.GetElem(op.QueryDoc, "insert")
		if exists == true {
			opQuery.FullCollectionName = strings.Replace(opQuery.FullCollectionName, "$cmd", collection.(string), 1)
			return HandleInsertFromQuery(op, opQuery, f)
		} 
		return HandleCommand(op, opQuery, f)
	}
	op.QueryDoc, err = ParseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	return Write(op, f)
}

func HandleInsertDocument(op *flashback.Op, document bson.D, f *os.File) error {
	op.Type = flashback.Insert
	op.InsertDoc = document
	return Write(op, f)
}

func HandleInsert(op *flashback.Op, opInsert *mongoproto.OpInsert, f *os.File) error {
	op.Ns = opInsert.FullCollectionName
	if opInsert.Documents != nil {
		for _, document := range opInsert.Documents {
			insert, err := ParseQuery(document)
			if err != nil {
				return err
			}
			err = HandleInsertDocument(op, insert, f)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func HandleInsertFromQuery(op *flashback.Op, opQuery *mongoproto.OpQuery, f *os.File) error {
	op.Ns = opQuery.FullCollectionName
	query, err := ParseQuery(opQuery.Query)
	if err != nil {
		return err
	}
	documents, exists := flashback.GetElem(query, "documents")
	if exists == true {
		if (reflect.TypeOf(documents).Kind() == reflect.Slice) {
			for _, document := range documents.([]interface{}) {
				err = HandleInsertDocument(op, document.(bson.D), f)
				if err != nil {
					return err
				}
			}
		} else {
			err = HandleInsertDocument(op, documents.(bson.D), f)
			if err != nil {
				return err
			}
		}
	} 
	return nil
}

func HandleDelete(op *flashback.Op, opDelete *mongoproto.OpDelete, f *os.File) error {
	var err error
	op.Type = flashback.Remove
	op.Ns = opDelete.FullCollectionName
	op.QueryDoc, err = ParseQuery(opDelete.Selector)
	if err != nil {
		return err
	}
	return Write(op, f)
}

func Write(op *flashback.Op, f *os.File) error {
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
			fbOp := &flashback.Op{
				Timestamp: op.Seen,
			}
			// todo: fix mongoproto.OpUpdate and mongoproto.OpDelete so they can be added
			if opInsert, ok := op.Op.(*mongoproto.OpInsert); ok {
				err = HandleInsert(fbOp, opInsert, f)
			} else if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				err = HandleQuery(fbOp, opQuery, f)
			} else if *debug == true {
				if _, ok := op.Op.(*mongoproto.OpUnknown); ok {
					fmt.Println("Found mongoproto.OpUnknown operation: ", op)
				} else {
					fmt.Println("No known type for operation: ", op)
				}
			}
			if err != nil {
				logger.Println("Error with", fbOp.Type, "operation:", err)
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
