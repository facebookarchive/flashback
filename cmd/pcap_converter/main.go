// Simple program which accepts a pcap file and prints a flashback-compatible ops stream
package main

import (
	"flag"
	"time"
	"fmt"
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

//func (fbOp *Operation) getOpNs(fullCollName string, query bson.D) string {
//	if _, exists := flashback.GetElem(query, ""
//
//
//}

func parseQuery(opQuery []byte) (bson.D, error) {
	var query bson.D
	err := bson.Unmarshal(opQuery, &query)
	return query, err
}

func (fbOp *Operation) handleCommand(query bson.D, f *os.File) error {
	fbOp.Type = flashback.Command
	fbOp.CommandDoc = query
	return fbOp.writeOp(f)
}

func (fbOp *Operation) handleQuery(query bson.D, f *os.File) error {
	fbOp.Type = flashback.Query
	fbOp.QueryDoc = query
	return fbOp.writeOp(f)
}

func (fbOp *Operation) handleInsert(query bson.D, f *os.File) error {
	var err error
	inserts := []*Operation{}
	fbOp.Type = flashback.Insert
	documents, _ := flashback.GetElem(query, "documents")
	if (reflect.TypeOf(documents).Kind() == reflect.Slice) {
		for _, document := range documents.([]interface{}) {
			multiInsertOp := &Operation{
				Ns: fbOp.Ns,
				Timestamp: fbOp.Timestamp,
				InsertDoc: document.(bson.D),
				Type: fbOp.Type,
			}
			inserts = append(inserts, multiInsertOp)
		}
	} else {
		fbOp.InsertDoc = documents.(bson.D)
		inserts = append(inserts, fbOp)
	}
	for _, insert := range inserts {
		err = insert.writeOp(f)
		if err != nil {
			return err
		}
	}
	return err 
}

func (op *Operation) writeOp(f *os.File) error {
//func writeOp(f *os.File, op *Operation) error {
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
			// TODO: add other op types
			if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				fbOp := &Operation{
					Ns: opQuery.FullCollectionName,
					NToSkip: opQuery.NumberToSkip,
					NToReturn: opQuery.NumberToReturn,
					Timestamp: op.Seen,
				}
				query, err := parseQuery(opQuery.Query)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				fmt.Println(opQuery)
				if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
					collection, exists := flashback.GetElem(query, "insert")
					if exists == true {
						fbOp.Ns = strings.Replace(fbOp.Ns, "$cmd", collection.(string), 1)
						err = fbOp.handleInsert(query, f)
					} else {
						err = fbOp.handleCommand(query, f)
					}
				} else {
					_, exists := flashback.GetElem(query, "insert")
					if exists == true {
						err = fbOp.handleInsert(query, f)
					} else {
						err = fbOp.handleQuery(query, f)
					}
				}
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
			}
		}
	}()

	if err := h.Handle(m, -1); err != nil {
		fmt.Fprintln(os.Stderr, "pcap_converter: error handling packet stream:", err)
	}
	<-ch
}
