// Simple program which accepts a pcap file and prints a flashback-compatible ops stream
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/google/gopacket/pcap"
	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/json"
	"github.com/tmc/mongocaputils"
	"github.com/tmc/mongoproto"
	"gopkg.in/mgo.v2/bson"
)

var (
	pcapFile        = flag.String("f", "-", "pcap file (or '-' for stdin)")
	packetBufSize   = flag.Int("size", 1000, "size of packet buffer used for ordering within streams")
	continueOnError = flag.Bool("continue_on_error", false, "Continue parsing lines if an error is encountered")
)

// Converts the raw BSON doc in op.Query to extended JSON
func rawBSONToJSON(rawBSON []byte) (interface{}, error) {
	// Use bson.D to preserve order when unmarshalling
	// http://godoc.org/github.com/mongodb/mongo-tools/common/bsonutil#MarshalD
	var data bson.D
	if err := bson.Unmarshal(rawBSON, &data); err != nil {
		return nil, err
	}
	bsonAsJSON, err := bsonutil.ConvertBSONValueToJSON(data)
	if err != nil {
		return nil, err
	}
	return bsonAsJSON, nil
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
		for op := range m.Ops {
			// TODO: add other op types
			if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				fbOp := map[string]interface{}{}
				fbOp["ns"] = opQuery.FullCollectionName
				fbOp["ntoskip"] = opQuery.NumberToSkip
				fbOp["ntoreturn"] = opQuery.NumberToReturn
				fbOp["ts"] = json.Date(op.Seen.Unix())
				query, err := rawBSONToJSON(opQuery.Query)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
					fbOp["op"] = "command"
					fbOp["command"] = query
				} else {
					fbOp["op"] = "query"
					fbOp["query"] = query
				}
				fbOpStr, err := json.Marshal(fbOp)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				fmt.Println(string(fbOpStr))
			}
		}
	}()

	if err := h.Handle(m, -1); err != nil {
		fmt.Fprintln(os.Stderr, "pcap_converter: error handling packet stream:", err)
	}
	<-ch
}
