// Simple program which accepts a pcap file and prints a flashback-compatible ops stream
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"runtime"
	"strings"

	"github.com/google/gopacket/pcap"
	"github.com/tmc/mongocaputils"
	"github.com/tmc/mongoproto"
	"gopkg.in/mgo.v2/bson"
)

var (
	pcapFile        = flag.String("f", "-", "pcap file (or '-' for stdin)")
	bsonFile	= flag.String("o", "flashback.bson", "bson output file")
	packetBufSize   = flag.Int("size", 1000, "size of packet buffer used for ordering within streams")
	continueOnError = flag.Bool("continue_on_error", false, "Continue parsing lines if an error is encountered")
)

type FlashbackOperation struct {
	Command	interface{}	`bson:"command,omitempty"`
	Query	interface{}	`bson:"query,omitempty"`
	Ns	string		`bson:"ns"`
	Ts	time.Time	`bson:"ts"`
	Op	string		`bons:"op"`
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
		}
		defer f.Close()
		for op := range m.Ops {
			// TODO: add other op types
			if opQuery, ok := op.Op.(*mongoproto.OpQuery); ok {
				fbOp := &FlashbackOperation{
					Ns: opQuery.FullCollectionName,
					Ts: op.Seen,
				}
				var query interface{}
				err := bson.Unmarshal(opQuery.Query, &query)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
					fbOp.Op = "command"
					fbOp.Command = query
				} else {
					fbOp.Op = "query"
					fbOp.Query = query
				}
				fbOpStr, err := bson.Marshal(fbOp)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				f.Write(fbOpStr)
			}
		}
	}()

	if err := h.Handle(m, -1); err != nil {
		fmt.Fprintln(os.Stderr, "pcap_converter: error handling packet stream:", err)
	}
	<-ch
}
