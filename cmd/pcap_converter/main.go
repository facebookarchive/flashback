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
	"github.com/tmc/mongocaputils"
	"github.com/tmc/mongoproto"
	"gopkg.in/mgo.v2/bson"
	"github.com/ParsePlatform/flashback"
)

var (
	pcapFile        = flag.String("f", "-", "pcap file (or '-' for stdin)")
	bsonFile	= flag.String("o", "flashback.bson", "bson output file, will be overwritten")
	packetBufSize   = flag.Int("size", 1000, "size of packet buffer used for ordering within streams")
	continueOnError = flag.Bool("continue_on_error", false, "Continue parsing lines if an error is encountered")
)

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
				fbOp := &flashback.Op{
					Ns: opQuery.FullCollectionName,
					NToSkip: int64(opQuery.NumberToSkip),
					NToReturn: int64(opQuery.NumberToReturn),
					Timestamp: op.Seen,
				}
				var query bson.D
				err := bson.Unmarshal(opQuery.Query, &query)
				if err != nil {
					logger.Println(err)
					if !*continueOnError {
						os.Exit(1)
					}
				}
				if strings.HasSuffix(opQuery.FullCollectionName, ".$cmd") {
					fbOp.Type = "command"
					fbOp.CommandDoc = query
				} else {
					fbOp.Type = "query"
					fbOp.QueryDoc = query
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
