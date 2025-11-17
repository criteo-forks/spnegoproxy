package main

import (
	"flag"
	"log"
	"net"
	"os"

	"github.com/matchaxnb/spnegoproxy/spnegoproxy"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

func main() {
	addr := flag.String("addr", "0.0.0.0:50070", "bind address")
	toProxy := flag.String("proxy-service", "", "host:port for the service to proxy to")
	properUsername := flag.String("proper-username", "", "for WebHDFS, user.name value to force-set")
	dropUsername := flag.Bool("drop-username", false, "drop user.name from all queries")
	metricsAddrS := flag.String("metrics-addr", "", "optional address to expose a prometheus metrics endpoint")
	debug := flag.Bool("debug", true, "turn on debugging")
	flag.Parse()
	spnegoproxy.DEBUGGING = *debug
	if *debug {
		logger.Printf("Listening on %s\n", *addr)
	}
	if len(*toProxy) == 0 {
		logger.Fatal("Need to provide -proxy-service flag")
	}
	listenAddr, err := net.ResolveTCPAddr("tcp", *addr)
	if err != nil {
		logger.Panicf("Wrong TCP address %s -> %s", *addr, err)
	}
	connListener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		logger.Panic(err)
	}

	eventChannel := make(spnegoproxy.WebHDFSEventChannel)
	delegationTokenChannel := make(chan spnegoproxy.StringAndError, 3) // don't keep more delegation tokens in chan than necessary
	go func() {                                                        // dummy delegation token generator
		for {
			delegationTokenChannel <- spnegoproxy.StringAndError{S: "", E: nil}
		}
	}()
	if len(*metricsAddrS) > 0 {
		// we have a prometheus metrics endpoint
		logger.Print("Starting metrics handler")
		spnegoproxy.EnableWebHDFSTracking(eventChannel)
		spnegoproxy.ExposeMetrics(*metricsAddrS, eventChannel)
		go spnegoproxy.ConsumeWebHDFSEventStream(eventChannel)
	}

	if *dropUsername {
		spnegoproxy.DropUsername()
	} else if len(*properUsername) > 0 {
		spnegoproxy.EnforceUserName(*properUsername)
	}
	errorCount := 0
	defer connListener.Close()
	for {
		conn, err := connListener.AcceptTCP()
		if err != nil {
			logger.Panic(err)
		}
		go spnegoproxy.HandleClient(conn, *toProxy, nil, delegationTokenChannel, &errorCount)
	}
}
