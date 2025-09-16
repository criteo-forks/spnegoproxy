package main

import (
	"os"
	"time"

	"github.com/matchaxnb/spnegoproxy/spnegoproxy"

	"log/slog"
)

func main() {
	handler := slog.NewTextHandler(os.Stdout, nil)
	buffered := spnegoproxy.NewBufferedLogger(handler, 5*time.Second, 200, 1024*1024)
	logger := spnegoproxy.NewStdLogger(buffered)

	for i := 0; i < 100; i++ {
		logger.Println("Hello world")
		logger.Println("Hello world") // Will be deduplicated
		logger.Println("Hello plommy")
	}

	for i := 0; i < 5; i++ {
		time.Sleep(time.Second * 10)
		println("slept 10 seconds")
		logger.Panicf("We have paniced %d\n", i)
	}
	defer buffered.Close()
}
