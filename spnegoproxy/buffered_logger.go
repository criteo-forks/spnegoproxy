package spnegoproxy

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"log/slog"
)

type bufferedLog struct {
	message string
	count   int
	size    int
}

type BufferedLogger struct {
	handler  slog.Handler
	mu       sync.Mutex
	buffer   []*bufferedLog
	maxLines int
	maxSize  int
	currSize int
	ticker   *time.Ticker
	done     chan bool
}

func NewBufferedLogger(handler slog.Handler, flushInterval time.Duration, maxLines int, maxSize int) *BufferedLogger {
	bl := &BufferedLogger{
		handler:  handler,
		buffer:   make([]*bufferedLog, 0, maxLines),
		maxLines: maxLines,
		maxSize:  maxSize,
		currSize: 0,
		ticker:   time.NewTicker(flushInterval),
		done:     make(chan bool),
	}

	go bl.periodicFlush()
	return bl
}

func (bl *BufferedLogger) Log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	size := len(msg)
	for _, attr := range attrs {
		size += len(attr.String())
	}

	// Deduplication
	for _, entry := range bl.buffer {
		if entry.message == msg {
			entry.count++
			return
		}
	}

	// Add new log entry
	entry := &bufferedLog{
		message: msg,
		count:   1,
		size:    size,
	}
	bl.buffer = append(bl.buffer, entry)
	bl.currSize += size

	// Trim buffer if needed
	for len(bl.buffer) > bl.maxLines || bl.currSize > bl.maxSize {
		removed := bl.buffer[0]
		bl.currSize -= removed.size
		bl.buffer = bl.buffer[1:]
	}
}

func (bl *BufferedLogger) periodicFlush() {
	for {
		select {
		case <-bl.ticker.C:
			bl.Flush()
		case <-bl.done:
			return
		}
	}
}

func (bl *BufferedLogger) Flush() {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	for _, entry := range bl.buffer {
		msg := entry.message
		if entry.count > 1 {
			msg = fmt.Sprintf("%s [%d times]", msg, entry.count)
		}
		bl.handler.Handle(context.Background(), slog.Record{
			Time:    time.Now(),
			Level:   slog.LevelInfo,
			Message: msg,
		})
	}
	bl.buffer = bl.buffer[:0]
	bl.currSize = 0
}

func (bl *BufferedLogger) Close() {
	bl.done <- true
	bl.ticker.Stop()
	bl.Flush()
}

// Drop-in replacement for log.Logger
type StdLogger struct {
	bl *BufferedLogger
}

func NewStdLogger(bl *BufferedLogger) *log.Logger {
	return log.New(&StdLogger{bl: bl}, "", 0)
}

func (sl *StdLogger) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	sl.bl.Log(context.Background(), slog.LevelInfo, msg)
	return len(p), nil
}
