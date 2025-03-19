package config

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	logCounter int
	counterMu  sync.Mutex
)

// Custom remote logger
type RemoteLogger struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	tick *time.Ticker
}

func NewRemoteLogger() *RemoteLogger {
	rl := &RemoteLogger{
		tick: time.NewTicker(10 * time.Second),
	}

	// Periodically send logs
	go func() {
		for range rl.tick.C {
			if err := rl.Send(); err != nil {
				fmt.Fprintf(os.Stdout, "Failed to send logs to remote sink: %v", err)
			}
		}
	}()

	return rl
}

const maxBufferSize = 5 * 1024 * 1024

var trimMsg = []byte("\n[Logs trimmed due to buffer size. Please log to file for full logs...]\n")

func (rl *RemoteLogger) Write(p []byte) (n int, err error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	requiredSpace := rl.buf.Len() + len(p) - maxBufferSize
	if requiredSpace > 0 {
		rl.buf.Next(requiredSpace)
		rl.buf.Write(trimMsg)
	}

	return rl.buf.Write(p)
}

func (rl *RemoteLogger) Send() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.buf.Len() > 0 {
		// Generate log filename
		counterMu.Lock()
		logFilename := fmt.Sprintf("wkube%d", logCounter)
		logCounter++
		counterMu.Unlock()

		// Send log batch
		data := rl.buf.Bytes()
		rl.buf.Reset()
		err := SendBatch(data, logFilename)
		if err != nil {
			return fmt.Errorf("error sending logs - %v", err)
		}
	} else {
		// Buffer is empty, still check health
		if err := CheckHealth(); err != nil {
			return fmt.Errorf("health check mechanism failed - %v", err)
		}
	}

	return nil
}
