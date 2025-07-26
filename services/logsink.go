package services

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

var (
	logCounter int
	counterMu  sync.Mutex
)

const (
	logChannelSize   = 1000 // Drop logs if channel full
	logFlushInterval = 10 * time.Second
)

var omittedMsgPrefix = "\n[Logs omitted: %d messages dropped due to full channel]\n"

// Custom remote logger
type RemoteLogger struct {
	logChan     chan []byte
	done        chan struct{}
	droppedLogs int
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewRemoteLogger() *RemoteLogger {
	rl := &RemoteLogger{
		logChan: make(chan []byte, logChannelSize),
		done:    make(chan struct{}),
	}

	rl.wg.Add(1)
	go rl.run()

	return rl
}

func (rl *RemoteLogger) Write(p []byte) (int, error) {
	select {
	case rl.logChan <- append([]byte(nil), p...): // Copy to avoid shared memory issues
	default:
		rl.mu.Lock()
		rl.droppedLogs++
		rl.mu.Unlock()
	}
	return len(p), nil
}

func (rl *RemoteLogger) run() {
	tick := time.NewTicker(logFlushInterval)
	defer tick.Stop()
	defer rl.wg.Done()

	for {
		select {
		case <-tick.C:
			rl.flushFromChannel()

		case <-rl.done:
			rl.flushFromChannel()
			return
		}
	}
}

func (rl *RemoteLogger) flushFromChannel() {
	var batch [][]byte

DrainLoop:
	for {
		select {
		case log := <-rl.logChan:
			batch = append(batch, log)
		default:
			break DrainLoop
		}
	}

	dropped := rl.getAndResetDroppedCount()

	if len(batch) == 0 && dropped == 0 {
		// Still check health even if nothing to flush
		if err := CheckHealth(); err != nil {
			fmt.Fprintf(MultiLogWriter, "Health check failed: %v\n", err)
		}
		return
	}

	var buf bytes.Buffer

	if dropped > 0 {
		buf.WriteString(fmt.Sprintf(omittedMsgPrefix, dropped))
	}

	for _, log := range batch {
		buf.Write(log)
	}

	counterMu.Lock()
	logFilename := fmt.Sprintf("wkube%d", logCounter)
	logCounter++
	counterMu.Unlock()

	if err := SendBatch(buf.Bytes(), logFilename); err != nil {
		fmt.Fprintf(MultiLogWriter, "Failed to send logs to remote sink: %v\n", err)
	}
}

func (rl *RemoteLogger) getAndResetDroppedCount() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	dropped := rl.droppedLogs
	rl.droppedLogs = 0
	return dropped
}

func (rl *RemoteLogger) Close() {
	close(rl.done)
	rl.wg.Wait()
}
