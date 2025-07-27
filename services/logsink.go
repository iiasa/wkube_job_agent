package services

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	logCounter int
	counterMu  sync.Mutex
)

const (
	logChannelSize   = 1000
	logFlushInterval = 10 * time.Second
)

var omittedMsgPrefix = "\n[Logs omitted: %d messages dropped due to full channel]\n"

type RemoteLogger struct {
	logChan     chan []byte
	ctx         context.Context
	droppedLogs int
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewRemoteLogger(ctx context.Context, cancel context.CancelFunc) *RemoteLogger {
	rl := &RemoteLogger{
		logChan: make(chan []byte, logChannelSize),
		ctx:     ctx,
	}

	rl.wg.Add(1)
	go rl.run(cancel)

	return rl
}

func (rl *RemoteLogger) Write(p []byte) (int, error) {
	select {
	case rl.logChan <- append([]byte(nil), p...):
	default:
		rl.mu.Lock()
		rl.droppedLogs++
		rl.mu.Unlock()
	}
	return len(p), nil
}

func (rl *RemoteLogger) run(cancel context.CancelFunc) {
	tick := time.NewTicker(logFlushInterval)
	defer tick.Stop()
	defer rl.wg.Done()

	for {
		select {
		case <-tick.C:
			rl.flushFromChannel(cancel)
		case <-rl.ctx.Done():
			rl.flushFromChannel(cancel)
			return
		}
	}
}

func (rl *RemoteLogger) flushFromChannel(cancel context.CancelFunc) {
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
		if err := CheckHealth(cancel); err != nil {
			fmt.Fprintf(MultiLogWriter, "error in health check function: %v\n", err)
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

	if err := SendBatch(buf.Bytes(), logFilename, cancel); err != nil {
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

// Optional: for use in tests or shutdown sync
func (rl *RemoteLogger) Wait() {
	rl.wg.Wait()
}

func (rl *RemoteLogger) FinalFlush() {
	rl.flushFromChannel(nil)
}
