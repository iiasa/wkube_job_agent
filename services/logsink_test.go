package services

import (
	"regexp"
	"strings"
	"testing"
)

var stampRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\t`)

func drain(rl *RemoteLogger) string {
	var b strings.Builder
	for {
		select {
		case p := <-rl.logChan:
			b.Write(p)
		default:
			return b.String()
		}
	}
}

func TestRemoteLoggerStampsEachLineStartOnce(t *testing.T) {
	rl := &RemoteLogger{logChan: make(chan []byte, 10), atLineStart: true}

	// A line split across writes gets one prefix; every new line gets its own.
	rl.Write([]byte("downloading par"))
	rl.Write([]byte("t 3\nTraceback\n  File x\n"))
	rl.Write([]byte("progress 40%"))

	lines := strings.Split(drain(rl), "\n")
	want := []string{"downloading part 3", "Traceback", "  File x", "progress 40%"}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines %q, want %d", len(lines), lines, len(want))
	}
	for i, line := range lines {
		if !stampRe.MatchString(line) {
			t.Fatalf("line %d %q has no timestamp prefix", i, line)
		}
		if got := stampRe.ReplaceAllString(line, ""); got != want[i] {
			t.Fatalf("line %d = %q, want %q", i, got, want[i])
		}
	}
}

func TestRemoteLoggerKeepsLineStateWhenChannelIsFull(t *testing.T) {
	rl := &RemoteLogger{logChan: make(chan []byte, 1), atLineStart: true}

	rl.Write([]byte("kept\n"))
	rl.Write([]byte("dropped\n")) // channel full: counted, not sent
	if rl.droppedLogs != 1 {
		t.Fatalf("droppedLogs = %d, want 1", rl.droppedLogs)
	}
	drain(rl)
	rl.Write([]byte("after\n"))
	if got := drain(rl); !stampRe.MatchString(got) {
		t.Fatalf("line after a dropped write should still be stamped, got %q", got)
	}
}
