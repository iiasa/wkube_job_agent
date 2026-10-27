package services

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseDebugFlag(t *testing.T) {
	for _, v := range []string{"true", "TRUE", "1", "yes", " Yes "} {
		if !parseDebugFlag(v) {
			t.Errorf("parseDebugFlag(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"", "false", "0", "no", "on", "enabled"} {
		if parseDebugFlag(v) {
			t.Errorf("parseDebugFlag(%q) = true, want false", v)
		}
	}
}

func TestDebugf(t *testing.T) {
	oldWriter := MultiLogWriter
	defer func() {
		MultiLogWriter = oldWriter
		debugForTest = nil
	}()

	for _, enabled := range []bool{false, true} {
		var buf bytes.Buffer
		MultiLogWriter = &buf
		debugForTest = &enabled

		debugf("queueing %s (%d bytes)", "a.rds", 42)

		got := buf.String()
		if !enabled && got != "" {
			t.Errorf("debug off: got output %q, want none", got)
		}
		if enabled && got != "[DEBUG] queueing a.rds (42 bytes)\n" {
			t.Errorf("debug on: got %q", got)
		}
	}
}

func TestOverlayFailureDetailsIgnoreDebugFlag(t *testing.T) {
	oldWriter := MultiLogWriter
	defer func() {
		MultiLogWriter = oldWriter
		debugForTest = nil
	}()
	off := false
	debugForTest = &off
	var buf bytes.Buffer
	MultiLogWriter = &buf

	logOverlayFailureDetails("heartbeat remove", t.TempDir()+"/missing", nil)

	if !strings.HasPrefix(buf.String(), "[DIAG] heartbeat remove failed on ") {
		t.Errorf("failure details must be logged even with debug off, got %q", buf.String())
	}
}
