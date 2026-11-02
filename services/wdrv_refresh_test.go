package services

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestReadRefreshFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".hf_refresh")
	if err := os.WriteFile(path, []byte("ok\n"), 0444); err != nil {
		t.Fatal(err)
	}
	if err := readRefreshFile(path, time.Second); err != nil {
		t.Fatalf("readRefreshFile: %v", err)
	}
}

func TestReadRefreshFileMissing(t *testing.T) {
	err := readRefreshFile(filepath.Join(t.TempDir(), ".hf_refresh"), time.Second)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want not-exist", err)
	}
}

// A FIFO with no writer blocks in open, like a mount that stopped answering.
func TestReadRefreshFileTimesOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".hf_refresh")
	if err := syscall.Mkfifo(path, 0644); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	err := readRefreshFile(path, 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("got %v, want timeout", err)
	}
}
