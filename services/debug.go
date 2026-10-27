package services

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Debug output is controlled by the WAGT_DEBUG environment variable ("true", "1" or
// "yes", case-insensitive). It is off by default and read once per process.
const debugEnvVar = "WAGT_DEBUG"

var (
	debugOnce    sync.Once
	debugOn      bool
	debugForTest *bool // set by tests to bypass the environment
)

func debugEnabled() bool {
	if debugForTest != nil {
		return *debugForTest
	}
	debugOnce.Do(func() {
		debugOn = parseDebugFlag(os.Getenv(debugEnvVar))
		if debugOn {
			fmt.Fprintf(MultiLogWriter, "Debug logging enabled (%s)\n", debugEnvVar)
		}
	})
	return debugOn
}

func parseDebugFlag(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes":
		return true
	}
	return false
}

// debugf writes a "[DEBUG] " prefixed line to the job log when WAGT_DEBUG is enabled.
func debugf(format string, args ...any) {
	if !debugEnabled() {
		return
	}
	fmt.Fprintf(MultiLogWriter, "[DEBUG] "+format+"\n", args...)
}

// logOverlayFailureDetails records, after a failed operation on an overlay path, what
// the kernel decided from: the errno, this process's ids, the owner and mode reported
// for the path and its parent directory, and the id maps of this process. Failures on
// /mnt/wdrv are intermittent, so the state is captured at the moment of the failure.
//
// It runs only after an error and is cheap, so it is always on and not tied to
// WAGT_DEBUG. Lines are prefixed "[DIAG]".
func logOverlayFailureDetails(op, path string, opErr error) {
	errno := "unknown"
	var en syscall.Errno
	if errors.As(opErr, &en) {
		errno = fmt.Sprintf("%d (%s)", int(en), en.Error())
	}
	fmt.Fprintf(MultiLogWriter, "[DIAG] %s failed on %s: errno=%s; process uid=%d gid=%d euid=%d egid=%d\n",
		op, path, errno, os.Getuid(), os.Getgid(), os.Geteuid(), os.Getegid())
	for _, p := range []string{path, filepath.Dir(path)} {
		fmt.Fprintf(MultiLogWriter, "[DIAG]   %s\n", describeOwnership(p))
	}
	// Which ids this process can represent: a uid/gid outside these maps is shown by
	// stat as the overflow id (65534) and makes the kernel refuse writes and deletes.
	for _, f := range []string{"/proc/self/uid_map", "/proc/self/gid_map"} {
		fmt.Fprintf(MultiLogWriter, "[DIAG]   %s: %s\n", f, readCompact(f))
	}
	if groups, err := os.Getgroups(); err == nil {
		fmt.Fprintf(MultiLogWriter, "[DIAG]   supplementary groups: %v\n", groups)
	}
}

// readCompact returns a small /proc file as one line, or the read error.
func readCompact(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("read error: %v", err)
	}
	return strings.Join(strings.Fields(string(data)), " ")
}

func describeOwnership(path string) string {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Sprintf("%s: lstat error: %v", path, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Sprintf("%s: mode=%v (no stat details)", path, info.Mode())
	}
	return fmt.Sprintf("%s: uid=%d gid=%d mode=%v ino=%d nlink=%d size=%d",
		path, st.Uid, st.Gid, info.Mode(), st.Ino, st.Nlink, st.Size)
}
