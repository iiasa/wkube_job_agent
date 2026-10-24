package services

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The /mnt/wdrv overlay is shared by every job of a project on the same node and is
// never cleared by hf-mount or the CSI driver. Anything a job leaves behind once it is
// done (failed uploads, crashes, rewrites keeping an old owner) has no value: it is
// never uploaded, and a leftover at a retry's output path turns the retry's create into
// an in-place rewrite that keeps the old owner. So each job, at start, deletes every
// overlay file that does not belong to a job still running on the node.
//
// Liveness: each running job keeps a heartbeat marker at /mnt/wdrv/.wagt_alive/<JOB_GID>,
// created before the job command runs and removed after its own files are uploaded.
// Markers live in the overlay itself, which is exactly the scope of jobs sharing it
// (same node, same project). All jobs are expected to run an agent with this heartbeat;
// a job on an older agent has no marker and its files are deleted by the next sweep.

const (
	wdrvRoot                = "/mnt/wdrv"
	overlayHeartbeatDirName = ".wagt_alive"
)

type overlaySweepConfig struct {
	HeartbeatStale time.Duration // a marker older than this means its job is gone
	DryRun         bool          // log what would be deleted without deleting
}

type overlaySweepResult struct {
	Scanned  int
	Deleted  int
	Bytes    int64
	SkipLive int
}

func loadOverlaySweepConfig() (overlaySweepConfig, bool) {
	if strings.EqualFold(os.Getenv("WAGT_OVERLAY_SWEEP_DISABLED"), "true") {
		return overlaySweepConfig{}, false
	}
	return overlaySweepConfig{
		HeartbeatStale: durationFromEnv("WAGT_OVERLAY_HEARTBEAT_STALE", 10*time.Minute),
		DryRun:         strings.EqualFold(os.Getenv("WAGT_OVERLAY_SWEEP_DRY_RUN"), "true"),
	}, true
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		fmt.Fprintf(MultiLogWriter, "warning: invalid %s=%q, using %s\n", key, raw, fallback)
		return fallback
	}
	return d
}

func heartbeatInterval() time.Duration {
	return durationFromEnv("WAGT_OVERLAY_HEARTBEAT_INTERVAL", 2*time.Minute)
}

var (
	heartbeatMu   sync.Mutex
	heartbeatStop chan struct{}
)

// StartOverlayHeartbeat creates this job's marker and refreshes it until StopOverlayHeartbeat.
// Without a marker other jobs' sweeps would delete this job's files, so callers must not
// run the job when it returns an error.
func StartOverlayHeartbeat() error {
	jobGid := os.Getenv("JOB_GID")
	if jobGid == "" {
		return nil
	}
	marker := filepath.Join(wdrvRoot, overlayHeartbeatDirName, jobGid)
	if err := writeHeartbeat(marker); err != nil {
		return fmt.Errorf("could not create overlay heartbeat %s: %w", marker, err)
	}

	heartbeatMu.Lock()
	defer heartbeatMu.Unlock()
	if heartbeatStop != nil {
		return nil
	}
	stop := make(chan struct{})
	heartbeatStop = stop

	go func() {
		ticker := time.NewTicker(heartbeatInterval())
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := writeHeartbeat(marker); err != nil {
					fmt.Fprintf(MultiLogWriter, "warning: could not refresh overlay heartbeat %s: %v\n", marker, err)
				}
			}
		}
	}()
	return nil
}

// StopOverlayHeartbeat stops refreshing and removes this job's marker. Call it only
// after the job's own overlay files have been uploaded: anything left after this is
// deleted by the next sweep on the node.
func StopOverlayHeartbeat() {
	heartbeatMu.Lock()
	if heartbeatStop != nil {
		close(heartbeatStop)
		heartbeatStop = nil
	}
	heartbeatMu.Unlock()

	jobGid := os.Getenv("JOB_GID")
	if jobGid == "" {
		return
	}
	marker := filepath.Join(wdrvRoot, overlayHeartbeatDirName, jobGid)
	if err := os.Remove(marker); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(MultiLogWriter, "warning: could not remove overlay heartbeat %s: %v\n", marker, err)
	}
}

// writeHeartbeat rewrites the marker content rather than only touching its mtime, so the
// new mtime lands on the physical overlay file and survives a mount pod restart.
func writeHeartbeat(marker string) error {
	if err := os.MkdirAll(filepath.Dir(marker), 0775); err != nil {
		return err
	}
	return os.WriteFile(marker, []byte(strconv.FormatInt(time.Now().Unix(), 10)+"\n"), 0664)
}

// SweepStaleOverlayFiles deletes every overlay file that no running job owns.
func SweepStaleOverlayFiles() {
	cfg, enabled := loadOverlaySweepConfig()
	if !enabled {
		fmt.Fprintln(MultiLogWriter, "Overlay sweep disabled via WAGT_OVERLAY_SWEEP_DISABLED")
		return
	}

	rawGid := os.Getenv("JOB_GID")
	ownGid, err := strconv.ParseUint(rawGid, 10, 32)
	if err != nil {
		fmt.Fprintf(MultiLogWriter, "warning: overlay sweep skipped, no valid JOB_GID (%q)\n", rawGid)
		return
	}

	// Read the index before scanning markers: every file in this snapshot was written by
	// a job whose marker already existed, so a job starting mid-sweep is never missed.
	entries, err := readOverlayWrites()
	if err != nil {
		fmt.Fprintf(MultiLogWriter, "warning: overlay sweep skipped, cannot read %s: %v\n", overlayWritesIndex, err)
		return
	}

	fmt.Fprintf(MultiLogWriter, "Overlay sweep started (heartbeat stale after %s, dry run %v)\n", cfg.HeartbeatStale, cfg.DryRun)
	res := sweepOverlay(wdrvRoot, entries, uint32(ownGid), cfg, time.Now())
	verb := "deleted"
	if cfg.DryRun {
		verb = "would delete"
	}
	fmt.Fprintf(MultiLogWriter, "Overlay sweep finished: scanned %d, %s %d files (%d bytes), kept %d of running jobs\n",
		res.Scanned, verb, res.Deleted, res.Bytes, res.SkipLive)
}

func sweepOverlay(root string, entries []overlayEntry, ownGid uint32, cfg overlaySweepConfig, now time.Time) overlaySweepResult {
	var res overlaySweepResult
	live := liveHeartbeatGids(root, cfg, now)
	emptiedDirs := map[string]struct{}{}

	for _, entry := range entries {
		if isAgentOverlayFile(entry.RelPath) || entry.Gid == ownGid {
			continue
		}
		res.Scanned++

		if live[entry.Gid] {
			res.SkipLive++
			continue
		}

		path := filepath.Join(root, entry.RelPath)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}

		if cfg.DryRun {
			fmt.Fprintf(MultiLogWriter, "[overlay sweep] would delete %s (gid %d, mtime %s)\n", path, entry.Gid, info.ModTime().Format(time.RFC3339))
		} else {
			if err := os.Remove(path); err != nil {
				fmt.Fprintf(MultiLogWriter, "[overlay sweep] failed to delete %s: %v\n", path, err)
				continue
			}
			fmt.Fprintf(MultiLogWriter, "[overlay sweep] deleted %s (gid %d, mtime %s)\n", path, entry.Gid, info.ModTime().Format(time.RFC3339))
			emptiedDirs[filepath.Dir(path)] = struct{}{}
		}
		res.Deleted++
		res.Bytes += info.Size()
	}

	// Remove directories the sweep emptied only while no other job runs on the node: a
	// parallel job may have just run `mkdir -p` on one and be about to create a file in
	// it. Markers are re-read here so a job that started during the sweep is noticed.
	if len(emptiedDirs) > 0 {
		if otherJobRunning(root, ownGid, cfg) {
			fmt.Fprintf(MultiLogWriter, "[overlay sweep] other jobs running on this node, keeping %d emptied directories\n", len(emptiedDirs))
		} else {
			for dir := range emptiedDirs {
				removeEmptyParents(root, dir)
			}
		}
	}
	return res
}

func otherJobRunning(root string, ownGid uint32, cfg overlaySweepConfig) bool {
	for gid := range liveHeartbeatGids(root, cfg, time.Now()) {
		if gid != ownGid {
			return true
		}
	}
	return false
}

// liveHeartbeatGids returns gids with a fresh marker and removes markers of dead jobs.
func liveHeartbeatGids(root string, cfg overlaySweepConfig, now time.Time) map[uint32]bool {
	live := map[uint32]bool{}
	dir := filepath.Join(root, overlayHeartbeatDirName)
	items, err := os.ReadDir(dir)
	if err != nil {
		return live
	}
	for _, item := range items {
		gid, err := strconv.ParseUint(item.Name(), 10, 32)
		if err != nil {
			continue
		}
		info, err := item.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) < cfg.HeartbeatStale {
			live[uint32(gid)] = true
			continue
		}
		if !cfg.DryRun {
			_ = os.Remove(filepath.Join(dir, item.Name()))
		}
	}
	return live
}

// removeEmptyParents removes dir and its parents up to (not including) root while they are empty.
func removeEmptyParents(root, dir string) {
	root = filepath.Clean(root)
	for dir = filepath.Clean(dir); dir != root && strings.HasPrefix(dir, root+string(os.PathSeparator)); dir = filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			return
		}
	}
}
