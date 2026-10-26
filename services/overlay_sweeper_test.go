package services

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeAgedFile(t *testing.T, root, rel string, age time.Duration, now time.Time) string {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	mtime := now.Add(-age)
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

func TestSweepOverlay(t *testing.T) {
	oldWriter := MultiLogWriter
	MultiLogWriter = io.Discard
	defer func() { MultiLogWriter = oldWriter }()

	const (
		ownGid      = 400524
		liveGid     = 731691
		deadGid     = 512345 // crashed hard: marker left behind, now stale
		finishedGid = 623456 // finalize removed its marker, upload failed
	)
	cfg := overlaySweepConfig{HeartbeatStale: 15 * time.Minute}

	setup := func(t *testing.T) (string, time.Time, map[string]string, []overlayEntry) {
		root := t.TempDir()
		now := time.Now()
		files := map[string]string{
			"unownedOld":    writeAgedFile(t, root, "globiom-results/13009/a.rds", 30*24*time.Hour, now),
			"unownedFresh":  writeAgedFile(t, root, "globiom-results/15228/b.rds", time.Minute, now),
			"fsGroupOld":    writeAgedFile(t, root, "globiom-results/12726/c.rds", 30*24*time.Hour, now),
			"liveOld":       writeAgedFile(t, root, "globiom-results/long-job/d.rds", 30*24*time.Hour, now),
			"deadFresh":     writeAgedFile(t, root, "globiom-results/crashed/e.rds", time.Minute, now),
			"finishedFresh": writeAgedFile(t, root, "globiom-results/failed-upload/g.rds", time.Minute, now),
			"ownFresh":      writeAgedFile(t, root, "globiom-results/mine/f.rds", time.Minute, now),
			"refresh":       writeAgedFile(t, root, "globiom-results/.wagt_refresh_1_2", 30*24*time.Hour, now),
			"liveMarker":    writeAgedFile(t, root, ".wagt_alive/731691", time.Minute, now),
			"deadMarker":    writeAgedFile(t, root, ".wagt_alive/512345", 20*time.Minute, now),
		}
		entries := []overlayEntry{
			{"globiom-results/13009/a.rds", 0, 0},
			{"globiom-results/15228/b.rds", 0, 0},
			{"globiom-results/12726/c.rds", 65534, 0},
			{"globiom-results/long-job/d.rds", liveGid, 0},
			{"globiom-results/crashed/e.rds", deadGid, 0},
			{"globiom-results/failed-upload/g.rds", finishedGid, 0},
			{"globiom-results/mine/f.rds", ownGid, 0},
			{"globiom-results/.wagt_refresh_1_2", 0, 0},
			{".wagt_alive/731691", liveGid, 0},
			{".wagt_alive/512345", deadGid, 0},
			{"globiom-results/gone/x.rds", 0, 0}, // listed but already removed
		}
		return root, now, files, entries
	}

	t.Run("deletes every file no running job owns, regardless of age", func(t *testing.T) {
		root, now, files, entries := setup(t)

		res := sweepOverlay(root, entries, ownGid, cfg, now)

		for _, k := range []string{"unownedOld", "unownedFresh", "fsGroupOld", "deadFresh", "finishedFresh", "deadMarker"} {
			if exists(files[k]) {
				t.Errorf("%s should have been deleted", k)
			}
		}
		for _, k := range []string{"liveOld", "ownFresh", "refresh", "liveMarker"} {
			if !exists(files[k]) {
				t.Errorf("%s should have been kept", k)
			}
		}
		if res.Deleted != 5 || res.SkipLive != 1 {
			t.Errorf("got deleted=%d skipLive=%d, want 5 and 1", res.Deleted, res.SkipLive)
		}
		if !exists(filepath.Join(root, "globiom-results", "13009")) {
			t.Errorf("emptied directory should be kept while another job is running")
		}
	})

	t.Run("removes emptied directories when no other job is running", func(t *testing.T) {
		root, now, files, entries := setup(t)
		if err := os.Remove(files["liveMarker"]); err != nil {
			t.Fatal(err)
		}

		sweepOverlay(root, entries, ownGid, cfg, now)

		if exists(filepath.Join(root, "globiom-results", "13009")) {
			t.Errorf("emptied directory should have been removed")
		}
		if !exists(filepath.Join(root, "globiom-results", "mine")) {
			t.Errorf("directory holding this job's own file should have been kept")
		}
	})

	t.Run("dry run deletes nothing", func(t *testing.T) {
		root, now, files, entries := setup(t)
		dry := cfg
		dry.DryRun = true

		res := sweepOverlay(root, entries, ownGid, dry, now)

		for k, p := range files {
			if !exists(p) {
				t.Errorf("%s was deleted in dry run", k)
			}
		}
		if res.Deleted != 5 {
			t.Errorf("dry run should report 5 candidates, got %d", res.Deleted)
		}
	})
}

func TestParseOverlayWritesAndAgentFiles(t *testing.T) {
	oldWriter := MultiLogWriter
	MultiLogWriter = io.Discard
	defer func() { MultiLogWriter = oldWriter }()

	entries := parseOverlayWrites(
		"out/x.rds:731691:1761000000\n" +
			"\n" +
			"log:2026:run.lst:400524:1761000123\n" + // paths may contain ':'
			"no-fields\n" +
			"a/b.rds:400524\n" + // missing mtime
			"x/y:notanumber:1761000000\n" +
			"x/z:400524:notanumber\n" +
			".wagt_alive/400524:400524:1761000200\n")
	want := []overlayEntry{
		{RelPath: "out/x.rds", Gid: 731691, Mtime: 1761000000},
		{RelPath: "log:2026:run.lst", Gid: 400524, Mtime: 1761000123},
		{RelPath: ".wagt_alive/400524", Gid: 400524, Mtime: 1761000200},
	}
	if len(entries) != len(want) {
		t.Fatalf("got %+v, want %+v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("entry %d: got %+v, want %+v", i, entries[i], want[i])
		}
	}

	if !isAgentOverlayFile(entries[2].RelPath) {
		t.Errorf("heartbeat marker should be treated as an agent file")
	}
	if isAgentOverlayFile(entries[1].RelPath) {
		t.Errorf("job output should not be treated as an agent file")
	}
}
