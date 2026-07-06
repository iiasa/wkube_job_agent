package services

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInputMappingFromMountedStorage_Wildcards(t *testing.T) {
	// Set MultiLogWriter to discard so we don't output clutter during tests
	oldWriter := MultiLogWriter
	MultiLogWriter = io.Discard
	defer func() {
		MultiLogWriter = oldWriter
	}()

	// Since the code enforces "/mnt/tmp" or "/mnt/wdrv" prefix:
	// We will create temporary directories under /mnt/tmp for testing if we have permission,
	// or we will bypass/mock it if needed. Let's try creating a directory under /mnt/tmp.
	testMntDir := "/mnt/tmp/wildcard_test_src"
	err := os.MkdirAll(testMntDir, 0775)
	if err != nil {
		t.Skipf("Skipping test because we cannot create /mnt/tmp: %v", err)
		return
	}
	defer os.RemoveAll(testMntDir)

	// Create test files
	file1 := filepath.Join(testMntDir, "file1.txt")
	file2 := filepath.Join(testMntDir, "file2.txt")
	file3 := filepath.Join(testMntDir, "other.csv")
	dir1 := filepath.Join(testMntDir, "subdir")

	_ = os.WriteFile(file1, []byte("one"), 0644)
	_ = os.WriteFile(file2, []byte("two"), 0644)
	_ = os.WriteFile(file3, []byte("three"), 0644)
	_ = os.MkdirAll(dir1, 0775)

	// Create a temp dest directory inside the workspace
	testDestDir, err := os.MkdirTemp("", "wildcard_test_dest")
	if err != nil {
		t.Fatalf("failed to create temp dest dir: %v", err)
	}
	defer os.RemoveAll(testDestDir)

	// Ensure destination ends with /
	destPattern := testDestDir + "/"

	// Case 1: Test matching files with *.txt
	sourcePattern := filepath.Join(testMntDir, "*.txt")
	err = inputMappingFromMountedStorage(sourcePattern, destPattern)
	if err != nil {
		t.Fatalf("wildcard mapping failed: %v", err)
	}

	// Verify symlinks exist and point to the right file
	sym1 := filepath.Join(testDestDir, "file1.txt")
	sym2 := filepath.Join(testDestDir, "file2.txt")
	sym3 := filepath.Join(testDestDir, "other.csv")

	// Verify file1.txt symlink
	link1, err := os.Readlink(sym1)
	if err != nil {
		t.Errorf("expected symlink at %s, got err: %v", sym1, err)
	}
	if link1 != file1 {
		t.Errorf("expected symlink %s to point to %s, got %s", sym1, file1, link1)
	}

	// Verify file2.txt symlink
	link2, err := os.Readlink(sym2)
	if err != nil {
		t.Errorf("expected symlink at %s, got err: %v", sym2, err)
	}
	if link2 != file2 {
		t.Errorf("expected symlink %s to point to %s, got %s", sym2, file2, link2)
	}

	// Verify other.csv was NOT mapped
	if _, err := os.Lstat(sym3); err == nil {
		t.Errorf("other.csv should not have been mapped")
	}

	// Case 2: Test destination ending check
	err = inputMappingFromMountedStorage(sourcePattern, testDestDir) // no trailing slash
	if err == nil {
		t.Errorf("expected error when destination doesn't end with slash, got nil")
	} else if !strings.Contains(err.Error(), "requires a directory destination ending with '/'") {
		t.Errorf("expected error about trailing slash, got: %v", err)
	}

	// Case 3: Wildcard matches subdirectories
	// We map testMntDir/* into a new clean dest dir
	testDestDir2, err := os.MkdirTemp("", "wildcard_test_dest2")
	if err != nil {
		t.Fatalf("failed to create temp dest dir 2: %v", err)
	}
	defer os.RemoveAll(testDestDir2)

	err = inputMappingFromMountedStorage(filepath.Join(testMntDir, "*"), testDestDir2+"/")
	if err != nil {
		t.Fatalf("wildcard mapping all matching files/dirs failed: %v", err)
	}

	// Verify subdir was symlinked
	symSubdir := filepath.Join(testDestDir2, "subdir")
	linkSubdir, err := os.Readlink(symSubdir)
	if err != nil {
		t.Errorf("expected symlink at %s, got err: %v", symSubdir, err)
	}
	if linkSubdir != dir1 {
		t.Errorf("expected symlink %s to point to %s, got %s", symSubdir, dir1, linkSubdir)
	}
}
