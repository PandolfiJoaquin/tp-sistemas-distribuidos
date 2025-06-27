// atomic/atomic_test.go
package common_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"tp-sistemas-distribuidos/server/common"
)

// AtomicWriteFile tests ---------------------------------------
// All the points at which we allow failure injection:
var allFailPoints = []common.FailPoint{
	common.FailAfterWrite,
	common.FailAfterSync,
	common.FailBeforeRename,
}

// 1) Happy path, small file
func TestAtomicWriteFile_Success_Small(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "foo.txt")
	data := []byte("hello")

	if err := common.AtomicWriteFile(target, data, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("content = %q; want %q", got, "hello")
	}
}

// 2) Happy path, "big" file (~100MB)
func TestAtomicWriteFile_Success_Big(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "big.bin")

	data := make([]byte, 100<<20) // 100 MiB of data
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := common.AtomicWriteFile(target, data, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(got) != len(data) {
		t.Errorf("size = %d; want %d", len(got), len(data))
	}
	// optional: spot-check first/last bytes
	if got[0] != data[0] || got[len(got)-1] != data[len(data)-1] {
		t.Error("data mismatch at boundaries")
	}
}

//  3. No-original case: for each fail‐point, ensure we get an error
//     and that the target file does NOT exist afterward.
func TestAtomicWriteFile_FailHooks_NoOriginal(t *testing.T) {
	for _, fp := range allFailPoints {
		t.Run(string(fp), func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "nofile.txt")

			// hook that only fails at fp
			hook := func(p common.FailPoint) error {
				if p == fp {
					return errors.New("injected " + string(p))
				}
				return nil
			}

			err := common.AtomicWriteFile(target, []byte("data"), hook)
			if err == nil || err.Error() != "injected "+string(fp) {
				t.Fatalf("for %s: expected injected error, got %v", fp, err)
			}

			if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
				t.Errorf("for %s: target should not exist, stat error = %v", fp, statErr)
			}
		})
	}
}

//  4. Keep‐original case: we start with an existing file,
//     then for each fail‐point, ensure the original stays intact.
func TestAtomicWriteFile_FailHooks_KeepOriginal(t *testing.T) {
	origContent := []byte("ORIGINAL")
	for _, fp := range allFailPoints {
		t.Run(string(fp), func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "keep.txt")

			// write the original file
			if err := common.AtomicWriteFile(target, origContent, nil); err != nil {
				t.Fatalf("setup (%s): could not write original: %v", fp, err)
			}

			// hook that only fails at fp
			hook := func(p common.FailPoint) error {
				if p == fp {
					return errors.New("injected " + string(p))
				}
				return nil
			}

			err := common.AtomicWriteFile(target, []byte("NEW"), hook)
			if err == nil || err.Error() != "injected "+string(fp) {
				t.Fatalf("for %s: expected injected error, got %v", fp, err)
			}

			// verify original still there
			got, readErr := os.ReadFile(target)
			if readErr != nil {
				t.Fatalf("for %s: could not read back original: %v", fp, readErr)
			}
			if string(got) != string(origContent) {
				t.Errorf("for %s: content = %q; want %q", fp, got, origContent)
			}

			// ensure no temp files remain
			dirEntries, _ := os.ReadDir(dir)
			for _, e := range dirEntries {
				if filepath.Ext(e.Name()) == ".tmp" {
					t.Errorf("for %s: leftover temp file: %s", fp, e.Name())
				}
			}
		})
	}
}

// CleanupOldTems --------------------------------------------
// helper to touch a file and set its mod-time to now-delta
func touchWithAge(t *testing.T, dir, name string, age time.Duration) {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatalf("failed to create %s: %v", name, err)
	}
	// set its mod time to now minus age
	past := time.Now().Add(-age)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatalf("failed to chtimes %s: %v", name, err)
	}
}

func TestCleanupOldTemps_RemovesOnlyOldMatching(t *testing.T) {
	dir := t.TempDir()
	base := "config"

	// create various files:
	touchWithAge(t, dir, base+".1.tmp", 2*time.Hour)    // should be removed
	touchWithAge(t, dir, base+".2.tmp", 30*time.Minute) // should stay
	touchWithAge(t, dir, "other.1.tmp", 2*time.Hour)    // wrong prefix
	touchWithAge(t, dir, base+".3.txt", 2*time.Hour)    // wrong suffix

	// run cleanup: remove anything older than 1h matching base*.tmp
	if err := common.CleanupOldTemps(dir, base, 1*time.Hour); err != nil {
		t.Fatalf("CleanupOldTemps failed: %v", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	exists := map[string]bool{}
	for _, e := range ents {
		exists[e.Name()] = true
	}

	// assert expectations
	if exists[base+".1.tmp"] {
		t.Error("expected config.1.tmp to be removed")
	}
	if !exists[base+".2.tmp"] {
		t.Error("expected config.2.tmp to remain")
	}
	if !exists["other.1.tmp"] {
		t.Error("expected other.1.tmp to remain (wrong prefix)")
	}
	if !exists[base+".3.txt"] {
		t.Error("expected config.3.txt to remain (wrong suffix)")
	}
}

func TestCleanupOldTemps_NothingToRemove(t *testing.T) {
	dir := t.TempDir()
	base := "foo"

	// create only fresh matching files
	touchWithAge(t, dir, base+".a.tmp", 10*time.Minute)
	touchWithAge(t, dir, base+".b.tmp", 5*time.Minute)

	if err := common.CleanupOldTemps(dir, base, 1*time.Hour); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ents, _ := os.ReadDir(dir)
	if len(ents) != 2 {
		t.Errorf("expected 2 files left, got %d", len(ents))
	}
}

func TestCleanupOldTemps_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	// no files at all
	if err := common.CleanupOldTemps(dir, "anything", time.Minute); err != nil {
		t.Fatalf("cleanup on empty dir should not error, got %v", err)
	}
}

// AppendLine tests ---------------------------------------
// TestAppendLine_Success ensures that AppendLine appends data correctly.
func TestAppendLine_Success(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "file.txt")

	// First append
	err := common.AppendLine(target, []byte("first\n"))
	if err != nil {
		t.Fatalf("unexpected error on first append: %v", err)
	}

	// Second append
	err = common.AppendLine(target, []byte("second\n"))
	if err != nil {
		t.Fatalf("unexpected error on second append: %v", err)
	}

	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	expect := "first\nsecond\n"
	if string(data) != expect {
		t.Errorf("content = %q, want %q", string(data), expect)
	}
}

// TestAppendLine_LargeWrite writes several MBs to exceed PIPE_BUF and ensures no partial writes
func TestAppendLine_LargeWrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "big.txt")

	// generate ~10MiB of data
	chunk := bytes.Repeat([]byte("A"), 1024*1024) // 1 MiB chunk
	var buf bytes.Buffer
	for i := 0; i < 10; i++ {
		buf.Write(chunk)
	}
	data := buf.Bytes()

	err := common.AppendLine(target, data)
	if err != nil {
		t.Fatalf("unexpected error on large write: %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}

	if info.Size() != int64(len(data)) {
		t.Errorf("size = %d, want %d", info.Size(), len(data))
	}
}
