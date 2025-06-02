package common

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FailPoint string

const (
	FailAfterWrite   FailPoint = "after_write"
	FailAfterSync    FailPoint = "after_sync"
	FailBeforeRename FailPoint = "before_rename"
)

// HookFunc lets tests inject failures at named stages.
// This functions should return an error if the failure should be simulated.
// Example usage:
//
//	hook := func(fp FailPoint) error {
//	    if fp == FailAfterWrite {
//	        return errors.New("simulated failure after write")
//	    }
//
// You can also simulate death by doing os.Exit(1) or panic().
// In production, you’ll pass `nil` to disable.
type HookFunc func(FailPoint) error

// AtomicWriteFile writes data to a temporary file and then renames it to the target filename.
// This ensures that the file is only replaced if the write is successful.
// It uses os.CreateTemp to create a temporary file in the same directory as the target file.
// Then it syncs the temporary file to ensure all data is written to disk.
// Finally, it renames the temporary file to the target filename.
// a test-only "hook" can be injected to simulate failures at various points in the process.
func AtomicWriteFile(filename string, data []byte, hook HookFunc) error {
	dir, base := filepath.Dir(filename), filepath.Base(filename)

	tmp, err := os.CreateTemp(dir, base+".tmp")
	if err != nil {
		return err
	}
	var success bool
	defer func() {
		if !success {
			if err := tmp.Close(); err != nil {
				slog.Error("failed to close temporary file", "error", err)
			}
		}
		os.Remove(tmp.Name())
	}()

	// 1) write
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to write data to temporary file %s: %w", tmp.Name(), err)
	}
	if err := callHook(hook, FailAfterWrite); err != nil {
		return err
	}

	// 2) fsync
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file %s: %w", tmp.Name(), err)
	}
	if err := callHook(hook, FailAfterSync); err != nil {
		return err
	}

	// 3) close
	if err := tmp.Close(); err != nil {
		return err
	}

	// 4) before rename
	if err := callHook(hook, FailBeforeRename); err != nil {
		return err
	}

	// 5) Set permissions before rename
	if err := os.Chmod(tmp.Name(), 0777); err != nil {
		return fmt.Errorf("failed to change permissions of temporary file %s: %w", tmp.Name(), err)
	}

	// 6) rename
	err = os.Rename(tmp.Name(), filename)
	if err != nil {
		return fmt.Errorf("failed to rename temporary file %s to %s: %w", tmp.Name(), filename, err)
	}
	success = true
	return nil
}

// callHook executes the provided hook function with the given fail point.
// If the hook is nil, it does nothing and returns nil.
func callHook(h HookFunc, point FailPoint) error {
	if h == nil {
		return nil
	}
	return h(point)
}

// CleanupOldTemps deletes .tmp files that are older than the olderThan parameter.
func CleanupOldTemps(dir, base string, olderThan time.Duration) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dir, err)
	}
	cutoff := time.Now().Add(-olderThan)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), base) && strings.HasSuffix(e.Name(), ".tmp") {
			info, err := e.Info()
			if err != nil {
				return fmt.Errorf("failed to get info for file %s: %w", e.Name(), err)
			}
			if info.ModTime().Before(cutoff) {
				err := os.Remove(filepath.Join(dir, e.Name()))
				if err != nil {
					return fmt.Errorf("failed to remove old temporary file %s: %w", e.Name(), err)
				}
			}
		}
	}
	return nil
}

// AppendLine appends a record to a file in a safe manner.
// It opens the file for appending, writes the data, and ensures that the file is synced to disk.
// If the file does not exist, it will be created.
func AppendLine(filename string, data []byte) error {
	// open file for append
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("failed to open file %s for appending: %w", filename, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("failed to close file after appending", "error", err)
		}
	}()

	// Write the record to the file
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to write data to file %s: %w", filename, err)
	}
	// sync file data
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %s: %w", filename, err)
	}

	// Ensure the file has the correct permissions
	if err := os.Chmod(filename, 0777); err != nil {
		return fmt.Errorf("failed to change permissions of file %s: %w", filename, err)
	}
	return nil
}
