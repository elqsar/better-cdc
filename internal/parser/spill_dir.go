package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"better-cdc/internal/subject"
	"golang.org/x/sys/unix"
)

// PrepareSpillDir locks a source-specific directory and removes only this
// program's orphaned transaction files. WAL remains the recovery source.
func PrepareSpillDir(base, slot string) (string, func(), error) {
	if base == "" {
		base = filepath.Join(os.TempDir(), "better-cdc-spill")
	}
	dir := filepath.Join(base, subject.Token(slot))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return "", nil, fmt.Errorf("spill directory must be a private directory (0700)")
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return "", nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return "", nil, fmt.Errorf("spill directory is already owned: %w", err)
	}
	release := func() { _ = unix.Flock(int(lock.Fd()), unix.LOCK_UN); _ = lock.Close() }
	entries, err := os.ReadDir(dir)
	if err != nil {
		release()
		return "", nil, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "better-cdc-pgoutput-") || !entry.Type().IsRegular() {
			continue
		}
		if err = os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			release()
			return "", nil, err
		}
	}
	return dir, release, nil
}
