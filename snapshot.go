package kvengine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	snapshotDirName   = "snapshots"
	snapshotSuffix    = ".snap"
	snapshotTmpSuffix = ".tmp"
)

var (
	// ErrSnapshotNotFound means the named persistent snapshot does not exist.
	ErrSnapshotNotFound = errors.New("kvengine: snapshot not found")
	// ErrInvalidSnapshotName means the snapshot name is empty, reserved, or
	// contains path separators.
	ErrInvalidSnapshotName = errors.New("kvengine: invalid snapshot name")
	// ErrCorruptSnapshot means a snapshot file on disk failed validation.
	ErrCorruptSnapshot = errors.New("kvengine: corrupt snapshot")
)

func validateSnapshotName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalidSnapshotName
	}
	if strings.ContainsAny(name, "/\\") || strings.ContainsRune(name, 0) {
		return ErrInvalidSnapshotName
	}
	if strings.HasSuffix(name, snapshotTmpSuffix) {
		return ErrInvalidSnapshotName
	}
	return nil
}

func (db *DB) snapshotDir() string {
	return filepath.Join(filepath.Dir(db.path), snapshotDirName)
}

func (db *DB) snapshotPath(name string) string {
	return filepath.Join(db.snapshotDir(), name+snapshotSuffix)
}

// SaveSnapshot durably publishes a persistent snapshot of all currently
// committed data under the given name, replacing any previous snapshot with
// the same name. The snapshot is a fixed point-in-time view: later batches,
// overwrites, deletes, and compactions never change it, and it survives
// process restarts.
//
// Publishing is atomic: the snapshot is fully written and fsynced to a
// temporary file, then atomically renamed into place and the directory is
// fsynced. A crash at any point leaves either the old snapshot or the new
// one fully visible, never a half-published view; a leftover temporary file
// is discarded on the next Open.
func (db *DB) SaveSnapshot(name string) error {
	if err := validateSnapshotName(name); err != nil {
		return err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ops := make([]Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, Op{Kind: Set, Key: key, Value: db.data[key]})
	}
	payload, err := encodeBatch(ops)
	if err != nil {
		return err
	}
	frame := encodeFrame(recordBatch, payload)

	dir := db.snapshotDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	final := db.snapshotPath(name)
	tmp := final + snapshotTmpSuffix
	tmpFile, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tmpFile.WriteAt(frame, 0); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(final)
}

// ListSnapshots returns the names of all persistent snapshots, sorted.
func (db *DB) ListSnapshots() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return listSnapshotFiles(db.snapshotDir())
}

func listSnapshotFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, snapshotSuffix) {
			names = append(names, strings.TrimSuffix(name, snapshotSuffix))
		}
	}
	sort.Strings(names)
	return names, nil
}

// OpenSnapshot reads the persistent snapshot with the given name from disk
// and returns its content as an immutable deep copy. It does not depend on
// any state of the process that saved the snapshot, so it works after a
// restart. Mutating the returned map or values does not affect the stored
// snapshot.
func (db *DB) OpenSnapshot(name string) (map[string][]byte, error) {
	if err := validateSnapshotName(name); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return readSnapshotFile(db.snapshotPath(name))
}

func readSnapshotFile(path string) (map[string][]byte, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrSnapshotNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < headerSize {
		return nil, fmt.Errorf("%w: incomplete header", ErrCorruptSnapshot)
	}
	header := raw[:headerSize]
	magic := binaryLittleEndianUint16(header[0:2])
	length := binaryLittleEndianUint32(header[4:8])
	checksum := binaryLittleEndianUint32(header[8:12])
	if magic != magic16 || header[2] != version1 || header[3] != recordBatch {
		return nil, fmt.Errorf("%w: invalid header", ErrCorruptSnapshot)
	}
	if int64(headerSize)+int64(length) != int64(len(raw)) {
		return nil, fmt.Errorf("%w: length mismatch", ErrCorruptSnapshot)
	}
	payload := raw[headerSize:]
	if crc32Checksum(header[:8], payload) != checksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptSnapshot)
	}
	ops, err := decodeBatch(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	data := make(map[string][]byte, len(ops))
	applyBatch(data, ops)
	return data, nil
}

// ReleaseSnapshot deletes the persistent snapshot with the given name,
// allowing its disk space to be reclaimed. The snapshot file is removed
// only after this call; if removal fails, an error is returned and the
// snapshot data is left fully intact, so a failed release never destroys a
// valid snapshot. Releasing a snapshot never affects the current view or
// any other snapshot.
func (db *DB) ReleaseSnapshot(name string) error {
	if err := validateSnapshotName(name); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	final := db.snapshotPath(name)
	if _, err := os.Lstat(final); errors.Is(err, os.ErrNotExist) {
		return ErrSnapshotNotFound
	} else if err != nil {
		return err
	}
	if err := os.Remove(final); err != nil {
		return err
	}
	return syncDir(final)
}

// discardSnapshotTemps removes leftover temporary files from interrupted
// snapshot publishes. A temp file is never authoritative: the rename onto
// the final snapshot name is atomic, so any snapshot visible under its
// final name is complete.
func discardSnapshotTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), snapshotTmpSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
