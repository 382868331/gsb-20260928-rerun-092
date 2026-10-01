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
	// ErrSnapshotExists means a persistent snapshot with that name is
	// already saved. Release it first to replace it.
	ErrSnapshotExists = errors.New("kvengine: snapshot already exists")
	// ErrSnapshotNotFound means no persistent snapshot with that name is
	// saved in the data directory.
	ErrSnapshotNotFound = errors.New("kvengine: snapshot not found")
	// ErrInvalidSnapshotName means the name is empty or cannot be used as
	// a file name inside the snapshot directory.
	ErrInvalidSnapshotName = errors.New("kvengine: invalid snapshot name")
	// ErrCorruptSnapshot means a saved snapshot file failed validation.
	ErrCorruptSnapshot = errors.New("kvengine: corrupt snapshot file")
)

func validateSnapshotName(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.IndexAny(name, "/\\") >= 0 || strings.IndexByte(name, 0) >= 0 {
		return ErrInvalidSnapshotName
	}
	return nil
}

func (db *DB) snapshotDir() string {
	return filepath.Join(db.dir, snapshotDirName)
}

func (db *DB) snapshotPath(name string) string {
	return filepath.Join(db.snapshotDir(), name+snapshotSuffix)
}

// SaveSnapshot durably stores the currently committed data under name, so
// the exact view can be reopened after this process exits. Values that are
// overwritten or deleted afterwards still belong to the saved snapshot.
//
// The snapshot is encoded as a single checksummed record, written and
// fsynced to a temporary file, then atomically renamed into place and the
// directory fsynced. A crash before the rename leaves only a temporary
// file, which is never listed or opened and is discarded on the next Open;
// a crash after the rename leaves the complete snapshot. A half-published
// view is therefore never visible. Saving fails with ErrSnapshotExists if
// the name is already taken, so an older snapshot is never silently
// clobbered.
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

	if err := os.MkdirAll(db.snapshotDir(), 0o755); err != nil {
		return err
	}
	final := db.snapshotPath(name)
	if _, err := os.Stat(final); err == nil {
		return ErrSnapshotExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
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
	if err := os.Link(tmp, final); err != nil {
		return err
	}
	if err := os.Remove(tmp); err != nil {
		_ = os.Remove(final)
		return err
	}
	return syncDir(final)
}

// ListSnapshots returns the names of all saved persistent snapshots,
// sorted. Leftover temporary files from interrupted saves are not listed.
func (db *DB) ListSnapshots() ([]string, error) {
	dirEntries, err := os.ReadDir(db.snapshotDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(dirEntries))
	for _, entry := range dirEntries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, snapshotSuffix) ||
			strings.HasSuffix(name, snapshotSuffix+snapshotTmpSuffix) {
			continue
		}
		names = append(names, strings.TrimSuffix(name, snapshotSuffix))
	}
	sort.Strings(names)
	return names, nil
}

// OpenSnapshot returns the saved view stored under name as an immutable
// deep copy. It works on any process that opens the data directory; the
// process that saved the snapshot does not need to be alive.
func (db *DB) OpenSnapshot(name string) (map[string][]byte, error) {
	if err := validateSnapshotName(name); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(db.snapshotPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrSnapshotNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < headerSize {
		return nil, fmt.Errorf("%w: %q is truncated", ErrCorruptSnapshot, name)
	}
	header := raw[:headerSize]
	length := binaryLittleEndianUint32(header[4:8])
	checksum := binaryLittleEndianUint32(header[8:12])
	if binaryLittleEndianUint16(header[0:2]) != magic16 ||
		header[2] != version1 || header[3] != recordBatch {
		return nil, fmt.Errorf("%w: %q has an invalid header", ErrCorruptSnapshot, name)
	}
	payload := raw[headerSize:]
	if uint64(len(payload)) != uint64(length) {
		return nil, fmt.Errorf("%w: %q has %d payload bytes, want %d",
			ErrCorruptSnapshot, name, len(payload), length)
	}
	if crc32Checksum(header[:8], payload) != checksum {
		return nil, fmt.Errorf("%w: %q checksum mismatch", ErrCorruptSnapshot, name)
	}
	ops, err := decodeBatch(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %v", ErrCorruptSnapshot, name, err)
	}
	data := make(map[string][]byte, len(ops))
	applyBatch(data, ops)
	return data, nil
}

// ReleaseSnapshot deletes the saved snapshot with the given name. Only the
// snapshot file itself is removed; live data and other snapshots are
// untouched. If the removal fails, the error is returned and the snapshot
// remains saved, so a failed release never loses valid data.
func (db *DB) ReleaseSnapshot(name string) error {
	if err := validateSnapshotName(name); err != nil {
		return err
	}
	err := os.Remove(db.snapshotPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return ErrSnapshotNotFound
	}
	if err != nil {
		return err
	}
	return syncDir(db.snapshotPath(name))
}

// discardSnapshotTemps removes leftover temporary files from interrupted
// SaveSnapshot calls. They are never authoritative: a snapshot becomes
// visible only through the atomic link of the fully fsynced final name.
func (db *DB) discardSnapshotTemps() error {
	dirEntries, err := os.ReadDir(db.snapshotDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range dirEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), snapshotSuffix+snapshotTmpSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(db.snapshotDir(), entry.Name())); err != nil {
			return err
		}
	}
	return nil
}
