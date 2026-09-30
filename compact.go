package kvengine

import (
	"os"
	"path/filepath"
	"sort"
)

const compactTmpSuffix = ".tmp"

// Compact rewrites the WAL so it contains exactly the currently committed
// live data as a single batch record, reclaiming space held by overwritten
// and deleted entries. Snapshots and Scan results obtained before Compact
// remain valid: they are independent in-memory views, so reclamation never
// touches data a reader can still observe.
//
// The rewrite is crash-safe: the new log is written and fsynced to a
// temporary file, then atomically renamed over kv.wal and the directory is
// fsynced. A crash at any point leaves either the old or the new log fully
// intact; kv.wal itself is never partially rewritten. A leftover temporary
// file is discarded on the next Open.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
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

	tmp := db.path + compactTmpSuffix
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
	if err := os.Rename(tmp, db.path); err != nil {
		return err
	}
	if err := syncDir(db.path); err != nil {
		return err
	}
	newFile, err := os.OpenFile(db.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	oldFile := db.file
	db.file = newFile
	return oldFile.Close()
}

func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
