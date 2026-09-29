package kvengine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrCorrupted = errors.New("kvengine: log is corrupted")
	ErrClosed    = errors.New("kvengine: database is closed")
)

const (
	KindPut    uint8 = opPut
	KindDelete uint8 = opDelete

	opPut    uint8 = 1
	opDelete uint8 = 2

	frameData   uint8 = 1
	frameCommit uint8 = 2

	headerSize     = 13
	maxPayloadSize = 64 << 20
	walName        = "wal.log"
)

type version struct {
	seq   uint64
	kind  uint8
	value []byte
}

// Op is one mutation in an atomic batch.
type Op struct {
	Kind  uint8
	Key   string
	Value []byte
}

// Put returns a put mutation.
func Put(key string, value []byte) Op {
	return Op{Kind: opPut, Key: key, Value: value}
}

// Delete returns a delete mutation.
func Delete(key string) Op {
	return Op{Kind: opDelete, Key: key}
}

// DB is an embedded, file-backed key-value engine.
type DB struct {
	mu       sync.RWMutex
	file     *os.File
	index    map[string][]version
	seq      uint64
	closed   bool
	snaps    map[uint64]uint64
	nextSnap uint64
}

// Snapshot is an immutable read view.
type Snapshot struct {
	db  *DB
	id  uint64
	seq uint64
}

// Open opens or recovers an embedded database in dir.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	db := &DB{
		file:  f,
		index: make(map[string][]version),
		snaps: make(map[uint64]uint64),
	}
	created, err := recoverLog(f, db.applyCommitted)
	if err != nil {
		f.Close()
		return nil, err
	}
	if created {
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		if err := syncDir(dir); err != nil {
			f.Close()
			return nil, err
		}
	}
	return db, nil
}

// Close flushes and closes the database files.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	return db.file.Close()
}

// WriteBatch atomically writes all mutations or none of them.
func (db *DB) WriteBatch(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	for _, op := range ops {
		if op.Key == "" {
			return errors.New("kvengine: key must not be empty")
		}
		if op.Kind != opPut && op.Kind != opDelete {
			return fmt.Errorf("kvengine: unknown op kind %d", op.Kind)
		}
	}

	payload := encodeOps(ops)
	dataFrame := encodeFrame(frameData, payload)
	commit := encodeCommit(len(ops), checksum(payload))
	commitFrame := encodeFrame(frameCommit, commit)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	_, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := writeFull(db.file, dataFrame); err != nil {
		return err
	}
	if _, err := writeFull(db.file, commitFrame); err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		return err
	}
	db.seq++
	db.applyCommittedLocked(db.seq, ops)
	db.gcModified(ops, db.seq+1)
	return nil
}

// Snapshot returns a stable view of all previously committed batches.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	db.nextSnap++
	id := db.nextSnap
	db.snaps[id] = db.seq
	return &Snapshot{db: db, id: id, seq: db.seq}, nil
}

// Get reads a key from the immutable snapshot.
func (s *Snapshot) Get(key string) ([]byte, bool, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.db.closed {
		return nil, false, ErrClosed
	}
	for i := len(s.db.index[key]) - 1; i >= 0; i-- {
		v := s.db.index[key][i]
		if v.seq > s.seq {
			continue
		}
		if v.kind == opDelete {
			return nil, false, nil
		}
		out := make([]byte, len(v.value))
		copy(out, v.value)
		return out, true, nil
	}
	return nil, false, nil
}

// Close releases snapshot-retained history.
func (s *Snapshot) Close() {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if _, ok := s.db.snaps[s.id]; !ok {
		return
	}
	delete(s.db.snaps, s.id)
	s.db.gcAll(0)
}

func (db *DB) applyCommitted(seq uint64, ops []Op) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.seq = seq
	db.applyCommittedLocked(seq, ops)
}

func (db *DB) applyCommittedLocked(seq uint64, ops []Op) {
	for _, op := range ops {
		var value []byte
		if op.Kind == opPut {
			value = append([]byte(nil), op.Value...)
		}
		db.index[op.Key] = append(db.index[op.Key], version{
			seq:   seq,
			kind:  op.Kind,
			value: value,
		})
	}
}

func (db *DB) gcModified(ops []Op, cutoff uint64) {
	if len(db.snaps) != 0 {
		cutoff = db.oldestSnapshotLocked(0)
	}
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if _, ok := seen[op.Key]; ok {
			continue
		}
		seen[op.Key] = struct{}{}
		db.index[op.Key] = compactVersions(db.index[op.Key], cutoff)
	}
}

func (db *DB) gcAll(cutoff uint64) {
	if len(db.snaps) != 0 {
		cutoff = db.oldestSnapshotLocked(0)
	} else {
		cutoff = db.seq + 1
	}
	for key, versions := range db.index {
		db.index[key] = compactVersions(versions, cutoff)
	}
}

func (db *DB) oldestSnapshotLocked(fallback uint64) uint64 {
	if len(db.snaps) == 0 {
		if fallback != 0 {
			return fallback
		}
		return db.seq + 1
	}
	oldest := ^uint64(0)
	for _, seq := range db.snaps {
		if seq < oldest {
			oldest = seq
		}
	}
	return oldest
}

func compactVersions(versions []version, cutoff uint64) []version {
	keepFrom := len(versions)
	for i, v := range versions {
		if v.seq >= cutoff {
			keepFrom = i
			break
		}
	}
	if keepFrom == 0 {
		return versions
	}
	if keepFrom == len(versions) {
		last := versions[len(versions)-1:]
		return append([]version(nil), last...)
	}
	if keepFrom == 1 {
		return versions
	}
	out := make([]version, 0, len(versions)-keepFrom+1)
	out = append(out, versions[keepFrom-1])
	out = append(out, versions[keepFrom:]...)
	return out
}
