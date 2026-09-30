package kvengine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walName            = "kv.wal"
	checkpointName     = "kv.snap"
	tempCheckpointName = "kv.snap.tmp"
	tempWALName        = "kv.wal.tmp"
	snapshotsDirName   = "snapshots"
	headerSize         = 12
	magic16            = uint16(0x4b56)
	version1           = uint8(1)
	recordBatch        = uint8(1)
	recordSnapshot     = uint8(2)
	recordCheckpoint   = uint8(3)
)

var (
	// ErrCorruptWAL means a complete WAL record has invalid magic, version,
	// type, checksum, or payload. It is distinct from a torn, incomplete tail.
	ErrCorruptWAL = errors.New("kvengine: corrupt write-ahead log")
	ErrEmptyKey   = errors.New("kvengine: key must not be empty")
	ErrClosed     = errors.New("kvengine: database is closed")
)

var (
	crashBeforeCheckpointRename = func() error { return nil }
	crashBeforeWALRename        = func() error { return nil }
	crashAfterWALRename         = func() error { return nil }
)

type OpKind uint8

const (
	Set OpKind = iota + 1
	Delete
)

type Op struct {
	Kind  OpKind
	Key   string
	Value []byte
}

type RecoveryInfo struct {
	LastGoodOffset  int64
	Truncated       bool
	TruncatedOffset int64
}

type RangeOptions struct {
	Start string
	End   string
}

type Iterator struct {
	keys   []string
	values [][]byte
	index  int
}

type Snapshot struct {
	id     string
	path   string
	values map[string][]byte
}

type DB struct {
	mu     sync.RWMutex
	path   string
	dir    string
	file   *os.File
	data   map[string][]byte
	closed bool
}

func Open(dir string) (*DB, *RecoveryInfo, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, walName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, nil, err
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if err := dirFile.Sync(); err != nil {
		dirFile.Close()
		file.Close()
		return nil, nil, err
	}
	if err := dirFile.Close(); err != nil {
		file.Close()
		return nil, nil, err
	}
	db := &DB{path: path, dir: dir, file: file, data: make(map[string][]byte)}
	info, err := db.recover()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return db, info, nil
}

func (db *DB) checkpointPath() string {
	return filepath.Join(db.dir, checkpointName)
}

func (db *DB) tempCheckpointPath() string {
	return filepath.Join(db.dir, tempCheckpointName)
}

func (db *DB) tempWALPath() string {
	return filepath.Join(db.dir, tempWALName)
}

func (db *DB) snapshotsDir() string {
	return filepath.Join(db.dir, snapshotsDirName)
}

func (db *DB) Path() string { return db.path }

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, ErrClosed
	}
	value, ok := db.data[key]
	return cloneBytes(value), ok, nil
}

// Snapshot returns an immutable point-in-time copy of all committed data.
func (db *DB) Snapshot() (map[string][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return cloneMap(db.data), nil
}

// Range returns committed keys in the half-open range [start, end). Empty
// start and end represent the beginning and end of the key space.
func (db *DB) Range(opts RangeOptions) (*Iterator, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return newIterator(db.data, opts), nil
}

// SaveSnapshot durably publishes an independent point-in-time snapshot.
func (db *DB) SaveSnapshot() (*Snapshot, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	data := cloneMap(db.data)
	snapshotID := nextSnapshotID()
	db.mu.RUnlock()

	if err := os.MkdirAll(db.snapshotsDir(), 0o755); err != nil {
		return nil, err
	}
	if err := syncDirectory(db.snapshotsDir()); err != nil {
		return nil, err
	}
	snapshot := newSnapshot(snapshotID, db.snapshotsDir(), data)
	if err := writeSnapshotFileWithHook(snapshot.path, snapshot.values, crashBeforeSnapshotRename); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (db *DB) OpenSnapshot(snapshotID string) (*Snapshot, error) {
	if snapshotID == "" || !validSnapshotID(snapshotID) {
		return nil, fmt.Errorf("kvengine: invalid snapshot id %q", snapshotID)
	}
	return openSnapshot(snapshotID, db.snapshotsDir())
}

// Compact publishes a checkpoint of current committed data and replaces the
// WAL. Persistent user snapshots are not modified.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	data := cloneMap(db.data)
	if err := db.writeCheckpointLocked(data); err != nil {
		return err
	}
	return db.rotateWALLocked()
}

func (db *DB) writeCheckpointLocked(data map[string][]byte) error {
	tempPath := db.tempCheckpointPath()
	checkpointID := nextSnapshotID()
	payload, err := encodeSnapshot(snapshotFile{ID: checkpointID, Values: data})
	if err != nil {
		return err
	}
	if err := writeFrameFile(tempPath, recordSnapshot, payload); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := crashBeforeCheckpointRename(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, db.checkpointPath()); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return syncDirectory(db.dir)
}

func (db *DB) rotateWALLocked() (err error) {
	defer func() {
		if err != nil {
			db.closed = true
			if db.file != nil {
				_ = db.file.Close()
				db.file = nil
			}
		}
	}()
	tempPath := db.tempWALPath()
	checkpointID, err := readSnapshotID(db.checkpointPath())
	if err != nil {
		return err
	}
	temp, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	marker := encodeFrame(recordCheckpoint, []byte(checkpointID))
	n, err := temp.Write(marker)
	if err == nil && n != len(marker) {
		err = io.ErrShortWrite
	}
	syncErr := temp.Sync()
	if err != nil || syncErr != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		if err != nil {
			return fmt.Errorf("kvengine: cannot create replacement WAL: %w", err)
		}
		return fmt.Errorf("kvengine: cannot sync replacement WAL: %w", syncErr)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := crashBeforeWALRename(); err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := db.file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, db.path); err != nil {
		_ = os.Remove(tempPath)
		reopened, reopenErr := os.OpenFile(db.path, os.O_RDWR, 0o644)
		if reopenErr == nil {
			db.file = reopened
		}
		return err
	}
	if err := crashAfterWALRename(); err != nil {
		db.closed = true
		db.file = nil
		return err
	}
	if err := syncDirectory(db.dir); err != nil {
		return err
	}
	reopened, err := os.OpenFile(db.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	db.file = reopened
	return nil
}

// Batch atomically and durably applies all operations.
func (db *DB) Batch(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	for _, op := range ops {
		if op.Key == "" {
			return ErrEmptyKey
		}
		switch op.Kind {
		case Set, Delete:
		default:
			return fmt.Errorf("kvengine: unknown operation kind %d", op.Kind)
		}
	}
	payload, err := encodeBatch(ops)
	if err != nil {
		return err
	}
	frame := encodeFrame(recordBatch, payload)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	start, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	n, err := db.file.WriteAt(frame, start)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = db.file.Truncate(start)
		_ = db.file.Sync()
		return err
	}
	if err := db.file.Sync(); err != nil {
		_ = db.file.Truncate(start)
		_ = db.file.Sync()
		return err
	}
	applyBatch(db.data, ops)
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}
	return db.Batch([]Op{{Kind: Set, Key: key, Value: value}})
}

func (db *DB) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return db.Batch([]Op{{Kind: Delete, Key: key}})
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	if err := db.file.Sync(); err != nil {
		_ = db.file.Close()
		return err
	}
	return db.file.Close()
}

func (db *DB) recover() (*RecoveryInfo, error) {
	_ = os.Remove(db.tempCheckpointPath())
	_ = os.Remove(db.tempWALPath())
	temporarySnapshots, _ := filepath.Glob(filepath.Join(db.snapshotsDir(), "*.tmp"))
	for _, temporarySnapshot := range temporarySnapshots {
		_ = os.Remove(temporarySnapshot)
	}
	checkpointValues, checkpointID, checkpointErr := readCheckpointIfExists(db.checkpointPath())
	if checkpointErr != nil {
		return nil, checkpointErr
	}
	if checkpointValues != nil {
		finished, err := db.resumeCheckpointSwitchLocked(checkpointID)
		if err != nil {
			return nil, err
		}
		if finished {
			db.data = checkpointValues
			return &RecoveryInfo{LastGoodOffset: int64(len(encodeFrame(recordCheckpoint, []byte(checkpointID))))}, nil
		}
		db.data = checkpointValues
		markerSize := int64(headerSize + len(checkpointID))
		if _, err := db.file.Seek(markerSize, io.SeekStart); err != nil {
			return nil, err
		}
		return db.replayWAL(&RecoveryInfo{LastGoodOffset: markerSize})
	}
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return db.replayWAL(&RecoveryInfo{})
}

func (db *DB) replayWAL(info *RecoveryInfo) (*RecoveryInfo, error) {
	stat, err := db.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	_, err = db.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	for info.LastGoodOffset < fileSize {
		offset, err := db.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		header := make([]byte, headerSize)
		_, err = io.ReadFull(db.file, header)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			info.Truncated = true
			info.TruncatedOffset = fileSize
			break
		}
		if err != nil {
			return nil, err
		}
		magic := binaryLittleEndianUint16(header[0:2])
		length := binaryLittleEndianUint32(header[4:8])
		checksum := binaryLittleEndianUint32(header[8:12])
		if magic != magic16 || header[2] != version1 || header[3] != recordBatch {
			return nil, fmt.Errorf("%w: invalid header at offset %d", ErrCorruptWAL, offset)
		}
		payloadEnd := offset + int64(headerSize) + int64(length)
		if payloadEnd > fileSize {
			info.Truncated = true
			info.TruncatedOffset = fileSize
			break
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(db.file, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				info.Truncated = true
				info.TruncatedOffset = fileSize
				break
			}
			return nil, err
		}
		if crc32Checksum(header[:8], payload) != checksum {
			return nil, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorruptWAL, offset)
		}
		ops, err := decodeBatch(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorruptWAL, err)
		}
		applyBatch(db.data, ops)
		info.LastGoodOffset = payloadEnd
	}
	if info.Truncated {
		if err := db.file.Truncate(info.LastGoodOffset); err != nil {
			return nil, err
		}
		if err := db.file.Sync(); err != nil {
			return nil, err
		}
	}
	return info, nil
}

func readCheckpointIfExists(path string) (map[string][]byte, string, error) {
	values, id, err := readSnapshotValues(path)
	if os.IsNotExist(err) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return values, id, nil
}

func (db *DB) resumeCheckpointSwitchLocked(checkpointID string) (bool, error) {
	contents, err := os.ReadFile(db.path)
	if err != nil {
		return false, err
	}
	if len(contents) == 0 {
		return true, db.installCheckpointWAL(checkpointID)
	}
	if len(contents) < headerSize {
		return true, db.installCheckpointWAL(checkpointID)
	}
	header := contents[:headerSize]
	if binaryLittleEndianUint16(header[0:2]) != magic16 || header[2] != version1 {
		return false, fmt.Errorf("%w: invalid WAL header", ErrCorruptWAL)
	}
	length := binaryLittleEndianUint32(header[4:8])
	if header[3] != recordCheckpoint {
		return true, db.installCheckpointWAL(checkpointID)
	}
	end := headerSize + int(length)
	if end > len(contents) {
		return true, db.installCheckpointWAL(checkpointID)
	}
	payload := contents[headerSize:end]
	if crc32Checksum(header[:8], payload) != binaryLittleEndianUint32(header[8:12]) {
		return true, db.installCheckpointWAL(checkpointID)
	}
	if string(payload) != checkpointID {
		return true, db.installCheckpointWAL(checkpointID)
	}
	return false, nil
}

func (db *DB) installCheckpointWAL(checkpointID string) error {
	tempPath := db.tempWALPath()
	marker := encodeFrame(recordCheckpoint, []byte(checkpointID))
	if err := os.WriteFile(tempPath, marker, 0o644); err != nil {
		return err
	}
	temp, err := os.OpenFile(tempPath, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := db.file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, db.path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := syncDirectory(db.dir); err != nil {
		return err
	}
	reopened, err := os.OpenFile(db.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	db.file = reopened
	return nil
}

func applyBatch(values map[string][]byte, ops []Op) {
	for _, op := range ops {
		switch op.Kind {
		case Set:
			values[op.Key] = cloneBytes(op.Value)
		case Delete:
			delete(values, op.Key)
		}
	}
}

func cloneMap(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for key, value := range in {
		out[key] = cloneBytes(value)
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
