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
	walName     = "kv.wal"
	headerSize  = 12
	magic16     = uint16(0x4b56)
	version1    = uint8(1)
	recordBatch = uint8(1)
)

var (
	// ErrCorruptWAL means a complete WAL record has invalid magic, version,
	// type, checksum, or payload. It is distinct from a torn, incomplete tail.
	ErrCorruptWAL = errors.New("kvengine: corrupt write-ahead log")
	ErrEmptyKey   = errors.New("kvengine: key must not be empty")
	ErrClosed     = errors.New("kvengine: database is closed")
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

type DB struct {
	mu     sync.RWMutex
	path   string
	file   *os.File
	data   map[string][]byte
	closed bool
}

func Open(dir string) (*DB, *RecoveryInfo, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	// A leftover compaction temp file is never authoritative: the rename
	// onto kv.wal is atomic, so kv.wal always holds a complete log.
	tmp := filepath.Join(dir, walName+compactTmpSuffix)
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	// Leftover snapshot temp files from interrupted publishes are never
	// authoritative either; only fully renamed snapshot files are visible.
	if err := discardSnapshotTemps(filepath.Join(dir, snapshotDirName)); err != nil {
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
	db := &DB{path: path, file: file, data: make(map[string][]byte)}
	info, err := db.recover()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return db, info, nil
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
	stat, err := db.file.Stat()
	if err != nil {
		return nil, err
	}
	info := &RecoveryInfo{}
	fileSize := stat.Size()
	for info.LastGoodOffset < fileSize {
		offset := info.LastGoodOffset
		header := make([]byte, headerSize)
		_, err := io.ReadFull(db.file, header)
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
