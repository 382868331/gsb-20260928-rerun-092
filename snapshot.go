package kvengine

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var crashBeforeSnapshotRename = func() error { return nil }

type snapshotFile struct {
	ID     string
	Values map[string][]byte
}

func newIterator(data map[string][]byte, opts RangeOptions) *Iterator {
	keys := make([]string, 0, len(data))
	for key := range data {
		if (opts.Start == "" || key >= opts.Start) && (opts.End == "" || key < opts.End) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = cloneBytes(data[key])
	}
	return &Iterator{keys: keys, values: values, index: -1}
}

func (it *Iterator) Next() bool {
	if it.index+1 >= len(it.keys) {
		return false
	}
	it.index++
	return true
}

func (it *Iterator) Key() string {
	return it.keys[it.index]
}

func (it *Iterator) Value() []byte {
	return cloneBytes(it.values[it.index])
}

func (it *Iterator) Close() error { return nil }

func newSnapshot(id, dir string, values map[string][]byte) *Snapshot {
	return &Snapshot{id: id, path: filepath.Join(dir, id), values: values}
}

func openSnapshot(id, dir string) (*Snapshot, error) {
	path := filepath.Join(dir, id)
	values, snapshotID, err := readSnapshotValues(path)
	if err != nil {
		return nil, err
	}
	if snapshotID != id {
		return nil, fmt.Errorf("kvengine: snapshot id mismatch")
	}
	return newSnapshot(id, dir, values), nil
}

func (snap *Snapshot) ID() string { return snap.id }

func (snap *Snapshot) Get(key string) ([]byte, bool) {
	value, ok := snap.values[key]
	return cloneBytes(value), ok
}

func (snap *Snapshot) Range(opts RangeOptions) *Iterator {
	return newIterator(snap.values, opts)
}

func (snap *Snapshot) Snapshot() map[string][]byte {
	return cloneMap(snap.values)
}

func (snap *Snapshot) Close() error { return nil }

func nextSnapshotID() string {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%016x-%016x", time.Now().UnixNano(), random[:8])
}

func validSnapshotID(id string) bool {
	if len(id) != 33 {
		return false
	}
	for _, r := range id {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && r != '-' {
			return false
		}
	}
	return true
}

func writeSnapshotFile(path string, values map[string][]byte) error {
	return writeSnapshotFileWithHook(path, values, nil)
}

func writeSnapshotFileWithHook(path string, values map[string][]byte, onTemp func() error) error {
	id := filepath.Base(path)
	payload, err := encodeSnapshot(snapshotFile{ID: id, Values: values})
	if err != nil {
		return err
	}
	return writeFrameFileWithHook(path, recordSnapshot, payload, onTemp)
}

func writeFrameFile(path string, recordType uint8, payload []byte) error {
	return writeFrameFileWithHook(path, recordType, payload, nil)
}

func writeFrameFileWithHook(path string, recordType uint8, payload []byte, onTemp func() error) error {
	frame := encodeFrame(recordType, payload)
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	writeErr := writeFull(file, frame)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(tempPath)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tempPath)
		return closeErr
	}
	if onTemp != nil {
		if err := onTemp(); err != nil {
			return err
		}
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func writeFull(file *os.File, data []byte) error {
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		return fmt.Errorf("kvengine: short snapshot write")
	}
	return err
}

func readSnapshotValues(path string) (map[string][]byte, string, error) {
	recordType, payload, err := readSingleFrame(path)
	if err != nil {
		return nil, "", err
	}
	if recordType != recordSnapshot {
		return nil, "", fmt.Errorf("%w: invalid snapshot record", ErrCorruptWAL)
	}
	snapshot, err := decodeSnapshot(payload)
	if err != nil {
		return nil, "", err
	}
	return snapshot.Values, snapshot.ID, nil
}

func readSnapshotID(path string) (string, error) {
	_, id, err := readSnapshotValues(path)
	return id, err
}

func readSingleFrame(path string) (uint8, []byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	if len(contents) < headerSize {
		return 0, nil, fmt.Errorf("%w: truncated snapshot header", ErrCorruptWAL)
	}
	header := contents[:headerSize]
	if binaryLittleEndianUint16(header[0:2]) != magic16 || header[2] != version1 {
		return 0, nil, fmt.Errorf("%w: invalid snapshot header", ErrCorruptWAL)
	}
	recordType := header[3]
	length := binaryLittleEndianUint32(header[4:8])
	checksum := binaryLittleEndianUint32(header[8:12])
	end := headerSize + int(length)
	if int(length) < 0 || end > len(contents) {
		return 0, nil, fmt.Errorf("%w: truncated snapshot", ErrCorruptWAL)
	}
	payload := contents[headerSize:end]
	if crc32Checksum(header[:8], payload) != checksum {
		return 0, nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptWAL)
	}
	if end != len(contents) {
		return 0, nil, fmt.Errorf("%w: trailing snapshot bytes", ErrCorruptWAL)
	}
	return recordType, payload, nil
}

func encodeSnapshot(snapshot snapshotFile) ([]byte, error) {
	keys := make([]string, 0, len(snapshot.Values))
	for key := range snapshot.Values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var buf []byte
	var err error
	buf, err = appendLength(buf, []byte(snapshot.ID))
	if err != nil {
		return nil, err
	}
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, key := range keys {
		buf, err = appendLength(buf, []byte(key))
		if err != nil {
			return nil, err
		}
		buf, err = appendLength(buf, snapshot.Values[key])
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func decodeSnapshot(payload []byte) (snapshotFile, error) {
	idBytes, pos, err := readLengthPrefixed(payload, 0)
	if err != nil {
		return snapshotFile{}, fmt.Errorf("invalid snapshot id: %w", err)
	}
	id := string(idBytes)
	if !validSnapshotID(id) {
		return snapshotFile{}, fmt.Errorf("%w: invalid snapshot id", ErrCorruptWAL)
	}
	count, n := binary.Uvarint(payload[pos:])
	if n <= 0 || count > uint64(len(payload)) {
		return snapshotFile{}, fmt.Errorf("%w: invalid snapshot count", ErrCorruptWAL)
	}
	pos += n
	values := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		key, next, err := readLengthPrefixed(payload, pos)
		if err != nil {
			return snapshotFile{}, fmt.Errorf("invalid snapshot key: %w", err)
		}
		if len(key) == 0 {
			return snapshotFile{}, ErrEmptyKey
		}
		pos = next
		value, next, err := readLengthPrefixed(payload, pos)
		if err != nil {
			return snapshotFile{}, fmt.Errorf("invalid snapshot value: %w", err)
		}
		pos = next
		values[string(key)] = value
	}
	if pos != len(payload) {
		return snapshotFile{}, fmt.Errorf("%w: trailing snapshot payload", ErrCorruptWAL)
	}
	return snapshotFile{ID: id, Values: values}, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func bytesMapEqual(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, ok := right[key]
		if !ok || !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}
