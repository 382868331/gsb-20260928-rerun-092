package kvengine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func binaryLittleEndianUint16(b []byte) uint16 { return binary.LittleEndian.Uint16(b) }
func binaryLittleEndianUint32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

func encodeFrame(recordType uint8, payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint16(frame[0:2], magic16)
	frame[2] = version1
	frame[3] = recordType
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[8:12], crc32Checksum(frame[:8], payload))
	copy(frame[headerSize:], payload)
	return frame
}

func crc32Checksum(headerWithoutChecksum, payload []byte) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc.Write(headerWithoutChecksum)
	crc.Write(payload)
	return crc.Sum32()
}

func encodeBatch(ops []Op) ([]byte, error) {
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(ops)))
	for _, op := range ops {
		buf = append(buf, byte(op.Kind))
		var err error
		buf, err = appendLength(buf, []byte(op.Key))
		if err != nil {
			return nil, err
		}
		if op.Kind == Set {
			buf, err = appendLength(buf, op.Value)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf, nil
}

func appendLength(dst, value []byte) ([]byte, error) {
	if len(value) > 0x7fffffff {
		return nil, fmt.Errorf("kvengine: value is too large: %d", len(value))
	}
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...), nil
}

func decodeBatch(payload []byte) ([]Op, error) {
	count, pos := binary.Uvarint(payload)
	if pos <= 0 {
		return nil, fmt.Errorf("invalid batch count")
	}
	if count > uint64(len(payload)) {
		return nil, fmt.Errorf("implausible batch count")
	}
	ops := make([]Op, 0, count)
	for i := uint64(0); i < count; i++ {
		if pos >= len(payload) {
			return nil, fmt.Errorf("truncated operation kind")
		}
		op := Op{Kind: OpKind(payload[pos])}
		pos++
		if op.Kind != Set && op.Kind != Delete {
			return nil, fmt.Errorf("invalid operation kind %d", op.Kind)
		}
		key, next, err := readLengthPrefixed(payload, pos)
		if err != nil {
			return nil, fmt.Errorf("invalid key: %w", err)
		}
		if len(key) == 0 {
			return nil, ErrEmptyKey
		}
		op.Key = string(key)
		pos = next
		if op.Kind == Set {
			op.Value, next, err = readLengthPrefixed(payload, pos)
			if err != nil {
				return nil, fmt.Errorf("invalid value: %w", err)
			}
			pos = next
		}
		ops = append(ops, op)
	}
	if pos != len(payload) {
		return nil, fmt.Errorf("%d trailing bytes", len(payload)-pos)
	}
	return ops, nil
}

func readLengthPrefixed(buf []byte, pos int) ([]byte, int, error) {
	length, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return nil, 0, fmt.Errorf("invalid length")
	}
	if length > 0x7fffffff {
		return nil, 0, fmt.Errorf("length too large")
	}
	start := pos + n
	end := start + int(length)
	if end > len(buf) || end < start {
		return nil, 0, fmt.Errorf("length exceeds payload")
	}
	return append([]byte(nil), buf[start:end]...), end, nil
}
