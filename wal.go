package kvengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type frame struct {
	kind    uint8
	payload []byte
}

// logTruncated marks a torn tail, not a completely framed corrupt record.
type logTruncated struct {
	offset int64
}

func (e logTruncated) Error() string {
	return fmt.Sprintf("kvengine: log is truncated at offset %d", e.offset)
}

func (e logTruncated) Unwrap() error { return ErrCorrupted }

type logCorrupted struct {
	offset int64
}

func (e logCorrupted) Error() string {
	return fmt.Sprintf("kvengine: corrupt log record at offset %d", e.offset)
}

func (e logCorrupted) Unwrap() error { return ErrCorrupted }

func recoverLog(f *os.File, apply func(uint64, []Op)) (bool, error) {
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := info.Size()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return false, err
	}
	reader := bytes.NewReader(buf)
	var seq uint64
	var pending []byte
	lastCommitEnd := int64(0)
	var tornEnd int64

	for reader.Len() > 0 {
		start := size - int64(reader.Len())
		fr, complete, err := readFrame(reader)
		if err != nil {
			var truncated logTruncated
			if errors.As(err, &truncated) {
				tornEnd = size
				break
			}
			return false, err
		}
		if !complete {
			tornEnd = size
			break
		}

		switch fr.kind {
		case frameData:
			if pending != nil {
				return false, logCorrupted{offset: start}
			}
			if len(fr.payload) == 0 {
				return false, logCorrupted{offset: start}
			}
			if _, err := decodeOps(fr.payload); err != nil {
				return false, logCorrupted{offset: start}
			}
			pending = append([]byte(nil), fr.payload...)
		case frameCommit:
			if pending == nil {
				return false, logCorrupted{offset: start}
			}
			count, sum, err := decodeCommit(fr.payload)
			if err != nil {
				return false, logCorrupted{offset: start}
			}
			ops, err := decodeOps(pending)
			if err != nil || len(ops) != count || checksum(pending) != sum {
				return false, logCorrupted{offset: start}
			}
			seq++
			apply(seq, ops)
			pending = nil
			lastCommitEnd = size - int64(reader.Len())
		default:
			return false, logCorrupted{offset: start}
		}
	}

	if pending != nil {
		tornEnd = size
	}
	if tornEnd != 0 {
		if err := f.Truncate(lastCommitEnd); err != nil {
			return false, err
		}
		if _, err := f.Seek(lastCommitEnd, io.SeekStart); err != nil {
			return false, err
		}
		return true, nil
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return false, err
	}
	return false, nil
}

func readFrame(r *bytes.Reader) (frame, bool, error) {
	start, _ := r.Seek(0, io.SeekCurrent)
	header := make([]byte, headerSize)
	_, err := io.ReadFull(r, header)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return frame{}, false, logTruncated{offset: start}
		}
		return frame{}, false, err
	}
	kind := header[0]
	length := binary.BigEndian.Uint32(header[1:5])
	headerSum := binary.BigEndian.Uint32(header[5:9])
	sum := binary.BigEndian.Uint32(header[9:13])
	if crc32.ChecksumIEEE(header[:5]) != headerSum {
		return frame{}, false, logCorrupted{offset: start}
	}
	if length > maxPayloadSize {
		return frame{}, false, logCorrupted{offset: start}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, false, logTruncated{offset: start}
	}
	if crc32.ChecksumIEEE(payload) != sum {
		return frame{}, false, logCorrupted{offset: start}
	}
	return frame{kind: kind, payload: payload}, true, nil
}

func encodeFrame(kind uint8, payload []byte) []byte {
	out := make([]byte, headerSize+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	binary.BigEndian.PutUint32(out[5:9], crc32.ChecksumIEEE(out[:5]))
	binary.BigEndian.PutUint32(out[9:13], crc32.ChecksumIEEE(payload))
	copy(out[headerSize:], payload)
	return out
}

func encodeOps(ops []Op) []byte {
	var b bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(ops)))
	b.Write(count[:])
	for _, op := range ops {
		kind := op.Kind
		b.WriteByte(kind)
		putBytes32(&b, []byte(op.Key))
		if kind == opPut {
			putBytes32(&b, op.Value)
		}
	}
	return b.Bytes()
}

func decodeOps(payload []byte) ([]Op, error) {
	r := bytes.NewReader(payload)
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	ops := make([]Op, 0)
	for i := uint32(0); i < count; i++ {
		kind, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if kind != opPut && kind != opDelete {
			return nil, fmt.Errorf("unknown op kind %d", kind)
		}
		key, err := readBytes32(r)
		if err != nil {
			return nil, err
		}
		op := Op{Kind: kind, Key: string(key)}
		if kind == opPut {
			value, err := readBytes32(r)
			if err != nil {
				return nil, err
			}
			op.Value = value
		}
		ops = append(ops, op)
	}
	if r.Len() != 0 {
		return nil, errors.New("trailing batch payload")
	}
	return ops, nil
}

func encodeCommit(count int, sum uint32) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint32(out[:4], uint32(count))
	binary.BigEndian.PutUint32(out[4:], sum)
	return out
}

func decodeCommit(payload []byte) (int, uint32, error) {
	if len(payload) != 8 {
		return 0, 0, errors.New("invalid commit frame")
	}
	return int(binary.BigEndian.Uint32(payload[:4])), binary.BigEndian.Uint32(payload[4:]), nil
}

func putBytes32(b *bytes.Buffer, value []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(value)))
	b.Write(n[:])
	b.Write(value)
}

func readBytes32(r *bytes.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(n[:])
	out := make([]byte, length)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

func checksum(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

func writeFull(f *os.File, b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		n, err := f.Write(b)
		if err != nil {
			return total + n, err
		}
		total += n
		b = b[n:]
	}
	return total, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
