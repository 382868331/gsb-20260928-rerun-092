package kvengine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type referenceDB struct {
	data map[string]string
}

func newReferenceDB() *referenceDB {
	return &referenceDB{data: make(map[string]string)}
}

func (r *referenceDB) apply(ops []Op) {
	for _, op := range ops {
		if op.Kind == KindDelete {
			delete(r.data, op.Key)
		} else {
			r.data[op.Key] = string(op.Value)
		}
	}
}

func TestRecoverEveryWriteBoundary(t *testing.T) {
	base := t.TempDir()
	db, err := Open(base)
	if err != nil {
		t.Fatal(err)
	}
	first := []Op{Put("alpha", []byte("one")), Put("beta", []byte("keep"))}
	second := []Op{Put("alpha", []byte("two")), Delete("beta")}
	if err := db.WriteBatch(first); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteBatch(second); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	third := []Op{Put("alpha", []byte("three")), Put("gamma", []byte("added"))}
	fourth := []Op{Put("alpha", []byte("four"))}
	prefixBytes, err := os.ReadFile(filepath.Join(base, walName))
	if err != nil {
		t.Fatal(err)
	}
	thirdPayload := encodeOps(third)
	thirdBytes := append(encodeFrame(frameData, thirdPayload),
		encodeFrame(frameCommit, encodeCommit(len(third), checksum(thirdPayload)))...)
	fourthOps := encodeOps(fourth)
	fourthBytes := append(encodeFrame(frameData, fourthOps),
		encodeFrame(frameCommit, encodeCommit(len(fourth), checksum(fourthOps)))...)
	tail := append(thirdBytes, fourthBytes...)

	for cut := 0; cut <= len(tail); cut++ {
		dir := t.TempDir()
		path := filepath.Join(dir, walName)
		record := append(append([]byte(nil), prefixBytes...), tail[:cut]...)
		if err := os.WriteFile(path, record, 0o644); err != nil {
			t.Fatal(err)
		}

		recovered, err := Open(dir)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}

		want := newReferenceDB()
		want.apply(first)
		want.apply(second)
		validTail := 0
		if cut >= len(thirdBytes) {
			want.apply(third)
			validTail = len(thirdBytes)
		}
		if cut == len(tail) {
			want.apply(fourth)
			validTail = len(tail)
		}
		assertReference(t, recovered, want)
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}

		healed, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wantSize := len(prefixBytes) + validTail
		if len(healed) != wantSize {
			t.Fatalf("cut %d: healed size %d, want %d", cut, len(healed), wantSize)
		}
	}
}

func TestCorruptCompleteRecordIsNotTreatedAsEnd(t *testing.T) {
	tests := []struct {
		name   string
		offset int64
	}{
		{name: "header checksum", offset: 5},
		{name: "payload checksum", offset: 9},
		{name: "payload byte", offset: 20},
		{name: "commit checksum", offset: -1},
		{name: "commit byte", offset: -4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, bytes := completeCommittedLog(t)
			path := filepath.Join(dir, walName)
			offset := int(tc.offset)
			if offset < 0 {
				offset = len(bytes) + offset
			}
			bytes[offset] ^= 0xff
			if err := os.WriteFile(path, bytes, 0o644); err != nil {
				t.Fatal(err)
			}
			db, err := Open(dir)
			if db != nil {
				_ = db.Close()
			}
			if !errors.Is(err, ErrCorrupted) {
				t.Fatalf("err = %v, want ErrCorrupted", err)
			}
		})
	}
}

func TestTornTailIsTruncatedAtCommitBoundary(t *testing.T) {
	dir, bytes := completeCommittedLog(t)
	path := filepath.Join(dir, walName)
	if err := os.WriteFile(path, bytes[:len(bytes)-1], 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, snap, "alpha", "", false)
	snap.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	healed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(healed) != 0 {
		t.Fatalf("uncommitted tail length = %d", len(healed))
	}
}

func TestSnapshotDoesNotObserveLaterOverwrites(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.WriteBatch([]Op{Put("alpha", []byte("old")), Put("beta", []byte("b1"))}); err != nil {
		t.Fatal(err)
	}
	old, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteBatch([]Op{Put("alpha", []byte("new")), Delete("beta")}); err != nil {
		t.Fatal(err)
	}
	current, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, old, "alpha", "old", true)
	assertValue(t, old, "beta", "b1", true)
	assertValue(t, current, "alpha", "new", true)
	assertValue(t, current, "beta", "", false)
	old.Close()
	current.Close()
}

func TestBatchIsAtomic(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.WriteBatch([]Op{Put("alpha", []byte("one"))}); err != nil {
		t.Fatal(err)
	}
	err = db.WriteBatch([]Op{Put("alpha", []byte("two")), Put("", []byte("bad"))})
	if err == nil {
		t.Fatal("invalid batch unexpectedly committed")
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	assertValue(t, snap, "alpha", "one", true)
}

func completeCommittedLog(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	ops := []Op{Put("alpha", []byte("one"))}
	payload := encodeOps(ops)
	record := append(encodeFrame(frameData, payload), encodeFrame(frameCommit, encodeCommit(len(ops), checksum(payload)))...)
	return dir, record
}

func assertReference(t *testing.T, db *DB, want *referenceDB) {
	t.Helper()
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	for _, key := range []string{"alpha", "beta", "gamma", "missing-after-delete"} {
		wantValue, exists := want.data[key]
		assertValue(t, snap, key, wantValue, exists)
	}
}

func assertValue(t *testing.T, snap *Snapshot, key, want string, exists bool) {
	t.Helper()
	value, ok, err := snap.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if ok != exists || (exists && string(value) != want) {
		t.Fatalf("%s = %q/%v, want %q/%v", key, value, ok, want, exists)
	}
}
