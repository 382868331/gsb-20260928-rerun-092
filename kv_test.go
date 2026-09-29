package kvengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type referenceDB map[string][]byte

func newReferenceDB() referenceDB { return referenceDB{} }

func (ref referenceDB) apply(ops []Op) {
	for _, op := range ops {
		switch op.Kind {
		case Set:
			ref[op.Key] = append([]byte(nil), op.Value...)
		case Delete:
			delete(ref, op.Key)
		}
	}
}

func (ref referenceDB) clone() referenceDB {
	out := newReferenceDB()
	for key, value := range ref {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, dir
}

func TestBatchAndReopen(t *testing.T) {
	db, dir := openTestDB(t)
	history := [][]Op{
		{{Kind: Set, Key: "a", Value: []byte("one")}, {Kind: Set, Key: "b", Value: []byte("old")}},
		{{Kind: Set, Key: "b", Value: []byte("two")}, {Kind: Delete, Key: "a"}},
	}
	ref := newReferenceDB()
	for _, batch := range history {
		if err := db.Batch(batch); err != nil {
			t.Fatal(err)
		}
		ref.apply(batch)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, info, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if info.Truncated {
		t.Fatalf("clean reopen unexpectedly truncated: %+v", info)
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertReference(t, snapshot, ref)
}

func TestSnapshotIsolation(t *testing.T) {
	db, _ := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Set("k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	old, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old["k"], []byte("old")) {
		t.Fatalf("old snapshot changed: %q", old["k"])
	}
	current, ok, err := db.Get("k")
	if err != nil || !ok || !bytes.Equal(current, []byte("new")) {
		t.Fatalf("current = %q, %v, %v; want new", current, ok, err)
	}
	old["k"][0] = 'X'
	again, _, err := db.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "new" {
		t.Fatalf("snapshot mutation changed database: %q", again)
	}
}

func TestRecoveryAtEveryWriteBoundary(t *testing.T) {
	committed := [][]Op{
		{{Kind: Set, Key: "before", Value: []byte("kept")}},
		{{Kind: Set, Key: "other", Value: []byte("also-kept")}, {Kind: Delete, Key: "missing"}},
	}
	pending := []Op{
		{Kind: Set, Key: "before", Value: []byte("must-not-show")},
		{Kind: Set, Key: "atomic", Value: []byte("also-must-not-show")},
	}
	ref := newReferenceDB()
	for _, batch := range committed {
		ref.apply(batch)
	}

	payload, err := encodeBatch(pending)
	if err != nil {
		t.Fatal(err)
	}
	frame := encodeFrame(recordBatch, payload)
	for cut := 1; cut < len(frame); cut++ {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			db, _, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, batch := range committed {
				if err := db.Batch(batch); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(dir, walName)
			base, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			torn := append(append([]byte(nil), base...), frame[:cut]...)
			if err := os.WriteFile(path, torn, 0o644); err != nil {
				t.Fatal(err)
			}

			db, info, err := Open(dir)
			if err != nil {
				t.Fatalf("incomplete tail treated as corruption: %v", err)
			}
			if !info.Truncated || info.TruncatedOffset != int64(len(torn)) || info.LastGoodOffset != int64(len(base)) {
				t.Fatalf("bad recovery info: %+v", info)
			}
			snapshot, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			assertReference(t, snapshot, ref)
			if _, ok := snapshot["atomic"]; ok {
				t.Fatal("partial batch became visible")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			repaired, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(repaired, base) {
				t.Fatal("WAL was not truncated back to last complete record")
			}

			db, info, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if info.Truncated {
				t.Fatalf("repaired tail truncated again: %+v", info)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCorruptionIsNotTruncation(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("good", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, walName)
	base, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := encodeBatch([]Op{{Kind: Set, Key: "bad", Value: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	frame := encodeFrame(recordBatch, payload)
	frame[len(frame)-1] ^= 0xff
	corrupt := append(append([]byte(nil), base...), frame...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	db, _, err = Open(dir)
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Open error = %v; want ErrCorruptWAL", err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("corrupt WAL should not be silently truncated or rewritten")
	}
}

func TestReferenceHistory(t *testing.T) {
	db, dir := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ref := newReferenceDB()
	history := [][]Op{
		{{Kind: Set, Key: "a", Value: []byte("1")}, {Kind: Set, Key: "b", Value: []byte("2")}},
		{{Kind: Set, Key: "a", Value: []byte("overwritten")}},
		{{Kind: Delete, Key: "b"}, {Kind: Set, Key: "c", Value: []byte("3")}},
	}
	for _, ops := range history {
		if err := db.Batch(ops); err != nil {
			t.Fatal(err)
		}
		ref.apply(ops)
		snapshot, err := db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		assertReference(t, snapshot, ref)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	final, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertReference(t, final, ref)
}

func assertReference(t *testing.T, got map[string][]byte, want referenceDB) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d; got=%v want=%v", len(got), len(want), got, want)
	}
	for key, wantValue := range want {
		gotValue, ok := got[key]
		if !ok || !bytes.Equal(gotValue, wantValue) {
			t.Fatalf("key %q = %q, ok=%v; want %q", key, gotValue, ok, wantValue)
		}
	}
}
