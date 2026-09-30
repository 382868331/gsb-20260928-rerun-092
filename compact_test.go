package kvengine

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func compactHistory(t *testing.T, db *DB) referenceDB {
	t.Helper()
	ref := newReferenceDB()
	history := [][]Op{
		{
			{Kind: Set, Key: "keep", Value: []byte("v1")},
			{Kind: Set, Key: "gone", Value: []byte("x")},
			{Kind: Set, Key: "empty", Value: []byte{}},
		},
		{
			{Kind: Set, Key: "keep", Value: []byte("v2")},
			{Kind: Delete, Key: "gone"},
			{Kind: Set, Key: "keep2", Value: []byte("y")},
		},
		{{Kind: Delete, Key: "keep2"}},
	}
	for _, ops := range history {
		if err := db.Batch(ops); err != nil {
			t.Fatal(err)
		}
		ref.apply(ops)
	}
	return ref
}

func TestCompactKeepsLiveDataAndSnapshotsSnapshots(t *testing.T) {
	db, dir := openTestDB(t)
	ref := compactHistory(t, db)

	oldSnapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldScan, err := db.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}

	sizeBefore, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// Live data matches the model; the log shrank to a single record.
	current, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertReference(t, current, ref)
	sizeAfter, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if sizeAfter.Size() >= sizeBefore.Size() {
		t.Fatalf("compact did not shrink log: before=%d after=%d", sizeBefore.Size(), sizeAfter.Size())
	}

	// Views taken before compaction are still fully readable.
	assertReference(t, oldSnapshot, ref)
	assertEntries(t, oldScan, ref.scan("", ""))

	// The engine keeps accepting writes after the log switch.
	if err := db.Set("after-compact", []byte("1")); err != nil {
		t.Fatal(err)
	}
	ref.apply([]Op{{Kind: Set, Key: "after-compact", Value: []byte("1")}})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, info, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if info.Truncated {
		t.Fatalf("clean reopen after compact truncated: %+v", info)
	}
	reopened, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertReference(t, reopened, ref)
}

// TestCompactCrashStages interrupts the publish/switch sequence at every
// stage and confirms no committed data is lost and no half-written
// compacted batch becomes visible.
func TestCompactCrashStages(t *testing.T) {
	setup := func(t *testing.T) (string, referenceDB, []byte) {
		t.Helper()
		db, dir := openTestDB(t)
		ref := compactHistory(t, db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		wal, err := os.ReadFile(filepath.Join(dir, walName))
		if err != nil {
			t.Fatal(err)
		}
		return dir, ref, wal
	}
	compactedFrame := func(t *testing.T, ref referenceDB) []byte {
		t.Helper()
		var ops []Op
		for _, entry := range ref.scan("", "") {
			ops = append(ops, Op{Kind: Set, Key: entry.Key, Value: entry.Value})
		}
		payload, err := encodeBatch(ops)
		if err != nil {
			t.Fatal(err)
		}
		return encodeFrame(recordBatch, payload)
	}
	reopenAndCheck := func(t *testing.T, dir string, ref referenceDB) {
		t.Helper()
		db, info, err := Open(dir)
		if err != nil {
			t.Fatalf("reopen after crash: %v", err)
		}
		snapshot, err := db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		assertReference(t, snapshot, ref)
		// The engine stays writable and durable after recovery.
		if err := db.Set("post-crash", []byte("ok")); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, info, err = Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if info.Truncated {
			t.Fatalf("second reopen truncated: %+v", info)
		}
		snapshot, err = db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		want := ref.clone()
		want.apply([]Op{{Kind: Set, Key: "post-crash", Value: []byte("ok")}})
		assertReference(t, snapshot, want)
		if _, err := os.Stat(filepath.Join(dir, walName+compactTmpSuffix)); !os.IsNotExist(err) {
			t.Fatalf("leftover temp file after open: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Stage 1: crash while the new log is being written — every possible
	// torn prefix of the compacted frame sits in the temp file; kv.wal is
	// untouched.
	frame := compactedFrame(t, func() referenceDB {
		_, ref, _ := setup(t)
		return ref
	}())
	for cut := 0; cut < len(frame); cut++ {
		t.Run("tmp-torn-"+strconv.Itoa(cut), func(t *testing.T) {
			dir, ref, wal := setup(t)
			if err := os.WriteFile(filepath.Join(dir, walName+compactTmpSuffix), frame[:cut], 0o644); err != nil {
				t.Fatal(err)
			}
			if got, _ := os.ReadFile(filepath.Join(dir, walName)); !bytes.Equal(got, wal) {
				t.Fatal("kv.wal changed before the atomic switch")
			}
			reopenAndCheck(t, dir, ref)
		})
	}

	// Stage 2: crash after the temp file is fully synced but before the
	// rename — both files exist, kv.wal remains authoritative.
	t.Run("tmp-complete-not-renamed", func(t *testing.T) {
		dir, ref, _ := setup(t)
		if err := os.WriteFile(filepath.Join(dir, walName+compactTmpSuffix), frame, 0o644); err != nil {
			t.Fatal(err)
		}
		reopenAndCheck(t, dir, ref)
	})

	// Stage 3: crash after the atomic rename — kv.wal is the compacted log
	// and no temp file remains.
	t.Run("renamed", func(t *testing.T) {
		dir, ref, _ := setup(t)
		if err := os.WriteFile(filepath.Join(dir, walName), frame, 0o644); err != nil {
			t.Fatal(err)
		}
		reopenAndCheck(t, dir, ref)
	})
}
