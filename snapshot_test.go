package kvengine

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func reopen(t *testing.T, db *DB, dir string) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPersistentSnapshotSurvivesReopen(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Batch([]Op{
		{Kind: Set, Key: "a", Value: []byte("1")},
		{Kind: Set, Key: "b", Value: []byte("2")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveSnapshot("v1"); err != nil {
		t.Fatal(err)
	}
	// Overwrite and delete after saving: the old values still belong to v1.
	if err := db.Batch([]Op{
		{Kind: Set, Key: "a", Value: []byte("new")},
		{Kind: Delete, Key: "b"},
		{Kind: Set, Key: "c", Value: []byte("3")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveSnapshot("v2"); err != nil {
		t.Fatal(err)
	}

	// The saving process is gone; a fresh process reopens the directory.
	db = reopen(t, db, dir)

	names, err := db.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"v1", "v2"}) {
		t.Fatalf("ListSnapshots = %v", names)
	}

	// Read the current view and the snapshots in different orders; every
	// result must match what was committed when each snapshot was saved.
	current, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	v1, err := db.OpenSnapshot("v1")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := db.OpenSnapshot("v2")
	if err != nil {
		t.Fatal(err)
	}
	wantV1 := map[string][]byte{"a": []byte("1"), "b": []byte("2")}
	wantV2 := map[string][]byte{"a": []byte("new"), "c": []byte("3")}
	if !reflect.DeepEqual(v1, wantV1) {
		t.Fatalf("v1 = %v", v1)
	}
	if !reflect.DeepEqual(v2, wantV2) {
		t.Fatalf("v2 = %v", v2)
	}
	if !reflect.DeepEqual(current, wantV2) {
		t.Fatalf("current = %v", current)
	}
	// Reverse order, same results.
	v1again, err := db.OpenSnapshot("v1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v1again, wantV1) {
		t.Fatalf("v1 reread = %v", v1again)
	}
	// Mutating the returned map must not corrupt the saved snapshot.
	v1again["a"][0] = 'X'
	delete(v1again, "b")
	v1third, err := db.OpenSnapshot("v1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v1third, wantV1) {
		t.Fatalf("v1 after caller mutation = %v", v1third)
	}
}

func TestSnapshotCoexistsWithCompact(t *testing.T) {
	db, dir := openTestDB(t)
	ref := compactHistory(t, db)
	if err := db.SaveSnapshot("pinned"); err != nil {
		t.Fatal(err)
	}
	// More churn, then compaction reclaims WAL history. The snapshot file
	// pins its own copy, so compaction must not touch it.
	if err := db.Set("keep", []byte("v3")); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	pinned, err := db.OpenSnapshot("pinned")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pinned, map[string][]byte(ref.clone())) {
		t.Fatalf("pinned snapshot changed after Compact: %v", pinned)
	}
	db = reopen(t, db, dir)
	pinned, err = db.OpenSnapshot("pinned")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pinned, map[string][]byte(ref.clone())) {
		t.Fatalf("pinned snapshot changed after reopen: %v", pinned)
	}
}

func TestSaveSnapshotDuplicateAndInvalidNames(t *testing.T) {
	db, _ := openTestDB(t)
	if err := db.SaveSnapshot("s"); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveSnapshot("s"); !errors.Is(err, ErrSnapshotExists) {
		t.Fatalf("duplicate save err = %v", err)
	}
	for _, bad := range []string{"", ".", "..", "a/b", "a\\b", "a" + string(rune(0)) + "b"} {
		if err := db.SaveSnapshot(bad); !errors.Is(err, ErrInvalidSnapshotName) {
			t.Fatalf("save %q err = %v", bad, err)
		}
	}
	// The failed duplicate save must not have clobbered the original.
	if err := db.Set("k", []byte("later")); err != nil {
		t.Fatal(err)
	}
	snap, err := db.OpenSnapshot("s")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap["k"]; ok {
		t.Fatalf("duplicate save clobbered snapshot: %v", snap)
	}
}

func TestInterruptedSaveLeavesNoPartialView(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveSnapshot("ok"); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-publish: a leftover temporary file exists but
	// the final name was never linked.
	tmp := filepath.Join(dir, snapshotDirName, "half"+snapshotSuffix+snapshotTmpSuffix)
	if err := os.WriteFile(tmp, []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}
	db = reopen(t, db, dir)
	names, err := db.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"ok"}) {
		t.Fatalf("ListSnapshots = %v", names)
	}
	if _, err := db.OpenSnapshot("half"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("half-published snapshot visible, err = %v", err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leftover temp not discarded: %v", err)
	}
	// The complete snapshot saved before the crash is intact.
	ok, err := db.OpenSnapshot("ok")
	if err != nil {
		t.Fatal(err)
	}
	if string(ok["k"]) != "v" {
		t.Fatalf("ok = %v", ok)
	}
}

func TestCorruptSnapshotRejected(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.SaveSnapshot("s"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, snapshotDirName, "s"+snapshotSuffix)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.OpenSnapshot("s"); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("corrupt snapshot err = %v", err)
	}
	if err := os.WriteFile(path, raw[:3], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.OpenSnapshot("s"); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("truncated snapshot err = %v", err)
	}
}

func TestReleaseSnapshot(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.SaveSnapshot("keep"); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveSnapshot("drop"); err != nil {
		t.Fatal(err)
	}
	if err := db.ReleaseSnapshot("drop"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.OpenSnapshot("drop"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("released snapshot still readable, err = %v", err)
	}
	if err := db.ReleaseSnapshot("drop"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("second release err = %v", err)
	}
	// Releasing one snapshot must not affect others or live data.
	if err := db.Set("live", []byte("1")); err != nil {
		t.Fatal(err)
	}
	db = reopen(t, db, dir)
	names, err := db.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"keep"}) {
		t.Fatalf("ListSnapshots = %v", names)
	}
	if value, ok, _ := db.Get("live"); !ok || string(value) != "1" {
		t.Fatalf("live data lost: %q %v", value, ok)
	}
}
