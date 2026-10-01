package kvengine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mustSaveSnapshot(t *testing.T, db *DB, name string) {
	t.Helper()
	if err := db.SaveSnapshot(name); err != nil {
		t.Fatalf("SaveSnapshot(%q): %v", name, err)
	}
}

func mustSnapshotContent(t *testing.T, db *DB, name string) map[string][]byte {
	t.Helper()
	data, err := db.OpenSnapshot(name)
	if err != nil {
		t.Fatalf("OpenSnapshot(%q): %v", name, err)
	}
	return data
}

func mustListSnapshots(t *testing.T, db *DB) []string {
	t.Helper()
	names, err := db.ListSnapshots()
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	return names
}

func TestSnapshotPersistsAcrossReopen(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Batch([]Op{
		{Kind: Set, Key: "a", Value: []byte("one")},
		{Kind: Set, Key: "b", Value: []byte("old")},
	}); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "v1")
	// Overwrite and delete after saving: old values must stay in v1.
	if err := db.Batch([]Op{
		{Kind: Set, Key: "b", Value: []byte("new")},
		{Kind: Delete, Key: "a"},
		{Kind: Set, Key: "c", Value: []byte("later")},
	}); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "v2")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if names := mustListSnapshots(t, db); !reflect.DeepEqual(names, []string{"v1", "v2"}) {
		t.Fatalf("ListSnapshots = %v, want [v1 v2]", names)
	}
	wantV1 := map[string][]byte{"a": []byte("one"), "b": []byte("old")}
	if got := mustSnapshotContent(t, db, "v1"); !reflect.DeepEqual(got, wantV1) {
		t.Fatalf("v1 = %v, want %v", got, wantV1)
	}
	wantV2 := map[string][]byte{"b": []byte("new"), "c": []byte("later")}
	if got := mustSnapshotContent(t, db, "v2"); !reflect.DeepEqual(got, wantV2) {
		t.Fatalf("v2 = %v, want %v", got, wantV2)
	}
	// Current view is unaffected by snapshots.
	if value, ok, err := db.Get("b"); err != nil || !ok || string(value) != "new" {
		t.Fatalf("Get(b) = %q, %v, %v", value, ok, err)
	}
}

func TestSnapshotReadOrderAfterReopen(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Batch([]Op{{Kind: Set, Key: "k", Value: []byte("v1")}}); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "s1")
	if err := db.Batch([]Op{{Kind: Set, Key: "k", Value: []byte("v2")}}); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "s2")
	if err := db.Batch([]Op{{Kind: Delete, Key: "k"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and read in different orders: snapshot-then-current and
	// current-then-snapshot must both match the state at save time.
	for round, order := range [][]string{{"snap-first"}, {"current-first"}} {
		db, _, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		readCurrent := func() map[string][]byte {
			snap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			return snap
		}
		var current, s1, s2 map[string][]byte
		if order[0] == "snap-first" {
			s1 = mustSnapshotContent(t, db, "s1")
			s2 = mustSnapshotContent(t, db, "s2")
			current = readCurrent()
		} else {
			current = readCurrent()
			s2 = mustSnapshotContent(t, db, "s2")
			s1 = mustSnapshotContent(t, db, "s1")
		}
		if !reflect.DeepEqual(s1, map[string][]byte{"k": []byte("v1")}) {
			t.Fatalf("round %d: s1 = %v", round, s1)
		}
		if !reflect.DeepEqual(s2, map[string][]byte{"k": []byte("v2")}) {
			t.Fatalf("round %d: s2 = %v", round, s2)
		}
		if len(current) != 0 {
			t.Fatalf("round %d: current = %v, want empty", round, current)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotRelease(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "keep")
	mustSaveSnapshot(t, db, "drop")
	if err := db.ReleaseSnapshot("drop"); err != nil {
		t.Fatal(err)
	}
	if err := db.ReleaseSnapshot("drop"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("re-release = %v, want ErrSnapshotNotFound", err)
	}
	if _, err := db.OpenSnapshot("drop"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("OpenSnapshot(drop) = %v, want ErrSnapshotNotFound", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if names := mustListSnapshots(t, db); !reflect.DeepEqual(names, []string{"keep"}) {
		t.Fatalf("ListSnapshots = %v, want [keep]", names)
	}
	if got := mustSnapshotContent(t, db, "keep"); !reflect.DeepEqual(got, map[string][]byte{"a": []byte("1")}) {
		t.Fatalf("keep = %v", got)
	}
}

func TestSnapshotReleaseFailureKeepsData(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "snap")
	snapPath := filepath.Join(dir, snapshotDirName, "snap"+snapshotSuffix)
	before, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	// Make the snapshot directory read-only so os.Remove fails.
	snapDir := filepath.Join(dir, snapshotDirName)
	if err := os.Chmod(snapDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(snapDir, 0o755)
	if err := db.ReleaseSnapshot("snap"); err == nil {
		t.Fatal("ReleaseSnapshot should fail on read-only directory")
	}
	// The failed release must not have damaged the snapshot.
	after, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("snapshot file changed after failed release")
	}
	if got := mustSnapshotContent(t, db, "snap"); !reflect.DeepEqual(got, map[string][]byte{"a": []byte("1")}) {
		t.Fatalf("snap = %v", got)
	}
}

func TestSnapshotPublishCrashLeavesNoPartialView(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "good")
	good, err := os.ReadFile(filepath.Join(dir, snapshotDirName, "good"+snapshotSuffix))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	snapDir := filepath.Join(dir, snapshotDirName)
	// Simulate interrupted publishes of a new snapshot and of an overwrite
	// of the existing one: temp files truncated at every length, plus a
	// complete temp file never renamed.
	for i := 0; i <= len(good); i++ {
		for _, base := range []string{"new", "good"} {
			tmp := filepath.Join(snapDir, base+snapshotSuffix+snapshotTmpSuffix)
			if err := os.WriteFile(tmp, good[:i], 0o644); err != nil {
				t.Fatal(err)
			}
			db, _, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen with %d-byte temp for %s: %v", i, base, err)
			}
			if names := mustListSnapshots(t, db); !reflect.DeepEqual(names, []string{"good"}) {
				t.Fatalf("temp %s/%d: ListSnapshots = %v", base, i, names)
			}
			if got := mustSnapshotContent(t, db, "good"); !reflect.DeepEqual(got, map[string][]byte{"a": []byte("1")}) {
				t.Fatalf("temp %s/%d: good = %v", base, i, got)
			}
			if _, err := os.Lstat(tmp); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temp %s/%d not cleaned up", base, i)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSnapshotOverwriteIsAtomic(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "s")
	if err := db.Set("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "s")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := map[string][]byte{"a": []byte("1"), "b": []byte("2")}
	if got := mustSnapshotContent(t, db, "s"); !reflect.DeepEqual(got, want) {
		t.Fatalf("s = %v, want %v", got, want)
	}
}

func TestSnapshotCorruptFile(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "bad")
	snapPath := filepath.Join(dir, snapshotDirName, "bad"+snapshotSuffix)
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(snapPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.OpenSnapshot("bad"); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("OpenSnapshot(bad) = %v, want ErrCorruptSnapshot", err)
	}
	// A corrupt snapshot does not affect the current view or other snapshots.
	if value, ok, err := db.Get("a"); err != nil || !ok || string(value) != "1" {
		t.Fatalf("Get(a) = %q, %v, %v", value, ok, err)
	}
}

func TestSnapshotInvalidNames(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()
	for _, name := range []string{"", ".", "..", "a/b", "a\\b", "x" + snapshotTmpSuffix} {
		if err := db.SaveSnapshot(name); !errors.Is(err, ErrInvalidSnapshotName) {
			t.Fatalf("SaveSnapshot(%q) = %v", name, err)
		}
		if err := db.ReleaseSnapshot(name); !errors.Is(err, ErrInvalidSnapshotName) {
			t.Fatalf("ReleaseSnapshot(%q) = %v", name, err)
		}
	}
}

func TestSnapshotSurvivesCompaction(t *testing.T) {
	db, dir := openTestDB(t)
	if err := db.Batch([]Op{
		{Kind: Set, Key: "a", Value: []byte("one")},
		{Kind: Set, Key: "b", Value: []byte("old")},
	}); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "before")
	// Churn the WAL so compaction has garbage to reclaim.
	for i := 0; i < 10; i++ {
		if err := db.Set("b", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete("a"); err != nil {
		t.Fatal(err)
	}
	mustSaveSnapshot(t, db, "after")
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Snapshots still pin their history after compaction reclaimed the WAL.
	wantBefore := map[string][]byte{"a": []byte("one"), "b": []byte("old")}
	if got := mustSnapshotContent(t, db, "before"); !reflect.DeepEqual(got, wantBefore) {
		t.Fatalf("before = %v, want %v", got, wantBefore)
	}
	if got := mustSnapshotContent(t, db, "after"); !reflect.DeepEqual(got, map[string][]byte{"b": []byte("v9")}) {
		t.Fatalf("after = %v", got)
	}
	if _, ok, err := db.Get("a"); err != nil || ok {
		t.Fatalf("Get(a) = %v, %v; want deleted", ok, err)
	}
	if names := mustListSnapshots(t, db); !reflect.DeepEqual(names, []string{"after", "before"}) {
		t.Fatalf("ListSnapshots = %v", names)
	}
}
