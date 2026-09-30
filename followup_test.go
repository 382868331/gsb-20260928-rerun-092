package kvengine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type crashHookError struct{}

func (crashHookError) Error() string { return "injected crash hook" }

func collectIterator(t *testing.T, iterator *Iterator) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	for iterator.Next() {
		value := iterator.Value()
		if value == nil {
			value = []byte{}
		}
		out[iterator.Key()] = append([]byte(nil), value...)
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func referenceRange(ref referenceDB, opts RangeOptions) map[string][]byte {
	keys := make([]string, 0)
	for key := range ref {
		if (opts.Start == "" || key >= opts.Start) && (opts.End == "" || key < opts.End) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		out[key] = append([]byte(nil), ref[key]...)
	}
	return out
}

func TestRangeSnapshotBoundariesAndIsolation(t *testing.T) {
	for _, opts := range []RangeOptions{
		{Start: "", End: ""},
		{Start: "a", End: "b"},
		{Start: "aa", End: "ab"},
		{Start: "aa", End: "b"},
	} {
		t.Run(fmt.Sprintf("%q-%q", opts.Start, opts.End), func(t *testing.T) {
			db, _ := openTestDB(t)
			t.Cleanup(func() { _ = db.Close() })
			ref := newReferenceDB()
			initial := []Op{
				{Kind: Set, Key: "a", Value: []byte("old-a")},
				{Kind: Set, Key: "aa", Value: []byte{}},
				{Kind: Set, Key: "ab", Value: []byte("keep-ab")},
				{Kind: Set, Key: "b", Value: []byte("old-b")},
			}
			if err := db.Batch(initial); err != nil {
				t.Fatal(err)
			}
			ref.apply(initial)
			iterator, err := db.Range(opts)
			if err != nil {
				t.Fatal(err)
			}
			updates := []Op{
				{Kind: Set, Key: "a", Value: []byte("new-a")},
				{Kind: Delete, Key: "ab"},
				{Kind: Set, Key: "aa", Value: []byte("new-aa")},
				{Kind: Set, Key: "aaa", Value: []byte("inserted")},
			}
			if err := db.Batch(updates); err != nil {
				t.Fatal(err)
			}
			got := collectIterator(t, iterator)
			want := referenceRange(ref, opts)
			if !bytesMapEqual(got, want) {
				t.Fatalf("range = %q; want %q", got, want)
			}

			current, err := db.Range(opts)
			if err != nil {
				t.Fatal(err)
			}
			currentRef := ref.clone()
			currentRef.apply(updates)
			if !bytesMapEqual(collectIterator(t, current), referenceRange(currentRef, opts)) {
				t.Fatal("new range iterator did not observe committed batch")
			}
		})
	}
}

func TestPersistentSnapshotSurvivesCompactionAndReopen(t *testing.T) {
	db, dir := openTestDB(t)
	initial := []Op{
		{Kind: Set, Key: "k", Value: []byte("old")},
		{Kind: Set, Key: "empty", Value: nil},
		{Kind: Set, Key: "adj", Value: []byte("1")},
	}
	if err := db.Batch(initial); err != nil {
		t.Fatal(err)
	}
	oldRef := referenceDB{}
	oldRef.apply(initial)
	snapshot, err := db.SaveSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshotID := snapshot.ID()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	updates := []Op{
		{Kind: Set, Key: "k", Value: []byte("new")},
		{Kind: Delete, Key: "adj"},
		{Kind: Set, Key: "later", Value: []byte("x")},
	}
	if err := db.Batch(updates); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("after-compact", []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, _, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	current, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	wantCurrent := oldRef.clone()
	wantCurrent.apply(updates)
	wantCurrent["after-compact"] = []byte("committed")
	if !bytesMapEqual(current, wantCurrent) {
		t.Fatalf("current after compact/reopen = %q; want %q", current, wantCurrent)
	}
	opened, err := db.OpenSnapshot(snapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesMapEqual(opened.Snapshot(), oldRef) {
		t.Fatalf("old snapshot = %q; want %q", opened.Snapshot(), oldRef)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionCrashStages(t *testing.T) {
	stages := []struct {
		name      string
		temporary string
		published bool
	}{
		{"before-checkpoint-rename", tempCheckpointName, false},
		{"before-wal-rename", tempWALName, true},
		{"after-wal-rename", "", true},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			defer func() {
				crashBeforeCheckpointRename = func() error { return nil }
				crashBeforeWALRename = func() error { return nil }
				crashAfterWALRename = func() error { return nil }
			}()
			var injected crashHookError
			crashBeforeCheckpointRename = func() error { return nil }
			crashBeforeWALRename = func() error { return nil }
			crashAfterWALRename = func() error { return nil }
			if stage.name == "before-checkpoint-rename" {
				crashBeforeCheckpointRename = func() error { return injected }
			} else if stage.name == "before-wal-rename" {
				crashBeforeWALRename = func() error { return injected }
			} else {
				crashAfterWALRename = func() error { return injected }
			}

			dir := t.TempDir()
			db, _, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			before := []Op{{Kind: Set, Key: "a", Value: []byte("old")}, {Kind: Set, Key: "empty", Value: []byte{}}}
			if err := db.Batch(before); err != nil {
				t.Fatal(err)
			}
			err = db.Compact()
			if !errors.Is(err, injected) {
				t.Fatalf("Compact error = %v; want injected crash", err)
			}
			if stage.temporary != "" {
				if _, err := os.Stat(filepath.Join(dir, stage.temporary)); err != nil {
					t.Fatalf("temporary file missing: %v", err)
				}
			}
			_ = db.Close()

			db, _, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			want := referenceDB{}
			want.apply(before)
			if !bytesMapEqual(got, want) {
				t.Fatalf("recovered data = %q; want %q", got, want)
			}
			if err := db.Set("post", []byte("committed")); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, _, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err = db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			want["post"] = []byte("committed")
			if !bytesMapEqual(got, want) {
				t.Fatalf("post-recovery commit lost: %q; want %q", got, want)
			}
			_ = db.Close()
			if stage.temporary != "" {
				if _, err := os.Stat(filepath.Join(dir, stage.temporary)); !os.IsNotExist(err) {
					t.Fatalf("temporary file survived reopen: %v", err)
				}
			}
		})
	}
}

func TestSnapshotPublishLeavesNoPartialFile(t *testing.T) {
	defer func() { crashBeforeSnapshotRename = func() error { return nil } }()
	var injected crashHookError
	crashBeforeSnapshotRename = func() error { return injected }
	db, dir := openTestDB(t)
	if err := db.Set("a", []byte("v")); err != nil {
		t.Fatal(err)
	}
	_, err := db.SaveSnapshot()
	if !errors.Is(err, injected) {
		t.Fatalf("SaveSnapshot error = %v; want injected crash", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, _, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	entries, err := os.ReadDir(filepath.Join(dir, snapshotsDirName))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" || bytes.Contains([]byte(entry.Name()), []byte(".tmp")) {
			t.Fatalf("partial snapshot published: %s", entry.Name())
		}
	}
}
