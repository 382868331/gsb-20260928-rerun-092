package kvengine

import "testing"

func TestRepeatedCompactionKeepsCurrentAndOldSnapshots(t *testing.T) {
	db, dir := openTestDB(t)
	initial := []Op{
		{Kind: Set, Key: "a", Value: []byte("first")},
		{Kind: Set, Key: "b", Value: []byte{}},
	}
	if err := db.Batch(initial); err != nil {
		t.Fatal(err)
	}
	firstRef := newReferenceDB()
	firstRef.apply(initial)
	first, err := db.SaveSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.ID()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	second := []Op{{Kind: Set, Key: "a", Value: []byte("second")}, {Kind: Delete, Key: "b"}}
	if err := db.Batch(second); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	third := []Op{{Kind: Set, Key: "c", Value: []byte("third")}}
	if err := db.Batch(third); err != nil {
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
	want := firstRef.clone()
	want.apply(second)
	want.apply(third)
	if !bytesMapEqual(current, want) {
		t.Fatalf("current = %q; want %q", current, want)
	}
	old, err := db.OpenSnapshot(firstID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesMapEqual(old.Snapshot(), firstRef) {
		t.Fatalf("first snapshot = %q; want %q", old.Snapshot(), firstRef)
	}
}
