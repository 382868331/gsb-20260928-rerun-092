package kvengine

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

func (ref referenceDB) scan(start, end string) []Entry {
	keys := make([]string, 0, len(ref))
	for key := range ref {
		if key < start || (end != "" && key >= end) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, Entry{Key: key, Value: append([]byte(nil), ref[key]...)})
	}
	return entries
}

func assertEntries(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("scan length = %d, want %d; got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = %q:%q, want %q:%q", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}

// TestScanAgainstReference drives overwrites, deletes, empty values, and
// keys around range boundaries, then compares every interesting range
// against the independent model after each batch.
func TestScanAgainstReference(t *testing.T) {
	db, _ := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ref := newReferenceDB()

	history := [][]Op{
		{
			{Kind: Set, Key: "a", Value: []byte("1")},
			{Kind: Set, Key: "a\x00", Value: []byte("2")},
			{Kind: Set, Key: "aa", Value: []byte("3")},
			{Kind: Set, Key: "ab", Value: []byte{}}, // empty value
			{Kind: Set, Key: "b", Value: []byte("5")},
			{Kind: Set, Key: "b\x00", Value: []byte("6")},
		},
		{
			{Kind: Set, Key: "aa", Value: []byte("overwritten")},
			{Kind: Delete, Key: "ab"},
			{Kind: Delete, Key: "missing"},
		},
		{
			{Kind: Set, Key: "ab", Value: nil}, // nil value resurrects key
			{Kind: Delete, Key: "a"},
			{Kind: Set, Key: "a", Value: []byte("back")},
		},
	}
	bounds := []string{"", "a", "a\x00", "a\x00\x00", "aa", "ab", "ab\x00", "b", "b\x00", "c"}
	check := func() {
		t.Helper()
		for _, start := range bounds {
			for _, end := range bounds {
				if end != "" && end < start {
					continue
				}
				got, err := db.Scan(start, end)
				if err != nil {
					t.Fatal(err)
				}
				assertEntries(t, got, ref.scan(start, end))
			}
		}
	}
	check()
	for i, ops := range history {
		if err := db.Batch(ops); err != nil {
			t.Fatal(err)
		}
		ref.apply(ops)
		t.Run(fmt.Sprintf("after-batch-%d", i), func(t *testing.T) { check() })
	}
}

// TestScanFixedView verifies a scan result stays pinned to the view at scan
// start even while large batches overwrite and delete the scanned keys.
func TestScanFixedView(t *testing.T) {
	db, _ := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ref := newReferenceDB()
	var seed []Op
	for i := 0; i < 50; i++ {
		seed = append(seed, Op{Kind: Set, Key: fmt.Sprintf("k%03d", i), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	if err := db.Batch(seed); err != nil {
		t.Fatal(err)
	}
	ref.apply(seed)

	before, err := db.Scan("k010", "k040")
	if err != nil {
		t.Fatal(err)
	}
	assertEntries(t, before, ref.scan("k010", "k040"))

	// Overwrite the first half of the range, delete the second half, and
	// add new keys inside it while the caller is still "iterating".
	var churn []Op
	for i := 10; i < 25; i++ {
		churn = append(churn, Op{Kind: Set, Key: fmt.Sprintf("k%03d", i), Value: []byte("new")})
	}
	for i := 25; i < 40; i++ {
		churn = append(churn, Op{Kind: Delete, Key: fmt.Sprintf("k%03d", i)})
	}
	churn = append(churn, Op{Kind: Set, Key: "k015x", Value: []byte("intruder")})
	if err := db.Batch(churn); err != nil {
		t.Fatal(err)
	}
	ref.apply(churn)

	// The earlier result is unchanged: no old-then-new mixing.
	assertEntries(t, before, referenceDB(nil).pinnedScan(seed, "k010", "k040"))

	// A fresh scan sees the full new state, matching the model.
	after, err := db.Scan("k010", "k040")
	if err != nil {
		t.Fatal(err)
	}
	assertEntries(t, after, ref.scan("k010", "k040"))

	// Mutating the returned values does not leak into the engine.
	after[0].Value[0] = 'X'
	again, err := db.Scan(after[0].Key, after[0].Key+"\x00")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(again[0].Value, after[0].Value) {
		t.Fatal("mutating scan result changed the engine")
	}
}

// pinnedScan replays ops onto an empty model and scans it, giving the test
// an independent expectation for a historical view.
func (ref referenceDB) pinnedScan(ops []Op, start, end string) []Entry {
	model := newReferenceDB()
	model.apply(ops)
	return model.scan(start, end)
}
