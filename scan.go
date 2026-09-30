package kvengine

import "sort"

// Entry is a single key/value pair returned by Scan.
type Entry struct {
	Key   string
	Value []byte
}

// Scan returns all committed entries with keys in [start, end), ordered by
// key. An empty end means no upper bound. The result is a fixed view taken
// at the moment Scan begins: batches committed while the caller iterates
// the returned slice do not change it, and mutating the returned values
// does not affect the engine.
func (db *DB) Scan(start, end string) ([]Entry, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		if key < start {
			continue
		}
		if end != "" && key >= end {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, Entry{Key: key, Value: cloneBytes(db.data[key])})
	}
	return entries, nil
}
