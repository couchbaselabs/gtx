package gtx

import (
	"fmt"
	"sort"
)

type Transaction struct {
	s        Server
	ts       Timestamp
	reads    map[Key]Timestamp
	writes   map[Key][]byte
	required map[Key]Timestamp
}

func NewTransaction(s Server, ts Timestamp) *Transaction {
	if ts <= 0 {
		panic("ts must be > 0")
	}
	return &Transaction{
		s:        s,
		ts:       ts, // Should be logicalClock + f(clientId, random).
		reads:    map[Key]Timestamp{},
		writes:   map[Key][]byte{},
		required: map[Key]Timestamp{},
	}
}

func (t *Transaction) Set(k Key, v []byte) error {
	t.writes[k] = v
	return nil
}

func (t *Transaction) Del(k Key) error {
	t.writes[k] = nil // A deletion is marked by a nil Val.
	return nil
}

func (t *Transaction) Get(k Key) ([]byte, error) {
	v, ok := t.writes[k]
	if ok {
		return v, nil // For per-Transaction read-your-writes.
	}
	tsRequired, _ := t.required[k]
	w, err := t.s.Get(k, tsRequired)
	if err != nil {
		return nil, err
	}
	if w == nil {
		if tsRequired > 0 {
			return nil, fmt.Errorf("missing pending write, k: %v, tsRequired: %v",
				k, tsRequired)
		}
		return nil, nil
	}
	tsRead, _ := t.reads[k]
	if w.Ts > tsRead {
		t.reads[k] = w.Ts
	}
	for _, sibKey := range w.Sibs {
		sibTsRequired, _ := t.required[sibKey]
		if w.Ts > sibTsRequired {
			t.required[sibKey] = w.Ts
		}
	}
	return w.Val, nil
}

func (t *Transaction) Commit(errorIfConcurrent bool) error {
	arr := make([]string, 0, len(t.writes))
	for k, _ := range t.writes {
		arr = append(arr, (string)(k))
	}
	sort.Strings(arr) // To avoid deadlocks.
	sibs := make([]Key, 0, len(arr))
	for _, x := range arr {
		sibs = append(sibs, (Key)(x))
	}
	for _, k := range sibs {
		v, _ := t.writes[k]
		var tsRead Timestamp
		if errorIfConcurrent {
			tsRead, _ = t.reads[k] // Provide the ts we saw at read/Get() time.
		}
		err := t.s.Set(&Write{Key: k, Val: v, Ts: t.ts, Sibs: sibs, Prev: tsRead})
		if err != nil {
			return err
		}
	}
	t.done()
	return nil
}

func (t *Transaction) Abort() error {
	t.done()
	return nil
}

func (t *Transaction) done() {
	t.s = nil
	t.reads = nil
	t.writes = nil
	t.required = nil
}
