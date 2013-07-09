package gtx

type Transaction struct {
	s        Server
	ts       Timestamp
	reads    map[Key]Timestamp
	writes   map[Key][]byte
	required map[Key]Timestamp
}

func NewTransaction(s Server, ts Timestamp) *Transaction {
	return &Transaction{
		s:        s,
		ts:       ts, // Should be clientId + logicalClock.
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
	if err != nil || w == nil {
		return nil, err
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

func (t *Transaction) Commit() error {
	sibs := make([]Key, 0, len(t.writes))
	for k, _ := range t.writes {
		sibs = append(sibs, k)
	}
	for k, v := range t.writes {
		tsRead, _ := t.reads[k]
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
	t.writes = nil
	t.required = nil
}
