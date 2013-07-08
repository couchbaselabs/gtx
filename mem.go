package cbtx

type MemStore struct { // Implements ServerStore interface.
	pending map[Key]Timestamp
	good    map[Key]Timestamp
	acks    map[Timestamp]int
	writes  map[Timestamp][]Write
}

func NewMemStore() *MemStore {
	return &MemStore{
		pending: map[Key]Timestamp{},
		good:    map[Key]Timestamp{},
		acks:    map[Timestamp]int{},
		writes:  map[Timestamp][]Write{},
	}
}

func (s *MemStore) GoodFind(k Key, tsMininum Timestamp) (*Write, error) {
	return nil, nil
}

func (s *MemStore) PendingGet(k Key, tsRequired Timestamp) (*Write, error) {
	return nil, nil
}

func (s *MemStore) PendingAdd(w Write) error {
	return nil
}

func (s *MemStore) PendingPromote(ts Timestamp) error {
	return nil
}

func (s *MemStore) AcksIncr(fromReplica Addr, ts Timestamp) (int, error) {
	return 0, nil
}

