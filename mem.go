package cbtx

type MemStore struct { // Implements ServerStore interface.
	replicas []Addr
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

func (s *MemStore) ReplicasFor(k Key) []Addr {
	return nil
}

func (s *MemStore) SendNotify(toReplica Addr, ts Timestamp) error {
	return nil
}

func (s *MemStore) AcksIncr(fromReplica Addr, ts Timestamp) (int, error) {
	return 0, nil
}

func (s *MemStore) AcksNeeded(ts Timestamp) int {
	return 0
}

func (s *MemStore) Promote(ts Timestamp) error {
	return nil
}
