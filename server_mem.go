package cbtx

type MemServerStore struct {
	replicas []Addr
}

func (s *MemServerStore) GoodFind(k Key, tsMininum Timestamp) (*Write, error) {
	return nil, nil
}

func (s *MemServerStore) PendingGet(k Key, tsRequired Timestamp) (*Write, error) {
	return nil, nil
}

func (s *MemServerStore) PendingAdd(w Write) error {
	return nil
}

func (s *MemServerStore) ReplicasFor(k Key) []Addr {
	return nil
}

func (s *MemServerStore) SendNotify(toReplica Addr, ts Timestamp) error {
	return nil
}

func (s *MemServerStore) AcksIncr(fromReplica Addr, ts Timestamp) (int, error) {
	return 0, nil
}

func (s *MemServerStore) AcksNeeded(ts Timestamp) int {
	return 0
}

func (s *MemServerStore) Promote(ts Timestamp) error {
	return nil
}
