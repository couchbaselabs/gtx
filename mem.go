package cbtx

type MemStore struct { // Implements ServerStore interface.
	pending map[Key]map[Timestamp]*Write
	good    map[Key]map[Timestamp]*Write
	acks    map[Timestamp]map[Addr]bool
}

func NewMemStore() *MemStore {
	return &MemStore{
		pending: map[Key]map[Timestamp]*Write{},
		good:    map[Key]map[Timestamp]*Write{},
		acks:    map[Timestamp]map[Addr]bool{},
	}
}

func (s *MemStore) GoodFind(k Key, tsMininum Timestamp) (*Write, error) {
	tsMap, ok := s.good[k]
	if !ok || tsMap == nil {
		return nil, nil
	}
	var best *Write
	for _, w := range tsMap {
		if best == nil || best.Ts < w.Ts {
			best = w
		}
	}
	return best, nil
}

func (s *MemStore) PendingGet(k Key, ts Timestamp) (*Write, error) {
	tsMap, ok := s.pending[k]
	if !ok || tsMap == nil {
		return nil, nil
	}
	w, _ := tsMap[ts]
	return w, nil
}

func (s *MemStore) PendingAdd(w *Write) error {
	tsMap, ok := s.pending[w.Key]
	if !ok || tsMap == nil {
		tsMap = map[Timestamp]*Write{}
		s.pending[w.Key] = tsMap
	}
	tsMap[w.Ts] = w
	return nil
}

func (s *MemStore) PendingPromote(ts Timestamp) error {
	return nil
}

func (s *MemStore) Ack(ts Timestamp, fromReplica Addr) (int, error) {
	m, ok := s.acks[ts]
	if !ok || m == nil {
		m = map[Addr]bool{}
		s.acks[ts] = m
	}
	m[fromReplica] = true
	return len(m), nil
}

// ------------------------------------------------------------

type MemPeer struct { // Implements ServerPeer interface.
	everyone map[Addr]*MemPeer
}

func (s *MemPeer) SendNotify(toReplica Addr, ts Timestamp) error {
	return nil
}

func (s *MemPeer) ReplicasFor(k Key) []Addr {
	replicas := make([]Addr, len(s.everyone))
	for a, _ := range s.everyone {
		replicas = append(replicas, a)
	}
	return replicas
}

func (s *MemPeer) AcksNeeded(ts Timestamp) int {
	return len(s.everyone)
}
