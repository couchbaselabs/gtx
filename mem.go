package gtx

import (
	"fmt"
)

type MemStore struct { // Implements ServerStore interface.
	pending map[Key]map[Timestamp]*Write
	good    map[Key]map[Timestamp]*Write
	acks    map[Key]map[Timestamp]map[Addr]bool
}

func NewMemStore() *MemStore {
	return &MemStore{
		pending: map[Key]map[Timestamp]*Write{},
		good:    map[Key]map[Timestamp]*Write{},
		acks:    map[Key]map[Timestamp]map[Addr]bool{},
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

func (s *MemStore) PendingPromote(k Key, ts Timestamp) error {
	ptsMap, ok := s.pending[k]
	if !ok || ptsMap == nil {
		return fmt.Errorf("no write to promote at k: %v, ts: %v", k, ts)
	}
	w, ok := ptsMap[ts]
	if !ok || w == nil {
		return fmt.Errorf("no write to promote at ts: %v, k: %v", ts, k)
	}
	gtsMap, ok := s.good[k]
	if !ok || gtsMap == nil {
		gtsMap = map[Timestamp]*Write{}
		s.good[k] = gtsMap
	}
	gtsMap[ts] = w
	// TODO: eventually clear out ptsMap and s.pending entries.
	return nil
}

func (s *MemStore) Ack(k Key, ts Timestamp, fromReplica Addr) (int, error) {
	mt, ok := s.acks[k]
	if !ok || mt == nil {
		mt = map[Timestamp]map[Addr]bool{}
		s.acks[k] = mt
	}
	ma, ok := mt[ts]
	if !ok || ma == nil {
		ma = map[Addr]bool{}
		mt[ts] = ma
	}
	ma[fromReplica] = true
	return len(ma), nil
}

// ------------------------------------------------------------

type MemPeer struct { // Implements ServerPeer interface.
	everyone map[Addr]*MemPeer
}

func (s *MemPeer) SendNotify(toReplica Addr, k Key, ts Timestamp, acksNeeeded int) error {
	return nil
}

func (s *MemPeer) ReplicasFor(k Key) []Addr {
	replicas := make([]Addr, len(s.everyone))
	for a, _ := range s.everyone {
		replicas = append(replicas, a)
	}
	return replicas
}
