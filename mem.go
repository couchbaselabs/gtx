package gtx

import (
	"fmt"
)

type MemStore struct { // Implements ServerStore interface for testing.
	pending map[Key]map[Timestamp]*Write
	stable  map[Key]map[Timestamp]*Write
	acks    map[Key]map[Timestamp]map[string]bool
}

func NewMemStore() *MemStore {
	return &MemStore{
		pending: map[Key]map[Timestamp]*Write{},
		stable:  map[Key]map[Timestamp]*Write{},
		acks:    map[Key]map[Timestamp]map[string]bool{},
	}
}

func (s *MemStore) StableFind(k Key, tsMinimum Timestamp) (*Write, error) {
	return findMaxWrite(s.stable[k], tsMinimum), nil
}

func findMaxWrite(tsMap map[Timestamp]*Write, tsMinimum Timestamp) *Write {
	var max *Write
	for _, w := range tsMap {
		if (max == nil || max.Ts < w.Ts) && w.Ts >= tsMinimum {
			max = w
		}
	}
	return max
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
	if w.Prev > 0 {
		prevPending := findMaxWrite(s.pending[w.Key], w.Prev)
		if prevPending != nil && prevPending.Ts > w.Prev {
			return fmt.Errorf("concurrent write already pending"+
				", prev ts: %v, ts: %v", prevPending.Ts, w.Prev)
		}
		prevStable := findMaxWrite(s.stable[w.Key], w.Prev)
		if prevStable != nil && prevStable.Ts > w.Prev {
			return fmt.Errorf("concurrent write already stable"+
				", prev ts: %v, ts: %v", prevStable.Ts, w.Prev)
		}
	}
	tsMap, ok := s.pending[w.Key]
	if !ok || tsMap == nil {
		tsMap = map[Timestamp]*Write{}
		s.pending[w.Key] = tsMap
	}
	tsMap[w.Ts] = w
	return nil
}

// Promote a pending write to stable.
func (s *MemStore) PendingPromote(k Key, ts Timestamp) error {
	ptsMap, ok := s.pending[k]
	if !ok || ptsMap == nil {
		return fmt.Errorf("no write to promote at k: %v, ts: %v", k, ts)
	}
	w, ok := ptsMap[ts]
	if !ok || w == nil {
		return fmt.Errorf("no write to promote at ts: %v, k: %v", ts, k)
	}
	gtsMap, ok := s.stable[k]
	if !ok || gtsMap == nil {
		gtsMap = map[Timestamp]*Write{}
		s.stable[k] = gtsMap
	}
	gtsMap[ts] = w
	// TODO: eventually clear out ptsMap and s.pending entries.
	return nil
}

func (s *MemStore) Ack(toKey Key, fromKey Key, ts Timestamp, fromReplica Addr) (int, error) {
	mt, ok := s.acks[toKey]
	if !ok || mt == nil {
		mt = map[Timestamp]map[string]bool{}
		s.acks[toKey] = mt
	}
	ma, ok := mt[ts]
	if !ok || ma == nil {
		ma = map[string]bool{}
		mt[ts] = ma
	}
	ma[fmt.Sprintf("%v:%v", fromReplica, fromKey)] = true
	return len(ma), nil
}

// ------------------------------------------------------------

type MemPeer struct { // Implements ServerPeer interface for testing.
	sc       *ServerController
	me       Addr
	everyone map[Addr]*MemPeer
	messages chan MemMsg
}

type MemMsg struct {
	dest       *MemPeer
	toKey      Key
	fromKey    Key
	ts         Timestamp
	acksNeeded int
}

func NewMemPeer(me Addr, everyone map[Addr]*MemPeer, messages chan MemMsg) *MemPeer {
	p := &MemPeer{nil, me, everyone, messages}
	everyone[me] = p
	return p
}

func (s *MemPeer) AsyncNotify(to Addr, toKey Key, fromKey Key, ts Timestamp, acksNeeded int) error {
	dest, ok := s.everyone[to]
	if !ok || dest == nil {
		return fmt.Errorf("no MemPeer.AsyncNotify dest: %v", to)
	}
	// Buffer messages in this channel.  A small channel can be used
	// by tests to simulate a clogged up system.
	s.messages <- MemMsg{dest, toKey, fromKey, ts, acksNeeded}
	return nil
}

func (s *MemPeer) ReplicasFor(k Key) []Addr {
	replicas := make([]Addr, 0, len(s.everyone))
	for a, _ := range s.everyone {
		replicas = append(replicas, a)
	}
	return replicas
}

// Allows caller to unclog maxSend number of messages.  A negative
// maxSend means send everything and return.
func (s *MemPeer) SendMessages(maxSend int) (sentOk, sentErr int) {
	for i := 0; maxSend < 0 || i < maxSend; i++ {
		select {
		case m := <-s.messages:
			err := m.dest.sc.ReceiveNotify(s.me, m.toKey, m.fromKey, m.ts, m.acksNeeded)
			if err == nil {
				sentOk++
			} else {
				sentErr++
			}
		default:
			return sentOk, sentErr
		}
	}
	return sentOk, sentErr
}
