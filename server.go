package gtx

// Non-blocking transactional atomicity (NBTA) algorithm
// from http://www.bailis.org/blob/non-blocking-transactional-atomicity

type Timestamp uint64 // Should be logicalClock + f(clientId, random).
type Addr string
type Key string

type Write struct {
	Key  Key
	Val  []byte    // When nil, the write is a deletion.
	Ts   Timestamp // Writes are orderable.
	Sibs []Key
	Prev Timestamp
}

// Client access interface.
type Server interface {
	Get(k Key, tsRequired Timestamp) (*Write, error)
	Set(w *Write) error
}

// Represents server-to-server communication.
type ServerPeer interface {
	AsyncNotify(toReplica Addr, toKey Key, fromKey Key, ts Timestamp, acksNeeeded int) error
	ReplicasFor(k Key) []Addr
}

// Represents server-local persistence.
type ServerStore interface {
	StableFind(k Key, tsMininum Timestamp) (*Write, error)
	PendingGet(k Key, ts Timestamp) (*Write, error)
	PendingAdd(w *Write) error
	PendingPromote(k Key, ts Timestamp) error
	Ack(toKey Key, fromKey Key, ts Timestamp, fromReplica Addr) (int, error)
}

type ServerController struct { // Implements Server interface.
	sp ServerPeer
	ss ServerStore
}

func NewServerController(sp ServerPeer, ss ServerStore) *ServerController {
	return &ServerController{sp, ss}
}

func (s *ServerController) Set(w *Write) error {
	err := s.ss.PendingAdd(w)
	if err != nil {
		return err
	}
	for _, sibKey := range w.Sibs {
		replicas := s.sp.ReplicasFor(sibKey)
		acksNeeded := len(w.Sibs) * len(replicas)
		for _, replica := range replicas {
			err := s.sp.AsyncNotify(replica, sibKey, w.Key, w.Ts, acksNeeded)
			if err != nil {
				return err
			}
		}
	}
	// TODO: Asynchronously send w to other replicas via anti-entropy.
	return nil
}

func (s *ServerController) Get(k Key, ts Timestamp) (*Write, error) {
	w, err := s.ss.StableFind(k, ts)
	if err != nil || w != nil {
		return w, err
	}
	if ts == 0 {
		return nil, nil
	}
	// TODO: Optimization if we can read from pending, then the ts
	// must come from a client which knows the ts is already stable on
	// some peer server.  So we can theoretically promote the pending
	// write to stable right now.
	return s.ss.PendingGet(k, ts)
}

func (s *ServerController) ReceiveNotify(fromReplica Addr,
	toKey Key, fromKey Key, ts Timestamp, acksNeeded int) error {
	acks, err := s.ss.Ack(toKey, fromKey, ts, fromReplica)
	if err != nil {
		return err
	}
	if acks >= acksNeeded {
		return s.ss.PendingPromote(toKey, ts)
	}
	return nil
}
