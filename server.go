package cbtx

type Timestamp uint64 // Should be clientId + logicalClock.
type Addr string
type Key string

type Write struct {
	Key  Key
	Val  []byte    // When nil, the write is a deletion.
	Ts   Timestamp // Writes are orderable.
	Sibs []Key
}

// Client access interface.
type Server interface {
	Get(k Key, tsRequired Timestamp) (*Write, error)
	Set(w *Write) error
}

// Represents server-to-server communication.
type ServerPeer interface {
	SendNotify(toReplica Addr, k Key, ts Timestamp, acksNeeeded int) error
	ReplicasFor(k Key) []Addr
}

// Represents server-local persistence.
type ServerStore interface {
	GoodFind(k Key, tsMininum Timestamp) (*Write, error)
	PendingGet(k Key, ts Timestamp) (*Write, error)
	PendingAdd(w *Write) error
	PendingPromote(k Key, ts Timestamp) error
	Ack(k Key, ts Timestamp, fromReplica Addr) (int, error)
}

type ServerController struct {
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
			s.sp.SendNotify(replica, sibKey, w.Ts, acksNeeded)
		}
	}
	// TODO: Asynchronously send w to other replicas via anti-entropy.
	return nil
}

func (s *ServerController) Get(k Key, ts Timestamp) (*Write, error) {
	w, err := s.ss.GoodFind(k, ts)
	if err != nil || w != nil {
		return w, err
	}
	if ts == 0 {
		return nil, nil
	}
	return s.ss.PendingGet(k, ts)
}

func (s *ServerController) ReceiveNotify(fromReplica Addr,
	k Key, ts Timestamp, acksNeeded int) error {
	acks, err := s.ss.Ack(k, ts, fromReplica)
	if err != nil {
		return err
	}
	if acks >= acksNeeded {
		return s.ss.PendingPromote(k, ts)
	}
	return nil
}
