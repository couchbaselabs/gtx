package cbtx

type Timestamp uint64
type Addr string
type Key string

type Write struct {
	Key  Key
	Val  []byte    // When nil, the write is a deletion.
	Ts   Timestamp // Writes are ordered.
	Sibs []Key
}

type Server interface {
	Get(k Key, tsRequired Timestamp) (*Write, error)
	Set(w Write) error
}

type ServerController struct {
	ss ServerStore
}

type ServerStore interface {
	GoodFind(k Key, tsMininum Timestamp) (*Write, error)
	PendingGet(k Key, tsRequired Timestamp) (*Write, error)
	PendingAdd(w Write) error
	ReplicasFor(k Key) []Addr
	SendNotify(toReplica Addr, ts Timestamp) error
	AcksIncr(fromReplica Addr, ts Timestamp) (int, error)
	AcksNeeded(ts Timestamp) int
	Promote(ts Timestamp) error
}

func NewServerController(ss ServerStore) *ServerController {
	return &ServerController{ss}
}

func (s *ServerController) Set(w Write) error {
	err := s.ss.PendingAdd(w)
	if err != nil {
		return err
	}
	for _, k := range w.Sibs {
		for _, replica := range s.ss.ReplicasFor(k) {
			s.ss.SendNotify(replica, w.Ts)
		}
	}
	// TODO: Asynchronously send w to other replicas via anti-entropy.
	return nil
}

func (s *ServerController) Get(k Key, tsRequired Timestamp) (*Write, error) {
	w, err := s.ss.GoodFind(k, tsRequired)
	if err != nil || w != nil {
		return w, err
	}
	if tsRequired == 0 {
		return nil, nil
	}
	return s.ss.PendingGet(k, tsRequired)
}

func (s *ServerController) ReceiveNotify(fromReplica Addr, ts Timestamp) error {
	acks, err := s.ss.AcksIncr(fromReplica, ts)
	if err != nil {
		return err
	}
	if acks >= s.ss.AcksNeeded(ts) {
		return s.ss.Promote(ts)
	}
	return nil
}
