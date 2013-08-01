package gtx

import (
	"bytes"
	"encoding/json"

	cb "github.com/couchbaselabs/go-couchbase"
)

var NUL = []byte{0}

const (
	STABLE_PREFIX  = "s_"
	PENDING_PREFIX = "p_"
)

type CBStore struct { // Implements ServerStore interface for testing.
	url            string // For connection.
	metaPoolName   string // Pool where we'll manage tx metadata.
	metaBucketName string // Bucket where we'll manage tx metadata.
	metaPrefix     string // Key prefix for tx metadata items for namespace de-collision.

	client     cb.Client
	metaPool   cb.Pool
	metaBucket *cb.Bucket
}

func NewCBStore(url, metaPoolName, metaBucketName, metaPrefix string) (*CBStore, error) {
	client, err := cb.Connect(url)
	if err != nil {
		return nil, err
	}
	metaPool, err := client.GetPool(metaPoolName)
	if err != nil {
		return nil, err
	}
	metaBucket, err := metaPool.GetBucket(metaBucketName)
	if err != nil {
		return nil, err
	}
	return &CBStore{
		url:            url,
		metaPoolName:   metaPoolName,
		metaBucketName: metaBucketName,
		metaPrefix:     metaPrefix,
		client:         client,
		metaPool:       metaPool,
		metaBucket:     metaBucket,
	}, nil
}

func (s *CBStore) StableFind(k Key, tsMinimum Timestamp) (*Write, error) {
	var c uint64
	b, err := s.metaBucket.GetsRaw(s.metaPrefix+STABLE_PREFIX+string(k), &c)
	if err != nil || len(b) <= 0 {
		return nil, err
	}
	var max *Write
	for _, x := range bytes.Split(b, NUL) {
		var w *Write
		err = json.Unmarshal(x, w)
		if err != nil {
			return nil, err
		}
		if (max == nil || max.Ts < w.Ts) && w.Ts >= tsMinimum {
			max = w
		}
	}
	// TODO: Perhaps look in non-meta bucket?
	// TODO: Randomly try to GC the stable sequence using the c CAS.
	return max, nil
}

func (s *CBStore) PendingGet(k Key, ts Timestamp) (*Write, error) {
	var w *Write
	err := s.metaBucket.Get(s.metaPrefix+PENDING_PREFIX+string(k)+
		":"+ts.String(), &w)
	return w, err
}

func (s *CBStore) PendingAdd(w *Write) error {
	return nil
}

func (s *CBStore) PendingPromote(k Key, ts Timestamp) error {
	return nil
}

func (s *CBStore) Ack(toKey Key, fromKey Key, ts Timestamp, fromReplica Addr) (int, error) {
	return 0, nil
}
