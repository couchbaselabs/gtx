package gtx

import (
	cb "github.com/couchbaselabs/go-couchbase"
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
	return nil, nil
}

func (s *CBStore) PendingGet(k Key, ts Timestamp) (*Write, error) {
	return nil, nil
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
