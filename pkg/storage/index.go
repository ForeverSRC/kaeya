package storage

import (
	"context"
	"errors"
	"sync"
)

var ErrIndexMiss = errors.New("not found in index")

type InMemoryIndexer struct {
	mu   sync.RWMutex
	hash map[string]int64
}

func NewInMemoryIndexer() *InMemoryIndexer {
	return &InMemoryIndexer{
		mu:   sync.RWMutex{},
		hash: map[string]int64{},
	}
}

func (ir *InMemoryIndexer) Index(ctx context.Context, key string, offset int64) error {
	ir.mu.Lock()
	defer ir.mu.Unlock()
	
	ir.hash[key] = offset
	return nil
}

func (ir *InMemoryIndexer) Search(ctx context.Context, key string) (int64, error) {
	ir.mu.RLock()
	defer ir.mu.RUnlock()

	offset, ok := ir.hash[key]
	if !ok {
		return 0, ErrIndexMiss
	}

	return offset, nil
}
