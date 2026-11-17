// Package idempotency provides fast-path de-duplication guards.
package idempotency

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis uses SET key NX EX ttl: the first caller wins, the others see false.
type Redis struct {
	client *redis.Client
}

// NewRedis builds a Redis guard.
func NewRedis(addr string) *Redis {
	return &Redis{client: redis.NewClient(&redis.Options{Addr: addr})}
}

// Acquire implements ledger.Guard.
func (r *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, "1", ttl).Result()
}

// Close closes the client.
func (r *Redis) Close() error { return r.client.Close() }

// Memory is an in-process guard with expiration, used in tests and local runs.
type Memory struct {
	mu   sync.Mutex
	seen map[string]time.Time
	now  func() time.Time
}

// NewMemory returns an empty in-memory guard.
func NewMemory() *Memory {
	return &Memory{seen: map[string]time.Time{}, now: time.Now}
}

// Acquire implements ledger.Guard.
func (m *Memory) Acquire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if exp, ok := m.seen[key]; ok && now.Before(exp) {
		return false, nil
	}
	m.seen[key] = now.Add(ttl)
	return true, nil
}
