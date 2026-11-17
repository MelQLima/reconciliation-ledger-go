package idempotency

import (
	"context"
	"testing"
	"time"
)

func TestMemoryGuard(t *testing.T) {
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	g := NewMemory()
	g.now = func() time.Time { return now }
	ctx := context.Background()

	if ok, _ := g.Acquire(ctx, "k", time.Minute); !ok {
		t.Fatal("first acquire should succeed")
	}
	if ok, _ := g.Acquire(ctx, "k", time.Minute); ok {
		t.Fatal("second acquire within TTL should fail")
	}
	now = now.Add(2 * time.Minute)
	if ok, _ := g.Acquire(ctx, "k", time.Minute); !ok {
		t.Fatal("acquire after TTL should succeed")
	}
}
