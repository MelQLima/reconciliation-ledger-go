package ledger

import (
	"context"
	"errors"
	"time"
)

// Store persists transactions. Implementations must reject a transaction whose
// ID was already stored with ErrDuplicate (idempotency at the database level).
type Store interface {
	Append(ctx context.Context, tx Transaction) error
	Balance(ctx context.Context, accountID string) (int64, error)
	TransactionsBetween(ctx context.Context, from, to time.Time) ([]Transaction, error)
	Ping(ctx context.Context) error
}

// Guard is a fast-path de-duplication check (Redis SETNX in production) that
// avoids hitting the database for events that were already processed.
type Guard interface {
	// Acquire returns true the first time a key is seen within the TTL.
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// Result tells the caller what happened with an event.
type Result string

const (
	ResultRecorded  Result = "recorded"
	ResultDuplicate Result = "duplicate"
	ResultInvalid   Result = "invalid"
)

// Service applies payment events to the ledger.
type Service struct {
	store Store
	guard Guard
	ttl   time.Duration
}

// NewService builds a Service. guard may be nil.
func NewService(store Store, guard Guard) *Service {
	return &Service{store: store, guard: guard, ttl: 24 * time.Hour}
}

// Apply records a payment event exactly once (at-least-once delivery +
// idempotent processing = effectively-once).
func (s *Service) Apply(ctx context.Context, e PaymentEvent) (Result, error) {
	tx, err := FromEvent(e)
	if err != nil {
		return ResultInvalid, err
	}
	if s.guard != nil {
		first, gerr := s.guard.Acquire(ctx, "ledger:event:"+e.EventID, s.ttl)
		if gerr == nil && !first {
			return ResultDuplicate, nil
		}
		// If Redis is down we fall back to the database unique constraint.
	}
	if err := s.store.Append(ctx, tx); err != nil {
		if errors.Is(err, ErrDuplicate) {
			return ResultDuplicate, nil
		}
		return "", err
	}
	return ResultRecorded, nil
}

// Balance returns the balance of an account in cents.
func (s *Service) Balance(ctx context.Context, accountID string) (int64, error) {
	return s.store.Balance(ctx, accountID)
}

// TransactionsBetween lists transactions in [from, to).
func (s *Service) TransactionsBetween(ctx context.Context, from, to time.Time) ([]Transaction, error) {
	return s.store.TransactionsBetween(ctx, from, to)
}

// Ready reports whether the backing store is reachable.
func (s *Service) Ready(ctx context.Context) error { return s.store.Ping(ctx) }
