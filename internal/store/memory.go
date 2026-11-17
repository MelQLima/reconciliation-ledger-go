// Package store contains Store implementations: an in-memory one for tests and
// local runs, and a PostgreSQL one (pgx) for production.
package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
)

// Memory is a thread-safe in-memory ledger store.
type Memory struct {
	mu       sync.RWMutex
	txs      map[string]ledger.Transaction
	balances map[string]int64
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{txs: map[string]ledger.Transaction{}, balances: map[string]int64{}}
}

// Append implements ledger.Store.
func (m *Memory) Append(_ context.Context, tx ledger.Transaction) error {
	if err := tx.CheckBalanced(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.txs[tx.ID]; ok {
		return ledger.ErrDuplicate
	}
	for _, other := range m.txs {
		if other.EndToEndID == tx.EndToEndID && other.Type == tx.Type {
			return ledger.ErrDuplicate
		}
	}
	m.txs[tx.ID] = tx
	for _, e := range tx.Entries {
		m.balances[e.AccountID] += e.AmountCents
	}
	return nil
}

// Balance implements ledger.Store.
func (m *Memory) Balance(_ context.Context, accountID string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.balances[accountID]
	if !ok {
		return 0, ledger.ErrUnknownAccount
	}
	return b, nil
}

// TransactionsBetween implements ledger.Store.
func (m *Memory) TransactionsBetween(_ context.Context, from, to time.Time) ([]ledger.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ledger.Transaction
	for _, tx := range m.txs {
		if !tx.OccurredAt.Before(from) && tx.OccurredAt.Before(to) {
			out = append(out, tx)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	return out, nil
}

// Ping implements ledger.Store.
func (m *Memory) Ping(context.Context) error { return nil }
