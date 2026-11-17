//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/store"
)

// Run with: go test -tags=integration ./internal/store/... (requires Docker).
func TestPostgresStore(t *testing.T) {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ledger"),
		postgres.WithUsername("ledger"),
		postgres.WithPassword("ledger"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pg, err := store.NewPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)

	svc := ledger.NewService(pg, nil)
	at := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	ev := ledger.PaymentEvent{EventID: "evt-1", Type: ledger.EventPixSettled, EndToEndID: "E2E1",
		PayerAccount: "c1", PayeeAccount: "m1", AmountCents: 5000, OccurredAt: at}

	// 20 concurrent deliveries of the same event: the unique constraints must
	// guarantee a single ledger transaction.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Apply(ctx, ev); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if b, err := svc.Balance(ctx, "m1"); err != nil || b != 5000 {
		t.Fatalf("balance = %d, %v", b, err)
	}
	if b, err := svc.Balance(ctx, "c1"); err != nil || b != -5000 {
		t.Fatalf("balance = %d, %v", b, err)
	}

	txs, err := svc.TransactionsBetween(ctx, at.Add(-time.Hour), at.Add(time.Hour))
	if err != nil || len(txs) != 1 || len(txs[0].Entries) != 2 {
		t.Fatalf("txs = %+v, %v", txs, err)
	}
}
