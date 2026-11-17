package ledger_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MelQLima/conciliacao-ledger-go/internal/idempotency"
	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/store"
)

func event(id, e2e string, typ ledger.EventType, cents int64) ledger.PaymentEvent {
	return ledger.PaymentEvent{
		EventID:      id,
		Type:         typ,
		EndToEndID:   e2e,
		PayerAccount: "customer-1",
		PayeeAccount: "merchant-1",
		AmountCents:  cents,
		OccurredAt:   time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestValidate(t *testing.T) {
	valid := event("evt-1", "E2E1", ledger.EventPixSettled, 1000)
	tests := []struct {
		name    string
		mutate  func(*ledger.PaymentEvent)
		wantErr bool
	}{
		{"valid event", func(*ledger.PaymentEvent) {}, false},
		{"missing event id", func(e *ledger.PaymentEvent) { e.EventID = " " }, true},
		{"unknown type", func(e *ledger.PaymentEvent) { e.Type = "boleto.paid" }, true},
		{"zero amount", func(e *ledger.PaymentEvent) { e.AmountCents = 0 }, true},
		{"negative amount", func(e *ledger.PaymentEvent) { e.AmountCents = -5 }, true},
		{"same accounts", func(e *ledger.PaymentEvent) { e.PayeeAccount = e.PayerAccount }, true},
		{"missing e2e", func(e *ledger.PaymentEvent) { e.EndToEndID = "" }, true},
		{"missing timestamp", func(e *ledger.PaymentEvent) { e.OccurredAt = time.Time{} }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid
			tt.mutate(&e)
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ledger.ErrInvalidEvent) {
				t.Fatalf("error should wrap ErrInvalidEvent, got %v", err)
			}
		})
	}
}

func TestFromEventIsBalanced(t *testing.T) {
	for _, typ := range []ledger.EventType{ledger.EventPixSettled, ledger.EventPixRefunded} {
		tx, err := ledger.FromEvent(event("evt", "E2E", typ, 2599))
		if err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if err := tx.CheckBalanced(); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if tx.AmountCents() != 2599 {
			t.Fatalf("%s: amount = %d", typ, tx.AmountCents())
		}
	}
}

func TestApplyAndRefund(t *testing.T) {
	ctx := context.Background()
	svc := ledger.NewService(store.NewMemory(), idempotency.NewMemory())

	mustApply(t, svc, event("evt-1", "E2E1", ledger.EventPixSettled, 10_000), ledger.ResultRecorded)
	mustApply(t, svc, event("evt-2", "E2E1", ledger.EventPixRefunded, 2_500), ledger.ResultRecorded)

	assertBalance(t, svc, "merchant-1", 7_500)
	assertBalance(t, svc, "customer-1", -7_500)

	if _, err := svc.Balance(ctx, "nobody"); !errors.Is(err, ledger.ErrUnknownAccount) {
		t.Fatalf("want ErrUnknownAccount, got %v", err)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	cases := map[string]ledger.Guard{"with redis-like guard": idempotency.NewMemory(), "database constraint only": nil}
	for name, guard := range cases {
		t.Run(name, func(t *testing.T) {
			svc := ledger.NewService(store.NewMemory(), guard)
			ev := event("evt-1", "E2E1", ledger.EventPixSettled, 10_000)
			mustApply(t, svc, ev, ledger.ResultRecorded)
			mustApply(t, svc, ev, ledger.ResultDuplicate)

			// Same end-to-end id and type with a new event id is still a duplicate
			// (PSPs re-send webhooks with fresh ids).
			ev.EventID = "evt-1-retry"
			mustApply(t, svc, ev, ledger.ResultDuplicate)
			assertBalance(t, svc, "merchant-1", 10_000)
		})
	}
}

func TestApplyConcurrentDuplicates(t *testing.T) {
	svc := ledger.NewService(store.NewMemory(), nil)
	ev := event("evt-1", "E2E1", ledger.EventPixSettled, 100)

	var wg sync.WaitGroup
	results := make(chan ledger.Result, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := svc.Apply(context.Background(), ev)
			if err != nil {
				t.Error(err)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)

	recorded := 0
	for r := range results {
		if r == ledger.ResultRecorded {
			recorded++
		}
	}
	if recorded != 1 {
		t.Fatalf("recorded %d times, want exactly 1", recorded)
	}
	assertBalance(t, svc, "merchant-1", 100)
}

func TestApplyInvalid(t *testing.T) {
	svc := ledger.NewService(store.NewMemory(), nil)
	r, err := svc.Apply(context.Background(), event("", "E2E1", ledger.EventPixSettled, 1))
	if r != ledger.ResultInvalid || !errors.Is(err, ledger.ErrInvalidEvent) {
		t.Fatalf("got %q, %v", r, err)
	}
}

func mustApply(t *testing.T, svc *ledger.Service, e ledger.PaymentEvent, want ledger.Result) {
	t.Helper()
	got, err := svc.Apply(context.Background(), e)
	if err != nil {
		t.Fatalf("Apply(%s): %v", e.EventID, err)
	}
	if got != want {
		t.Fatalf("Apply(%s) = %q, want %q", e.EventID, got, want)
	}
}

func assertBalance(t *testing.T, svc *ledger.Service, account string, want int64) {
	t.Helper()
	got, err := svc.Balance(context.Background(), account)
	if err != nil {
		t.Fatalf("Balance(%s): %v", account, err)
	}
	if got != want {
		t.Fatalf("Balance(%s) = %d, want %d", account, got, want)
	}
}
