package reconcile_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/reconcile"
	"github.com/MelQLima/conciliacao-ledger-go/internal/store"
)

var day = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)

func seed(t *testing.T) *ledger.Service {
	t.Helper()
	svc := ledger.NewService(store.NewMemory(), nil)
	events := []ledger.PaymentEvent{
		{EventID: "1", Type: ledger.EventPixSettled, EndToEndID: "E2E-OK", PayerAccount: "c1", PayeeAccount: "m1", AmountCents: 1000, OccurredAt: day.Add(9 * time.Hour)},
		{EventID: "2", Type: ledger.EventPixSettled, EndToEndID: "E2E-DIFF", PayerAccount: "c2", PayeeAccount: "m1", AmountCents: 2000, OccurredAt: day.Add(10 * time.Hour)},
		{EventID: "3", Type: ledger.EventPixSettled, EndToEndID: "E2E-ONLY-LEDGER", PayerAccount: "c3", PayeeAccount: "m1", AmountCents: 3000, OccurredAt: day.Add(11 * time.Hour)},
		// Next day: must be ignored.
		{EventID: "4", Type: ledger.EventPixSettled, EndToEndID: "E2E-TOMORROW", PayerAccount: "c4", PayeeAccount: "m1", AmountCents: 4000, OccurredAt: day.Add(30 * time.Hour)},
		// Refunds are not part of the settlement statement.
		{EventID: "5", Type: ledger.EventPixRefunded, EndToEndID: "E2E-OK", PayerAccount: "c1", PayeeAccount: "m1", AmountCents: 100, OccurredAt: day.Add(12 * time.Hour)},
	}
	for _, e := range events {
		if _, err := svc.Apply(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	return svc
}

const statement = `end_to_end_id,amount_cents,settled_at
E2E-OK,1000,2026-11-16T09:00:01Z
E2E-DIFF,1999,2026-11-16T10:00:01Z
E2E-ONLY-BANK,500,2026-11-16T13:00:00Z
`

func TestRun(t *testing.T) {
	lines, err := reconcile.ParseStatement(strings.NewReader(statement))
	if err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 4, 16} {
		rep, err := reconcile.New(seed(t), workers).Run(context.Background(), day, lines)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Matched != 1 || rep.BankLines != 3 || rep.LedgerTxs != 3 || rep.OK() {
			t.Fatalf("workers=%d unexpected report: %+v", workers, rep)
		}
		want := []reconcile.Divergence{
			{EndToEndID: "E2E-DIFF", Kind: "amount_mismatch", BankCents: 1999, LedgerCents: 2000},
			{EndToEndID: "E2E-ONLY-LEDGER", Kind: "missing_in_bank", LedgerCents: 3000},
			{EndToEndID: "E2E-ONLY-BANK", Kind: "missing_in_ledger", BankCents: 500},
		}
		if len(rep.Divergences) != len(want) {
			t.Fatalf("workers=%d divergences = %+v", workers, rep.Divergences)
		}
		for i := range want {
			if rep.Divergences[i] != want[i] {
				t.Fatalf("workers=%d divergence[%d] = %+v, want %+v", workers, i, rep.Divergences[i], want[i])
			}
		}
	}
}

func TestParseStatementErrors(t *testing.T) {
	tests := map[string]string{
		"empty":        "",
		"wrong header": "id,amount,date\n",
		"bad amount":   "end_to_end_id,amount_cents,settled_at\nE2E,abc,2026-11-16T09:00:00Z\n",
		"bad date":     "end_to_end_id,amount_cents,settled_at\nE2E,10,16/11/2026\n",
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := reconcile.ParseStatement(strings.NewReader(in)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRunHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lines := make([]reconcile.StatementLine, 1000)
	if _, err := reconcile.New(seed(t), 2).Run(ctx, day, lines); err == nil {
		t.Fatal("expected context error")
	}
}
