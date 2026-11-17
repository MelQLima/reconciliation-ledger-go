// Package reconcile compares the bank (PSP) settlement statement with the
// ledger for a given day and reports every divergence.
package reconcile

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
)

// StatementLine is one settled Pix in the bank statement.
type StatementLine struct {
	EndToEndID  string
	AmountCents int64
	SettledAt   time.Time
}

// Divergence describes one problem found.
type Divergence struct {
	EndToEndID   string `json:"end_to_end_id"`
	Kind         string `json:"kind"` // missing_in_ledger | missing_in_bank | amount_mismatch
	BankCents    int64  `json:"bank_cents,omitempty"`
	LedgerCents  int64  `json:"ledger_cents,omitempty"`
}

// Report is the result of a reconciliation run.
type Report struct {
	Date        string       `json:"date"`
	BankLines   int          `json:"bank_lines"`
	LedgerTxs   int          `json:"ledger_transactions"`
	Matched     int          `json:"matched"`
	Divergences []Divergence `json:"divergences"`
}

// OK is true when bank and ledger agree.
func (r Report) OK() bool { return len(r.Divergences) == 0 }

// Lister is the subset of the ledger the reconciler needs.
type Lister interface {
	TransactionsBetween(ctx context.Context, from, to time.Time) ([]ledger.Transaction, error)
}

// ParseStatement reads a CSV with header end_to_end_id,amount_cents,settled_at (RFC 3339).
func ParseStatement(r io.Reader) ([]StatementLine, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	want := []string{"end_to_end_id", "amount_cents", "settled_at"}
	if len(header) != len(want) {
		return nil, fmt.Errorf("unexpected header %v, want %v", header, want)
	}
	for i := range want {
		if strings.TrimSpace(strings.ToLower(header[i])) != want[i] {
			return nil, fmt.Errorf("unexpected header %v, want %v", header, want)
		}
	}
	var lines []StatementLine
	for n := 2; ; n++ {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		amount, err := strconv.ParseInt(rec[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid amount %q", n, rec[1])
		}
		at, err := time.Parse(time.RFC3339, rec[2])
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid settled_at %q", n, rec[2])
		}
		lines = append(lines, StatementLine{EndToEndID: rec[0], AmountCents: amount, SettledAt: at.UTC()})
	}
	return lines, nil
}

// Reconciler runs reconciliations with a bounded worker pool.
type Reconciler struct {
	ledger  Lister
	workers int
}

// New builds a Reconciler. workers <= 0 means 4.
func New(l Lister, workers int) *Reconciler {
	if workers <= 0 {
		workers = 4
	}
	return &Reconciler{ledger: l, workers: workers}
}

// Run reconciles the statement against settled (non-refund) ledger
// transactions of the given UTC day.
func (r *Reconciler) Run(ctx context.Context, day time.Time, lines []StatementLine) (Report, error) {
	from := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	txs, err := r.ledger.TransactionsBetween(ctx, from, from.Add(24*time.Hour))
	if err != nil {
		return Report{}, fmt.Errorf("load ledger: %w", err)
	}
	byE2E := make(map[string]int64, len(txs))
	for _, tx := range txs {
		if tx.Type == ledger.EventPixSettled {
			byE2E[tx.EndToEndID] = tx.AmountCents()
		}
	}

	var (
		mu          sync.Mutex
		matched     int
		divergences []Divergence
		seen        = make(map[string]bool, len(lines))
	)
	jobs := make(chan StatementLine)
	g, gctx := errgroup.WithContext(ctx)

	for w := 0; w < r.workers; w++ {
		g.Go(func() error {
			for line := range jobs {
				if err := gctx.Err(); err != nil {
					return err
				}
				ledgerCents, ok := byE2E[line.EndToEndID]
				mu.Lock()
				seen[line.EndToEndID] = true
				switch {
				case !ok:
					divergences = append(divergences, Divergence{EndToEndID: line.EndToEndID, Kind: "missing_in_ledger", BankCents: line.AmountCents})
				case ledgerCents != line.AmountCents:
					divergences = append(divergences, Divergence{EndToEndID: line.EndToEndID, Kind: "amount_mismatch", BankCents: line.AmountCents, LedgerCents: ledgerCents})
				default:
					matched++
				}
				mu.Unlock()
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(jobs)
		for _, line := range lines {
			select {
			case jobs <- line:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return Report{}, err
	}

	for e2e, cents := range byE2E {
		if !seen[e2e] {
			divergences = append(divergences, Divergence{EndToEndID: e2e, Kind: "missing_in_bank", LedgerCents: cents})
		}
	}
	sort.Slice(divergences, func(i, j int) bool {
		if divergences[i].Kind != divergences[j].Kind {
			return divergences[i].Kind < divergences[j].Kind
		}
		return divergences[i].EndToEndID < divergences[j].EndToEndID
	})

	return Report{
		Date:        from.Format("2006-01-02"),
		BankLines:   len(lines),
		LedgerTxs:   len(byE2E),
		Matched:     matched,
		Divergences: divergences,
	}, nil
}
