package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Postgres is a ledger.Store backed by PostgreSQL using a pgx connection pool.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to PostgreSQL and runs the embedded migrations.
func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// Close releases the pool.
func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) migrate(ctx context.Context) error {
	sql, err := migrations.ReadFile("migrations/001_ledger.sql")
	if err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// Append inserts the transaction and its entries atomically. The primary key
// on transactions.id and the unique (end_to_end_id, type) index make the
// operation idempotent.
func (p *Postgres) Append(ctx context.Context, tx ledger.Transaction) error {
	if err := tx.CheckBalanced(); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, p.pool, func(dbtx pgx.Tx) error {
		_, err := dbtx.Exec(ctx,
			`INSERT INTO transactions (id, type, end_to_end_id, occurred_at) VALUES ($1, $2, $3, $4)`,
			tx.ID, string(tx.Type), tx.EndToEndID, tx.OccurredAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
				return ledger.ErrDuplicate
			}
			return err
		}
		batch := &pgx.Batch{}
		for _, e := range tx.Entries {
			batch.Queue(`INSERT INTO entries (transaction_id, account_id, amount_cents) VALUES ($1, $2, $3)`,
				tx.ID, e.AccountID, e.AmountCents)
			batch.Queue(`INSERT INTO account_balances (account_id, balance_cents) VALUES ($1, $2)
				ON CONFLICT (account_id) DO UPDATE SET balance_cents = account_balances.balance_cents + EXCLUDED.balance_cents,
				updated_at = now()`, e.AccountID, e.AmountCents)
		}
		return dbtx.SendBatch(ctx, batch).Close()
	})
}

// Balance reads the materialized balance of an account.
func (p *Postgres) Balance(ctx context.Context, accountID string) (int64, error) {
	var b int64
	err := p.pool.QueryRow(ctx, `SELECT balance_cents FROM account_balances WHERE account_id = $1`, accountID).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ledger.ErrUnknownAccount
	}
	return b, err
}

// TransactionsBetween loads transactions and entries for a time window.
func (p *Postgres) TransactionsBetween(ctx context.Context, from, to time.Time) ([]ledger.Transaction, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT t.id, t.type, t.end_to_end_id, t.occurred_at, e.account_id, e.amount_cents
		FROM transactions t JOIN entries e ON e.transaction_id = t.id
		WHERE t.occurred_at >= $1 AND t.occurred_at < $2
		ORDER BY t.occurred_at, t.id, e.id`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ledger.Transaction
	index := map[string]int{}
	for rows.Next() {
		var (
			id, typ, e2e, account string
			at                    time.Time
			amount                int64
		)
		if err := rows.Scan(&id, &typ, &e2e, &at, &account, &amount); err != nil {
			return nil, err
		}
		i, ok := index[id]
		if !ok {
			out = append(out, ledger.Transaction{ID: id, Type: ledger.EventType(typ), EndToEndID: e2e, OccurredAt: at.UTC()})
			i = len(out) - 1
			index[id] = i
		}
		out[i].Entries = append(out[i].Entries, ledger.Entry{AccountID: account, AmountCents: amount})
	}
	return out, rows.Err()
}

// Ping checks connectivity (used by /readyz).
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }
