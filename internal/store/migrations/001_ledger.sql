-- Double-entry ledger schema. Idempotent: safe to run on every start.
CREATE TABLE IF NOT EXISTS transactions (
    id            TEXT PRIMARY KEY,               -- payment event id
    type          TEXT        NOT NULL,           -- pix.settled | pix.refunded
    end_to_end_id TEXT        NOT NULL,           -- Pix E2E id (BACEN)
    occurred_at   TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_e2e_type ON transactions (end_to_end_id, type);
CREATE INDEX IF NOT EXISTS ix_transactions_occurred_at ON transactions (occurred_at);

CREATE TABLE IF NOT EXISTS entries (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT   NOT NULL REFERENCES transactions (id),
    account_id     TEXT   NOT NULL,
    amount_cents   BIGINT NOT NULL CHECK (amount_cents <> 0)
);

CREATE INDEX IF NOT EXISTS ix_entries_account ON entries (account_id);

-- Materialized balances, updated in the same database transaction as entries.
CREATE TABLE IF NOT EXISTS account_balances (
    account_id    TEXT PRIMARY KEY,
    balance_cents BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
