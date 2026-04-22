-- Migration 011: trade_latency
-- Per-trade latency breakdown (buy & sell). Complements pipeline_latency
-- (which covers detection → execution handoff). This table captures the
-- execution engine's internal step timings and confirmation outcome.
--
-- Written by src/execution/mod.rs::log_latency (buy side)
-- and           src/exit/mod.rs::log_exit_latency (sell side).

CREATE TABLE IF NOT EXISTS trade_latency (
    id           BIGSERIAL PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    position_id  BIGINT,
    mint         TEXT NOT NULL,
    side         TEXT NOT NULL,           -- 'buy' | 'sell'

    -- Buy-side step breakdown (null on sell-side rows)
    quote_ms           INTEGER,
    swap_tx_ms         INTEGER,
    sign_ms            INTEGER,
    submit_confirm_ms  INTEGER,
    price_derive_ms    INTEGER,

    -- Totals / submission info (both sides)
    total_ms            INTEGER NOT NULL,
    used_jito           BOOLEAN,
    used_helius_sender  BOOLEAN,
    tx_sig              TEXT,

    -- Sell-side extras (null on buy-side rows)
    exit_reason   TEXT,
    attempts      INTEGER,
    slippage_bps  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_trade_latency_position_id ON trade_latency(position_id);
CREATE INDEX IF NOT EXISTS idx_trade_latency_mint        ON trade_latency(mint);
CREATE INDEX IF NOT EXISTS idx_trade_latency_side        ON trade_latency(side);
CREATE INDEX IF NOT EXISTS idx_trade_latency_created_at  ON trade_latency(created_at DESC);
