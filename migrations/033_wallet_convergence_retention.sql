-- =====================================================================
-- Migration 033: Wallet convergence retention snapshots
-- =====================================================================
-- Observe-only follow-up layer for migration 032. Records whether the
-- watched wallets that converged on a mint held, added, partially exited,
-- or dumped during post-signal checkpoints.
--
-- This table is independent from live execution and must not open trades.
-- =====================================================================

CREATE TABLE IF NOT EXISTS wallet_convergence_retention_snapshots (
    id                           BIGSERIAL PRIMARY KEY,
    dedupe_key                   TEXT NOT NULL UNIQUE,
    convergence_dedupe_key       TEXT NOT NULL,
    mint                         TEXT NOT NULL,

    signal_reason                TEXT NOT NULL,
    retention_reason             TEXT NOT NULL,
    checkpoint_secs              INTEGER NOT NULL,
    signal_at                    TIMESTAMPTZ NOT NULL,
    checked_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    parent_count                 INTEGER NOT NULL DEFAULT 0,
    child_count                  INTEGER NOT NULL DEFAULT 0,
    watched_buy_sol              DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    watched_sell_sol             DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    watched_net_sol              DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    sell_through_pct             DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    retained_wallet_count        INTEGER NOT NULL DEFAULT 0,
    exited_wallet_count          INTEGER NOT NULL DEFAULT 0,
    added_wallet_count           INTEGER NOT NULL DEFAULT 0,
    added_buy_sol_after_signal   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    retention_score              DOUBLE PRECISION NOT NULL,

    bc_progress_pct              DOUBLE PRECISION,
    virtual_sol_reserves         DOUBLE PRECISION,
    virtual_token_reserves       DOUBLE PRECISION,
    market_cap_sol               DOUBLE PRECISION,

    parent_wallets               JSONB NOT NULL DEFAULT '[]'::jsonb,
    child_wallets                JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_context                  JSONB NOT NULL DEFAULT '{}'::jsonb,

    strategy_version             TEXT,
    is_shadow                    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_mint
    ON wallet_convergence_retention_snapshots(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_signal_at
    ON wallet_convergence_retention_snapshots(signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_checked_at
    ON wallet_convergence_retention_snapshots(checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_reason
    ON wallet_convergence_retention_snapshots(retention_reason, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_score
    ON wallet_convergence_retention_snapshots(retention_score DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_retention_source
    ON wallet_convergence_retention_snapshots(convergence_dedupe_key);

COMMENT ON TABLE wallet_convergence_retention_snapshots IS
  'Observe-only follow-up snapshots showing whether converged watched wallets held, added, or dumped after a convergence signal.';
