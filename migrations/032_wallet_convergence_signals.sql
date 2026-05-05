-- =====================================================================
-- Migration 032: Wallet convergence shadow signals
-- =====================================================================
-- Observe-only wallet graph lane. Records a stronger signal only when
-- multiple watched entities converge on the same Pump.fun mint:
--   - 2+ proven parents buy the same mint within a short window
--   - parent + derived child touch the same mint
--   - 2+ derived children touch the same mint
--
-- This table is independent from live execution and must not open trades.
-- =====================================================================

CREATE TABLE IF NOT EXISTS wallet_convergence_signals (
    id                         BIGSERIAL PRIMARY KEY,
    dedupe_key                 TEXT NOT NULL UNIQUE,
    mint                       TEXT NOT NULL,

    signal_reason              TEXT NOT NULL,
    signal_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_at              TIMESTAMPTZ,
    last_seen_at               TIMESTAMPTZ,

    parent_count               INTEGER NOT NULL DEFAULT 0,
    child_count                INTEGER NOT NULL DEFAULT 0,
    total_watched_buy_sol      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    max_parent_score           DOUBLE PRECISION,
    avg_parent_score           DOUBLE PRECISION,
    max_edge_score             DOUBLE PRECISION,
    convergence_score          DOUBLE PRECISION NOT NULL,

    bc_progress_pct            DOUBLE PRECISION,
    virtual_sol_reserves       DOUBLE PRECISION,
    virtual_token_reserves     DOUBLE PRECISION,
    market_cap_sol             DOUBLE PRECISION,

    parent_wallets             JSONB NOT NULL DEFAULT '[]'::jsonb,
    child_wallets              JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_context                JSONB NOT NULL DEFAULT '{}'::jsonb,

    checked_at                 TIMESTAMPTZ,
    max_return_10m             DOUBLE PRECISION,
    max_return_1h              DOUBLE PRECISION,
    max_return_24h             DOUBLE PRECISION,
    rugged                     BOOLEAN,
    graduated                  BOOLEAN,
    outcome_label              TEXT,
    outcome_notes              TEXT,

    strategy_version           TEXT,
    is_shadow                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_convergence_signals_mint
    ON wallet_convergence_signals(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_signals_signal_at
    ON wallet_convergence_signals(signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_signals_reason_time
    ON wallet_convergence_signals(signal_reason, signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_convergence_signals_score
    ON wallet_convergence_signals(convergence_score DESC);

COMMENT ON TABLE wallet_convergence_signals IS
  'Observe-only wallet graph convergence signals: multiple proven parents/derived children touching the same Pump.fun mint.';
