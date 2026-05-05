-- =====================================================================
-- Migration 031: Proven-wallet graph shadow lane
-- =====================================================================
-- On-chain-only wallet graph research.
-- Solana wallets are created offline, so there is no literal on-chain
-- wallet-created event. This schema records the actionable equivalent:
-- a proven parent wallet funds a fresh child wallet, then that child/parent
-- touches a Pump.fun mint. Observe-only; never opens positions.
-- =====================================================================

CREATE TABLE IF NOT EXISTS proven_wallets (
    wallet                         TEXT PRIMARY KEY,
    label                          TEXT NOT NULL DEFAULT 'UNKNOWN_PROVEN',
    status                         TEXT NOT NULL DEFAULT 'active',
    parent_score                   DOUBLE PRECISION NOT NULL DEFAULT 60.0,
    source                         TEXT,
    discovered_from                TEXT,
    win_rate                       DOUBLE PRECISION,
    avg_entry_age_ms               BIGINT,
    sample_size                    INTEGER,
    last_reviewed_at               TIMESTAMPTZ,
    notes                          TEXT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT proven_wallets_status_chk
        CHECK (status IN ('active', 'paused', 'removed'))
);

CREATE INDEX IF NOT EXISTS idx_proven_wallets_status_score
    ON proven_wallets(status, parent_score DESC);
CREATE INDEX IF NOT EXISTS idx_proven_wallets_label
    ON proven_wallets(label);

CREATE TABLE IF NOT EXISTS wallet_funding_edges (
    id                                  BIGSERIAL PRIMARY KEY,
    edge_key                            TEXT NOT NULL UNIQUE,
    parent_wallet                       TEXT NOT NULL,
    child_wallet                        TEXT NOT NULL,
    parent_label                        TEXT,
    parent_score                        DOUBLE PRECISION,
    funded_at                           TIMESTAMPTZ NOT NULL,
    amount_sol                          DOUBLE PRECISION NOT NULL,
    tx_signature                        TEXT NOT NULL,
    slot                                BIGINT,
    child_previous_signature_count      INTEGER,
    is_fresh_child                      BOOLEAN NOT NULL DEFAULT TRUE,
    edge_type                           TEXT NOT NULL DEFAULT 'PROVEN_PARENT_FUNDED_FRESH_CHILD',
    edge_score                          DOUBLE PRECISION NOT NULL,
    source                              TEXT NOT NULL DEFAULT 'solana_rpc',
    is_shadow                           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_funding_edges_parent_time
    ON wallet_funding_edges(parent_wallet, funded_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_funding_edges_child_time
    ON wallet_funding_edges(child_wallet, funded_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_funding_edges_score
    ON wallet_funding_edges(edge_score DESC);

CREATE TABLE IF NOT EXISTS derived_wallets (
    wallet                         TEXT PRIMARY KEY,
    parent_wallet                  TEXT NOT NULL,
    parent_label                   TEXT,
    parent_score                   DOUBLE PRECISION,
    first_edge_key                 TEXT,
    first_seen_at                  TIMESTAMPTZ NOT NULL,
    funded_amount_sol              DOUBLE PRECISION,
    child_previous_signature_count INTEGER,
    edge_score                     DOUBLE PRECISION,
    activity_status                TEXT NOT NULL DEFAULT 'funded',
    first_pump_mint                TEXT,
    first_pump_action              TEXT,
    first_pump_at                  TIMESTAMPTZ,
    last_seen_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    risk_label                     TEXT,
    notes                          TEXT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT derived_wallets_activity_status_chk
        CHECK (activity_status IN ('funded', 'active', 'expired', 'ignored'))
);

CREATE INDEX IF NOT EXISTS idx_derived_wallets_parent
    ON derived_wallets(parent_wallet);
CREATE INDEX IF NOT EXISTS idx_derived_wallets_first_seen
    ON derived_wallets(first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_derived_wallets_status
    ON derived_wallets(activity_status);

CREATE TABLE IF NOT EXISTS wallet_linked_mint_signals (
    id                         BIGSERIAL PRIMARY KEY,
    dedupe_key                 TEXT NOT NULL UNIQUE,
    mint                       TEXT NOT NULL,
    name                       TEXT,
    symbol                     TEXT,
    parent_wallet              TEXT,
    child_wallet               TEXT,
    parent_label               TEXT,
    parent_score               DOUBLE PRECISION,
    edge_score                 DOUBLE PRECISION,
    signal_type                TEXT NOT NULL,
    signal_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_age_ms               BIGINT,
    amount_sol                 DOUBLE PRECISION,
    wallet_graph_score         DOUBLE PRECISION NOT NULL,
    flow_score                 DOUBLE PRECISION,
    buy_count                  BIGINT,
    sell_count                 BIGINT,
    unique_buyers              BIGINT,
    buy_volume_sol             DOUBLE PRECISION,
    sell_volume_sol            DOUBLE PRECISION,
    largest_buy_sol            DOUBLE PRECISION,
    buy_pressure_pct           DOUBLE PRECISION,
    bc_progress_pct            DOUBLE PRECISION,
    virtual_sol_reserves       DOUBLE PRECISION,
    virtual_token_reserves     DOUBLE PRECISION,
    market_cap_sol             DOUBLE PRECISION,
    child_count                INTEGER,
    trigger_reason             TEXT NOT NULL,
    raw_context                JSONB,
    strategy_version           TEXT,
    is_shadow                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_linked_mint_signals_mint
    ON wallet_linked_mint_signals(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_linked_mint_signals_parent_time
    ON wallet_linked_mint_signals(parent_wallet, signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_linked_mint_signals_child_time
    ON wallet_linked_mint_signals(child_wallet, signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_linked_mint_signals_type_time
    ON wallet_linked_mint_signals(signal_type, signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_linked_mint_signals_score
    ON wallet_linked_mint_signals(wallet_graph_score DESC);

CREATE TABLE IF NOT EXISTS wallet_graph_outcomes (
    id                         BIGSERIAL PRIMARY KEY,
    signal_id                  BIGINT REFERENCES wallet_linked_mint_signals(id) ON DELETE CASCADE,
    mint                       TEXT NOT NULL,
    signal_type                TEXT,
    signal_at                  TIMESTAMPTZ,
    checked_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    max_return_10m             DOUBLE PRECISION,
    max_return_1h              DOUBLE PRECISION,
    max_return_24h             DOUBLE PRECISION,
    rugged                     BOOLEAN,
    graduated                  BOOLEAN,
    outcome_label              TEXT,
    notes                      TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_graph_outcomes_mint
    ON wallet_graph_outcomes(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_graph_outcomes_checked
    ON wallet_graph_outcomes(checked_at DESC);

COMMENT ON TABLE proven_wallets IS
  'Active roster of proven parent wallets for on-chain-only wallet graph research.';
COMMENT ON TABLE wallet_funding_edges IS
  'Observed parent-to-fresh-child SOL funding edges. Fresh wallet means low prior visible signature count, not literal creation.';
COMMENT ON TABLE derived_wallets IS
  'Fresh child wallets derived from proven parent funding edges and watched for Pump.fun activity.';
COMMENT ON TABLE wallet_linked_mint_signals IS
  'Observe-only Pump.fun mint signals linked to proven parent/child wallet graph behavior.';
COMMENT ON TABLE wallet_graph_outcomes IS
  'Outcome tracking for wallet-linked mint signals.';

-- Optional manual seed example after migration:
-- INSERT INTO proven_wallets(wallet, label, parent_score, source, notes)
-- VALUES ('<PARENT_WALLET>', 'EARLY_FLOW_SCALPER', 70, 'manual', 'Manually reviewed proven wallet')
-- ON CONFLICT (wallet) DO UPDATE SET
--   label = EXCLUDED.label,
--   parent_score = EXCLUDED.parent_score,
--   status = 'active',
--   updated_at = NOW();
