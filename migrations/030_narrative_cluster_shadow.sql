-- ═══════════════════════════════════════════════════════════════════
--  Migration 030: Narrative-cluster armed post-grad shadow lane
--  Observe-only rows for repeated narrative/name clusters. This lane can
--  tolerate creator rebuy for research, but never forwards live execution.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS narrative_cluster_shadow (
    id                                  BIGSERIAL PRIMARY KEY,
    mint                                TEXT NOT NULL UNIQUE,
    symbol                              TEXT,
    name                                TEXT,
    creator_wallet                      TEXT,
    token_created_at_ms                 BIGINT,

    -- Narrative cluster identity.
    normalized_label                    TEXT,
    cluster_rank                        INTEGER,
    prior_same_label_mints_6h           INTEGER,
    prior_same_label_creators_6h        INTEGER,
    seconds_since_label_seen            BIGINT,

    -- Shadow arming decision.
    entry_trigger                       TEXT NOT NULL DEFAULT 'narrative_cluster_armed_post_grad',
    armed_at                            TIMESTAMPTZ,
    narrative_score                     DOUBLE PRECISION,
    score_reasons                       JSONB,
    score_penalties                     JSONB,
    score_breakdown                     JSONB,
    creator_rebuy_bypassed              BOOLEAN NOT NULL DEFAULT FALSE,
    would_trade_live                    BOOLEAN NOT NULL DEFAULT FALSE,

    -- Bonding-curve snapshot when the row armed.
    entry_token_age_secs                DOUBLE PRECISION,
    entry_volume_sol                    DOUBLE PRECISION,
    entry_buy_count                     INTEGER,
    entry_sell_count                    INTEGER,
    entry_unique_buyers                 INTEGER,
    entry_buy_sell_ratio                DOUBLE PRECISION,
    entry_buy_pressure_pct              DOUBLE PRECISION,
    entry_creator_rebuy                 BOOLEAN,
    creator_sold_during_bc              BOOLEAN,
    whale_buy                           BOOLEAN,
    whale_buy_count                     INTEGER,
    whale_buy_max_sol                   DOUBLE PRECISION,

    -- Early-buyer rebuy overlap for scoring/noise analysis.
    first_buyer_wallets                 JSONB,
    first_buyer_count                   INTEGER,
    rebuy_wallets                       JSONB,
    rebuyer_count                       INTEGER,
    rebuy_count                         INTEGER,
    rebuy_sol_total                     DOUBLE PRECISION,
    rebuy_max_sol                       DOUBLE PRECISION,
    first_rebuy_after_secs              DOUBLE PRECISION,
    early_seller_wallets                JSONB,
    early_seller_count                  INTEGER,

    -- Bonding-curve state at arm time.
    bc_progress_pct                     DOUBLE PRECISION,
    bc_virtual_sol_reserves             DOUBLE PRECISION,
    bc_virtual_token_reserves           DOUBLE PRECISION,
    bc_market_cap_usd                   DOUBLE PRECISION,
    entry_price_usd                     DOUBLE PRECISION,
    entry_metrics                       JSONB,

    -- Armed-post-grad simulation outcome.
    graduated                           BOOLEAN NOT NULL DEFAULT FALSE,
    graduated_at                        TIMESTAMPTZ,
    initial_liquidity_sol               DOUBLE PRECISION,
    sim_entry_at                        TIMESTAMPTZ,
    sim_entry_price                     DOUBLE PRECISION,
    price_at_graduation                 DOUBLE PRECISION,
    price_1m                            DOUBLE PRECISION,
    price_5m                            DOUBLE PRECISION,
    price_15m                           DOUBLE PRECISION,
    price_1h                            DOUBLE PRECISION,
    peak_price                          DOUBLE PRECISION,
    peak_multiplier                     DOUBLE PRECISION,

    status                              TEXT NOT NULL DEFAULT 'armed',
    status_message                      TEXT,
    completed_at                        TIMESTAMPTZ,
    created_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ncs_created_at
    ON narrative_cluster_shadow(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ncs_label_created_at
    ON narrative_cluster_shadow(normalized_label, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ncs_score
    ON narrative_cluster_shadow(narrative_score DESC);

CREATE INDEX IF NOT EXISTS idx_ncs_graduated
    ON narrative_cluster_shadow(graduated);

CREATE INDEX IF NOT EXISTS idx_ncs_peak_multiplier
    ON narrative_cluster_shadow(peak_multiplier DESC);

CREATE INDEX IF NOT EXISTS idx_ncs_status
    ON narrative_cluster_shadow(status);