-- ═══════════════════════════════════════════════════════════════════
--  Migration 035: Flow-quality shadow monitors (rate-limited)
--  Data-only table for high-interest post-graduation continuation,
--  sell-absorption, proven-wallet overlap, creator lifecycle, and gated
--  top-holder flow snapshots. No live entry/exit logic reads this table.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS post_grad_flow_shadow (
    id                                  BIGSERIAL PRIMARY KEY,
    mint                                TEXT NOT NULL UNIQUE,
    symbol                              TEXT,
    name                                TEXT,
    creator_wallet                      TEXT,
    token_created_at_ms                 BIGINT,
    graduated_at                        TIMESTAMPTZ,
    initial_liquidity_sol               DOUBLE PRECISION,
    shadow_version                      TEXT,

    -- High-interest gate that allowed the shadow tracker to spend requests.
    gate_reasons                        JSONB,
    bc_score                            DOUBLE PRECISION,
    narrative_score                     DOUBLE PRECISION,
    narrative_sequence_score            DOUBLE PRECISION,
    normalized_label                    TEXT,
    cluster_rank                        INTEGER,
    prior_same_label_mints_6h           INTEGER,
    prior_same_label_creators_6h        INTEGER,
    seconds_since_label_seen            BIGINT,
    creator_prior_mints_6h              INTEGER,
    creator_same_label_prior_mints_6h   INTEGER,
    creator_seconds_since_last_mint     BIGINT,

    -- Bonding-curve flow snapshot at graduation.
    entry_token_age_secs                DOUBLE PRECISION,
    entry_volume_sol                    DOUBLE PRECISION,
    entry_buy_count                     INTEGER,
    entry_sell_count                    INTEGER,
    entry_unique_buyers                 INTEGER,
    entry_buy_pressure_pct              DOUBLE PRECISION,
    entry_buy_sell_ratio                DOUBLE PRECISION,
    entry_creator_rebuy                 BOOLEAN,
    creator_sold_during_bc              BOOLEAN,
    creator_buy_count_bc                INTEGER,
    creator_buy_sol_total_bc            DOUBLE PRECISION,
    creator_buy_max_sol_bc              DOUBLE PRECISION,
    creator_sell_count_bc               INTEGER,
    creator_sell_sol_total_bc           DOUBLE PRECISION,
    creator_net_sol_bc                  DOUBLE PRECISION,
    creator_buy_share_pct               DOUBLE PRECISION,
    whale_buy_count                     INTEGER,
    whale_buy_sol_total                 DOUBLE PRECISION,
    whale_buy_max_sol                   DOUBLE PRECISION,
    whale_sell_count                    INTEGER,
    whale_sell_sol_total                DOUBLE PRECISION,
    whale_sell_max_sol                  DOUBLE PRECISION,
    whale_net_sol                       DOUBLE PRECISION,
    early_buyer_buy_count               INTEGER,
    early_buyer_buy_sol_total           DOUBLE PRECISION,
    early_buyer_sell_count              INTEGER,
    early_buyer_sell_sol_total          DOUBLE PRECISION,
    early_buyer_net_sol                 DOUBLE PRECISION,
    proven_wallet_buy_count_bc          INTEGER,
    proven_wallet_buy_sol_total_bc      DOUBLE PRECISION,
    proven_wallet_sell_count_bc         INTEGER,
    proven_wallet_sell_sol_total_bc     DOUBLE PRECISION,
    proven_wallet_net_sol_bc            DOUBLE PRECISION,
    proven_wallet_unique_buyers_bc      INTEGER,
    proven_wallet_unique_sellers_bc     INTEGER,
    proven_wallets_seen                 JSONB,
    bc_progress_pct                     DOUBLE PRECISION,
    bc_virtual_sol_reserves             DOUBLE PRECISION,
    entry_price_usd                     DOUBLE PRECISION,

    -- First-minute post-grad continuation / sell-absorption proxy.
    baseline_price                      DOUBLE PRECISION,
    price_30s                           DOUBLE PRECISION,
    price_60s                           DOUBLE PRECISION,
    multiplier_30s                      DOUBLE PRECISION,
    multiplier_60s                      DOUBLE PRECISION,
    first_minute_peak_price             DOUBLE PRECISION,
    first_minute_low_price              DOUBLE PRECISION,
    first_minute_peak_multiplier        DOUBLE PRECISION,
    first_minute_close_multiplier       DOUBLE PRECISION,
    first_minute_drawdown_pct           DOUBLE PRECISION,
    first_minute_recovery_pct           DOUBLE PRECISION,
    absorption_status                   TEXT,

    -- Gated top-holder flow. These JSON snapshots are written only for the
    -- strongest candidates and are protected by RPC guard + max-active caps.
    top_holder_eligible                 BOOLEAN NOT NULL DEFAULT FALSE,
    top_holder_status                   TEXT,
    top_holder_initial                  JSONB,
    top_holder_final                    JSONB,
    top_holder_flow                     JSONB,
    top_holder_top1_initial_pct         DOUBLE PRECISION,
    top_holder_top10_initial_pct        DOUBLE PRECISION,
    top_holder_top1_final_pct           DOUBLE PRECISION,
    top_holder_top10_final_pct          DOUBLE PRECISION,
    top_holder_completed_at             TIMESTAMPTZ,

    status                              TEXT NOT NULL DEFAULT 'armed',
    created_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pgfs_created_at
    ON post_grad_flow_shadow(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pgfs_gate_gin
    ON post_grad_flow_shadow USING GIN (gate_reasons);

CREATE INDEX IF NOT EXISTS idx_pgfs_bc_score
    ON post_grad_flow_shadow(bc_score DESC);

CREATE INDEX IF NOT EXISTS idx_pgfs_narrative_score
    ON post_grad_flow_shadow(narrative_score DESC);

CREATE INDEX IF NOT EXISTS idx_pgfs_proven_wallets
    ON post_grad_flow_shadow(proven_wallet_buy_count_bc DESC);

CREATE INDEX IF NOT EXISTS idx_pgfs_absorption_status
    ON post_grad_flow_shadow(absorption_status);

CREATE INDEX IF NOT EXISTS idx_pgfs_top_holder_status
    ON post_grad_flow_shadow(top_holder_status);

COMMENT ON TABLE post_grad_flow_shadow IS
    'Shadow-only high-interest post-grad monitor: first-minute continuation, absorption, proven-wallet overlap, creator lifecycle, and gated top-holder flow.';

COMMENT ON COLUMN post_grad_flow_shadow.gate_reasons IS
    'Why this graduated token was worth spending shadow-monitor requests on (score, narrative, proven wallet, whale, creator support).';

COMMENT ON COLUMN post_grad_flow_shadow.absorption_status IS
    'first-minute price proxy: continued, absorbed, neutral, or dumped.';

COMMENT ON COLUMN post_grad_flow_shadow.top_holder_flow IS
    'Comparison of initial vs delayed top-holder snapshots. Written only under high-score/proven-wallet gates.';
