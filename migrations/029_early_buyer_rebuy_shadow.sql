-- ═══════════════════════════════════════════════════════════════════
--  Migration 029: Early-buyer rebuy shadow lane
--  Observe-only rows for the first-N-buyer rebuy signal. This is a
--  separate OR-candidate research lane and never forwards live by itself.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS early_buyer_rebuy_shadow (
    id                              BIGSERIAL PRIMARY KEY,
    mint                            TEXT NOT NULL UNIQUE,
    symbol                          TEXT,
    name                            TEXT,
    creator_wallet                  TEXT,
    token_created_at_ms             BIGINT,
    entry_trigger                   TEXT NOT NULL DEFAULT 'first5_any_rebuy_shadow',

    -- Signal definition at entry time.
    first_n                         INTEGER NOT NULL DEFAULT 5,
    min_rebuy_wallets               INTEGER NOT NULL DEFAULT 1,
    min_rebuy_sol                   DOUBLE PRECISION NOT NULL DEFAULT 0,
    first_buyer_wallets             JSONB,
    first_buyer_count               INTEGER,
    rebuy_wallets                   JSONB,
    rebuyer_count                   INTEGER,
    rebuy_count                     INTEGER,
    rebuy_sol_total                 DOUBLE PRECISION,
    rebuy_max_sol                   DOUBLE PRECISION,
    first_rebuy_after_secs          DOUBLE PRECISION,
    early_seller_wallets            JSONB,
    early_seller_count              INTEGER,

    -- Bonding-curve snapshot when the shadow row fired.
    entry_token_age_secs            DOUBLE PRECISION,
    entry_volume_sol                DOUBLE PRECISION,
    entry_buy_count                 INTEGER,
    entry_sell_count                INTEGER,
    entry_unique_buyers             INTEGER,
    entry_buy_sell_ratio            DOUBLE PRECISION,
    entry_buy_pressure_pct          DOUBLE PRECISION,
    entry_creator_rebuy             BOOLEAN,
    creator_sold_during_bc          BOOLEAN,
    whale_buy                       BOOLEAN,
    whale_buy_count                 INTEGER,
    whale_buy_max_sol               DOUBLE PRECISION,
    bc_progress_pct                 DOUBLE PRECISION,
    bc_virtual_sol_reserves         DOUBLE PRECISION,
    bc_virtual_token_reserves       DOUBLE PRECISION,
    bc_market_cap_usd               DOUBLE PRECISION,
    entry_price_usd                 DOUBLE PRECISION,
    entry_metrics                   JSONB,

    -- Graduation + price outcome tracking. Price columns mirror bc_paper_trades.
    graduated                       BOOLEAN NOT NULL DEFAULT FALSE,
    graduated_at                    TIMESTAMPTZ,
    initial_liquidity_sol           DOUBLE PRECISION,
    price_at_graduation             DOUBLE PRECISION,
    price_1m                        DOUBLE PRECISION,
    price_5m                        DOUBLE PRECISION,
    price_15m                       DOUBLE PRECISION,
    price_1h                        DOUBLE PRECISION,
    peak_price                      DOUBLE PRECISION,
    peak_multiplier                 DOUBLE PRECISION,

    status                          TEXT NOT NULL DEFAULT 'pending',
    status_message                  TEXT,
    completed_at                    TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ebrs_created_at
    ON early_buyer_rebuy_shadow(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ebrs_graduated
    ON early_buyer_rebuy_shadow(graduated);

CREATE INDEX IF NOT EXISTS idx_ebrs_rebuyer_count
    ON early_buyer_rebuy_shadow(rebuyer_count DESC);

CREATE INDEX IF NOT EXISTS idx_ebrs_peak_multiplier
    ON early_buyer_rebuy_shadow(peak_multiplier DESC);

CREATE INDEX IF NOT EXISTS idx_ebrs_status
    ON early_buyer_rebuy_shadow(status);