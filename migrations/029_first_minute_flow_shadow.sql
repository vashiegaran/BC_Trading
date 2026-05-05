-- =====================================================================
-- Migration 029: First-minute flow shadow candidates
-- =====================================================================
-- Plain on-chain/Pump.fun trade-flow shadow lane.
-- No Twitter, Telegram, DexScreener, Birdeye, GoPlus, or paid social data.
-- One row is written when a new Pump.fun mint shows explosive first-minute
-- buy flow. This is observe-only and never opens positions.
-- =====================================================================

CREATE TABLE IF NOT EXISTS first_minute_flow_shadow (
    id                         BIGSERIAL PRIMARY KEY,
    mint                       TEXT NOT NULL UNIQUE,
    name                       TEXT,
    symbol                     TEXT,
    creator_wallet             TEXT,

    detected_at                TIMESTAMPTZ,
    signal_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_age_ms               BIGINT NOT NULL,

    buy_count                  BIGINT NOT NULL,
    sell_count                 BIGINT NOT NULL,
    unique_buyers              BIGINT NOT NULL,
    buy_volume_sol             DOUBLE PRECISION NOT NULL,
    sell_volume_sol            DOUBLE PRECISION NOT NULL,
    total_volume_sol           DOUBLE PRECISION NOT NULL,
    largest_buy_sol            DOUBLE PRECISION NOT NULL,
    buy_pressure_pct           DOUBLE PRECISION NOT NULL,

    bc_progress_pct            DOUBLE PRECISION,
    virtual_sol_reserves       DOUBLE PRECISION,
    virtual_token_reserves     DOUBLE PRECISION,
    market_cap_sol             DOUBLE PRECISION,

    flow_score                 DOUBLE PRECISION NOT NULL,
    trigger_reason             TEXT NOT NULL,
    strategy_version           TEXT,
    is_shadow                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_first_minute_flow_shadow_signal_at
    ON first_minute_flow_shadow(signal_at DESC);
CREATE INDEX IF NOT EXISTS idx_first_minute_flow_shadow_score
    ON first_minute_flow_shadow(flow_score DESC);
CREATE INDEX IF NOT EXISTS idx_first_minute_flow_shadow_age
    ON first_minute_flow_shadow(token_age_ms);

COMMENT ON TABLE first_minute_flow_shadow IS
  'Observe-only first-minute Pump.fun flow candidates. Uses only live trade-flow data; no social or enrichment APIs.';
