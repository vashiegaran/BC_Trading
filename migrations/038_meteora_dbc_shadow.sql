-- ═══════════════════════════════════════════════════════════════════
--  Migration 038: Meteora DBC shadow lane
--  Observe-only research table for Solana Meteora Dynamic Bonding Curve
--  launches discovered outside the PumpFun bonding-curve pipeline.
--
--  No live execution path reads this table. The in-process Rust collector
--  writes only research rows and never forwards to filters/execution.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS meteora_dbc_shadow (
    id                              BIGSERIAL PRIMARY KEY,
    mint                            TEXT NOT NULL UNIQUE,
    symbol                          TEXT,
    name                            TEXT,

    -- Discovery / pair identity.
    source                          TEXT NOT NULL DEFAULT 'dexscreener_search',
    chain_id                        TEXT NOT NULL DEFAULT 'solana',
    dbc_dex_id                      TEXT DEFAULT 'meteoradbc',
    dbc_pair_address                TEXT,
    dbc_pair_url                    TEXT,
    dbc_pair_created_at             TIMESTAMPTZ,
    meteora_pair_address            TEXT,
    meteora_pair_url                TEXT,
    meteora_pair_created_at         TIMESTAMPTZ,

    -- Current DexScreener snapshot.
    price_usd                       DOUBLE PRECISION,
    market_cap_usd                  DOUBLE PRECISION,
    fdv_usd                         DOUBLE PRECISION,
    liquidity_usd                   DOUBLE PRECISION,
    liquidity_base                  DOUBLE PRECISION,
    liquidity_quote                 DOUBLE PRECISION,
    volume_m5_usd                   DOUBLE PRECISION,
    volume_h1_usd                   DOUBLE PRECISION,
    volume_h6_usd                   DOUBLE PRECISION,
    volume_h24_usd                  DOUBLE PRECISION,
    txns_m5_buys                    INTEGER,
    txns_m5_sells                   INTEGER,
    txns_h1_buys                    INTEGER,
    txns_h1_sells                   INTEGER,
    txns_h6_buys                    INTEGER,
    txns_h6_sells                   INTEGER,
    txns_h24_buys                   INTEGER,
    txns_h24_sells                  INTEGER,
    buy_pressure_h1_pct             DOUBLE PRECISION,
    buy_sell_ratio_h1               DOUBLE PRECISION,
    price_change_m5_pct             DOUBLE PRECISION,
    price_change_h1_pct             DOUBLE PRECISION,
    price_change_h6_pct             DOUBLE PRECISION,
    price_change_h24_pct            DOUBLE PRECISION,

    -- Shadow scoring. This is PumpFun-score inspired but DBC-specific.
    meteora_dbc_score               DOUBLE PRECISION,
    score_reasons                   JSONB,
    score_penalties                 JSONB,
    would_trade_shadow              BOOLEAN NOT NULL DEFAULT FALSE,
    min_score_threshold             DOUBLE PRECISION,

    -- Outcome tracking across repeated collector polls.
    first_seen_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_market_cap_usd       DOUBLE PRECISION,
    first_seen_price_usd            DOUBLE PRECISION,
    peak_market_cap_usd             DOUBLE PRECISION,
    peak_price_usd                  DOUBLE PRECISION,
    peak_multiplier                 DOUBLE PRECISION,
    sample_count                    INTEGER NOT NULL DEFAULT 1,
    last_seen_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_status                     TEXT NOT NULL DEFAULT 'tracking',
    raw_pairs                       JSONB,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_shadow_created_at
    ON meteora_dbc_shadow(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_shadow_last_seen_at
    ON meteora_dbc_shadow(last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_shadow_score
    ON meteora_dbc_shadow(meteora_dbc_score DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_shadow_would_trade
    ON meteora_dbc_shadow(would_trade_shadow, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_shadow_peak_multiplier
    ON meteora_dbc_shadow(peak_multiplier DESC);

COMMENT ON TABLE meteora_dbc_shadow IS
    'Observe-only Meteora Dynamic Bonding Curve launch research lane. Not read by live execution.';

COMMENT ON COLUMN meteora_dbc_shadow.meteora_dbc_score IS
    'PumpFun-score-inspired but DBC-specific shadow score; use only for research until proven.';

COMMENT ON COLUMN meteora_dbc_shadow.would_trade_shadow IS
    'Research flag only. No live trading code reads this flag.';