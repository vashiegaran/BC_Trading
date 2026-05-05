-- =====================================================================
-- Migration 030: Token lifecycle classifier research tables
-- =====================================================================
-- Separate research namespace for the new on-chain-only lifecycle classifier.
-- These tables are independent from the old sniper/paper-trade tables and
-- from the v0 first_minute_flow_shadow table.
--
-- Signal source policy:
--   - live Pump.fun launch/trade stream
--   - Solana/Pump.fun on-chain flow fields already present in the stream
--   - no Twitter, Telegram, news, DexScreener, Birdeye, GoPlus, Jupiter,
--     OpenAI, or other enrichment APIs
-- =====================================================================

-- One row per observed token tracked by the lifecycle classifier.
CREATE TABLE IF NOT EXISTS token_lifecycle_tokens (
    mint                         TEXT PRIMARY KEY,
    name                         TEXT,
    symbol                       TEXT,
    creator_wallet               TEXT,

    detected_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_trade_at               TIMESTAMPTZ,
    last_trade_at                TIMESTAMPTZ,
    graduated_at                 TIMESTAMPTZ,

    first_price_sol              DOUBLE PRECISION,
    first_market_cap_sol         DOUBLE PRECISION,
    first_virtual_sol_reserves   DOUBLE PRECISION,
    first_virtual_token_reserves DOUBLE PRECISION,

    tracking_status              TEXT NOT NULL DEFAULT 'active',
    terminal_state               TEXT,
    strategy_version             TEXT,

    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Every live classification event. A token may receive multiple classes over
-- time, e.g. FAST_IGNITION at 20s, FAKE_PUMP_RISK at 90s, DEAD_FLOW at 8m.
CREATE TABLE IF NOT EXISTS token_lifecycle_classifications (
    id                           BIGSERIAL PRIMARY KEY,
    mint                         TEXT NOT NULL REFERENCES token_lifecycle_tokens(mint) ON DELETE CASCADE,

    classified_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_age_ms                 BIGINT NOT NULL,
    class_name                   TEXT NOT NULL,
    class_group                  TEXT,
    class_score                  DOUBLE PRECISION NOT NULL,

    flow_score                   DOUBLE PRECISION,
    organic_score                DOUBLE PRECISION,
    risk_score                   DOUBLE PRECISION,
    whale_dependency_score       DOUBLE PRECISION,
    sell_pressure_score          DOUBLE PRECISION,
    continuation_score           DOUBLE PRECISION,

    price_sol                    DOUBLE PRECISION,
    market_cap_sol               DOUBLE PRECISION,
    bc_progress_pct              DOUBLE PRECISION,

    buy_count                    BIGINT,
    sell_count                   BIGINT,
    unique_buyers                BIGINT,
    buy_volume_sol               DOUBLE PRECISION,
    sell_volume_sol              DOUBLE PRECISION,
    net_buy_sol                  DOUBLE PRECISION,
    largest_buy_sol              DOUBLE PRECISION,
    repeat_wallet_ratio          DOUBLE PRECISION,

    metrics                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_live_classification       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Reduced-interval outcome snapshots for interesting tokens after the first
-- classification window. Intended schedule:
--   10m-30m every 1m, 30m-2h every 5m, 2h-6h every 15m, 6h-24h every 30m.
CREATE TABLE IF NOT EXISTS token_lifecycle_snapshots (
    id                           BIGSERIAL PRIMARY KEY,
    mint                         TEXT NOT NULL REFERENCES token_lifecycle_tokens(mint) ON DELETE CASCADE,

    snapshot_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    age_seconds                  BIGINT NOT NULL,
    phase                        TEXT NOT NULL,

    price_sol                    DOUBLE PRECISION,
    market_cap_sol               DOUBLE PRECISION,
    multiplier_from_first_class  DOUBLE PRECISION,
    multiplier_from_best_class   DOUBLE PRECISION,
    peak_multiplier_so_far       DOUBLE PRECISION,
    drawdown_from_peak_pct       DOUBLE PRECISION,

    buy_count_total              BIGINT,
    sell_count_total             BIGINT,
    unique_buyers_total          BIGINT,
    buy_volume_sol_total         DOUBLE PRECISION,
    sell_volume_sol_total        DOUBLE PRECISION,
    bc_progress_pct              DOUBLE PRECISION,
    graduated                    BOOLEAN NOT NULL DEFAULT FALSE,

    data_source                  TEXT NOT NULL DEFAULT 'pumpfun_ws_onchain',
    metrics                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Final 24h research label. This is outcome-only and must not be used as a
-- live signal. It exists to validate which live classes had real edge.
CREATE TABLE IF NOT EXISTS token_lifecycle_outcomes (
    mint                         TEXT PRIMARY KEY REFERENCES token_lifecycle_tokens(mint) ON DELETE CASCADE,

    first_class_name             TEXT,
    first_class_at               TIMESTAMPTZ,
    best_class_name              TEXT,
    best_class_at                TIMESTAMPTZ,

    peak_price_sol_24h           DOUBLE PRECISION,
    peak_market_cap_sol_24h      DOUBLE PRECISION,
    peak_multiplier_24h          DOUBLE PRECISION,
    low_multiplier_24h           DOUBLE PRECISION,

    time_to_2x_secs              BIGINT,
    time_to_3x_secs              BIGINT,
    time_to_5x_secs              BIGINT,
    time_to_10x_secs             BIGINT,
    time_to_20x_secs             BIGINT,
    time_to_50x_secs             BIGINT,
    time_to_100x_secs            BIGINT,

    graduated                    BOOLEAN NOT NULL DEFAULT FALSE,
    rugged_fast                  BOOLEAN NOT NULL DEFAULT FALSE,
    died_before_grad             BOOLEAN NOT NULL DEFAULT FALSE,
    final_outcome                TEXT,

    completed_at                 TIMESTAMPTZ,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_token_lifecycle_tokens_status
    ON token_lifecycle_tokens(tracking_status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_tokens_detected_at
    ON token_lifecycle_tokens(detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_token_lifecycle_classifications_mint_time
    ON token_lifecycle_classifications(mint, classified_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_classifications_class
    ON token_lifecycle_classifications(class_name, classified_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_classifications_score
    ON token_lifecycle_classifications(class_score DESC);

CREATE INDEX IF NOT EXISTS idx_token_lifecycle_snapshots_mint_time
    ON token_lifecycle_snapshots(mint, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_snapshots_phase
    ON token_lifecycle_snapshots(phase, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_snapshots_peak
    ON token_lifecycle_snapshots(peak_multiplier_so_far DESC);

CREATE INDEX IF NOT EXISTS idx_token_lifecycle_outcomes_final
    ON token_lifecycle_outcomes(final_outcome, peak_multiplier_24h DESC);
CREATE INDEX IF NOT EXISTS idx_token_lifecycle_outcomes_peak
    ON token_lifecycle_outcomes(peak_multiplier_24h DESC);

COMMENT ON TABLE token_lifecycle_tokens IS
  'One token per lifecycle-classifier observation. On-chain/Pump.fun stream only.';
COMMENT ON TABLE token_lifecycle_classifications IS
  'Live behavior class events such as FAST_IGNITION, DELAYED_IGNITION, CLEAN_GRIND, FAKE_PUMP_RISK.';
COMMENT ON TABLE token_lifecycle_snapshots IS
  'Reduced-interval 24h outcome snapshots for interesting lifecycle-classified tokens.';
COMMENT ON TABLE token_lifecycle_outcomes IS
  'Final research labels used to validate whether live classifications predicted 2x/10x+/rug/death outcomes.';
